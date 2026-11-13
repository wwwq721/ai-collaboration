"""Bind an authenticated caller and each host submission to its registered original chat."""
import hashlib
import json
import time
import uuid

from access_policy import chat_ref, compact
from delivery_ticket import DeliveryTickets
from workbuddy import BridgeError


def owner_of(peer):
    return "origin:" + hashlib.sha256(compact(chat_ref(peer)).encode("utf8")).hexdigest()


def notice_id_for(source, message_id):
    origin = dict(zip(("device_id", "software", "conversation_id"), chat_ref(source)))
    encoded = json.dumps(origin, ensure_ascii=True, sort_keys=True)
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "ai-result:" + encoded + ":" + message_id))


class ManagedBridge:
    def __init__(self, root, local, peers, policy, mailbox, instance):
        self.root, self.local, self.peers = root, local, peers
        self.policy, self.mailbox = policy, mailbox
        self.instance, self.tickets = instance, DeliveryTickets()
        self.token_peers = {}
        config = local["a2a"]
        for alias in config.get("chat_tokens", {}):
            peer = peers.get(alias.casefold())
            binding = config.get("chat_bindings", {}).get(alias)
            if not peer or not binding or chat_ref(binding) != chat_ref(peer):
                raise ValueError("原聊天发件凭据缺少正确的完整身份绑定")
            self.token_peers["chat:" + alias] = peer

    def source(self, principal):
        if principal in self.token_peers:
            peer = self.token_peers[principal]
        elif principal.startswith("device:"):
            candidates = [p for p in self.peers.values() if p["device_id"] == principal[7:]
                          and p.get("conversation_id") and p.get("transport") != "a2a"]
            if len(candidates) != 1:
                return None
            peer = candidates[0]
        else:
            return None
        ref = chat_ref(peer)
        if any(c["ref"] == ref and c["verified"] for c in self.policy.chat_list()):
            return peer
        return None

    def decision(self, row):
        if row and row["source_ref"] and row["target_ref"] and row["instance"] == self.instance:
            return self.policy.check(json.loads(row["source_ref"]), json.loads(row["target_ref"]))
        return {"contact": False, "busy": False, "reason": "任务没有可核验的来源和接收聊天"}

    def issue(self, task_id, target, text, message_id, action, busy, wait):
        token = self.tickets.issue(task_id, target, text, message_id, action, busy,
                                   ttl=min(660, max(30, wait + 30)))
        return {"ticket": token, "task_id": task_id, "action": action, "allow_busy": busy}

    def approve_external(self, task_id):
        """Called immediately before the SDK transmits to the registered, verified endpoint."""
        with self.mailbox.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM replies WHERE task_id=?", (task_id,)).fetchone()
            if not row or row["state"] != "waiting" or row["issued_at"] or not self.decision(row)["contact"]:
                return False
            db.execute("UPDATE replies SET issued_at=? WHERE task_id=?", (time.time(), task_id))
        return True

    def consume(self, data):
        # Consume first so a failed, revoked or modified attempt cannot be replayed.
        permit = self.tickets.consume(data["ticket"], data["target"], data["message_id"],
                                      data["text_hash"], data["action"], data["task_id"])
        with self.mailbox.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM replies WHERE task_id=?", (permit["task_id"],)).fetchone()
            if not row or not row["source_ref"] or not row["target_ref"]:
                raise BridgeError("不存在已绑定原聊天的任务")
            if permit["action"] == "notification":
                if row["mode"] != "legacy":
                    raise BridgeError("普通消息不接受旧结果通知")
                if (json.loads(row["source_ref"]) != chat_ref(data["target"])
                        or data["message_id"] != notice_id_for(data["target"], row["message_id"])
                        or row["notice_id"] is not None):
                    raise BridgeError("通知与已投递任务不匹配或已使用")
                db.execute("UPDATE replies SET notice_id=?,notice_state='dispatching' WHERE task_id=?",
                           (data["message_id"], row["task_id"]))
                return permit
            decision = self.decision(row)
            if (not decision["contact"] or row["state"] != "waiting" or row["issued_at"]
                    or json.loads(row["target_ref"]) != chat_ref(data["target"])):
                raise BridgeError("投递前权限已失效或投递许可已用")
            db.execute("UPDATE replies SET issued_at=? WHERE task_id=?", (time.time(), row["task_id"]))
            permit["allow_busy"] = permit["allow_busy"] and decision["busy"]
            return permit

    async def notify_local(self, job, text):
        from host_adapters import deliver_local
        row = self.mailbox.row(job["task_id"])
        if (not row or row.get("mode", "legacy") != "legacy" or not row["source_ref"] or not row["target_ref"]
                or row["owner"] != owner_of(job["origin"])
                or json.loads(row["source_ref"]) != chat_ref(job["origin"])
                or row["message_id"] != job["id"] or row["agent"].casefold() != job["peer"]["alias"].casefold()
                or job["notification_id"] != notice_id_for(job["origin"], job["id"])):
            raise BridgeError("回件通知与原发件任务不符")
        wait = self.local["a2a"].get("idle_wait_seconds", 300)
        permit = self.issue(row["task_id"], job["origin"], text, job["notification_id"], "notification", False, wait)
        try:
            receipt = await deliver_local(self.root, self.local, self.peers, job["origin"], text,
                                          job["notification_id"], wait_idle=wait, permit=permit)
        except BridgeError as error:
            if error.state == "not_sent":
                with self.mailbox.connect() as db:
                    db.execute("UPDATE replies SET notice_id=NULL,notice_state=NULL WHERE task_id=? "
                               "AND notice_id=? AND notice_state='dispatching'", (row["task_id"], job["notification_id"]))
            raise
        if receipt.get("state") == "accepted":
            with self.mailbox.connect() as db:
                db.execute("UPDATE replies SET notice_state='notified' WHERE task_id=? AND notice_id=?",
                           (row["task_id"], job["notification_id"]))
        return receipt

    def reported_notice(self, sender, data):
        state = data["state"]
        if state not in {"accepted", "notified", "not_sent", "uncertain"}:
            raise BridgeError("通知状态无效")
        with self.mailbox.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM replies WHERE task_id=?", (data["task_id"],)).fetchone()
            if not row or row["mode"] != "legacy" or not row["source_ref"] or not row["target_ref"] or row["state"] == "waiting":
                raise BridgeError("尚无可回报的该任务结果")
            source = json.loads(row["source_ref"])
            if (source != chat_ref(sender) or row["owner"] != owner_of(source)
                    or data["notification_id"] != notice_id_for(source, row["message_id"])):
                raise BridgeError("设备不能更新这个原聊天的通知")
            status = {"accepted": "notified", "notified": "notified", "not_sent": "failed", "uncertain": "uncertain"}[state]
            if row["notice_state"]:
                if row["notice_id"] == data["notification_id"] and row["notice_state"] == status:
                    return
                raise BridgeError("该通知回执已经确认")
            db.execute("UPDATE replies SET notice_state=?,notice_id=? WHERE task_id=?",
                       (status, data["notification_id"], row["task_id"]))
