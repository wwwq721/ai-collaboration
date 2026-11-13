"""Private delivery transport behind A2A. A device polls; only its chat can reply."""
import asyncio
import json
import time

from workbuddy import BridgeError
from message_delivery import encode_payload, normalize_receipt


class DeviceQueue:
    def __init__(self, mailbox):
        self.mailbox = mailbox
        with mailbox.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS device_deliveries (
                    delivery_id TEXT PRIMARY KEY, device_id TEXT NOT NULL,
                    peer TEXT NOT NULL, conversation_id TEXT NOT NULL,
                    reply_to TEXT NOT NULL, text TEXT,
                    state TEXT NOT NULL, scheduled_task_id TEXT,
                    created REAL NOT NULL, expires_at REAL NOT NULL,
                    finished REAL
                );
                CREATE INDEX IF NOT EXISTS device_pending
                    ON device_deliveries(device_id,state,created);
            """)
            columns = {row[1] for row in db.execute("PRAGMA table_info(device_deliveries)")}
            for name, definition in (("mode", "TEXT NOT NULL DEFAULT 'legacy'"), ("receipt", "TEXT")):
                if name not in columns:
                    db.execute("ALTER TABLE device_deliveries ADD COLUMN " + name + " " + definition)

    def enqueue(self, device_id, peer, conversation_id, text, delivery_id, reply_to, timeout, mode=None):
        record = self.mailbox.row(delivery_id)
        mode = mode or (record["mode"] if record else "legacy")
        if mode not in {"legacy", "message"} or record and mode != record["mode"]:
            raise BridgeError("投递模式与消息记录不符")
        if mode == "message":
            encoded = encode_payload(text)
            if len(text["message"].encode("utf8")) > self.mailbox.limits.max_text_bytes:
                raise BridgeError("业务正文超过本机限制")
            text = encoded
        elif not isinstance(text, str) or len(text.encode("utf8")) > self.mailbox.limits.max_text_bytes + 2048:
            raise BridgeError("包装后的来信超过上限，未投递")
        now = time.time()
        with self.mailbox.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM device_deliveries WHERE delivery_id=?", (delivery_id,)).fetchone():
                raise BridgeError("该任务已登记网络投递，未重复派发")
            pending = db.execute("SELECT count(*) FROM device_deliveries WHERE text IS NOT NULL").fetchone()[0]
            if pending >= self.mailbox.limits.max_active_tasks:
                raise BridgeError("网络待投递任务已满，未派发")
            db.execute(
                "INSERT INTO device_deliveries(delivery_id,device_id,peer,conversation_id,reply_to,text,state,created,expires_at,mode) "
                "VALUES(?,?,?,?,?,?,'queued',?,?,?)",
                (delivery_id, device_id, peer, conversation_id, reply_to, text, now, now + timeout, mode),
            )

    def next(self, device_id, now=None, *, can_offer=None, message_capable=False):
        now = time.time() if now is None else now
        with self.mailbox.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            # A restarted A2A service must never redeliver an abandoned task.
            db.execute(
                "UPDATE device_deliveries SET state='closed',text=NULL,finished=coalesce(finished,?) "
                "WHERE device_id=? AND state='queued' AND (expires_at<=? OR delivery_id NOT IN "
                "(SELECT task_id FROM replies WHERE state='waiting'))", (now, device_id, now),
            )
            while True:
                row = db.execute(
                "SELECT delivery_id,peer,conversation_id,reply_to,text,expires_at,mode FROM device_deliveries "
                "WHERE device_id=? AND state='queued' AND (mode='legacy' OR ?=1) ORDER BY created LIMIT 1",
                (device_id, int(message_capable)),
                ).fetchone()
                if row is None:
                    return {"state": "empty"}
                task = db.execute("SELECT * FROM replies WHERE task_id=?", (row["delivery_id"],)).fetchone()
                if task and (can_offer is None or task["issued_at"] or can_offer(task)):
                    result = {"state": "delivery", **dict(row)}
                    if can_offer is not None or row["mode"] == "message":
                        result["require_authorize"] = True
                    if row["mode"] == "message":
                        result["message"] = json.loads(result.pop("text"))
                        result["source_ref"] = json.loads(task["source_ref"])
                        result["target_ref"] = json.loads(task["target_ref"])
                    else:
                        result.pop("mode")
                    return result
                if task and task["mode"] == "message":
                    self.mailbox._record_delivery(db, task, {"state": "not_sent", "stage": "relay"})
                db.execute("UPDATE device_deliveries SET state='not_sent',text=NULL,finished=? WHERE delivery_id=?",
                           (now, row["delivery_id"]))

    def authorize(self, device_id, delivery_id, instance, decide):
        """Final permission decision, once, immediately before the receiver calls its host."""
        now = time.time()
        with self.mailbox.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT r.* FROM replies r JOIN device_deliveries d ON d.delivery_id=r.task_id "
                "WHERE r.task_id=? AND d.device_id=? AND r.instance=? AND r.state='waiting' "
                "AND d.state='queued' AND d.expires_at>? AND r.issued_at IS NULL",
                (delivery_id, device_id, instance, now)).fetchone()
            if not row:
                return {"allowed": False}
            allowed = decide(row)
            if not allowed["contact"]:
                db.execute("UPDATE device_deliveries SET state='not_sent',text=NULL,finished=? WHERE delivery_id=?",
                           (now, delivery_id))
                return {"allowed": False}
            db.execute("UPDATE replies SET issued_at=? WHERE task_id=?", (now, delivery_id))
            return {"allowed": True, "allow_busy": allowed["busy"]}

    def ack(self, device_id, delivery_id, state, scheduled_task_id=None, receipt=None):
        if state not in {"accepted", "uncertain", "not_sent"}:
            raise BridgeError("收件状态不受支持")
        if scheduled_task_id is not None and (not isinstance(scheduled_task_id, str) or len(scheduled_task_id) > 128):
            raise BridgeError("触发记录编号无效")
        with self.mailbox.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM device_deliveries WHERE delivery_id=? AND device_id=?",
                             (delivery_id, device_id)).fetchone()
            if not row:
                raise BridgeError("该设备没有这项投递")
            if row["mode"] == "message":
                task = db.execute("SELECT * FROM replies WHERE task_id=?", (delivery_id,)).fetchone()
                if not task or task["mode"] != "message" or not task["target_ref"]:
                    raise BridgeError("消息投递记录不存在")
                target = json.loads(task["target_ref"])
                if target[0] != device_id or target[2] != row["conversation_id"]:
                    raise BridgeError("该设备不能确认其他原聊天")
                checked = normalize_receipt(receipt)
                expected_state = "accepted" if checked["state"] in {"accepted", "queued"} else checked["state"]
                if state != expected_state:
                    raise BridgeError("回执与投递状态不符")
                if checked["state"] in {"accepted", "queued"}:
                    allowed = {"minis": {("queued", "minis_scheduler")},
                               "codex": {("accepted", "native_host")},
                               "workbuddy": {("accepted", "native_host"), ("queued", "host_queue")}}
                    if (checked["state"], checked["stage"]) not in allowed.get(target[1], set()):
                        raise BridgeError("该宿主不能证明这种收件状态")
                    if target[1] == "minis" and (not scheduled_task_id or checked.get("native_id") != scheduled_task_id):
                        raise BridgeError("Minis收件回执缺少相符的调度编号")
                saved = self.mailbox._record_delivery(db, task, checked)
                transport_state = "accepted" if saved["state"] in {"accepted", "queued"} else saved["state"]
                stored = {k: saved[k] for k in ("state", "stage", "native_id") if k in saved}
                db.execute("UPDATE device_deliveries SET state=?,text=NULL,scheduled_task_id=coalesce(?,scheduled_task_id),"
                           "receipt=?,finished=coalesce(finished,?) WHERE delivery_id=?",
                           (transport_state, scheduled_task_id, json.dumps(stored), time.time(), delivery_id))
                return saved
            if row["state"] == state:
                return
            if row["state"] == "closed":
                # Terminal tasks no longer need an ACK, but a lost ACK response
                # is safe for the same authenticated device to confirm again.
                return
            if row["state"] != "queued":
                raise BridgeError("已有收件状态，未覆盖")
            # No input body remains after the native dispatch attempt. Unknown
            # delivery is never automatically retried, but a real result may arrive.
            db.execute("UPDATE device_deliveries SET state=?,text=NULL,scheduled_task_id=?,finished=? "
                       "WHERE delivery_id=?", (state, scheduled_task_id, time.time(), delivery_id))

    def state(self, delivery_id):
        with self.mailbox.connect() as db:
            row = db.execute("SELECT state FROM device_deliveries WHERE delivery_id=?", (delivery_id,)).fetchone()
            return row["state"] if row else "closed"

    def sender_for_reply(self, device_id, address, instance):
        """The delivery row, not client-supplied sender fields, grants reply rights."""
        with self.mailbox.connect() as db:
            row = db.execute(
                "SELECT d.peer,d.conversation_id,d.state,d.expires_at,r.instance,r.state AS task_state,"
                "r.agent,r.conversation FROM device_deliveries d JOIN replies r ON r.task_id=d.delivery_id "
                "WHERE d.device_id=? AND d.reply_to=?", (device_id, address),
            ).fetchone()
            if (not row or row["instance"] != instance or row["state"] not in {"queued", "accepted", "uncertain", "closed"}
                    or row["task_state"] not in {"waiting", "done"}
                    or row["state"] == "closed" and row["task_state"] != "done"
                    or row["peer"].casefold() != row["agent"].casefold()
                    or row["conversation_id"] != row["conversation"]
                    or row["state"] == "queued" and row["expires_at"] <= time.time()):
                raise BridgeError("该设备没有当前实例的这项回件授权")
            return {"peer": row["peer"], "conversation_id": row["conversation_id"]}

    def close(self, delivery_id):
        with self.mailbox.connect() as db:
            db.execute("UPDATE device_deliveries SET state=CASE WHEN state='queued' THEN 'closed' ELSE state END,text=NULL,finished=coalesce(finished,?) "
                       "WHERE delivery_id=?", (time.time(), delivery_id))

    def cleanup(self, now=None):
        now = time.time() if now is None else now
        with self.mailbox.connect() as db:
            db.execute("UPDATE device_deliveries SET state='closed',text=NULL,finished=coalesce(finished,?) "
                       "WHERE text IS NOT NULL AND (expires_at<=? OR delivery_id NOT IN "
                       "(SELECT task_id FROM replies WHERE state='waiting'))", (now, now))
            db.execute("DELETE FROM device_deliveries WHERE finished<?", (now-self.mailbox.limits.metadata_ttl_seconds,))
            db.execute("DELETE FROM device_deliveries WHERE delivery_id IN (SELECT delivery_id FROM device_deliveries "
                       "WHERE finished IS NOT NULL ORDER BY finished DESC LIMIT -1 OFFSET ?)",
                       (self.mailbox.limits.max_metadata,))

    async def deliver(self, device_id, alias, conversation_id, text, task_id, timeout):
        row = self.mailbox.row(task_id)
        self.enqueue(device_id, alias, conversation_id, text, task_id, row["address"], timeout, mode=row["mode"])
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            reply = self.mailbox.row(task_id)
            if row["mode"] == "message" and reply and reply["receipt_state"] in {"accepted", "queued", "uncertain", "not_sent"}:
                receipt = self.mailbox._receipt_result(reply)
                return {"state": "accepted" if receipt["state"] in {"accepted", "queued"} else receipt["state"],
                        "receipt": {k: receipt[k] for k in ("state", "stage", "native_id") if k in receipt}}
            if reply and reply["result"] is not None:
                return {"state": "accepted"}
            state = self.state(task_id)
            if state in {"accepted", "uncertain"}:
                return {"state": state}
            if state in {"not_sent", "closed"}:
                if row["mode"] == "message":
                    value = {"state": "not_sent" if state == "not_sent" or not reply["issued_at"] else "uncertain",
                             "stage": "relay"}
                    saved = self.mailbox.record_delivery(task_id, value)
                    return {"state": saved["state"], "receipt": {k: saved[k] for k in ("state", "stage", "native_id") if k in saved}}
                raise BridgeError("手机未接纳此任务，请核对原聊天和接收器；未重新投递")
            await asyncio.sleep(.25)
        if row["mode"] == "message":
            receipt = self.mailbox.record_delivery(task_id, {"state": "uncertain", "stage": "relay"})
            return {"state": "uncertain", "receipt": {k: receipt[k] for k in ("state", "stage", "native_id") if k in receipt}}
        raise BridgeError("手机未及时确认收件，请核对接收器；未重新投递", "uncertain")
