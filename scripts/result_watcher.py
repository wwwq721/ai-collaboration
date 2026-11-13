"""Bounded task-result notifications, hosted by the existing device process.

No prompt bodies or credentials are persisted.  A notification is durably
claimed before invoking a native host; an interrupted claim is never replayed.
"""
import asyncio
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
import json
from pathlib import Path
import sqlite3
import threading
import time
from urllib.parse import quote
import uuid

from a2a_state import Limits
from workbuddy import BridgeError


FINAL = {"completed", "input_required", "auth_required", "failed", "canceled", "rejected", "expired"}


class _PeerIdentityError(BridgeError):
    """A local pairing changed, rather than a temporary read failing."""


def _chat_ref(value):
    value = asdict(value) if is_dataclass(value) else value
    if not isinstance(value, dict):
        raise BridgeError("发件原聊天身份不完整")
    result = {key: value.get(key) for key in ("device_id", "software", "conversation_id")}
    if any(not isinstance(v, str) or not v or len(v) > 512 for v in result.values()):
        raise BridgeError("发件原聊天身份不完整")
    return result


def _peer_ref(value):
    if not isinstance(value, dict):
        raise BridgeError("回件伙伴身份未登记")
    result = {key: value.get(key, "") for key in
              ("alias", "device_id", "software", "host_software", "conversation_id", "peer_id")}
    if any(not isinstance(result[k], str) or not result[k] or len(result[k]) > 512
           for k in ("alias", "device_id", "software")):
        raise BridgeError("回件伙伴身份未登记")
    if any(not isinstance(v, str) or len(v) > 512 for v in result.values()):
        raise BridgeError("回件伙伴身份无效")
    return result


def _message_target(peer):
    """The registered host identity, independent of its SDK transport wrapper."""
    from access_policy import chat_ref
    return chat_ref({**peer, "software": peer.get("host_software") or peer["software"]})


class PendingResults:
    def __init__(self, root, limits=None, clock=time.time):
        self.root = Path(root)
        self.limits = limits or Limits.from_root(root)
        self.clock = clock
        self.path = self.root / "runtime" / "pending-results.db"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        config_path = self.root / "local.json"
        config = json.loads(config_path.read_text(encoding="utf-8-sig")) if config_path.exists() else {}
        a2a = config.get("a2a", {})
        self.timeout = max(60, float(a2a.get("task_timeout", 900))
                           + float(a2a.get("idle_wait_seconds", 300)) + 120)
        self.receipt_timeout = max(30, min(3600, float(a2a.get("delivery_timeout", 300)) + 30))
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS pending_results (
                    id TEXT PRIMARY KEY, origin TEXT NOT NULL, peer TEXT NOT NULL,
                    state TEXT NOT NULL, task_id TEXT, context_id TEXT,
                    notification_id TEXT NOT NULL UNIQUE, result TEXT,
                    created REAL NOT NULL, updated REAL NOT NULL,
                    deadline REAL NOT NULL, next_poll REAL NOT NULL,
                    result_at REAL, notified_at REAL, receipt TEXT,
                    cleanup_done INTEGER NOT NULL DEFAULT 0,
                    cleanup_after REAL NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS pending_results_due
                    ON pending_results(state,next_poll);
            """)
            columns = {row[1] for row in db.execute("PRAGMA table_info(pending_results)")}
            if "cleanup_done" not in columns:
                db.execute("ALTER TABLE pending_results ADD COLUMN cleanup_done INTEGER NOT NULL DEFAULT 0")
            if "cleanup_after" not in columns:
                db.execute("ALTER TABLE pending_results ADD COLUMN cleanup_after REAL NOT NULL DEFAULT 0")
            if "notification_reported" not in columns:
                db.execute("ALTER TABLE pending_results ADD COLUMN notification_reported INTEGER NOT NULL DEFAULT 0")
            for name, declaration in (("mode", "TEXT NOT NULL DEFAULT 'legacy'"),
                                      ("request_hash", "TEXT"), ("summary", "TEXT")):
                if name not in columns:
                    db.execute("ALTER TABLE pending_results ADD COLUMN " + name + " " + declaration)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA secure_delete=ON")
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _decode(row):
        if row is None:
            return None
        result = dict(row)
        for key in ("origin", "peer", "result", "receipt"):
            result[key] = json.loads(result[key]) if result[key] else None
        return result

    def get(self, job_id):
        with self.connect() as db:
            return self._decode(db.execute("SELECT * FROM pending_results WHERE id=?", (job_id,)).fetchone())

    def prepare(self, origin, peer, message_id):
        origin, peer = _chat_ref(origin), _peer_ref(peer)
        if not isinstance(message_id, str) or not 0 < len(message_id) <= 128:
            raise BridgeError("任务消息编号无效")
        self.cleanup()
        now = self.clock()
        origin_json = json.dumps(origin, ensure_ascii=True, sort_keys=True)
        peer_json = json.dumps(peer, ensure_ascii=True, sort_keys=True)
        notice = str(uuid.uuid5(uuid.NAMESPACE_URL, "ai-result:" + origin_json + ":" + message_id))
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT origin,peer FROM pending_results WHERE id=?", (message_id,)).fetchone()
            if old:
                if old["origin"] != origin_json or old["peer"] != peer_json:
                    raise BridgeError("消息编号已属于其他身份")
                return message_id
            active = db.execute("SELECT count(*) FROM pending_results WHERE state IN ('prepared','waiting','ready','dispatching')").fetchone()[0]
            if active >= self.limits.max_active_tasks:
                raise BridgeError("待回件任务已满，未发送新任务")
            if db.execute("SELECT count(*) FROM pending_results").fetchone()[0] >= self.limits.max_metadata:
                raise BridgeError("回件状态已达保留上限，未发送新任务")
            db.execute("INSERT INTO pending_results(id,origin,peer,state,notification_id,created,updated,deadline,next_poll) "
                       "VALUES(?,?,?,'prepared',?,?,?,?,?)",
                       (message_id, origin_json, peer_json, notice, now, now, now + self.timeout, now))
        return message_id

    def prepare_message(self, origin, peer, message_id, text):
        from message_delivery import request_hash, message_summary
        origin, peer = _chat_ref(origin), _peer_ref(peer)
        target = dict(zip(("device_id", "software", "conversation_id"), _message_target(peer)))
        if not isinstance(message_id, str) or not 0 < len(message_id) <= 128:
            raise BridgeError("消息编号无效")
        digest = request_hash(target, text)
        origin_json = json.dumps(origin, ensure_ascii=True, sort_keys=True)
        peer_json = json.dumps(peer, ensure_ascii=True, sort_keys=True)
        notice = str(uuid.uuid5(uuid.NAMESPACE_URL, "ai-receipt:" + origin_json + ":" + message_id))
        self.cleanup()
        now = self.clock()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT mode,origin,peer,request_hash FROM pending_results WHERE id=?", (message_id,)).fetchone()
            if old:
                if (old["mode"] != "message" or old["origin"] != origin_json
                        or _message_target(json.loads(old["peer"])) != _message_target(peer)
                        or old["request_hash"] != digest):
                    raise BridgeError("消息编号与原发送不符")
                return message_id
            active = db.execute("SELECT count(*) FROM pending_results WHERE state IN ('prepared','waiting','ready','dispatching')").fetchone()[0]
            if active >= self.limits.max_active_tasks:
                raise BridgeError("等待确认的消息已满")
            if db.execute("SELECT count(*) FROM pending_results").fetchone()[0] >= self.limits.max_metadata:
                raise BridgeError("发送记录已达保留上限")
            db.execute("INSERT INTO pending_results(id,origin,peer,state,notification_id,created,updated,deadline,next_poll,mode,request_hash,summary) "
                       "VALUES(?,?,?,'prepared',?,?,?,?,?,'message',?,?)",
                       (message_id, origin_json, peer_json, notice, now, now, now + self.receipt_timeout, now,
                        digest, message_summary(text)[:24]))
        return message_id

    def latest_message(self, origin, peer):
        self.cleanup()
        origin_json = json.dumps(_chat_ref(origin), ensure_ascii=True, sort_keys=True)
        with self.connect() as db:
            rows = db.execute("SELECT * FROM pending_results WHERE mode='message' AND origin=? ORDER BY created DESC,rowid DESC",
                              (origin_json,)).fetchall()
            for row in rows:
                decoded = self._decode(row)
                if _message_target(decoded["peer"]) == _message_target(peer):
                    return decoded
        return None

    def record_receipt(self, job_id, receipt):
        from message_delivery import normalize_receipt
        safe = normalize_receipt(receipt)
        now = self.clock()
        state = "waiting" if safe["state"] == "waiting" else "receipt_done"
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT receipt FROM pending_results WHERE id=? AND mode='message'", (job_id,)).fetchone()
            if row is None:
                raise BridgeError("发送记录不存在")
            previous = json.loads(row[0]) if row[0] else None
            if previous == safe:
                return previous
            if previous and (previous["state"] in {"accepted", "not_sent"}
                             or previous["state"] == "queued" and safe["state"] != "accepted"):
                return previous
            db.execute("UPDATE pending_results SET state=?,receipt=?,result=NULL,result_at=coalesce(result_at,?),updated=?,cleanup_done=1 "
                       "WHERE id=? AND mode='message'",
                       (state, json.dumps(safe), now if state == "receipt_done" else None, now, job_id))
        return safe

    def receipt_status(self, job_id):
        from message_delivery import failure_text
        job = self.get(job_id)
        if not job or job["mode"] != "message":
            raise BridgeError("发送记录不存在")
        receipt = job["receipt"] or {"state": "waiting", "stage": "relay"}
        result = {"state": receipt["state"], "to": job["peer"]["alias"], "stage": receipt["stage"]}
        if receipt["state"] in {"uncertain", "not_sent"}:
            return {"state": receipt["state"],
                    "error": failure_text(job["peer"]["alias"], job["summary"] or "", receipt["state"])}
        return result

    def bind(self, job_id, task_id, context_id=""):
        if not isinstance(task_id, str) or not 0 < len(task_id) <= 512:
            raise BridgeError("未取得有效的远端任务编号")
        now = self.clock()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT state,task_id FROM pending_results WHERE id=?", (job_id,)).fetchone()
            if old is None:
                raise BridgeError("发件任务未预登记")
            if old["task_id"] and old["task_id"] != task_id:
                raise BridgeError("远端任务编号与原记录冲突")
            if old["state"] == "prepared":
                db.execute("UPDATE pending_results SET state='waiting',task_id=?,context_id=?,updated=?,next_poll=? WHERE id=?",
                           (task_id, context_id, now, now, job_id))

    def _finish(self, job_id, state, receipt=None):
        safe = None
        if isinstance(receipt, dict):
            safe = {k: receipt[k][:512] if isinstance(receipt[k], str) else receipt[k]
                    for k in ("state", "route", "turn_id", "scheduled_task_id", "delivery_id")
                    if k in receipt and isinstance(receipt[k], (str, int, bool, type(None)))}
        allowed = {"inline": ("prepared", "waiting"), "not_sent": ("prepared", "waiting"), "uncertain": ("prepared", "waiting", "dispatching"),
                   "notified": ("dispatching",)}[state]
        with self.connect() as db:
            cursor = db.execute("UPDATE pending_results SET state=?,result=NULL,receipt=?,updated=?,notified_at=?,cleanup_done=? WHERE id=? "
                                "AND state IN (" + ",".join("?" for _ in allowed) + ")",
                                (state, json.dumps(safe) if safe is not None else None,
                                 self.clock(), self.clock(), 0 if safe and safe.get("scheduled_task_id") else 1,
                                 job_id, *allowed))
            return cursor.rowcount == 1

    def mark_uncertain(self, job_id):
        return self._finish(job_id, "uncertain")

    def mark_not_sent(self, job_id):
        return self._finish(job_id, "not_sent")

    def mark_inline(self, job_id):
        return self._finish(job_id, "inline")

    def record_notification(self, job_id, receipt):
        return self._finish(job_id, "notified", receipt)

    def notifications_to_report(self):
        with self.connect() as db:
            return [self._decode(row) for row in db.execute(
                "SELECT * FROM pending_results WHERE mode='legacy' AND state IN ('notified','uncertain') "
                "AND notification_reported=0 AND task_id IS NOT NULL ORDER BY updated LIMIT ?",
                (self.limits.max_active_tasks,))]

    def record_report(self, job_id):
        with self.connect() as db:
            db.execute("UPDATE pending_results SET notification_reported=1 WHERE id=?", (job_id,))

    def notifications_to_cleanup(self):
        with self.connect() as db:
            return [self._decode(row) for row in db.execute(
                "SELECT * FROM pending_results WHERE state='notified' AND cleanup_done=0 AND receipt IS NOT NULL "
                "AND cleanup_after<=? ORDER BY cleanup_after,notified_at LIMIT ?",
                (self.clock(), self.limits.max_active_tasks))]

    def defer_cleanup(self, job_id):
        with self.connect() as db:
            db.execute("UPDATE pending_results SET cleanup_after=? WHERE id=?",
                       (self.clock() + self.limits.cleanup_interval_seconds, job_id))

    def record_cleanup(self, job_id):
        with self.connect() as db:
            db.execute("UPDATE pending_results SET cleanup_done=1 WHERE id=? AND state='notified'", (job_id,))

    def result(self, job_id, result):
        state = result.get("state")
        if state not in FINAL:
            raise BridgeError("回件结果状态尚未明确")
        safe = {"state": state}
        size = 0
        for key in ("reply", "error"):
            if isinstance(result.get(key), str):
                size += len(result[key].encode("utf8"))
                safe[key] = result[key]
        if size > self.limits.max_text_bytes:
            safe = {"state": "failed", "error": "远端回件超过正文上限，请核对原任务"}
        now = self.clock()
        with self.connect() as db:
            db.execute("UPDATE pending_results SET state='ready',result=?,result_at=?,updated=?,next_poll=? "
                       "WHERE id=? AND state IN ('prepared','waiting')",
                       (json.dumps(safe, ensure_ascii=True), now, now, now, job_id))

    def due(self, interval=3):
        now = self.clock()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            rows = list(db.execute("SELECT * FROM pending_results WHERE state IN ('prepared','waiting','ready') "
                                   "AND next_poll<=? ORDER BY created LIMIT ?", (now, self.limits.max_active_tasks)))
            if rows:
                db.executemany("UPDATE pending_results SET next_poll=? WHERE id=?",
                               [(now + interval, row["id"]) for row in rows])
            return [self._decode(row) for row in rows]

    def claim_notification(self, job_id):
        with self.connect() as db:
            cursor = db.execute("UPDATE pending_results SET state='dispatching',updated=? WHERE id=? AND state='ready'",
                                (self.clock(), job_id))
            return cursor.rowcount == 1

    def retry_notification(self, job_id, delay=5):
        with self.connect() as db:
            db.execute("UPDATE pending_results SET state='ready',next_poll=?,updated=? WHERE id=? AND state='dispatching'",
                       (self.clock() + delay, self.clock(), job_id))

    def cleanup(self):
        now = self.clock()
        with self.connect() as db:
            db.execute("UPDATE pending_results SET summary=NULL WHERE mode='message' AND state='receipt_done' "
                       "AND result_at<?", (now-self.limits.result_ttl_seconds,))
            db.execute("DELETE FROM pending_results WHERE mode='message' AND state='receipt_done' AND updated<?",
                       (now-self.limits.metadata_ttl_seconds,))
            # An unconfirmed native dispatch is never returned to ready.
            db.execute("DELETE FROM pending_results WHERE (state IN ('inline','not_sent','uncertain','dispatching') "
                       "OR (state='notified' AND cleanup_done=1)) AND updated<?",
                       (now - self.limits.metadata_ttl_seconds,))
            # Keep the fact of an undelivered result, but not its expired body.
            stale = json.dumps({"state": "expired", "error": "结果缓存已到期，请核对远端原聊天；未重新派题"})
            db.execute("UPDATE pending_results SET result=?,result_at=?,updated=? "
                       "WHERE state='ready' AND result_at<?",
                       (stale, now, now, now - self.limits.result_ttl_seconds))
            db.execute("DELETE FROM pending_results WHERE id IN (SELECT id FROM pending_results "
                       "WHERE state IN ('inline','not_sent','uncertain','receipt_done') OR (state='notified' AND cleanup_done=1) "
                       "ORDER BY updated DESC LIMIT -1 OFFSET ?)",
                       (self.limits.max_metadata,))


def _peers_list(peers):
    if isinstance(peers, dict) and isinstance(peers.get("peers"), dict):
        peers = peers["peers"]
    if isinstance(peers, dict):
        return [{**p, "alias": p.get("alias", name)} for name, p in peers.items() if isinstance(p, dict)]
    return list(peers)


class ResultWatcher:
    def __init__(self, root, local, peers, query=None, deliver=None, clock=time.time, managed_deliver=None):
        self.root, self.local, self.peers = Path(root), local, _peers_list(peers)
        self.pending = PendingResults(root, clock=clock)
        self.query = query or self._query
        self.deliver = deliver or self._deliver
        self.managed_deliver = managed_deliver
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(4)
        self._last_cleanup = None

    def _peer(self, saved):
        candidates = [p for p in self.peers if p.get("alias", "").casefold() == saved["alias"].casefold()
                      and p.get("device_id") == saved["device_id"] and p.get("enabled", True) is True
                      and p.get("host_software", p.get("software")) == (saved.get("host_software") or saved["software"])
                      and all(not saved.get(k) or p.get(k) == saved[k] for k in ("conversation_id", "peer_id"))]
        if len(candidates) != 1:
            raise _PeerIdentityError("原任务伙伴身份已变更，请核对配对")
        peer = candidates[0]
        from delivery_ticket import managed
        if managed(self.local):
            from collaboration_client import central_peer
            return central_peer(self.local, peer)
        if peer.get("transport") == "a2a" and peer.get("software") != "a2a":
            peer = {**peer, "host_software": peer["software"], "software": "a2a"}
        if peer.get("software") != saved["software"]:
            if saved["software"] != "a2a" or peer.get("device_id") == self.local.get("device_id"):
                raise _PeerIdentityError("原任务软件身份已变更")
        if peer.get("software") != "a2a":
            config = self.local.get("a2a", {})
            if peer["device_id"] not in config.get("device_tokens", {}):
                raise _PeerIdentityError("原任务设备已取消配对")
            peer = {**peer, "host_software": peer["software"], "software": "a2a", "local_a2a": True,
                    "url": "http://127.0.0.1:" + str(config.get("port", 8788))
                           + "/agents/" + quote(peer["alias"], safe="")}
        return peer

    async def _query(self, job):
        from a2a_client import exchange
        peer = self._peer(job["peer"])
        credentials = self.local.get("a2a_credentials", {}).get(peer["alias"], {})
        from delivery_ticket import managed
        if peer.get("local_a2a") and not managed(self.local):
            credentials = {"token": self.local.get("a2a", {}).get("tokens", {}).get("local", "")}
        if managed(self.local):
            from collaboration_client import source_token
            credentials = {"token": source_token(self.local, self.peers, job["origin"])}
        proxy = None
        if self.local.get("network", {}).get("mode") != "paired_ip":
            from device_receiver import proxy_url
            proxy = proxy_url(self.local)
        return await exchange({
            "root": str(self.root), "local": self.local, "peer": peer,
            "mode": "task-status", "task_id": job["task_id"], "source": job["origin"]["conversation_id"],
            "token": credentials.get("token", ""), "timeout": 0,
            "proxy_url": proxy,
            "origin": job["origin"],
        })

    async def _query_message(self, job):
        from a2a_client import query_receipt
        from collaboration_client import central_peer, source_token
        candidates = [p for p in self.peers if p.get("conversation_id") and _message_target(p) == _message_target(job["peer"])
                      and p.get("enabled", True) is True]
        if len(candidates) != 1:
            raise _PeerIdentityError("目标原聊天登记已变化")
        peer = central_peer(self.local, candidates[0])
        token = source_token(self.local, self.peers, job["origin"])
        return await query_receipt({"local": self.local, "peer": peer, "token": token}, job)

    async def _deliver(self, origin, text, notification_id):
        from delivery_ticket import managed_pc
        if managed_pc(self.local):
            raise BridgeError("管理模式回件需要服务核验原任务绑定")
        from host_adapters import ChatRef, deliver_local
        return await deliver_local(self.root, self.local, self.peers, ChatRef(**origin), text, notification_id,
                                   source="A2A结果", wait_idle=300)

    async def _one(self, job):
        async with self._semaphore:
            now = self.pending.clock()
            if job.get("mode") == "message":
                try:
                    receipt = await self._query_message(job)
                except asyncio.CancelledError:
                    raise
                except _PeerIdentityError:
                    receipt = {"state": "uncertain", "stage": "relay"}
                except Exception:
                    if now < job["deadline"]:
                        return
                    receipt = {"state": "uncertain", "stage": "relay"}
                if receipt["state"] == "waiting" and now >= job["deadline"]:
                    receipt = {"state": "uncertain", "stage": "relay"}
                self.pending.record_receipt(job["id"], receipt)
                return
            if job["state"] in {"prepared", "waiting"}:
                if now >= job["deadline"]:
                    self.pending.result(job["id"], {"state": "expired", "error": "等待任务结果已超时，请核对原任务；未自动重发"})
                elif not job["task_id"]:
                    return
                else:
                    try:
                        result = await self.query(job)
                    except asyncio.CancelledError:
                        raise
                    except _PeerIdentityError as error:
                        self.pending.result(job["id"], {"state": "failed", "error": str(error)[:350]})
                    except Exception:
                        return  # A read can be retried; no prompt is sent here.
                    else:
                        if not isinstance(result, dict) or result.get("state") not in FINAL:
                            return
                        self.pending.result(job["id"], result)
            job = self.pending.get(job["id"])
            if job is None or job["state"] != "ready" or not self.pending.claim_notification(job["id"]):
                return
            job = self.pending.get(job["id"])
            result = job["result"]
            state = result["state"]
            text = ("【协作任务回件，来自 " + job["peer"]["alias"] + "】\n"
                    + "任务：" + (job["task_id"] or job["id"]) + "；状态：" + state + "\n"
                    + (result.get("reply") or result.get("error") or "远端任务状态已更新")
                    + "\n通知号：" + job["notification_id"] + "。这是结果通知，无需回复确认。")
            try:
                from delivery_ticket import managed_pc
                if managed_pc(self.local):
                    if self.managed_deliver is None:
                        raise BridgeError("管理模式回件缺少服务原任务核验")
                    receipt = await self.managed_deliver(job, text)
                else:
                    receipt = await self.deliver(job["origin"], text, job["notification_id"])
                if not isinstance(receipt, dict) or receipt.get("state") != "accepted":
                    if isinstance(receipt, dict) and receipt.get("state") == "not_sent":
                        self.pending.retry_notification(job["id"])
                    else:
                        self.pending.mark_uncertain(job["id"])
                    return
                self.pending.record_notification(job["id"], receipt)
            except asyncio.CancelledError:
                # Leave durable dispatching: cancellation may follow native send.
                raise
            except BridgeError as error:
                if error.state == "not_sent":
                    self.pending.retry_notification(job["id"])
                else:
                    self.pending.mark_uncertain(job["id"])
            except Exception:
                self.pending.mark_uncertain(job["id"])

    async def tick(self):
        async with self._lock:
            self.pending.cleanup()
            await self._cleanup_notifications()
            jobs = self.pending.due()
            await asyncio.gather(*(self._one(job) for job in jobs))
            await self._report_notifications()
            return len(jobs)

    async def _report_notifications(self):
        from delivery_ticket import managed
        if self.local.get("platform") != "minis" or not managed(self.local):
            return
        from device_receiver import report_notification
        for job in self.pending.notifications_to_report():
            try:
                state = "accepted" if job["state"] == "notified" else "uncertain"
                await asyncio.to_thread(report_notification, self.local, job["task_id"], job["notification_id"], state,
                                        origin=job["origin"], peers=self.peers)
            except Exception:
                continue  # Retry metadata only; never re-enter native delivery.
            self.pending.record_report(job["id"])

    async def _cleanup_notifications(self):
        if self.local.get("platform") != "minis":
            return
        now = self.pending.clock()
        if self._last_cleanup is not None and now - self._last_cleanup < self.pending.limits.cleanup_interval_seconds:
            return
        self._last_cleanup = now
        import minis
        for job in self.pending.notifications_to_cleanup():
            self.pending.defer_cleanup(job["id"])
            task_id = job["receipt"].get("scheduled_task_id")
            if job["origin"]["software"] != "minis" or not task_id:
                self.pending.record_cleanup(job["id"])
                continue
            # cleanup_task reads native completion and ownership markers. It
            # neither starts a model nor deletes another producer's schedule.
            result = await asyncio.to_thread(minis.cleanup_task, task_id, job["notification_id"],
                                             job["origin"]["conversation_id"], expired=now >= job["deadline"])
            if result.get("state") in {"removed", "missing"}:
                self.pending.record_cleanup(job["id"])

    async def run(self, stop=None, interval=2):
        while stop is None or not stop():
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass  # Operational errors contain no message text in logs.
            await asyncio.sleep(interval)


@contextmanager
def background_results(root, local, peers):
    """Run the same watcher beside the synchronous mobile long-poll receiver."""
    stop, started = threading.Event(), threading.Event()
    holder = {}

    def worker():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        holder["loop"] = loop
        try:
            watcher = ResultWatcher(root, local, peers)
            task = loop.create_task(watcher.run(stop=stop.is_set))
            holder["task"] = task
            started.set()
            try:
                loop.run_until_complete(task)
            except asyncio.CancelledError:
                pass
            remaining = asyncio.all_tasks(loop)
            for other in remaining:
                other.cancel()
            if remaining:
                loop.run_until_complete(asyncio.gather(*remaining, return_exceptions=True))
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception as error:
            holder["error"] = type(error).__name__
            started.set()
        finally:
            loop.close()

    thread = threading.Thread(target=worker, name="ai-result-watcher", daemon=True)
    thread.start()
    if not started.wait(5) or "error" in holder:
        stop.set()
        thread.join(timeout=1)
        raise BridgeError("后台回件组件未能启动，请核对本机状态")
    try:
        yield holder
    finally:
        stop.set()
        loop, task = holder.get("loop"), holder.get("task")
        if loop is not None and task is not None and not loop.is_closed():
            try:
                loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                pass
        thread.join(timeout=5)
