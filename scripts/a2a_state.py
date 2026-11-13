"""Local reply addresses and client continuity. No network or SDK dependency."""
from dataclasses import dataclass
import hashlib
import json
import secrets
import re
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from workbuddy import BridgeError
from message_delivery import normalize_receipt


@dataclass(frozen=True)
class Limits:
    result_ttl_seconds: int = 600
    metadata_ttl_seconds: int = 86400
    max_completed_tasks: int = 200
    max_metadata: int = 5000
    max_active_tasks: int = 16
    max_text_bytes: int = 65536
    cleanup_interval_seconds: int = 60

    def __post_init__(self):
        if any(isinstance(v, bool) or not isinstance(v, (int, float)) or v <= 0
               for v in self.__dict__.values()):
            raise BridgeError("A2A 清理期限和数量上限须为正数")

    @classmethod
    def from_root(cls, root):
        path = Path(root) / "local.json"
        config = json.loads(path.read_text(encoding="utf-8-sig")).get("a2a", {}) if path.exists() else {}
        return cls(**{k: config[k] for k in cls.__dataclass_fields__ if k in config})


class Mailbox:
    def __init__(self, root, limits=None):
        self.limits = limits or Limits.from_root(root)
        self.path = Path(root) / "runtime/a2a-mailbox.db"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS replies (
                    task_id TEXT PRIMARY KEY, address TEXT UNIQUE NOT NULL,
                    agent TEXT NOT NULL, conversation TEXT NOT NULL,
                    owner TEXT NOT NULL, message_id TEXT NOT NULL,
                    instance TEXT NOT NULL, state TEXT NOT NULL,
                    result TEXT, created REAL NOT NULL,
                    UNIQUE(agent, owner, message_id)
                );
                CREATE TABLE IF NOT EXISTS outgoing (
                    source TEXT NOT NULL, peer TEXT NOT NULL,
                    context_id TEXT NOT NULL, task_id TEXT NOT NULL,
                    PRIMARY KEY(source, peer)
                );
            """)
            columns = {r[1] for r in db.execute("PRAGMA table_info(replies)")}
            for name, kind in [("finished", "REAL"), ("result_hash", "TEXT"),
                               ("source_ref", "TEXT"), ("target_ref", "TEXT"), ("issued_at", "REAL"),
                               ("notice_id", "TEXT"), ("notice_state", "TEXT"),
                               ("mode", "TEXT NOT NULL DEFAULT 'legacy'"),
                               ("request_hash", "TEXT"), ("receipt_state", "TEXT"),
                               ("receipt_stage", "TEXT"), ("native_id", "TEXT"),
                               ("external_result", "TEXT"), ("external_result_at", "REAL"),
                               ("external_result_state", "TEXT")]:
                if name not in columns:
                    db.execute("ALTER TABLE replies ADD COLUMN " + name + " " + kind)
            columns = {r[1] for r in db.execute("PRAGMA table_info(outgoing)")}
            if "updated" not in columns:
                db.execute("ALTER TABLE outgoing ADD COLUMN updated REAL NOT NULL DEFAULT 0")
            db.execute("CREATE INDEX IF NOT EXISTS reply_state ON replies(state,finished)")
            db.execute("CREATE UNIQUE INDEX IF NOT EXISTS message_sender_key "
                       "ON replies(owner,message_id) WHERE mode='message'")

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

    def claim(self, task_id, agent, conversation, owner, message_id, instance, *, source_ref=None, target_ref=None):
        address = "A2A-" + secrets.token_hex(6)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute(
                "SELECT task_id FROM replies WHERE agent=? AND owner=? AND message_id=?",
                (agent, owner, message_id),
            ).fetchone()
            if old:
                raise BridgeError("消息已对应任务 " + old["task_id"] + "，请查询原任务，未重复投递")
            if db.execute("SELECT count(*) FROM replies WHERE state='waiting'").fetchone()[0] >= self.limits.max_active_tasks:
                raise BridgeError("当前待办已达上限，请等已有任务结束；未投递新任务")
            try:
                db.execute(
                    "INSERT INTO replies(task_id,address,agent,conversation,owner,message_id,instance,state,result,created,source_ref,target_ref) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (task_id, address, agent, conversation, owner, message_id,
                     instance, "waiting", None, time.time(),
                     json.dumps(source_ref) if source_ref is not None else None,
                     json.dumps(target_ref) if target_ref is not None else None),
                )
            except sqlite3.IntegrityError:
                old = db.execute(
                    "SELECT task_id FROM replies WHERE agent=? AND owner=? AND message_id=?",
                    (agent, owner, message_id),
                ).fetchone()
                if old:
                    raise BridgeError("消息已对应任务 " + old["task_id"] + "，请查询原任务，未重复投递")
                raise
        return address

    def claim_message(self, task_id, agent, conversation, owner, message_id, instance,
                      source_ref, target_ref, request_hash):
        """Reserve a native handoff before issuing it; duplicates only read its receipt."""
        from access_policy import chat_ref
        source_ref, target_ref = chat_ref(source_ref), chat_ref(target_ref)
        if (not all(isinstance(v, str) and 0 < len(v) <= 512
                    for v in (task_id, agent, conversation, owner, message_id, instance))
                or not isinstance(request_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", request_hash)):
            raise BridgeError("消息登记字段无效")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT * FROM replies WHERE owner=? AND message_id=? AND mode='message'",
                             (owner, message_id)).fetchone()
            if old:
                if (old["request_hash"] != request_hash
                        or json.loads(old["source_ref"]) != source_ref
                        or json.loads(old["target_ref"]) != target_ref):
                    raise BridgeError("消息编号与已登记的目标或正文冲突")
                return dict(old), False
            if db.execute("SELECT 1 FROM replies WHERE task_id=? OR (agent=? AND owner=? AND message_id=?)",
                          (task_id, agent, owner, message_id)).fetchone():
                raise BridgeError("该编号已用于其他投递记录")
            if db.execute("SELECT count(*) FROM replies WHERE state='waiting'").fetchone()[0] >= self.limits.max_active_tasks:
                raise BridgeError("当前待投递消息已达上限")
            db.execute(
                "INSERT INTO replies(task_id,address,agent,conversation,owner,message_id,instance,state,result,"
                "created,source_ref,target_ref,mode,request_hash,receipt_state,receipt_stage) "
                "VALUES(?,?,?,?,?,?,?,'waiting',NULL,?,?,?,'message',?,'waiting','relay')",
                (task_id, "MSG-" + task_id, agent, conversation, owner, message_id, instance, time.time(),
                 json.dumps(source_ref), json.dumps(target_ref), request_hash))
            return dict(db.execute("SELECT * FROM replies WHERE task_id=?", (task_id,)).fetchone()), True

    @staticmethod
    def _receipt_result(row):
        result = {"message_id": row["message_id"], "target_ref": json.loads(row["target_ref"]),
                  "state": row["receipt_state"] or "waiting", "stage": row["receipt_stage"] or "relay"}
        if row["native_id"]:
            result["native_id"] = row["native_id"]
        return result

    def receipt_for(self, owner, message_id):
        with self.connect() as db:
            row = db.execute("SELECT * FROM replies WHERE owner=? AND message_id=? AND mode='message'",
                             (owner, message_id)).fetchone()
            return self._receipt_result(row) if row else None

    def _record_delivery(self, db, row, receipt):
        """Update using the caller's transaction, including an authenticated device ACK."""
        if not row or row["mode"] != "message":
            raise BridgeError("消息收件记录不存在")
        receipt = normalize_receipt(receipt)
        current = row["receipt_state"] or "waiting"
        old = {"state": current, "stage": row["receipt_stage"] or "relay"}
        if row["native_id"]:
            old["native_id"] = row["native_id"]
        if current in {"accepted", "queued", "not_sent"}:
            if receipt["state"] in {"waiting", "uncertain"}:
                return self._receipt_result(row)
            if old != receipt:
                raise BridgeError("已确认的收件状态不能被矛盾回执覆盖")
            return self._receipt_result(row)
        if receipt["state"] in {"accepted", "queued"} and not row["issued_at"]:
            raise BridgeError("消息尚未取得实际投递许可")
        if receipt["state"] == "waiting" and current != "waiting":
            return self._receipt_result(row)
        state = {"accepted": "done", "queued": "done", "not_sent": "failed",
                 "uncertain": "uncertain", "waiting": "waiting"}[receipt["state"]]
        finished = None if state == "waiting" else time.time()
        db.execute("UPDATE replies SET receipt_state=?,receipt_stage=?,native_id=?,state=?,"
                   "finished=coalesce(finished,?),result=NULL WHERE task_id=?",
                   (receipt["state"], receipt["stage"], receipt.get("native_id"), state,
                    finished, row["task_id"]))
        return self._receipt_result(db.execute("SELECT * FROM replies WHERE task_id=?", (row["task_id"],)).fetchone())

    def record_delivery(self, task_id, receipt):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM replies WHERE task_id=?", (task_id,)).fetchone()
            return self._record_delivery(db, row, receipt)

    def _expire_external_results(self, db, now):
        db.execute("UPDATE replies SET external_result=NULL,external_result_state='expired' "
                   "WHERE mode='message' AND external_result IS NOT NULL AND external_result_at<=?",
                   (now - self.limits.result_ttl_seconds,))

    def store_external_result(self, task_id, text):
        """Cache a real external reply before attempting a separate reverse delivery."""
        if not isinstance(text, str) or len(text.encode("utf8")) > self.limits.max_text_bytes:
            raise BridgeError("外部正文超过结果上限")
        if not text.strip():
            return False
        digest = hashlib.sha256(text.encode("utf8")).hexdigest()
        now = time.time()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM replies WHERE task_id=? AND mode='message'", (task_id,)).fetchone()
            if not row:
                raise BridgeError("外部正文缺少对应的消息记录")
            if row["result_hash"]:
                if row["result_hash"] == digest:
                    return False
                raise BridgeError("该消息已有不同的外部结果")
            self._expire_external_results(db, now)
            if db.execute("SELECT count(*) FROM replies WHERE mode='message' AND external_result IS NOT NULL").fetchone()[0] >= self.limits.max_completed_tasks:
                raise BridgeError("外部正文短期缓存已满")
            db.execute("UPDATE replies SET external_result=?,external_result_at=?,external_result_state='ready',"
                       "result_hash=? WHERE task_id=?", (text, now, digest, task_id))
            return True

    def pending_external_results(self):
        with self.connect() as db:
            return [dict(row) for row in db.execute(
                "SELECT * FROM replies WHERE mode='message' AND external_result_state='ready' "
                "AND external_result IS NOT NULL AND external_result_at>? ORDER BY external_result_at LIMIT ?",
                (time.time() - self.limits.result_ttl_seconds, self.limits.max_completed_tasks))]

    def mark_external_result_handled(self, task_id):
        with self.connect() as db:
            db.execute("UPDATE replies SET external_result_state='handled' "
                       "WHERE task_id=? AND mode='message' AND external_result_state='ready'", (task_id,))

    def external_result_for(self, owner, message_id):
        with self.connect() as db:
            row = db.execute("SELECT message_id,target_ref,external_result,external_result_at,external_result_state "
                             "FROM replies WHERE owner=? AND message_id=? AND mode='message' "
                             "AND external_result IS NOT NULL AND external_result_at>?",
                             (owner, message_id, time.time() - self.limits.result_ttl_seconds)).fetchone()
            if not row:
                return None
            return {"message_id": row["message_id"], "target_ref": json.loads(row["target_ref"]),
                    "text": row["external_result"], "at": row["external_result_at"],
                    "state": row["external_result_state"]}

    def row(self, task_id):
        with self.connect() as db:
            row = db.execute("SELECT * FROM replies WHERE task_id=?", (task_id,)).fetchone()
            return dict(row) if row else None

    def submit(self, address, text, sender, *, instance=None, require_delivery=False):
        if len(text.encode("utf8")) > self.limits.max_text_bytes:
            raise BridgeError("A2A 回件超过正文上限，请缩短结果或给出文件位置")
        digest = hashlib.sha256(text.encode("utf8")).hexdigest()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM replies WHERE address=?", (address,)).fetchone()
            if not row:
                raise BridgeError("A2A 回信地址不存在")
            if row["mode"] == "message":
                raise BridgeError("普通消息由自动收件回执确认")
            if instance is not None and row["instance"] != instance:
                raise BridgeError("A2A 回件不属于当前服务实例")
            if require_delivery and (not row["issued_at"] or not row["target_ref"]
                    or json.loads(row["target_ref"]) != [sender.get(k) for k in ("device_id", "software", "conversation_id")]):
                raise BridgeError("这个原聊天没有取得本次投递")
            if row["agent"].casefold() != sender["alias"].casefold() or row["conversation"] != sender["conversation_id"]:
                raise BridgeError("当前聊天不是这项 A2A 任务的收件方")
            if row["result"] is not None:
                if row["result"] == text:
                    return
                raise BridgeError("该 A2A 任务已有结果，未覆盖")
            if row["result_hash"]:
                if row["result_hash"] == digest:
                    return
                raise BridgeError("该 A2A 任务已有结果，未覆盖")
            if row["state"] != "waiting":
                raise BridgeError("该 A2A 任务已停止等待，未提交新结果")
            db.execute("UPDATE replies SET result=? WHERE task_id=?", (text, row["task_id"]))

    def finish(self, task_id, state):
        with self.connect() as db:
            row = db.execute("SELECT * FROM replies WHERE task_id=?", (task_id,)).fetchone()
            if row and row["mode"] == "message":
                if row["receipt_state"] in {"accepted", "queued", "not_sent", "uncertain"}:
                    return
                receipt = {"state": "uncertain" if row["issued_at"] else "not_sent", "stage": "relay"}
                self._record_delivery(db, row, receipt)
                return
            db.execute("UPDATE replies SET state=?,finished=coalesce(finished,?) WHERE task_id=?",
                       (state, time.time(), task_id))

    def seal(self, task_id, state):
        """The SDK stored the terminal task; the mailbox no longer needs its text."""
        with self.connect() as db:
            row = db.execute("SELECT * FROM replies WHERE task_id=?", (task_id,)).fetchone()
            if row:
                if row["mode"] == "message":
                    if row["receipt_state"] not in {"accepted", "queued", "not_sent", "uncertain"}:
                        self._record_delivery(db, row, {"state": "uncertain" if row["issued_at"] else "not_sent",
                                                        "stage": "relay"})
                    db.execute("UPDATE replies SET result=NULL WHERE task_id=?", (task_id,))
                    return
                digest = hashlib.sha256(row["result"].encode("utf8")).hexdigest() if row["result"] is not None else row["result_hash"]
                db.execute("UPDATE replies SET result=NULL,result_hash=?,state=?,finished=coalesce(finished,?) WHERE task_id=?",
                           (digest, state, time.time(), task_id))

    def previous_instance(self, instance):
        with self.connect() as db:
            return [dict(r) for r in db.execute(
                "SELECT * FROM replies WHERE instance<>? AND (state IN ('waiting','interrupted','done') "
                "OR (mode='message' AND state IN ('uncertain','failed')))", (instance,)
            )]

    def outgoing(self, source, peer):
        with self.connect() as db:
            row = db.execute(
                "SELECT context_id,task_id FROM outgoing WHERE source=? AND peer=?",
                (source, peer),
            ).fetchone()
            return dict(row) if row else {"context_id": "", "task_id": ""}

    def save_outgoing(self, source, peer, context_id, task_id):
        with self.connect() as db:
            db.execute(
                "INSERT INTO outgoing(source,peer,context_id,task_id,updated) VALUES(?,?,?,?,?) ON CONFLICT(source,peer) "
                "DO UPDATE SET context_id=excluded.context_id, task_id=excluded.task_id,updated=excluded.updated",
                (source, peer, context_id, task_id, time.time()),
            )

    def forget_outgoing(self, source, peer):
        with self.connect() as db:
            db.execute("DELETE FROM outgoing WHERE source=? AND peer=?", (source, peer))

    def cleanup(self, now=None, removed_tasks=()):
        now = time.time() if now is None else now
        with self.connect() as db:
            self._expire_external_results(db, now)
            # Upgrade old completed records without retaining their plaintext.
            db.execute("UPDATE replies SET finished=created WHERE state<>'waiting' AND finished IS NULL")
            rows = db.execute("SELECT task_id,result FROM replies WHERE state<>'waiting' AND result IS NOT NULL AND finished<?",
                              (now-self.limits.result_ttl_seconds,)).fetchall()
            for row in rows:
                db.execute("UPDATE replies SET result=NULL,result_hash=?,finished=coalesce(finished,created) WHERE task_id=?",
                           (hashlib.sha256(row["result"].encode("utf8")).hexdigest(), row["task_id"]))
            db.execute("DELETE FROM replies WHERE state<>'waiting' AND finished<?", (now-self.limits.metadata_ttl_seconds,))
            db.execute("DELETE FROM replies WHERE task_id IN (SELECT task_id FROM replies WHERE state<>'waiting' ORDER BY finished DESC LIMIT -1 OFFSET ?)",
                       (int(self.limits.max_metadata),))
            db.executemany("DELETE FROM outgoing WHERE task_id=?", [(v,) for v in removed_tasks])
            db.execute("DELETE FROM outgoing WHERE updated<?", (now-self.limits.metadata_ttl_seconds,))
            db.execute("DELETE FROM outgoing WHERE rowid IN (SELECT rowid FROM outgoing ORDER BY updated DESC LIMIT -1 OFFSET ?)",
                       (int(self.limits.max_metadata),))
