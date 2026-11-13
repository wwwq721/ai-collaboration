"""Phone-side HTTPS long-poll receiver for A2A deliveries."""
import argparse
from contextlib import contextmanager
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
import uuid

from workbuddy import BridgeError

ROOT = Path(__file__).resolve().parent.parent
RECEIPT_TTL = 86400
MAX_RECEIPTS = 5000
MAX_RESPONSE = 1024 * 1024
MAINTENANCE_INTERVAL = 60


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class _PairedOpener:
    """urllib-shaped bounded responses using the shared strict TLS transport."""
    def __init__(self, local):
        self.local = local

    @contextmanager
    def open(self, request, timeout=35):
        import httpx
        from network_transport import build_sync_http_options
        options = build_sync_http_options(self.local)
        try:
            with httpx.Client(trust_env=False, follow_redirects=False, timeout=timeout, **options) as client:
                with client.stream(request.get_method(), request.full_url,
                                   headers=dict(request.header_items()), content=request.data) as response:
                    if not 200 <= response.status_code < 300:
                        raise HTTPError(request.full_url, response.status_code, "request rejected", {}, None)

                    class BoundedResponse:
                        def read(self, count=MAX_RESPONSE + 1):
                            limit = min(count if count >= 0 else MAX_RESPONSE + 1, MAX_RESPONSE + 1)
                            data = bytearray()
                            for chunk in response.iter_bytes(chunk_size=16384):
                                data.extend(chunk[:max(0, limit-len(data))])
                                if len(data) >= limit:
                                    break
                            return bytes(data)

                    yield BoundedResponse()
        except httpx.HTTPError as error:
            raise URLError("paired HTTPS connection failed") from error


def network_opener(local, opener=None):
    if opener is not None:
        return opener
    if local.get("network", {}).get("mode") == "paired_ip":
        return _PairedOpener(local)
    proxy = proxy_url(local)
    return build_opener(ProxyHandler({"https": proxy, "http": proxy} if proxy else {}), _NoRedirect())


def _json_bytes(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as error:
        raise BridgeError("设备接收配置不可用") from error


def _valid_base_url(value):
    if not isinstance(value, str):
        return False
    parsed = urlsplit(value)
    if parsed.scheme == "https" and parsed.hostname and not parsed.username and not parsed.password:
        return not parsed.query and not parsed.fragment
    # Cleartext is permitted only for local test servers.
    return (parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "::1", "localhost"}
            and not parsed.username and not parsed.password and not parsed.query and not parsed.fragment)


def proxy_url(local, environ=os.environ):
    """Return only an explicitly opted-in loopback HTTP proxy URL.

    Ambient proxy variables are ignored unless network.proxy_from_env is true.
    """
    network = local.get("network", {}) if isinstance(local, dict) else {}
    if not isinstance(network, dict):
        return None
    value = network.get("proxy_url")
    if value is None and network.get("proxy_from_env") is True:
        candidates = []
        platform_name = local.get("platform") if isinstance(local, dict) else None
        if isinstance(platform_name, str) and platform_name.casefold() == "minis":
            candidates.append(environ.get("MINIS_MCP_PROXY"))
        candidates.extend((environ.get("HTTPS_PROXY"), environ.get("https_proxy"), environ.get("http_proxy")))
        value = next((candidate for candidate in candidates if candidate), None)
    if value is None:
        return None
    if not isinstance(value, str):
        raise BridgeError("本机代理配置不安全")
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        port = parsed.port
        loopback = hostname == "localhost" or (hostname is not None and ipaddress.ip_address(hostname).is_loopback)
        if (parsed.scheme != "http" or not loopback or port is None or not 1 <= port <= 65535
                or parsed.path not in ("", "/") or parsed.query or parsed.fragment):
            raise ValueError("unsafe proxy")
    except (ValueError, TypeError):
        raise BridgeError("本机代理配置不安全")
    return value


@contextmanager
def _instance_lock(root):
    """Hold an OS advisory lock; the OS releases it when this process exits."""
    directory = Path(root) / "runtime"
    directory.mkdir(parents=True, exist_ok=True)
    handle = (directory / "device-receiver.lock").open("a+b")
    acquired = False
    try:
        if os.name == "nt":
            import msvcrt
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as error:
                raise BridgeError("设备接收服务已经运行") from error
            acquired = True
        else:
            import fcntl
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as error:
                raise BridgeError("设备接收服务已经运行") from error
            acquired = True
        yield handle
    finally:
        if acquired:
            try:
                if os.name == "nt":
                    import msvcrt
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        handle.close()


def _is_running(root):
    try:
        lock = _instance_lock(root)
        handle = lock.__enter__()
    except BridgeError:
        return True
    try:
        return False
    finally:
        lock.__exit__(None, None, None)


class Receiver:
    def __init__(self, root=ROOT, local=None, peers=None, deliver=None, opener=None, clock=None,
                 instance=None):
        self.root = Path(root)
        self.local = local if local is not None else _read_json(self.root / "local.json")
        if peers is None:
            book = _read_json(self.root / "peers.json")
            peers = book.get("peers", {}) if book.get("version") == 1 else {}
            peers = [{**peer, "alias": alias} for alias, peer in peers.items() if isinstance(peer, dict)]
        self.peers = list(peers.values()) if isinstance(peers, dict) else list(peers)
        self.deliver_callback = deliver
        self.clock = clock or time.time
        self.instance = instance
        self.process_started_at = self.clock()
        self.last_successful_poll = None
        network = self.local.get("network", {})
        self.base_url = network.get("base_url") if isinstance(network, dict) else None
        self.token = network.get("token") if isinstance(network, dict) else None
        if not self.local.get("device_id") or not _valid_base_url(self.base_url):
            raise BridgeError("设备接收配置缺少设备名称或安全服务地址")
        if not isinstance(self.token, str) or not self.token:
            raise BridgeError("设备接收配置缺少网络令牌")
        limits = self.local.get("a2a", {})
        self.max_text_bytes = limits.get("max_text_bytes", 65536) if isinstance(limits, dict) else 65536
        if isinstance(self.max_text_bytes, bool) or not isinstance(self.max_text_bytes, int) or self.max_text_bytes <= 0:
            self.max_text_bytes = 65536
        # Permit only an explicitly configured local proxy; TLS validation for
        # the HTTPS service remains urllib's default.
        self.opener = network_opener(self.local, opener)
        self.db_path = self.root / "runtime" / "device-receipts.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._last_maintenance = None
        with self._db() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS receipts (
                delivery_id TEXT PRIMARY KEY, state TEXT NOT NULL,
                scheduled_task_id TEXT, created REAL NOT NULL,
                updated REAL NOT NULL, acked INTEGER NOT NULL DEFAULT 0,
                reply_to TEXT, peer TEXT, conversation_id TEXT,
                expires_at REAL, reply_received INTEGER NOT NULL DEFAULT 0,
                cleanup_done INTEGER NOT NULL DEFAULT 0
            )""")
            columns = {row[1] for row in db.execute("PRAGMA table_info(receipts)")}
            for name, definition in (
                ("reply_to", "TEXT"), ("peer", "TEXT"), ("conversation_id", "TEXT"),
                ("expires_at", "REAL"), ("reply_received", "INTEGER NOT NULL DEFAULT 0"),
                ("cleanup_done", "INTEGER NOT NULL DEFAULT 0"),
                ("mode", "TEXT NOT NULL DEFAULT 'legacy'"), ("receipt", "TEXT"),
                ("ack_expired", "INTEGER NOT NULL DEFAULT 0"),
            ):
                if name not in columns:
                    db.execute("ALTER TABLE receipts ADD COLUMN " + name + " " + definition)
            db.execute("CREATE INDEX IF NOT EXISTS receipts_created ON receipts(created)")

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.db_path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA secure_delete=ON")
        try:
            with db:
                yield db
        finally:
            db.close()

    @property
    def endpoint(self):
        return self.base_url.rstrip("/") + "/devices/" + quote(self.local["device_id"], safe="")

    @property
    def status_path(self):
        return self.root / "runtime" / "device-receiver-status.json"

    @property
    def stop_path(self):
        return self.root / "runtime" / "device-receiver-stop.json"

    def write_health(self):
        if not self.instance:
            return
        data = {"instance": self.instance, "pid": os.getpid(),
                "process_started_at": self.process_started_at,
                "last_successful_poll": self.last_successful_poll,
                "heartbeat_at": self.clock()}
        temporary = self.status_path.with_name(
            self.status_path.name + ".tmp-" + str(os.getpid()))
        try:
            temporary.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
            os.replace(temporary, self.status_path)
        except OSError:
            # Health is advisory. A status reader can briefly hold the file on
            # Windows; that must never terminate polling or delivery.
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass

    def stop_requested(self):
        if not self.instance:
            return False
        try:
            request = json.loads(self.stop_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return False
        return (isinstance(request, dict) and request.get("instance") == self.instance
                and request.get("pid") == os.getpid()
                and request.get("process_started_at") == self.process_started_at)

    def remove_lifecycle_files(self):
        if not self.instance:
            return
        for path, token_key in ((self.status_path, "instance"), (self.stop_path, "instance")):
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(value, dict) and value.get(token_key) == self.instance:
                    path.unlink(missing_ok=True)
            except (OSError, ValueError):
                pass

    def _request(self, method, path, payload=None, timeout=35):
        body = _json_bytes(payload) if payload is not None else None
        headers = {"Authorization": "Bearer " + self.token, "Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json; charset=utf-8"
        request = Request(self.endpoint + path, data=body, headers=headers, method=method)
        try:
            with self.opener.open(request, timeout=timeout) as response:
                raw = response.read(MAX_RESPONSE + 1)
                if len(raw) > MAX_RESPONSE:
                    raise BridgeError("接收服务返回内容超过限制", "uncertain")
                if not raw:
                    return {}
                result = json.loads(raw.decode("utf-8"))
                if not isinstance(result, dict):
                    raise ValueError("response is not an object")
                return result
        except HTTPError as error:
            # Do not follow redirects: the bearer token is never sent to a new origin.
            raise BridgeError("接收服务未确认请求（HTTP " + str(error.code) + ")", "uncertain") from error
        except (URLError, TimeoutError, OSError, ValueError) as error:
            raise BridgeError("接收服务连接或回执异常", "uncertain") from error

    def poll(self, wait=25):
        if isinstance(wait, bool) or not isinstance(wait, int) or not 0 <= wait <= 25:
            raise BridgeError("长轮询等待秒数须在 0 到 25 之间")
        return self._request("GET", "/next?" + urlencode({"wait": wait, "policy": 2, "message_version": 1}), timeout=wait + 10)

    def _receipt(self, delivery_id):
        with self._db() as db:
            row = db.execute("SELECT * FROM receipts WHERE delivery_id=?", (delivery_id,)).fetchone()
            return dict(row) if row else None

    def _save_state(self, delivery_id, state, task_id=None, receipt=None):
        with self._db() as db:
            row = db.execute("SELECT mode FROM receipts WHERE delivery_id=?", (delivery_id,)).fetchone()
            if row and row["mode"] == "message":
                from message_delivery import normalize_receipt
                receipt = normalize_receipt(receipt or {"state": state, "stage": "relay"})
            db.execute("UPDATE receipts SET state=?, scheduled_task_id=?, updated=?,receipt=? WHERE delivery_id=?",
                       (state, task_id, self.clock(), json.dumps(receipt) if receipt else None, delivery_id))

    def _claim(self, delivery_id, delivery, state="dispatching"):
        now = self.clock()
        expiry = delivery.get("expires_at") if isinstance(delivery, dict) else None
        if isinstance(expiry, bool) or not isinstance(expiry, (int, float)) or not math.isfinite(expiry):
            expiry = None
        with self._db() as db:
            count = db.execute("SELECT count(*) FROM receipts").fetchone()[0]
            if count >= MAX_RECEIPTS:
                return False
            db.execute("""INSERT OR IGNORE INTO receipts(
                delivery_id,state,created,updated,acked,reply_to,peer,conversation_id,expires_at,mode
            ) VALUES(?,?,?,?,0,?,?,?,?,?)""",
                       (delivery_id, state, now, now,
                        delivery.get("reply_to") if isinstance(delivery.get("reply_to"), str) else None,
                        delivery.get("peer") if isinstance(delivery.get("peer"), str) else None,
                        delivery.get("conversation_id") if isinstance(delivery.get("conversation_id"), str) else None,
                        expiry, "message" if delivery.get("mode") == "message" else "legacy"))
            return db.execute("SELECT changes()").fetchone()[0] == 1

    def _ack(self, delivery_id, state, task_id=None):
        payload = {"delivery_id": delivery_id, "state": state}
        if task_id:
            payload["scheduled_task_id"] = task_id
        old = self._receipt(delivery_id)
        if old and old["mode"] == "message":
            from message_delivery import normalize_receipt
            payload["receipt"] = normalize_receipt(json.loads(old["receipt"]) if old["receipt"]
                                                   else {"state": state, "stage": "relay"})
        result = self._request("POST", "/ack", payload, timeout=15)
        if result.get("state") != "accepted":
            raise BridgeError("接收服务没有确认回执", "uncertain")
        with self._db() as db:
            db.execute("UPDATE receipts SET acked=1, updated=? WHERE delivery_id=?", (self.clock(), delivery_id))

    def _peer_for(self, delivery):
        alias = delivery.get("peer")
        conversation = delivery.get("conversation_id")
        matches = [peer for peer in self.peers
                   if isinstance(peer, dict) and isinstance(alias, str)
                   and isinstance(peer.get("alias"), str)
                   and peer["alias"].casefold() == alias.casefold()
                   and peer.get("software") in {"codex", "workbuddy", "minis"}
                   and peer.get("device_id") == self.local["device_id"]
                   and peer.get("conversation_id") == conversation
                   and peer.get("enabled", True) is True]
        return matches[0] if len(matches) == 1 else None

    def _invoke(self, peer, delivery):
        from delivery_ticket import managed
        if delivery.get("mode") == "message" or delivery.get("require_authorize") is True or managed(self.local):
            if peer.get("software") != "minis":
                raise BridgeError("该手机适配器尚未核验安全排队能力，未投递")
            try:
                permission = self._request("POST", "/authorize", {"delivery_id": delivery["delivery_id"]}, timeout=15)
            except BridgeError as error:
                # No native submit happened. Keep a terminal local receipt;
                # reconnects may retry only this receipt's ACK.
                raise BridgeError("后台没有确认最终投递许可，未投递", "not_sent") from error
            if permission.get("allowed") is not True:
                raise BridgeError("后台拒绝最终投递，未投递")
        if self.deliver_callback:
            return self.deliver_callback(self.local, peer, delivery["text"], delivery["delivery_id"])
        # Kept lazy so the receiver itself remains stdlib-only until a real
        # delivery reaches an installed Minis native adapter.
        from host_adapters import get_adapter
        return get_adapter(self.local, peer["software"]).invoke(
            peer, "send", delivery["text"], delivery["delivery_id"], source="A2A",
        )

    def _validate(self, delivery):
        if not isinstance(delivery, dict) or delivery.get("state") != "delivery":
            return None, "not_sent"
        delivery_id = delivery.get("delivery_id")
        try:
            delivery_id = str(uuid.UUID(delivery_id))
        except (ValueError, TypeError, AttributeError):
            return None, "not_sent"
        text = delivery.get("text")
        if delivery.get("mode") == "message":
            from message_delivery import encode_payload
            from access_policy import chat_ref
            try:
                payload = delivery.get("message")
                text = encode_payload(payload)
                if len(payload["message"].encode("utf8")) > self.max_text_bytes:
                    raise ValueError("body limit")
                sources = [peer for peer in self.peers if isinstance(peer, dict)
                           and isinstance(peer.get("alias"), str)
                           and peer["alias"].casefold() == payload["source_conversation"].casefold()
                           and peer.get("software") == payload["source"]
                           and peer.get("enabled", True) is True
                           and chat_ref(peer) == chat_ref(delivery.get("source_ref"))]
                if len(sources) != 1:
                    raise ValueError("source address not registered")
                target = self._peer_for(delivery)
                if target is None or chat_ref(target) != chat_ref(delivery.get("target_ref")):
                    raise ValueError("wrong target")
            except (BridgeError, ValueError, TypeError, KeyError, UnicodeError):
                return {**delivery, "delivery_id": delivery_id}, "not_sent"
            expiry = delivery.get("expires_at")
            if isinstance(expiry, bool) or not isinstance(expiry, (int, float)) or not math.isfinite(expiry) or expiry <= self.clock():
                return {**delivery, "delivery_id": delivery_id}, "not_sent"
            return (target, {**delivery, "delivery_id": delivery_id, "text": text}), None
        if delivery.get("mode", "legacy") != "legacy":
            return {**delivery, "delivery_id": delivery_id}, "not_sent"
        if not isinstance(text, str) or not text.strip():
            return {**delivery, "delivery_id": delivery_id}, "not_sent"
        try:
            size = len(text.encode("utf-8"))
        except UnicodeEncodeError:
            return {**delivery, "delivery_id": delivery_id}, "not_sent"
        if size > self.max_text_bytes + 2048:
            return {**delivery, "delivery_id": delivery_id}, "not_sent"
        expiry = delivery.get("expires_at")
        if isinstance(expiry, bool) or not isinstance(expiry, (int, float)) or not math.isfinite(expiry) or expiry <= self.clock():
            return {**delivery, "delivery_id": delivery_id}, "not_sent"
        if not isinstance(delivery.get("reply_to"), str) or not re.fullmatch(r"A2A-[0-9a-fA-F]{12}", delivery["reply_to"]):
            return {**delivery, "delivery_id": delivery_id}, "not_sent"
        peer = self._peer_for(delivery)
        if peer is None:
            return {**delivery, "delivery_id": delivery_id}, "not_sent"
        return (peer, {**delivery, "delivery_id": delivery_id}), None

    def process(self, delivery):
        validated, error_state = self._validate(delivery)
        raw_id = delivery.get("delivery_id") if isinstance(delivery, dict) else None
        try:
            delivery_id = str(uuid.UUID(raw_id))
        except (ValueError, TypeError, AttributeError):
            return {"state": "not_sent"}
        old = self._receipt(delivery_id)
        if old:
            state = "uncertain" if old["state"] == "dispatching" else old["state"]
            if state not in {"accepted", "uncertain", "not_sent"}:
                state = "uncertain"
            if old["state"] == "dispatching":
                self._save_state(delivery_id, state, old["scheduled_task_id"])
            # A duplicate never enters the native adapter. Retry only its metadata ACK.
            if not old["acked"] and not old["ack_expired"]:
                self._ack(delivery_id, state, old["scheduled_task_id"])
            answer = {"state": state, **({"scheduled_task_id": old["scheduled_task_id"]} if old["scheduled_task_id"] else {})}
            if old["mode"] == "message" and old["receipt"]:
                answer["receipt"] = json.loads(old["receipt"])
            return answer
        self._prune_receipts()
        inserted = self._claim(delivery_id, delivery)
        if not inserted:
            # At the cap we leave the server delivery unacknowledged so it can
            # be retried after safe receipt pruning creates room.
            raise BridgeError("本机接收回执已满，消息尚未处理，请稍后重试", "not_sent")
        task_id, receipt = None, None
        if error_state:
            state = error_state
        else:
            peer, normalized = validated
            try:
                result = self._invoke(peer, normalized)
                if not isinstance(result, dict) or result.get("state") != "accepted":
                    state = "uncertain"
                else:
                    state = "accepted"
                task_id = result.get("scheduled_task_id") if isinstance(result, dict) else None
                if task_id is not None and (not isinstance(task_id, str) or len(task_id) > 200):
                    task_id = None
                if delivery.get("mode") == "message":
                    from message_delivery import native_receipt
                    receipt = native_receipt(peer, result)
                    state = "accepted" if receipt["state"] in {"accepted", "queued"} else receipt["state"]
            except BridgeError as error:
                state = error.state if error.state in {"accepted", "uncertain", "not_sent"} else "uncertain"
                task_id = None
            except Exception:
                state, task_id = "uncertain", None
        self._save_state(delivery_id, state, task_id, receipt)
        self._ack(delivery_id, state, task_id)
        self._prune_receipts()
        answer = {"state": state}
        if task_id:
            answer["scheduled_task_id"] = task_id
        if delivery.get("mode") == "message":
            answer["receipt"] = json.loads(self._receipt(delivery_id)["receipt"])
        return answer

    def _prune_receipts(self):
        now = self.clock()
        network = self.local.get("network", {})
        server_ttl = network.get("server_ttl_seconds", RECEIPT_TTL) if isinstance(network, dict) else RECEIPT_TTL
        if isinstance(server_ttl, bool) or not isinstance(server_ttl, (int, float)) or server_ttl <= 0:
            server_ttl = RECEIPT_TTL
        with self._db() as db:
            rows = db.execute("SELECT * FROM receipts WHERE acked=1 OR ack_expired=1").fetchall()
            for row in rows:
                row = dict(row)
                if row["state"] == "uncertain" and row["expires_at"] is not None:
                    if now < row["expires_at"] + server_ttl:
                        continue
                elif now < row["created"] + RECEIPT_TTL:
                    continue
                if row["state"] == "accepted" and row["scheduled_task_id"] and not row["cleanup_done"]:
                    continue
                db.execute("DELETE FROM receipts WHERE delivery_id=?", (row["delivery_id"],))

    def maintenance(self, force=False):
        """Retry durable ACKs and clean owned Minis follow-ups at most once/minute."""
        now_mono = time.monotonic()
        if (not force and self._last_maintenance is not None
                and now_mono - self._last_maintenance < MAINTENANCE_INTERVAL):
            return {"state": "skipped"}
        self._last_maintenance = now_mono
        now = self.clock()
        server_ttl = self.local.get("network", {}).get("server_ttl_seconds", RECEIPT_TTL)
        if isinstance(server_ttl, bool) or not isinstance(server_ttl, (int, float)) or server_ttl <= 0:
            server_ttl = RECEIPT_TTL
        with self._db() as db:
            db.execute("UPDATE receipts SET ack_expired=1 WHERE mode='message' AND acked=0 AND ack_expired=0 "
                       "AND max(created,coalesce(expires_at,created))+?<=?", (server_ttl, now))
            pending = [dict(row) for row in db.execute("SELECT * FROM receipts WHERE acked=0 AND ack_expired=0")]
            cleanup = [dict(row) for row in db.execute(
                "SELECT * FROM receipts WHERE state='accepted' AND scheduled_task_id IS NOT NULL AND cleanup_done=0")]
        acked = 0
        for row in pending:
            state = "uncertain" if row["state"] == "dispatching" else row["state"]
            if state not in {"accepted", "uncertain", "not_sent"}:
                state = "uncertain"
            if row["state"] == "dispatching":
                self._save_state(row["delivery_id"], state, row["scheduled_task_id"])
            try:
                self._ack(row["delivery_id"], state, row["scheduled_task_id"])
                acked += 1
            except BridgeError:
                pass
        removed = 0
        for row in cleanup:
            expired = row["expires_at"] is not None and row["expires_at"] <= now
            should_cleanup = row["mode"] == "message" or row["reply_received"] or expired
            if not should_cleanup or not row["conversation_id"]:
                continue
            try:
                import minis
                result = minis.cleanup_task(row["scheduled_task_id"], row["delivery_id"],
                                            row["conversation_id"], expired=expired and not row["reply_received"])
            except Exception:
                continue
            if isinstance(result, dict) and result.get("state") in {"removed", "missing"}:
                with self._db() as db:
                    db.execute("UPDATE receipts SET cleanup_done=1,updated=? WHERE delivery_id=?",
                               (self.clock(), row["delivery_id"]))
                removed += 1
        self._prune_receipts()
        return {"state": "maintained", "acks": acked, "cleanups": removed}

    def run_once(self, wait=25):
        self.maintenance()
        delivery = self.poll(wait)
        self.last_successful_poll = self.clock()
        self.write_health()
        if delivery.get("state") == "empty":
            return {"state": "empty"}
        result = self.process(delivery)
        self.maintenance()
        return result

    def run(self, wait=25, stop=None):
        while stop is None or not stop():
            try:
                result = self.run_once(wait)
                if result.get("state") == "empty":
                    # Some compatible servers return immediately instead of
                    # holding the long poll. Avoid a hot loop in that case.
                    time.sleep(0.1)
            except BridgeError:
                # Network faults are retried by the next poll. The exception
                # text is deliberately not logged because it may contain URLs.
                time.sleep(5)


def report_notification(local, task_id, notification_id, state, opener=None, origin=None, peers=None):
    """Report a native receipt; no message body or native operation is sent."""
    network = local.get("network", {})
    base_url, token = network.get("base_url"), network.get("token")
    if (not _valid_base_url(base_url) or not isinstance(token, str) or not token
            or not local.get("device_id") or state not in {"accepted", "uncertain", "not_sent"}
            or not isinstance(task_id, str) or not task_id or len(task_id) > 512
            or not isinstance(notification_id, str) or not notification_id or len(notification_id) > 200):
        raise BridgeError("结果通知回执参数无效")
    from delivery_ticket import managed
    if managed(local):
        from collaboration_client import source_token
        if not isinstance(origin, dict) or peers is None:
            raise BridgeError("管理模式通知回执需要原任务发件聊天绑定")
        token = source_token(local, peers, origin)
    path = "/devices/" + quote(local["device_id"], safe="") + "/notification"
    request = Request(base_url.rstrip("/") + path,
                      data=_json_bytes({"task_id": task_id, "notification_id": notification_id, "state": state}),
                      method="POST", headers={"Authorization": "Bearer " + token,
                                               "Content-Type": "application/json", "Accept": "application/json"})
    try:
        with network_opener(local, opener).open(request, timeout=15) as response:
            raw = response.read(MAX_RESPONSE + 1)
        if len(raw) > MAX_RESPONSE:
            raise ValueError("response too large")
        result = json.loads(raw)
        if not isinstance(result, dict) or result.get("state") != "accepted":
            raise ValueError("not acknowledged")
        return result
    except (HTTPError, URLError, TimeoutError, OSError, ValueError) as error:
        raise BridgeError("后台未确认通知状态，仅待重报元数据", "uncertain") from error


def remote_reply(root, local, address, text, sender, opener=None, peers=None):
    """Forward an A2A reply over the configured HTTPS receiver channel."""
    network = local.get("network", {})
    base_url, token = network.get("base_url"), network.get("token")
    if not local.get("device_id") or not _valid_base_url(base_url) or not isinstance(token, str) or not token:
        raise BridgeError("设备接收配置不可用")
    if not isinstance(address, str) or not re.fullmatch(r"A2A-[0-9a-fA-F]{12}", address):
        raise BridgeError("A2A 回信地址格式不正确")
    if not isinstance(text, str) or not text.strip() or not isinstance(sender, dict):
        raise BridgeError("A2A 回信内容或发件身份不正确")
    limits = local.get("a2a", {})
    limit = limits.get("max_text_bytes", 65536) if isinstance(limits, dict) else 65536
    try:
        if len(text.encode("utf-8")) > limit:
            raise BridgeError("A2A 回信超过正文上限")
    except UnicodeEncodeError as error:
        raise BridgeError("A2A 回信内容编码不正确") from error
    if not all(isinstance(sender.get(key), str) and sender[key]
               for key in ("alias", "conversation_id")):
        raise BridgeError("A2A 回信发件身份不正确")
    from delivery_ticket import managed
    if managed(local):
        from collaboration_client import source_token
        if peers is None:
            peers = _read_json(Path(root) / "peers.json")
        token = source_token(local, peers, sender)
    payload = {"address": address, "text": text, "sender": sender}
    client = network_opener(local, opener)
    path = "/devices/" + quote(local["device_id"], safe="") + "/reply"
    req = Request(base_url.rstrip("/") + path, data=_json_bytes(payload), method="POST",
                  headers={"Authorization": "Bearer " + token, "Accept": "application/json",
                           "Content-Type": "application/json; charset=utf-8"})
    try:
        with client.open(req, timeout=15) as response:
            raw = response.read(MAX_RESPONSE + 1)
        if len(raw) > MAX_RESPONSE:
            raise ValueError("response too large")
        result = json.loads(raw.decode("utf-8")) if raw else {}
        if not isinstance(result, dict) or result.get("state") != "accepted":
            raise ValueError("reply not accepted")
        receipt_db = Path(root) / "runtime" / "device-receipts.db"
        if receipt_db.is_file():
            db = None
            try:
                db = sqlite3.connect(receipt_db, timeout=3)
                with db:
                    db.execute("UPDATE receipts SET reply_received=1,updated=? WHERE reply_to=?",
                               (time.time(), address))
            except sqlite3.Error:
                # A2A accepted the reply; expiry maintenance remains the fallback.
                pass
            finally:
                if db is not None:
                    db.close()
        return result
    except (HTTPError, URLError, TimeoutError, OSError, ValueError) as error:
        raise BridgeError("接收服务未确认回信", "uncertain") from error


def _service_status(root, device_id):
    root = Path(root)
    running = _is_running(root)
    metadata = {}
    status_path = root / "runtime" / "device-receiver-status.json"
    if running:
        try:
            value = json.loads(status_path.read_text(encoding="utf-8"))
            if isinstance(value, dict):
                metadata = value
        except (OSError, ValueError):
            pass
    stop_path = root / "runtime" / "device-receiver-stop.json"
    stopping = False
    if running and metadata:
        try:
            request = json.loads(stop_path.read_text(encoding="utf-8"))
            stopping = (isinstance(request, dict)
                        and request.get("instance") == metadata.get("instance")
                        and request.get("pid") == metadata.get("pid")
                        and request.get("process_started_at") == metadata.get("process_started_at"))
        except (OSError, ValueError):
            pass
    state = "stopped" if not running else "starting" if not metadata else "stopping" if stopping else "running"
    return {"state": state, "configured": True, "running": running, "device": device_id,
            "pid": metadata.get("pid"), "process_started_at": metadata.get("process_started_at"),
            "last_successful_poll": metadata.get("last_successful_poll"),
            "heartbeat_at": metadata.get("heartbeat_at")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--once", action="store_true", help="只轮询并处理一次")
    parser.add_argument("--status", action="store_true", help="显示接收端配置状态")
    parser.add_argument("--instance", help=argparse.SUPPRESS)
    args = parser.parse_args()
    try:
        receiver = Receiver(args.root, instance=args.instance)
        if args.status:
            print(json.dumps(_service_status(args.root, receiver.local["device_id"]), ensure_ascii=False))
        elif args.once:
            with _instance_lock(args.root):
                print(json.dumps(receiver.run_once(), ensure_ascii=False))
        else:
            with _instance_lock(args.root):
                from result_watcher import background_results
                receiver.write_health()
                try:
                    with background_results(args.root, receiver.local, receiver.peers):
                        receiver.run(stop=receiver.stop_requested)
                finally:
                    receiver.remove_lifecycle_files()
        return 0
    except BridgeError as error:
        print(json.dumps({"state": error.state, "error": str(error)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
