"""Phone receiver safety, deduplication and reply forwarding tests."""
import json
import base64
from contextlib import redirect_stderr
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest import mock
import uuid

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))
import device_receiver
from device_receiver import Receiver, _instance_lock, _is_running, proxy_url, remote_reply
from workbuddy import BridgeError


class Response:
    def __init__(self, value):
        self.data = json.dumps(value).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def read(self, count=-1):
        return self.data if count < 0 else self.data[:count]


class FakeOpener:
    def __init__(self, responses=None):
        self.responses = list(responses or [])
        self.requests = []

    def open(self, request, timeout=0):
        self.requests.append((request, timeout))
        result = self.responses.pop(0) if self.responses else {"state": "accepted"}
        return Response(result)


class ReceiverTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.local = {"device_id": "phone / 一号", "network": {
            "base_url": "https://pc.ts.net", "token": "secret-token"},
            "a2a": {"max_text_bytes": 64}}
        self.peers = [{"alias": "Minis", "software": "minis", "device_id": "phone / 一号",
                       "conversation_id": "registered-sid"}]
        self.now = 2000000000
        self.opener = FakeOpener()
        self.calls = []
        self.receiver = Receiver(self.root, self.local, self.peers, self.deliver,
                                 self.opener, clock=lambda: self.now)

    def deliver(self, local, peer, text, delivery_id):
        self.calls.append((local, peer, text, delivery_id))
        return {"state": "accepted", "scheduled_task_id": "task-17"}

    def message(self, **updates):
        value = {"state": "delivery", "delivery_id": str(uuid.uuid4()), "peer": "Minis",
                 "conversation_id": "registered-sid", "text": "请处理这项任务",
                 "reply_to": "A2A-abcdef012345", "expires_at": self.now + 60}
        value.update(updates)
        return value

    def test_delivery_calls_exact_registered_minis_and_ack_contains_metadata_only(self):
        delivery = self.message()
        result = self.receiver.process(delivery)
        self.assertEqual(result, {"state": "accepted", "scheduled_task_id": "task-17"})
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.calls[0][1]["conversation_id"], "registered-sid")
        ack = self.opener.requests[-1][0]
        self.assertTrue(ack.full_url.endswith("/ack"))
        self.assertEqual(json.loads(ack.data), {"delivery_id": delivery["delivery_id"],
                                                "state": "accepted", "scheduled_task_id": "task-17"})
        self.assertNotIn("请处理", ack.data.decode())
        self.assertEqual(ack.get_header("Authorization"), "Bearer secret-token")

    def test_duplicate_delivery_never_invokes_minis_twice(self):
        delivery = self.message()
        self.receiver.process(delivery)
        self.receiver.process(delivery)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(len(self.opener.requests), 1)

    def test_crash_marker_is_converted_to_uncertain_without_native_retry(self):
        delivery = self.message()
        with self.receiver._db() as db:
            db.execute("INSERT INTO receipts(delivery_id,state,created,updated,acked) VALUES(?,?,?,?,0)",
                       (delivery["delivery_id"], "dispatching", self.now, self.now,))
        result = self.receiver.process(delivery)
        self.assertEqual(result["state"], "uncertain")
        self.assertEqual(self.calls, [])
        self.assertEqual(json.loads(self.opener.requests[-1][0].data)["state"], "uncertain")

    def test_wrong_conversation_expired_and_oversized_messages_are_not_sent(self):
        for delivery in (self.message(conversation_id="another-sid"),
                         self.message(expires_at=self.now - 1),
                         self.message(text="字" * 3000)):
            with self.subTest(delivery=delivery):
                self.assertEqual(self.receiver.process(delivery)["state"], "not_sent")
        self.assertEqual(self.calls, [])

    def test_native_ambiguity_is_saved_and_not_retried(self):
        delivery = self.message()
        self.receiver.deliver_callback = mock.Mock(side_effect=BridgeError("unknown", "uncertain"))
        self.receiver.process(delivery)
        self.receiver.process(delivery)
        self.assertEqual(self.receiver.deliver_callback.call_count, 1)
        self.assertEqual(json.loads(self.opener.requests[-1][0].data)["state"], "uncertain")

    def test_ack_failure_keeps_receipt_for_safe_ack_retry(self):
        delivery = self.message()
        self.opener.responses = [{"state": "rejected"}, {"state": "accepted"}]
        with self.assertRaises(BridgeError):
            self.receiver.process(delivery)
        self.assertEqual(len(self.calls), 1)
        self.receiver.process(delivery)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(len(self.opener.requests), 2)

    def test_maintenance_retries_ack_even_without_server_redelivery(self):
        delivery = self.message()
        with self.receiver._db() as db:
            db.execute("""INSERT INTO receipts(delivery_id,state,scheduled_task_id,created,updated,acked,
                reply_to,peer,conversation_id,expires_at) VALUES(?,?,?,?,?,0,?,?,?,?)""",
                       (delivery["delivery_id"], "accepted", "task-17", self.now, self.now,
                        delivery["reply_to"], delivery["peer"], delivery["conversation_id"],
                        delivery["expires_at"]))
        result = self.receiver.maintenance(force=True)
        self.assertEqual(result["acks"], 1)
        self.assertEqual(json.loads(self.opener.requests[0][0].data), {
            "delivery_id": delivery["delivery_id"], "state": "accepted", "scheduled_task_id": "task-17"})
        row = self.receiver._receipt(delivery["delivery_id"])
        self.assertEqual(row["acked"], 1)

    def test_maintenance_cleans_exact_owned_task_only_after_reply_or_expiry(self):
        delivery = self.message()
        self.receiver.process(delivery)
        calls = []
        results = iter(({"state": "pending"}, {"state": "removed"}))
        class Minis:
            @staticmethod
            def cleanup_task(task_id, message_id, conversation_id, expired=False):
                calls.append((task_id, message_id, conversation_id))
                return next(results)
        with mock.patch.dict(sys.modules, {"minis": Minis}):
            self.receiver.maintenance(force=True)
            row = self.receiver._receipt(delivery["delivery_id"])
            self.assertEqual(row["cleanup_done"], 0)
            with self.receiver._db() as db:
                db.execute("UPDATE receipts SET reply_received=1 WHERE delivery_id=?", (delivery["delivery_id"],))
            self.receiver.maintenance(force=True)
            self.receiver.maintenance(force=True)
        row = self.receiver._receipt(delivery["delivery_id"])
        self.assertEqual(row["cleanup_done"], 1)
        self.assertEqual(calls, [("task-17", delivery["delivery_id"], "registered-sid")] * 2)

    def test_expired_task_cleanup_uses_expired_flag(self):
        delivery = self.message()
        self.receiver.process(delivery)
        self.now = delivery["expires_at"] + 1
        calls = []
        class Minis:
            @staticmethod
            def cleanup_task(task_id, message_id, conversation_id, expired=False):
                calls.append(expired)
                return {"state": "removed"}
        with mock.patch.dict(sys.modules, {"minis": Minis}):
            self.receiver.maintenance(force=True)
        self.assertEqual(calls, [True])
        self.assertEqual(self.receiver._receipt(delivery["delivery_id"])["cleanup_done"], 1)

    def test_legacy_database_migrates_and_uncertain_receipt_retains_server_ttl(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime = root / "runtime"
            runtime.mkdir()
            import sqlite3
            db = sqlite3.connect(runtime / "device-receipts.db")
            db.execute("CREATE TABLE receipts(delivery_id TEXT PRIMARY KEY,state TEXT NOT NULL,"
                       "scheduled_task_id TEXT,created REAL NOT NULL,updated REAL NOT NULL,"
                       "acked INTEGER NOT NULL DEFAULT 0)")
            db.execute("INSERT INTO receipts VALUES(?,?,?,?,?,1)", ("migrated", "uncertain", None,
                                                                      self.now - 90000, self.now - 90000))
            db.commit()
            db.close()
            receiver = Receiver(root, self.local, self.peers, clock=lambda: self.now)
            conn = sqlite3.connect(receiver.db_path)
            columns = {row[1] for row in conn.execute("PRAGMA table_info(receipts)")}
            conn.close()
            self.assertTrue({"reply_to", "peer", "conversation_id", "expires_at", "cleanup_done"} <= columns)
            with receiver._db() as conn:
                conn.execute("UPDATE receipts SET expires_at=? WHERE delivery_id='migrated'", (self.now - 3600,))
            receiver._prune_receipts()
            self.assertIsNotNone(receiver._receipt("migrated"))
            future = self.now + 86401
            receiver.clock = lambda: future
            receiver._prune_receipts()
            self.assertIsNone(receiver._receipt("migrated"))

    def test_capacity_refuses_claim_without_evicting_or_invoking(self):
        delivery = self.message()
        with mock.patch("device_receiver.MAX_RECEIPTS", 0):
            with self.assertRaises(BridgeError):
                self.receiver.process(delivery)
        self.assertEqual(self.calls, [])
        self.assertIsNone(self.receiver._receipt(delivery["delivery_id"]))

    def test_single_instance_lock_is_released_on_scope_exit(self):
        with _instance_lock(self.root):
            self.assertTrue(_is_running(self.root))
        self.assertFalse(_is_running(self.root))

    def test_health_replace_permission_error_does_not_stop_later_polls(self):
        self.receiver.instance = "health-test"
        self.opener.responses = [{"state": "empty"}, {"state": "empty"}]
        with mock.patch("device_receiver.os.replace", side_effect=PermissionError("reader race")):
            self.assertEqual(self.receiver.run_once(wait=0)["state"], "empty")
            self.assertEqual(self.receiver.run_once(wait=0)["state"], "empty")
        self.assertEqual(len(self.opener.requests), 2)

    def test_immediate_empty_polls_are_throttled(self):
        iterations = [0]
        def should_stop():
            iterations[0] += 1
            return iterations[0] > 3
        self.receiver.poll = mock.Mock(return_value={"state": "empty"})
        with mock.patch("device_receiver.time.sleep") as sleep:
            self.receiver.run(wait=0, stop=should_stop)
        self.assertEqual(sleep.call_count, 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [0.1, 0.1, 0.1])

    def test_once_mode_refuses_to_race_a_running_service(self):
        (self.root / "local.json").write_text(json.dumps(self.local), encoding="utf-8")
        (self.root / "peers.json").write_text(json.dumps({"version": 1, "peers": {}}), encoding="utf-8")
        stderr = io.StringIO()
        with _instance_lock(self.root), mock.patch.object(device_receiver.sys, "argv", [
            "device_receiver.py", "--root", str(self.root), "--once"]), redirect_stderr(stderr):
            self.assertEqual(device_receiver.main(), 2)
            self.assertTrue(_is_running(self.root))
        self.assertIn("已经运行", stderr.getvalue())

    def test_client_quotes_device_id_and_never_accepts_arbitrary_http_server(self):
        self.receiver.poll(0)
        request, _ = self.opener.requests[0]
        self.assertIn("phone%20%2F%20%E4%B8%80%E5%8F%B7", request.full_url)
        with self.assertRaises(BridgeError):
            Receiver(self.root, {**self.local, "network": {"base_url": "http://example.com", "token": "x"}},
                     self.peers, self.deliver)


class ReplyForwardingTests(unittest.TestCase):
    def test_reply_uses_device_endpoint_and_keeps_credentials_in_header(self):
        opener = FakeOpener([{"state": "accepted"}])
        result = remote_reply(Path("unused"), {
            "device_id": "phone", "network": {"base_url": "https://pc.ts.net", "token": "t"}},
            "A2A-abcdef012345", "完成", {"alias": "Minis", "conversation_id": "sid"}, opener)
        request = opener.requests[0][0]
        self.assertEqual(result["state"], "accepted")
        self.assertTrue(request.full_url.endswith("/devices/phone/reply"))
        self.assertEqual(request.get_header("Authorization"), "Bearer t")
        self.assertEqual(json.loads(request.data)["text"], "完成")

    def test_accepted_remote_reply_marks_receipt_for_task_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            local = {"device_id": "phone", "network": {
                "base_url": "https://pc.ts.net", "token": "t"}}
            receiver = Receiver(Path(tmp), local, [])
            delivery_id = str(uuid.uuid4())
            delivery = {"reply_to": "A2A-abcdef012345", "peer": "Minis",
                        "conversation_id": "sid", "expires_at": 2000000000}
            receiver._claim(delivery_id, delivery)
            receiver._save_state(delivery_id, "accepted", "task-id")
            result = remote_reply(Path(tmp), local, delivery["reply_to"], "完成",
                                  {"alias": "Minis", "conversation_id": "sid"},
                                  FakeOpener([{"state": "accepted"}]))
            self.assertEqual(result["state"], "accepted")
            self.assertEqual(receiver._receipt(delivery_id)["reply_received"], 1)

    def test_proxy_requires_explicit_opt_in_and_loopback_address(self):
        env = {"HTTPS_PROXY": "http://tag:secret@127.0.0.1:43210"}
        self.assertIsNone(proxy_url({"network": {}}, env))
        self.assertIsNone(proxy_url({"network": {"proxy_from_env": True}}, {"HTTPS_PROXY": ""}))
        self.assertEqual(proxy_url({"network": {"proxy_from_env": True}}, env), env["HTTPS_PROXY"])
        self.assertEqual(proxy_url({"network": {"proxy_url": "http://127.0.0.1:3210"}}, {}),
                         "http://127.0.0.1:3210")
        with self.assertRaises(BridgeError):
            proxy_url({"network": {"proxy_url": "http://proxy.example:3210"}}, {})
        with self.assertRaises(BridgeError):
            proxy_url({"network": {"proxy_url": "socks5://127.0.0.1:3210"}}, {})
        minis_env = {"MINIS_MCP_PROXY": "http://tag:secret@127.0.0.1:43211",
                     "HTTPS_PROXY": "", "https_proxy": "", "http_proxy": ""}
        self.assertEqual(proxy_url({"platform": "minis", "network": {"proxy_from_env": True}}, minis_env),
                         minis_env["MINIS_MCP_PROXY"])
        self.assertIsNone(proxy_url({"platform": "windows", "network": {"proxy_from_env": True}}, minis_env))

    def test_urllib_sends_proxy_credentials_only_in_local_connect_header(self):
        seen = []
        connected = threading.Event()

        class ProxyHandler(BaseHTTPRequestHandler):
            def do_CONNECT(self):
                seen.append(self.headers.get("Proxy-Authorization"))
                connected.set()
                self.send_response(200)
                self.end_headers()
                self.close_connection = True

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), ProxyHandler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        local = {"device_id": "phone", "network": {
            "base_url": "https://pc.ts.net", "token": "device-secret",
            "proxy_url": "http://tag:proxy-secret@127.0.0.1:" + str(server.server_port)}}
        with tempfile.TemporaryDirectory() as tmp:
            receiver = Receiver(Path(tmp), local, [], opener=None)
            try:
                with self.assertRaises(BridgeError):
                    receiver.poll(0)
                self.assertTrue(connected.wait(2))
                expected = "Basic " + base64.b64encode(b"tag:proxy-secret").decode("ascii")
                self.assertEqual(seen, [expected])
            finally:
                receiver.db_path.unlink(missing_ok=True)
                receiver.db_path.parent.rmdir()


if __name__ == "__main__":
    unittest.main()
