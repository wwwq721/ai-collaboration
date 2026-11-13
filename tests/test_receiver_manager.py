"""Cooperative receiver lifecycle over a local empty-delivery fixture."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))
import receiver_manager


class ReceiverManagerTests(unittest.TestCase):
    def setUp(self):
        self.polls = []
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                parent.polls.append(self.path)
                body = json.dumps({"state": "empty"}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                self.send_error(405)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        local = {"platform": "minis", "device_id": "phone-device", "network": {
            "base_url": "http://127.0.0.1:" + str(self.server.server_port),
            "token": "loopback-test-token"}}
        (self.root / "local.json").write_text(json.dumps(local), encoding="utf-8")
        (self.root / "peers.json").write_text(json.dumps({"version": 1, "peers": {}}), encoding="utf-8")

    def test_start_status_duplicate_start_and_cooperative_stop(self):
        initial = receiver_manager.status(self.root)
        self.assertEqual(initial["state"], "stopped")
        self.assertFalse(initial["running"])

        started = receiver_manager.start(self.root, timeout=6)
        self.assertIn(started["action"], {"started", "starting"}, started)
        self.assertTrue(started["running"])
        pid = started["pid"]
        self.assertIsInstance(pid, int)

        duplicate = receiver_manager.start(self.root, timeout=1)
        self.assertEqual(duplicate["action"], "already_running")
        self.assertEqual(duplicate["pid"], pid)

        deadline = time.monotonic() + 4
        while not self.polls and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertTrue(self.polls)
        self.assertTrue(all("/devices/phone-device/next?wait=25" in path for path in self.polls))
        for _ in range(40):
            snapshot = receiver_manager.status(self.root)
            self.assertTrue(snapshot["running"], snapshot)
            time.sleep(0.005)

        stopped = receiver_manager.stop(self.root, timeout=8)
        self.assertEqual(stopped["action"], "stopped", stopped)
        self.assertFalse(stopped["running"])
        self.assertEqual(receiver_manager.status(self.root)["state"], "stopped")
        self.assertEqual(self.polls, ["/devices/phone-device/next?wait=25&policy=2&message_version=1"] * len(self.polls))

    def test_foreign_stop_token_does_not_stop_receiver(self):
        started = receiver_manager.start(self.root, timeout=6)
        self.assertTrue(started["running"])
        status_path = self.root / "runtime" / "device-receiver-status.json"
        metadata = json.loads(status_path.read_text(encoding="utf-8"))
        stop_path = self.root / "runtime" / "device-receiver-stop.json"
        stop_path.write_text(json.dumps({"instance": "different-instance", "pid": metadata["pid"],
                                         "process_started_at": metadata["process_started_at"]}),
                             encoding="utf-8")
        time.sleep(0.3)
        self.assertTrue(receiver_manager.status(self.root)["running"])
        stopped = receiver_manager.stop(self.root, timeout=8)
        self.assertEqual(stopped["action"], "stopped", stopped)


if __name__ == "__main__":
    unittest.main()
