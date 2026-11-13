"""Business bodies and transport receipts keep separate validated limits."""
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from message_delivery import business_payload, encode_payload, native_receipt, normalize_receipt, failure_text
from workbuddy import BridgeError


class MessageProtocolTests(unittest.TestCase):
    def test_control_characters_preserve_full_supported_body(self):
        source = {"device_id": "pc", "software": "codex", "conversation_id": "chat", "alias": "Sender"}
        text = "x" + "\x00" * 65535
        payload = business_payload(source, text)
        encoded = encode_payload(payload)
        self.assertEqual(json.loads(encoded)["message"], text)
        self.assertGreater(len(encoded.encode("utf8")), 65536)
        self.assertEqual(set(payload), {"message", "source", "source_conversation"})

    def test_relay_cannot_confirm_native_receipt(self):
        with self.assertRaises(BridgeError):
            normalize_receipt({"state": "accepted", "stage": "relay"})

    def test_minis_is_scheduler_only(self):
        self.assertEqual(native_receipt({"software": "minis"},
                         {"state": "accepted", "route": "follow-up", "scheduled_task_id": "schedule"}),
                         {"state": "queued", "stage": "minis_scheduler", "native_id": "schedule"})
        with self.assertRaises(BridgeError):
            native_receipt({"software": "minis"},
                           {"state": "accepted", "receipt": {"state": "accepted", "stage": "native_host"}})

    def test_unknown_workbuddy_admission_stays_unknown(self):
        self.assertEqual(native_receipt({"software": "workbuddy"}, {"state": "accepted"})["state"], "uncertain")

    def test_failure_identifies_send_in_one_line(self):
        result = failure_text("Minis", "DOCS-PURPOSE 文档同步\n更多技术内容")
        self.assertIn("Minis", result)
        self.assertIn("DOCS-PURPOSE", result)
        self.assertNotIn("\n", result)
        self.assertLess(len(result), 80)

    def test_external_task_admission_is_queue_evidence(self):
        self.assertEqual(normalize_receipt({"state": "queued", "stage": "external_service"}),
                         {"state": "queued", "stage": "external_service"})
