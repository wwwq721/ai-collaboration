"""Message receipts are durable, source-scoped metadata and never business replies."""
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest import mock
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from a2a_state import Mailbox
from device_queue import DeviceQueue
from device_receiver import Receiver
from message_delivery import business_payload, request_hash
from workbuddy import BridgeError


SOURCE = {"alias": "Codex", "software": "codex", "device_id": "pc", "conversation_id": "source-chat"}
TARGET = {"alias": "Minis", "software": "minis", "device_id": "phone", "conversation_id": "target-chat"}
QUEUED = {"state": "queued", "stage": "minis_scheduler", "native_id": "schedule-1"}


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.box = Mailbox(self.tmp.name)
        self.queue = DeviceQueue(self.box)

    def claim(self, message="message-1", text="check this", target=TARGET):
        return self.box.claim_message(str(uuid.uuid4()), target["alias"], target["conversation_id"],
                                      "source-owner", message, "instance-1", SOURCE, target,
                                      request_hash(target, text))

    def queued(self, message="message-1", text="check this"):
        row, _ = self.claim(message, text)
        self.queue.enqueue("phone", "Minis", "target-chat", business_payload(SOURCE, text),
                           row["task_id"], row["address"], 60)
        return row

    def issue(self, row):
        result = self.queue.authorize("phone", row["task_id"], "instance-1",
                                      lambda task: {"contact": True, "busy": False})
        self.assertTrue(result["allowed"])

    def test_same_key_returns_original_and_changed_request_is_rejected(self):
        first, fresh = self.claim()
        same, again = self.claim()
        self.assertTrue(fresh)
        self.assertFalse(again)
        self.assertEqual(same["task_id"], first["task_id"])
        self.assertTrue(first["address"].startswith("MSG-"))
        with self.assertRaises(BridgeError):
            self.claim(text="different content")
        with self.assertRaises(BridgeError):
            self.claim(target={**TARGET, "conversation_id": "another-chat"})
        self.assertIsNone(self.box.receipt_for("another-owner", "message-1"))

    def test_restart_includes_terminal_receipts_before_sdk_projection(self):
        unknown, _ = self.claim("unknown")
        rejected, _ = self.claim("rejected")
        self.box.record_delivery(unknown["task_id"], {"state": "uncertain", "stage": "relay"})
        self.box.record_delivery(rejected["task_id"], {"state": "not_sent", "stage": "relay"})
        recovered = {row["task_id"] for row in self.box.previous_instance("instance-2")}
        self.assertEqual(recovered, {unknown["task_id"], rejected["task_id"]})
        self.assertEqual(self.box.previous_instance("instance-1"), [])

    def test_multiple_messages_do_not_wait_for_an_ai_result(self):
        first = self.queued()
        second = self.queued("message-2")
        self.assertNotEqual(first["task_id"], second["task_id"])
        self.assertEqual(self.queue.next("phone"), {"state": "empty"})
        delivery = self.queue.next("phone", message_capable=True)
        self.assertEqual(delivery["mode"], "message")
        self.assertEqual(delivery["message"], business_payload(SOURCE, "check this"))
        self.assertNotIn("text", delivery)
        self.assertTrue(delivery["require_authorize"])

    def test_positive_receipt_requires_actual_permission_and_updates_both_tables(self):
        row = self.queued()
        with self.assertRaises(BridgeError):
            self.queue.ack("phone", row["task_id"], "accepted", "schedule-1", QUEUED)
        self.assertEqual(self.queue.state(row["task_id"]), "queued")
        self.assertEqual(self.box.receipt_for("source-owner", "message-1")["state"], "waiting")
        self.issue(row)
        self.queue.ack("phone", row["task_id"], "accepted", "schedule-1", QUEUED)
        receipt = self.box.receipt_for("source-owner", "message-1")
        self.assertEqual((receipt["state"], receipt["stage"]), ("queued", "minis_scheduler"))
        self.assertEqual(self.box.row(row["task_id"])["state"], "done")
        self.assertIsNone(self.box.row(row["task_id"])["result"])
        self.assertEqual(self.queue.state(row["task_id"]), "accepted")
        self.assertEqual(self.queue.next("phone", message_capable=True), {"state": "empty"})
        with self.box.connect() as db:
            saved = db.execute("SELECT text,receipt FROM device_deliveries WHERE delivery_id=?",
                               (row["task_id"],)).fetchone()
        self.assertIsNone(saved["text"])
        self.assertEqual(json.loads(saved["receipt"]), QUEUED)

    def test_late_ack_confirms_closed_unknown_without_native_redelivery(self):
        row = self.queued()
        self.issue(row)
        self.box.record_delivery(row["task_id"], {"state": "uncertain", "stage": "relay"})
        self.queue.close(row["task_id"])
        self.queue.ack("phone", row["task_id"], "accepted", "schedule-1", QUEUED)
        self.queue.ack("phone", row["task_id"], "accepted", "schedule-1", QUEUED)
        self.assertEqual(self.box.receipt_for("source-owner", "message-1")["state"], "queued")
        self.box.finish(row["task_id"], "interrupted")
        self.box.seal(row["task_id"], "failed")
        self.assertEqual(self.box.receipt_for("source-owner", "message-1")["state"], "queued")
        self.assertEqual(self.queue.next("phone", message_capable=True), {"state": "empty"})

    def test_conflicting_and_cross_device_ack_cannot_overwrite_proof(self):
        row = self.queued()
        self.issue(row)
        self.queue.ack("phone", row["task_id"], "accepted", "schedule-1", QUEUED)
        for device, state, native, receipt in (
            ("other", "accepted", "schedule-1", QUEUED),
            ("phone", "not_sent", None, {"state": "not_sent", "stage": "relay"}),
            ("phone", "accepted", "wrong", {**QUEUED, "native_id": "wrong"}),
            ("phone", "accepted", "schedule-1", {"state": "accepted", "stage": "native_host"}),
        ):
            with self.subTest(device=device, state=state, native=native), self.assertRaises(BridgeError):
                self.queue.ack(device, row["task_id"], state, native, receipt)
        self.assertEqual(self.box.receipt_for("source-owner", "message-1")["native_id"], "schedule-1")

    def test_rejection_before_issue_is_recorded_without_text(self):
        row = self.queued()
        self.queue.ack("phone", row["task_id"], "not_sent", receipt={"state": "not_sent", "stage": "relay"})
        self.assertEqual(self.box.receipt_for("source-owner", "message-1")["state"], "not_sent")
        with self.box.connect() as db:
            self.assertIsNone(db.execute("SELECT text FROM device_deliveries").fetchone()[0])

    def test_sdk_cache_removal_does_not_remove_receipt_and_message_rejects_business_submit(self):
        row = self.queued()
        with self.assertRaises(BridgeError):
            self.box.submit(row["address"], "business answer", TARGET)
        self.issue(row)
        self.queue.ack("phone", row["task_id"], "accepted", "schedule-1", QUEUED)
        self.box.cleanup(now=time.time() + 601, removed_tasks=(row["task_id"],))
        self.assertEqual(self.box.receipt_for("source-owner", "message-1")["state"], "queued")

    def test_external_body_is_owned_and_preserves_independent_delivery_receipt(self):
        row = self.queued()
        self.issue(row)
        self.queue.ack("phone", row["task_id"], "accepted", "schedule-1", QUEUED)
        self.assertTrue(self.box.store_external_result(row["task_id"], "external answer"))
        self.assertEqual(self.box.pending_external_results()[0]["external_result"], "external answer")
        self.assertIsNone(self.box.external_result_for("another-owner", "message-1"))
        self.assertEqual(self.box.external_result_for("source-owner", "message-1")["text"], "external answer")
        self.box.mark_external_result_handled(row["task_id"])
        self.assertEqual(self.box.pending_external_results(), [])
        self.assertEqual(self.box.external_result_for("source-owner", "message-1")["state"], "handled")
        self.assertEqual(self.box.receipt_for("source-owner", "message-1")["state"], "queued")

    def test_external_body_replays_do_not_renew_expired_plaintext(self):
        row, _ = self.claim()
        with mock.patch("a2a_state.time.time", return_value=1000):
            self.box.store_external_result(row["task_id"], "external answer")
        with mock.patch("a2a_state.time.time", return_value=1200):
            self.assertFalse(self.box.store_external_result(row["task_id"], "external answer"))
            self.assertEqual(self.box.external_result_for("source-owner", "message-1")["at"], 1000)
        with mock.patch("a2a_state.time.time", return_value=1601):
            self.box.cleanup(now=1601)
            self.assertIsNone(self.box.external_result_for("source-owner", "message-1"))
            self.assertEqual(self.box.pending_external_results(), [])
            self.assertFalse(self.box.store_external_result(row["task_id"], "external answer"))
        stored = self.box.row(row["task_id"])
        self.assertIsNone(stored["external_result"])
        self.assertIsNotNone(stored["result_hash"])
        self.assertEqual(stored["external_result_state"], "expired")

    def test_external_body_cache_is_bounded_and_rejects_conflicting_or_unknown_records(self):
        row, _ = self.claim()
        self.box.store_external_result(row["task_id"], "first")
        with self.assertRaises(BridgeError):
            self.box.store_external_result(row["task_id"], "conflicting")
        with self.assertRaises(BridgeError):
            self.box.store_external_result(row["task_id"], "x" * 65537)
        with self.assertRaises(BridgeError):
            self.box.store_external_result("missing", "unknown")
        from a2a_state import Limits
        limited = Mailbox(self.tmp.name, Limits(max_completed_tasks=1))
        second, _ = self.claim("message-2")
        with self.assertRaises(BridgeError):
            limited.store_external_result(second["task_id"], "second")


class Response:
    def __init__(self, value):
        self.value = json.dumps(value).encode("utf8")
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass
    def read(self, count):
        return self.value[:count]


class ReceiverMessageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = time.time()
        self.requests = []
        self.native_calls = []
        self.fail_ack = False
        outer = self
        class Opener:
            def open(self, request, timeout=0):
                outer.requests.append(request)
                if request.full_url.endswith("/authorize"):
                    return Response({"allowed": True})
                if outer.fail_ack:
                    outer.fail_ack = False
                    return Response({"state": "rejected"})
                return Response({"state": "accepted"})
        local = {"platform": "minis", "device_id": "phone", "network": {
            "base_url": "https://paired.example.ts.net", "token": "fictional-test-token"}}
        def deliver(local, peer, text, delivery_id):
            self.native_calls.append((peer, json.loads(text), delivery_id))
            return {"state": "accepted", "scheduled_task_id": "schedule-1", "route": "follow-up"}
        self.receiver = Receiver(self.tmp.name, local, [SOURCE, TARGET], deliver, Opener(),
                                 clock=lambda: self.now)

    def message(self, text="check this", **updates):
        value = {"state": "delivery", "mode": "message", "delivery_id": str(uuid.uuid4()),
                 "peer": "Minis", "conversation_id": "target-chat", "expires_at": self.now + 60,
                 "source_ref": ["pc", "codex", "source-chat"],
                 "target_ref": ["phone", "minis", "target-chat"],
                 "message": business_payload(SOURCE, text)}
        return {**value, **updates}

    def test_queued_receipt_is_metadata_and_duplicate_only_retries_ack(self):
        delivery = self.message()
        self.fail_ack = True
        with self.assertRaises(BridgeError):
            self.receiver.process(delivery)
        result = self.receiver.process(delivery)
        self.assertEqual(result["receipt"], QUEUED)
        self.assertEqual(len(self.native_calls), 1)
        self.assertEqual(self.native_calls[0][1], business_payload(SOURCE, "check this"))
        acknowledgements = [json.loads(req.data) for req in self.requests if req.full_url.endswith("/ack")]
        self.assertEqual(len(acknowledgements), 2)
        self.assertEqual(acknowledgements[0], acknowledgements[1])
        self.assertEqual(acknowledgements[-1]["receipt"], QUEUED)
        self.assertNotIn("check this", json.dumps(acknowledgements))

    def test_source_mismatch_is_never_submitted(self):
        delivery = self.message(source_ref=["other", "codex", "source-chat"])
        self.assertEqual(self.receiver.process(delivery)["state"], "not_sent")
        self.assertEqual(self.native_calls, [])

    def test_source_alias_can_use_registered_casefold_equivalent(self):
        delivery = self.message()
        delivery["message"]["source_conversation"] = "codex"
        self.assertEqual(self.receiver.process(delivery)["receipt"], QUEUED)

    def test_restart_uncertainty_never_reenters_native_and_ack_retention_is_bounded(self):
        delivery = self.message()
        self.receiver._claim(delivery["delivery_id"], delivery)
        self.assertEqual(self.receiver.process(delivery)["state"], "uncertain")
        self.assertEqual(self.native_calls, [])
        with self.receiver._db() as db:
            db.execute("UPDATE receipts SET acked=0 WHERE delivery_id=?", (delivery["delivery_id"],))
        before = len(self.requests)
        self.now += 86461
        self.receiver.maintenance(force=True)
        self.assertEqual(len(self.requests), before)
        self.assertIsNone(self.receiver._receipt(delivery["delivery_id"]))

    def test_control_character_body_keeps_full_64k_support(self):
        text = "a" + "\x00" * 65535
        self.assertEqual(self.receiver.process(self.message(text))["receipt"], QUEUED)
        self.assertEqual(self.native_calls[0][1]["message"], text)

    def test_cleanup_checks_owned_native_completion_without_business_reply(self):
        delivery = self.message()
        self.receiver.process(delivery)
        with mock.patch.dict(sys.modules, {"minis": mock.Mock()}) as modules:
            cleanup = modules["minis"].cleanup_task
            cleanup.return_value = {"state": "removed"}
            self.receiver.maintenance(force=True)
            cleanup.assert_called_once_with("schedule-1", delivery["delivery_id"], "target-chat", expired=False)
        self.assertEqual(self.receiver._receipt(delivery["delivery_id"])["cleanup_done"], 1)


if __name__ == "__main__":
    unittest.main()
