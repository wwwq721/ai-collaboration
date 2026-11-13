"""Independent regression checks; no real named pipe or model calls."""

import copy
import importlib.util
import io
import json
from pathlib import Path
import struct
import sys
import types
import unittest
from unittest import mock


class FakePipeError(OSError):
    pass


STUBS = {
    name: types.ModuleType(name)
    for name in ("win32con", "win32file", "win32pipe", "pywintypes")
}
STUBS["win32con"].GENERIC_READ = 1
STUBS["win32con"].GENERIC_WRITE = 2
STUBS["win32con"].OPEN_EXISTING = 3
STUBS["pywintypes"].error = FakePipeError
STUBS["win32file"].CreateFile = mock.Mock(side_effect=AssertionError("real pipe forbidden"))
STUBS["win32file"].ReadFile = mock.Mock(side_effect=AssertionError("real pipe forbidden"))
STUBS["win32file"].WriteFile = mock.Mock(side_effect=AssertionError("real pipe forbidden"))
STUBS["win32file"].CloseHandle = mock.Mock()
STUBS["win32pipe"].PeekNamedPipe = mock.Mock(side_effect=AssertionError("real pipe forbidden"))

SOURCE = Path(__file__).resolve().parent.parent / "scripts" / "codex_ipc.py"
SPEC = importlib.util.spec_from_file_location("codex_ipc_under_test", SOURCE)
ipc = importlib.util.module_from_spec(SPEC)
with mock.patch.dict(sys.modules, STUBS):
    SPEC.loader.exec_module(ipc)


TID = "thread-target"
CUID = "message-new"
TURN = "turn-new"
FIXTURE_OWNER = "fixture-owner"


def turn(ident=TURN, cuid=CUID, status="completed", text="fresh answer"):
    return {
        "turnId": ident,
        "status": status,
        "params": {"threadId": TID, "clientUserMessageId": cuid},
        "items": ([{"type": "agentMessage", "id": ident + "-answer",
                    "phase": "final_answer", "text": text}] if text is not None else []),
        "error": None,
    }


def snapshot(*turns, revision=1, runtime="idle"):
    return {
        "type": "snapshot",
        "revision": revision,
        "conversationState": {
            "id": TID,
            "resumeState": "resumed",
            "threadRuntimeStatus": {"type": runtime},
            "turns": [],
            "turnHistory": {
                "kind": "canonical",
                "history": {
                    "entitiesByKey": {f"tail:0:{t['turnId']}": t for t in turns},
                    "generation": 1,
                    "isComplete": False,
                    "islands": [],
                },
            },
        },
    }


def broadcast(change, tid=TID):
    return {
        "type": "broadcast", "method": "thread-stream-state-changed", "version": 11,
        "sourceClientId": FIXTURE_OWNER,
        "params": {"conversationId": tid, "hostId": "local", "change": change},
    }


class CanonicalHistoryTests(unittest.TestCase):
    def test_extract_completed_canonical_turn(self):
        self.assertEqual(ipc._extract_turn_text(snapshot(turn()), CUID), "fresh answer")

    def test_in_progress_with_final_message_does_not_complete(self):
        change = snapshot(turn(status="inProgress"), runtime="active")
        self.assertIsNone(ipc._extract_turn_text(change, CUID))

    def test_completed_without_assistant_text_is_not_fabricated_success(self):
        self.assertIsNone(ipc._extract_turn_text(snapshot(turn(text=None)), CUID))

    def test_other_message_in_same_thread_cannot_supply_old_answer(self):
        old = turn("turn-old", "message-old", text="old answer")
        self.assertIsNone(ipc._extract_turn_text(snapshot(old), CUID))

    def test_explicit_turn_id_cannot_fall_back_to_a_different_turn(self):
        wrong = turn("turn-wrong", CUID, text="wrong turn answer")
        self.assertIsNone(ipc._extract_turn_text(snapshot(wrong), CUID, turn_id=TURN))

    def test_existing_turn_id_can_be_used_for_accepted_steering(self):
        steered = turn(TURN, "original-message", text="steered answer")
        self.assertEqual(ipc._extract_turn_text(snapshot(steered), CUID, turn_id=TURN),
                         "steered answer")

    def test_steering_excludes_answers_that_existed_before_submission(self):
        steered = turn(TURN, "original-message", text="old final answer")
        steered["items"].append({"type": "agentMessage", "id": "answer-after-steer",
                                 "phase": "final_answer", "text": "new steered answer"})
        self.assertEqual(ipc._extract_turn_text(
            snapshot(steered), CUID, turn_id=TURN, exclude_item_ids=(TURN + "-answer",)),
            "new steered answer")

    def test_completed_old_turn_without_new_answer_is_not_steering_success(self):
        old = turn(TURN, "original-message", text="old final answer")
        self.assertIsNone(ipc._extract_turn_text(
            snapshot(old), CUID, turn_id=TURN, exclude_item_ids=(TURN + "-answer",)))

    def test_failed_turn_is_explicit_failure(self):
        failed = turn(status="failed", text=None)
        failed["error"] = {"message": "model request failed"}
        with self.assertRaises(RuntimeError):
            ipc._extract_turn_text(snapshot(failed), CUID)

    def test_interrupted_turn_is_explicit_failure(self):
        with self.assertRaises(RuntimeError):
            ipc._extract_turn_text(snapshot(turn(status="interrupted")), CUID)


class StateMergeTests(unittest.TestCase):
    def setUp(self):
        self.client = ipc.CodexIpc()
        self.client._followed.add(TID)
        self.client.owners[TID] = {"id": FIXTURE_OWNER}

    def test_snapshot_is_stored_with_revision(self):
        change = snapshot(turn(status="inProgress", text=None), revision=10)
        self.client._handle_frame(broadcast(change))
        self.assertEqual(self.client.revisions[TID], 10)
        self.assertEqual(self.client.states[TID], change)

    def test_patches_update_canonical_turn_and_append_answer(self):
        change = snapshot(turn(status="inProgress", text=None), revision=10)
        self.client._handle_frame(broadcast(copy.deepcopy(change)))
        path = ["turnHistory", "history", "entitiesByKey", "tail:0:turn-new"]
        self.client._handle_frame(broadcast({
            "type": "patches", "baseRevision": 10, "revision": 11,
            "patches": [
                {"op": "replace", "path": path + ["status"], "value": "completed"},
                {"op": "add", "path": path + ["items", 0], "value": {
                    "type": "agentMessage", "id": "new-answer", "text": "patch answer",
                    "phase": "final_answer"}},
            ],
        }))
        self.assertEqual(self.client.revisions[TID], 11)
        self.assertEqual(self.client.states[TID]["type"], "snapshot")
        self.assertEqual(ipc._extract_turn_text(self.client.states[TID], CUID), "patch answer")

    def test_mismatching_patch_revision_does_not_apply(self):
        change = snapshot(turn(status="inProgress", text=None), revision=10)
        self.client._handle_frame(broadcast(copy.deepcopy(change)))
        try:
            self.client._handle_frame(broadcast({
                "type": "patches", "baseRevision": 9, "revision": 11,
                "patches": [{"op": "replace", "path": ["resumeState"], "value": "wrong"}],
            }))
        except (RuntimeError, ValueError):
            pass
        retained = self.client.states.get(TID)
        if retained is not None:
            self.assertNotEqual(retained["conversationState"]["resumeState"], "wrong")

    def test_non_thread_broadcast_is_not_interpreted_as_snapshot(self):
        self.client._handle_frame({"type": "broadcast", "method": "client-status-changed",
                                   "params": {"clientId": "some-client", "status": "connected"}})
        self.assertEqual(self.client.states, {})

    def test_snapshot_from_wrong_owner_is_ignored(self):
        self.client._handle_frame(broadcast(snapshot(turn(), revision=10)))
        wrong = broadcast(snapshot(turn(text="wrong owner answer"), revision=11))
        wrong["sourceClientId"] = "different-owner"
        self.client._handle_frame(wrong)
        self.assertEqual(self.client.revisions[TID], 10)
        self.assertEqual(ipc._extract_turn_text(self.client.states[TID], CUID), "fresh answer")

    def test_snapshot_from_wrong_host_is_ignored(self):
        wrong = broadcast(snapshot(turn()))
        wrong["params"]["hostId"] = "remote-host"
        self.client._handle_frame(wrong)
        self.assertNotIn(TID, self.client.states)

    def test_new_protocol_version_from_valid_owner_invalidates_state(self):
        self.client._handle_frame(broadcast(snapshot(turn(), revision=10)))
        wrong = broadcast(snapshot(turn(), revision=11))
        wrong["version"] = 12
        with self.assertRaises(RuntimeError):
            self.client._handle_frame(wrong)
        self.assertNotIn(TID, self.client.states)


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.client = ipc.CodexIpc()
        self.client.handle = "fake-handle"
        self.client._followed.add(TID)
        self.client.owners[TID] = {"id": FIXTURE_OWNER}

    def test_response_does_not_discard_later_frames_in_same_batch(self):
        before = broadcast(snapshot(turn(status="inProgress", text=None), revision=10))
        after = broadcast(snapshot(turn(text="answer after response"), revision=11))
        response = {"type": "response", "requestId": "request-fixed", "resultType": "success",
                    "method": "test-method", "result": {"ok": True}}
        self.client._read_frames = mock.Mock(return_value=[before, response, after])
        self.client._write = mock.Mock()
        with mock.patch.object(ipc.uuid, "uuid4", return_value="request-fixed"):
            result = self.client.request("test-method", 1, {}, timeout_ms=1000)
        self.assertEqual(result["result"], {"ok": True})
        self.assertEqual(self.client.revisions[TID], 11)
        self.assertEqual(ipc._extract_turn_text(self.client.states[TID], CUID),
                         "answer after response")

    def test_failed_write_is_not_retried(self):
        self.client._write = mock.Mock(side_effect=OSError("write failed"))
        with self.assertRaises(OSError):
            self.client.request("test-method", 1, {}, timeout_ms=100)
        self.assertEqual(self.client._write.call_count, 1)

    def test_invalid_frame_lengths_are_rejected(self):
        for size in (0, ipc.MAX_FRAME + 1):
            with self.subTest(size=size):
                self.client._buf = b""
                with mock.patch.object(ipc.win32pipe, "PeekNamedPipe", return_value=(b"", 4, 0)), \
                     mock.patch.object(ipc.win32file, "ReadFile", return_value=(0, struct.pack("<I", size))):
                    with self.assertRaises((RuntimeError, ValueError)):
                        self.client._read_frames(0.1)

    def test_disconnect_is_not_silently_returned_as_no_frames(self):
        with mock.patch.object(ipc.win32pipe, "PeekNamedPipe", side_effect=FakePipeError("pipe closed")):
            with self.assertRaises((OSError, RuntimeError)):
                self.client._read_frames(0.1)


class WaitTests(unittest.TestCase):
    def setUp(self):
        self.client = ipc.CodexIpc()
        self.client.follow = mock.Mock()

    def test_wait_timeout_is_failure_not_none(self):
        with self.assertRaises(TimeoutError):
            self.client.wait_for_turn(TID, CUID, deadline_s=0)

    def test_wait_returns_only_target_answer(self):
        self.client.states[TID] = snapshot(
            turn("old-turn", "old-message", text="old answer"), turn(text="target answer"))
        self.assertEqual(self.client.wait_for_turn(TID, CUID, deadline_s=1, turn_id=TURN),
                         "target answer")

    def test_wait_propagates_failed_status(self):
        self.client.states[TID] = snapshot(turn(status="failed", text=None))
        with self.assertRaises(RuntimeError):
            self.client.wait_for_turn(TID, CUID, deadline_s=1, turn_id=TURN)

    def test_wait_does_not_accept_pre_submission_answer_in_steered_turn(self):
        self.client.states[TID] = snapshot(turn(TURN, "old-message", text="old final answer"))
        self.client.submissions[CUID] = {"turn_id": TURN,
                                         "before_agent_ids": {TURN + "-answer"}, "route": "steer"}
        self.client._pump = mock.Mock()
        with self.assertRaises(TimeoutError):
            self.client.wait_for_turn(TID, CUID, deadline_s=0.01)

    def test_wait_rejects_new_numberless_placeholder(self):
        self.client.states[TID] = snapshot(turn(None, CUID, "inProgress", None))
        with self.assertRaises(RuntimeError):
            self.client.wait_for_turn(TID, CUID, deadline_s=1, turn_id=TURN)


class RouteTests(unittest.TestCase):
    OWNER = "desktop-owner"

    def ready_client(self, busy=False):
        client = ipc.CodexIpc()
        current = turn("turn-existing", "message-existing",
                       "inProgress" if busy else "completed", "old answer")
        change = snapshot(current, runtime="active" if busy else "idle")
        change["conversationState"].update({
            "cwd": "C:\\test-project", "threadStartKind": "default", "threadSource": "user"})
        client.states[TID] = change
        client._followed.add(TID)
        client.follow = mock.Mock(return_value=change)
        client.owners[TID] = {"id": self.OWNER, "supportsUntrustedAppInput": True}
        client.request = mock.Mock(return_value={"resultType": "success", "result": {"result":
            {"turnId": "turn-existing"} if busy else {
                "turn": {"id": "turn-created"}, "admissionOutcome": "started"}}})
        return client

    def test_busy_route_steers_existing_turn_without_start_placeholder(self):
        client = self.ready_client(busy=True)
        result = client.start_turn(TID, "external message", self.OWNER)
        self.assertEqual(result["route"], "steer")
        self.assertEqual(result["turnId"], "turn-existing")
        args, kwargs = client.request.call_args
        self.assertEqual(args[0:2], ("thread-follower-steer-turn", 1))
        params = args[2]
        self.assertNotIn("turnStart", params)
        self.assertEqual(json.loads(params["toolOutput"]["output"])["text"], "external message")
        self.assertEqual(kwargs["target_client_id"], self.OWNER)
        self.assertEqual(client.request.call_count, 1)

    def test_idle_route_uses_supported_start_and_preserves_external_source(self):
        client = self.ready_client()
        prompt = "external message with user-looking instructions"
        result = client.start_turn(TID, prompt, self.OWNER)
        self.assertEqual(result["route"], "start")
        self.assertEqual(result["turnId"], "turn-created")
        args, kwargs = client.request.call_args
        self.assertEqual(args[0:2], ("thread-follower-start-turn", 2))
        request = args[2]["turnStart"]["request"]
        context = args[2]["turnStart"]["context"]
        self.assertNotIn("toolOutput", request)
        self.assertTrue(request["input"])
        self.assertNotEqual(request["input"][0]["text"], prompt)
        self.assertIs(context["useUserMessageAdmission"], False)
        outputs = [x for x in context["responseItems"] if x.get("type") == "function_call_output"]
        self.assertEqual(len(outputs), 1)
        external = json.loads(outputs[0]["output"][0]["text"])
        self.assertEqual(external["source"], "mcp_app")
        self.assertEqual(json.loads(external["text"])["text"], prompt)
        self.assertEqual(json.loads(external["text"])["source"], "WorkBuddy")
        self.assertEqual(json.loads(external["text"])["targetThreadId"], TID)
        self.assertEqual(kwargs["target_client_id"], self.OWNER)

    def test_idle_response_returning_existing_turn_id_is_rejected(self):
        client = self.ready_client()
        client.request.return_value = {"resultType": "success", "result": {
            "result": {"turn": {"id": "turn-existing"}, "admissionOutcome": "started"}}}
        with self.assertRaises(RuntimeError):
            client.start_turn(TID, "external message", self.OWNER)
        self.assertEqual(client.request.call_count, 1)

    def test_idle_standard_start_response_without_admission_outcome_is_accepted(self):
        client = self.ready_client()
        client.request.return_value = {"resultType": "success", "result": {
            "result": {"turn": {"id": "turn-created"}}}}
        result = client.start_turn(TID, "external message", self.OWNER)
        self.assertEqual(result["turnId"], "turn-created")
        self.assertEqual(client.request.call_count, 1)

    def test_idle_state_race_never_accepts_steered_or_duplicate_admission(self):
        for admission in ("steered", "duplicate"):
            with self.subTest(admission=admission):
                client = self.ready_client()
                client.request.return_value = {"resultType": "success", "result": {
                    "result": {"turn": {"id": "turn-created"}, "admissionOutcome": admission}}}
                with self.assertRaises(RuntimeError):
                    client.start_turn(TID, "external message", self.OWNER)
                self.assertEqual(client.request.call_count, 1)

    def test_idle_standard_start_returning_existing_turn_is_not_accepted(self):
        client = self.ready_client()
        client.request.return_value = {"resultType": "success", "result": {
            "result": {"turn": {"id": "turn-existing"}}}}
        with self.assertRaises(RuntimeError):
            client.start_turn(TID, "external message", self.OWNER)
        self.assertEqual(client.request.call_count, 1)

    def test_response_from_different_desktop_owner_is_not_accepted(self):
        client = self.ready_client()
        client.request.return_value = {"resultType": "success", "handledByClientId": "other-owner",
                                       "result": {"result": {"turn": {"id": "turn-created"}}}}
        with self.assertRaises(RuntimeError):
            client.start_turn(TID, "external message", self.OWNER)
        self.assertEqual(client.request.call_count, 1)

    def test_idle_durable_thread_is_not_sent_to_standard_start(self):
        client = self.ready_client()
        client.states[TID]["conversationState"]["mode"] = "durable"
        with self.assertRaises(RuntimeError):
            client.start_turn(TID, "external message", self.OWNER)
        client.request.assert_not_called()

    def test_numberless_placeholder_prevents_any_send(self):
        client = self.ready_client(busy=True)
        client.states[TID]["conversationState"]["turnHistory"]["history"]["entitiesByKey"] = {
            "local-placeholder": turn(None, CUID, "inProgress", None)}
        with self.assertRaises(RuntimeError):
            client.start_turn(TID, "external message", self.OWNER)
        client.request.assert_not_called()

    def test_inconsistent_runtime_and_turn_state_prevent_any_send(self):
        client = self.ready_client(busy=True)
        client.states[TID]["conversationState"]["threadRuntimeStatus"] = {"type": "idle"}
        with self.assertRaises(RuntimeError):
            client.start_turn(TID, "external message", self.OWNER)
        client.request.assert_not_called()

    def test_unready_state_prevents_any_send(self):
        client = self.ready_client()
        client.states[TID]["conversationState"]["resumeState"] = "needs_resume"
        with self.assertRaises(RuntimeError):
            client.start_turn(TID, "external message", self.OWNER)
        client.request.assert_not_called()

    def test_missing_untrusted_capability_prevents_any_send(self):
        client = self.ready_client()
        client.owners[TID]["supportsUntrustedAppInput"] = False
        with self.assertRaises(RuntimeError):
            client.start_turn(TID, "external message", self.OWNER)
        client.request.assert_not_called()

    def test_absent_fresh_snapshot_prevents_any_send(self):
        client = self.ready_client()
        client.follow.return_value = None
        with self.assertRaises(RuntimeError):
            client.start_turn(TID, "external message", self.OWNER)
        client.request.assert_not_called()

    def test_steering_race_failure_does_not_fall_back_or_retry(self):
        client = self.ready_client(busy=True)
        client.request.return_value = {"resultType": "error", "error": "active turn already ended"}
        with self.assertRaises(RuntimeError):
            client.start_turn(TID, "external message", self.OWNER)
        self.assertEqual(client.request.call_count, 1)
        self.assertEqual(client.request.call_args.args[0], "thread-follower-steer-turn")

    def test_uncertain_request_timeout_does_not_retry(self):
        client = self.ready_client()
        client.request.side_effect = TimeoutError("unknown delivery status")
        with self.assertRaises(TimeoutError):
            client.start_turn(TID, "external message", self.OWNER)
        self.assertEqual(client.request.call_count, 1)

    def test_response_without_turn_id_is_not_submission_success(self):
        for busy in (False, True):
            with self.subTest(busy=busy):
                client = self.ready_client(busy)
                client.request.return_value = {"resultType": "success", "result": {"result": {}}}
                with self.assertRaises(RuntimeError):
                    client.start_turn(TID, "external message", self.OWNER)
                self.assertEqual(client.request.call_count, 1)


    def test_registered_sender_and_delivery_id_survive_native_wrapper(self):
        client = self.ready_client()
        delivery = "00000000-0000-4000-8000-000000000001"
        result = client.start_turn(TID, "reply", self.OWNER,
                                   source="Another AI", delivery_id=delivery)
        items = client.request.call_args.args[2]["turnStart"]["context"]["responseItems"]
        output = next(x for x in items if x["type"] == "function_call_output")
        external = json.loads(json.loads(output["output"][0]["text"])["text"])
        self.assertEqual(external["source"], "Another AI")
        self.assertEqual(external["deliveryId"], delivery)
        self.assertEqual(result["clientUserMessageId"], delivery)

    def test_wait_idle_policy_does_not_steer_a_chat_that_became_busy(self):
        client = self.ready_client(busy=True)
        with self.assertRaises(RuntimeError):
            client.start_turn(TID, "reply", self.OWNER, require_idle=True)
        client.request.assert_not_called()

    def test_idle_race_is_definitely_not_sent_and_can_refresh_reads(self):
        client = self.ready_client(busy=True)
        with self.assertRaises(ipc.NotSentError) as caught:
            client.start_turn(TID, "reply", self.OWNER, require_idle=True)
        self.assertTrue(caught.exception.retryable)
        client.request.assert_not_called()

    def test_snapshot_failure_is_not_an_ambiguous_native_send(self):
        client = self.ready_client()
        client.follow.side_effect = TimeoutError("readonly snapshot timed out")
        with self.assertRaises(ipc.NotSentError) as caught:
            client.start_turn(TID, "reply", self.OWNER)
        self.assertFalse(caught.exception.retryable)
        self.assertIsInstance(caught.exception.__cause__, TimeoutError)
        client.request.assert_not_called()

    def test_bad_delivery_id_is_rejected_before_native_request(self):
        client = self.ready_client()
        with self.assertRaises(ipc.NotSentError):
            client.start_turn(TID, "reply", self.OWNER, delivery_id="invalid-id")
        client.request.assert_not_called()

    def test_error_after_native_request_is_never_not_sent(self):
        client = self.ready_client()
        client.request.return_value = {"resultType": "error", "error": "unknown delivery"}
        with self.assertRaises(RuntimeError) as caught:
            client.start_turn(TID, "reply", self.OWNER)
        self.assertNotIsInstance(caught.exception, ipc.NotSentError)
        client.request.assert_called_once()

class CommandLineTests(unittest.TestCase):
    def test_missing_thread_is_rejected_before_connecting_to_any_chat(self):
        with mock.patch.object(ipc, "CodexIpc") as client, \
             mock.patch("sys.stderr", io.StringIO()):
            with self.assertRaises(SystemExit) as result:
                ipc.main([])
        self.assertEqual(result.exception.code, 2)
        client.assert_not_called()

    def fake_client(self):
        client = mock.Mock()
        client.connect.return_value = "client"
        client.discover_owner.return_value = "owner"
        client.follow.return_value = snapshot(turn())
        client.owners = {TID: {"id": "owner", "supportsUntrustedAppInput": True}}
        client.start_turn.return_value = {"turnId": TURN, "clientUserMessageId": CUID,
                                           "ownerClientId": "owner", "route": "start"}
        return client

    def test_wait_timeout_exits_nonzero_and_never_prints_completed_true(self):
        client = self.fake_client()
        client.wait_for_turn.side_effect = TimeoutError("no confirmed reply")
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(ipc, "CodexIpc", return_value=client), \
             mock.patch("sys.stdout", stdout), mock.patch("sys.stderr", stderr):
            result = ipc.main([TID, "--send", "--text", "test message", "--timeout", "0.01"])
        self.assertNotEqual(result, 0)
        lines = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertFalse(any(row.get("completed") is True for row in lines))
        self.assertIn("no confirmed reply", stderr.getvalue())
        client.close.assert_called_once()

    def test_default_cli_is_read_only(self):
        client = self.fake_client()
        with mock.patch.object(ipc, "CodexIpc", return_value=client), \
             mock.patch("sys.stdout", io.StringIO()), mock.patch("sys.stderr", io.StringIO()):
            result = ipc.main([TID])
        self.assertEqual(result, 0)
        client.start_turn.assert_not_called()
        client.wait_for_turn.assert_not_called()
        client.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
