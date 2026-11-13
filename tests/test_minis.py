import json
from pathlib import Path
import sys
import unittest
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import minis
from workbuddy import BridgeError


def completed(value, returncode=0):
    return SimpleNamespace(stdout=json.dumps(value), stderr="private diagnostic", returncode=returncode)


def status(session_id, **fields):
    return {"ok": True, "tool": "minis-sessions-cli", "action": "status",
            "data": {"session_id": session_id, **fields}}


def created_task(task_id, label, session_id):
    return {
        "created": {
            "id": task_id, "label": label, "target": "APPEND_TO:" + session_id,
            "trigger": "once", "afterSec": 1, "enabled": True,
        },
        "delivery": "Delivery: follow-up",
        "deliveryTarget": {
            "target": "follow-up", "defaulted": False, "sessionId": session_id,
        },
    }


def listed_task(task_id="task-123", label="a2a-msg-1", session_id="root", **fields):
    task = {
        "id": task_id, "label": label, "target": "APPEND_TO:" + session_id,
        "trigger": "once", "enabled": False, "firedCount": 1,
        "lastFiredAt": 123456, "lastResultSessionId": session_id,
        "prompt": "do not retain or return this prompt",
    }
    task.update(fields)
    return {"tasks": [task], "count": 1}


class Runner:
    def __init__(self, *results):
        self.results = list(results)
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class MinisAdapterTests(unittest.TestCase):
    def test_sender_child_resolves_recursively_to_root(self):
        runner = Runner(
            completed(status("child", parent_session_id="parent")),
            completed(status("parent", parent_session_id="root")),
            completed(status("root")),
        )
        resolved = minis.resolve_conversation({"MINIS_CHAT_SESSION_ID": "child"}, runner)
        self.assertEqual(resolved, "root")
        self.assertEqual([call[0][3] for call in runner.calls], ["child", "parent", "root"])
        self.assertTrue(all(call[1]["timeout"] == 30 for call in runner.calls))

    def test_missing_or_invalid_current_identity_is_rejected_without_cli(self):
        for environ in ({}, {"MINIS_CHAT_SESSION_ID": "bad id"}, {"MINIS_CHAT_SESSION_ID": ""}):
            runner = Runner()
            with self.subTest(environ=environ), self.assertRaises(BridgeError):
                minis.resolve_conversation(environ, runner)
            self.assertEqual(runner.calls, [])

    def test_missing_parent_record_and_cycle_fail_closed(self):
        with self.assertRaises(BridgeError):
            minis.resolve_conversation(
                {"MINIS_CHAT_SESSION_ID": "child"},
                Runner(completed(status("child", parent_session_id="parent")),
                       completed(status("wrong"))),
            )
        with self.assertRaisesRegex(BridgeError, "循环"):
            minis.resolve_conversation(
                {"MINIS_CHAT_SESSION_ID": "one"},
                Runner(completed(status("one", parent_session_id="two")),
                       completed(status("two", parent_session_id="one"))),
            )

    def test_status_reports_only_explicit_busy_evidence(self):
        unknown = Runner(completed({"ok": True, "data": {"session_id": "root", "title": "Chat"}}))
        result = minis.invoke({}, {"conversation_id": "root"}, "status", runner=unknown)
        self.assertEqual(result, {"state": "ready", "exists": True})

        busy = Runner(completed(status("root", is_running=True)))
        result = minis.invoke({}, {"conversation_id": "root"}, "status", runner=busy)
        self.assertEqual(result, {"state": "ready", "exists": True, "busy": True})

    def test_target_hidden_child_is_rejected_without_schedule(self):
        runner = Runner(completed(status("child", parent_session_id="root")))
        with self.assertRaisesRegex(BridgeError, "隐藏子会话"):
            minis.invoke({}, {"conversation_id": "child"}, "send", "hello", "msg-1", runner=runner)
        self.assertEqual(len(runner.calls), 1)

    def test_send_schedules_explicit_follow_up_and_returns_task_id(self):
        runner = Runner(
            completed(status("root")),
            completed(created_task("task-123", "a2a-m-123", "root")),
        )
        result = minis.invoke(
            {"device_id": "phone"}, {"conversation_id": "root"}, "send",
            "【AI 来信】\n请处理", "m-123", source="laptop", wait_idle=10, runner=runner,
        )
        self.assertEqual(result, {
            "state": "accepted", "scheduled_task_id": "task-123", "route": "follow-up",
        })
        argv = runner.calls[1][0]
        self.assertEqual(argv[:7], ["minis-scheduled", "create", "--target", "follow-up",
                                    "--session", "root", "--after"])
        self.assertEqual(argv[7], "1s")
        self.assertIn("--label", argv)
        self.assertIn("a2a-m-123", argv)
        self.assertEqual(argv[argv.index("--prompt") + 1], "【AI 来信】\n请处理")
        self.assertEqual(runner.calls[1][1]["timeout"], 30)

    def test_ambiguous_schedule_results_are_uncertain_and_not_retried(self):
        for schedule_result in (
            completed({"ok": True, "created": {"id": "x"}}),
            completed(created_task("x", "wrong-label", "root")),
            completed(created_task("x", "a2a-msg", "other")),
            completed(created_task("x", "a2a-msg", "root"), returncode=1),
            RuntimeError("secret prompt details"),
        ):
            runner = Runner(completed(status("root")), schedule_result)
            with self.subTest(result=schedule_result), self.assertRaises(BridgeError) as caught:
                minis.invoke({}, {"conversation_id": "root"}, "send", "hello", "msg", runner=runner)
            self.assertEqual(caught.exception.state, "uncertain")
            self.assertNotIn("secret prompt", str(caught.exception))
            self.assertEqual(len(runner.calls), 2)

    def test_unreadable_or_mismatched_status_does_not_prove_session(self):
        for result in (completed({"ok": True, "data": {"title": "some chat"}}),
                       completed(status("other")),
                       completed(status("root"), returncode=1)):
            with self.subTest(result=result), self.assertRaises(BridgeError):
                minis.invoke({}, {"conversation_id": "root"}, "status", runner=Runner(result))

    def test_cleanup_removes_only_a_finished_exact_owned_task(self):
        runner = Runner(
            completed(status("root", is_running=False)),
            completed(listed_task()),
            completed({"deleted": "task-123"}),
        )
        result = minis.cleanup_task("task-123", "msg-1", "root", runner=runner)
        self.assertEqual(result, {"state": "removed"})
        self.assertEqual([call[0] for call in runner.calls], [
            ["minis-sessions-cli", "status", "--id", "root", "--compact"],
            ["minis-scheduled", "list", "--compact"],
            ["minis-scheduled", "delete", "--id", "task-123"],
        ])
        self.assertNotIn("prompt", result)

    def test_cleanup_does_not_delete_while_busy_or_unproven(self):
        for session_receipt in (status("root", is_running=True), status("root")):
            runner = Runner(completed(session_receipt))
            self.assertEqual(minis.cleanup_task("task-123", "msg-1", "root", runner=runner),
                             {"state": "pending"})
            self.assertEqual(len(runner.calls), 1)

    def test_cleanup_requires_exact_task_markers_and_completed_run(self):
        invalid_tasks = (
            listed_task(label="a2a-other"),
            listed_task(session_id="other"),
            listed_task(enabled=True),
            listed_task(firedCount=0),
            listed_task(lastFiredAt=None),
            listed_task(lastResultSessionId="other"),
            listed_task(trigger="loop"),
        )
        for task in invalid_tasks:
            with self.subTest(task={k: v for k, v in task["tasks"][0].items() if k != "prompt"}):
                runner = Runner(completed(status("root", is_running=False)),
                                completed({"tasks": [task["tasks"][0]], "count": 1}))
                self.assertEqual(minis.cleanup_task("task-123", "msg-1", "root", runner=runner),
                                 {"state": "pending"})
                self.assertEqual(len(runner.calls), 2)

    def test_cleanup_returns_missing_without_deleting_and_delete_errors_are_nonfatal(self):
        missing = Runner(completed(status("root", is_running=False)), completed({"tasks": [], "count": 0}))
        self.assertEqual(minis.cleanup_task("task-123", "msg-1", "root", runner=missing),
                         {"state": "missing"})
        self.assertEqual(len(missing.calls), 2)

        failed_delete = Runner(completed(status("root", is_running=False)), completed(listed_task()),
                               completed({"error": "private diagnostic"}, returncode=1))
        self.assertEqual(minis.cleanup_task("task-123", "msg-1", "root", runner=failed_delete),
                         {"state": "pending"})
        self.assertEqual(len(failed_delete.calls), 3)

    def test_expired_cleanup_cancels_only_exact_unfired_task(self):
        runner = Runner(
            completed(status("root", is_running=False)),
            completed(listed_task(enabled=True, firedCount=0, lastFiredAt=None,
                                  lastResultSessionId=None)),
            completed({"deleted": "task-123"}),
        )
        self.assertEqual(minis.cleanup_task("task-123", "msg-1", "root", runner=runner, expired=True),
                         {"state": "removed"})
        self.assertEqual(runner.calls[-1][0], ["minis-scheduled", "delete", "--id", "task-123"])

    def test_unexpired_unfired_and_expired_claimed_or_running_tasks_stay_pending(self):
        unfired = listed_task(enabled=True, firedCount=0, lastFiredAt=None,
                              lastResultSessionId=None)
        runner = Runner(completed(status("root", is_running=False)), completed(unfired))
        self.assertEqual(minis.cleanup_task("task-123", "msg-1", "root", runner=runner),
                         {"state": "pending"})
        self.assertEqual(len(runner.calls), 2)

        # The alarm has claimed the one-shot but its dispatch has not yet
        # completed; no lastFiredAt receipt means deletion could race a run.
        claimed = listed_task(enabled=False, firedCount=1, lastFiredAt=None,
                              lastResultSessionId=None)
        runner = Runner(completed(status("root", is_running=False)), completed(claimed))
        self.assertEqual(minis.cleanup_task("task-123", "msg-1", "root", runner=runner, expired=True),
                         {"state": "pending"})
        self.assertEqual(len(runner.calls), 2)

        runner = Runner(completed(status("root", is_running=True)))
        self.assertEqual(minis.cleanup_task("task-123", "msg-1", "root", runner=runner, expired=True),
                         {"state": "pending"})
        self.assertEqual(len(runner.calls), 1)

    def test_expiry_never_deletes_a_task_with_wrong_ownership_markers(self):
        for task in (listed_task(label="a2a-other", enabled=True, firedCount=0),
                     listed_task(session_id="other", enabled=True, firedCount=0),
                     listed_task(trigger="loop", enabled=True, firedCount=0)):
            with self.subTest(task=task["tasks"][0]):
                runner = Runner(completed(status("root", is_running=False)), completed(task))
                self.assertEqual(minis.cleanup_task("task-123", "msg-1", "root",
                                                    runner=runner, expired=True), {"state": "pending"})
                self.assertEqual(len(runner.calls), 2)


if __name__ == "__main__":
    unittest.main()
