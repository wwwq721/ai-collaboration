"""Routing and receipt checks without either real AI or network access."""
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))
import send
import workbuddy
import codex_ipc


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.local = {"machine_name": platform.node(), "device_id": "here"}
        self.book = {"version": 1, "peers": {
            "Codex": {"software": "codex", "device_id": "here", "conversation_id": "cx"},
            "WorkBuddy": {"software": "workbuddy", "device_id": "here", "conversation_id": "wb"},
            "LaterPhone": {"software": "minis", "device_id": "phone", "conversation_id": "mobile"},
        }}
        self.save()
        self.env = mock.patch.dict(os.environ, {"CODEX_THREAD_ID": "cx", "CODEBUDDY_SESSION_ID": ""})
        self.env.start()
        self.addCleanup(self.env.stop)

    def save(self):
        (self.root / "local.json").write_text(json.dumps(self.local), encoding="utf8")
        (self.root / "peers.json").write_text(json.dumps(self.book), encoding="utf8")

    def test_only_two_user_fields_and_correct_target(self):
        text = "中文\n引号\" ' $() ` %PATH%"
        with mock.patch.object(send, "invoke", return_value={"state": "accepted", "turn_id": "private"}) as api:
            result = send.run({"to": "workbuddy", "text": text}, self.root)
        self.assertEqual(result, {"state": "accepted", "to": "WorkBuddy"})
        args = api.call_args.args
        self.assertEqual(args[1]["conversation_id"], "wb")
        self.assertTrue(args[3].endswith(text))
        self.assertIn("回信给 Codex", args[3])
        self.assertEqual(api.call_args.kwargs["source"], "Codex")
        api.assert_called_once()

    def test_workbuddy_host_identity_needs_no_from_field(self):
        with mock.patch.dict(os.environ, {"CODEBUDDY_SESSION_ID": "wb", "CODEX_THREAD_ID": ""}), \
             mock.patch.object(send, "invoke", return_value={"state": "accepted"}) as api:
            send.run({"to": "Codex", "text": "回复"}, self.root)
        self.assertEqual(api.call_args.kwargs["source"], "WorkBuddy")
        self.assertEqual(api.call_args.args[1]["conversation_id"], "cx")

    def test_unknown_unsupported_or_disabled_peer_never_contacts_adapter(self):
        self.book["peers"]["Disabled"] = {
            "software": "codex", "device_id": "here", "conversation_id": "other", "enabled": False,
        }
        self.save()
        for alias in ("Unknown", "LaterPhone", "Disabled"):
            with self.subTest(alias=alias), mock.patch.object(send, "invoke") as api:
                with self.assertRaises(send.BridgeError):
                    send.run({"to": alias, "text": "不要发"}, self.root)
                api.assert_not_called()

    def test_new_chat_does_not_claim_original_chat_identity(self):
        with mock.patch.dict(os.environ, {"CODEX_THREAD_ID": "another-chat"}), mock.patch.object(send, "invoke") as api:
            with self.assertRaisesRegex(send.BridgeError, "未唯一登记"):
                send.run({"to": "WorkBuddy", "text": "消息"}, self.root)
            api.assert_not_called()

    def test_copying_local_config_to_different_computer_is_rejected(self):
        self.local["machine_name"] = "different-computer"
        self.save()
        with mock.patch.object(send, "invoke") as api:
            with self.assertRaisesRegex(send.BridgeError, "另一台电脑"):
                send.run({"to": "WorkBuddy", "text": "消息"}, self.root)
            api.assert_not_called()

    def test_ambiguous_receipt_not_retried_or_reported_as_not_sent(self):
        with mock.patch.object(send, "invoke", side_effect=send.BridgeError("超时", "uncertain")) as api:
            with self.assertRaises(send.BridgeError) as caught:
                send.run({"to": "WorkBuddy", "text": "消息"}, self.root)
        self.assertEqual(caught.exception.state, "uncertain")
        self.assertIn("本次消息号", str(caught.exception))
        api.assert_called_once()

    def test_unexpected_adapter_result_is_not_accepted(self):
        with mock.patch.object(send, "invoke", return_value={}) as api:
            with self.assertRaises(send.BridgeError) as caught:
                send.run({"to": "WorkBuddy", "text": "消息"}, self.root)
        self.assertEqual(caught.exception.state, "uncertain")
        api.assert_called_once()


class ReceiptTests(unittest.TestCase):
    def test_malformed_or_duplicate_native_receipt_is_uncertain(self):
        for output in ('{"event":broken', "", '{"event":"result","result":{}}\n' * 2):
            with self.subTest(output=output), self.assertRaises(workbuddy.BridgeError) as caught:
                workbuddy.parse_native_output(output)
            self.assertEqual(caught.exception.state, "uncertain")

    @unittest.skipUnless(shutil.which("node"), "Node not available")
    def test_native_host_rejection_after_send_is_uncertain_and_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            counter = root / "send-count"
            fake = root / "fake-sdk.cjs"
            fake.write_text(
                "function init_lib() {}\n"
                "const lib_exports={handshake:async()=>({sdk:{request:async(ch,cmd)=>{"
                "if(cmd.type.endsWith(':get'))return {payload:{success:true,result:{info:{id:'wb',state:'idle'}}}};"
                "require('fs').appendFileSync(" + json.dumps(str(counter)) + ",'sent\\n');"
                "return {payload:{success:false,error:'rejected'}};"
                "}}})};", encoding="utf8",
            )
            request = {"sdkFile": str(fake), "userId": "test", "clientId": "client",
                       "credentials": {}, "deviceId": "device", "conversationId": "wb",
                       "waitIdle": 0, "mode": "send", "text": "test", "messageId": "unique"}
            process = subprocess.run(
                ["node", str(SCRIPTS / "workbuddy_sync.cjs")],
                input=json.dumps(request), text=True, capture_output=True, timeout=10,
            )
            self.assertNotEqual(process.returncode, 0)
            event = json.loads(process.stdout)
            self.assertEqual(event["state"], "uncertain")
            self.assertEqual(counter.read_text().splitlines(), ["sent"])


class CodexIdleRaceTests(unittest.TestCase):
    def setUp(self):
        self.local = {"device_id": "here"}
        self.peer = {"device_id": "here", "conversation_id": "cx"}
        self.clock = 0.0
        self.sleeps = []
        self.client = codex_ipc.CodexIpc()
        self.client.connect = mock.Mock()
        self.client.discover_owner = mock.Mock(return_value="owner")
        self.client.close = mock.Mock()
        self.client.owners["cx"] = {"id": "owner", "supportsUntrustedAppInput": True}
        self.client.request = mock.Mock(return_value={"resultType": "success", "result": {
            "result": {"turn": {"id": "new-turn"}}}})

    @staticmethod
    def snapshot(state):
        return {"conversationState": {
            "resumeState": "resumed", "cwd": "C:\\fixture",
            "threadRuntimeStatus": {"type": "idle" if state == "mismatch" else state},
            "turns": [{"turnId": "old-turn", "items": [],
                       "status": "completed" if state == "idle" else "inProgress"}],
        }}

    def states(self, *states):
        sequence = iter(states)
        last = [states[-1]]

        def follow(thread):
            last[0] = next(sequence, last[0])
            change = self.snapshot(last[0])
            self.client.states[thread] = change
            return change

        self.client.follow = mock.Mock(side_effect=follow)

    def invoke(self, wait_idle):
        def sleep(seconds):
            self.sleeps.append(seconds)
            self.clock += seconds

        with mock.patch.object(codex_ipc, "CodexIpc", return_value=self.client), \
             mock.patch.object(send.time, "monotonic", side_effect=lambda: self.clock), \
             mock.patch.object(send.time, "sleep", side_effect=sleep):
            return send.codex_invoke(self.local, self.peer, "send", "body", None, "fixture", wait_idle)

    def test_user_arrival_between_idle_reads_waits_then_sends_once(self):
        # Outer read is idle, preflight sees the newly active user turn. All
        # retries remain reads; only the final ready state produces one RPC.
        self.states("idle", "active", "active", "idle", "idle")
        result = self.invoke(5)
        self.assertEqual(result["state"], "accepted")
        self.assertEqual(result["route"], "start")
        self.client.request.assert_called_once()
        self.assertEqual(self.client.request.call_args.args[0], "thread-follower-start-turn")
        self.assertEqual(self.sleeps, [2, 2])
        self.client.close.assert_called_once()

    def test_preflight_race_deadline_does_not_send_or_claim_uncertainty(self):
        self.states("idle", "active")
        with self.assertRaises(send.BridgeError) as caught:
            self.invoke(3)
        self.assertEqual(caught.exception.state, "not_sent")
        self.assertEqual(self.clock, 3)
        self.client.request.assert_not_called()

    def test_without_wait_idle_an_inconsistent_snapshot_is_not_retried(self):
        self.states("idle", "mismatch")
        with self.assertRaises(send.BridgeError) as caught:
            self.invoke(0)
        self.assertEqual(caught.exception.state, "not_sent")
        self.assertEqual(self.sleeps, [])
        self.client.request.assert_not_called()

    def test_missing_capability_is_not_retried_even_with_wait_idle(self):
        self.states("idle")
        self.client.owners["cx"]["supportsUntrustedAppInput"] = False
        with self.assertRaises(send.BridgeError) as caught:
            self.invoke(5)
        self.assertEqual(caught.exception.state, "not_sent")
        self.assertEqual(self.sleeps, [])
        self.client.request.assert_not_called()

    def test_rpc_timeout_remains_uncertain_and_is_never_retried(self):
        self.states("idle")
        self.client.request.side_effect = TimeoutError("ambiguous native response")
        with self.assertRaises(send.BridgeError) as caught:
            self.invoke(5)
        self.assertEqual(caught.exception.state, "uncertain")
        self.assertEqual(self.sleeps, [])
        self.client.request.assert_called_once()


if __name__ == "__main__":
    unittest.main()
