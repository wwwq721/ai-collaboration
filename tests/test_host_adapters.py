"""Registered native identities, helper isolation and uncertain-delivery checks."""
import asyncio
import json
from pathlib import Path
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import host_adapters as hosts
from workbuddy import BridgeError


class IdentityTests(unittest.TestCase):
    def setUp(self):
        self.local = {"device_id": "pc", "python": "native-python"}
        self.peers = {"codex": {"alias": "Codex", "software": "codex", "device_id": "pc",
                                "conversation_id": "same-chat"},
                      "phone": {"alias": "Phone", "software": "minis", "device_id": "phone",
                                "conversation_id": "same-chat"}}

    def test_complete_chat_ref_roundtrip_and_bad_fields(self):
        ref = hosts.ChatRef.from_peer(self.peers["codex"])
        self.assertEqual(ref.to_dict(), {"device_id": "pc", "software": "codex",
                                         "conversation_id": "same-chat"})
        for value in (None, "", "x" * 201):
            with self.subTest(value=value), self.assertRaises(BridgeError):
                hosts.ChatRef(value, "codex", "same-chat")

    def test_unknown_software_never_becomes_module_name(self):
        for software in ("a2a", "os", "custom.module", "Codex"):
            with self.subTest(software=software), self.assertRaises(BridgeError):
                hosts.get_adapter(self.local, software, self.peers)

    def test_current_chat_is_host_context_and_must_be_registered(self):
        adapter = hosts.get_adapter(self.local, "codex", self.peers)
        self.assertIsNone(adapter.current_chat({}))
        self.assertEqual(adapter.current_chat({"CODEX_THREAD_ID": "same-chat"}), "same-chat")
        with self.assertRaises(BridgeError):
            adapter.current_chat({"CODEX_THREAD_ID": "unregistered"})

    def test_minis_resolves_parent_using_native_host(self):
        local = {"device_id": "phone", "platform": "minis"}
        adapter = hosts.get_adapter(local, "minis", self.peers)
        with mock.patch("minis.resolve_conversation", return_value="same-chat") as resolver:
            self.assertEqual(adapter.current_chat({"MINIS_CHAT_SESSION_ID": "child"}), "same-chat")
        resolver.assert_called_once_with({"MINIS_CHAT_SESSION_ID": "child"})

    def test_ambiguous_or_disabled_chat_is_rejected(self):
        adapter = hosts.get_adapter(self.local, "codex", self.peers)
        self.peers["duplicate"] = {**self.peers["codex"], "alias": "Copy"}
        with self.assertRaises(BridgeError):
            adapter.current_chat({"CODEX_THREAD_ID": "same-chat"})
        del self.peers["duplicate"]
        self.peers["codex"]["enabled"] = False
        with self.assertRaises(BridgeError):
            adapter.current_chat({"CODEX_THREAD_ID": "same-chat"})

    def test_native_invoke_reuses_tested_functions_and_checks_device(self):
        adapter = hosts.get_adapter(self.local, "codex", self.peers)
        with mock.patch("send.codex_invoke", return_value={"state": "accepted"}) as native:
            adapter.invoke(self.peers["codex"], "send", "text", "id", "source", 8)
            native.assert_called_once_with(self.local, self.peers["codex"], "send", "text", "id", "source", 8)
        with mock.patch("send.codex_invoke") as native:
            with self.assertRaises(BridgeError):
                adapter.invoke({**self.peers["codex"], "device_id": "other"}, "status")
            native.assert_not_called()

    def test_persisted_book_shape_and_live_unknown_capabilities(self):
        book = {"version": 1, "peers": {"Codex": self.peers["codex"]}}
        adapter = hosts.get_adapter(self.local, "codex", book)
        self.assertEqual(adapter.current_chat({"CODEX_THREAD_ID": "same-chat"}), "same-chat")
        caps = adapter.capabilities()
        for name in ("available", "native_idempotency", "software_version", "verified_at"):
            self.assertIsNone(caps[name])
        self.assertTrue(caps["target_original_chat"])


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.root = Path("candidate")
        self.local = {"device_id": "pc", "python": "native-python"}
        self.peer = {"alias": "Codex", "software": "codex", "device_id": "pc", "conversation_id": "chat"}
        self.peers = {"codex": self.peer}

    def process(self, output, code=0):
        process = mock.Mock(returncode=code)
        process.communicate = mock.AsyncMock(return_value=(output, b"secret native stderr"))
        process.wait = mock.AsyncMock()
        return process

    async def test_windows_uses_native_interpreter_and_receipt_preserves_route(self):
        process = self.process(b'{"state":"accepted","route":"start","turn_id":"turn"}')
        with mock.patch.object(asyncio, "create_subprocess_exec", new=mock.AsyncMock(return_value=process)) as launch:
            result = await hosts.deliver_local(self.root, self.local, self.peers,
                hosts.ChatRef("pc", "codex", "chat"), "result", "notice-1", wait_idle=10)
        self.assertEqual(launch.call_args.args[:5], ("native-python", "-X", "utf8", "-B",
                                                   str(self.root / "scripts/send.py")))
        payload = json.loads(process.communicate.call_args.args[0])
        self.assertEqual(payload, {"mode": "native-deliver", "to": "Codex", "text": "result",
                                  "message_id": "notice-1", "source": "A2A结果", "wait_idle": 10})
        self.assertEqual(result["route"], "start")
        process.communicate.assert_awaited_once()

    async def test_different_device_or_software_with_same_chat_cannot_receive_notice(self):
        for origin in (hosts.ChatRef("other", "codex", "chat"),
                       hosts.ChatRef("pc", "minis", "chat"), {"device_id": "pc"}):
            with self.subTest(origin=origin), mock.patch.object(asyncio, "create_subprocess_exec") as launch:
                with self.assertRaises(BridgeError):
                    await hosts.deliver_local(self.root, self.local, self.peers, origin, "result", "id")
                launch.assert_not_called()

    async def test_bad_payload_rejected_before_native_process(self):
        for text, message_id in (("", "id"), ("x" * 70000, "id"), ("text", "id/path")):
            with self.subTest(text=len(text), message_id=message_id), \
                 mock.patch.object(asyncio, "create_subprocess_exec") as launch:
                with self.assertRaises(BridgeError):
                    await hosts.deliver_local(self.root, self.local, self.peers,
                        self.peer, text, message_id)
                launch.assert_not_called()

    async def test_uncertain_or_invalid_receipt_never_resends_or_leaks_output(self):
        for output, code, expected in ((b'{"state":"uncertain","error":"secret"}', 2, "uncertain"),
                                       (b'{"state":"not_sent","error":"secret"}', 2, "not_sent"),
                                       (b"secret", 0, "uncertain"), (b"[]", 0, "uncertain"),
                                       (b'{"state":"completed"}', 0, "uncertain")):
            with self.subTest(output=output):
                process = self.process(output, code)
                with mock.patch.object(asyncio, "create_subprocess_exec", new=mock.AsyncMock(return_value=process)) as launch:
                    with self.assertRaises(BridgeError) as caught:
                        await hosts.deliver_local(self.root, self.local, self.peers,
                            self.peer, "result", "notice")
                self.assertEqual(caught.exception.state, expected)
                self.assertNotIn("secret", str(caught.exception))
                launch.assert_awaited_once()
                process.communicate.assert_awaited_once()

    async def test_timeout_is_uncertain_and_kills_only_started_helper(self):
        process = self.process(b"")
        process.communicate.side_effect = TimeoutError()
        with mock.patch.object(asyncio, "create_subprocess_exec", new=mock.AsyncMock(return_value=process)) as launch:
            with self.assertRaises(BridgeError) as caught:
                await hosts.deliver_local(self.root, self.local, self.peers, self.peer, "result", "id")
        self.assertEqual(caught.exception.state, "uncertain")
        process.kill.assert_called_once()
        process.wait.assert_awaited_once()
        launch.assert_awaited_once()

    async def test_minis_uses_existing_native_without_sdk_in_system_python(self):
        local = {"device_id": "phone", "platform": "minis"}
        peer = {"alias": "Minis", "device_id": "phone", "software": "minis", "conversation_id": "session"}
        receipt = {"state": "accepted", "route": "follow-up", "scheduled_task_id": "schedule"}
        with mock.patch("minis.invoke", return_value=receipt) as native, \
             mock.patch.object(asyncio, "create_subprocess_exec") as launch:
            result = await hosts.deliver_local(self.root, local, {"minis": peer}, peer, "result", "notice")
        self.assertEqual(result, receipt)
        native.assert_called_once_with(local, peer, "send", "result", "notice", "A2A结果", 300)
        launch.assert_not_called()

    async def test_status_uses_read_only_worker_request(self):
        process = self.process(b'{"state":"idle"}')
        adapter = hosts.get_adapter(self.local, "codex", self.peers, self.root)
        with mock.patch.object(asyncio, "create_subprocess_exec", new=mock.AsyncMock(return_value=process)):
            self.assertEqual(await adapter.status("chat"), {"state": "idle"})
        self.assertEqual(json.loads(process.communicate.call_args.args[0])["mode"], "status")


if __name__ == "__main__":
    unittest.main()
