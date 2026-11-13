"""Two-field sending still binds the real local chat across device roles."""
import json
import os
from pathlib import Path
import platform
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import send
import minis


class MobileRoutingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.local = {"platform": "minis", "machine_name": platform.node(), "device_id": "phone",
                      "network": {"base_url": "https://pc.example.ts.net", "token": "t"*40}}
        self.book = {"version": 1, "peers": {
            "Minis": {"software": "minis", "device_id": "phone", "conversation_id": "root-chat"},
            "Codex": {"software": "a2a", "device_id": "pc", "conversation_id": "cx",
                      "url": "https://pc.example.ts.net/agents/Codex"}}}

    def save(self):
        (self.root / "local.json").write_text(json.dumps(self.local), encoding="utf8")
        (self.root / "peers.json").write_text(json.dumps(self.book), encoding="utf8")

    def test_hidden_minis_sender_is_normalized_without_a_from_field(self):
        self.save()
        with mock.patch.dict(os.environ, {"MINIS_CHAT_SESSION_ID": "child", "CODEX_THREAD_ID": "", "CODEBUDDY_SESSION_ID": ""}), \
             mock.patch.object(minis, "resolve_conversation", return_value="root-chat"), \
             mock.patch("a2a_transport.invoke", return_value={"state": "accepted"}) as transport:
            send.run({"to": "Codex", "text": "question"}, self.root)
        self.assertEqual(transport.call_args.kwargs["source"], "root-chat")
        self.assertEqual(transport.call_args.args[2]["conversation_id"], "cx")

    def test_minis_reply_uses_network_and_does_not_create_a_local_mailbox(self):
        self.save()
        with mock.patch.dict(os.environ, {"MINIS_CHAT_SESSION_ID": "root-chat"}), \
             mock.patch.object(minis, "resolve_conversation", return_value="root-chat"), \
             mock.patch("device_receiver.remote_reply", return_value={"state": "accepted"}) as reply:
            send.run({"to": "A2A-abcdef123456", "text": "answer"}, self.root)
        self.assertEqual(reply.call_args.args[-1]["conversation_id"], "root-chat")
        self.assertFalse((self.root / "runtime/a2a-mailbox.db").exists())

    def test_unregistered_minis_chat_cannot_submit_another_chats_result(self):
        self.save()
        with mock.patch.dict(os.environ, {"MINIS_CHAT_SESSION_ID": "other"}), \
             mock.patch.object(minis, "resolve_conversation", return_value="other"), \
             mock.patch("device_receiver.remote_reply") as reply:
            with self.assertRaises(send.BridgeError):
                send.run({"to": "A2A-abcdef123456", "text": "answer"}, self.root)
        reply.assert_not_called()

    def test_desktop_minis_alias_uses_registered_device_via_its_local_a2a_service(self):
        self.local = {"machine_name": platform.node(), "device_id": "pc", "a2a": {
            "port": 8788, "tokens": {"local": "l"*40}, "device_tokens": {"phone": "p"*40}}}
        self.book["peers"]["Codex"] = {"software": "codex", "device_id": "pc", "conversation_id": "cx"}
        self.save()
        with mock.patch.dict(os.environ, {"MINIS_CHAT_SESSION_ID": "", "CODEX_THREAD_ID": "cx", "CODEBUDDY_SESSION_ID": ""}), \
             mock.patch("a2a_transport.invoke", return_value={"state": "accepted"}) as transport:
            send.run({"to": "Minis", "text": "question"}, self.root)
        target = transport.call_args.args[2]
        self.assertEqual(target["url"], "http://127.0.0.1:8788/agents/Minis")
        self.assertEqual(target["conversation_id"], "root-chat")
        self.assertTrue(target["local_a2a"])


if __name__ == "__main__":
    unittest.main()
