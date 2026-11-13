"""Host-owned per-call identity never uses request JSON or shared process state."""
from collections import UserDict
from concurrent.futures import ThreadPoolExecutor
import copy
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import threading
from types import MappingProxyType
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import send
import a2a_transport


class SenderContextTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.local = {"machine_name": platform.node(), "device_id": "fictional-pc",
                      "a2a": {"message_delivery": True, "port": 8788}}
        self.book = {"version": 1, "peers": {
            "Codex": {"software": "codex", "device_id": "fictional-pc", "conversation_id": "cx", "verified": True},
            "BuddyOne": {"software": "workbuddy", "device_id": "fictional-pc", "conversation_id": "wb-one", "verified": True},
            "BuddyTwo": {"software": "workbuddy", "device_id": "fictional-pc", "conversation_id": "wb-two", "verified": True},
        }}
        self.save()

    def save(self):
        (self.root / "local.json").write_text(json.dumps(self.local), encoding="utf8")
        (self.root / "peers.json").write_text(json.dumps(self.book), encoding="utf8")

    def run_send(self, context, request=None):
        return send.run(request or {"to": "Codex", "text": "hello"}, self.root, sender_context=context)

    def test_parallel_chats_keep_each_call_identity_and_ignore_codex_process_environment(self):
        barrier = threading.Barrier(2)

        def transport(root, local, target, mode, text, **kwargs):
            barrier.wait(timeout=5)
            return {"source": kwargs["source"], "origin": kwargs["origin"]}

        contexts = [{"CODEBUDDY_SESSION_ID": "wb-one"}, {"CODEBUDDY_SESSION_ID": "wb-two"}]
        originals = copy.deepcopy(contexts)
        with mock.patch.dict(os.environ, {"CODEX_THREAD_ID": "cx", "CODEBUDDY_SESSION_ID": "process-wrong"}), \
             mock.patch("a2a_transport.invoke", side_effect=transport):
            before = dict(os.environ)
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(self.run_send, contexts))
            self.assertEqual(dict(os.environ), before)
        self.assertEqual([result["source"] for result in results], ["wb-one", "wb-two"])
        self.assertEqual([result["origin"]["alias"] for result in results], ["BuddyOne", "BuddyTwo"])
        self.assertEqual(contexts, originals)

    def test_context_accepts_mapping_types_but_rejects_empty_invalid_or_ambiguous_values(self):
        with mock.patch("a2a_transport.invoke", return_value={"state": "accepted"}) as transport:
            for context in (UserDict({"CODEBUDDY_SESSION_ID": "wb-one"}), MappingProxyType({"CODEBUDDY_SESSION_ID": "wb-two"})):
                self.assertEqual(self.run_send(context)["state"], "accepted")
            transport.reset_mock()
            invalid = [None, {}, [], "wb-one", {"CODEBUDDY_SESSION_ID": ""},
                       {"CODEBUDDY_SESSION_ID": "  "}, {"CODEBUDDY_SESSION_ID": 1},
                       {"CODEBUDDY_SESSION_ID": "wb-one\n"}, {"CODEBUDDY_SESSION_ID": "x" * 513},
                       {"CODEBUDDY_SESSION_ID": "wb-one", "PATH": "not-identity"},
                       {1: "wb-one"}, {"CODEBUDDY_SESSION_ID": "wb-one", "CODEX_THREAD_ID": "cx"}]
            with mock.patch.dict(os.environ, {"CODEX_THREAD_ID": "cx"}):
                for context in invalid:
                    with self.subTest(context=context), self.assertRaises(send.BridgeError):
                        self.run_send(context)
            transport.assert_not_called()

    def test_unknown_disabled_other_device_and_duplicate_sources_are_rejected(self):
        for case in ("unknown", "disabled", "other-device", "duplicate"):
            with self.subTest(case=case):
                book = copy.deepcopy(self.book)
                if case == "disabled":
                    book["peers"]["BuddyOne"]["enabled"] = False
                elif case == "other-device":
                    book["peers"]["BuddyOne"]["device_id"] = "other-pc"
                elif case == "duplicate":
                    book["peers"]["OtherAlias"] = copy.deepcopy(book["peers"]["BuddyOne"])
                # Managed registration normalization must not resurrect a disabled sender.
                local = {**self.local, "a2a": {**self.local["a2a"], "managed_ui": True}}
                (self.root / "local.json").write_text(json.dumps(local), encoding="utf8")
                (self.root / "peers.json").write_text(json.dumps(book), encoding="utf8")
                with mock.patch("a2a_transport.invoke") as transport, self.assertRaises(send.BridgeError):
                    self.run_send({"CODEBUDDY_SESSION_ID": "unregistered" if case == "unknown" else "wb-one"})
                transport.assert_not_called()

    def test_json_identity_fields_cannot_supply_or_replace_host_context(self):
        request = {"to": "Codex", "text": "hello", "sender_context": {"CODEBUDDY_SESSION_ID": "wb-two"},
                   "origin": {"conversation_id": "wb-two"}, "from": "BuddyTwo"}
        with mock.patch.dict(os.environ, {"CODEX_THREAD_ID": "cx", "CODEBUDDY_SESSION_ID": ""}), \
             mock.patch("a2a_transport.invoke", return_value={"state": "accepted"}) as transport:
            self.run_send({"CODEBUDDY_SESSION_ID": "wb-one"}, request.copy())
            self.assertEqual(transport.call_args.kwargs["origin"]["alias"], "BuddyOne")
            transport.reset_mock()
            with self.assertRaises(send.BridgeError):
                self.run_send({}, request.copy())
            transport.assert_not_called()
            send.run(request.copy(), self.root)
            self.assertEqual(transport.call_args.kwargs["origin"]["alias"], "Codex")

    def test_explicit_as_is_refused_with_trusted_context_even_in_legacy_mode(self):
        self.local["a2a"]["message_delivery"] = False
        self.save()
        with mock.patch.object(send, "invoke") as native, mock.patch("a2a_transport.invoke") as transport:
            with self.assertRaises(send.BridgeError):
                self.run_send({"CODEBUDDY_SESSION_ID": "wb-one"}, {"to": "Codex", "text": "hello", "as": "BuddyTwo"})
            native.assert_not_called()
            transport.assert_not_called()

    def test_status_uses_bound_sender_and_reserved_legacy_addresses_are_refused(self):
        context = {"CODEBUDDY_SESSION_ID": "wb-two"}
        with mock.patch("a2a_transport.invoke", return_value={"state": "ready"}) as transport:
            self.run_send(context, {"mode": "status", "to": "Codex"})
            self.assertEqual(transport.call_args.kwargs["origin"]["alias"], "BuddyTwo")
            self.assertEqual(transport.call_args.kwargs["sender_context"], context)
        for network in (False, True):
            if network:
                self.local["network"] = {"base_url": "https://fictional.example", "token": "fictional-token"}
                self.save()
            with mock.patch("device_receiver.remote_reply") as reply, mock.patch("a2a_state.Mailbox.submit") as submit:
                for alias in ("A2A-123456789abc", "a2a-123456789abc", "A2a-not-even-an-address"):
                    with self.subTest(network=network, alias=alias), self.assertRaises(send.BridgeError):
                        self.run_send(context, {"to": alias, "text": "legacy result"})
                reply.assert_not_called()
                submit.assert_not_called()
        for request in ({"mode": "list"}, {"mode": "status", "to": "Codex"}):
            with self.subTest(request=request), self.assertRaises(send.BridgeError):
                self.run_send({"CODEBUDDY_SESSION_ID": "unknown"}, request)

    def test_current_config_must_still_enable_messages_and_verify_source_for_every_operation(self):
        for operation in ("send", "status", "list"):
            for revoked in ("message-mode", "source-verification"):
                with self.subTest(operation=operation, revoked=revoked):
                    local = copy.deepcopy(self.local)
                    book = copy.deepcopy(self.book)
                    if revoked == "message-mode":
                        local["a2a"]["message_delivery"] = False
                    else:
                        book["peers"]["BuddyOne"]["verified"] = False
                    (self.root / "local.json").write_text(json.dumps(local), encoding="utf8")
                    (self.root / "peers.json").write_text(json.dumps(book), encoding="utf8")
                    with mock.patch.object(send, "invoke") as native, mock.patch("a2a_transport.invoke") as transport:
                        with self.assertRaises(send.BridgeError):
                            self.run_send({"CODEBUDDY_SESSION_ID": "wb-one"}, {"mode": operation, "to": "Codex", "text": "hello"})
                        native.assert_not_called()
                        transport.assert_not_called()

    def _real_child_transport(self, context, seen):
        """Run the real SDK child's identity check in status mode; no network call is needed."""
        from collaboration_client import central_peer
        local, peers = send.load_config(self.root)
        target = central_peer(local, peers["codex"])
        origin = send.sender_at(local, peers, environ=context, strict=True)
        return a2a_transport.invoke(self.root, local, target, "status", origin=origin,
                                    source=origin["conversation_id"], message_mode=True, sender_context=context)

    def _child_runner(self, seen):
        real_run = subprocess.run
        script = ("import sys,json,asyncio; from pathlib import Path; "
                  "sys.path.insert(0," + repr(str(Path(send.__file__).parent)) + "); "
                  "import a2a_client; r=json.loads(sys.stdin.read()); "
                  "h=type('Http',(),{'headers':{}})(); "
                  "out=asyncio.run(a2a_client.exchange_message(r,None,h)); "
                  "out['source']=r['origin']['conversation_id']; print(json.dumps(out))")

        def launch(args, **kwargs):
            seen.append(copy.deepcopy(kwargs))
            return real_run([sys.executable, "-X", "utf8", "-B", "-c", script], **kwargs)
        return launch

    def _prepare_child(self):
        self.local["a2a"]["chat_tokens"] = {"BuddyOne": "fictional-one-token", "BuddyTwo": "fictional-two-token"}
        self.save()
        # The actual receiver creates this schema before ordinary concurrent sends.
        from result_watcher import PendingResults
        PendingResults(self.root)

    def test_transport_child_gets_current_context_when_parent_identity_is_missing_or_stale(self):
        self._prepare_child()
        for stale in ({}, {"CODEX_THREAD_ID": "cx", "CODEBUDDY_SESSION_ID": "wrong-buddy",
                           "MINIS_CHAT_SESSION_ID": "wrong-minis"}):
            seen = []
            identity_names = send._SENDER_IDENTITY_KEYS
            clean = {key: value for key, value in os.environ.items() if key.upper() not in identity_names}
            clean.update(stale)
            with self.subTest(stale=bool(stale)), mock.patch.dict(os.environ, clean, clear=True), \
                 mock.patch.object(Path, "is_file", return_value=True), \
                 mock.patch("a2a_transport.subprocess.run", side_effect=self._child_runner(seen)):
                before = dict(os.environ)
                result = self._real_child_transport({"CODEBUDDY_SESSION_ID": "wb-one"}, seen)
                self.assertEqual(result["source"], "wb-one")
                self.assertEqual(result["state"], "ready")
                self.assertEqual(dict(os.environ), before)
            env = seen[0]["env"]
            self.assertEqual(env["CODEBUDDY_SESSION_ID"], "wb-one")
            self.assertNotIn("CODEX_THREAD_ID", env)
            self.assertNotIn("MINIS_CHAT_SESSION_ID", env)
            request = json.loads(seen[0]["input"])
            self.assertNotIn("sender_context", request)

    def test_parallel_real_transport_children_keep_each_identity_without_mutating_parent(self):
        self._prepare_child()
        seen = []
        contexts = [{"CODEBUDDY_SESSION_ID": "wb-one"}, {"CODEBUDDY_SESSION_ID": "wb-two"}]
        with mock.patch.dict(os.environ, {"CODEX_THREAD_ID": "cx", "MINIS_CHAT_SESSION_ID": "stale"}), \
             mock.patch.object(Path, "is_file", return_value=True), \
             mock.patch("a2a_transport.subprocess.run", side_effect=self._child_runner(seen)):
            before = dict(os.environ)
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda context: self._real_child_transport(context, seen), contexts))
            self.assertEqual(dict(os.environ), before)
        self.assertEqual([r["source"] for r in results], ["wb-one", "wb-two"])
        self.assertEqual({call["env"]["CODEBUDDY_SESSION_ID"] for call in seen}, {"wb-one", "wb-two"})
        self.assertTrue(all("CODEX_THREAD_ID" not in call["env"] and "MINIS_CHAT_SESSION_ID" not in call["env"] for call in seen))

    def test_transport_without_internal_context_keeps_cli_inheritance(self):
        self._prepare_child()
        from collaboration_client import central_peer
        local, peers = send.load_config(self.root)
        process = subprocess.CompletedProcess([], 0, '{"state":"ready"}', '')
        with mock.patch.object(Path, "is_file", return_value=True), \
             mock.patch("a2a_transport.subprocess.run", return_value=process) as launch:
            a2a_transport.invoke(self.root, local, central_peer(local, peers["codex"]), "status",
                                 origin=peers["buddyone"], source="wb-one", message_mode=True)
        self.assertNotIn("env", launch.call_args.kwargs)

    def test_real_child_still_rejects_origin_that_disagrees_with_trusted_environment(self):
        self._prepare_child()
        from collaboration_client import central_peer
        local, peers = send.load_config(self.root)
        seen = []
        with mock.patch.object(Path, "is_file", return_value=True), \
             mock.patch("a2a_transport.subprocess.run", side_effect=self._child_runner(seen)):
            with self.assertRaises(send.BridgeError):
                a2a_transport.invoke(self.root, local, central_peer(local, peers["codex"]), "status",
                                     origin=peers["buddytwo"], source="wb-two", message_mode=True,
                                     sender_context={"CODEBUDDY_SESSION_ID": "wb-one"})
        self.assertEqual(len(seen), 1)


if __name__ == "__main__":
    unittest.main()
