"""Independent messages and automatic receipts use no business-result wakeups."""
import json
import os
from pathlib import Path
import platform
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock
import uuid

import httpx
from a2a.types import StreamResponse, Task, TaskState, TaskStatus

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import a2a_client
import collaboration_client
import send
from message_delivery import request_hash
from result_watcher import PendingResults, ResultWatcher
from workbuddy import BridgeError


SOURCE = {"alias": "Codex", "device_id": "pc", "software": "codex", "conversation_id": "source"}
TARGET = {"alias": "Buddy", "device_id": "pc", "software": "workbuddy", "conversation_id": "target"}


class MessageClientTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.local = {"machine_name": platform.node(), "device_id": "pc", "a2a": {
            "message_delivery": True, "port": 8788,
            "chat_tokens": {"Codex": "fake-bound-token"}, "tokens": {"local": "fake-admin-token"}}}
        self.peers = {"Codex": SOURCE, "Buddy": TARGET}
        (self.root / "local.json").write_text(json.dumps(self.local), encoding="utf8")
        (self.root / "peers.json").write_text(json.dumps({"version": 1, "peers": self.peers}), encoding="utf8")
        self.env = mock.patch.dict(os.environ, {"CODEX_THREAD_ID": "source", "CODEBUDDY_SESSION_ID": "", "MINIS_CHAT_SESSION_ID": ""})
        self.env.start()
        self.now = [1000.0]
        self.clock = lambda: self.now[0]
        self.peer = collaboration_client.central_peer(self.local, TARGET)

    def tearDown(self):
        self.env.stop()
        self.directory.cleanup()

    def pending(self):
        return PendingResults(self.root, clock=self.clock)

    def prepare(self, text="first message", source=SOURCE, peer=None):
        key = str(uuid.uuid4())
        self.pending().prepare_message(source, peer or self.peer, key, text)
        return key

    def request(self, text="new body", key=None):
        return {"root": str(self.root), "local": self.local, "peer": self.peer, "origin": SOURCE,
                "mode": "send", "text": text, "message_id": key or str(uuid.uuid4()), "source": "source",
                "token": "fake-bound-token", "message_mode": True}

    def http(self, receipt=None, status=200):
        target = ["pc", "workbuddy", "target"]
        def respond(request):
            value = receipt or {"state": "queued", "stage": "host_queue", "target_ref": target}
            return httpx.Response(status, json=value)
        return httpx.AsyncClient(transport=httpx.MockTransport(respond))

    async def test_two_messages_create_independent_prepared_records(self):
        first = self.prepare("one")
        self.now[0] += 1
        second = self.prepare("two")
        pending = self.pending()
        self.assertEqual(pending.get(first)["state"], "prepared")
        self.assertEqual(pending.get(second)["state"], "prepared")
        self.assertEqual(pending.latest_message(SOURCE, self.peer)["id"], second)
        self.assertEqual(pending.get(first)["request_hash"], request_hash(TARGET, "one"))

    async def test_duplicate_key_with_changed_body_or_target_is_refused(self):
        key = self.prepare("one")
        pending = self.pending()
        self.assertEqual(pending.prepare_message(SOURCE, self.peer, key, "one"), key)
        with self.assertRaises(BridgeError):
            pending.prepare_message(SOURCE, self.peer, key, "two")
        sibling = {**self.peer, "conversation_id": "sibling"}
        with self.assertRaises(BridgeError):
            pending.prepare_message(SOURCE, sibling, key, "one")

    async def test_status_is_scoped_to_current_source_and_original_target(self):
        self.prepare("mine")
        sibling = {**SOURCE, "conversation_id": "other-source"}
        other = self.prepare("theirs", source=sibling)
        self.assertIsNone(self.pending().latest_message(SOURCE, {**self.peer, "conversation_id": "different"}))
        self.assertNotEqual(self.pending().latest_message(SOURCE, self.peer)["id"], other)

    async def test_send_is_prepared_before_post_and_carries_no_old_task_context(self):
        request = self.request()
        seen = []
        root = self.root
        class Client:
            async def send_message(self, outgoing):
                seen.append(outgoing)
                pending = PendingResults(root)
                assert pending.get(request["message_id"])["mode"] == "message"
                yield StreamResponse(task=Task(id="protocol-task", context_id="new-context",
                                               status=TaskStatus(state=TaskState.TASK_STATE_WORKING)))
        async with self.http() as http:
            result = await a2a_client.exchange_message(request, Client(), http)
        self.assertEqual(result["state"], "queued")
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0].message.task_id, "")
        self.assertEqual(seen[0].message.context_id, "")
        self.assertEqual(seen[0].message.metadata["collaboration_mode"], "message")
        self.assertTrue(seen[0].configuration.return_immediately)
        self.assertNotIn("task_id", result)
        self.assertNotIn("reply", result)

    async def test_lost_post_response_keeps_key_for_read_without_resending(self):
        request = self.request()
        calls = []
        class Client:
            async def send_message(self, outgoing):
                calls.append(outgoing)
                raise httpx.ReadTimeout("ambiguous response")
                yield
        async with self.http() as http:
            with self.assertRaises(httpx.ReadTimeout):
                await a2a_client.exchange_message(request, Client(), http)
            job = PendingResults(self.root).get(request["message_id"])
            result = await a2a_client.query_receipt(request, job, http)
        self.assertEqual(len(calls), 1)
        self.assertEqual(result["state"], "queued")
        self.assertEqual(job["mode"], "message")

    async def test_absent_receipt_is_waiting_and_other_target_is_rejected(self):
        key = self.prepare()
        job = self.pending().get(key)
        async with self.http(status=404) as http:
            result = await a2a_client.query_receipt(self.request(key=key), job, http)
        self.assertEqual(result, {"state": "waiting", "stage": "relay"})
        changed = {"state": "accepted", "stage": "native_host", "target_ref": ["pc", "workbuddy", "wrong"]}
        async with self.http(receipt=changed) as http:
            with self.assertRaises(ValueError):
                await a2a_client.query_receipt(self.request(key=key), job, http)

    async def test_receipt_query_is_only_to_exact_paired_central_origin(self):
        key = self.prepare()
        bad = self.request(key=key)
        bad["peer"] = {**self.peer, "url": "https://other.example/agents/Buddy"}
        async with self.http() as http:
            with self.assertRaises(BridgeError):
                await a2a_client.query_receipt(bad, self.pending().get(key), http)

    async def test_optional_external_result_does_not_become_a_receipt_notification(self):
        key = self.prepare("external question")
        receipt = {"state": "queued", "stage": "external_service", "target_ref": ["pc", "workbuddy", "target"],
                   "result": {"state": "ready", "text": "actual external answer"}}
        async with self.http(receipt=receipt) as http:
            result = await a2a_client.query_receipt(self.request(key=key), self.pending().get(key), http)
        self.assertEqual(result, {"state": "queued", "stage": "external_service"})
        self.pending().record_receipt(key, result)
        self.assertNotIn("result", self.pending().receipt_status(key))
        self.assertNotIn("actual external answer", self.pending().path.read_bytes().decode("utf8", errors="ignore"))

    async def test_legacy_generic_a2a_software_is_not_guessed_from_a_matching_chat_id(self):
        generic = {**self.peer, "software": "a2a"}
        generic.pop("host_software", None)
        key = self.prepare(peer=generic)
        async with self.http() as http:
            with self.assertRaises(ValueError):
                await a2a_client.query_receipt(self.request(key=key), self.pending().get(key), http)

    async def test_background_receipts_never_deliver_to_an_ai_chat(self):
        key = self.prepare("no answer required")
        deliver = mock.AsyncMock()
        watcher = ResultWatcher(self.root, self.local, self.peers, deliver=deliver, clock=self.clock)
        with mock.patch.object(watcher, "_query_message", mock.AsyncMock(return_value={"state": "queued", "stage": "host_queue"})):
            await watcher.tick()
            await watcher.tick()
        self.assertEqual(watcher.pending.get(key)["state"], "receipt_done")
        deliver.assert_not_called()

    async def test_unknown_receipt_deadline_does_not_require_a_business_reply(self):
        key = self.prepare("body")
        deliver = mock.AsyncMock()
        watcher = ResultWatcher(self.root, self.local, self.peers, deliver=deliver, clock=self.clock)
        with mock.patch.object(watcher, "_query_message", mock.AsyncMock(return_value={"state": "waiting", "stage": "relay"})):
            await watcher.tick()
            self.assertEqual(watcher.pending.get(key)["state"], "waiting")
            self.now[0] += 1000
            await watcher.tick()
        self.assertEqual(watcher.pending.receipt_status(key)["state"], "uncertain")
        deliver.assert_not_called()

    async def test_short_failure_and_summary_expiry(self):
        key = self.prepare("DOCS-1111-PURPOSE 文档同步 后面的很长内容")
        pending = self.pending()
        pending.record_receipt(key, {"state": "not_sent", "stage": "relay"})
        result = pending.receipt_status(key)
        self.assertEqual(set(result), {"state", "error"})
        self.assertIn("DOCS-1111-PURPOSE", result["error"])
        self.assertEqual(len(result["error"].splitlines()), 1)
        self.assertNotIn(key, result["error"])
        self.now[0] += pending.limits.result_ttl_seconds + 1
        pending.cleanup()
        self.assertIsNone(pending.get(key)["summary"])

    async def test_confirmed_receipt_cannot_be_downgraded(self):
        key = self.prepare()
        pending = self.pending()
        pending.record_receipt(key, {"state": "accepted", "stage": "native_host"})
        pending.record_receipt(key, {"state": "uncertain", "stage": "relay"})
        self.assertEqual(pending.receipt_status(key)["state"], "accepted")

    async def test_unknown_record_remains_queryable_after_short_sdk_cache_window(self):
        key = self.prepare("original message")
        pending = self.pending()
        pending.record_receipt(key, {"state": "uncertain", "stage": "relay"})
        self.now[0] += pending.limits.result_ttl_seconds + 1
        job = pending.latest_message(SOURCE, self.peer)
        self.assertEqual(job["id"], key)
        self.assertIsNone(job["summary"])
        async with self.http() as http:
            receipt = await a2a_client.query_receipt(self.request(key=key), job, http)
        pending.record_receipt(key, receipt)
        self.assertEqual(pending.receipt_status(key)["state"], "queued")

    async def test_message_routing_uses_source_bound_credentials_and_guards_native_helper(self):
        with mock.patch("a2a_transport.invoke", return_value={"state": "waiting"}) as invoke:
            send.run({"to": "Buddy", "text": "one"}, self.root)
        self.assertTrue(invoke.call_args.kwargs["message_mode"])
        self.assertTrue(invoke.call_args.args[2]["local_a2a"])
        with self.assertRaises(BridgeError):
            send.run({"mode": "native-deliver", "to": "Buddy", "text": "bypass", "message_id": str(uuid.uuid4())}, self.root)
        with self.assertRaises(BridgeError):
            send.run({"to": "Buddy", "text": "spoof", "as": "Codex"}, self.root)
        with mock.patch.dict(os.environ, {"CODEBUDDY_SESSION_ID": "target"}), self.assertRaises(BridgeError):
            send.run({"to": "Buddy", "text": "ambiguous"}, self.root)

    async def test_full_exchange_skips_legacy_continuity_and_accepts_two_messages(self):
        from a2a_state import Mailbox
        from message_delivery import SKILL_ID
        source_key = json.dumps({k: SOURCE[k] for k in ("device_id", "software", "conversation_id")},
                                sort_keys=True, separators=(",", ":"))
        Mailbox(self.root).save_outgoing(source_key, a2a_client.continuity_key(self.peer), "old-context", "old-active-task")
        calls = []
        class Client:
            async def send_message(self, outgoing):
                calls.append(outgoing)
                yield StreamResponse(task=Task(id="protocol-" + outgoing.message.message_id,
                                               status=TaskStatus(state=TaskState.TASK_STATE_WORKING)))
        card = SimpleNamespace(supported_interfaces=[SimpleNamespace(url=self.peer["url"])],
                               skills=[SimpleNamespace(id=SKILL_ID)])
        resolver = mock.Mock()
        resolver.get_agent_card = mock.AsyncMock(return_value=card)
        real_http = httpx.AsyncClient
        def make_http(**kwargs):
            def respond(request):
                return httpx.Response(200, json={"message_id": request.url.path.rsplit("/", 1)[1],
                    "state": "queued", "stage": "host_queue", "target_ref": ["pc", "workbuddy", "target"]})
            return real_http(transport=httpx.MockTransport(respond), **kwargs)
        with mock.patch("a2a_client.A2ACardResolver", return_value=resolver), \
             mock.patch("a2a_client.ClientFactory") as factory, \
             mock.patch("a2a_client.httpx.AsyncClient", side_effect=make_http):
            factory.return_value.create.return_value = Client()
            first = await a2a_client.exchange(self.request("first"))
            second = await a2a_client.exchange(self.request("second"))
        self.assertEqual((first["state"], second["state"]), ("queued", "queued"))
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(not item.message.task_id and not item.message.context_id for item in calls))

    async def test_new_mode_requires_card_capability_and_cannot_fall_back_to_legacy(self):
        card = SimpleNamespace(supported_interfaces=[SimpleNamespace(url=self.peer["url"])], skills=[])
        resolver = mock.Mock()
        resolver.get_agent_card = mock.AsyncMock(return_value=card)
        real_http = httpx.AsyncClient
        with mock.patch("a2a_client.A2ACardResolver", return_value=resolver), \
             mock.patch("a2a_client.ClientFactory") as factory, \
             mock.patch("a2a_client.httpx.AsyncClient", side_effect=lambda **kw: real_http(transport=httpx.MockTransport(lambda req: httpx.Response(200)), **kw)):
            factory.return_value.create.return_value = mock.Mock()
            with self.assertRaises(ValueError):
                await a2a_client.exchange(self.request())
            factory.return_value.create.return_value.send_message.assert_not_called()

    async def test_bound_credential_cannot_follow_foreign_card_even_if_initial_token_was_empty(self):
        from message_delivery import SKILL_ID
        card = SimpleNamespace(supported_interfaces=[SimpleNamespace(url="https://foreign.example/agent")],
                               skills=[SimpleNamespace(id=SKILL_ID)])
        resolver = mock.Mock()
        resolver.get_agent_card = mock.AsyncMock(return_value=card)
        request = self.request()
        request["token"] = ""
        real_http = httpx.AsyncClient
        with mock.patch("a2a_client.A2ACardResolver", return_value=resolver), \
             mock.patch("a2a_client.ClientFactory") as factory, \
             mock.patch("a2a_client.httpx.AsyncClient", side_effect=lambda **kw: real_http(transport=httpx.MockTransport(lambda req: httpx.Response(200)), **kw)):
            factory.return_value.create.return_value = mock.Mock()
            with self.assertRaises(ValueError):
                await a2a_client.exchange(request)
            factory.return_value.create.return_value.send_message.assert_not_called()


if __name__ == "__main__":
    unittest.main()
