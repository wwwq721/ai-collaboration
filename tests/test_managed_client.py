"""Managed client boundaries with invented identities, tokens and host receipts."""
import asyncio
import concurrent.futures
import json
import os
from pathlib import Path
import platform
import sys
import tempfile
import unittest
import uuid
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import collaboration_client as client
import delivery_ticket as tickets
import host_adapters
import send
from result_watcher import ResultWatcher
from workbuddy import BridgeError


PC = {"alias": "Codex", "device_id": "pc", "software": "codex", "conversation_id": "source"}
TARGET = {"alias": "Buddy", "device_id": "pc", "software": "workbuddy", "conversation_id": "target"}
PHONE = {"alias": "Minis", "device_id": "phone", "software": "minis", "conversation_id": "mobile"}


class TicketTests(unittest.TestCase):
    def test_atomic_single_use_and_separate_process_store_cannot_mint_permission(self):
        store = tickets.DeliveryTickets()
        ticket = store.issue("task", PC, "body", "message")
        other = tickets.DeliveryTickets()
        with self.assertRaises(BridgeError):
            other.consume(ticket, PC, "message", tickets.text_hash("body"), "task", "task")
        def consume(_):
            try:
                return store.consume(ticket, PC, "message", tickets.text_hash("body"), "task", "task")["allowed"]
            except BridgeError:
                return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            self.assertEqual(sum(pool.map(consume, range(16))), 1)

    def test_expiry_hash_target_action_and_task_bindings(self):
        for field in ("expiry", "hash", "target", "action", "task"):
            with self.subTest(field=field):
                clock = [10]
                store = tickets.DeliveryTickets(clock=lambda: clock[0])
                ticket = store.issue("task", PC, "body", "message", ttl=5)
                clock[0] = 20 if field == "expiry" else 10
                with self.assertRaises(BridgeError):
                    store.consume(ticket, TARGET if field == "target" else PC, "message",
                                  tickets.text_hash("changed" if field == "hash" else "body"),
                                  "notification" if field == "action" else "task",
                                  "different" if field == "task" else "task")
                with self.assertRaises(BridgeError):
                    store.consume(ticket, PC, "message", tickets.text_hash("body"), "task", "task")

    def test_long_idle_wait_ticket_is_bounded_and_defaults_are_short(self):
        clock = [0]
        store = tickets.DeliveryTickets(clock=lambda: clock[0])
        short = store.issue("task", PC, "body", "message")
        long = store.issue("task", PC, "body", "message", ttl=660)
        clock[0] = 61
        with self.assertRaises(BridgeError):
            store.consume(short, PC, "message", tickets.text_hash("body"), "task", "task")
        self.assertTrue(store.consume(long, PC, "message", tickets.text_hash("body"), "task", "task")["allowed"])
        with self.assertRaises(BridgeError):
            store.issue("task", PC, "body", "message", ttl=661)


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.local = {"machine_name": platform.node(), "device_id": "pc", "a2a": {
            "managed_ui": True, "port": 8788, "chat_tokens": {"Codex": "fake-chat-token"},
            "tokens": {"local": "fake-admin-token"}, "device_tokens": {"phone": "fake-device-token"}}}
        self.peers = {p["alias"]: p for p in (PC, TARGET, PHONE)}
        (self.root / "local.json").write_text(json.dumps(self.local), encoding="utf8")
        (self.root / "peers.json").write_text(json.dumps({"version": 1, "peers": self.peers}), encoding="utf8")
        patch = mock.patch.dict(os.environ, {"CODEX_THREAD_ID": "source", "CODEBUDDY_SESSION_ID": "", "MINIS_CHAT_SESSION_ID": ""})
        patch.start()
        self.addCleanup(patch.stop)

    def test_two_fields_local_and_remote_both_use_central_service(self):
        for target in ("Buddy", "Minis"):
            with self.subTest(target=target), mock.patch("a2a_transport.invoke", return_value={"state": "accepted"}) as transport, mock.patch("send.invoke") as native:
                send.run({"to": target, "text": "new task"}, self.root)
                self.assertEqual(transport.call_args.args[2]["url"], "http://127.0.0.1:8788/agents/" + target)
                self.assertEqual(transport.call_args.kwargs["origin"], PC)
                native.assert_not_called()

    def test_sdk_subprocess_receives_chat_token_instead_of_admin_token(self):
        import a2a_transport
        python = self.root / (".a2a-env/Scripts/python.exe" if os.name == "nt" else ".a2a-env/bin/python")
        python.parent.mkdir(parents=True)
        python.touch()
        target = client.central_peer(self.local, TARGET)
        process = mock.Mock(returncode=0, stdout='{"state":"accepted"}')
        with mock.patch("a2a_transport.subprocess.run", return_value=process) as run:
            a2a_transport.invoke(self.root, self.local, target, "status", origin=PC)
        request = json.loads(run.call_args.kwargs["input"])
        self.assertEqual(request["token"], "fake-chat-token")

    def test_sdk_exchange_itself_rebinds_token_and_rejects_external_url(self):
        import a2a_client
        request = {"root": str(self.root), "local": self.local, "peer": client.central_peer(self.local, TARGET),
                   "mode": "task-status", "task_id": "task", "origin": PC, "source": "source",
                   "token": "fake-admin-token", "proxy_url": None}
        with mock.patch("a2a_client.httpx.AsyncClient", side_effect=RuntimeError("stop before I/O")) as http:
            with self.assertRaises(RuntimeError):
                asyncio.run(a2a_client.exchange(request))
        self.assertEqual(http.call_args.kwargs["headers"]["Authorization"], "Bearer fake-chat-token")
        request["peer"] = {**request["peer"], "url": "https://external.example/agents/Buddy"}
        with mock.patch("a2a_client.httpx.AsyncClient") as http, self.assertRaises(BridgeError):
            asyncio.run(a2a_client.exchange(request))
        http.assert_not_called()

    def test_explicit_identity_is_rejected_for_send_reply_and_query(self):
        for request in ({"to": "Buddy", "text": "x"}, {"to": "A2A-012345abcdef", "text": "x"}, {"to": "Buddy", "mode": "status"}):
            with self.subTest(request=request), self.assertRaises(BridgeError):
                send.run({**request, "as": "Codex"}, self.root)

    def test_reply_uses_bound_chat_bearer_and_does_not_write_mailbox(self):
        with mock.patch("collaboration_client.local_request", return_value={"state": "accepted"}) as post:
            send.run({"to": "A2A-012345abcdef", "text": "answer"}, self.root)
        self.assertEqual(post.call_args.args[1:], ("/collaboration/reply", {"address": "A2A-012345abcdef", "text": "answer"}, "fake-chat-token"))
        self.assertFalse((self.root / "runtime/a2a-mailbox.db").exists())

    def test_lost_reply_response_stays_uncertain_and_does_not_retry(self):
        from urllib.error import URLError
        opener = mock.Mock()
        opener.open.side_effect = URLError("lost response")
        with mock.patch("collaboration_client.build_opener", return_value=opener), self.assertRaises(BridgeError) as caught:
            client.local_request(self.local, "/collaboration/reply", {"address": "A2A-012345abcdef", "text": "answer"}, "fake-chat")
        self.assertEqual(caught.exception.state, "uncertain")
        opener.open.assert_called_once()

    def test_missing_chat_bearer_cannot_use_admin_fallback(self):
        del self.local["a2a"]["chat_tokens"]
        with self.assertRaises(BridgeError):
            client.source_token(self.local, self.peers, PC)

    def test_ambiguous_managed_host_context_cannot_choose_first(self):
        with mock.patch.dict(os.environ, {"CODEBUDDY_SESSION_ID": "target"}), self.assertRaises(BridgeError):
            send.run({"to": "Minis", "text": "new task"}, self.root)

    def test_phone_single_source_device_token_and_multiple_source_tokens(self):
        local = {"device_id": "phone", "platform": "minis", "network": {"managed_ui": True, "token": "fake-device"}}
        self.assertEqual(client.source_token(local, {"Minis": PHONE}, PHONE), "fake-device")
        peers = {"Minis": PHONE, "Other": {**PHONE, "alias": "Other", "conversation_id": "another"}}
        with self.assertRaises(BridgeError):
            client.source_token(local, peers, PHONE)
        local["network"]["chat_tokens"] = {"Minis": "fake-minis"}
        self.assertEqual(client.source_token(local, peers, PHONE), "fake-minis")

    def test_phone_reply_uses_chat_bound_token_when_multiple_sources_are_registered(self):
        from device_receiver import remote_reply
        from test_device_receiver import FakeOpener
        local = {"device_id": "phone", "platform": "minis", "network": {"managed_ui": True,
                 "base_url": "https://pc.example", "token": "fake-device", "chat_tokens": {"Minis": "fake-source"}}}
        peers = {"Minis": PHONE, "Other": {**PHONE, "alias": "Other", "conversation_id": "another"}}
        opener = FakeOpener()
        remote_reply(self.root, local, "A2A-012345abcdef", "answer", PHONE, opener=opener, peers=peers)
        self.assertEqual(opener.requests[0][0].get_header("Authorization"), "Bearer fake-source")
        del local["network"]["chat_tokens"]
        missing = FakeOpener()
        with self.assertRaises(BridgeError):
            remote_reply(self.root, local, "A2A-012345abcdef", "answer", PHONE, opener=missing, peers=peers)
        self.assertEqual(missing.requests, [])

    def test_phone_reply_single_source_keeps_verified_device_token(self):
        from device_receiver import remote_reply
        from test_device_receiver import FakeOpener
        local = {"device_id": "phone", "platform": "minis", "network": {"managed_ui": True,
                 "base_url": "https://pc.example", "token": "fake-device"}}
        opener = FakeOpener()
        remote_reply(self.root, local, "A2A-012345abcdef", "answer", PHONE, opener=opener, peers={"Minis": PHONE})
        self.assertEqual(opener.requests[0][0].get_header("Authorization"), "Bearer fake-device")

    def test_notification_multiple_sources_use_task_origin_bearer_and_other_chat_is_rejected(self):
        from device_receiver import report_notification
        from test_device_receiver import FakeOpener
        from urllib.error import HTTPError
        local = {"device_id": "phone", "platform": "minis", "network": {"managed_ui": True,
                 "base_url": "https://pc.example", "token": "fake-device",
                 "chat_tokens": {"Minis": "fake-source", "Other": "fake-other"}}}
        other = {**PHONE, "alias": "Other", "conversation_id": "another"}
        peers = {"Minis": PHONE, "Other": other}
        class BoundTask(FakeOpener):
            def open(self, request, timeout=0):
                if request.get_header("Authorization") != "Bearer fake-source":
                    self.requests.append((request, timeout))
                    raise HTTPError(request.full_url, 403, "origin mismatch", {}, None)
                return super().open(request, timeout)
        opener = BoundTask()
        report_notification(local, "original-task", "notice", "accepted", opener=opener, origin=PHONE, peers=peers)
        self.assertEqual(opener.requests[0][0].get_header("Authorization"), "Bearer fake-source")
        with self.assertRaises(BridgeError):
            report_notification(local, "original-task", "notice", "accepted", opener=opener, origin=other, peers=peers)
        self.assertEqual(opener.requests[1][0].get_header("Authorization"), "Bearer fake-other")
        self.assertEqual(json.loads(opener.requests[0][0].data),
                         {"task_id": "original-task", "notification_id": "notice", "state": "accepted"})

    def test_notification_single_source_and_legacy_shim_keep_device_token(self):
        from device_receiver import report_notification
        from test_device_receiver import FakeOpener
        local = {"device_id": "phone", "platform": "minis", "network": {"managed_ui": True,
                 "base_url": "https://pc.example", "token": "fake-device"}}
        opener = FakeOpener()
        report_notification(local, "task", "notice", "accepted", opener=opener, origin=PHONE, peers={"Minis": PHONE})
        self.assertEqual(opener.requests[0][0].get_header("Authorization"), "Bearer fake-device")
        local["network"]["managed_ui"] = False
        report_notification(local, "task", "notice", "accepted", opener)
        self.assertEqual(opener.requests[1][0].get_header("Authorization"), "Bearer fake-device")

    def test_managed_notification_cannot_fall_back_without_origin_or_multi_source_token(self):
        from device_receiver import report_notification
        from test_device_receiver import FakeOpener
        local = {"device_id": "phone", "platform": "minis", "network": {"managed_ui": True,
                 "base_url": "https://pc.example", "token": "fake-device"}}
        peers = {"Minis": PHONE, "Other": {**PHONE, "alias": "Other", "conversation_id": "another"}}
        opener = FakeOpener()
        with self.assertRaises(BridgeError):
            report_notification(local, "task", "notice", "accepted", opener, origin=PHONE, peers=peers)
        with self.assertRaises(BridgeError):
            report_notification(local, "task", "notice", "accepted", opener)
        self.assertEqual(opener.requests, [])

    def test_external_a2a_rejected_and_paired_central_phone_route_preserved(self):
        external = {**TARGET, "transport": "a2a", "url": "https://external.example/agents/Buddy"}
        with self.assertRaises(BridgeError):
            client.central_peer(self.local, external)
        mobile = {"platform": "minis", "device_id": "phone", "network": {"base_url": "https://pc.example"}}
        peer = {**PC, "software": "a2a", "url": "https://pc.example/agents/Codex"}
        routed = client.central_peer(mobile, peer)
        client.validate_central(mobile, routed)
        with self.assertRaises(BridgeError):
            client.validate_central(mobile, {**routed, "url": "http://pc.example/agents/Codex"})


class NativeTests(unittest.TestCase):
    def test_missing_permit_never_submits_native(self):
        local = {"device_id": "pc", "a2a": {"managed_ui": True}}
        native = mock.Mock()
        native.discover_owner.return_value = "owner"
        native.follow.return_value = {"conversationState": {"threadRuntimeStatus": {"type": "idle"}}}
        with mock.patch("codex_ipc.CodexIpc", return_value=native), self.assertRaises(BridgeError):
            send.codex_invoke(local, PC, "send", "body", "message", "source", 0)
        native.start_turn.assert_not_called()

    def test_final_idle_check_consumes_once_and_requires_idle_without_busy_permission(self):
        local = {"device_id": "pc", "a2a": {"managed_ui": True}}
        native = mock.Mock()
        native.discover_owner.return_value = "owner"
        native.follow.return_value = {"conversationState": {"threadRuntimeStatus": {"type": "idle"}}}
        native.start_turn.return_value = {"route": "start", "turnId": "turn"}
        events = []
        native.follow.side_effect = lambda *_: events.append("follow") or {"conversationState": {"threadRuntimeStatus": {"type": "idle"}}}
        native.start_turn.side_effect = lambda *a, **k: events.append("submit") or {"route": "start", "turnId": "turn"}
        def consume(*a):
            events.append("consume")
            return {"allowed": True, "allow_busy": False, "action": "task"}
        with mock.patch("codex_ipc.CodexIpc", return_value=native), mock.patch("delivery_ticket.consume_local", side_effect=consume) as verify:
            send.codex_invoke(local, PC, "send", "body", "message", "source", 0, permit={"ticket": "fake"})
        self.assertEqual(events, ["follow", "consume", "submit"])
        self.assertTrue(native.start_turn.call_args.kwargs["require_idle"])
        verify.assert_called_once()

    def test_busy_cannot_steer_without_permission_and_unknown_busy_is_truthful(self):
        local = {"device_id": "pc", "a2a": {"managed_ui": True}}
        native = mock.Mock()
        native.discover_owner.return_value = "owner"
        native.follow.return_value = {"conversationState": {"threadRuntimeStatus": {"type": "active"}}}
        with mock.patch("codex_ipc.CodexIpc", return_value=native), mock.patch("delivery_ticket.consume_local", return_value={"allowed": True, "allow_busy": False}), self.assertRaises(BridgeError):
            send.codex_invoke(local, PC, "send", "body", "message", "source", 0, permit={"ticket": "fake"})
        native.start_turn.assert_not_called()
        caps = host_adapters.get_adapter(local, "workbuddy").capabilities()
        self.assertTrue(caps["busy_state_supported"])
        self.assertFalse(caps["atomic_idle_submit_supported"])
        with mock.patch("workbuddy.invoke", return_value={"state": "accepted"}) as buddy:
            host_adapters.get_adapter(local, "workbuddy").invoke(TARGET, "send", "body", "message", permit={"ticket": "fake"})
        self.assertEqual(buddy.call_args.kwargs["permit"], {"ticket": "fake"})


class FailureStateTests(unittest.TestCase):
    def test_rpc_refusal_and_connect_failure_are_not_sent_but_lost_response_is_uncertain(self):
        import a2a_client
        import httpx
        from a2a.client.errors import A2AClientError
        from a2a.utils.errors import InvalidParamsError
        self.assertEqual(a2a_client.failure_state({"attempted": True}, InvalidParamsError("fictional denied")), "not_sent")
        for cause, expected in ((httpx.ConnectError("fictional no connect"), "not_sent"),
                                (httpx.ConnectTimeout("fictional no connect"), "not_sent"),
                                (httpx.ReadTimeout("fictional lost response"), "uncertain"),
                                (httpx.WriteError("fictional partial send"), "uncertain")):
            error = A2AClientError("wrapped error")
            error.__cause__ = cause
            self.assertEqual(a2a_client.failure_state({"attempted": True}, error), expected)
        self.assertEqual(a2a_client.failure_state({"attempted": True, "task_received": True}, InvalidParamsError("later error")), "uncertain")

    def test_definite_not_sent_pending_job_never_polls_or_notifies_and_can_be_pruned(self):
        from result_watcher import PendingResults
        with tempfile.TemporaryDirectory() as tmp:
            now = [0]
            pending = PendingResults(Path(tmp), clock=lambda: now[0])
            job = pending.prepare(PC, PHONE, "message")
            pending.mark_not_sent(job)
            self.assertEqual(pending.get(job)["state"], "not_sent")
            self.assertEqual(pending.due(), [])
            now[0] = pending.limits.metadata_ttl_seconds + 1
            pending.cleanup()
            self.assertIsNone(pending.get(job))


class WatcherTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.local = {"device_id": "pc", "a2a": {"managed_ui": True}}
        self.query = mock.AsyncMock(return_value={"state": "completed", "reply": "answer"})
        self.deliver = mock.AsyncMock(return_value={"state": "accepted"})

    def prepare(self, watcher, source=PC, peer=PHONE):
        job = watcher.pending.prepare(source, peer, "message")
        watcher.pending.bind(job, "remote-task")
        return job

    async def test_managed_pc_requires_service_callback_even_if_ordinary_deliver_injected(self):
        watcher = ResultWatcher(self.root, self.local, [PC, PHONE], query=self.query, deliver=self.deliver)
        job = self.prepare(watcher)
        await watcher.tick()
        self.deliver.assert_not_awaited()
        self.assertEqual(watcher.pending.get(job)["state"], "ready")

    async def test_service_callback_gets_claimed_task_and_native_receipt_is_separate(self):
        authorized = mock.AsyncMock(return_value={"state": "accepted", "turn_id": "native-turn"})
        watcher = ResultWatcher(self.root, self.local, [PC, PHONE], query=self.query, deliver=self.deliver, managed_deliver=authorized)
        job = self.prepare(watcher)
        await watcher.tick()
        row, text = authorized.call_args.args
        self.assertEqual((row["task_id"], row["state"], row["origin"]["conversation_id"]), ("remote-task", "dispatching", "source"))
        self.assertIn("answer", text)
        self.assertEqual(watcher.pending.get(job)["state"], "notified")
        self.deliver.assert_not_awaited()

    async def test_mobile_report_retry_never_repeats_native_notification(self):
        local = {"platform": "minis", "device_id": "phone", "network": {"managed_ui": True}}
        watcher = ResultWatcher(self.root, local, [PC, PHONE], query=self.query, deliver=self.deliver)
        job = self.prepare(watcher, PHONE, PC)
        with mock.patch("device_receiver.report_notification", side_effect=BridgeError("offline")) as report:
            await watcher.tick()
            self.assertEqual(watcher.pending.get(job)["notification_reported"], 0)
        with mock.patch("device_receiver.report_notification", return_value={"state": "accepted"}) as report:
            await watcher.tick()
            report.assert_called_once_with(local, "remote-task", watcher.pending.get(job)["notification_id"], "accepted",
                                           origin=watcher.pending.get(job)["origin"], peers=watcher.peers)
        self.assertEqual(watcher.pending.get(job)["notification_reported"], 1)
        self.deliver.assert_awaited_once()

    async def test_background_query_uses_original_chat_token_and_origin(self):
        self.local["a2a"].update({"chat_tokens": {"Codex": "fake-chat"}, "tokens": {"local": "fake-admin"},
                                  "device_tokens": {"phone": "fake-device"}})
        watcher = ResultWatcher(self.root, self.local, [PC, PHONE])
        job = self.prepare(watcher)
        with mock.patch("a2a_client.exchange", new=mock.AsyncMock(return_value={"state": "completed"})) as query:
            await watcher._query(watcher.pending.get(job))
        self.assertEqual(query.call_args.args[0]["token"], "fake-chat")
        self.assertEqual(query.call_args.args[0]["origin"], {k: PC[k] for k in ("device_id", "software", "conversation_id")})


class ReceiverTests(unittest.TestCase):
    def setUp(self):
        from device_receiver import Receiver
        from test_device_receiver import FakeOpener
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.local = {"device_id": "phone", "platform": "minis", "network": {
            "base_url": "https://pc.example", "token": "fake-device", "managed_ui": True}}
        self.opener = FakeOpener()
        self.deliver = mock.Mock(return_value={"state": "accepted", "scheduled_task_id": "native-schedule"})
        self.receiver = Receiver(Path(self.tmp.name), self.local, [PHONE], self.deliver, self.opener, clock=lambda: 1000)
        self.message = {"state": "delivery", "delivery_id": str(uuid.uuid4()), "peer": "Minis",
                        "conversation_id": "mobile", "text": "task", "reply_to": "A2A-012345abcdef",
                        "expires_at": 1100, "require_authorize": True}

    def test_poll_advertises_final_authorization_protocol(self):
        self.receiver.poll(0)
        self.assertIn("policy=2", self.opener.requests[0][0].full_url)

    def test_authorize_immediately_precedes_submit_and_duplicates_only_ack(self):
        self.opener.responses = [{"allowed": True, "allow_busy": False}, {"state": "accepted"}]
        self.receiver.process(self.message)
        self.receiver.process(self.message)
        self.assertEqual([r.full_url.rsplit("/", 1)[1] for r, _ in self.opener.requests], ["authorize", "ack"])
        self.assertEqual(json.loads(self.opener.requests[0][0].data), {"delivery_id": self.message["delivery_id"]})
        self.deliver.assert_called_once()

    def test_authorization_denied_or_disconnected_never_submits_or_retries_prompt(self):
        self.opener.responses = [{"allowed": False}, {"state": "accepted"}]
        self.assertEqual(self.receiver.process(self.message)["state"], "not_sent")
        self.receiver.process(self.message)
        self.deliver.assert_not_called()
        self.assertEqual(len(self.opener.requests), 2)

    def test_managed_phone_cannot_fall_back_to_legacy_server_without_policy(self):
        del self.message["require_authorize"]
        self.opener.responses = [{"state": "accepted"}, {"state": "accepted"}]
        self.assertEqual(self.receiver.process(self.message)["state"], "not_sent")
        self.deliver.assert_not_called()


if __name__ == "__main__":
    unittest.main()
