"""Real official SDK over two fictional ASGI services; no live host or secrets."""
from contextlib import contextmanager
import asyncio
import json
from pathlib import Path
import platform
import sys
import tempfile
import time
import unittest
from unittest import mock

import httpx
from google.protobuf.json_format import MessageToDict, ParseDict
from a2a.helpers import get_message_text, new_text_message, new_text_part
from a2a.types import Artifact, SendMessageRequest, SendMessageResponse, Task, TaskState, TaskStatus
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import a2a_client
from a2a_server import agent_card
from collaboration_client import central_peer
from host_adapters import ChatRef
import managed_egress
from message_delivery import request_hash


SOURCE = {"alias": "Codex", "device_id": "pc", "software": "codex", "conversation_id": "source"}
PEER = {"alias": "External", "device_id": "remote", "software": "research-app", "conversation_id": "original",
        "transport": "a2a", "url": "https://remote.example/agents/External", "verified": True,
        "verified_url": "https://remote.example/agents/External", "verified_ref": ["remote", "research-app", "original"],
        "verified_at": "first-verification"}


def answer_task(task_id, context_id, state, text):
    return Task(id=task_id, context_id=context_id, status=TaskStatus(state=state),
                artifacts=[Artifact(artifact_id="answer", parts=[new_text_part(text)])])


class Remote:
    def __init__(self):
        self.requests, self.messages, self.tasks = [], [], {}
        self.card_url = PEER["url"]
        self.card_name = "External"
        self.mode = "complete"
        self.redirect = False
        self.lost = False

    async def __call__(self, scope, receive, send):
        request = Request(scope, receive)
        self.requests.append((request.method, str(request.url), request.headers.get("authorization")))
        if request.headers.get("authorization") != "Bearer external-token":
            return await JSONResponse({"error": "wrong remote credential"}, 403)(scope, receive, send)
        if request.method == "GET":
            response = RedirectResponse("https://evil.example/card") if self.redirect else JSONResponse(MessageToDict(agent_card(self.card_name, self.card_url)))
            return await response(scope, receive, send)
        body = await request.json()
        if body["method"] == "SendMessage":
            params = ParseDict(body["params"], SendMessageRequest())
            message = params.message
            self.messages.append(message)
            if self.lost:
                raise httpx.ReadTimeout("fictional response lost after acceptance")
            if self.mode == "refuse":
                return await JSONResponse({"jsonrpc": "2.0", "id": body["id"], "error": {"code": -32602, "message": "fictional denied"}})(scope, receive, send)
            if self.mode in {"message", "oversize"}:
                response = SendMessageResponse(message=new_text_message("x" * 65537 if self.mode == "oversize" else "immediate answer"))
                return await JSONResponse({"jsonrpc": "2.0", "id": body["id"], "result": MessageToDict(response)})(scope, receive, send)
            task_id = message.task_id or "remote-" + str(len(self.messages))
            context = message.context_id or "remote-context"
            state = (TaskState.TASK_STATE_REJECTED if self.mode == "rejected"
                     else TaskState.TASK_STATE_FAILED if self.mode == "failed"
                     else TaskState.TASK_STATE_CANCELED if self.mode == "canceled"
                     else TaskState.TASK_STATE_UNSPECIFIED if self.mode == "unspecified"
                     else TaskState.TASK_STATE_INPUT_REQUIRED if self.mode in {"always-input", "input"} and (self.mode == "always-input" or len(self.messages) == 1)
                     else TaskState.TASK_STATE_WORKING if self.mode in {"working", "stall"}
                     else TaskState.TASK_STATE_COMPLETED)
            task = answer_task(task_id, context, state, "external answer")
            self.tasks[task_id] = task
            response = SendMessageResponse(task=task)
            return await JSONResponse({"jsonrpc": "2.0", "id": body["id"], "result": MessageToDict(response)})(scope, receive, send)
        task_id = body["params"]["id"]
        task = self.tasks[task_id]
        if self.mode == "working":
            task.status.state = TaskState.TASK_STATE_COMPLETED
        return await JSONResponse({"jsonrpc": "2.0", "id": body["id"], "result": MessageToDict(task)})(scope, receive, send)


class Central:
    def __init__(self, root, local):
        self.root, self.local = root, local
        self.requests, self.results = [], []
        self.approvals = 0
        self.allow = True

    def approve(self):
        self.approvals += 1
        return self.allow

    async def __call__(self, scope, receive, send):
        request = Request(scope, receive)
        self.requests.append(request.headers.get("authorization"))
        if request.headers.get("authorization") != "Bearer source-chat-token":
            return await JSONResponse({"error": "wrong central credential"}, 403)(scope, receive, send)
        if request.method == "GET":
            return await JSONResponse(MessageToDict(agent_card("External", "http://127.0.0.1:8788/agents/External")))(scope, receive, send)
        body = await request.json()
        params = ParseDict(body["params"], SendMessageRequest())
        message = params.message
        result = await managed_egress.deliver(self.root, self.local, PEER, SOURCE,
                    get_message_text(message), message.message_id, self.approve, task_timeout=.1)
        self.results.append(result)
        task = answer_task("central-" + message.message_id, "central-context",
                           TaskState.TASK_STATE_COMPLETED if result["state"] == "completed" else TaskState.TASK_STATE_FAILED,
                           result.get("reply") or result.get("error", ""))
        return await JSONResponse({"jsonrpc": "2.0", "id": body["id"],
                                   "result": MessageToDict(SendMessageResponse(task=task))})(scope, receive, send)


class EgressTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.local = {"machine_name": platform.node(), "device_id": "pc", "a2a": {"managed_ui": True,
                      "port": 8788, "chat_tokens": {"Codex": "source-chat-token"}, "tokens": {"local": "management-token"}},
                      "a2a_credentials": {"External": {"token": "external-token"}}}
        (self.root / "local.json").write_text(json.dumps(self.local), encoding="utf8")
        (self.root / "peers.json").write_text(json.dumps({"version": 1, "peers": {"Codex": SOURCE, "External": PEER}}), encoding="utf8")
        self.remote = Remote()
        self.central = Central(self.root, self.local)
        self.approve = mock.Mock(return_value=True)

    @contextmanager
    def network(self):
        original = httpx.AsyncClient
        remote, central = self.remote, self.central
        calls = []
        class Routes(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request):
                if request.url.host == "remote.example":
                    app = remote
                elif request.url.host == "127.0.0.1":
                    app = central
                else:
                    raise AssertionError("credential must not leave verified origins")
                return await httpx.ASGITransport(app=app).handle_async_request(request)
        def make(*args, **kwargs):
            calls.append(kwargs.copy())
            kwargs["transport"] = Routes()
            return original(*args, **kwargs)
        with mock.patch("managed_egress.httpx.AsyncClient", side_effect=make):
            yield calls

    async def deliver(self, task="local-1", text="raw research question", peer=None, source=None, timeout=.1):
        return await managed_egress.deliver(self.root, self.local, peer or PEER, source or SOURCE,
                                           text, task, self.approve, task_timeout=timeout)

    async def test_two_services_real_sdk_separate_credentials_and_raw_text(self):
        routed = central_peer(self.local, PEER)
        self.assertEqual(routed["url"], "http://127.0.0.1:8788/agents/External")
        self.assertEqual(PEER["url"], "https://remote.example/agents/External")
        with self.network() as calls:
            result = await a2a_client.exchange({"root": str(self.root), "local": self.local, "peer": routed,
                "origin": SOURCE, "source": "source", "mode": "send", "text": "raw research question",
                "message_id": "local-1", "token": "management-token", "timeout": .1})
        self.assertEqual(result["state"], "completed")
        self.assertEqual(result["reply"], "external answer")
        self.assertEqual(set(self.central.requests), {"Bearer source-chat-token"})
        self.assertEqual({r[2] for r in self.remote.requests}, {"Bearer external-token"})
        self.assertEqual(get_message_text(self.remote.messages[0]), "raw research question")
        self.assertEqual(dict(self.remote.messages[0].metadata)["collaboration_origin"], ChatRef.from_peer(SOURCE).to_dict())
        self.assertEqual(self.central.approvals, 1)
        self.assertTrue(all(c["follow_redirects"] is False and c["trust_env"] is False for c in calls))

    async def test_revoke_before_final_send_and_explicit_rpc_refusal(self):
        self.approve.return_value = False
        with self.network():
            denied = await self.deliver()
        self.assertEqual(denied["state"], "failed")
        self.assertEqual(self.remote.messages, [])
        self.approve.assert_called_once()
        self.approve.return_value = True
        self.remote.mode = "refuse"
        with self.network():
            refused = await self.deliver("local-2")
        self.assertEqual(refused["state"], "failed")
        self.assertEqual(len(self.remote.messages), 1)

    async def test_input_required_follows_same_remote_task_context_and_keeps_sources_separate(self):
        self.remote.mode = "input"
        with self.network():
            first = await self.deliver()
            second = await self.deliver("local-2", "answer to clarification")
            other = await self.deliver("local-3", source={**SOURCE, "conversation_id": "other-original"})
        self.assertEqual(first["state"], "input_required")
        self.assertEqual(second["state"], "completed")
        self.assertEqual(self.remote.messages[1].task_id, first["task_id"])
        self.assertEqual(self.remote.messages[1].context_id, first["context_id"])
        self.assertFalse(self.remote.messages[2].task_id)
        self.assertFalse(self.remote.messages[2].context_id)

    async def test_duplicate_task_only_queries_bound_remote_and_unknown_does_not_resend(self):
        with self.network():
            first = await self.deliver()
            second = await self.deliver()
        self.assertEqual(second["task_id"], first["task_id"])
        self.assertEqual(len(self.remote.messages), 1)
        self.approve.assert_called_once()
        self.remote.lost = True
        with self.network():
            lost = await self.deliver("local-2")
            repeated = await self.deliver("local-2")
        self.assertEqual(lost["state"], "uncertain")
        self.assertEqual(repeated["state"], "uncertain")
        self.assertEqual(len(self.remote.messages), 2)

    async def test_concurrent_duplicate_never_submits_twice_and_unknown_tombstone_survives_ttl(self):
        with self.network():
            results = await asyncio.gather(self.deliver(), self.deliver())
        self.assertEqual(len(self.remote.messages), 1)
        self.approve.assert_called_once()
        self.remote.lost = True
        with self.network():
            await self.deliver("local-2")
            with mock.patch("managed_egress.time.time", return_value=10**12):
                repeated = await self.deliver("local-2")
        self.assertEqual(repeated["state"], "uncertain")
        self.assertEqual(len(self.remote.messages), 2)

    async def test_timeout_queries_same_task_without_reissue(self):
        self.remote.mode = "stall"
        with self.network():
            first = await self.deliver(timeout=.01)
            self.remote.mode = "working"
            resumed = await self.deliver(timeout=.01)
        self.assertEqual(first["state"], "uncertain")
        self.assertEqual(resumed["state"], "completed")
        self.assertEqual(len(self.remote.messages), 1)
        self.approve.assert_called_once()

    async def test_cross_origin_card_redirect_and_changed_routing_fail_closed(self):
        self.remote.card_url = "https://evil.example/agent"
        with self.network():
            result = await self.deliver()
        self.assertEqual(result["state"], "failed")
        self.assertEqual(self.remote.messages, [])
        self.remote.card_url = PEER["url"]
        self.remote.redirect = True
        with self.network():
            result = await self.deliver("local-2")
        self.assertEqual(result["state"], "failed")
        self.remote.redirect = False
        with self.network():
            await self.deliver("local-3")
            self.remote.card_url = PEER["url"] + "/changed"
            changed = await self.deliver("local-4")
            self.remote.card_url = PEER["url"]
            self.remote.card_name = "new harmless description"
            harmless = await self.deliver("local-5")
        self.assertEqual(changed["state"], "failed")
        self.assertEqual(harmless["state"], "completed")
        self.assertEqual(len(self.remote.messages), 2)

    async def test_plaintext_unverified_rebound_and_shared_credentials_never_connect(self):
        variants = [{**PEER, "verified": False}, {**PEER, "conversation_id": "unverified-rebind"},
                    {**PEER, "url": "http://remote.example/agent", "verified_url": "http://remote.example/agent"}]
        with self.network():
            for index, peer in enumerate(variants):
                self.assertEqual((await self.deliver("bad-"+str(index), peer=peer))["state"], "failed")
            self.local["a2a_credentials"]["External"]["token"] = "source-chat-token"
            self.assertEqual((await self.deliver("shared"))["state"], "failed")
        self.assertEqual(self.remote.requests, [])

    async def test_explicit_reverification_can_update_routing_pin(self):
        with self.network():
            await self.deliver()
            self.remote.card_url = PEER["url"] + "/new-rpc"
            updated = await self.deliver("new-route", peer={**PEER, "verified_at": "second-verification"})
        self.assertEqual(updated["state"], "completed")
        self.assertEqual(len(self.remote.messages), 2)

    async def test_metadata_has_no_plaintext_or_tokens_and_wrong_binding_cannot_corrupt_original(self):
        with self.network():
            first = await self.deliver()
            wrong = await self.deliver(source={**SOURCE, "conversation_id": "other"})
            correct = await self.deliver()
        self.assertEqual(wrong["state"], "failed")
        self.assertEqual(correct["state"], "completed")
        self.assertEqual(len(self.remote.messages), 1)
        store = managed_egress.EgressState(self.root)
        with store.mailbox.connect() as db:
            rows = str([tuple(r) for r in db.execute("SELECT * FROM managed_egress_runs")])
        for secret in ("raw research question", "external-token", "source-chat-token", "external answer"):
            self.assertNotIn(secret, rows)
        self.assertEqual([p.name for p in (self.root / "runtime").glob("*.db")], ["a2a-mailbox.db"])

    async def test_admission_requires_actual_remote_acceptance_and_bounded_message_body(self):
        callback = mock.Mock()
        with self.network():
            for mode in ("rejected", "failed", "canceled", "unspecified", "oversize"):
                self.remote.mode = mode
                result = await managed_egress.deliver(self.root, self.local, PEER, SOURCE,
                    "body", "local-" + mode, self.approve, task_timeout=.02,
                    independent=True, on_admission=callback)
                self.assertIn(result["state"], {"failed", "rejected", "canceled", "uncertain"})
                callback.assert_not_called()
            for mode in ("complete", "always-input", "message"):
                self.remote.mode = mode
                result = await managed_egress.deliver(self.root, self.local, PEER, SOURCE,
                    "body", "positive-" + mode, self.approve, task_timeout=.2,
                    independent=True, on_admission=callback)
                self.assertIn(result["state"], {"completed", "input_required"})
                self.assertEqual(callback.call_args.args[0]["stage"], "external_service")
                callback.reset_mock()

    async def test_restart_resume_reads_exact_binding_without_reapproval_or_resend(self):
        self.remote.mode = "stall"
        with self.network():
            first = await managed_egress.deliver(self.root, self.local, PEER, SOURCE,
                "body", "local-1", self.approve, task_timeout=.03, independent=True)
            self.remote.mode = "working"
            result = await managed_egress.resume(self.root, self.local, PEER, SOURCE, "local-1", task_timeout=1)
        self.assertEqual(first["state"], "uncertain")
        self.assertEqual(result["state"], "completed")
        self.assertEqual(result["task_id"], first["task_id"])
        self.assertEqual(result["context_id"], first["context_id"])
        self.assertEqual(len(self.remote.messages), 1)
        self.approve.assert_called_once()

    async def test_resume_missing_binding_deadline_and_identity_do_not_send(self):
        self.remote.lost = True
        with self.network():
            await managed_egress.deliver(self.root, self.local, PEER, SOURCE,
                "body", "lost", self.approve, task_timeout=.1, independent=True)
            count = len(self.remote.requests)
            result = await managed_egress.resume(self.root, self.local, PEER, SOURCE, "lost", task_timeout=1)
            self.assertEqual(result["state"], "uncertain")
            self.assertEqual(len(self.remote.requests), count)
            self.remote.lost = False
            await managed_egress.deliver(self.root, self.local, PEER, SOURCE,
                "body", "local-1", self.approve, task_timeout=1, independent=True)
            store = managed_egress.EgressState(self.root)
            with store.mailbox.connect() as db:
                db.execute("UPDATE managed_egress_runs SET created=? WHERE local_task='local-1'", (time.time()-100,))
            count = len(self.remote.requests)
            expired = await managed_egress.resume(self.root, self.local, PEER, SOURCE, "local-1", task_timeout=1)
            self.assertEqual(expired["state"], "uncertain")
            self.assertEqual(len(self.remote.requests), count)
            before = store.existing("local-1")
            wrong = await managed_egress.resume(self.root, self.local, PEER,
                {**SOURCE, "conversation_id": "other"}, "local-1", task_timeout=1)
            self.assertEqual(wrong["state"], "failed")
            self.assertEqual(store.existing("local-1"), before)
            self.assertEqual(len(self.remote.requests), count)
        self.assertEqual(len(self.remote.messages), 2)

    async def test_resume_rechecks_card_and_cannot_change_bound_remote_context(self):
        with self.network():
            first = await self.deliver()
            self.remote.card_url = PEER["url"] + "/changed"
            changed = await managed_egress.resume(self.root, self.local, PEER, SOURCE, "local-1", task_timeout=1)
            self.assertEqual(changed["state"], "uncertain")
            self.remote.card_url = PEER["url"]
            self.remote.tasks[first["task_id"]].context_id = "wrong-context"
            context = await managed_egress.resume(self.root, self.local, PEER, SOURCE, "local-1", task_timeout=1)
            self.assertEqual(context["state"], "uncertain")
        self.assertEqual(len(self.remote.messages), 1)

    async def test_external_body_persisted_before_result_and_resume_is_idempotent(self):
        store = managed_egress.EgressState(self.root)
        store.mailbox.claim_message("local-1", "External", "original", "source-owner", "m1", "instance",
                                   SOURCE, PEER, request_hash(PEER, "body"))
        with store.mailbox.connect() as db:
            db.execute("UPDATE replies SET issued_at=? WHERE task_id='local-1'", (time.time(),))
        store.mailbox.record_delivery("local-1", {"state": "queued", "stage": "external_service"})
        with self.network():
            result = await managed_egress.deliver(self.root, self.local, PEER, SOURCE,
                "body", "local-1", self.approve, task_timeout=1, independent=True)
            saved = store.mailbox.external_result_for("source-owner", "m1")
            self.assertEqual(saved["text"], result["reply"])
            self.assertEqual(store.mailbox.receipt_for("source-owner", "m1")["state"], "queued")
            store.mailbox.mark_external_result_handled("local-1")
            resumed = await managed_egress.resume(self.root, self.local, PEER, SOURCE, "local-1", task_timeout=1)
        self.assertEqual(resumed["state"], "completed")
        self.assertEqual(store.mailbox.pending_external_results(), [])
        self.assertEqual(len(self.remote.messages), 1)

    async def test_independent_messages_do_not_guess_multiple_suspended_tasks(self):
        self.remote.mode = "always-input"
        with self.network():
            for name in ("first", "second", "third"):
                result = await managed_egress.deliver(self.root, self.local, PEER, SOURCE,
                    "body", name, self.approve, task_timeout=1, independent=True)
                self.assertEqual(result["state"], "input_required")
            await self.deliver("legacy-next")
        self.assertTrue(all(not message.task_id and not message.context_id for message in self.remote.messages))


if __name__ == "__main__":
    unittest.main()
