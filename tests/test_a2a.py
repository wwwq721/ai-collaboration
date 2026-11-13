"""Behavioral checks through the official A2A SDK and real ASGI routes.

Only the final native desktop send is substituted. All state is temporary.
"""
import asyncio
from contextlib import asynccontextmanager
import json
import logging
from pathlib import Path
import platform
import re
import sys
import tempfile
import time
import unittest
from unittest import mock
import uuid

import httpx
from a2a.client import A2ACardResolver, ClientConfig, ClientFactory
from a2a.helpers import get_message_text, new_text_message
from a2a.server.context import ServerCallContext
from a2a.server.tasks import DatabaseTaskStore
from a2a.types import CancelTaskRequest, GetTaskRequest, Part, Role, SendMessageRequest, Task, TaskState, TaskStatus
from sqlalchemy.ext.asyncio import create_async_engine

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))
import a2a_client
import a2a_server
from a2a_state import Mailbox
from workbuddy import BridgeError

TOKEN_A = "a" * 40
TOKEN_B = "b" * 40
WORKING = TaskState.TASK_STATE_WORKING
COMPLETED = TaskState.TASK_STATE_COMPLETED
FAILED = TaskState.TASK_STATE_FAILED


class A2ATests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        logging.getLogger("a2a.server.agent_execution.active_task").setLevel(logging.CRITICAL)
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.local = {"machine_name": platform.node(), "device_id": "test",
                      "a2a": {"tokens": {"caller-a": TOKEN_A, "caller-b": TOKEN_B},
                              "task_timeout": 4, "idle_wait_seconds": 1}}
        self.peers = {"version": 1, "peers": {
            "Codex": {"software": "codex", "device_id": "test", "conversation_id": "cx"},
            "WorkBuddy": {"software": "workbuddy", "device_id": "test", "conversation_id": "wb"}}}
        self.save()
        self.calls = []
        self.reply = None
        self.background_before = set(asyncio.all_tasks())

    def save(self):
        (self.root / "local.json").write_text(json.dumps(self.local), encoding="utf8")
        (self.root / "peers.json").write_text(json.dumps(self.peers), encoding="utf8")

    async def asyncTearDown(self):
        pending = [t for t in asyncio.all_tasks() if t not in self.background_before and t is not asyncio.current_task()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.wait(pending, timeout=2)
        self.tmp.cleanup()

    async def deliver(self, root, local, alias, text, message_id, wait_idle):
        self.calls.append({"alias": alias, "text": text, "message_id": message_id, "wait": wait_idle})
        if self.reply is not None:
            address = re.search(r"A2A-[0-9a-f]{12}", text).group()
            conversation = self.peers["peers"][alias]["conversation_id"]
            Mailbox(root).submit(address, self.reply, {"alias": alias, "conversation_id": conversation})

    @asynccontextmanager
    async def app_context(self, instance=None):
        app = a2a_server.create_app(self.root, instance or str(uuid.uuid4()), self.deliver)
        async with app.app.router.lifespan_context(app.app):
            yield app

    @asynccontextmanager
    async def client(self, app, alias="Codex", token=TOKEN_A):
        headers = {}
        if token:
            headers["Authorization"] = "Bearer " + token
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                    base_url="http://127.0.0.1:8788", headers=headers) as http:
            card = await A2ACardResolver(http, "http://127.0.0.1:8788/agents/" + alias).get_agent_card()
            sdk = ClientFactory(ClientConfig(httpx_client=http, streaming=False, polling=True,
                                            supported_protocol_bindings=["JSONRPC"])).create(card)
            yield sdk

    async def send(self, sdk, text="算 7 × 9", message_id=None, context_id=None, task_id=None):
        message = new_text_message(text, role=Role.ROLE_USER, context_id=context_id, task_id=task_id)
        if message_id:
            message.message_id = message_id
        task = None
        async for event in sdk.send_message(SendMessageRequest(message=message)):
            if event.HasField("task"):
                task = event.task
        self.assertIsNotNone(task)
        return task

    async def terminal(self, sdk, task_id):
        for _ in range(120):
            task = await sdk.get_task(GetTaskRequest(id=task_id))
            if task.status.state in {COMPLETED, FAILED}:
                return task
            await asyncio.sleep(.03)
        self.fail("task never reached a terminal state")

    async def test_health_unauthenticated_but_agent_card_requires_token(self):
        async with self.app_context() as app:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788") as http:
                self.assertEqual((await http.get("/health")).status_code, 200)
                self.assertEqual((await http.get("/agents/Codex/.well-known/agent-card.json")).status_code, 401)
            async with self.client(app) as sdk:
                self.assertIsNotNone(sdk)
        self.assertEqual(self.calls, [])

    async def test_native_acceptance_is_working_then_correlated_reply_completes(self):
        async with self.app_context() as app, self.client(app) as sdk:
            task = await self.send(sdk)
            await asyncio.sleep(.05)
            fetched = await sdk.get_task(GetTaskRequest(id=task.id))
            self.assertEqual(fetched.status.state, WORKING)
            self.assertEqual(len(self.calls), 1)
            address = Mailbox(self.root).row(task.id)["address"]
            Mailbox(self.root).submit(address, "63", {"alias": "Codex", "conversation_id": "cx"})
            done = await self.terminal(sdk, task.id)
            self.assertEqual(done.status.state, COMPLETED)
            self.assertEqual(done.artifacts[0].parts[0].text, "63")
            self.assertEqual(get_message_text(done.status.message), "63")

    async def test_wrong_chat_cannot_submit_result(self):
        async with self.app_context() as app, self.client(app) as sdk:
            task = await self.send(sdk)
            await asyncio.sleep(.04)
            address = Mailbox(self.root).row(task.id)["address"]
            for sender in ({"alias": "WorkBuddy", "conversation_id": "wb"},
                           {"alias": "Codex", "conversation_id": "different"}):
                with self.assertRaises(BridgeError):
                    Mailbox(self.root).submit(address, "wrong", sender)
            self.assertIsNone(Mailbox(self.root).row(task.id)["result"])

    async def test_tasks_private_to_caller_and_agent(self):
        self.reply = "63"
        async with self.app_context() as app, self.client(app) as sdk:
            task = await self.send(sdk)
            await self.terminal(sdk, task.id)
            async with self.client(app, token=TOKEN_B) as other:
                with self.assertRaises(Exception):
                    await other.get_task(GetTaskRequest(id=task.id))
            async with self.client(app, alias="WorkBuddy") as other:
                with self.assertRaises(Exception):
                    await other.get_task(GetTaskRequest(id=task.id))

    async def test_duplicate_message_id_never_redelivers(self):
        self.reply = "63"
        message_id = str(uuid.uuid4())
        async with self.app_context() as app, self.client(app) as sdk:
            task = await self.send(sdk, message_id=message_id)
            await self.terminal(sdk, task.id)
            with self.assertRaises(Exception):
                await self.send(sdk, message_id=message_id)
            self.assertEqual(len(self.calls), 1)

    async def test_continued_context_creates_new_task(self):
        self.reply = "first"
        async with self.app_context() as app, self.client(app) as sdk:
            first = await self.send(sdk)
            await self.terminal(sdk, first.id)
            self.reply = "second"
            second = await self.send(sdk, "再问一次", context_id=first.context_id)
            second = await self.terminal(sdk, second.id)
            self.assertNotEqual(second.id, first.id)
            self.assertEqual(second.context_id, first.context_id)
            self.assertEqual(second.artifacts[0].parts[0].text, "second")
            self.assertEqual(len(self.calls), 2)

    async def test_reply_is_idempotent_but_cannot_overwrite(self):
        box = Mailbox(self.root)
        address = box.claim("task", "Codex", "cx", "owner", "msg", "instance")
        sender = {"alias": "Codex", "conversation_id": "cx"}
        box.submit(address, "same", sender)
        box.submit(address, "same", sender)
        with self.assertRaises(BridgeError):
            box.submit(address, "different", sender)
        self.assertEqual(box.row("task")["result"], "same")

    async def seed_previous(self, result=None):
        box = Mailbox(self.root)
        address = box.claim("old-task", "Codex", "cx", "caller-a", "old-message", "old-instance")
        if result is not None:
            box.submit(address, result, {"alias": "Codex", "conversation_id": "cx"})
        engine = create_async_engine("sqlite+aiosqlite:///" + (self.root / "runtime/a2a-tasks.db").as_posix())
        store = DatabaseTaskStore(engine, owner_resolver=lambda ctx: "codex:" + ctx.user.user_name)
        await store.initialize()
        task = Task(id="old-task", context_id="old-context", status=TaskStatus(state=WORKING))
        await store.save(task, ServerCallContext(user=a2a_server.Caller("caller-a")))
        await engine.dispose()

    async def test_restart_recovers_recorded_result_without_redelivery(self):
        await self.seed_previous("recovered")
        async with self.app_context() as app, self.client(app) as sdk:
            task = await sdk.get_task(GetTaskRequest(id="old-task"))
            self.assertEqual(task.status.state, COMPLETED)
            self.assertEqual(task.artifacts[0].parts[0].text, "recovered")
        self.assertEqual(self.calls, [])

    async def test_restart_fails_unanswered_task_without_redelivery(self):
        await self.seed_previous()
        async with self.app_context() as app, self.client(app) as sdk:
            task = await sdk.get_task(GetTaskRequest(id="old-task"))
            self.assertEqual(task.status.state, FAILED)
        self.assertEqual(self.calls, [])

    async def test_native_failure_becomes_failed_task_without_retry(self):
        async def fail(*args):
            self.calls.append("attempt")
            raise RuntimeError("native unavailable")
        self.deliver = fail
        async with self.app_context() as app, self.client(app) as sdk:
            task = await self.send(sdk)
            task = await self.terminal(sdk, task.id)
            self.assertEqual(task.status.state, FAILED)
            self.assertIn("native unavailable", get_message_text(task.status.message))
        self.assertEqual(self.calls, ["attempt"])

    async def test_empty_text_rejected_without_native_send(self):
        async with self.app_context() as app, self.client(app) as sdk:
            with self.assertRaises(Exception):
                await self.send(sdk, "   ")
        self.assertEqual(self.calls, [])

    async def test_unsupported_cancel_preserves_working_task_and_later_reply(self):
        async with self.app_context() as app, self.client(app) as sdk:
            task = await self.send(sdk)
            await asyncio.sleep(.05)
            with self.assertRaises(Exception):
                await sdk.cancel_task(CancelTaskRequest(id=task.id))
            fetched = await sdk.get_task(GetTaskRequest(id=task.id))
            self.assertEqual(fetched.status.state, WORKING)
            box = Mailbox(self.root)
            box.submit(box.row(task.id)["address"], "63", {"alias": "Codex", "conversation_id": "cx"})
            done = await self.terminal(sdk, task.id)
            self.assertEqual(done.status.state, COMPLETED)

    async def test_nontext_part_rejected_without_native_send(self):
        async with self.app_context() as app, self.client(app) as sdk:
            message = new_text_message("text", role=Role.ROLE_USER)
            message.parts.append(Part(raw=b"file", media_type="application/octet-stream"))
            with self.assertRaises(Exception):
                async for _ in sdk.send_message(SendMessageRequest(message=message)):
                    pass
        self.assertEqual(self.calls, [])

    async def test_timeout_is_failed_and_never_redelivered(self):
        self.local["a2a"]["task_timeout"] = .1
        self.save()
        async with self.app_context() as app, self.client(app) as sdk:
            task = await self.send(sdk)
            task = await self.terminal(sdk, task.id)
            self.assertEqual(task.status.state, FAILED)
        self.assertEqual(len(self.calls), 1)

    @asynccontextmanager
    async def exchange_transport(self, app):
        original = httpx.AsyncClient
        def make_client(*args, **kwargs):
            return original(*args, transport=httpx.ASGITransport(app=app), **kwargs)
        with mock.patch.object(a2a_client.httpx, "AsyncClient", side_effect=make_client):
            yield

    def outgoing_request(self, mode="send"):
        return {"root": str(self.root), "peer": {"alias": "Target", "url": "http://127.0.0.1:8788/agents/Codex"},
                "token": TOKEN_A, "source": "source-chat", "mode": mode, "text": "7*9?",
                "message_id": str(uuid.uuid4()), "timeout": .08}

    async def test_outgoing_helper_sends_and_returns_standard_result(self):
        self.reply = "63"
        async with self.app_context() as app, self.exchange_transport(app):
            result = await a2a_client.exchange(self.outgoing_request())
            self.assertEqual(result["state"], "completed")
            self.assertEqual(result["reply"], "63")
        self.assertEqual(len(self.calls), 1)

    async def test_outgoing_pending_task_blocks_duplicate_then_status_reads_reply(self):
        async with self.app_context() as app, self.exchange_transport(app):
            first = await a2a_client.exchange(self.outgoing_request())
            self.assertEqual(first["state"], "accepted")
            again = await a2a_client.exchange(self.outgoing_request())
            self.assertEqual(again["state"], "not_sent")
            self.assertEqual(len(self.calls), 1)
            address = Mailbox(self.root).row(first["task_id"])["address"]
            Mailbox(self.root).submit(address, "later", {"alias": "Codex", "conversation_id": "cx"})
            await asyncio.sleep(.4)
            status = await a2a_client.exchange(self.outgoing_request(mode="status"))
            self.assertEqual(status["state"], "completed")
            self.assertEqual(status["reply"], "later")

    async def test_remote_card_cannot_redirect_bearer_to_another_origin(self):
        original = a2a_server.agent_card
        def foreign_card(alias, base_url):
            card = original(alias, base_url)
            card.supported_interfaces[0].url = "https://example.invalid/a2a/"
            return card
        with mock.patch.object(a2a_server, "agent_card", side_effect=foreign_card):
            async with self.app_context() as app, self.exchange_transport(app):
                request = self.outgoing_request()
                with self.assertRaisesRegex(ValueError, "未转送访问令牌"):
                    await a2a_client.exchange(request)
                self.assertFalse(request.get("attempted", False))
        self.assertEqual(self.calls, [])

    async def test_parallel_incoming_tasks_keep_separate_return_addresses(self):
        async with self.app_context() as app, self.client(app) as sdk:
            first, second = await asyncio.gather(self.send(sdk, "first"), self.send(sdk, "second"))
            await asyncio.sleep(.05)
            box = Mailbox(self.root)
            first_address = box.row(first.id)["address"]
            second_address = box.row(second.id)["address"]
            self.assertNotEqual(first_address, second_address)
            sender = {"alias": "Codex", "conversation_id": "cx"}
            box.submit(second_address, "answer second", sender)
            box.submit(first_address, "answer first", sender)
            first, second = await asyncio.gather(self.terminal(sdk, first.id), self.terminal(sdk, second.id))
            self.assertEqual(first.artifacts[0].parts[0].text, "answer first")
            self.assertEqual(second.artifacts[0].parts[0].text, "answer second")

    async def test_completed_batch_releases_registry_and_worker_tasks(self):
        self.reply = "short result"
        async with self.app_context() as app, self.client(app) as sdk:
            for index in range(20):
                task = await self.send(sdk, "batch " + str(index))
                await self.terminal(sdk, task.id)
            await asyncio.sleep(.1)
            for handler in app.app.state.handlers:
                registry = handler._active_task_registry
                self.assertEqual(len(registry._active_tasks), 0)
                self.assertEqual(len(registry._cleanup_tasks), 0)
                self.assertEqual(len(handler._background_tasks), 0)
            self.assertFalse(any(
                not task.done() and task.get_name().startswith(("producer:", "consumer:"))
                for task in asyncio.all_tasks()))

    async def test_shutdown_drains_working_tasks_before_database_close(self):
        async with self.app_context() as app, self.client(app) as sdk:
            task = await self.send(sdk)
            await asyncio.sleep(.05)
            fetched = await sdk.get_task(GetTaskRequest(id=task.id))
            self.assertEqual(fetched.status.state, WORKING)
            handlers = list(app.app.state.handlers)
            self.assertTrue(any(handler._active_task_registry._active_tasks for handler in handlers))
        for handler in handlers:
            self.assertEqual(len(handler._active_task_registry._active_tasks), 0)
            self.assertEqual(len(handler._active_task_registry._cleanup_tasks), 0)
        self.assertFalse(any(
            not task.done() and task.get_name().startswith(("producer:", "consumer:"))
            for task in asyncio.all_tasks()))

    async def test_completed_result_does_not_keep_input_history_or_mailbox_plaintext(self):
        self.reply = "temporary result"
        async with self.app_context() as app, self.client(app) as sdk:
            task = await self.send(sdk, "input body should not stay in SDK history")
            task = await self.terminal(sdk, task.id)
            self.assertEqual(list(task.history), [])
            self.assertEqual(task.artifacts[0].parts[0].text, self.reply)
            self.assertIsNone(Mailbox(self.root).row(task.id)["result"])

    async def test_metadata_count_and_age_are_bounded_while_waiting_task_survives(self):
        self.local["a2a"]["max_metadata"] = 3
        self.save()
        box = Mailbox(self.root)
        waiting_address = box.claim("waiting", "Codex", "cx", "caller-a", "waiting-message", "instance")
        for index in range(10):
            task_id = "finished-" + str(index)
            box.claim(task_id, "Codex", "cx", "caller-a", "message-" + str(index), "instance")
            box.seal(task_id, "done")
            box.save_outgoing("source-" + str(index), "http://fixture/", "context", task_id)
        box.cleanup()
        with box.connect() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM replies WHERE state<>'waiting'").fetchone()[0], 3)
            self.assertEqual(db.execute("SELECT count(*) FROM outgoing").fetchone()[0], 3)
        self.assertEqual(box.row("waiting")["address"], waiting_address)
        box.cleanup(now=time.time() + 86401)
        with box.connect() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM replies WHERE state<>'waiting'").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT count(*) FROM outgoing").fetchone()[0], 0)
        self.assertEqual(box.row("waiting")["state"], "waiting")

    async def test_mailbox_active_limit_releases_after_completion(self):
        self.local["a2a"]["max_active_tasks"] = 2
        self.save()
        box = Mailbox(self.root)
        box.claim("one", "Codex", "cx", "caller-a", "one", "instance")
        box.claim("two", "Codex", "cx", "caller-a", "two", "instance")
        with self.assertRaises(BridgeError):
            box.claim("three", "Codex", "cx", "caller-a", "three", "instance")
        box.seal("one", "done")
        box.claim("three", "Codex", "cx", "caller-a", "three", "instance")
        self.assertEqual(box.row("three")["state"], "waiting")

    async def test_concurrent_claims_cannot_exceed_active_limit(self):
        self.local["a2a"]["max_active_tasks"] = 1
        self.save()
        box = Mailbox(self.root)
        async def claim(index):
            try:
                await asyncio.to_thread(box.claim, "task-" + str(index), "Codex", "cx",
                                        "caller-a", "message-" + str(index), "instance")
                return True
            except BridgeError:
                return False
        results = await asyncio.gather(*(claim(index) for index in range(6)))
        self.assertEqual(sum(results), 1)
        with box.connect() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM replies WHERE state='waiting'").fetchone()[0], 1)

    async def test_result_size_limit_counts_utf8_bytes(self):
        self.local["a2a"]["max_text_bytes"] = 8
        self.save()
        box = Mailbox(self.root)
        address = box.claim("task", "Codex", "cx", "caller-a", "message", "instance")
        sender = {"alias": "Codex", "conversation_id": "cx"}
        with self.assertRaises(BridgeError):
            box.submit(address, "三个字", sender)
        self.assertIsNone(box.row("task")["result"])
        box.submit(address, "两个", sender)
        self.assertEqual(box.row("task")["result"], "两个")

    async def test_sealed_reply_can_be_repeated_without_storing_plaintext(self):
        box = Mailbox(self.root)
        address = box.claim("task", "Codex", "cx", "caller-a", "message", "instance")
        sender = {"alias": "Codex", "conversation_id": "cx"}
        box.submit(address, "short result", sender)
        box.seal("task", "done")
        self.assertIsNone(box.row("task")["result"])
        box.submit(address, "short result", sender)
        self.assertIsNone(box.row("task")["result"])
        with self.assertRaises(BridgeError):
            box.submit(address, "different", sender)

    async def test_expired_results_are_removed_and_next_message_can_be_sent(self):
        self.reply = "temporary result"
        async with self.app_context() as app, self.exchange_transport(app):
            first = await a2a_client.exchange(self.outgoing_request())
            self.assertEqual(first["state"], "completed")
            removed = await app.app.state.cleaner(now=time.time() + 601)
            self.assertGreaterEqual(removed, 1)
            box = Mailbox(self.root)
            previous = box.outgoing("source-chat", "http://127.0.0.1:8788/agents/Codex")
            self.assertEqual(previous["task_id"], "")
            self.assertIsNotNone(box.row(first["task_id"]))
            self.assertIsNone(box.row(first["task_id"])["result"])
            second = await a2a_client.exchange(self.outgoing_request())
            self.assertEqual(second["state"], "completed")
            self.assertNotEqual(first["task_id"], second["task_id"])
        self.assertEqual(len(self.calls), 2)

    async def test_remote_expiry_without_local_cleanup_does_not_block_new_outgoing(self):
        self.reply = "temporary result"
        async with self.app_context() as app, self.exchange_transport(app):
            first = await a2a_client.exchange(self.outgoing_request())
            store = app.app.state.handlers[0].task_store
            await store.delete(first["task_id"], ServerCallContext(user=a2a_server.Caller("caller-a")))
            old = Mailbox(self.root).outgoing("source-chat", a2a_client.continuity_key(self.outgoing_request()["peer"]))
            self.assertEqual(old["task_id"], first["task_id"])
            second = await a2a_client.exchange(self.outgoing_request())
            self.assertEqual(second["state"], "completed")
            self.assertNotEqual(first["task_id"], second["task_id"])
        self.assertEqual(len(self.calls), 2)

    async def test_unavailable_remote_does_not_forget_old_task_or_resend(self):
        self.reply = "temporary result"
        async with self.app_context() as app:
            async with self.exchange_transport(app):
                first = await a2a_client.exchange(self.outgoing_request())
            async def unavailable(scope, receive, send):
                if scope["type"] == "http" and scope["method"] == "POST":
                    return await a2a_server.JSONResponse({"error": "unavailable"}, 503)(scope, receive, send)
                return await app(scope, receive, send)
            async with self.exchange_transport(unavailable):
                request = self.outgoing_request()
                with self.assertRaises(Exception):
                    await a2a_client.exchange(request)
                self.assertFalse(request.get("attempted", False))
            previous = Mailbox(self.root).outgoing("source-chat", a2a_client.continuity_key(self.outgoing_request()["peer"]))
            self.assertEqual(previous["task_id"], first["task_id"])
        self.assertEqual(len(self.calls), 1)

    async def test_cleanup_preserves_inflight_task_and_reply_address(self):
        async with self.app_context() as app, self.client(app) as sdk:
            task = await self.send(sdk)
            await asyncio.sleep(.05)
            removed = await app.app.state.cleaner(now=time.time() + 601)
            self.assertEqual(removed, 0)
            fetched = await sdk.get_task(GetTaskRequest(id=task.id))
            self.assertEqual(fetched.status.state, WORKING)
            box = Mailbox(self.root)
            address = box.row(task.id)["address"]
            box.submit(address, "still valid", {"alias": "Codex", "conversation_id": "cx"})
            task = await self.terminal(sdk, task.id)
            self.assertEqual(task.artifacts[0].parts[0].text, "still valid")

    async def test_completed_task_count_is_bounded_without_removing_dedup_metadata(self):
        self.local["a2a"]["max_completed_tasks"] = 3
        self.save()
        self.reply = "short result"
        async with self.app_context() as app, self.client(app) as sdk:
            ids = []
            for index in range(8):
                task = await self.send(sdk, "item " + str(index))
                await self.terminal(sdk, task.id)
                ids.append(task.id)
            removed = await app.app.state.cleaner()
            self.assertEqual(removed, 5)
            found = 0
            for task_id in ids:
                try:
                    task = await sdk.get_task(GetTaskRequest(id=task_id))
                except Exception:
                    continue
                self.assertEqual(task.status.state, COMPLETED)
                found += 1
            self.assertEqual(found, 3)
            box = Mailbox(self.root)
            self.assertTrue(all(box.row(task_id) is not None for task_id in ids))

    async def test_oversize_input_and_http_body_rejected_before_native_dispatch(self):
        self.local["a2a"]["max_text_bytes"] = 8
        self.save()
        async with self.app_context() as app, self.client(app) as sdk:
            with self.assertRaises(Exception):
                await self.send(sdk, "三个字")
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                        headers={"Authorization": "Bearer " + TOKEN_A}) as http:
                response = await http.post("http://127.0.0.1:8788/agents/Codex/", content=b"x" * 17000)
                self.assertEqual(response.status_code, 413)
        self.assertEqual(self.calls, [])

    async def test_active_task_limit_rejects_sdk_request_before_native_dispatch(self):
        self.local["a2a"]["max_active_tasks"] = 2
        self.save()
        async with self.app_context() as app, self.client(app) as sdk:
            first = await self.send(sdk, "one")
            second = await self.send(sdk, "two")
            await asyncio.sleep(.05)
            with self.assertRaises(Exception):
                await self.send(sdk, "three")
            self.assertEqual(len(self.calls), 2)
            for task in (first, second):
                self.assertEqual((await sdk.get_task(GetTaskRequest(id=task.id))).status.state, WORKING)

    async def test_unicode_alias_card_route_and_sdk_task_delivery(self):
        self.peers["peers"]["数学伙伴 甲"] = {
            "software": "codex", "device_id": "test", "conversation_id": "math"}
        self.save()
        self.reply = "中文回复"
        async with self.app_context() as app, self.client(app, alias="数学伙伴 甲") as sdk:
            task = await self.send(sdk, "中文问题")
            task = await self.terminal(sdk, task.id)
            self.assertEqual(task.artifacts[0].parts[0].text, "中文回复")
        self.assertEqual(self.calls[0]["alias"], "数学伙伴 甲")


class LoggingTests(unittest.TestCase):
    def test_log_rotation_is_bounded_and_excludes_message_and_exception_text(self):
        import a2a_runtime
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            previous_level = logging.getLogger().level
            handler = a2a_runtime.configure_logging(root)
            secret = "DO-NOT-LOG-BODY-OR-TOKEN-" + str(uuid.uuid4())
            record = logging.LogRecord("httpx", logging.ERROR, __file__, 1, secret, (), None)
            record.task_id = "t" * 128
            record.error_type = "ValueError"
            record.exc_info = (ValueError, ValueError(secret), None)
            try:
                self.assertEqual(handler.maxBytes, 1024 * 1024)
                self.assertEqual(handler.backupCount, 2)
                for _ in range(16000):
                    handler.emit(record)
                handler.flush()
                paths = list((root / "runtime").glob("a2a.log*"))
                self.assertEqual(len(paths), 3)
                for path in paths:
                    self.assertLessEqual(path.stat().st_size, 1024 * 1024)
                    self.assertNotIn(secret, path.read_text(encoding="utf8"))
            finally:
                logging.getLogger().removeHandler(handler)
                handler.close()
                logging.getLogger().setLevel(previous_level)


if __name__ == "__main__":
    unittest.main()
