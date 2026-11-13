"""Background delivery behavior through the official A2A SDK and ASGI app."""
import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
import platform
import sys
import tempfile
import unittest
import uuid
from unittest import mock

import httpx
from a2a.client import A2ACardResolver, ClientConfig, ClientFactory
from a2a.types import GetTaskRequest, Role, SendMessageRequest, TaskState
from a2a.helpers import new_text_message
from a2a.types import SendMessageResponse
from google.protobuf.json_format import MessageToDict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import a2a_client
import a2a_server
from a2a_state import Mailbox
from host_adapters import ChatRef
from result_watcher import PendingResults, ResultWatcher

TOKEN = "background-test-token-" + "b" * 32


class BackgroundDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.client_tmp = tempfile.TemporaryDirectory()
        self.client_root = Path(self.client_tmp.name)
        self.local = {"machine_name": platform.node(), "device_id": "origin-device", "a2a": {
            "tokens": {"local": TOKEN}, "task_timeout": 2, "idle_wait_seconds": 1},
            "a2a_credentials": {"Codex": {"token": TOKEN}}}
        self.server_local = {"machine_name": platform.node(), "device_id": "receiver-device", "a2a": {
            "tokens": {"caller": TOKEN}, "task_timeout": 2, "idle_wait_seconds": 1}}
        self.peers = {"version": 1, "peers": {
            "Codex": {"software": "codex", "device_id": "receiver-device", "conversation_id": "receiver-chat"}}}
        self.save(self.root, self.server_local, self.peers)
        self.save(self.client_root, self.local, {"version": 1, "peers": {}})
        self.native_calls = []

    def save(self, root, local, peers):
        (root / "local.json").write_text(json.dumps(local), encoding="utf8")
        (root / "peers.json").write_text(json.dumps(peers), encoding="utf8")

    async def asyncTearDown(self):
        self.tmp.cleanup()
        self.client_tmp.cleanup()

    async def native(self, root, local, alias, text, message_id, wait_idle):
        self.native_calls.append({"alias": alias, "text": text, "message_id": message_id})

    @asynccontextmanager
    async def app(self):
        app = a2a_server.create_app(self.root, "background-test", self.native)
        async with app.app.router.lifespan_context(app.app):
            yield app

    @asynccontextmanager
    async def exchange_transport(self, app):
        original = httpx.AsyncClient

        def make_client(*args, **kwargs):
            kwargs["transport"] = httpx.ASGITransport(app=app)
            return original(*args, **kwargs)

        with mock.patch.object(a2a_client.httpx, "AsyncClient", side_effect=make_client):
            yield

    @asynccontextmanager
    async def immediate_message_transport(self, app):
        original = httpx.AsyncClient

        class MessageTransport(httpx.AsyncBaseTransport):
            def __init__(self):
                self.asgi = httpx.ASGITransport(app=app)

            async def handle_async_request(inner, request):
                if request.method != "POST":
                    return await inner.asgi.handle_async_request(request)
                body = json.loads(request.content)
                result = SendMessageResponse(message=new_text_message("instant A2A reply"))
                return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"],
                                                 "result": MessageToDict(result)})

            async def aclose(inner):
                await inner.asgi.aclose()

        def make_client(*args, **kwargs):
            kwargs["transport"] = MessageTransport()
            return original(*args, **kwargs)

        with mock.patch.object(a2a_client.httpx, "AsyncClient", side_effect=make_client):
            yield

    async def sdk(self, app):
        http = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788",
                                 headers={"Authorization": "Bearer " + TOKEN})
        self.addAsyncCleanup(http.aclose)
        card = await A2ACardResolver(http, "http://127.0.0.1:8788/agents/Codex").get_agent_card()
        return ClientFactory(ClientConfig(httpx_client=http, streaming=False, polling=True,
                                         supported_protocol_bindings=["JSONRPC"])).create(card)

    def request(self, origin, mode="send", message_id=None):
        peer = {"alias": "Codex", "url": "http://127.0.0.1:8788/agents/Codex",
                "device_id": "receiver-device", "software": "a2a", "host_software": "codex",
                "transport": "a2a", "conversation_id": "receiver-chat"}
        return {"root": str(self.client_root), "local": self.local, "peer": peer, "token": TOKEN,
                "source": "legacy-source", "origin": origin, "mode": mode, "text": "Solve this later",
                "message_id": message_id or str(uuid.uuid4()), "timeout": 0.1, "background": True}

    async def finish(self, sdk, task_id, text="answer"):
        row = Mailbox(self.root).row(task_id)
        Mailbox(self.root).submit(row["address"], text,
                                  {"alias": "Codex", "conversation_id": "receiver-chat"})
        for _ in range(100):
            task = await sdk.get_task(GetTaskRequest(id=task_id))
            if task.status.state in {TaskState.TASK_STATE_COMPLETED, TaskState.TASK_STATE_FAILED}:
                return task
            await asyncio.sleep(.02)
        self.fail("remote task did not finish")

    async def test_background_exchange_delivers_once_to_full_origin_chat(self):
        origin = {"device_id": "origin-device", "software": "workbuddy", "conversation_id": "original-thread"}
        delivered = []

        async def deliver(chat, text, notification_id):
            delivered.append((chat, text, notification_id))
            return {"state": "accepted", "route": "original-thread"}

        async with self.app() as app, self.exchange_transport(app):
            result = await a2a_client.exchange(self.request(origin))
            self.assertEqual(result["state"], "accepted")
            self.assertTrue(result["background"])
            self.assertEqual(result["to"], "Codex")
            self.assertTrue(result["job_id"])
            sdk = await self.sdk(app)
            await self.finish(sdk, result["task_id"], "background answer")
            watcher = ResultWatcher(self.client_root, self.local, [self.request(origin)["peer"]], deliver=deliver)
            self.assertEqual(await watcher.tick(), 1)
            self.assertEqual(len(delivered), 1)
            self.assertEqual(delivered[0][0], origin)
            self.assertIn("background answer", delivered[0][1])
            self.assertEqual(await watcher.tick(), 0)
            self.assertEqual(len(delivered), 1)
            self.assertEqual(PendingResults(self.client_root).get(result["job_id"])["state"], "notified")
        self.assertEqual(len(self.native_calls), 1)

    async def test_synchronous_completed_send_is_not_notified_later(self):
        origin = {"device_id": "origin-device", "software": "codex", "conversation_id": "same-session"}
        async def immediate(root, local, alias, text, message_id, wait_idle):
            import re
            address = re.search(r"A2A-[0-9a-f]{12}", text).group()
            Mailbox(root).submit(address, "inline answer", {"alias": "Codex", "conversation_id": "receiver-chat"})

        app = a2a_server.create_app(self.root, "inline-test", immediate)
        async with app.app.router.lifespan_context(app.app), self.exchange_transport(app):
            request = self.request(origin)
            request.update(background=False, timeout=3)
            result = await a2a_client.exchange(request)
            self.assertEqual(result["state"], "completed")
            self.assertEqual(result["reply"], "inline answer")
            notices = []
            watcher = ResultWatcher(self.client_root, self.local, [self.request(origin)["peer"]],
                                    deliver=lambda *args: notices.append(args))
            await watcher.tick()
            self.assertEqual(notices, [])

    async def test_background_send_with_immediate_a2a_message_is_not_notified_later(self):
        origin = {"device_id": "origin-device", "software": "codex", "conversation_id": "instant-thread"}
        async with self.app() as app, self.immediate_message_transport(app):
            request = self.request(origin)
            result = await a2a_client.exchange(request)
            self.assertEqual(result["state"], "completed")
            self.assertEqual(result["reply"], "instant A2A reply")
            self.assertEqual(PendingResults(self.client_root).get(request["message_id"])["state"], "inline")
            notices = []
            watcher = ResultWatcher(self.client_root, self.local, [self.request(origin)["peer"]],
                                    deliver=lambda *args: notices.append(args))
            await watcher.tick()
            self.assertEqual(notices, [])

    async def test_task_status_reads_explicit_task_without_touching_outgoing_or_sending(self):
        origin = {"device_id": "origin-device", "software": "codex", "conversation_id": "status-thread"}
        async with self.app() as app, self.exchange_transport(app):
            first = await a2a_client.exchange(self.request(origin))
            box = Mailbox(self.client_root)
            before = box.outgoing(json.dumps(ChatRef(**origin).to_dict(), sort_keys=True, separators=(",", ":")),
                                  a2a_client.continuity_key(self.request(origin)["peer"]))
            status_request = self.request(origin, mode="task-status")
            status_request["task_id"] = first["task_id"]
            status = await a2a_client.exchange(status_request)
            after = box.outgoing(json.dumps(ChatRef(**origin).to_dict(), sort_keys=True, separators=(",", ":")),
                                 a2a_client.continuity_key(self.request(origin)["peer"]))
            self.assertEqual(status["task_id"], first["task_id"])
            self.assertEqual(status["state"], "accepted")
            self.assertEqual((before["context_id"], before["task_id"]), (after["context_id"], after["task_id"]))
            self.assertEqual(len(self.native_calls), 1)

    async def test_same_conversation_id_isolated_by_device_and_software(self):
        first = self.request({"device_id": "d1", "software": "codex", "conversation_id": "shared"})
        second = self.request({"device_id": "d2", "software": "codex", "conversation_id": "shared"})
        third = self.request({"device_id": "d1", "software": "workbuddy", "conversation_id": "shared"})
        async with self.app() as app, self.exchange_transport(app):
            one = await a2a_client.exchange(first)
            two = await a2a_client.exchange(second)
            three = await a2a_client.exchange(third)
            self.assertEqual(len({one["task_id"], two["task_id"], three["task_id"]}), 3)
            self.assertEqual(len(self.native_calls), 3)

    async def test_standard_a2a_peer_keeps_host_software_identity_for_watcher(self):
        peer = {"alias": "NewApp", "device_id": "peer-device", "software": "newapp",
                "transport": "a2a", "conversation_id": "newapp-chat", "url": "https://peer.invalid/a2a"}
        watcher = ResultWatcher(self.client_root, self.local, [peer], deliver=lambda *args: None)
        resolved = watcher._peer({"alias": "NewApp", "device_id": "peer-device", "software": "a2a",
                                  "host_software": "newapp", "conversation_id": "newapp-chat"})
        self.assertEqual(resolved["software"], "a2a")
        self.assertEqual(resolved["host_software"], "newapp")
        self.assertEqual(resolved["url"], "https://peer.invalid/a2a")


if __name__ == "__main__":
    unittest.main()
