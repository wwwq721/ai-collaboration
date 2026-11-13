"""SDK tasks cross the private device channel, without an AI or real phone."""
import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
import platform
import sys
import tempfile
import time
import unittest

import httpx
from a2a.client import A2ACardResolver, ClientConfig, ClientFactory
from a2a.helpers import new_text_message
from a2a.types import GetTaskRequest, Role, SendMessageRequest, TaskState

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import a2a_server
from a2a_state import Mailbox, Limits
from device_queue import DeviceQueue
from workbuddy import BridgeError

LOCAL = "local-test-" + "x"*40
PHONE = "phone-test-" + "y"*40
OTHER = "other-test-" + "z"*40


class DeviceChannelTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.local = {"machine_name": platform.node(), "device_id": "pc", "a2a": {
            "tokens": {"local": LOCAL}, "device_tokens": {"phone": PHONE, "other": OTHER},
            "device_permissions": {"phone": ["Minis"], "other": []},
            "task_timeout": 2, "delivery_timeout": 1}}
        self.book = {"version": 1, "peers": {
            "Minis": {"software": "minis", "device_id": "phone", "conversation_id": "mobile-chat"},
            "Codex": {"software": "codex", "device_id": "pc", "conversation_id": "cx"}}}
        self.save()

    def save(self):
        (self.root / "local.json").write_text(json.dumps(self.local), encoding="utf8")
        (self.root / "peers.json").write_text(json.dumps(self.book), encoding="utf8")

    async def asyncTearDown(self):
        self.tmp.cleanup()

    @asynccontextmanager
    async def app(self, instance="one"):
        async def never_native(*args, **kwargs):
            raise AssertionError("Remote phone must not use a desktop native interface")
        app = a2a_server.create_app(self.root, instance, deliver=never_native)
        async with app.app.router.lifespan_context(app.app):
            yield app

    def http(self, app, token=PHONE):
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788",
                                 headers={"Authorization": "Bearer " + token})

    async def sdk(self, http):
        card = await A2ACardResolver(http, "http://127.0.0.1:8788/agents/Minis").get_agent_card()
        return ClientFactory(ClientConfig(httpx_client=http, streaming=False, polling=True,
                                         supported_protocol_bindings=["JSONRPC"])).create(card)

    async def send(self, sdk):
        message = new_text_message("检查 7×13", role=Role.ROLE_USER)
        task = None
        async for event in sdk.send_message(SendMessageRequest(message=message)):
            if event.HasField("task"):
                task = event.task
        self.assertIsNotNone(task)
        return task

    async def delivery(self, http):
        for _ in range(30):
            value = (await http.get("/devices/phone/next?wait=0")).json()
            if value["state"] == "delivery":
                return value
            await asyncio.sleep(.02)
        self.fail("delivery missing")

    async def terminal(self, sdk, task):
        for _ in range(100):
            task = await sdk.get_task(GetTaskRequest(id=task.id))
            if task.status.state in {TaskState.TASK_STATE_COMPLETED, TaskState.TASK_STATE_FAILED}:
                return task
            await asyncio.sleep(.03)
        self.fail("no terminal task")

    async def test_phone_poll_ack_and_correlated_result_complete_official_task(self):
        async with self.app() as app, self.http(app, LOCAL) as caller, self.http(app) as phone:
            sdk = await self.sdk(caller)
            task = await self.send(sdk)
            delivery = await self.delivery(phone)
            self.assertEqual(delivery["conversation_id"], "mobile-chat")
            self.assertIn("send.sh " + delivery["reply_to"], delivery["text"])
            self.assertEqual((await phone.get("/devices/phone/next?wait=0")).json()["delivery_id"], task.id)
            ack = {"delivery_id": task.id, "state": "accepted", "scheduled_task_id": "native-task"}
            self.assertEqual((await phone.post("/devices/phone/ack", json=ack)).status_code, 200)
            self.assertEqual((await phone.post("/devices/phone/ack", json=ack)).status_code, 200)
            self.assertEqual((await phone.get("/devices/phone/next?wait=0")).json()["state"], "empty")
            payload = {"address": delivery["reply_to"], "text": "91", "sender": {"alias": "Minis", "conversation_id": "mobile-chat"}}
            self.assertEqual((await phone.post("/devices/phone/reply", json=payload)).status_code, 200)
            self.assertEqual((await phone.post("/devices/phone/reply", json=payload)).status_code, 200)
            done = await self.terminal(sdk, task)
            self.assertEqual(done.status.state, TaskState.TASK_STATE_COMPLETED)
            self.assertEqual(done.artifacts[0].parts[0].text, "91")
            self.assertEqual((await phone.post("/devices/phone/reply", json=payload)).status_code, 200)
            with Mailbox(self.root).connect() as db:
                self.assertIsNone(db.execute("SELECT text FROM device_deliveries WHERE delivery_id=?", (task.id,)).fetchone()[0])
            self.assertEqual((await phone.post("/devices/phone/ack", json=ack)).status_code, 200)

    async def test_reply_requires_matching_device_delivery_and_rejects_old_instance(self):
        box = Mailbox(self.root)
        orphan = box.claim("orphan", "Minis", "mobile-chat", "device:phone", "orphan-msg", "one")
        cross_device = box.claim("other-device-task", "Minis", "mobile-chat", "device:phone", "other-msg", "one")
        queue = DeviceQueue(box)
        queue.enqueue("other", "Minis", "mobile-chat", "for other device", "other-device-task", cross_device, 30)
        async with self.app("one") as app, self.http(app) as phone:
            for address in (orphan, cross_device):
                response = await phone.post("/devices/phone/reply", json={"address": address, "text": "forged",
                    "sender": {"alias": "Minis", "conversation_id": "mobile-chat"}})
                self.assertIn(response.status_code, {403, 409})
            self.assertIsNone(box.row("orphan")["result"])
            self.assertIsNone(box.row("other-device-task")["result"])

        stale = box.claim("old-instance-task", "Minis", "mobile-chat", "device:phone", "old-msg", "old-instance")
        queue.enqueue("phone", "Minis", "mobile-chat", "stale message", "old-instance-task", stale, 30)
        async with self.app("new-instance") as app, self.http(app) as phone:
            response = await phone.post("/devices/phone/reply", json={"address": stale, "text": "too late",
                "sender": {"alias": "Minis", "conversation_id": "mobile-chat"}})
            self.assertIn(response.status_code, {403, 409})
            self.assertIsNone(box.row("old-instance-task")["result"])

    async def test_other_device_and_wrong_chat_cannot_read_or_reply(self):
        async with self.app() as app, self.http(app, LOCAL) as caller, self.http(app) as phone, self.http(app, OTHER) as other:
            task = await self.send(await self.sdk(caller))
            delivery = await self.delivery(phone)
            self.assertEqual((await other.get("/devices/phone/next?wait=0")).status_code, 403)
            self.assertEqual((await caller.get("/devices/phone/next?wait=0")).status_code, 403)
            payload = {"address": delivery["reply_to"], "text": "wrong", "sender": {"alias": "Codex", "conversation_id": "cx"}}
            self.assertEqual((await phone.post("/devices/phone/reply", json=payload)).status_code, 403)
            payload["sender"] = {"alias": "Minis", "conversation_id": "wrong"}
            self.assertEqual((await phone.post("/devices/phone/reply", json=payload)).status_code, 403)
            self.assertEqual((await phone.post("/shutdown", json={})).status_code, 403)
            self.assertIsNone(Mailbox(self.root).row(task.id)["result"])

    async def test_uncertain_native_ack_is_not_redelivered_but_real_reply_can_complete(self):
        async with self.app() as app, self.http(app, LOCAL) as caller, self.http(app) as phone:
            sdk = await self.sdk(caller)
            task = await self.send(sdk)
            value = await self.delivery(phone)
            await phone.post("/devices/phone/ack", json={"delivery_id": task.id, "state": "uncertain"})
            self.assertEqual((await phone.get("/devices/phone/next?wait=0")).json()["state"], "empty")
            await phone.post("/devices/phone/reply", json={"address": value["reply_to"], "text": "91", "sender": {"alias": "Minis", "conversation_id": "mobile-chat"}})
            self.assertEqual((await self.terminal(sdk, task)).status.state, TaskState.TASK_STATE_COMPLETED)

    async def test_refused_native_delivery_fails_without_repeating(self):
        async with self.app() as app, self.http(app, LOCAL) as caller, self.http(app) as phone:
            sdk = await self.sdk(caller)
            task = await self.send(sdk)
            await self.delivery(phone)
            await phone.post("/devices/phone/ack", json={"delivery_id": task.id, "state": "not_sent"})
            self.assertEqual((await self.terminal(sdk, task)).status.state, TaskState.TASK_STATE_FAILED)
            self.assertEqual((await phone.get("/devices/phone/next?wait=0")).json()["state"], "empty")

    async def test_agent_card_is_canonical_https_for_network_and_loopback_for_local(self):
        self.local["a2a"]["public_base_url"] = "https://example.tailnet.ts.net"
        self.save()
        async with self.app() as app, self.http(app, LOCAL) as local, self.http(app) as network:
            path = "/agents/Minis/.well-known/agent-card.json"
            local_card = (await local.get(path)).json()
            remote_card = (await network.get(path, headers={"Host": "attacker.example"})).json()
            self.assertEqual(local_card["supportedInterfaces"][0]["url"], "http://127.0.0.1:8788/agents/Minis/")
            self.assertEqual(remote_card["supportedInterfaces"][0]["url"], "https://example.tailnet.ts.net/agents/Minis/")

    async def test_recovered_abandoned_queue_never_redelivers_after_restart(self):
        box = Mailbox(self.root)
        address = box.claim("abandoned", "Minis", "mobile-chat", "local", "msg", "old")
        queue = DeviceQueue(box)
        queue.enqueue("phone", "Minis", "mobile-chat", "temporary text", "abandoned", address, 30)
        box.finish("abandoned", "interrupted")
        async with self.app("new") as app, self.http(app) as phone:
            self.assertEqual((await phone.get("/devices/phone/next?wait=0")).json()["state"], "empty")
        with box.connect() as db:
            self.assertIsNone(db.execute("SELECT text FROM device_deliveries").fetchone()[0])


class QueueBoundsTests(unittest.TestCase):
    def test_device_poll_cleanup_is_scoped_but_global_cleanup_remains_global(self):
        with tempfile.TemporaryDirectory() as tmp:
            box = Mailbox(tmp)
            q = DeviceQueue(box)
            first = box.claim("task-a", "A", "chat-a", "owner", "msg-a", "instance")
            second = box.claim("task-b", "B", "chat-b", "owner", "msg-b", "instance")
            q.enqueue("device-a", "A", "chat-a", "private-a", "task-a", first, 30)
            q.enqueue("device-b", "B", "chat-b", "private-b", "task-b", second, 30)

            # Simulate both deliveries past their expiry without waiting in wall time.
            future = time.time() + 31
            self.assertEqual(q.next("device-a", now=future)["state"], "empty")
            with box.connect() as db:
                row = db.execute("SELECT state,text FROM device_deliveries WHERE delivery_id='task-b'").fetchone()
                self.assertEqual((row["state"], row["text"]), ("queued", "private-b"))

            q.cleanup(now=future)
            with box.connect() as db:
                row = db.execute("SELECT state,text FROM device_deliveries WHERE delivery_id='task-b'").fetchone()
                self.assertEqual((row["state"], row["text"]), ("closed", None))

    def test_queued_bodies_expire_and_metadata_is_bounded(self):
        with tempfile.TemporaryDirectory() as tmp:
            box = Mailbox(tmp, Limits(max_active_tasks=2, max_metadata=1, metadata_ttl_seconds=2))
            q = DeviceQueue(box)
            address = box.claim("one", "Minis", "chat", "owner", "msg", "instance")
            q.enqueue("phone", "Minis", "chat", "private", "one", address, .1)
            self.assertEqual(q.next("other")["state"], "empty")
            self.assertEqual(q.next("phone")["delivery_id"], "one")
            q.cleanup(now=time.time()+1)
            self.assertEqual(q.next("phone")["state"], "empty")
            with box.connect() as db:
                self.assertIsNone(db.execute("SELECT text FROM device_deliveries").fetchone()[0])
            q.cleanup(now=time.time()+5)
            with box.connect() as db:
                self.assertEqual(db.execute("SELECT count(*) FROM device_deliveries").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
