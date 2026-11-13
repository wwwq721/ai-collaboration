"""UI-free SDK delivery keeps source, task ownership and one-use permits bound."""
import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
import platform
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid

import httpx
from a2a.client import A2ACardResolver, ClientConfig, ClientFactory
from a2a.helpers import new_text_message
from a2a.types import GetTaskRequest, Role, SendMessageRequest, TaskState

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from a2a_server import create_app
from a2a_state import Mailbox
from delivery_ticket import text_hash
from managed_bridge import notice_id_for
from workbuddy import BridgeError


class ManagedIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.public = "https://desk.example.test:18443"
        self.loopback = "http://127.0.0.1:8788"
        self.tokens = {alias: (alias + "-fixture") * 7 for alias in ("Codex", "WorkBuddy", "phone", "local")}
        self.peers = {alias: dict(device_id=dev, software=sw, conversation_id=chat, verified=True)
                      for alias, dev, sw, chat in (("Codex", "pc", "codex", "a"), ("WorkBuddy", "pc", "workbuddy", "b"),
                                                    ("Minis", "phone", "minis", "c"))}
        bindings = {k: {f: p[f] for f in ("device_id", "software", "conversation_id")}
                    for k, p in self.peers.items() if k in {"Codex", "WorkBuddy"}}
        url = "https://research.example.test/agents/research"
        self.peers["Research"] = {"device_id": "external-box", "software": "research",
                                  "conversation_id": "verified-research-chat", "transport": "a2a",
                                  "url": url, "verified_url": url, "verified": True,
                                  "verified_ref": ["external-box","research","verified-research-chat"]}
        local = {"machine_name": platform.node(), "device_id": "pc", "a2a": {
            "tokens": {"local": self.tokens["local"]}, "device_tokens": {"phone": self.tokens["phone"]},
            "chat_tokens": {k: self.tokens[k] for k in bindings}, "chat_bindings": bindings,
            "managed_ui": True, "public_base_url": self.public, "task_timeout": 7,
            "device_permissions": {"phone": ["Codex"]}, "idle_wait_seconds": 1},
            "a2a_credentials":{"Research":{"token":"external-fixture-"*4}}}
        (self.root / "local.json").write_text(json.dumps(local))
        (self.root / "peers.json").write_text(json.dumps({"version": 1, "peers": self.peers}))
        self.mailbox = Mailbox(self.root)
        self.prepare, self.go = asyncio.Event(), asyncio.Event()
        self.go.set()
        self.submitted = []

        async def native(root, local, alias, text, msg, wait, *, permit):
            self.prepare.set()
            await self.go.wait()
            async with self.http() as h:
                data = {**permit, "target": self.peers[alias], "message_id": msg, "text_hash": text_hash(text)}
                first = await h.post("/collaboration/consume", json=data)
                if first.status_code != 200:
                    raise BridgeError("切断前权限已撤销")
                repeated = await h.post("/collaboration/consume", json=data)
                self.assertEqual(repeated.status_code, 403)
            self.submitted.append(msg)
            return {"state": "accepted"}
        self.app = create_app(self.root, "fixture-instance", native)
        self.life = self.app.app.router.lifespan_context(self.app.app)
        await self.life.__aenter__()

    async def asyncTearDown(self):
        self.go.set()
        await self.life.__aexit__(None, None, None)
        self.temp.cleanup()

    def http(self, caller=None, public=False):
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url=self.public if public else self.loopback,
                                 headers={"Authorization": "Bearer " + self.tokens[caller]} if caller else {})

    def revoke(self, alias):
        # Identity removal remains effective immediately before native handoff.
        self.app.app.state.policy.peers[alias.casefold()]["verified"] = False

    @asynccontextmanager
    async def sdk(self, caller, target):
        async with self.http(caller, public=caller=="phone") as client:
            base = self.public if caller=="phone" else self.loopback
            card = await A2ACardResolver(client, base + "/agents/" + target).get_agent_card()
            self.assertTrue(card.supported_interfaces[0].url.startswith(base + "/"))
            agent = ClientFactory(ClientConfig(httpx_client=client, streaming=False, polling=True,
                                              supported_protocol_bindings=["JSONRPC"])).create(card)
            yield agent

    async def send(self, client, body, claim=None):
        msg = new_text_message(body, role=Role.ROLE_USER)
        if claim:
            msg.metadata.update({"collaboration_origin": self.peers[claim]})
        stream = client.send_message(SendMessageRequest(message=msg))
        return [e.task async for e in stream if e.HasField("task")][-1]

    async def terminal(self, sdk, uid):
        for _ in range(180):
            task = await sdk.get_task(GetTaskRequest(id=uid))
            if task.status.state in (TaskState.TASK_STATE_COMPLETED, TaskState.TASK_STATE_FAILED):
                return task
            await asyncio.sleep(.025)
        self.fail("task not terminal")

    async def test_ui_and_password_routes_are_not_installed(self):
        paths = {"/ui", "/ui/", "/ui/app.js", "/ui/style.css", "/ui/api/session",
                 "/ui/api/chats", "/ui/api/rules", "/ui/api/messages",
                 "/devices/phone/management-status"}
        async with self.http("phone", public=True) as client:
            for path in paths:
                self.assertEqual((await client.get(path)).status_code, 404, path)
            self.assertEqual((await client.post("/ui/api/session", json={"password": "unused"})).status_code, 404)
        self.assertFalse(hasattr(self.app.app.state, "activity"))

    async def test_phone_can_send_and_receive_only_its_result_without_ui(self):
        async with self.sdk("phone", "Codex") as phone:
            task = await self.send(phone, "6 times 7", "Minis")
            await asyncio.wait_for(self.prepare.wait(), 3)
            for _ in range(100):
                if task.id in self.submitted:
                    break
                await asyncio.sleep(.01)
            self.assertEqual(self.submitted, [task.id])
            address = self.mailbox.row(task.id)["address"]
            async with self.http("WorkBuddy") as other, self.http("Codex") as recipient:
                body = {"address": address, "text": "42"}
                self.assertEqual((await other.post("/collaboration/reply", json=body)).status_code, 403)
                self.assertEqual((await recipient.post("/collaboration/reply", json=body)).status_code, 200)
            result = await self.terminal(phone, task.id)
            self.assertEqual(result.artifacts[0].parts[0].text, "42")
            async with self.sdk("WorkBuddy", "Codex") as other:
                with self.assertRaises(Exception):
                    await other.get_task(GetTaskRequest(id=task.id))
        row = self.mailbox.row(task.id)
        payload = {"task_id": task.id, "notification_id": notice_id_for(self.peers["Minis"], row["message_id"]), "state":"accepted"}
        async with self.http("phone", public=True) as remote:
            for _ in range(2):
                self.assertEqual((await remote.post("/devices/phone/notification", json=payload)).status_code, 200)
            self.assertEqual((await remote.get("/ui/api/messages")).status_code, 404)
        self.assertFalse(hasattr(self.app.app.state, "activity"))

    async def test_unregistered_target_after_queue_offer_blocks_final_handoff_and_reply(self):
        async with self.sdk("Codex","Minis") as sdk, self.http("phone", public=True) as device:
            task = await self.send(sdk, "pending")
            self.assertEqual((await device.get("/devices/phone/next?wait=0")).status_code,409)
            msg = (await device.get("/devices/phone/next?wait=2&policy=2")).json()
            self.assertTrue(msg["require_authorize"])
            self.revoke("Minis")
            response = await device.post("/devices/phone/authorize",json={"delivery_id":task.id})
            self.assertEqual(response.status_code,403)
            self.assertEqual((await device.post("/devices/phone/reply",json={"address":msg["reply_to"],"text":"forged"})).status_code,403)
            self.assertEqual((await self.terminal(sdk,task.id)).status.state,TaskState.TASK_STATE_FAILED)

    async def test_authentic_queued_delivery_gets_one_permit_and_reply_is_still_bound(self):
        async with self.sdk("WorkBuddy","Minis") as sdk, self.http("phone",public=True) as device:
            task = await self.send(sdk,"allowed")
            msg = (await device.get("/devices/phone/next?wait=2&policy=2")).json()
            for status in (200,403):
                res = await device.post("/devices/phone/authorize", json={"delivery_id":task.id})
                self.assertEqual(res.status_code,status)
            self.assertEqual((await device.post("/devices/phone/ack",json={"delivery_id":task.id,"state":"accepted"})).status_code,200)
            self.assertEqual((await device.post("/devices/phone/reply",json={"address":msg["reply_to"],"text":"done"})).status_code,200)
            self.assertEqual((await self.terminal(sdk,task.id)).artifacts[0].parts[0].text,"done")

    async def test_local_final_consume_rechecks_identity_after_initial_permit(self):
        self.go.clear()
        async with self.sdk("Codex","WorkBuddy") as sdk:
            task = await self.send(sdk,"wait")
            await asyncio.wait_for(self.prepare.wait(),3)
            self.revoke("WorkBuddy")
            self.go.set()
            self.assertEqual((await self.terminal(sdk,task.id)).status.state,TaskState.TASK_STATE_FAILED)
            self.assertEqual(self.submitted,[])

    async def test_shared_owner_or_claimed_sibling_cannot_use_another_chat(self):
        async with self.http("local") as owner:
            self.assertEqual((await owner.get("/agents/Codex/.well-known/agent-card.json")).status_code,403)
        async with self.sdk("WorkBuddy","Codex") as sdk:
            with self.assertRaises(Exception):
                await self.send(sdk, "spoof", "Minis")
        self.assertEqual(self.submitted,[])

    async def test_verified_standard_partner_accepts_any_registered_chat(self):
        seen = []
        async def external(root, local, peer, source, raw_text, local_task_id, approve, *, task_timeout):
            self.assertEqual(peer["conversation_id"], "verified-research-chat")
            self.assertIn(source["alias"], {"Minis", "Codex"})
            self.assertTrue(approve())
            self.assertFalse(approve())
            seen.append(raw_text)
            return {"state":"completed", "reply":"standard result", "task_id":"external-task"}
        with patch("managed_egress.deliver", side_effect=external):
            async with self.sdk("phone","Research") as sdk:
                task = await self.send(sdk,"plain task for the other software")
                self.assertEqual((await self.terminal(sdk,task.id)).artifacts[0].parts[0].text,"standard result")
            async with self.sdk("Codex","Research") as sdk:
                task = await self.send(sdk,"another registered source")
                self.assertEqual((await self.terminal(sdk,task.id)).artifacts[0].parts[0].text,"standard result")
        self.assertEqual(seen,["plain task for the other software", "another registered source"])
