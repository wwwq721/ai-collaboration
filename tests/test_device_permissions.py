"""Paired device authentication and ownership survive removal of software ACLs."""
import asyncio
from contextlib import asynccontextmanager
import io
import json
from pathlib import Path
import platform
import sys
import tempfile
import unittest
import zipfile

import httpx
from a2a.client import A2ACardResolver, ClientConfig, ClientFactory
from a2a.helpers import new_text_message
from a2a.types import GetTaskRequest, Role, SendMessageRequest, TaskState
from a2a.utils.errors import InvalidParamsError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import a2a_server
from a2a_state import Mailbox
from delivery_ticket import text_hash

LOCAL = "owner-token-" + "l" * 32
PHONE = "phone-token-" + "p" * 32
OTHER = "other-token-" + "o" * 32
CODEX = "codex-chat-token-" + "c" * 32
WORKBUDDY = "workbuddy-chat-token-" + "w" * 32


class DevicePermissionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.local = {"machine_name": platform.node(), "device_id": "desktop", "a2a": {
            "tokens": {"local": LOCAL}, "device_tokens": {"phone": PHONE, "other": OTHER},
            "device_permissions": {"phone": ["Codex"], "other": []},
            "managed_ui": True,
            "chat_tokens": {"Codex": CODEX, "WorkBuddy": WORKBUDDY},
            "chat_bindings": {
                "Codex": {"software": "codex", "device_id": "desktop", "conversation_id": "codex-chat"},
                "WorkBuddy": {"software": "workbuddy", "device_id": "desktop", "conversation_id": "workbuddy-chat"}},
            "task_timeout": 2, "idle_wait_seconds": 5}}
        self.peers = {"version": 1, "peers": {
            "Codex": {"software": "codex", "device_id": "desktop", "conversation_id": "codex-chat", "verified": True},
            "WorkBuddy": {"software": "workbuddy", "device_id": "desktop", "conversation_id": "workbuddy-chat", "verified": True},
            "Minis": {"software": "minis", "device_id": "phone", "conversation_id": "phone-chat", "verified": True}}}
        self.save()
        self.calls = []

    def save(self):
        (self.root / "local.json").write_text(json.dumps(self.local), encoding="utf8")
        (self.root / "peers.json").write_text(json.dumps(self.peers), encoding="utf8")

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def native(self, root, local, alias, text, message_id, wait_idle, *, permit=None):
        if permit:
            async with self.http(self.active_app, LOCAL) as transport:
                proof = {**permit, "target": self.peers["peers"][alias], "message_id": message_id,
                         "text_hash": text_hash(text)}
                self.assertEqual((await transport.post("/collaboration/consume", json=proof)).status_code, 200)
        self.calls.append({"alias": alias, "text": text, "message_id": message_id})
        return {"state": "accepted"}

    @asynccontextmanager
    async def app(self):
        app = a2a_server.create_app(self.root, "device-permission-test", self.native,
                                    shutdown=lambda: None)
        self.active_app = app
        async with app.app.router.lifespan_context(app.app):
            yield app

    def http(self, app, token, *, client=("127.0.0.1", 123), extra_headers=None):
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=app, client=client), base_url="http://127.0.0.1:8788",
                                 headers={"Authorization": "Bearer " + token, **(extra_headers or {})})

    async def sdk(self, http, alias="Codex"):
        card = await A2ACardResolver(http, "http://127.0.0.1:8788/agents/" + alias).get_agent_card()
        return ClientFactory(ClientConfig(httpx_client=http, streaming=False, polling=True,
                                         supported_protocol_bindings=["JSONRPC"])).create(card)

    async def send(self, sdk, origin=None, **metadata):
        message = new_text_message("Find the answer", role=Role.ROLE_USER)
        message.metadata.update(metadata)
        if origin is not None:
            message.metadata.update({"collaboration_origin": origin})
        request = SendMessageRequest(message=message)
        request.configuration.return_immediately = True
        async for event in sdk.send_message(request):
            if event.HasField("task"):
                return event.task
        self.fail("No submitted task")

    async def test_device_tokens_can_contact_any_chat_but_only_own_their_queue(self):
        async with self.app() as app:
            async with self.http(app, PHONE) as phone, self.http(app, OTHER) as other, \
                    self.http(app, LOCAL) as owner, self.http(app, "wrong-token") as wrong:
                self.assertEqual((await phone.get("/agents/Codex/.well-known/agent-card.json")).status_code, 200)
                self.assertEqual((await phone.get("/agents/WorkBuddy/.well-known/agent-card.json")).status_code, 200)
                self.assertEqual((await other.get("/agents/Codex/.well-known/agent-card.json")).status_code, 403)
                self.assertEqual((await phone.post("/shutdown", json={})).status_code, 403)
                self.assertEqual((await phone.get("/devices/phone/next?wait=0&policy=2")).status_code, 200)
                self.assertEqual((await other.get("/devices/phone/next?wait=0&policy=2")).status_code, 403)
                self.assertEqual((await owner.get("/devices/phone/next?wait=0&policy=2")).status_code, 403)
                self.assertEqual((await wrong.get("/devices/phone/next?wait=0&policy=2")).status_code, 401)

    async def test_missing_legacy_direction_list_does_not_block_registered_chat(self):
        self.local["a2a"].pop("device_permissions")
        self.save()
        async with self.app() as app, self.http(app, PHONE) as phone:
            for alias in ("Codex", "WorkBuddy"):
                self.assertEqual((await phone.get("/agents/" + alias + "/.well-known/agent-card.json")).status_code, 200)
            sdk = await self.sdk(phone, "WorkBuddy")
            origin = {"device_id": "phone", "software": "minis", "conversation_id": "phone-chat"}
            await self.send(sdk, origin)
            for _ in range(50):
                if self.calls:
                    break
                await asyncio.sleep(.02)
            self.assertEqual([call["alias"] for call in self.calls], ["WorkBuddy"])

    async def test_public_health_is_minimal_and_local_health_is_detailed(self):
        async with self.app() as app:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                         base_url="http://127.0.0.1:8788") as public:
                public_health = (await public.get("/health")).json()
            async with self.http(app, LOCAL) as local:
                local_health = (await local.get("/health")).json()
            self.assertEqual(public_health, {"service": "ai-collaboration-a2a"})
            self.assertNotIn("instance", public_health)
            self.assertNotIn("agents", public_health)
            self.assertEqual(local_health["instance"], "device-permission-test")
            self.assertEqual(set(local_health["agents"]), {"Codex", "WorkBuddy", "Minis"})

    async def test_public_update_package_is_authenticated_and_fixed_path_only(self):
        async with self.app() as app:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                         base_url="http://127.0.0.1:8788") as anonymous:
                self.assertEqual((await anonymous.get("/package")).status_code, 401)
            async with self.http(app, PHONE) as phone:
                self.assertEqual((await phone.get("/package")).status_code, 404)

                archive = io.BytesIO()
                with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as package:
                    package.writestr("public.txt", "public update payload")
                public_bytes = archive.getvalue()
                (self.root / "runtime" / "public-update.zip").write_bytes(public_bytes)
                config_bytes = b'{"private_config_sentinel":"not for package endpoint"}'
                (self.root / "local.json").write_bytes(config_bytes)

                response = await phone.get("/package")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.content, public_bytes)
                self.assertNotEqual(response.content, config_bytes)

                query_response = await phone.get("/package?path=local.json")
                if query_response.status_code == 200:
                    self.assertEqual(query_response.content, public_bytes)
                    self.assertNotEqual(query_response.content, config_bytes)
                else:
                    self.assertIn(query_response.status_code, {400, 403, 404, 422})

    async def test_phone_origin_must_match_its_registered_minis_chat(self):
        valid = {"device_id": "phone", "software": "minis", "conversation_id": "phone-chat"}
        forged = {"device_id": "phone", "software": "minis", "conversation_id": "forged-chat"}
        wrong_device = {"device_id": "other", "software": "minis", "conversation_id": "phone-chat"}
        async with self.app() as app, self.http(app, PHONE) as phone:
            sdk = await self.sdk(phone)
            with self.assertRaises(Exception):
                await self.send(sdk, forged)
            with self.assertRaises(Exception):
                await self.send(sdk, wrong_device)
            self.assertEqual(self.calls, [])

            accepted = await self.send(sdk, valid)
            self.assertIsNotNone(accepted)
            self.assertIn(accepted.status.state, {TaskState.TASK_STATE_SUBMITTED, TaskState.TASK_STATE_WORKING})
            for _ in range(50):
                if self.calls:
                    break
                await asyncio.sleep(.02)
            self.assertEqual(len(self.calls), 1)
            self.assertIn("Minis", self.calls[0]["text"])

    async def test_device_token_cannot_read_another_callers_task(self):
        async with self.app() as app, self.http(app, CODEX) as owner, self.http(app, PHONE) as phone:
            owner_sdk = await self.sdk(owner, "WorkBuddy")
            phone_sdk = await self.sdk(phone, "WorkBuddy")
            task = await self.send(owner_sdk)
            self.assertIsNotNone(task)
            with self.assertRaises(Exception):
                await phone_sdk.get_task(GetTaskRequest(id=task.id))
            for _ in range(100):
                if self.calls:
                    break
                await asyncio.sleep(.02)
            self.assertEqual(len(self.calls), 1)

    async def test_all_six_registered_directions_ignore_legacy_software_list(self):
        targets = ((PHONE, "Codex"), (PHONE, "WorkBuddy"), (CODEX, "WorkBuddy"), (WORKBUDDY, "Codex"))
        async with self.app() as app:
            for token, alias in targets:
                async with self.http(app, token) as client:
                    sdk = await self.sdk(client, alias)
                    await self.send(sdk)
            for _ in range(100):
                if len(self.calls) == len(targets):
                    break
                await asyncio.sleep(.02)
            self.assertCountEqual([call["alias"] for call in self.calls], [alias for _, alias in targets])
            for token in (CODEX, WORKBUDDY):
                async with self.http(app, token) as sender, self.http(app, PHONE) as phone:
                    sdk = await self.sdk(sender, "Minis")
                    task = await self.send(sdk)
                    delivery = (await phone.get("/devices/phone/next?wait=2&policy=2")).json()
                    self.assertEqual(delivery["delivery_id"], task.id)
                    self.assertEqual((await phone.post("/devices/phone/authorize", json={"delivery_id": task.id})).status_code, 200)
                    self.assertEqual((await phone.post("/devices/phone/ack", json={"delivery_id": task.id, "state": "accepted"})).status_code, 200)
                    self.assertEqual((await phone.post("/devices/phone/reply", json={"address": delivery["reply_to"], "text": "fixture done"})).status_code, 200)

    async def test_legacy_no_bridge_device_or_forwarded_caller_cannot_create_task(self):
        self.local["a2a"].pop("managed_ui")
        self.peers["peers"]["Minis"]["verified"] = False
        self.save()
        variants = [(PHONE, {}, ("127.0.0.1", 123)),
                    (OTHER, {}, ("127.0.0.1", 123)),
                    (LOCAL, {}, ("100.64.0.7", 123))]
        for header in ("Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Forwarded-Unrecognized"):
            variants.append((LOCAL, {header: "fixture"}, ("127.0.0.1", 123)))
        claimed = {"device_id": "phone", "software": "minis", "conversation_id": "phone-chat"}
        async with self.app() as app:
            for token, headers, client_address in variants:
                async with self.http(app, token, client=client_address, extra_headers=headers) as client:
                    sdk = await self.sdk(client)
                    with self.assertRaises(InvalidParamsError):
                        await self.send(sdk, claimed, no_bridge_legacy_write_allowed=True)
            self.assertEqual(self.calls, [])
            with Mailbox(self.root).connect() as db:
                self.assertEqual(db.execute("SELECT count(*) FROM replies").fetchone()[0], 0)

    async def test_legacy_local_task_compatibility_does_not_grant_original_chat_identity(self):
        self.local["a2a"].pop("managed_ui")
        self.save()
        async with self.app() as app, self.http(app, LOCAL) as direct:
            sdk = await self.sdk(direct)
            task = await self.send(sdk)
            async with self.http(app, LOCAL, client=("100.64.0.7", 123)) as remote:
                old_owner = await self.sdk(remote)
                self.assertEqual((await old_owner.get_task(GetTaskRequest(id=task.id))).id, task.id)
            row = Mailbox(self.root).row(task.id)
            self.assertIsNone(row["source_ref"])
            self.assertEqual(row["owner"], "local")

    async def test_published_service_without_bridge_cannot_create_legacy_task_even_on_loopback(self):
        self.local["a2a"].pop("managed_ui")
        self.save()
        # First retain a genuine local legacy task to check upgrade-time reads.
        async with self.app() as app, self.http(app, LOCAL) as direct:
            task = await self.send(await self.sdk(direct))
        self.calls.clear()
        self.local["a2a"]["public_base_url"] = "https://desk.example.test:18443"
        self.save()
        async with self.app() as app, self.http(app, LOCAL) as direct:
            sdk = await self.sdk(direct)
            self.assertEqual((await sdk.get_task(GetTaskRequest(id=task.id))).id, task.id)
            with self.assertRaises(InvalidParamsError):
                await self.send(sdk, no_bridge_legacy_write_allowed=True)
            self.assertEqual(self.calls, [])


if __name__ == "__main__":
    unittest.main()
