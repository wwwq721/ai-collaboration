"""Conversation delivery over a real in-process A2A service, with synthetic identities."""
import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
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
from a2a.utils.errors import InvalidParamsError
from google.protobuf.json_format import MessageToDict

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from a2a_server import create_app
from delivery_ticket import text_hash
from workbuddy import BridgeError


class ConversationMessages(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.url = "http://127.0.0.1:8788"
        self.bindings = {
            alias: {"device_id": "pc", "software": software, "conversation_id": conversation}
            for alias, software, conversation in (
                ("Sender", "codex", "first"), ("Receiver", "workbuddy", "second"),
                ("Sibling", "codex", "third"),
            )
        }
        self.tokens = {name: name + "-synthetic-token-" * 4 for name in self.bindings}
        self.bindings["ExternalCaller"] = {"device_id": "caller-box", "software": "remote",
                                           "conversation_id": "caller-chat"}
        self.tokens["ExternalCaller"] = "external-caller-synthetic-" * 4
        self.device_token = "isolated-phone-fixture-" * 4
        phone = {"device_id": "phone", "software": "minis", "conversation_id": "phone-chat"}
        research = ["external-box", "research", "research-chat"]
        caller = ["caller-box", "remote", "caller-chat"]
        research_url = "https://research.example.test/agent"
        local = {"device_id": "pc", "machine_name": platform.node(), "a2a": {
            "tokens": {"local": "fixture-service-token-" * 4}, "chat_tokens": self.tokens,
            "chat_bindings": self.bindings, "device_tokens": {"phone": self.device_token},
            "message_delivery": True,
            "idle_wait_seconds": 1, "task_timeout": 3, "delivery_timeout": 2,
        }}
        local["a2a_credentials"] = {"Research": {"token": "external-fixture-" * 4},
                                    "ExternalCaller": {"token": "external-outbound-fixture-" * 4}}
        (self.root / "local.json").write_text(json.dumps(local), encoding="utf8")
        peers = {alias: {**ref, "verified": True} for alias, ref in self.bindings.items()}
        peers["Mobile"] = {**phone, "verified": True}
        caller_url = "https://caller.example.test/agent"
        peers["ExternalCaller"].update(transport="a2a", url=caller_url, verified_url=caller_url, verified_ref=caller)
        peers["Research"] = {"device_id": research[0], "software": research[1],
                             "conversation_id": research[2], "verified_ref": research, "verified": True,
                             "transport": "a2a", "url": research_url, "verified_url": research_url}
        (self.root / "peers.json").write_text(json.dumps({"version": 1, "peers": peers}), encoding="utf8")
        self.native_calls = []
        self.host_entries = []
        self.host_controls = {}
        self.host_gates = {}
        self.prepared, self.resume = asyncio.Event(), asyncio.Event()
        self.resume.set()
        self.native_result = {"state": "accepted", "receipt": {
            "state": "accepted", "stage": "native_host", "native_id": "fixture"}}
        self.app = create_app(self.root, "isolated-fixture", self.native)
        self.service = await self.enterAsyncContext(AsyncExitStack())
        await self.service.enter_async_context(self.app.app.router.lifespan_context(self.app.app))

    def http(self, source=None):
        headers = {"Authorization": "Bearer " + self.tokens[source]} if source else {}
        return httpx.AsyncClient(base_url=self.url, transport=httpx.ASGITransport(app=self.app),
                                 headers=headers)

    async def native(self, root, local, alias, text, task_id, wait, *, permit):
        payload = json.loads(text)
        self.host_entries.append((alias, payload["message"]))
        self.host_controls[(alias, payload["message"])] = wait, permit["allow_busy"]
        self.prepared.set()
        await self.resume.wait()
        async with self.http() as h:
            proof = {**permit, "target": self.bindings[alias], "message_id": task_id,
                     "text_hash": text_hash(text)}
            response = await h.post("/collaboration/consume", json=proof)
        if response.status_code != 200:
            raise BridgeError("final delivery permission denied")
        self.native_calls.append((alias, payload))
        gate = self.host_gates.get((alias, payload["message"]))
        if gate:
            entered, release = gate
            entered.set()
            await release.wait()
        if isinstance(self.native_result, Exception):
            raise self.native_result
        return self.native_result

    @asynccontextmanager
    async def sdk(self, source="Sender", target="Receiver"):
        async with self.http(source) as h:
            card = await A2ACardResolver(h, self.url + "/agents/" + target).get_agent_card()
            self.assertIn("conversation-message-v1", [skill.id for skill in card.skills])
            yield ClientFactory(ClientConfig(httpx_client=h, streaming=False, polling=True,
                                             supported_protocol_bindings=["JSONRPC"])).create(card)

    async def test_external_result_is_owner_scoped_and_each_registered_source_gets_body(self):
        body = "  外部回件\r\n  "

        async def remote(root, local, target, source, text, task_id, approve, **options):
            self.assertTrue(approve())
            options["on_admission"]({"state": "queued", "stage": "external_service",
                                     "native_id": "remote-task"})
            return {"state": "completed", "reply": body}

        with patch("managed_egress.deliver", side_effect=remote):
            for source in ("Sender", "Sibling"):
                async with self.sdk(source, "Research") as sdk:
                    message_id, task_id = await self.submit(sdk, "request")
                    task = await self.terminal(sdk, task_id)
                    self.assertEqual(task.artifacts[0].parts[0].text, body)
                path = "/collaboration/receipts/" + message_id
                async with self.http(source) as h, self.http("Receiver") as unrelated:
                    self.assertNotIn("result", (await h.get(path)).json())
                    self.assertEqual((await h.get(path + "?include_result=1")).json()["result"]["text"], body)
                    self.assertEqual((await unrelated.get(path + "?include_result=1")).status_code, 404)
        async with asyncio.timeout(8):
            from a2a_state import Mailbox
            mailbox = Mailbox(self.root)
            while mailbox.pending_external_results():
                await asyncio.sleep(.05)
        self.assertCountEqual(self.native_calls, [(alias, {"source": "research", "source_conversation": "Research",
                                                           "message": body}) for alias in ("Sender", "Sibling")])

    async def test_task_only_external_caller_receives_artifact_without_device_or_new_sdk_send(self):
        from a2a_state import Mailbox
        async def remote(root, local, peer, source, text, task_id, approve, **options):
            self.assertTrue(approve())
            options["on_admission"]({"state": "queued", "stage": "external_service"})
            return {"state": "completed", "reply": "answer for the caller"}

        with patch("managed_egress.deliver", side_effect=remote) as outbound:
            async with self.sdk("ExternalCaller", "Research") as sdk:
                _, task_id = await self.submit(sdk, "question")
                task = await self.terminal(sdk, task_id)
                self.assertEqual(task.artifacts[0].parts[0].text, "answer for the caller")
            mailbox = Mailbox(self.root)
            async with asyncio.timeout(8):
                while mailbox.pending_external_results():
                    await asyncio.sleep(.05)
            with mailbox.connect() as db:
                self.assertEqual(db.execute("SELECT count(*) FROM device_deliveries").fetchone()[0], 0)
                self.assertEqual(db.execute("SELECT count(*) FROM replies WHERE mode='message'").fetchone()[0], 1)
            outbound.assert_awaited_once()
        self.assertFalse(self.native_calls)

    async def test_restart_finishes_sdk_task_after_negative_receipt_already_persisted(self):
        from a2a_runtime import TemporaryTaskStore
        saved = TemporaryTaskStore.save
        negative = asyncio.Event()

        async def interrupt_terminal_write(store, task, context):
            if task.status.state in {TaskState.TASK_STATE_FAILED, TaskState.TASK_STATE_REJECTED}:
                negative.set()
                return
            return await saved(store, task, context)

        cases = []
        async with self.sdk() as sdk:
            with patch.object(TemporaryTaskStore, "save", interrupt_terminal_write):
                for state in ("uncertain", "not_sent"):
                    negative.clear()
                    self.native_result = {"state": state}
                    _, task_id = await self.submit(sdk, state)
                    await asyncio.wait_for(negative.wait(), 5)
                    self.assertEqual((await sdk.get_task(GetTaskRequest(id=task_id))).status.state,
                                     TaskState.TASK_STATE_WORKING)
                    cases.append((task_id, state))
        await self.service.aclose()
        self.app = create_app(self.root, "second-fixture-instance", self.native)
        await self.service.enter_async_context(self.app.app.router.lifespan_context(self.app.app))
        async with self.sdk() as sdk:
            for task_id, state in cases:
                task = await self.terminal(sdk, task_id)
                expected = TaskState.TASK_STATE_REJECTED if state == "not_sent" else TaskState.TASK_STATE_FAILED
                self.assertEqual(task.status.state, expected)
                self.assertEqual(MessageToDict(task.metadata)["collaboration_receipt"]["state"], state)
        self.assertEqual(len(self.native_calls), 2)

    async def submit(self, sdk, text, message_id=None, **meta):
        message = new_text_message(text, role=Role.ROLE_USER)
        message.message_id = message_id or str(uuid.uuid4())
        message.metadata.update({"collaboration_mode": "message", **meta})
        request = SendMessageRequest(message=message)
        request.configuration.return_immediately = True
        async for event in sdk.send_message(request):
            if event.HasField("task"):
                return message.message_id, event.task.id
            if event.HasField("message"):
                return message.message_id, None
        self.fail("missing SDK response")

    async def terminal(self, sdk, task_id):
        async with asyncio.timeout(5):
            while True:
                task = await sdk.get_task(GetTaskRequest(id=task_id))
                if task.status.state in {TaskState.TASK_STATE_COMPLETED, TaskState.TASK_STATE_FAILED,
                                         TaskState.TASK_STATE_REJECTED}:
                    return task
                await asyncio.sleep(.015)

    async def test_two_messages_in_flight_ack_and_reverse_business_message(self):
        texts = ["  first\r\n\"quoted\" \\ 🪶  ", "已收到"]
        self.resume.clear()
        async with self.sdk() as sdk:
            try:
                sends = [await self.submit(sdk, text, collaboration_wait_idle=10) for text in texts]
                self.assertNotEqual(sends[0][1], sends[1][1])
            finally:
                self.resume.set()
            for message_id, task_id in sends:
                task = await self.terminal(sdk, task_id)
                self.assertEqual(task.status.state, TaskState.TASK_STATE_COMPLETED)
                receipt = MessageToDict(task.metadata)["collaboration_receipt"]
                self.assertEqual(receipt["state"], "accepted")
                self.assertFalse(task.artifacts)
                path = "/collaboration/receipts/" + message_id
                async with self.http("Sender") as owner, self.http("Sibling") as sibling:
                    self.assertEqual((await owner.get(path)).json(), receipt)
                    self.assertEqual((await sibling.get(path)).status_code, 404)
        self.assertCountEqual(self.native_calls, [("Receiver", {"message": text, "source": "codex",
                                                              "source_conversation": "Sender"}) for text in texts])
        async with self.sdk("Receiver", "Sender") as reverse:
            _, reverse_id = await self.submit(reverse, "new reverse business message")
            self.assertEqual((await self.terminal(reverse, reverse_id)).status.state, TaskState.TASK_STATE_COMPLETED)
        self.assertEqual(self.native_calls[-1], ("Sender", {"message": "new reverse business message",
                                                          "source": "workbuddy", "source_conversation": "Receiver"}))
        self.assertEqual(len(self.native_calls), 3)

    async def test_same_codex_target_serializes_while_other_target_can_finish(self):
        entered, release = asyncio.Event(), asyncio.Event()
        self.host_gates[("Sibling", "first")] = entered, release
        async with self.sdk(target="Sibling") as codex, self.sdk(target="Receiver") as other:
            try:
                _, first = await self.submit(codex, "first", collaboration_wait_idle=5)
                await asyncio.wait_for(entered.wait(), 3)
                _, second = await self.submit(codex, "second", collaboration_wait_idle=5)
                _, separate = await self.submit(other, "separate", collaboration_wait_idle=5)
                self.assertEqual((await self.terminal(other, separate)).status.state,
                                 TaskState.TASK_STATE_COMPLETED)
                self.assertEqual(self.host_entries, [("Sibling", "first"), ("Receiver", "separate")])
            finally:
                release.set()
            for task_id in (first, second):
                self.assertEqual((await self.terminal(codex, task_id)).status.state,
                                 TaskState.TASK_STATE_COMPLETED)
            self.assertEqual(self.host_entries[-1], ("Sibling", "second"))

    async def test_target_registration_revoked_while_waiting_skips_native_entry(self):
        entered, release = asyncio.Event(), asyncio.Event()
        self.host_gates[("Sibling", "holding")] = entered, release
        async with self.sdk(target="Sibling") as sdk, self.http("Sender") as owner:
            try:
                _, first = await self.submit(sdk, "holding", collaboration_wait_idle=5)
                await asyncio.wait_for(entered.wait(), 3)
                message_id, second = await self.submit(sdk, "revoked", collaboration_wait_idle=5)
                receipt = (await owner.get("/collaboration/receipts/" + message_id)).json()
                self.assertEqual(receipt["state"], "waiting")
                self.app.app.state.policy.peers["sibling"]["verified"] = False
            finally:
                release.set()
            self.assertEqual((await self.terminal(sdk, first)).status.state, TaskState.TASK_STATE_COMPLETED)
            rejected = await self.terminal(sdk, second)
            self.assertEqual(MessageToDict(rejected.metadata)["collaboration_receipt"]["state"], "not_sent")
            self.assertEqual(self.host_entries, [("Sibling", "holding")])

    async def test_timeout_waiting_for_target_does_not_release_current_handoff(self):
        entered, release = asyncio.Event(), asyncio.Event()
        self.host_gates[("Sibling", "holding")] = entered, release
        async with self.sdk(target="Sibling") as sdk:
            try:
                _, first = await self.submit(sdk, "holding", collaboration_wait_idle=5)
                await asyncio.wait_for(entered.wait(), 3)
                for text in ("timed out", "still held"):
                    _, task_id = await self.submit(sdk, text, collaboration_wait_idle=1)
                    terminal = await self.terminal(sdk, task_id)
                    self.assertEqual(MessageToDict(terminal.metadata)["collaboration_receipt"]["state"], "not_sent")
                self.assertEqual(self.host_entries, [("Sibling", "holding")])
            finally:
                release.set()
            self.assertEqual((await self.terminal(sdk, first)).status.state, TaskState.TASK_STATE_COMPLETED)
            _, next_task = await self.submit(sdk, "new", collaboration_wait_idle=1)
            self.assertEqual((await self.terminal(sdk, next_task)).status.state, TaskState.TASK_STATE_COMPLETED)

    async def test_timeout_after_native_submission_remains_uncertain(self):
        self.native_result = TimeoutError("simulated lost native response")
        async with self.sdk(target="Sibling") as sdk:
            _, task_id = await self.submit(sdk, "submitted once", collaboration_wait_idle=1)
            terminal = await self.terminal(sdk, task_id)
        self.assertEqual(MessageToDict(terminal.metadata)["collaboration_receipt"]["state"], "uncertain")
        self.assertEqual(self.host_entries, [("Sibling", "submitted once")])

    async def test_subsecond_budget_after_queue_only_allows_immediate_idle_check(self):
        entered, release = asyncio.Event(), asyncio.Event()
        self.host_gates[("Sibling", "holding")] = entered, release
        async with self.sdk(target="Sibling") as sdk:
            try:
                _, first = await self.submit(sdk, "holding", collaboration_wait_idle=5)
                await asyncio.wait_for(entered.wait(), 3)
                _, second = await self.submit(sdk, "idle only", collaboration_wait_idle=1)
                await asyncio.sleep(.05)
            finally:
                release.set()
            for task_id in (first, second):
                self.assertEqual((await self.terminal(sdk, task_id)).status.state, TaskState.TASK_STATE_COMPLETED)
            self.assertEqual(self.host_controls[("Sibling", "idle only")], (0, False))

    async def test_queue_and_unknown_are_distinct(self):
        async with self.sdk() as sdk:
            for result, expected, stage, terminal in (
                ({"state": "accepted", "receipt": {"state": "queued", "stage": "host_queue",
                                                   "native_id": "fixture-queue"}},
                 "queued", "host_queue", TaskState.TASK_STATE_COMPLETED),
                ({}, "uncertain", "relay", TaskState.TASK_STATE_FAILED),
            ):
                self.native_result = result
                _, task_id = await self.submit(sdk, expected)
                task = await self.terminal(sdk, task_id)
                self.assertEqual(task.status.state, terminal)
                receipt = MessageToDict(task.metadata)["collaboration_receipt"]
                self.assertEqual((receipt["state"], receipt["stage"]), (expected, stage))
        self.assertEqual(len(self.native_calls), 2)

    async def test_stale_ui_policy_cannot_restrict_registered_delivery(self):
        self.resume.clear()
        async with self.sdk() as sdk:
            try:
                _, task_id = await self.submit(sdk, "withdrawn before handoff")
                await asyncio.wait_for(self.prepared.wait(), 3)
                document = {"version": 1, "pauseAll": False, "grants": [], "pairs": {}, "chats": {}}
                (self.root / "runtime/ui-policy.json").write_text(json.dumps(document), encoding="utf8")
            finally:
                self.resume.set()
            task = await self.terminal(sdk, task_id)
            self.assertEqual(MessageToDict(task.metadata)["collaboration_receipt"]["state"], "accepted")
        self.assertEqual(len(self.native_calls), 1)

    async def test_late_phone_ack_updates_receipt_without_reopening_sdk_task(self):
        async with self.sdk(target="Mobile") as sdk, self.http() as device, self.http("Sender") as owner:
            device.headers["Authorization"] = "Bearer " + self.device_token
            message_id, task_id = await self.submit(sdk, "late acknowledgement")
            poll = "/devices/phone/next?wait=1&policy=2&message_version=1"
            self.assertEqual((await device.get(poll)).json()["delivery_id"], task_id)
            self.assertEqual((await device.post("/devices/phone/authorize",
                                               json={"delivery_id": task_id})).status_code, 200)
            old = await self.terminal(sdk, task_id)
            self.assertEqual(old.status.state, TaskState.TASK_STATE_FAILED)
            path = "/collaboration/receipts/" + message_id
            self.assertEqual((await owner.get(path)).json()["state"], "uncertain")
            ack = {"delivery_id": task_id, "state": "accepted", "scheduled_task_id": "later-alarm",
                   "receipt": {"state": "queued", "stage": "minis_scheduler", "native_id": "later-alarm"}}
            self.assertEqual((await device.post("/devices/phone/ack", json=ack)).status_code, 200)
            self.assertEqual((await owner.get(path)).json()["state"], "queued")
            self.assertEqual((await sdk.get_task(GetTaskRequest(id=task_id))).status.state, old.status.state)
            exhausted = await device.get("/devices/phone/next?wait=0&policy=2&message_version=1")
            self.assertEqual(exhausted.json()["state"], "empty")

    async def test_declared_sibling_cannot_impersonate_authenticated_source(self):
        async with self.sdk() as sdk:
            with self.assertRaises(InvalidParamsError):
                await self.submit(sdk, "wrong origin", collaboration_origin=self.bindings["Sibling"])
        self.assertFalse(self.native_calls)

    async def test_repeated_identical_message_is_not_dispatched_twice(self):
        async with self.sdk() as sdk:
            message_id, task_id = await self.submit(sdk, "once")
            await self.terminal(sdk, task_id)
            await self.submit(sdk, "once", message_id)
            with self.assertRaises(InvalidParamsError):
                await self.submit(sdk, "changed", message_id)
        self.assertEqual(len(self.native_calls), 1)

    async def test_mobile_only_receives_supported_format_and_receipt_is_bound(self):
        async with self.sdk(target="Mobile") as sdk, self.http() as device, self.http("Sender") as owner:
            device.headers["Authorization"] = "Bearer " + self.device_token
            message_id, task_id = await self.submit(sdk, " mobile\r\n")
            path = "/collaboration/receipts/" + message_id
            self.assertEqual((await owner.get(path)).json()["state"], "waiting")
            poll = "/devices/phone/next?policy=2"
            self.assertEqual((await device.get(poll + "&wait=0")).json()["state"], "empty")
            delivery = (await device.get(poll + "&wait=2&message_version=1")).json()
            self.assertEqual(delivery["delivery_id"], task_id)
            self.assertEqual(delivery["message"], {"source": "codex", "source_conversation": "Sender",
                                                   "message": " mobile\r\n"})
            ack = {"delivery_id": task_id, "state": "accepted", "scheduled_task_id": "fixture-alarm",
                   "receipt": {"state": "queued", "stage": "minis_scheduler", "native_id": "fixture-alarm"}}
            self.assertEqual((await device.post("/devices/phone/ack", json=ack)).status_code, 403)
            self.assertEqual((await device.post("/devices/phone/authorize",
                                                 json={"delivery_id": task_id})).status_code, 200)
            for _ in range(2):
                self.assertEqual((await device.post("/devices/phone/ack", json=ack)).status_code, 200)
            task = await self.terminal(sdk, task_id)
            self.assertEqual(task.status.state, TaskState.TASK_STATE_COMPLETED)
            receipt = (await owner.get(path)).json()
            self.assertEqual((receipt["state"], receipt["stage"]), ("queued", "minis_scheduler"))
        self.assertFalse(self.native_calls)
