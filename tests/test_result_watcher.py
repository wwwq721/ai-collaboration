"""Real SQLite state and notification behavior with fake network/native edges."""
import asyncio
from dataclasses import replace
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest import mock
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from a2a_state import Limits
from result_watcher import PendingResults, ResultWatcher, background_results
from workbuddy import BridgeError


class ResultWatcherTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.now = [1000.0]
        self.clock = lambda: self.now[0]
        self.local = {"device_id": "phone", "a2a": {"task_timeout": 10, "idle_wait_seconds": 1}}
        (self.root / "local.json").write_text(json.dumps(self.local), encoding="utf8")
        self.origin = {"device_id": "phone", "software": "minis", "conversation_id": "original-chat"}
        self.peer = {"alias": "Codex", "device_id": "pc", "software": "a2a", "conversation_id": "pc-chat",
                     "url": "https://private.example/agents/Codex", "token": "NEVER_PERSIST_ME"}
        self.queries, self.deliveries = [], []
        self.response = {"state": "completed", "reply": "a real answer"}

    def tearDown(self):
        self.tmp.cleanup()

    def store(self, **changes):
        return PendingResults(self.root, limits=replace(Limits(), **changes), clock=self.clock)

    def job(self, store=None, bound=True):
        store = store or self.store()
        job_id = store.prepare(self.origin, self.peer, str(uuid.uuid4()))
        if bound:
            store.bind(job_id, "remote-task-" + job_id, "context")
        return job_id

    async def query(self, job):
        self.queries.append(job)
        return self.response

    async def deliver(self, origin, text, notification_id):
        self.deliveries.append((origin, text, notification_id))
        return {"state": "accepted", "scheduled_task_id": "schedule-1", "token": "receipt-secret", "text": text}

    def watcher(self, **kwargs):
        return ResultWatcher(self.root, self.local, {"Codex": self.peer}, query=kwargs.get("query", self.query),
                             deliver=kwargs.get("deliver", self.deliver), clock=self.clock)

    async def test_no_pending_jobs_never_queries_or_wakes_a_model(self):
        self.assertEqual(await self.watcher().tick(), 0)
        self.assertEqual(self.queries, [])
        self.assertEqual(self.deliveries, [])

    async def test_completed_result_uses_original_chat_once_and_discards_plaintext(self):
        job_id = self.job()
        watcher = self.watcher()
        await watcher.tick()
        self.now[0] += 10
        await watcher.tick()
        self.assertEqual(len(self.queries), 1)
        self.assertEqual(self.queries[0]["task_id"], "remote-task-" + job_id)
        self.assertEqual(len(self.deliveries), 1)
        self.assertEqual(self.deliveries[0][0], self.origin)
        self.assertIn("a real answer", self.deliveries[0][1])
        row = watcher.pending.get(job_id)
        self.assertEqual(row["state"], "notified")
        self.assertIsNone(row["result"])
        self.assertEqual(row["receipt"], {"state": "accepted", "scheduled_task_id": "schedule-1"})

    async def test_all_terminal_and_input_states_are_notified(self):
        for state in ("input_required", "auth_required", "failed", "canceled", "rejected", "expired"):
            with self.subTest(state=state):
                self.response = {"state": state, "error": state + " details"}
                job_id = self.job()
                await self.watcher().tick()
                self.assertEqual(self.store().get(job_id)["state"], "notified")
                self.assertIn("状态：" + state, self.deliveries[-1][1])

    async def test_working_or_unknown_state_does_not_notify(self):
        job_id = self.job()
        watcher = self.watcher()
        for state in ("accepted", "working", "unknown"):
            self.response = {"state": state}
            await watcher.tick()
            self.now[0] += 4
        self.assertEqual(self.deliveries, [])
        self.assertEqual(self.store().get(job_id)["state"], "waiting")

    async def test_network_reads_can_retry_the_same_task_without_resending(self):
        job_id = self.job()
        async def unstable(job):
            self.queries.append(job)
            if len(self.queries) == 1:
                raise ConnectionError("secret unlogged network error")
            return self.response
        watcher = self.watcher(query=unstable)
        await watcher.tick()
        self.now[0] += 4
        await watcher.tick()
        self.assertEqual({job["task_id"] for job in self.queries}, {"remote-task-" + job_id})
        self.assertEqual(len(self.deliveries), 1)

    async def test_temporary_native_network_locator_failure_keeps_task_for_retry(self):
        job_id = self.job()
        async def unavailable(job):
            raise BridgeError("network node lookup temporarily unavailable")
        await self.watcher(query=unavailable).tick()
        self.assertEqual(self.store().get(job_id)["state"], "waiting")
        self.assertEqual(self.deliveries, [])
        self.now[0] += 4
        await self.watcher().tick()
        self.assertEqual(len(self.deliveries), 1)

    async def test_unknown_native_delivery_is_never_replayed(self):
        job_id = self.job()
        async def unknown(origin, text, notification_id):
            self.deliveries.append(notification_id)
            raise BridgeError("native send unconfirmed", "uncertain")
        await self.watcher(deliver=unknown).tick()
        self.now[0] += 10
        await self.watcher(deliver=unknown).tick()
        self.assertEqual(len(self.deliveries), 1)
        self.assertEqual(self.store().get(job_id)["state"], "uncertain")
        self.assertIsNone(self.store().get(job_id)["result"])

    async def test_proven_not_sent_can_retry_same_notification_id(self):
        job_id = self.job()
        async def initially_busy(origin, text, notification_id):
            self.deliveries.append(notification_id)
            if len(self.deliveries) == 1:
                raise BridgeError("target is busy", "not_sent")
            return {"state": "accepted"}
        watcher = self.watcher(deliver=initially_busy)
        await watcher.tick()
        self.now[0] += 6
        await watcher.tick()
        self.assertEqual(len(self.queries), 1)
        self.assertEqual(len(self.deliveries), 2)
        self.assertEqual(self.deliveries[0], self.deliveries[1])
        self.assertEqual(self.store().get(job_id)["state"], "notified")

    async def test_interrupted_dispatch_claim_survives_restart_without_replay(self):
        pending = self.store()
        job_id = self.job(pending)
        pending.result(job_id, self.response)
        self.assertTrue(pending.claim_notification(job_id))
        self.now[0] += 100
        await self.watcher().tick()
        self.assertEqual(self.deliveries, [])
        self.assertEqual(self.store().get(job_id)["state"], "dispatching")
        self.assertFalse(pending.mark_inline(job_id))

    async def test_inline_result_is_not_notified_again(self):
        pending = self.store()
        job_id = self.job(pending, bound=False)
        pending.mark_inline(job_id)
        await self.watcher().tick()
        self.assertEqual(self.queries, [])
        self.assertEqual(self.deliveries, [])

    async def test_unbound_crash_and_waiting_timeout_notify_without_new_prompt(self):
        self.job(bound=False)
        self.job()
        self.now[0] += 200
        await self.watcher().tick()
        self.assertEqual(self.queries, [])
        self.assertEqual(len(self.deliveries), 2)
        self.assertTrue(all("expired" in item[1] for item in self.deliveries))

    def test_state_never_persists_tokens_url_or_input_history(self):
        job_id = self.job()
        row = self.store().get(job_id)
        self.assertNotIn("url", row["peer"])
        self.assertNotIn("token", row["peer"])
        raw = self.store().path.read_bytes()
        self.assertNotIn(b"NEVER_PERSIST_ME", raw)
        self.assertNotIn(b"private.example", raw)

    def test_message_id_cannot_be_reused_for_another_origin_or_task(self):
        pending = self.store()
        job_id = self.job(pending)
        self.assertEqual(pending.prepare(self.origin, self.peer, job_id), job_id)
        with self.assertRaises(BridgeError):
            pending.prepare({**self.origin, "software": "codex"}, self.peer, job_id)
        with self.assertRaises(BridgeError):
            pending.bind(job_id, "a different task")

    def test_active_capacity_prevents_another_send_and_terminal_state_expires(self):
        pending = self.store(max_active_tasks=1, metadata_ttl_seconds=10)
        job_id = self.job(pending)
        with self.assertRaises(BridgeError):
            self.job(pending)
        pending.mark_inline(job_id)
        self.now[0] += 11
        pending.cleanup()
        self.assertIsNone(pending.get(job_id))
        self.job(pending)

    async def test_cached_body_expires_but_visible_expiry_notification_remains(self):
        pending = self.store(result_ttl_seconds=10)
        job_id = self.job(pending)
        pending.result(job_id, self.response)
        self.now[0] += 11
        pending.cleanup()
        self.assertEqual(pending.get(job_id)["result"]["state"], "expired")
        await self.watcher().tick()
        self.assertNotIn("a real answer", self.deliveries[0][1])
        self.assertIn("expired", self.deliveries[0][1])

    async def test_parallel_watchers_cannot_notify_the_same_result_twice(self):
        self.job()
        await asyncio.gather(self.watcher().tick(), self.watcher().tick())
        self.assertEqual(len(self.deliveries), 1)

    async def test_oversize_result_is_replaced_with_small_failure(self):
        job_id = self.job()
        self.response = {"state": "completed", "reply": "x" * 65537}
        await self.watcher().tick()
        self.assertEqual(self.store().get(job_id)["state"], "notified")
        self.assertIn("failed", self.deliveries[0][1])
        self.assertLess(len(self.deliveries[0][1]), 1000)

    async def test_minis_notification_schedule_cleanup_checks_owned_markers(self):
        self.local["platform"] = "minis"
        job_id = self.job()
        watcher = self.watcher()
        with mock.patch("minis.cleanup_task", return_value={"state": "removed"}) as cleanup:
            await watcher.tick()
            self.assertFalse(cleanup.called)
            self.now[0] += 61
            await watcher.tick()
            row = self.store().get(job_id)
            cleanup.assert_called_once_with("schedule-1", row["notification_id"], "original-chat", expired=False)
            self.assertEqual(row["cleanup_done"], 1)
            self.now[0] += 61
            await watcher.tick()
            self.assertEqual(cleanup.call_count, 1)

    def test_peer_rebinding_is_rejected_before_query(self):
        watcher = self.watcher()
        saved = {k: self.peer[k] for k in ("alias", "device_id", "software", "conversation_id")}
        self.assertEqual(watcher._peer(saved)["conversation_id"], "pc-chat")
        watcher.peers[0]["conversation_id"] = "different-chat"
        with self.assertRaises(BridgeError):
            watcher._peer(saved)

    def test_remote_host_software_is_part_of_stable_peer_identity(self):
        native = {"alias": "Other", "device_id": "remote", "software": "minis", "conversation_id": "same-id"}
        local = {"device_id": "phone", "a2a": {"device_tokens": {"remote": "private"}}}
        watcher = ResultWatcher(self.root, local, {"Other": native}, query=self.query, deliver=self.deliver)
        saved = {**native, "software": "a2a", "host_software": "minis"}
        self.assertEqual(watcher._peer(saved)["host_software"], "minis")
        watcher.peers[0]["software"] = "codex"
        with self.assertRaises(BridgeError):
            watcher._peer(saved)

    def test_unfinished_native_schedule_keeps_bounded_ownership_metadata(self):
        pending = self.store(metadata_ttl_seconds=10)
        job_id = self.job(pending)
        pending.result(job_id, self.response)
        pending.claim_notification(job_id)
        pending.record_notification(job_id, {"state": "accepted", "scheduled_task_id": "owned-schedule"})
        self.now[0] += 11
        pending.cleanup()
        self.assertIsNotNone(pending.get(job_id))
        pending.record_cleanup(job_id)
        pending.cleanup()
        self.assertIsNone(pending.get(job_id))

    async def test_paired_ip_query_does_not_inspect_old_proxy(self):
        self.local["network"] = {"mode": "paired_ip"}
        job_id = self.job()
        watcher = self.watcher()
        with mock.patch("a2a_client.exchange", new=mock.AsyncMock(return_value=self.response)) as exchange:
            with mock.patch("device_receiver.proxy_url", side_effect=AssertionError("old proxy must not be read")):
                result = await watcher._query(self.store().get(job_id))
        self.assertEqual(result, self.response)
        self.assertEqual(exchange.call_args.args[0]["mode"], "task-status")
        self.assertEqual(exchange.call_args.args[0]["task_id"], "remote-task-" + job_id)
        self.assertIsNone(exchange.call_args.args[0]["proxy_url"])


class BackgroundLifecycleTests(unittest.TestCase):
    def test_background_thread_does_not_block_receiver_and_stops(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local = {"device_id": "phone"}
            with background_results(root, local, []) as handle:
                thread = next(t for t in threading.enumerate() if t.name == "ai-result-watcher")
                self.assertTrue(thread.is_alive())
                self.assertFalse(handle["task"].done())
            self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()
