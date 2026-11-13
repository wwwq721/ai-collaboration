"""Pinned native WorkBuddy admission semantics with a fictional broker."""
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import workbuddy
from workbuddy import BridgeError


@unittest.skipUnless(shutil.which("node"), "Node not available")
class NativeAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.record = self.root / "broker.jsonl"
        self.received = []
        self.status = 200
        self.permission = {"allowed": True, "allow_busy": False, "action": "task", "task_id": "task"}
        test = self
        class Consume(BaseHTTPRequestHandler):
            def do_POST(self):
                payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                test.received.append(payload)
                self.send_response(test.status)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(test.permission).encode())
            def log_message(self, *args):
                pass
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Consume)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.input = {"userId": "fictional-user", "clientId": "fictional-client", "credentials": {},
                      "deviceId": "fictional-native-device", "conversationId": "wb", "waitIdle": 0,
                      "mode": "send", "text": "fictional task", "messageId": "message", "managed": True,
                      "consumePort": self.server.server_port, "target": {"device_id": "pc", "software": "workbuddy", "conversation_id": "wb"},
                      "permit": {"ticket": "fictional-once-ticket", "task_id": "task", "action": "task"}}

    def run_broker(self, states=("idle",), outcome="accepted", wait=0, history="empty"):
        self.input["waitIdle"] = wait
        if self.record.exists():
            self.record.unlink()
        fake = self.root / "fake-sdk.cjs"
        script = """
function init_lib() {}
const fs = require('node:fs');
const states = STATES;
let index = 0;
let admission;
let wireText;
let reads = 0;
const lib_exports = {handshake:async()=>({sdk:{request:async(channel, cmd, options)=>{
  fs.appendFileSync(RECORD, JSON.stringify({...cmd, timeoutMs:options.timeoutMs})+'\\n');
  if (cmd.type.endsWith(':get')) return {payload:{success:true,result:{info:{id:'wb',state:states[Math.min(index++,states.length-1)]}}}};
  if (cmd.type.endsWith(':inputHistory')) throw new Error('unsupported remote command');
  if (cmd.type.endsWith(':requests')) {
    reads++;
    if (HISTORY==='lost' || (HISTORY==='delayed_lost' && reads===1)) throw new Error('fictional requests read failed');
    let entries = [];
    if (HISTORY!=='empty' && !(['delayed','delayed_lost'].includes(HISTORY) && reads<3)) {
      const entry = {id:HISTORY==='wrong_id'?'another':admission,
        userMessage:{content:[{type:'text',text:HISTORY==='wrong_text'?'another text':wireText}]}};
      if (HISTORY==='extra_blocks') entry.userMessage.content.push({type:'text',text:'unexpected'});
      entries = HISTORY==='duplicate'?[entry,entry]:[entry];
    }
    if (HISTORY==='old_shape') return {payload:{success:true,result:[{requestId:admission,blocks:[{type:'text',text:wireText}]}]}};
    return {payload:{success:true,result:{items:entries}}};
  }
  if (!cmd.type.endsWith(':sendPrompt')) throw new Error('unsupported remote command');
  admission = cmd.args[2]._meta?.['codebuddy.ai/conversationRequestId'];
  wireText = cmd.args[1][0].text.replace(/@skill:[A-Za-z0-9_-]+/g,'fictional-expanded-skill');
  if (OUTCOME==='lost') throw new Error('fictional lost response');
  if (OUTCOME==='writer_occupied') return {payload:{success:false,error:{code:'writer_occupied',message:'busy'}}};
  return {payload:{success:true,result:OUTCOME.startsWith('queued')?{disposition:'queued',item:{id:'queue',clientRequestId:OUTCOME==='queued_wrong_id'?'another':cmd.args[2].clientRequestId}}:{}}};
}}})};
""".replace("STATES", json.dumps(states)).replace("RECORD", json.dumps(str(self.record))).replace("OUTCOME", json.dumps(outcome)).replace("HISTORY", json.dumps(history))
        fake.write_text(script, encoding="utf8")
        self.input["sdkFile"] = str(fake)
        result = subprocess.run(["node", str(Path(workbuddy.__file__).with_name("workbuddy_sync.cjs"))],
                                input=json.dumps(self.input), text=True, capture_output=True, timeout=12)
        event = json.loads(result.stdout)
        commands = [json.loads(line) for line in self.record.read_text().splitlines()]
        sends = [c for c in commands if c["type"].endswith(":sendPrompt")]
        return event, commands, sends

    def test_idle_consumes_bound_permission_then_submits_once_without_steer(self):
        event, commands, sends = self.run_broker()
        self.assertEqual(event["result"], {"state": "accepted", "route": "native-sendPrompt"})
        self.assertEqual(len(sends), 1)
        self.assertNotIn("queueDelivery", sends[0]["args"][2])
        self.assertTrue(sends[0]["args"][2]["_expectQueueReceipt"])
        self.assertEqual(self.received, [{"ticket": "fictional-once-ticket", "task_id": "task", "action": "task",
                                         "target": self.input["target"], "message_id": "message",
                                         "text_hash": hashlib.sha256(b"fictional task").hexdigest()}])

    def test_busy_without_wait_never_consumes_or_submits(self):
        for state in ("working", "planning", "pending", "unknown", "", None):
            with self.subTest(state=state):
                event, commands, sends = self.run_broker(states=(state,))
                self.assertEqual(event["state"], "not_sent")
                self.assertEqual(sends, [])
                self.assertEqual(self.received, [])

    def test_native_prompt_ready_states_submit_with_or_without_wait(self):
        self.input["confirmDelivery"] = True
        for state in ("idle", "error", "failed", "terminated"):
            for wait in (0, 5):
                with self.subTest(state=state, wait=wait):
                    self.received.clear()
                    event, commands, sends = self.run_broker(states=(state,), wait=wait, history="matching")
                    self.assertEqual(event["result"]["receipt"]["state"], "accepted")
                    self.assertEqual(len(sends), 1)
                    self.assertNotIn("queueDelivery", sends[0]["args"][2])
                    self.assertEqual(sum(command["type"].endswith(":get") for command in commands), 1)
                    self.assertEqual(sum(command["type"].endswith(":requests") for command in commands), 1)
                    self.assertEqual(len(self.received), 1)

    def test_busy_waits_until_idle_and_consumes_only_after_wait(self):
        event, commands, sends = self.run_broker(states=("working", "idle"), wait=5)
        self.assertEqual(event["result"]["state"], "accepted")
        self.assertEqual(len(commands), 3)
        self.assertEqual(len(sends), 1)
        self.assertEqual(len(self.received), 1)

    def test_unknown_waits_for_ready_terminal_state_before_consuming(self):
        event, commands, sends = self.run_broker(states=("unknown", "terminated"), wait=5)
        self.assertEqual(event["result"]["state"], "accepted")
        self.assertEqual(len(commands), 3)
        self.assertEqual(len(sends), 1)
        self.assertEqual(len(self.received), 1)

    def test_revocation_at_final_consume_never_submits(self):
        self.status = 403
        self.permission = {"allowed": False}
        event, commands, sends = self.run_broker()
        self.assertEqual(event["state"], "not_sent")
        self.assertEqual(sends, [])
        self.assertEqual(len(self.received), 1)

    def test_native_race_queue_is_accepted_without_steer_or_resubmit(self):
        event, commands, sends = self.run_broker(outcome="queued")
        self.assertEqual(event["result"], {"state": "accepted", "route": "native-queue"})
        self.assertEqual(len(sends), 1)
        self.assertNotIn("queueDelivery", sends[0]["args"][2])

    def test_native_writer_occupied_is_not_sent_and_no_retry(self):
        event, commands, sends = self.run_broker(outcome="writer_occupied")
        self.assertEqual(event["state"], "not_sent")
        self.assertEqual(len(sends), 1)

    def test_lost_submit_response_is_uncertain_and_no_retry(self):
        event, commands, sends = self.run_broker(outcome="lost")
        self.assertEqual(event["state"], "uncertain")
        self.assertEqual(len(sends), 1)

    def test_confirmed_queue_receipt_matches_this_client_message(self):
        self.input["confirmDelivery"] = True
        event, commands, sends = self.run_broker(outcome="queued")
        self.assertEqual(event["result"]["receipt"], {"state": "queued", "stage": "host_queue", "native_id": "queue"})
        self.assertEqual(len(sends), 1)
        options = sends[0]["args"][2]
        self.assertEqual(options["clientRequestId"], self.input["messageId"])
        self.assertRegex(options["_meta"]["codebuddy.ai/conversationRequestId"], r"^[0-9a-f]{12}7[0-9a-f]{19}$")
        self.assertFalse(any(command["type"].endswith(":requests") for command in commands))

    def test_confirmed_direct_receipt_requires_exact_remote_request(self):
        self.input["confirmDelivery"] = True
        event, commands, sends = self.run_broker(history="matching")
        admission = sends[0]["args"][2]["_meta"]["codebuddy.ai/conversationRequestId"]
        self.assertEqual(event["result"]["receipt"], {"state": "accepted", "stage": "native_host", "native_id": admission})
        self.assertEqual(len(sends), 1)
        history = [command for command in commands if command["type"].endswith(":requests")]
        self.assertEqual([command["args"] for command in history], [["wb", 0, 3]])
        self.assertEqual(history[0]["timeoutMs"], 3000)
        self.assertFalse(any(command["type"].endswith(":inputHistory") for command in commands))

    def test_empty_or_ambiguous_history_stays_unconfirmed(self):
        self.input["confirmDelivery"] = True
        for history in ("empty", "wrong_id", "wrong_text", "extra_blocks", "duplicate", "old_shape"):
            with self.subTest(history=history):
                event, commands, sends = self.run_broker(history=history)
                self.assertEqual(event["result"]["state"], "uncertain")
                self.assertNotIn("receipt", event["result"])
                self.assertEqual(len(sends), 1)
                self.assertEqual(sum(command["type"].endswith(":requests") for command in commands), 3)

    def test_history_read_failure_never_repeats_prompt_submission(self):
        self.input["confirmDelivery"] = True
        event, commands, sends = self.run_broker(history="lost")
        self.assertEqual(event["result"]["state"], "uncertain")
        self.assertEqual(len(sends), 1)
        self.assertEqual(sum(command["type"].endswith(":requests") for command in commands), 3)

    def test_delayed_request_visibility_retries_only_reads(self):
        self.input["confirmDelivery"] = True
        for history in ("delayed", "delayed_lost"):
            with self.subTest(history=history):
                event, commands, sends = self.run_broker(history=history)
                self.assertEqual(event["result"]["receipt"]["state"], "accepted")
                self.assertEqual(len(sends), 1)
                self.assertEqual(sum(command["type"].endswith(":requests") for command in commands), 3)

    def test_unknown_send_marker_is_not_reused_or_reconciled(self):
        self.input["confirmDelivery"] = True
        first, _, first_sends = self.run_broker(history="empty")
        self.input["messageId"] = "next-message"
        second, _, second_sends = self.run_broker(history="matching")
        first_marker = first_sends[0]["args"][2]["_meta"]["codebuddy.ai/conversationRequestId"]
        second_marker = second_sends[0]["args"][2]["_meta"]["codebuddy.ai/conversationRequestId"]
        self.assertEqual(first["result"]["state"], "uncertain")
        self.assertNotEqual(first_marker, second_marker)
        self.assertEqual(second["result"]["receipt"]["native_id"], second_marker)

    def test_three_field_message_escapes_skill_mentions_without_changing_json_or_permission(self):
        self.input["confirmDelivery"] = True
        payload = {"message": "literal @skill:danger and \\@skill:other", "source": "codex", "source_conversation": "name@home"}
        self.input["text"] = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        event, commands, sends = self.run_broker(history="matching")
        wire_text = sends[0]["args"][1][0]["text"]
        self.assertEqual(event["result"]["receipt"]["state"], "accepted")
        self.assertNotIn("@", wire_text)
        self.assertIn("\\u0040skill:danger", wire_text)
        self.assertEqual(json.loads(wire_text), payload)
        self.assertEqual(self.received[-1]["text_hash"], hashlib.sha256(self.input["text"].encode()).hexdigest())

    def test_legacy_or_non_message_json_keeps_its_wire_text(self):
        for text in ("legacy @skill:example", '{"message":"@skill:example"}',
                     '{"message":"@skill:example","source":"codex","source_conversation":"chat"}',
                     '{"message":"@skill:example","source":"codex","source_conversation":"chat","extra":true}',
                     '{"message":42,"source":"codex","source_conversation":"chat"}'):
            with self.subTest(text=text):
                self.input["text"] = text
                event, commands, sends = self.run_broker()
                self.assertEqual(event["result"]["state"], "accepted")
                self.assertEqual(sends[0]["args"][1][0]["text"], text)

    def test_new_mode_keeps_non_message_text_unescaped(self):
        self.input["confirmDelivery"] = True
        for text in ("legacy @skill:example", '{"message":"@skill:example"}',
                     '{"message":"@skill:example","source":"codex","source_conversation":"chat","extra":true}'):
            with self.subTest(text=text):
                self.input["text"] = text
                event, commands, sends = self.run_broker(outcome="queued")
                self.assertEqual(event["result"]["receipt"]["state"], "queued")
                self.assertEqual(sends[0]["args"][1][0]["text"], text)

    def test_unmatched_queue_message_does_not_confirm_receipt(self):
        self.input["confirmDelivery"] = True
        event, commands, sends = self.run_broker(outcome="queued_wrong_id")
        self.assertEqual(event["result"]["state"], "uncertain")
        self.assertNotIn("receipt", event["result"])
        self.assertEqual(len(sends), 1)


class HelperTests(unittest.TestCase):
    def test_managed_missing_permit_rejected_before_any_broker_or_credentials(self):
        with mock.patch("workbuddy.importlib.import_module") as load, self.assertRaises(BridgeError):
            workbuddy.invoke({"device_id": "pc", "a2a": {"managed_ui": True}},
                             {"device_id": "pc", "software": "workbuddy", "conversation_id": "wb"},
                             "send", "task", "message")
        load.assert_not_called()


if __name__ == "__main__":
    unittest.main()
