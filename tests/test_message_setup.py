"""Phone source registration and queue maintenance preserve delivery identity."""
from contextlib import closing, redirect_stdout
import copy
import io
import json
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from access_policy import chat_ref
from device_receiver import _instance_lock
from result_watcher import PendingResults
import managed_setup as setup


class MessageSetupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()
        self.peers = {
            "codex": {"alias": "Codex", "device_id": "pc", "software": "codex", "conversation_id": "cx", "verified": True},
            "workbuddy": {"alias": "WorkBuddy", "device_id": "pc", "software": "workbuddy", "conversation_id": "wb", "verified": True},
            "minis": {"alias": "Minis", "device_id": "phone", "software": "minis", "conversation_id": "ms", "verified": True},
        }
        self.local = {"device_id": "pc", "keep": {"choice": "retain"}, "network": {"unrelated": "fictional-network-value"},
                      "a2a": {"port": port, "host": "127.0.0.1", "managed_ui": False,
                              "tokens": {"local": "fictional-local-token" * 3},
                              "device_tokens": {"phone": "fictional-phone-token" * 3},
                              "device_permissions": {"phone": ["Codex"]},
                              "chat_tokens": {p["alias"]: "fictional-" + p["conversation_id"] * 32
                                              for p in self.peers.values() if p["device_id"] == "pc"},
                              "chat_bindings": {p["alias"]: {k: p[k] for k in ("device_id", "software", "conversation_id")}
                                                for p in self.peers.values() if p["device_id"] == "pc"}}}
        self.save()

    def write(self, name, document):
        path = self.root / name
        path.parent.mkdir(exist_ok=True)
        path.write_text(json.dumps(document), encoding="utf8")

    def save(self):
        self.write("local.json", self.local)
        self.write("peers.json", {"version": 1, "peers": {
            p["alias"]: {k: v for k, v in p.items() if k != "alias"} for p in self.peers.values()}})

    def load(self):
        return json.loads((self.root / "local.json").read_text(encoding="utf8"))

    def pending(self, table, state, mode="message"):
        runtime = self.root / "runtime"
        runtime.mkdir(exist_ok=True)
        filename = "pending-results.db" if table == "pending_results" else "device-receipts.db"
        with closing(sqlite3.connect(runtime / filename)) as db, db:
            if table == "pending_results":
                db.execute("CREATE TABLE pending_results(state TEXT, mode TEXT)")
                db.execute("INSERT INTO pending_results VALUES (?,?)", (state, mode))
            else:
                db.execute("CREATE TABLE receipts(state TEXT, mode TEXT, acked INTEGER, ack_expired INTEGER, reply_received INTEGER)")
                db.execute("INSERT INTO receipts VALUES (?,?,1,0,0)", (state, mode))

    def test_confirmed_message_receipts_and_unknown_tombstones_do_not_wait_for_business_reply(self):
        self.pending("pending_results", "uncertain")
        self.pending("receipts", "accepted")
        self.assertEqual(setup._counts(self.root)["active"], 0)

    def test_legacy_unanswered_receipt_keeps_original_guard(self):
        self.pending("receipts", "accepted", mode="legacy")
        self.assertEqual(setup._counts(self.root)["active"], 1)

    def test_old_and_new_pending_schema_terminal_unknown_does_not_block_or_replay(self):
        for schema in ("old", "new-legacy", "new-message"):
            with self.subTest(schema=schema):
                root = self.root / schema
                root.mkdir()
                (root / "local.json").write_text(json.dumps(self.local), encoding="utf8")
                (root / "peers.json").write_bytes((self.root / "peers.json").read_bytes())
                pending = PendingResults(root, clock=lambda: 100)
                pending.prepare(self.peers["codex"], self.peers["workbuddy"], "unknown")
                with pending.connect() as db:
                    db.execute("UPDATE pending_results SET state='uncertain',mode=?",
                               ("message" if schema == "new-message" else "legacy",))
                    if schema == "old":
                        db.execute("ALTER TABLE pending_results DROP COLUMN mode")
                self.assertEqual(setup._counts(root)["active"], 0)
                self.assertEqual(pending.due(), [])
                self.assertFalse(pending.claim_notification("unknown"))
                self.assertEqual(pending.get("unknown")["state"], "uncertain")

    def test_old_and_new_pending_schema_dispatching_and_other_ready_still_block(self):
        for schema in ("old", "new-legacy", "new-message"):
            with self.subTest(schema=schema):
                root = self.root / schema
                root.mkdir()
                (root / "local.json").write_text(json.dumps(self.local), encoding="utf8")
                (root / "peers.json").write_bytes((self.root / "peers.json").read_bytes())
                pending = PendingResults(root, clock=lambda: 100)
                for identifier in ("unknown", "handoff"):
                    pending.prepare(self.peers["codex"], self.peers["workbuddy"], identifier)
                with pending.connect() as db:
                    db.execute("UPDATE pending_results SET state='uncertain',mode=?",
                               ("message" if schema == "new-message" else "legacy",))
                    db.execute("UPDATE pending_results SET state='dispatching' WHERE id='handoff'")
                    if schema == "old":
                        db.execute("ALTER TABLE pending_results DROP COLUMN mode")
                self.assertEqual(setup._counts(root)["active"], 1)
                self.assertEqual(pending.due(), [])
                self.assertFalse(pending.claim_notification("handoff"))
                with pending.connect() as db:
                    db.execute("UPDATE pending_results SET state='ready' WHERE id='handoff'")
                self.assertEqual(setup._counts(root)["active"], 1)
                self.assertEqual([row["id"] for row in pending.due()], ["handoff"])
                self.assertTrue(pending.claim_notification("handoff"))
                self.assertEqual(pending.get("unknown")["state"], "uncertain")

    def phone(self):
        local = copy.deepcopy(self.local)
        local.update(platform="minis", device_id="phone")
        local["network"] = {"base_url": "https://paired.example.ts.net:18443", "token": "fictional-paired-token"}
        book = {"version": 1, "peers": {
            "Codex": {"device_id": "pc", "software": "a2a", "conversation_id": "cx", "verified": True,
                      "url": "https://paired.example.ts.net:18443/agents/Codex", "keep": "preserve"},
            "Minis": {k: v for k, v in self.peers["minis"].items() if k != "alias"}}}
        self.write("local.json", local)
        self.write("peers.json", book)
        return local, book

    def test_phone_source_migration_only_corrects_three_fields_and_is_idempotent(self):
        local, before = self.phone()
        original_local = (self.root / "local.json").read_bytes()
        result = setup.bind_phone_sources(self.root, [("Codex", "pc", "codex", "cx")])
        self.assertEqual(result["phone_sources_updated"], 1)
        after = json.loads((self.root / "peers.json").read_text(encoding="utf8"))
        after["peers"]["Codex"].pop("transport")
        after["peers"]["Codex"].pop("host_software")
        after["peers"]["Codex"]["software"] = "a2a"
        self.assertEqual(after, before)
        self.assertEqual((self.root / "local.json").read_bytes(), original_local)
        repeated = setup.bind_phone_sources(self.root, [("codex", "pc", "codex", "cx")])
        self.assertEqual(repeated["phone_sources_updated"], 0)
        self.assertIsNone(repeated["backup"])
        self.assertNotIn("fictional", json.dumps(result))

    def test_phone_sources_reject_different_routes_and_established_software(self):
        for change in ("url", "software", "host_software", "conversation"):
            with self.subTest(change=change):
                local, book = self.phone()
                if change == "url":
                    book["peers"]["Codex"]["url"] = "https://other.example.ts.net:18443/agents/Codex"
                elif change == "conversation":
                    book["peers"]["Codex"]["conversation_id"] = "other-chat"
                else:
                    book["peers"]["Codex"][change] = "workbuddy"
                self.write("peers.json", book)
                original = (self.root / "peers.json").read_bytes()
                with self.assertRaises(setup.SetupError):
                    setup.bind_phone_sources(self.root, [("Codex", "pc", "codex", "cx")])
                self.assertEqual((self.root / "peers.json").read_bytes(), original)

    def test_phone_source_migration_requires_receiver_stopped_and_no_handoff(self):
        self.phone()
        with _instance_lock(self.root), self.assertRaises(setup.SetupError):
            setup.bind_phone_sources(self.root, [("Codex", "pc", "codex", "cx")])
        self.pending("receipts", "dispatching")
        with self.assertRaises(setup.SetupError):
            setup.bind_phone_sources(self.root, [("Codex", "pc", "codex", "cx")])

    def test_phone_source_correction_allows_acked_legacy_awaiting_business_result(self):
        self.phone()
        self.pending("receipts", "accepted", mode="legacy")
        self.pending("pending_results", "waiting", mode="legacy")
        result = setup.bind_phone_sources(self.root, [("Codex", "pc", "codex", "cx")])
        self.assertEqual(result["phone_sources_updated"], 1)

    def test_phone_source_correction_refuses_unacked_handoff(self):
        self.phone()
        self.pending("receipts", "accepted", mode="legacy")
        with closing(sqlite3.connect(self.root / "runtime/device-receipts.db")) as db, db:
            db.execute("UPDATE receipts SET acked=0")
        with self.assertRaises(setup.SetupError):
            setup.bind_phone_sources(self.root, [("Codex", "pc", "codex", "cx")])

    def test_phone_sources_accept_future_host_without_direction_grants(self):
        local,book=self.phone()
        result=setup.bind_phone_sources(self.root,[("Codex","pc","future-host","cx")])
        self.assertEqual(result["phone_sources_updated"],1)
        entry=json.loads((self.root/"peers.json").read_text(encoding="utf8"))["peers"]["Codex"]
        self.assertEqual(entry["software"],"future-host")
        self.assertNotIn("message_permissions",self.load()["a2a"])

    def test_cli_only_exposes_inspection_and_identity_registration(self):
        output=io.StringIO()
        with patch.object(sys,"argv",["managed_setup.py","--help"]), redirect_stdout(output), self.assertRaises(SystemExit) as stopped:
            setup.main()
        self.assertEqual(stopped.exception.code,0)
        help_text=output.getvalue()
        self.assertIn("phone-sources",help_text)
        self.assertNotIn("--allow",help_text)
        self.assertNotIn("--policy-version",help_text)
        self.assertNotIn("activate",help_text)



if __name__ == "__main__":
    unittest.main()
