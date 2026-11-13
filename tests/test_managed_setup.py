from contextlib import closing
import copy
import ctypes
import json
import os
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from access_policy import Policy, chat_ref
import managed_setup as setup
from test_access_policy import fixtures


class ManagedSetupTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.local, self.peers = fixtures()
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()
        self.local["a2a"].update(port=port, public_base_url="https://fictional.example:18443")
        self.local["network"] = {"never-change": "fictional-network-secret"}
        self.book = {"version": 1, "peers": {p["alias"]: {key: value for key, value in p.items() if key != "alias"} for p in self.peers.values()}}
        self.write("local.json", self.local)
        self.write("peers.json", self.book)

    def write(self, name, value):
        path = self.root / name
        path.parent.mkdir(exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")

    def load(self, name):
        return json.loads((self.root / name).read_text(encoding="utf-8"))

    def test_inspect_read_only_and_metadata_only(self):
        before = {p.name: p.read_bytes() for p in self.root.iterdir()}
        result = setup.inspect(self.root)
        self.assertEqual(result["queues"]["active"], 0)
        self.assertNotIn("password_ready", result)
        self.assertNotIn("policy_ready", result)
        serialized = json.dumps(result)
        self.assertNotIn("fictional-token", serialized)
        self.assertNotIn("fictional-network-secret", serialized)
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.root.iterdir()})

    def test_prepare_exact_identity_preserves_network_and_secrets(self):
        with self.assertRaises(setup.SetupError):
            setup.prepare(self.root, [["pc", "codex", "wrong"]])
        result = setup.prepare(self.root, [chat_ref(self.peers["research"]), chat_ref(self.peers["phone"])])
        local = self.load("local.json")
        self.assertEqual(local["network"], self.local["network"])
        self.assertEqual(local["a2a"]["device_tokens"], self.local["a2a"]["device_tokens"])
        self.assertNotIn("message_permissions", local["a2a"])
        self.assertNotIn("managed", result)
        token = local["a2a"]["chat_tokens"]["研究"]
        self.assertGreaterEqual(len(token), 32)
        self.assertEqual(local["a2a"]["chat_bindings"]["研究"], {k: self.peers["research"][k] for k in ("device_id", "software", "conversation_id")})
        self.assertNotIn(token, json.dumps(result))
        setup.prepare(self.root, [chat_ref(self.peers["research"])])
        self.assertEqual(self.load("local.json")["a2a"]["chat_tokens"]["研究"], token)
        self.assertTrue((self.root / result["backup"] / "local.json").is_file())

    def test_existing_token_wrong_binding_refused_without_changes(self):
        self.local["a2a"]["chat_tokens"] = {"研究": "x" * 48}
        self.local["a2a"]["chat_bindings"] = {"研究": {"device_id": "pc", "software": "codex", "conversation_id": "other"}}
        self.write("local.json", self.local)
        original = (self.root / "local.json").read_bytes()
        with self.assertRaises(setup.SetupError):
            setup.prepare(self.root, [chat_ref(self.peers["research"])])
        self.assertEqual((self.root / "local.json").read_bytes(), original)

    def test_transaction_failure_restores_both_files(self):
        originals = {name: (self.root / name).read_bytes() for name in ("local.json", "peers.json")}
        write = setup._write
        once = [False]
        def failing(path, raw, template):
            if path == self.root / "local.json" and not once[0]:
                once[0] = True
                raise OSError("simulated-private-failure")
            return write(path, raw, template)
        with patch.object(setup, "_write", side_effect=failing):
            with self.assertRaises(setup.SetupError):
                setup.prepare(self.root, [chat_ref(self.peers["research"])])
        for name in originals:
            self.assertEqual((self.root / name).read_bytes(), originals[name])

    def test_security_applied_before_private_bytes(self):
        secure = setup._security
        staged = []
        def checked(path, template):
            if path.name.startswith(".managed-"):
                self.assertEqual(path.read_bytes(), b"")
                staged.append(path)
            return secure(path, template)
        with patch.object(setup, "_security", side_effect=checked):
            setup.prepare(self.root, [chat_ref(self.peers["research"])])
        self.assertGreaterEqual(len(staged), 4)

    def test_queue_inspection_is_metadata_only_and_reports_unknown_database(self):
        dbpath = self.root / "runtime" / "pending-results.db"
        dbpath.parent.mkdir()
        with closing(sqlite3.connect(dbpath)) as db, db:
            db.execute("CREATE TABLE pending_results(state TEXT, result TEXT)")
            db.execute("INSERT INTO pending_results VALUES ('waiting','never-output-this-body')")
        inspected=setup.inspect(self.root)
        self.assertEqual(inspected["queues"]["active"],1)
        self.assertNotIn("never-output-this-body",json.dumps(inspected))
        dbpath.write_bytes(b"not-a-database")
        self.assertIn("pending-results.db",setup.inspect(self.root)["queues"]["unavailable"])

    def test_pending_ui_registration_cannot_create_or_verify_an_unknown_chat(self):
        self.write("runtime/ui-policy.json", {"registrations": [{"alias":"新聊天", "device_id":"pc",
            "software":"workbuddy", "conversation_id":"new-chat"}]})
        before={name:(self.root/name).read_bytes() for name in ("local.json","peers.json","runtime/ui-policy.json")}
        with self.assertRaises(setup.SetupError):
            setup.prepare(self.root, [["pc","workbuddy","new-chat"]])
        self.assertEqual(before,{name:(self.root/name).read_bytes() for name in before})

    def test_phone_prepare_preserves_paired_credentials_without_ui(self):
        self.local.update(platform="minis",device_id="phone")
        self.local["network"]={"base_url":"https://fictional.example:18443", "token":"fictional-paired-token"}
        self.write("local.json",self.local)
        setup.prepare(self.root,[chat_ref(self.peers["phone"])])
        self.assertEqual(self.load("local.json")["network"],self.local["network"])
        self.assertNotIn("chat_tokens",self.load("local.json")["network"])

    def test_prepare_rejects_duplicate_refs_and_unpaired_device_without_changes(self):
        before=(self.root/"peers.json").read_bytes()
        ref=chat_ref(self.peers["research"])
        with self.assertRaises(setup.SetupError):
            setup.prepare(self.root,[ref,ref])
        self.local["a2a"]["device_tokens"]={}
        self.write("local.json",self.local)
        with self.assertRaises(setup.SetupError):
            setup.prepare(self.root,[chat_ref(self.peers["phone"])])
        self.assertEqual((self.root/"peers.json").read_bytes(),before)

    def external(self):
        ref = ["external-device", "external-host", "original-chat"]
        self.book["peers"]["外部服务"] = {"device_id": ref[0], "software": ref[1], "conversation_id": ref[2],
                                               "transport": "a2a", "url": "https://trusted.example/a2a"}
        self.local["a2a_credentials"] = {"外部服务": {"token": "independent-fictional-target-token-12345"}}
        self.write("local.json", self.local)
        self.write("peers.json", self.book)
        return ref

    def test_external_prepare_pins_endpoint_and_original_chat(self):
        ref = self.external()
        result = setup.prepare(self.root, [ref])
        peer = self.load("peers.json")["peers"]["外部服务"]
        self.assertTrue(peer["verified"])
        self.assertEqual(peer["verified_ref"], ref)
        self.assertEqual(peer["verified_url"], peer["url"])
        self.assertNotIn("independent-fictional", json.dumps(result))
        inspected = setup.inspect(self.root)
        self.assertTrue(next(c for c in inspected["chats"] if c["alias"] == "外部服务")["verified"])
        self.assertEqual(self.load("local.json")["a2a_credentials"], self.local["a2a_credentials"])

    def test_external_prepare_accepts_private_dns_issuer_bearer(self):
        ref = self.external()
        self.book["peers"]["外部服务"]["url"] = "https://lab.internal/a2a"
        self.local["a2a_credentials"]["外部服务"]["token"] = "issuer-key"
        self.write("local.json", self.local)
        self.write("peers.json", self.book)
        setup.prepare(self.root, [ref])
        peer = self.load("peers.json")["peers"]["外部服务"]
        self.assertTrue(peer["verified"])
        self.assertEqual(peer["verified_url"], "https://lab.internal/a2a")
        self.assertEqual(self.load("local.json")["a2a_credentials"]["外部服务"]["token"], "issuer-key")

    def test_external_prepare_rejects_missing_shared_or_dangerous_credentials(self):
        ref = self.external()
        self.local["a2a_credentials"] = {}
        self.write("local.json", self.local)
        with self.assertRaises(setup.SetupError):
            setup.prepare(self.root, [ref])
        self.local["a2a_credentials"] = {"外部服务": {"token": "x" * 40}}
        self.local["a2a"]["tokens"] = {"local": "x" * 40}
        self.write("local.json", self.local)
        with self.assertRaises(setup.SetupError):
            setup.prepare(self.root, [ref])
        self.local["a2a_credentials"]["外部服务"]["token"] = "independent-fictional-target-token-12345"
        self.write("local.json", self.local)
        self.book["peers"]["外部服务"]["url"] = "https://127.0.0.1/ui/api/messages"
        self.write("peers.json", self.book)
        with self.assertRaises(setup.SetupError):
            setup.prepare(self.root, [ref])
        self.assertNotIn("verified", self.load("peers.json")["peers"]["外部服务"])

    @unittest.skipUnless(os.name == "nt", "Windows DACL retention")
    def test_windows_replace_preserves_original_dacl(self):
        from ctypes import wintypes
        def dacl(path):
            api = ctypes.WinDLL("advapi32", use_last_error=True)
            needed = wintypes.DWORD()
            api.GetFileSecurityW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.LPVOID, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]
            api.GetFileSecurityW(str(path), 4, None, 0, ctypes.byref(needed))
            data = ctypes.create_string_buffer(needed.value)
            self.assertTrue(api.GetFileSecurityW(str(path), 4, data, needed, ctypes.byref(needed)))
            pointer, present, defaulted = ctypes.c_void_p(), wintypes.BOOL(), wintypes.BOOL()
            api.GetSecurityDescriptorDacl.argtypes = [wintypes.LPVOID, ctypes.POINTER(wintypes.BOOL), ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(wintypes.BOOL)]
            self.assertTrue(api.GetSecurityDescriptorDacl(data, ctypes.byref(present), ctypes.byref(pointer), ctypes.byref(defaulted)))
            self.assertTrue(present.value)
            size = ctypes.c_ushort.from_address(pointer.value + 2).value
            control, revision = wintypes.WORD(), wintypes.DWORD()
            api.GetSecurityDescriptorControl.argtypes = [wintypes.LPVOID, ctypes.POINTER(wintypes.WORD), ctypes.POINTER(wintypes.DWORD)]
            self.assertTrue(api.GetSecurityDescriptorControl(data, ctypes.byref(control), ctypes.byref(revision)))
            return ctypes.string_at(pointer, size), control.value & 0x1000
        original = dacl(self.root / "local.json")
        setup.prepare(self.root, [chat_ref(self.peers["research"])])
        self.assertEqual(dacl(self.root / "local.json"), original)

if __name__ == "__main__":
    unittest.main()
