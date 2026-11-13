"""All connected original chats can communicate; identity boundaries still apply."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from access_policy import Policy, chat_ref, external_a2a_ready


def fixtures():
    local = {"device_id": "pc", "a2a": {"device_tokens": {"phone": "fictional-token"}}}
    peers = {"research": {"alias": "研究", "device_id": "pc", "software": "codex", "conversation_id": "chat-a", "verified": True},
             "phone": {"alias": "手机", "device_id": "phone", "software": "minis", "conversation_id": "chat-b", "verified": True}}
    return local, peers


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.local, self.peers = fixtures()
        self.policy = Policy(self.root, self.local, self.peers)
        self.a, self.b = self.peers["research"], self.peers["phone"]

    def test_any_connected_pair_in_both_directions_without_rules(self):
        self.peers["work"] = {"alias": "工作", "device_id": "pc", "software": "workbuddy",
                              "conversation_id": "chat-c", "verified": True}
        for source in self.peers.values():
            for target in self.peers.values():
                if source != target:
                    decision = self.policy.check(source, target)
                    self.assertTrue(decision["contact"])
                    self.assertFalse(decision["busy"])
        self.assertEqual(list(self.root.iterdir()), [])

    def test_legacy_ui_and_direction_fields_are_unused(self):
        self.local["a2a"].update(device_permissions={}, message_permissions=[], managed_ui=True)
        runtime=self.root / "runtime"
        runtime.mkdir()
        paths=[runtime / "ui-policy.json", runtime / "ui-auth.json"]
        for path in paths:
            path.write_bytes(b"broken retired file")
        self.assertTrue(self.policy.check(self.a, self.b)["contact"])
        self.assertTrue(self.policy.check(self.b, self.a)["contact"])
        self.assertEqual([p.read_bytes() for p in paths], [b"broken retired file"] * 2)
        self.assertFalse(hasattr(self.policy, "snapshot"))

    def test_new_connected_software_inherits_contact_without_grants(self):
        self.local["a2a"]["device_tokens"]["new-device"]="fictional-new-pair"
        peer={"alias": "新伙伴", "device_id": "new-device", "software": "future-host",
              "conversation_id": "new-original", "verified": True}
        self.peers["new"]=peer
        self.assertTrue(self.policy.check(peer, self.b)["contact"])
        self.assertTrue(self.policy.check(self.b, peer)["contact"])

    def test_unregistered_unverified_unpaired_disabled_and_duplicate_are_inert(self):
        for kind in ("unregistered", "unverified", "unpaired", "disabled", "duplicate_ref", "duplicate_alias"):
            with self.subTest(kind=kind):
                local, peers=fixtures()
                a,b=peers["research"],peers["phone"]
                if kind == "unregistered":
                    peers.pop("phone")
                elif kind == "unverified":
                    b["verified"]=False
                elif kind == "unpaired":
                    local["a2a"]["device_tokens"]={}
                elif kind == "disabled":
                    b["enabled"]=False
                elif kind == "duplicate_ref":
                    peers["copy"]={**b,"alias":"另一个别名"}
                else:
                    peers["copy"]={**b,"conversation_id":"other-chat"}
                policy=Policy(self.root,local,peers)
                self.assertFalse(policy.check(a,b)["contact"])
                self.assertFalse(policy.check(b,a)["contact"])

    def test_identity_is_complete_and_different(self):
        self.assertFalse(self.policy.check(self.a,self.a)["contact"])
        for bad in ({}, ["pc","codex"], ["pc","codex", ""], ["pc","codex","bad\nchat"], None):
            self.assertFalse(self.policy.check(bad,self.a)["contact"])
        claimed={**self.b,"conversation_id":"another-original","verified":True}
        self.assertFalse(self.policy.check(claimed,self.a)["contact"])

    def external(self):
        peer = {"alias": "外部", "device_id": "independent-device", "software": "new-host", "transport": "a2a",
                "conversation_id": "verified-original-chat", "url": "https://trusted.example/a2a/chat-one", "verified": True}
        peer.update(verified_url=peer["url"], verified_ref=[peer[k] for k in ("device_id", "software", "conversation_id")])
        self.local["a2a_credentials"] = {"外部": {"token": "fictional-independent-destination-token"}}
        self.peers["external"] = peer
        return peer

    def test_external_verified_identity_needs_no_device_token(self):
        peer = self.external()
        self.assertTrue(external_a2a_ready(self.local, peer))
        self.assertTrue(self.policy.check(self.a, peer)["contact"])
        chat = next(c for c in self.policy.chat_list() if c["alias"] == "外部")
        self.assertTrue(chat["verified"])
        self.assertTrue(chat["paired"])
        peer["software"] = "codex"
        peer["verified_ref"] = [peer[k] for k in ("device_id", "software", "conversation_id")]
        self.assertFalse(self.policy.check(self.a, peer)["busy"])
        peer["software"] = "new-host"
        peer["verified_ref"] = [peer[k] for k in ("device_id", "software", "conversation_id")]
        self.assertNotIn("fictional-independent", json.dumps(chat))
        peer["url"] = "https://trusted.example/a2a/changed"
        self.assertFalse(self.policy.check(self.a, peer)["contact"])
        peer["url"] = peer["verified_url"]
        peer["conversation_id"] = "changed-chat"
        self.assertFalse(external_a2a_ready(self.local, peer))

    def test_external_shared_or_missing_credentials_are_inert(self):
        peer = self.external()
        for field in ("tokens", "device_tokens", "chat_tokens"):
            self.local["a2a"].setdefault(field, {})["some-central-caller"] = "x" * 40
            self.local["a2a_credentials"]["外部"]["token"] = "x" * 40
            self.assertFalse(external_a2a_ready(self.local, peer))
            del self.local["a2a"][field]["some-central-caller"]
        self.local["a2a_credentials"] = {}
        self.assertFalse(external_a2a_ready(self.local, peer))

    def test_external_private_dns_and_short_issuer_token(self):
        peer = self.external()
        self.local["a2a_credentials"]["外部"]["token"] = "issuer-key"
        for url in ("https://lab.internal/a2a", "https://research.local:8443/agent"):
            peer["url"] = peer["verified_url"] = url
            self.assertTrue(external_a2a_ready(self.local, peer))
        self.local["a2a_credentials"]["另一个目标"] = {"token": "issuer-key"}
        self.assertFalse(external_a2a_ready(self.local, peer))
        del self.local["a2a_credentials"]["另一个目标"]
        for token in ("", "contains space", "unsafe\r\nheader", "非ASCII", "x" * 4097):
            self.local["a2a_credentials"]["外部"]["token"] = token
            self.assertFalse(external_a2a_ready(self.local, peer))

    def test_external_dangerous_endpoint_and_network_claims_do_not_verify(self):
        peer = self.external()
        for url in ("http://trusted.example/a2a", "https://u:p@trusted.example/a2a", "https://trusted.example/a2a?q=1", "https://trusted.example/a2a#x",
                    "https://localhost/a2a", "https://127.1/a2a", "https://0177.0.0.1/a2a", "https://127.0.0.1/a2a", "https://[::1]/a2a",
                    "https://169.254.169.254/latest", "https://100.100.100.200/a2a", "https://metadata.google.internal/a2a",
                    "https://trusted.example/ui/api/rules", "https://trusted.example/%2575i/api/rules", "https://trusted.example/a/../ui", "https://trusted.example/a\\b"):
            peer["url"] = peer["verified_url"] = url
            self.assertFalse(external_a2a_ready(self.local, peer), url)
        peer["url"] = peer["verified_url"] = "https://trusted.example/a2a"
        peer["verified"] = False
        self.assertFalse(external_a2a_ready(self.local, peer))
        self.assertTrue(external_a2a_ready(self.local, peer, require_verified=False))
        self.local["a2a"]["public_base_url"] = "https://trusted.example"
        self.assertFalse(external_a2a_ready(self.local, peer, require_verified=False))



if __name__ == "__main__":
    unittest.main()
