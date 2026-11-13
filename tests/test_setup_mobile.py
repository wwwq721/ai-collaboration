import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import setup_mobile


class SetupMobileTests(unittest.TestCase):
    def test_init_writes_minis_identity_without_network_pairing(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with mock.patch.object(setup_mobile.sys, "executable", "/phone/python"):
                result = setup_mobile.initialize(root, resolver=lambda: "root-session")
            local = json.loads((root / "local.json").read_text(encoding="utf-8"))
            peers = json.loads((root / "peers.json").read_text(encoding="utf-8"))
            self.assertEqual(result, {"config_ready": True, "network_paired": False, "receiver_started": False})
            self.assertEqual(local["platform"], "minis")
            self.assertEqual(local["machine_name"], setup_mobile.platform.node())
            self.assertEqual(local["python"], "/phone/python")
            self.assertEqual(local["network"], {})
            self.assertEqual(peers["peers"]["Minis"]["conversation_id"], "root-session")
            self.assertRegex(local["device_id"], r"^minis-[A-Za-z0-9_-]{12}$")

    def test_init_honors_explicit_id_and_validates_format(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            setup_mobile.initialize(root, device_id="test-mobile-device", resolver=lambda: "root-session")
            local = json.loads((root / "local.json").read_text(encoding="utf-8"))
            self.assertEqual(local["device_id"], "test-mobile-device")
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(setup_mobile.SetupError):
                setup_mobile.initialize(Path(temp), device_id="bad id", resolver=lambda: "root-session")

    def test_init_does_not_overwrite_existing_config(self):
        with __import__("tempfile").TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "local.json").write_text('{"device_id":"other"}')
            with self.assertRaises(setup_mobile.SetupError):
                setup_mobile.initialize(root, resolver=lambda: "unused")
            self.assertEqual(json.loads((root / "local.json").read_text(encoding="utf-8"))["device_id"], "other")
            self.assertFalse((root / "peers.json").exists())

    def test_init_requires_trusted_minis_conversation(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self.assertRaises(setup_mobile.SetupError):
                setup_mobile.initialize(root, resolver=lambda: None)
            self.assertFalse((root / "local.json").exists())

    def test_install_uses_isolated_venv_wheel_only_and_records_versions_under_root(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with mock.patch.object(setup_mobile, "_venv_python", lambda env: env / "python"):
                calls = []

                def runner(command, **kwargs):
                    calls.append(command)
                    if command[1:3] == ["-m", "venv"]:
                        (root / ".a2a-env").mkdir()
                        (root / ".a2a-env" / "python").write_text("")
                        return subprocess.CompletedProcess(command, 0, "", "")
                    if "install" in command:
                        self.assertIn("--only-binary=:all:", command)
                        self.assertIn("--no-cache-dir", command)
                        self.assertNotIn("--system-site-packages", command)
                        return subprocess.CompletedProcess(command, 0, "", "")
                    return subprocess.CompletedProcess(command, 0, '[["a2a-sdk", "1.3.0"]]', "")

                (root / "requirements-mobile.txt").write_text("a2a-sdk==1.3.0\n")
                result = setup_mobile.install_client(root, runner=runner)
            self.assertEqual(result["packages"], [["a2a-sdk", "1.3.0"]])
            self.assertEqual(len(calls), 3)
            record = root / "runtime" / "mobile-client-installed.json"
            self.assertEqual(json.loads(record.read_text(encoding="utf-8")), {"packages": [["a2a-sdk", "1.3.0"]]})

    def test_install_failure_does_not_echo_installer_logs(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with mock.patch.object(setup_mobile, "_venv_python", lambda env: env / "python"):
                (root / ".a2a-env").mkdir()
                (root / ".a2a-env" / "python").write_text("")
                (root / "requirements-mobile.txt").write_text("a2a-sdk==1.3.0\n")

                def runner(command, **kwargs):
                    return subprocess.CompletedProcess(command, 1, "SECRET OUTPUT", "SECRET ERROR")

                with self.assertRaises(setup_mobile.SetupError) as context:
                    setup_mobile.install_client(root, runner=runner)
            self.assertNotIn("SECRET", str(context.exception))

    def test_check_keeps_config_network_and_receiver_states_separate(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "local.json").write_text(json.dumps({
                "device_id": "phone", "platform": "minis", "network": {}
            }), encoding="utf-8")
            (root / "peers.json").write_text(json.dumps({"version": 1, "peers": {"Minis": {}}}), encoding="utf-8")
            with mock.patch.object(setup_mobile, "_sdk_status", return_value=True), \
                    mock.patch.object(setup_mobile.shutil, "which", return_value=None), \
                    mock.patch.object(setup_mobile, "_venv_python", return_value=root / "missing"):
                result = setup_mobile.check(root)
            self.assertTrue(result["config_ready"])
            self.assertFalse(result["network_paired"])
            self.assertFalse(result["receiver_running"])


if __name__ == "__main__":
    unittest.main()
