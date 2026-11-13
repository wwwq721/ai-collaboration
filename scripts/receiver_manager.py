"""Portable cooperative start/status/stop commands for the phone receiver."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from device_receiver import _read_json, _service_status
from workbuddy import BridgeError

_CHILDREN = {}


def _health(root):
    path = Path(root) / "runtime" / "device-receiver-status.json"
    try:
        result = json.loads(path.read_text(encoding="utf-8"))
        return result if isinstance(result, dict) else {}
    except (OSError, ValueError):
        return {}


def status(root=ROOT):
    root = Path(root)
    local = _read_json(root / "local.json")
    return _service_status(root, local.get("device_id"))


def start(root=ROOT, timeout=8):
    root = Path(root).resolve()
    current = status(root)
    if current["running"]:
        deadline = time.monotonic() + max(0.1, timeout)
        while current.get("state") == "starting" and time.monotonic() < deadline:
            time.sleep(0.1)
            current = status(root)
        return {**current, "action": "already_running" if current.get("pid") else "starting"}
    previous_instance = _health(root).get("instance")
    instance = uuid.uuid4().hex
    command = [sys.executable, "-X", "utf8", "-B", str(SCRIPT_DIR / "device_receiver.py"),
               "--root", str(root), "--instance", instance]
    options = {"stdin": subprocess.DEVNULL, "stdout": subprocess.DEVNULL,
               "stderr": subprocess.DEVNULL, "close_fds": True}
    if os.name == "nt":
        options["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    else:
        options["start_new_session"] = True
    try:
        child = subprocess.Popen(command, **options)
    except OSError as error:
        raise BridgeError("无法启动设备接收服务（" + type(error).__name__ + "）") from error
    _CHILDREN[instance] = child
    deadline = time.monotonic() + max(0.1, timeout)
    while time.monotonic() < deadline:
        current = status(root)
        if current["running"]:
            health = _health(root)
            observed_instance = health.get("instance")
            if observed_instance == instance:
                return {**current, "action": "started" if current["state"] == "running" else "starting"}
            if current.get("state") == "starting" or observed_instance == previous_instance:
                time.sleep(0.1)
                continue
            # Another concurrent start won the advisory instance lock.
            if child.poll() is not None:
                child.wait()
                _CHILDREN.pop(instance, None)
            return {**current, "action": "already_running"}
        if child.poll() is not None:
            child.wait()
            _CHILDREN.pop(instance, None)
            return {**current, "state": "failed", "action": "failed"}
        time.sleep(0.1)
    current = status(root)
    if current["running"]:
        observed_instance = _health(root).get("instance")
        if observed_instance == instance:
            return {**current, "action": "started" if current["state"] == "running" else "starting"}
        if current.get("state") == "starting" or observed_instance == previous_instance:
            return {**current, "action": "starting"}
        return {**current, "action": "already_running"}
    return {**current, "state": "starting", "action": "starting"}


def _write_stop(root, metadata):
    path = Path(root) / "runtime" / "device-receiver-stop.json"
    request = {"instance": metadata.get("instance"), "pid": metadata.get("pid"),
               "process_started_at": metadata.get("process_started_at"),
               "requested_at": time.time()}
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(request, separators=(",", ":")), encoding="utf-8")
    os.replace(temporary, path)


def stop(root=ROOT, timeout=40):
    root = Path(root).resolve()
    current = status(root)
    if not current["running"]:
        return {**current, "action": "already_stopped"}
    # The lock is authoritative. Wait briefly for the child to finish writing
    # its identity metadata; never use a PID to signal or kill a process.
    deadline = time.monotonic() + min(5, max(0.1, timeout))
    while current["running"] and not current.get("pid") and time.monotonic() < deadline:
        time.sleep(0.1)
        current = status(root)
    if not current["running"]:
        return {**current, "action": "already_stopped"}
    if not current.get("pid") or not current.get("process_started_at"):
        return {**current, "state": "starting", "action": "stopping"}
    state_path = root / "runtime" / "device-receiver-status.json"
    try:
        metadata = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {**current, "state": "starting", "action": "stopping"}
    if (not isinstance(metadata, dict)
            or metadata.get("pid") != current["pid"]
            or metadata.get("process_started_at") != current["process_started_at"]
            or not metadata.get("instance")):
        return {**current, "state": "starting", "action": "stopping"}
    target_instance = metadata["instance"]
    _write_stop(root, metadata)
    deadline = time.monotonic() + max(0.1, timeout)
    while time.monotonic() < deadline:
        current = status(root)
        if not current["running"] or current.get("pid") != metadata["pid"]:
            child = _CHILDREN.pop(target_instance, None)
            if child is not None:
                try:
                    child.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
            else:
                # The lock is already released; briefly allow the process to
                # close its lock handle before callers remove a temp root.
                time.sleep(0.1)
            return {**current, "action": "stopped"}
        try:
            live_meta = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            live_meta = {}
        if live_meta.get("instance") != target_instance:
            # A later instance may have started; its identity was not targeted.
            return {**current, "action": "stopped"}
        time.sleep(0.2)
    current = status(root)
    return {**current, "action": "stopping"}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "status", "stop"))
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args(argv)
    try:
        result = {"start": start, "status": status, "stop": stop}[args.action](args.root)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except BridgeError as error:
        print(json.dumps({"state": error.state, "error": str(error)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
