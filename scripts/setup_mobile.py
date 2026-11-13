"""Prepare the Minis phone client without starting its network receiver."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import uuid


ROOT = Path(__file__).resolve().parent.parent
ENV_DIR = ROOT / ".a2a-env"
LOCAL = ROOT / "local.json"
PEERS = ROOT / "peers.json"


class SetupError(Exception):
    pass


def _write_private(path: Path, value: dict, *, replace: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if replace:
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        try:
            os.chmod(temporary, 0o600)
        except OSError:
            pass
        os.replace(temporary, path)
    else:
        # Exclusive creation protects an existing file even if it appears
        # after initialize()'s initial presence check.
        with path.open("x", encoding="utf-8") as stream:
            stream.write(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def initialize(root: Path = ROOT, device_id: str | None = None, resolver=None) -> dict:
    root = Path(root).resolve()
    local_path, peers_path = root / "local.json", root / "peers.json"
    if local_path.exists() or peers_path.exists():
        raise SetupError("本机配置文件已存在；为避免覆盖或接管其他设备，未作更改。")
    if resolver is None:
        sys.path.insert(0, str(root / "scripts"))
        from minis import resolve_conversation
        resolver = resolve_conversation
    try:
        conversation_id = resolver()
    except Exception as error:
        raise SetupError("无法确认当前 Minis 原聊天身份；未创建配置文件。") from error
    if not isinstance(conversation_id, str) or not conversation_id:
        raise SetupError("Minis 未返回有效的当前聊天身份；未创建配置文件。")
    chosen_id = device_id or "minis-" + uuid.uuid4().hex[:12]
    if (not isinstance(chosen_id, str)
            or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", chosen_id)):
        raise SetupError("设备编号无效。")
    machine_name = platform.node()
    local = {
        "device_id": chosen_id,
        "platform": "minis",
        "machine_name": machine_name,
        "python": sys.executable,
        "network": {},
        "a2a": {"host": "127.0.0.1", "port": 8788},
    }
    peers = {"version": 1, "peers": {"Minis": {
        "software": "minis", "device_id": chosen_id,
        "conversation_id": conversation_id,
    }}}
    # Recheck before writing so an independently created config is never replaced.
    if local_path.exists() or peers_path.exists():
        raise SetupError("本机配置文件刚刚出现；为避免覆盖，未作更改。")
    _write_private(local_path, local)
    try:
        _write_private(peers_path, peers)
    except Exception:
        try:
            local_path.unlink()
        except OSError:
            pass
        raise
    return {"config_ready": True, "network_paired": False, "receiver_started": False}


def _venv_python(env_dir: Path) -> Path:
    return env_dir / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def install_client(root: Path = ROOT, runner=subprocess.run) -> dict:
    root = Path(root).resolve()
    env_dir = root / ".a2a-env"
    python = _venv_python(env_dir)
    if not python.is_file():
        try:
            result = runner([sys.executable, "-m", "venv", str(env_dir)],
                            cwd=str(root), capture_output=True, text=True, timeout=180)
        except Exception as error:
            raise SetupError("无法创建隔离的 Python 环境。") from error
        if result.returncode != 0 or not python.is_file():
            raise SetupError("无法创建隔离的 Python 环境。")
    requirements = root / "requirements-mobile.txt"
    if not requirements.is_file():
        raise SetupError("缺少手机端依赖清单 requirements-mobile.txt。")
    try:
        result = runner([str(python), "-m", "pip", "install", "--only-binary=:all:",
                         "--no-cache-dir", "-r", str(requirements)],
                        cwd=str(root), capture_output=True, text=True, timeout=900)
    except Exception as error:
        raise SetupError("手机端依赖安装失败；未输出安装器日志。") from error
    if result.returncode != 0:
        raise SetupError("手机端依赖安装失败；仅允许使用预编译 wheel，未输出安装器日志。")
    try:
        result = runner([str(python), "-c",
                         "import importlib.metadata as m, json; print(json.dumps(sorted((d.metadata['Name'], d.version) for d in m.distributions() if d.metadata.get('Name'))))"],
                        cwd=str(root), capture_output=True, text=True, timeout=60)
        packages = json.loads(result.stdout) if result.returncode == 0 else None
        if not isinstance(packages, list):
            raise ValueError("metadata")
    except Exception as error:
        raise SetupError("依赖已安装，但无法读取安装版本清单。") from error
    _write_private(root / "runtime" / "mobile-client-installed.json",
                   {"packages": packages}, replace=True)
    return {"client_installed": True, "packages": packages}


def _sdk_status(root: Path) -> bool:
    python = _venv_python(root / ".a2a-env")
    if not python.is_file():
        return False
    try:
        result = subprocess.run([str(python), "-c", "import a2a, httpx"],
                                cwd=str(root), capture_output=True, timeout=30)
        return result.returncode == 0
    except Exception:
        return False


def check(root: Path = ROOT) -> dict:
    root = Path(root).resolve()
    config_ready = False
    network_paired = False
    try:
        local = json.loads((root / "local.json").read_text(encoding="utf-8"))
        book = json.loads((root / "peers.json").read_text(encoding="utf-8"))
        config_ready = (isinstance(local, dict) and local.get("platform") == "minis"
                        and isinstance(local.get("device_id"), str) and bool(local["device_id"])
                        and isinstance(book, dict) and book.get("version") == 1
                        and isinstance(book.get("peers"), dict) and bool(book["peers"]))
        network = local.get("network", {}) if isinstance(local, dict) else {}
        network_paired = (isinstance(network, dict)
                          and isinstance(network.get("base_url"), str)
                          and network["base_url"].startswith("https://")
                          and isinstance(network.get("token"), str)
                          and len(network["token"]) >= 32)
    except (OSError, ValueError, TypeError):
        pass
    receiver_running = False
    if config_ready:
        try:
            sys.path.insert(0, str(root / "scripts"))
            from receiver_manager import status
            receiver_running = bool(status(root).get("running"))
        except Exception:
            receiver_running = False
    return {
        "config_ready": bool(config_ready),
        "network_paired": bool(network_paired),
        "client_sdk_ready": _sdk_status(root),
        "minis_sessions_cli_available": bool(shutil.which("minis-sessions-cli")),
        "minis_scheduled_cli_available": bool(shutil.which("minis-scheduled")),
        "receiver_running": receiver_running,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("init", "check", "install-client"))
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--device-id", help="显式指定本设备编号")
    args = parser.parse_args(argv)
    try:
        if args.command == "init":
            result = initialize(args.root, args.device_id)
        elif args.command == "install-client":
            result = install_client(args.root)
        else:
            result = check(args.root)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except SetupError as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
