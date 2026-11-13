"""Launch the official A2A client in its isolated environment."""
import json
import os
from pathlib import Path
import subprocess

from workbuddy import BridgeError
_NO_SENDER_CONTEXT = object()


def invoke(root, local, peer, mode, text="", source="", message_id=None, origin=None, wait_idle=0, message_mode=False,
           *, sender_context=_NO_SENDER_CONTEXT):
    launch_options = {}
    if sender_context is not _NO_SENDER_CONTEXT:
        from send import _SENDER_IDENTITY_KEYS, _sender_environment
        context = _sender_environment(sender_context)
        child_env = {key: value for key, value in os.environ.items()
                     if key.upper() not in _SENDER_IDENTITY_KEYS}
        child_env.update(context)
        launch_options["env"] = child_env
    root = Path(root)
    python = root / (".a2a-env/Scripts/python.exe" if __import__("os").name == "nt" else ".a2a-env/bin/python")
    if not python.is_file():
        raise BridgeError("A2A 依赖未准备，请运行 a2a.ps1 -Mode Install")
    credentials = local.get("a2a_credentials", {}).get(peer["alias"], {})
    from delivery_ticket import managed
    if peer.get("local_a2a") and not managed(local) and not message_mode:
        credentials = {"token": local["a2a"]["tokens"]["local"]}
    if managed(local) or message_mode:
        if not peer.get("local_a2a") or not origin:
            raise BridgeError("管理模式未核验该 A2A 中央授权路径或来源，未发送")
        from send import load_config
        from collaboration_client import source_token
        _, peers = load_config(root)
        credentials = {"token": source_token(local, peers, origin)}
    from device_receiver import proxy_url
    request = {
        "root": str(root), "peer": peer, "mode": mode, "text": text,
        "source": source, "message_id": message_id, "wait_idle": wait_idle,
        "token": credentials.get("token", ""),
        "timeout": peer.get("timeout", 180),
        "proxy_url": None if local.get("network", {}).get("mode") == "paired_ip" else proxy_url(local),
        "local": local, "origin": origin,
        "background": bool(origin and mode == "send"),
        "message_mode": bool(message_mode),
    }
    try:
        process = subprocess.run(
            [str(python), "-X", "utf8", "-B", str(root / "scripts/a2a_client.py")],
            input=json.dumps(request), capture_output=True, text=True, encoding="utf8",
            timeout=request["timeout"] + 120, **launch_options,
        )
    except subprocess.TimeoutExpired as error:
        raise BridgeError("A2A 请求超时，请查询对方原任务，未自动重发", "uncertain") from error
    try:
        result = json.loads(process.stdout)
    except ValueError as error:
        raise BridgeError("A2A 客户端没有返回有效回执，请先核对", "uncertain") from error
    if process.returncode:
        raise BridgeError(result.get("error", "A2A 请求失败"), result.get("state", "uncertain"))
    return result
