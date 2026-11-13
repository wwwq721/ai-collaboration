"""Use the installed authenticated broker, without copying its credentials."""
import base64
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid


class BridgeError(RuntimeError):
    def __init__(self, message, state="not_sent"):
        super().__init__(message)
        self.state = state


def parse_native_output(stdout):
    events = []
    for line in stdout.splitlines():
        if line.startswith('{"event":'):
            try:
                events.append(json.loads(line))
            except ValueError as error:
                raise BridgeError("原生回执格式不完整，请先核对原聊天", "uncertain") from error
    if len(events) != 1:
        raise BridgeError("原生接口未返回唯一回执，请先核对原聊天", "uncertain")
    event = events[0]
    if event["event"] == "error":
        raise BridgeError(event.get("error", "原生接口失败"), event.get("state", "uncertain"))
    if event["event"] != "result" or not isinstance(event.get("result"), dict):
        raise BridgeError("原生回执格式已变化，请先核对原聊天", "uncertain")
    return event["result"]


def invoke(local, peer, mode, text="", message_id=None, wait_idle=0, permit=None):
    from delivery_ticket import managed_pc
    guarded = (managed_pc(local) or local.get("a2a", {}).get("message_delivery") is True) and mode == "send"
    if guarded and (not isinstance(permit, dict) or not isinstance(permit.get("ticket"), str)):
        raise BridgeError("原生投递需要后台签发的一次性许可")
    install = Path(local["workbuddy_install"])
    sdk_file = install / "resources/app.asar.unpacked/resources/extensions/edge-sync/server/index.cjs"
    library = install / "resources/app.asar.unpacked/resources/plugins/workbuddy-builtin/skills/library"
    if not sdk_file.is_file() or not (library / "workbuddy_ipc/__init__.py").is_file():
        raise BridgeError("WorkBuddy 安装路径或内置接口不匹配，请重新检查本机配置")
    os.environ["WORKBUDDY_CONFIG_DIR"] = local["workbuddy_config"]
    sys.path.insert(0, str(library))
    ipc = importlib.import_module("workbuddy_ipc")
    target = peer["native_device_id"]
    user_id = peer["account_id"]
    client_id = str(uuid.uuid4())
    with ipc.connect(client_id="ai-collaboration", client_kind="cli") as client:
        pipe = client.get_pipe("wb.request")

        def fetch(method, path, body=None):
            request = {"method": method, "path": path}
            if body is not None:
                request["body_b64"] = base64.b64encode(json.dumps(body).encode()).decode()
            response = pipe.invoke("http.fetch", request, timeout=30)
            result = json.loads(base64.b64decode(response["body_b64"]))
            if response["status"] != 200 or result.get("code") != 0:
                raise BridgeError("WorkBuddy 授权接口拒绝请求，请检查登录和当前账号")
            return result["data"]

        accounts = fetch("GET", "/console/accounts")["accounts"]
        if not any(account.get("uid") == user_id for account in accounts):
            raise BridgeError("目标账号与当前登录不匹配，未发信")
        devices = fetch("GET", "/console/as/conversation-sync/devices/online")["devices"]
        matches = [device for device in devices if device.get("deviceId") == target]
        if len(matches) != 1 or not matches[0].get("onlineFlag") or matches[0].get("remoteViewFlag") != 1:
            raise BridgeError("目标 WorkBuddy 设备离线或既有远程通道不可用")
        credentials = fetch("POST", "/edge-cloud/credentials", {
            "userId": user_id,
            "deviceId": client_id,
            "clientType": "desktop",
            "deviceName": "AI collaboration",
            "useDefaultScopes": True,
            "scopes": ["executor:" + target],
            "chSdkVersion": matches[0].get("chSdkVersion") or "2.3.1",
            "appVersion": matches[0].get("appVersion") or "5.9.3",
        })
    request = {
        "sdkFile": str(sdk_file),
        "credentials": credentials,
        "clientId": client_id,
        "userId": user_id,
        "deviceId": target,
        "conversationId": peer["conversation_id"],
        "mode": mode,
        "text": text,
        "messageId": message_id,
        "confirmDelivery": local.get("a2a", {}).get("message_delivery") is True,
        "waitIdle": wait_idle,
    }
    if guarded:
        from host_adapters import ChatRef
        port = local.get("a2a", {}).get("port", 8788)
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise BridgeError("本机协作服务端口无效")
        request.update(managed=True, permit=permit, target=ChatRef.from_peer(peer).to_dict(), consumePort=port)
    try:
        process = subprocess.run(
            [local["node"], str(Path(__file__).with_name("workbuddy_sync.cjs"))],
            input=json.dumps(request), capture_output=True, text=True,
            encoding="utf-8", timeout=max(90, wait_idle + 100),
        )
    except subprocess.TimeoutExpired as error:
        raise BridgeError("原生接口超时，先核对原聊天，未自动重发", "uncertain") from error
    result = parse_native_output(process.stdout)
    if process.returncode:
        raise BridgeError("原生接口异常退出，请先核对原聊天", "uncertain")
    return result
