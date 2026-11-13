"""One small send action. AI supplies a recipient and message, not API details."""
import argparse
from collections.abc import Mapping
import json
import os
from pathlib import Path
import platform
import sys
import time
import uuid

from workbuddy import BridgeError

ROOT = Path(__file__).resolve().parent.parent
SUPPORTED = {"codex", "workbuddy", "minis", "a2a"}
_NO_SENDER_CONTEXT = object()
_SENDER_IDENTITY_KEYS = frozenset({"CODEBUDDY_SESSION_ID", "CODEX_THREAD_ID", "MINIS_CHAT_SESSION_ID"})


def message_enabled(local):
    return (local.get("a2a", {}).get("message_delivery") is True
            or local.get("network", {}).get("message_delivery") is True)


def load_config(root=ROOT):
    try:
        local = json.loads((root / "local.json").read_text(encoding="utf-8-sig"))
        book = json.loads((root / "peers.json").read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as error:
        raise BridgeError("请先按迁移说明填写 local.json 和 peers.json") from error
    if book.get("version") != 1:
        raise BridgeError("伙伴地址簿版本不受支持")
    if local.get("machine_name", "").casefold() != platform.node().casefold():
        raise BridgeError("本机配置属于另一台电脑，请重新填写 local.json")
    if not local.get("device_id"):
        raise BridgeError("本机设备名称尚未配置")
    peers = book.get("peers")
    if not isinstance(peers, dict) or not peers:
        raise BridgeError("伙伴地址簿为空")
    normalized = {}
    for alias, peer in peers.items():
        if not isinstance(alias, str) or not alias.strip() or not isinstance(peer, dict):
            raise BridgeError("伙伴地址簿格式不正确")
        key = alias.casefold()
        if key in normalized:
            raise BridgeError("伙伴名称重复")
        if alias.upper().startswith("A2A-"):
            raise BridgeError("A2A- 前缀保留给临时回信地址")
        fields = ("software", "device_id")
        fields += ("url",) if peer.get("software") == "a2a" or peer.get("transport") == "a2a" else ("conversation_id",)
        if not all(isinstance(peer.get(k), str) and peer[k] for k in fields):
            raise BridgeError("伙伴缺少软件、设备或聊天地址")
        normalized[key] = {**peer, "alias": alias}
    return local, normalized


def peer_at(peers, alias):
    peer = peers.get(alias.casefold())
    if peer is None:
        raise BridgeError("未知伙伴，请先列出伙伴或登记地址")
    if peer.get("enabled", True) is not True:
        raise BridgeError("该伙伴尚未启用")
    if peer.get("transport") not in (None, "native", "a2a"):
        raise BridgeError("该协议连接器尚未接入")
    if peer["software"] not in SUPPORTED and peer.get("transport") != "a2a":
        raise BridgeError("该软件还没有经过验证的收件方式")
    return peer


def sender_at(local, peers, environ=os.environ, explicit=None, strict=False):
    if explicit:
        from delivery_ticket import managed
        if managed(local) or message_enabled(local) or strict:
            raise BridgeError("管理模式禁止用 --as 改变发件原聊天身份")
        peer = peer_at(peers, explicit)
        if peer["software"] not in {"codex", "workbuddy", "minis"}:
            raise BridgeError("发件身份须是已登记的本端原聊天")
        if peer["device_id"] != local["device_id"]:
            raise BridgeError("发件身份属于另一台设备")
        return peer
    minis_id = environ.get("MINIS_CHAT_SESSION_ID")
    if minis_id:
        from minis import resolve_conversation
        minis_id = resolve_conversation(environ)
    contexts = (
        ("minis", minis_id),
        ("workbuddy", environ.get("CODEBUDDY_SESSION_ID")),
        ("codex", environ.get("CODEX_THREAD_ID")),
    )
    from delivery_ticket import managed
    if (managed(local) or message_enabled(local) or strict) and len({(software, chat) for software, chat in contexts if chat}) > 1:
        raise BridgeError("当前存在多个宿主原聊天来源，管理模式拒绝猜测身份")
    for software, conversation in contexts:
        if not conversation:
            continue
        matches = [
            peer for peer in peers.values()
            if peer["software"] == software and peer.get("conversation_id") == conversation
            and peer["device_id"] == local["device_id"] and peer.get("enabled", True) is True
        ]
        if len(matches) != 1:
            raise BridgeError("当前聊天尚未唯一登记，请先登记本端地址")
        return matches[0]
    raise BridgeError("未识别到当前 AI 聊天；人工检查时可用 --as 指定已登记的本端")


def codex_invoke(local, peer, mode, text, message_id, source, wait_idle, permit=None):
    import codex_ipc
    if peer["device_id"] != local["device_id"]:
        raise BridgeError("当前 Codex 收件入口只验证过本机，未向远端发送")
    client = codex_ipc.CodexIpc()
    sending = False
    try:
        client.connect()
        owner = client.discover_owner(peer["conversation_id"])
        if not owner:
            raise BridgeError("Codex 桌面未持有目标原聊天，请先打开目标聊天")
        deadline = time.monotonic() + wait_idle
        while True:
            snapshot = client.follow(peer["conversation_id"])
            if not snapshot:
                raise BridgeError("未取得 Codex 原聊天的当前状态")
            state = snapshot["conversationState"].get("threadRuntimeStatus", {}).get("type")
            if state not in {"active", "idle"}:
                raise BridgeError("Codex 原聊天尚未就绪")
            if mode == "status":
                return {"state": state}
            if wait_idle and state != "idle":
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BridgeError("等待 Codex 空闲超时，未发信")
                time.sleep(min(2, remaining))
                continue
            from delivery_ticket import managed_pc, consume_local
            guarded = managed_pc(local) or local.get("a2a", {}).get("message_delivery") is True
            permission = consume_local(local, permit, peer, message_id, text) if guarded else None
            if guarded and state != "idle" and not permission["allow_busy"]:
                raise BridgeError("目标忙，后台未许可忙时插入，未发信")
            sending = True
            try:
                result = client.start_turn(
                    peer["conversation_id"], text, owner, source=source,
                    delivery_id=message_id, require_idle=bool(wait_idle) or guarded and not permission["allow_busy"],
                )
            except codex_ipc.NotSentError as error:
                sending = False
                remaining = deadline - time.monotonic()
                if not guarded and wait_idle and error.retryable and remaining > 0:
                    # Only refresh reads before any native send, inside the
                    # original deadline. A sent RPC is never retried.
                    time.sleep(min(2, remaining))
                    continue
                raise BridgeError(str(error), "not_sent") from error
            return {"state": "accepted", "route": result["route"], "turn_id": result["turnId"]}
    except BridgeError:
        raise
    except Exception as error:
        raise BridgeError(
            "Codex 接口未确认投递，请核对原聊天和软件版本",
            "uncertain" if sending else "not_sent",
        ) from error
    finally:
        try:
            client.close()
        except Exception:
            pass


def invoke(local, peer, mode, text="", message_id=None, source="", wait_idle=0, permit=None):
    from host_adapters import get_adapter
    return get_adapter(local, peer["software"]).invoke(
        peer, mode, text, message_id, source, wait_idle, permit=permit,
    )


def check(local):
    if local.get("platform") == "minis":
        import shutil
        missing = [name for name in ("minis-scheduled", "minis-sessions-cli") if not shutil.which(name)]
        if missing:
            raise BridgeError("Minis 原生接口不可用：" + "、".join(missing))
        return {"state": "ready", "device": local["device_id"]}
    problems = []
    for name in ("python", "node"):
        if not Path(local.get(name, "")).is_file():
            problems.append(name + " 路径不可用")
    install = Path(local.get("workbuddy_install", ""))
    if not (install / "resources/app.asar.unpacked/resources/extensions/edge-sync/server/index.cjs").is_file():
        problems.append("WorkBuddy 安装路径不可用")
    if not Path(local.get("workbuddy_config", "")).is_dir():
        problems.append("WorkBuddy 用户目录不可用")
    try:
        import win32file
    except ImportError:
        problems.append("当前 Python 缺少 pywin32")
    if problems:
        raise BridgeError("；".join(problems))
    return {"state": "ready", "device": local["device_id"]}


def _sender_environment(sender_context):
    """Copy only host-provided identity fields, never a request's JSON fields."""
    if not isinstance(sender_context, Mapping):
        raise BridgeError("宿主当前聊天上下文格式不正确")
    context = dict(sender_context)
    if not context or any(
            not isinstance(key, str) or key not in _SENDER_IDENTITY_KEYS
            or not isinstance(value, str) or not value.strip() or len(value) > 512
            or any(ord(char) < 32 or ord(char) == 127 for char in value)
            for key, value in context.items()):
        raise BridgeError("宿主当前聊天上下文格式不正确")
    return context


def run(request, root=ROOT, *, sender_context=_NO_SENDER_CONTEXT):
    """Optional in-process host context is isolated from CLI JSON and process environment."""
    strict_context = sender_context is not _NO_SENDER_CONTEXT
    context_environ = _sender_environment(sender_context) if strict_context else None
    local, peers = load_config(root)
    request["_message_mode"] = message_enabled(local)
    if strict_context and not message_enabled(local):
        raise BridgeError("宿主消息入口需要已启用消息模式，未转为旧版直发")
    mode = request.get("mode", "send")
    from delivery_ticket import managed, managed_pc
    if (managed(local) or message_enabled(local) or strict_context) and request.get("as"):
        raise BridgeError("管理模式禁止用 --as 改变发件原聊天身份")
    bound_sender = None

    def resolve_sender():
        if strict_context:
            # A disconnected registration cannot authenticate a host invocation.
            registered = {name: peer for name, peer in peers.items()
                          if peer.get("enabled", True) is True}
            return bound_sender or sender_at(local, registered, environ=context_environ, strict=True)
        return sender_at(local, peers, explicit=request.get("as"))

    if strict_context:
        bound_sender = resolve_sender()
        if bound_sender.get("verified") is not True:
            raise BridgeError("宿主当前原聊天尚未核验")
    if mode == "check":
        return check(local)
    if mode == "list":
        return {"peers": [
            {"name": peer["alias"], "software": peer["software"], "device": peer["device_id"]}
            for peer in peers.values() if peer.get("enabled", True) is True
        ]}
    alias = request.get("to")
    if not isinstance(alias, str) or not alias:
        raise BridgeError("请提供收件人")
    if strict_context and alias.upper().startswith("A2A-"):
        raise BridgeError("宿主消息入口不接受旧版临时回信地址")
    if alias.upper().startswith("A2A-") and mode == "send":
        from a2a_state import Mailbox
        sender = resolve_sender()
        text = request.get("text")
        if not isinstance(text, str) or not text.strip():
            raise BridgeError("正文不能为空")
        if local.get("network"):
            from device_receiver import remote_reply
            return remote_reply(root, local, alias, text, sender, peers=peers)
        if managed(local):
            from collaboration_client import local_request, source_token
            return local_request(local, "/collaboration/reply", {"address": alias, "text": text},
                                 source_token(local, peers, sender))
        Mailbox(root).submit(alias, text, sender)
        return {"state": "accepted", "to": alias}
    target = peer_at(peers, alias)
    if mode == "native-deliver":
        if local.get("platform") == "minis" and (managed(local) or message_enabled(local)):
            raise BridgeError("手机原生投递只允许已授权收件器执行")
        if managed(local) and not managed_pc(local):
            raise BridgeError("手机原生收件须由已配对收件器领取并取得交付许可")
        if target["software"] == "a2a" or target.get("transport") == "a2a":
            raise BridgeError("原生收件入口不接受 A2A 转发目标")
        if message_enabled(local) and not isinstance(request.get("permit"), dict):
            raise BridgeError("原生投递需要后台许可")
        return invoke(local, target, "send", request["text"], request["message_id"],
                      source=request.get("source", "A2A"), wait_idle=request.get("wait_idle", 0),
                      permit=request.get("permit"))
    if managed(local) or message_enabled(local):
        from collaboration_client import central_peer
        target = central_peer(local, target)
    if target.get("transport") == "a2a" and target["software"] != "a2a":
        target = {**target, "host_software": target["software"], "software": "a2a"}
    if target["software"] != "a2a" and target["device_id"] != local["device_id"]:
        # Public sending stays two fields. This computer's SDK endpoint bridges
        # the registered remote chat to its device's outbound connection.
        config = local.get("a2a", {})
        if target["device_id"] not in config.get("device_tokens", {}):
            raise BridgeError("目标设备尚未完成网络配对")
        from urllib.parse import quote
        target = {**target, "host_software": target["software"], "software": "a2a", "local_a2a": True,
                  "url": "http://127.0.0.1:" + str(config.get("port", 8788))
                         + "/agents/" + quote(target["alias"], safe="")}
    if mode == "status":
        if target["software"] == "a2a":
            import a2a_transport
            sender = resolve_sender()
            return a2a_transport.invoke(root, local, target, "status", source=sender["conversation_id"], origin=sender,
                                        **({"message_mode": True} if message_enabled(local) else {}),
                                        **({"sender_context": context_environ} if strict_context else {}))
        result = invoke(local, target, "status")
        return {"to": target["alias"], **result}
    if mode != "send":
        raise BridgeError("不支持的操作")
    text = request.get("text")
    if not isinstance(text, str) or not text.strip():
        raise BridgeError("正文不能为空")
    wait_idle = request.get("wait_idle", 0)
    if isinstance(wait_idle, bool) or not isinstance(wait_idle, int) or not 0 <= wait_idle <= 600:
        raise BridgeError("等待空闲秒数须在 0 到 600 之间")
    sender = resolve_sender()
    uid = str(uuid.uuid4())
    if target["software"] == "a2a":
        import a2a_transport
        try:
            return a2a_transport.invoke(root, local, target, "send", text,
                                        source=sender["conversation_id"], message_id=uid, origin=sender,
                                        **({"wait_idle": wait_idle} if wait_idle and (managed(local) or message_enabled(local)) else {}),
                                        **({"message_mode": True} if message_enabled(local) else {}),
                                        **({"sender_context": context_environ} if strict_context else {}))
        except BridgeError as error:
            if message_enabled(local):
                from message_delivery import failure_text
                raise BridgeError(failure_text(alias, text, error.state), error.state) from error
            raise
    message = "【AI 来信，来自 " + sender["alias"] + "，回信给 " + sender["alias"] + "】\n" + text
    try:
        result = invoke(
            local, target, "send", message, uid,
            source=sender["alias"], wait_idle=wait_idle,
        )
    except BridgeError as error:
        if error.state == "uncertain":
            error.args = (str(error) + "；本次消息号 " + uid + "，请先核对，勿盲目重发",)
        raise
    if result.get("state") != "accepted":
        raise BridgeError("目标软件没有给出接纳回执，请先核对", "uncertain")
    reply = {"state": result["state"], "to": target["alias"]}
    if request.get("debug"):
        reply.update({"message_id": uid, "from": sender["alias"], **result})
    return reply


def main():
    output_encoding = (getattr(sys.stdout, "encoding", None) or "").lower().replace("_", "-")
    ascii_only = output_encoding not in {"utf-8", "utf8", "cp65001"}
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("to", nargs="?")
    parser.add_argument("text", nargs="?")
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--file", type=Path)
    parser.add_argument("--as", dest="sender")
    parser.add_argument("--wait-idle", type=int, default=0)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    request = {}
    try:
        if args.stdin:
            request = json.load(sys.stdin)
            if not isinstance(request, dict):
                raise BridgeError("请求应为对象")
        else:
            text = args.file.read_text(encoding="utf-8-sig") if args.file else args.text
            mode = "list" if args.list else "check" if args.check else "status" if args.status else "send"
            request = {"mode": mode, "to": args.to, "text": text, "as": args.sender,
                       "wait_idle": args.wait_idle, "debug": args.debug}
        print(json.dumps(run(request), ensure_ascii=ascii_only), flush=True)
        return 0
    except BridgeError as error:
        text = str(error)
        if isinstance(request, dict) and request.get("_message_mode") and request.get("mode", "send") == "send" and not request.get("debug"):
            from message_delivery import failure_text
            text = failure_text(request.get("to", ""), request.get("text", ""), error.state)
        print(json.dumps({"state": error.state, "error": text}, ensure_ascii=ascii_only), flush=True)
        return 2
    except Exception as error:
        # Do not echo SDK logs, temporary credentials, or full native errors.
        text = "配置或连接检查失败（" + type(error).__name__ + "）"
        if isinstance(request, dict) and request.get("_message_mode") and request.get("mode", "send") == "send" and not request.get("debug"):
            from message_delivery import failure_text
            text = failure_text(request.get("to", ""), request.get("text", ""))
        print(json.dumps({"state": "not_sent", "error": text},
                         ensure_ascii=ascii_only), flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
