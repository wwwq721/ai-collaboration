"""Allowlisted native chat adapters shared by local sends and result delivery.

These wrappers preserve each application's native receipt and busy semantics.
They do not turn a receipt into a completed AI task, or implement native retry.
"""
import asyncio
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import re

from workbuddy import BridgeError


ROOT = Path(__file__).resolve().parent.parent
NATIVE_SOFTWARE = frozenset({"codex", "workbuddy", "minis"})
_MESSAGE_ID = re.compile(r"^[A-Za-z0-9._:-]{1,200}$")


@dataclass(frozen=True)
class ChatRef:
    device_id: str
    software: str
    conversation_id: str

    def __post_init__(self):
        if any(not isinstance(value, str) or not value or len(value) > 200
               for value in (self.device_id, self.software, self.conversation_id)):
            raise BridgeError("原聊天身份不完整或过长")

    @classmethod
    def from_peer(cls, peer):
        return cls(peer.get("device_id"), peer.get("software"), peer.get("conversation_id"))

    def to_dict(self):
        return asdict(self)


def _peer_values(peers):
    """Accept send.load_config's normalized map or the persisted address book."""
    if isinstance(peers, (list, tuple)):
        for peer in peers:
            if isinstance(peer, dict) and isinstance(peer.get("alias"), str):
                yield peer
        return
    if peers.get("version") == 1 and isinstance(peers.get("peers"), dict):
        peers = peers["peers"]
    for key, peer in peers.items():
        if isinstance(peer, dict):
            yield {**peer, "alias": peer.get("alias", key)}


class NativeAdapter:
    def __init__(self, local, software, peers=None, root=None):
        if software not in NATIVE_SOFTWARE:
            raise BridgeError("该软件尚无已登记的原生适配器")
        if not isinstance(local.get("device_id"), str) or not local["device_id"]:
            raise BridgeError("本机设备身份尚未配置")
        self.local, self.software = local, software
        self.peers = peers
        self.root = Path(root) if root is not None else ROOT

    def capabilities(self):
        # Configuration/install/online state needs a real status probe. The
        # values below describe adapter mechanisms, not live availability.
        busy = {"codex": "wait_or_steer", "workbuddy": "wait_or_native_queue",
                "minis": "native_follow_up_queue_or_reject"}
        return {"software": self.software, "available": None,
                "target_original_chat": True, "idle_wake": True,
                "busy_policy": busy[self.software], "status_supported": True,
                "native_idempotency": None, "software_version": None,
                "verified_at": None, "scope": "adapter_mechanism",
                "busy_state_supported": True if self.software in {"codex", "workbuddy"} else None,
                "atomic_idle_submit_supported": self.software == "codex",
                "safe_native_queue": self.software == "workbuddy",
                "busy_insert_supported": self.software == "codex"}

    def _registered_peer(self, chat_id):
        peers = self.peers
        if peers is None:
            try:
                peers = json.loads((self.root / "peers.json").read_text(encoding="utf-8-sig"))
            except (OSError, ValueError) as error:
                raise BridgeError("无法读取本机原聊天登记") from error
        matches = [peer for peer in _peer_values(peers)
                   if peer.get("software") == self.software
                   and peer.get("device_id") == self.local["device_id"]
                   and peer.get("conversation_id") == chat_id
                   and peer.get("enabled", True) is True]
        if len(matches) != 1:
            raise BridgeError("本机原聊天尚未唯一登记")
        return matches[0]

    def current_chat(self, environ=None):
        environ = os.environ if environ is None else environ
        names = {"codex": "CODEX_THREAD_ID", "workbuddy": "CODEBUDDY_SESSION_ID",
                 "minis": "MINIS_CHAT_SESSION_ID"}
        chat_id = environ.get(names[self.software])
        if not chat_id:
            return None
        if self.software == "minis":
            from minis import resolve_conversation
            chat_id = resolve_conversation(environ)
        self._registered_peer(chat_id)
        return chat_id

    def _validate(self, peer, mode, text, message_id, source, wait_idle):
        ref = ChatRef.from_peer(peer)
        if ref.software != self.software or ref.device_id != self.local["device_id"]:
            raise BridgeError("原生适配器只处理本设备已登记的软件")
        if peer.get("enabled", True) is not True:
            raise BridgeError("该原聊天尚未启用")
        if mode not in {"send", "status"}:
            raise BridgeError("原生适配器不支持该操作")
        if isinstance(wait_idle, bool) or not isinstance(wait_idle, int) or not 0 <= wait_idle <= 600:
            raise BridgeError("等待空闲秒数须在 0 到 600 之间")
        if not isinstance(source, str) or len(source) > 200:
            raise BridgeError("原生来源标记无效")
        if mode == "send":
            if not isinstance(text, str) or not text.strip():
                raise BridgeError("正文不能为空")
            if not isinstance(message_id, str) or not _MESSAGE_ID.fullmatch(message_id):
                raise BridgeError("原生投递需要有效的唯一消息号")
            from message_delivery import MAX_WIRE_BYTES, encode_payload
            bound = self.local.get("a2a", {}).get("max_text_bytes", 65536) + 4096
            if text.startswith("{"):
                try:
                    payload = json.loads(text)
                except ValueError:
                    payload = None
                if isinstance(payload, dict) and set(payload) == {"message", "source", "source_conversation"}:
                    encode_payload(payload)
                    bound = MAX_WIRE_BYTES
            if len(text.encode("utf8")) > bound:
                raise BridgeError("原生来信超过长度上限")

    def invoke(self, peer, mode, text="", message_id=None, source="", wait_idle=0, permit=None):
        """Synchronous primitive for send.py's native worker, never a resend."""
        self._validate(peer, mode, text, message_id, source, wait_idle)
        if self.software == "codex":
            # Lazy import keeps send.py -> adapter -> Codex's tested primitive
            # acyclic during module import and preserves owner/idle race logic.
            from send import codex_invoke
            if permit is None:
                return codex_invoke(self.local, peer, mode, text, message_id, source, wait_idle)
            return codex_invoke(self.local, peer, mode, text, message_id, source, wait_idle, permit=permit)
        from delivery_ticket import managed_pc, consume_local
        if mode == "send" and (managed_pc(self.local) or self.local.get("a2a", {}).get("message_delivery") is True) and self.software != "workbuddy":
            permission = consume_local(self.local, permit, peer, message_id, text)
            if permission.get("action") != "notification":
                raise BridgeError("该适配器不能保证最终提交时仍空闲，管理模式未投递新任务")
        if self.software == "minis":
            import minis
            return minis.invoke(self.local, peer, mode, text, message_id, source, wait_idle)
        import workbuddy
        if (managed_pc(self.local) or self.local.get("a2a", {}).get("message_delivery") is True) and mode == "send":
            return workbuddy.invoke(self.local, peer, mode, text, message_id, wait_idle, permit=permit)
        return workbuddy.invoke(self.local, peer, mode, text, message_id, wait_idle)

    async def _dispatch(self, peer, mode, text="", message_id=None, source="", wait_idle=0, permit=None):
        self._validate(peer, mode, text, message_id, source, wait_idle)
        if self.local.get("platform") == "minis":
            return await asyncio.to_thread(self.invoke, peer, mode, text, message_id, source, wait_idle, permit)
        # The A2A SDK interpreter may not have pywin32. Windows native work
        # therefore uses the already configured, tested helper interpreter.
        request = {"mode": "native-deliver" if mode == "send" else "status",
                   "to": peer["alias"], "text": text, "message_id": message_id,
                   "source": source, "wait_idle": wait_idle}
        if permit is not None:
            request["permit"] = permit
        python = self.local.get("python")
        if not isinstance(python, str) or not python:
            raise BridgeError("原生辅助 Python 尚未配置")
        try:
            process = await asyncio.create_subprocess_exec(
                python, "-X", "utf8", "-B", str(self.root / "scripts/send.py"), "--stdin",
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except Exception as error:
            raise BridgeError("无法启动原生辅助进程") from error
        try:
            stdout, _ = await asyncio.wait_for(
                process.communicate(json.dumps(request).encode("utf8")), wait_idle + 120,
            )
        except (TimeoutError, asyncio.CancelledError) as error:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await process.wait()
            if isinstance(error, asyncio.CancelledError):
                raise
            raise BridgeError("原生辅助进程超时，请先核对，勿自动重发", "uncertain") from error
        try:
            result = json.loads(stdout)
            if not isinstance(result, dict) or not isinstance(result.get("state"), str):
                raise ValueError("invalid receipt")
        except (TypeError, ValueError) as error:
            raise BridgeError("原生辅助回执不完整，请先核对", "uncertain") from error
        if process.returncode or mode == "send" and result["state"] != "accepted":
            state = result["state"] if result["state"] in {"not_sent", "uncertain"} else "uncertain"
            # Do not expose arbitrary helper output, which can contain text or
            # credentials. The state is enough to distinguish safe vs unknown.
            raise BridgeError("原生辅助进程未确认收件，请先核对", state)
        return result

    async def status(self, chat_id):
        return await self._dispatch(self._registered_peer(chat_id), "status")

    async def deliver(self, chat_id, text, message_id, source="", wait_idle=300, permit=None):
        peer = self._registered_peer(chat_id)
        return await self._dispatch(peer, "send", text, message_id, source, wait_idle, permit)


def get_adapter(local, software, peers=None, root=None):
    return NativeAdapter(local, software, peers, root)


async def deliver_local(root, local, peers, origin, text, message_id,
                        source="A2A结果", wait_idle=300, permit=None):
    """Deliver only to the complete, registered source identity of a job."""
    if isinstance(origin, dict):
        origin = ChatRef.from_peer(origin)
    if not isinstance(origin, ChatRef) or origin.device_id != local.get("device_id"):
        raise BridgeError("回件来源不是本设备原聊天")
    adapter = get_adapter(local, origin.software, peers, root)
    return await adapter.deliver(origin.conversation_id, text, message_id, source, wait_idle, permit)
