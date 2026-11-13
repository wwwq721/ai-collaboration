"""Official A2A v1 text endpoints for the registered desktop chats."""
import argparse
import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
import hmac
import json
import logging
import os
from pathlib import Path
import sys
import time
from urllib.parse import quote, urlsplit
import uuid

from a2a.auth.user import User
from a2a.helpers import get_message_text, new_task_from_user_message, new_text_message, new_text_part
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.context import ServerCallContext
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import ServerCallContextBuilder, create_agent_card_routes, create_jsonrpc_routes
from a2a.server.tasks import TaskUpdater
from a2a.types import (
    AgentCard, AgentCapabilities, AgentInterface, AgentSkill, Artifact, GetTaskRequest,
    HTTPAuthSecurityScheme, SecurityRequirement, SecurityScheme, TaskState,
)
from a2a.utils.errors import InvalidParamsError, TaskNotCancelableError
from sqlalchemy import event
from sqlalchemy.ext.asyncio import create_async_engine
from starlette.applications import Starlette
from starlette.responses import FileResponse, JSONResponse
from starlette.routing import Mount, Route

from a2a_state import Mailbox
from device_queue import DeviceQueue
from a2a_runtime import TemporaryTaskStore, cleanup_once, cleanup_loop, configure_logging
from send import load_config
from workbuddy import BridgeError
from message_delivery import MODE, SKILL_ID, business_payload, encode_payload, request_hash, native_receipt

log = logging.getLogger("ai_collaboration")
request_caller = ContextVar("request_caller", default="local")


class Caller(User):
    def __init__(self, name):
        self.name = name

    @property
    def is_authenticated(self):
        return True

    @property
    def user_name(self):
        return self.name


class CallContext(ServerCallContextBuilder):
    def build(self, request):
        return ServerCallContext(
            user=Caller(request.scope["a2a_caller"]), state={
                "headers": {k: v for k, v in request.headers.items() if k not in {"authorization", "cookie"}},
                "origin_peer": request.scope.get("collaboration_source"),
                "no_bridge_legacy_write_allowed": request.scope.get("no_bridge_legacy_write_allowed") is True,
            },
        )


class BearerAccess:
    def __init__(self, app, tokens, limits, *, bridge=None, legacy_local_only=False):
        self.app, self.tokens, self.limits = app, tokens, limits
        self.bridge = bridge
        self.legacy_local_only = legacy_local_only

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        headers = dict(scope["headers"])
        path = scope["path"]
        if self.bridge and path == "/collaboration/consume":
            host = headers.get(b"host", b"").split(b":")[0]
            if (scope.get("client") and scope["client"][0] in {"127.0.0.1", "::1"} and
                    host == b"127.0.0.1" and not any(k in headers for k in (b"forwarded", b"x-forwarded-for", b"x-forwarded-host"))):
                return await self.forward(scope, receive, send)
            return await JSONResponse({"error": "loopback_required"}, 403)(scope, receive, send)
        presented = headers.get(b"authorization", b"").decode("latin1")
        for name, token in self.tokens.items():
            if hmac.compare_digest(presented, "Bearer " + token):
                # Legacy local task callers have no original-chat identity. Keep
                # that compatibility narrow; neither metadata nor proxy headers
                # can turn a remote/device caller into a bound conversation.
                try:
                    host = urlsplit("http://" + headers.get(b"host", b"").decode("latin1")).hostname
                except ValueError:
                    host = None
                direct_loopback = (scope.get("client") and scope["client"][0] in {"127.0.0.1", "::1"}
                                   and host in {"127.0.0.1", "::1"}
                                   and not any(k == b"forwarded" or k.startswith(b"x-forwarded-") for k in headers))
                scope["no_bridge_legacy_write_allowed"] = bool(
                    self.bridge is None and self.legacy_local_only
                    and not name.startswith(("device:", "chat:", "origin:")) and direct_loopback)
                scope["bridge_principal"] = name
                caller, card_caller = name, name
                if self.bridge and (path.startswith("/agents/") or path == "/collaboration/reply" or path.startswith("/collaboration/receipts/")):
                    from managed_bridge import owner_of
                    peer = self.bridge.source(name)
                    if peer is None:
                        return await JSONResponse({"error": "unbound_chat"}, 403)(scope, receive, send)
                    scope["collaboration_source"] = peer
                    caller = owner_of(peer)
                    if peer["device_id"] == self.bridge.local["device_id"]:
                        card_caller = "local"
                scope["a2a_caller"] = caller
                marker = request_caller.set(card_caller)
                try:
                    return await self.forward(scope, receive, send)
                finally:
                    request_caller.reset(marker)
        if scope["path"] == "/health" and scope.get("method") in {"GET", "HEAD"}:
            return await self.app(scope, receive, send)
        response = JSONResponse({"error": "unauthorized"}, 401)
        await response(scope, receive, send)

    async def forward(self, scope, receive, send):
        if scope.get("method") != "POST":
            return await self.app(scope, receive, send)
        chunks, size = [], 0
        while True:
            event = await receive()
            if event["type"] == "http.disconnect":
                return
            chunk = event.get("body", b"")
            size += len(chunk)
            if size > self.limits.max_text_bytes*8 + 16384:
                return await JSONResponse({"error": "request_too_large"}, 413)(scope, receive, send)
            chunks.append(chunk)
            if not event.get("more_body", False):
                break
        body, delivered = b"".join(chunks), False

        async def replay():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        await self.app(scope, replay, send)


async def native_deliver(root, local, alias, text, message_id, wait_idle, *, permit=None):
    from host_adapters import deliver_local
    _, peers = load_config(root)
    peer = peers[alias.casefold()]
    return await deliver_local(root, local, peers, peer, text, message_id, source="A2A", wait_idle=wait_idle, permit=permit)


class DesktopExecutor(AgentExecutor):
    def __init__(self, root, local, alias, peer, mailbox, instance, deliver, bridge=None):
        self.root, self.local, self.alias, self.peer = root, local, alias, peer
        self.mailbox, self.instance, self.deliver = mailbox, instance, deliver
        self.bridge = bridge
        self.message_deliver = None

    async def execute_message(self, context, event_queue, text):
        from access_policy import chat_ref
        from managed_bridge import owner_of
        if not self.local["a2a"].get("message_delivery") or not self.bridge:
            raise InvalidParamsError("请先启用已核验的消息收件能力")
        source = context.call_context.state.get("origin_peer")
        from google.protobuf.json_format import MessageToDict
        metadata = MessageToDict(context.message.metadata)
        claimed = metadata.get("collaboration_origin")
        if context.current_task:
            raise InvalidParamsError("普通消息须使用独立投递")
        if claimed is not None:
            try:
                mismatch = source is None or chat_ref(claimed) != chat_ref(source)
            except (TypeError, ValueError):
                mismatch = True
            if mismatch:
                raise InvalidParamsError("消息声明与认证来源不符")
        if source is None or not self.bridge.policy.check(source, self.peer)["contact"]:
            raise InvalidParamsError("消息来源或接收聊天未完成接入")
        requested_wait = metadata.get("collaboration_wait_idle")
        if requested_wait is not None and (type(requested_wait) not in (int, float) or
                not 1 <= requested_wait <= 600 or int(requested_wait) != requested_wait):
            raise InvalidParamsError("等待空闲秒数无效")
        task = new_task_from_user_message(context.message)
        try:
            record, fresh = self.mailbox.claim_message(
                task.id, self.alias, self.peer["conversation_id"], owner_of(source),
                context.message.message_id, self.instance,
                source_ref=chat_ref(source), target_ref=chat_ref(self.peer),
                request_hash=request_hash(self.peer, text),
            )
        except BridgeError as error:
            raise InvalidParamsError(str(error)) from error
        if not fresh:
            receipt = self.mailbox.receipt_for(owner_of(source), context.message.message_id)
            response = new_text_message("")
            response.metadata.update({"collaboration_mode": MODE, "collaboration_receipt": receipt})
            await event_queue.enqueue_event(response)
            return
        await event_queue.enqueue_event(task)
        updater = TaskUpdater(event_queue, task_id=task.id, context_id=task.context_id)
        await updater.update_status(TaskState.TASK_STATE_WORKING, metadata={"collaboration_mode": MODE})
        decision = self.bridge.policy.check(source, self.peer)
        wait_idle = int(requested_wait) if requested_wait is not None else (0 if decision["busy"] else self.local["a2a"].get("idle_wait_seconds", 300))
        try:
            if self.peer.get("transport") == "a2a" or self.peer["software"] == "a2a":
                await self.exchange_message_external(task, updater, source, record, text)
                return
            result = await self.message_deliver(source, self.peer, text, task.id, wait_idle)
            receipt = native_receipt(self.peer, result)
        except asyncio.CancelledError:
            self.mailbox.record_delivery(task.id, {"state": "uncertain", "stage": "relay"})
            raise
        except BridgeError as error:
            receipt = {"state": error.state if error.state in {"uncertain", "not_sent"} else "uncertain", "stage": "relay"}
        except Exception:
            receipt = {"state": "uncertain", "stage": "relay"}
        saved = self.mailbox.record_delivery(task.id, receipt)
        state = saved["state"]
        sdk_state = (TaskState.TASK_STATE_COMPLETED if state in {"accepted", "queued"}
                     else TaskState.TASK_STATE_REJECTED if state == "not_sent" else TaskState.TASK_STATE_FAILED)
        await updater.update_status(sdk_state, metadata={"collaboration_mode": MODE, "collaboration_receipt": saved})
        if self.peer["device_id"] != self.local["device_id"]:
            DeviceQueue(self.mailbox).close(task.id)

    async def exchange_message_external(self, task, updater, source, record, text):
        from managed_egress import deliver
        from managed_bridge import owner_of
        from access_policy import chat_ref

        def admitted(receipt):
            self.mailbox.record_delivery(task.id, receipt)

        result = await deliver(self.root, self.local, self.peer, source, text, task.id,
                               lambda: self.bridge.approve_external(task.id),
                               task_timeout=self.local["a2a"].get("task_timeout", 900),
                               independent=True, on_admission=admitted)
        if result.get("state") == "completed" and not self.mailbox.receipt_for(record["owner"], record["message_id"]).get("state") in {"accepted", "queued"}:
            admitted({"state": "queued", "stage": "external_service"})
        body = result.get("reply")
        if isinstance(body, str) and body:
            self.mailbox.store_external_result(task.id, body)
            await updater.add_artifact(parts=[new_text_part(body)])
        receipt = self.mailbox.receipt_for(record["owner"], record["message_id"])
        if receipt["state"] == "waiting":
            failure = "not_sent" if result.get("state") in {"not_sent", "rejected"} else "uncertain"
            receipt = self.mailbox.record_delivery(task.id, {"state": failure, "stage": "relay"})
        await updater.update_status(
            TaskState.TASK_STATE_COMPLETED if receipt["state"] in {"accepted", "queued"} else TaskState.TASK_STATE_FAILED,
            metadata={"collaboration_mode": MODE, "collaboration_receipt": receipt})

    async def exchange_external(self, task, updater, source, record, text):
        from managed_egress import deliver
        states = {
            "completed": TaskState.TASK_STATE_COMPLETED,
            "input_required": TaskState.TASK_STATE_INPUT_REQUIRED,
            "auth_required": TaskState.TASK_STATE_AUTH_REQUIRED,
            "rejected": TaskState.TASK_STATE_REJECTED,
            "canceled": TaskState.TASK_STATE_CANCELED,
            "not_sent": TaskState.TASK_STATE_REJECTED,
        }
        result = await deliver(self.root, self.local, self.peer, source, text, task.id,
                               lambda: self.bridge.approve_external(task.id),
                               task_timeout=self.local["a2a"].get("task_timeout", 900))
        state = result.get("state", "uncertain")
        sdk_state = states.get(state, TaskState.TASK_STATE_FAILED)
        body = result.get("reply") or result.get("error") or "对方尚未确认任务结果"
        if not isinstance(body, str) or len(body.encode("utf8")) > self.mailbox.limits.max_text_bytes:
            raise BridgeError("标准伙伴的结果超过本地正文上限")
        if state == "completed":
            await updater.add_artifact(parts=[new_text_part(body)])
        # Remember the foreign outcome without persisting a second copy of the text.
        self.mailbox.finish(task.id, "done" if state=="completed" else state if state in {"input_required","auth_required"} else "failed")
        await updater.update_status(sdk_state, message=new_text_message(body),
                                    metadata={"collaboration_delivery_state": state})

    async def execute(self, context: RequestContext, event_queue):
        if self.bridge is None and context.call_context.state.get("no_bridge_legacy_write_allowed") is not True:
            raise InvalidParamsError("来源未绑定原聊天；旧任务入口只接受本机直接调用")
        text = get_message_text(context.message)
        if not text.strip() or any(not part.HasField("text") for part in context.message.parts):
            raise InvalidParamsError("当前入口仅接受非空文本消息")
        if len(text.encode("utf8")) > self.mailbox.limits.max_text_bytes:
            raise InvalidParamsError("正文超过64KiB上限，请缩短或发送文件位置")
        if any(len(value) > 128 for value in [context.message.message_id, context.message.context_id, context.message.task_id]):
            raise InvalidParamsError("消息或任务编号过长")
        from google.protobuf.json_format import MessageToDict
        if MessageToDict(context.message.metadata).get("collaboration_mode") == MODE:
            await self.execute_message(context, event_queue, text)
            return
        if self.local["a2a"].get("message_delivery"):
            raise InvalidParamsError("此原聊天接口已启用普通消息，请更新发信程序")
        if context.current_task:
            raise InvalidParamsError("原任务仍在处理，请查询该任务，勿重复发起")
        origin_label = context.call_context.user.user_name
        source = None
        wait_idle = self.local["a2a"].get("idle_wait_seconds", 300)
        if self.bridge:
            from access_policy import chat_ref
            from google.protobuf.json_format import MessageToDict
            source = context.call_context.state.get("origin_peer")
            metadata = MessageToDict(context.message.metadata)
            claimed = metadata.get("collaboration_origin")
            requested = metadata.get("collaboration_wait_idle")
            if requested is not None and (type(requested) not in (int, float) or not 1 <= requested <= 600
                                          or requested != int(requested)):
                raise InvalidParamsError("等待空闲秒数无效")
            try:
                if source is None or (claimed is not None and chat_ref(claimed) != chat_ref(source)):
                    raise ValueError()
            except (TypeError, ValueError):
                raise InvalidParamsError("发件声明与已核验的原聊天不符") from None
            decision = self.bridge.policy.check(source, self.peer)
            if requested is not None:
                wait_idle = int(requested)
            elif decision["busy"]:
                wait_idle = 0
            if not decision["contact"]:
                raise InvalidParamsError("未授权投递：" + decision["reason"])
            origin_label = source["alias"]
        elif origin_label.startswith("device:"):
            from google.protobuf.json_format import MessageToDict
            from host_adapters import ChatRef
            try:
                claimed = MessageToDict(context.message.metadata).get("collaboration_origin", {})
                origin = ChatRef.from_peer(claimed)
                _, peers = load_config(self.root)
                matches = [p for p in peers.values() if all(p.get(k) == v for k, v in origin.to_dict().items())
                           and p.get("enabled", True) is True and p["device_id"] == origin_label[7:]]
                if len(matches) != 1:
                    raise ValueError()
                origin_label = matches[0]["alias"]
            except Exception as error:
                raise InvalidParamsError("发件原聊天未获设备身份授权") from error
        task = new_task_from_user_message(context.message)
        try:
            address = self.mailbox.claim(
                task.id, self.alias, self.peer["conversation_id"],
                context.call_context.user.user_name, context.message.message_id, self.instance,
                source_ref=chat_ref(source) if self.bridge else None,
                target_ref=chat_ref(self.peer) if self.bridge else None,
            )
        except Exception as error:
            raise InvalidParamsError(str(error)) from error
        record = self.mailbox.row(task.id) if self.bridge else None
        await event_queue.enqueue_event(task)
        updater = TaskUpdater(event_queue, task_id=task.id, context_id=task.context_id)
        await updater.update_status(TaskState.TASK_STATE_WORKING)
        log.info("received", extra={"event": "task_received", "task_id": task.id})
        message = (
            "【A2A 来信，来自 " + origin_label + "，回信给 " + address + "】\n" + text
            + "\n\n完成后用现有 " + ("send.sh " if self.peer["software"] == "minis" else "send.ps1 ") + address
            + ' "结果正文" 回信；仍只填收件人和正文。此地址仅属于本次任务。'
        )
        try:
            if self.bridge and (self.peer.get("transport") == "a2a" or self.peer["software"] == "a2a"):
                await self.exchange_external(task, updater, source, record, text)
                return
            receipt = await self.deliver(
                self.root, self.local, self.alias, message, task.id,
                wait_idle,
            )
            until = time.monotonic() + self.local["a2a"].get("task_timeout", 900)
            while time.monotonic() < until:
                row = self.mailbox.row(task.id)
                if row["result"] is not None:
                    await updater.add_artifact(parts=[new_text_part(row["result"])])
                    await updater.update_status(
                        TaskState.TASK_STATE_COMPLETED,
                        message=new_text_message(row["result"]),
                    )
                    self.mailbox.finish(task.id, "done")
                    log.info("completed", extra={"event": "task_completed", "task_id": task.id})
                    return
                await asyncio.sleep(0.3)
            raise TimeoutError("尚未收到原聊天回件，请核对原聊天；未自动重发")
        except asyncio.CancelledError:
            self.mailbox.finish(task.id, "interrupted")
            raise
        except Exception as error:
            self.mailbox.finish(task.id, "failed")
            log.warning("failed", extra={"event": "task_failed", "task_id": task.id, "error_type": type(error).__name__})
            await updater.update_status(
                TaskState.TASK_STATE_FAILED,
                message=new_text_message(str(error)[:350]),
            )
        finally:
            # A queued body belongs only to its live A2A task. Never leave an
            # abandoned task available for native redelivery after recovery.
            if self.peer["device_id"] != self.local["device_id"] and self.peer.get("transport") != "a2a" and self.peer["software"] != "a2a":
                DeviceQueue(self.mailbox).close(task.id)

    async def cancel(self, context, event_queue):
        raise TaskNotCancelableError("无法取消已进入桌面原聊天的工作")


class DesktopHandler(DefaultRequestHandler):
    async def on_cancel_task(self, params, context):
        # SDK cancellation stops its producer even when executor.cancel refuses.
        # Reject before that path, after the normal owner-scoped existence check.
        await self.on_get_task(GetTaskRequest(id=params.id), context)
        raise TaskNotCancelableError("桌面工作已交给原聊天，当前入口不支持取消；可以继续查询回件")


def agent_card(alias, base_url, message_delivery=False):
    return AgentCard(
        name=alias, description=("向已配对的 " + alias + " 原聊天投递消息，协议状态表示收件确认。" if message_delivery else "联系已配对的 " + alias + " 原聊天，文本结果通过本任务返回。"),
        version="1.0.0", default_input_modes=["text/plain"], default_output_modes=["text/plain"],
        capabilities=AgentCapabilities(streaming=False, push_notifications=False),
        supported_interfaces=[AgentInterface(
            protocol_binding="JSONRPC", url=base_url + "/", protocol_version="1.0",
        )],
        skills=[AgentSkill(
            id=SKILL_ID if message_delivery else "conversation", name="原聊天协作", description="向原聊天发送消息，接收程序自动确认。" if message_delivery else "向原聊天派题、询问或交换结果。",
            tags=["chat"], input_modes=["text/plain"], output_modes=["text/plain"],
        )],
        security_schemes={"bearer": SecurityScheme(
            http_auth_security_scheme=HTTPAuthSecurityScheme(scheme="bearer"),
        )},
        security_requirements=[SecurityRequirement(schemes={"bearer": {"list": []}})],
    )


def create_app(root, instance, deliver=native_deliver, shutdown=None):
    root = Path(root)
    local, peers = load_config(root)
    config = local["a2a"]
    tokens = dict(config["tokens"])
    if any(name.startswith(("device:", "chat:", "origin:")) for name in tokens):
        raise ValueError("设备和原聊天身份使用保留前缀")
    device_tokens = config.get("device_tokens", {})
    for device_id, token in device_tokens.items():
        if not isinstance(device_id, str) or not device_id or len(device_id) > 64 or "/" in device_id:
            raise ValueError("配对设备编号无效")
        tokens["device:" + device_id] = token
    managed = config.get("managed_ui") is True
    messages = config.get("message_delivery") is True
    if managed or messages:
        tokens.update({"chat:" + alias: token for alias, token in config.get("chat_tokens", {}).items()})
    if not tokens or any(not isinstance(v, str) or len(v) < 32 for v in tokens.values()) or len(set(tokens.values())) != len(tokens):
        raise ValueError("A2A 访问令牌未配置")
    host, port = config.get("host", "127.0.0.1"), config.get("port", 8788)
    if host != "127.0.0.1":
        raise ValueError("服务保持本机监听；跨设备通过已配对的私网 HTTPS 代理")
    public_base = config.get("public_base_url", "").rstrip("/")
    if public_base:
        parsed = urlsplit(public_base)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.path or parsed.query or parsed.fragment:
            raise ValueError("对外地址须是无凭据、路径或参数的 HTTPS 根地址")
    runtime = root / "runtime"
    runtime.mkdir(exist_ok=True)
    engine = create_async_engine("sqlite+aiosqlite:///" + (runtime / "a2a-tasks.db").as_posix())

    @event.listens_for(engine.sync_engine, "connect")
    def secure_delete(connection, record):
        cursor = connection.cursor()
        cursor.execute("PRAGMA secure_delete=ON")
        cursor.close()

    mailbox = Mailbox(root)
    devices = DeviceQueue(mailbox)
    from access_policy import Policy
    from managed_bridge import ManagedBridge
    policy = Policy(root, local, peers)
    bridge = ManagedBridge(root, local, peers, policy, mailbox, instance) if managed or messages else None
    stores, routes, handlers = {}, [], []
    resumed_tasks, result_dispatches = set(), {}
    delivery_locks = {(p["device_id"], p["software"], p["conversation_id"]): asyncio.Lock()
                      for p in peers.values() if p["device_id"] == local["device_id"]
                      and p.get("transport") != "a2a" and p["software"] != "a2a"}
    delivery_users = dict.fromkeys(delivery_locks, 0)

    async def deliver_message(source, target, text, task_id, wait_idle):
        if target["device_id"] != local["device_id"]:
            return await submit_message(source, target, text, task_id, wait_idle)
        key = (target["device_id"], target["software"], target["conversation_id"])
        lock = delivery_locks.get(key)
        if lock is None:
            raise BridgeError("目标没有已登记的原聊天主动收件入口")
        started = time.monotonic()
        queued = delivery_users[key] > 0
        if queued and not wait_idle:
            raise BridgeError("目标正在处理另一条原生投递，未发信")
        delivery_users[key] += 1
        acquired = False
        try:
            if queued:
                try:
                    await asyncio.wait_for(lock.acquire(), timeout=wait_idle)
                except TimeoutError:
                    raise BridgeError("等待目标投递窗口超时，未发信") from None
            else:
                await lock.acquire()
            acquired = True
            time_left = wait_idle - (time.monotonic() - started)
            if queued and time_left <= 0:
                raise BridgeError("等待目标投递窗口超时，未发信")
            remaining = int(time_left) if queued else wait_idle
            return await submit_message(source, target, text, task_id, remaining,
                                        force_idle=queued and remaining == 0)
        finally:
            if acquired:
                lock.release()
            delivery_users[key] -= 1

    async def submit_message(source, target, text, task_id, wait_idle, force_idle=False):
        if target.get("transport") == "a2a" or target["software"] == "a2a":
            raise BridgeError("标准任务服务的结果须从原请求读取，不能推定支持原聊天主动收件")
        payload = business_payload(source, text)
        record = mailbox.row(task_id)
        if not bridge or not bridge.decision(record)["contact"]:
            raise BridgeError("实际投递前方向权限已失效")
        if target["device_id"] == local["device_id"]:
            encoded = encode_payload(payload)
            permit = bridge.issue(task_id, target, encoded, task_id, "task",
                                  bridge.decision(record)["busy"] and not force_idle, wait_idle)
            return await deliver(root, local, target["alias"], encoded, task_id, wait_idle, permit=permit)
        return await devices.deliver(target["device_id"], target["alias"], target["conversation_id"],
                                     payload, task_id, config.get("delivery_timeout", 300))
    for alias, peer in peers.items():
        from access_policy import external_a2a_ready
        external = peer.get("transport") == "a2a" or peer["software"] == "a2a"
        if external and (not (managed or messages) or not external_a2a_ready(local, peer)):
            continue
        if not peer.get("enabled", True) or (not external and peer["software"] not in {"codex", "workbuddy", "minis"}):
            continue
        remote = peer["device_id"] != local["device_id"]
        if remote and not external and peer["device_id"] not in device_tokens:
            continue
        name = peer["alias"]
        prefix = "/agents/" + name
        card_url = "http://" + host + ":" + str(port) + "/agents/" + quote(name, safe="")
        card = agent_card(name, card_url, True) if messages else agent_card(name, card_url)
        store = TemporaryTaskStore(
            engine, mailbox=mailbox, owner_resolver=lambda ctx, a=alias: a + ":" + ctx.user.user_name,
        )
        stores[name] = store
        async def delivery(root_, local_, alias_, text_, task_id_, wait_idle_, p=peer):
            if p["device_id"] == local["device_id"]:
                if bridge:
                    decision = bridge.decision(mailbox.row(task_id_))
                    if not decision["contact"]:
                        raise BridgeError("实际投递前已撤销权限")
                    permit = bridge.issue(task_id_, p, text_, task_id_, "task", decision["busy"], wait_idle_)
                    return await deliver(root_, local_, alias_, text_, task_id_, wait_idle_, permit=permit)
                return await deliver(root_, local_, alias_, text_, task_id_, wait_idle_)
            return await devices.deliver(p["device_id"], alias_, p["conversation_id"], text_, task_id_,
                                         config.get("delivery_timeout", 300))
        executor = DesktopExecutor(root, local, name, peer, mailbox, instance, delivery, bridge)
        executor.message_deliver = deliver_message
        handler = DesktopHandler(
            agent_executor=executor, task_store=store, agent_card=card, validate_input_modes=True,
        )
        handlers.append(handler)
        async def choose_card(default_card, n=name):
            # Local callers keep the loopback card. Paired network credentials
            # get a configured canonical HTTPS address, never a supplied Host.
            if public_base and request_caller.get() != "local":
                return agent_card(n, public_base + "/agents/" + quote(n, safe=""), True) if messages else agent_card(n, public_base + "/agents/" + quote(n, safe=""))
            return default_card
        agent_routes = create_agent_card_routes(card, card_modifier=choose_card)
        agent_routes += create_jsonrpc_routes(handler, "/", context_builder=CallContext(), enable_v0_3_compat=True)
        routes.append(Mount(prefix, app=Starlette(routes=agent_routes)))

    async def forward_external_result(original):
        from access_policy import chat_ref
        from managed_bridge import owner_of
        source_ref, target_ref = json.loads(original["target_ref"]), json.loads(original["source_ref"])
        source = next((p for p in peers.values() if chat_ref(p) == source_ref), None)
        target = next((p for p in peers.values() if chat_ref(p) == target_ref), None)
        if (not bridge or source is None or target is None
                or target.get("transport") == "a2a" or target["software"] == "a2a"
                or not bridge.policy.check(source, target)["contact"]):
            mailbox.mark_external_result_handled(original["task_id"])
            return
        body = original["external_result"]
        message_id = str(uuid.uuid5(uuid.NAMESPACE_URL, "external-reply:" + original["task_id"]))
        try:
            returned, fresh = mailbox.claim_message(
                str(uuid.uuid4()), target["alias"], target["conversation_id"], owner_of(source),
                message_id, instance, source_ref=source_ref, target_ref=target_ref,
                request_hash=request_hash(target, body))
            if fresh:
                try:
                    native = await deliver_message(source, target, body, returned["task_id"],
                                                   config.get("idle_wait_seconds", 300))
                    mailbox.record_delivery(returned["task_id"], native_receipt(target, native))
                except Exception:
                    mailbox.record_delivery(returned["task_id"], {"state": "uncertain", "stage": "relay"})
            mailbox.mark_external_result_handled(original["task_id"])
        except BridgeError:
            # No new dispatch happened at capacity. Keep the short-lived original result.
            return


    async def forward_external_results():
        # One unavailable phone must not block other original conversations.
        for original in mailbox.pending_external_results():
            key = original["task_id"]
            if key in result_dispatches:
                continue
            if len(result_dispatches) >= mailbox.limits.max_active_tasks:
                break
            task = asyncio.create_task(forward_external_result(original))
            result_dispatches[key] = task
            def finished(done, key=key):
                result_dispatches.pop(key, None)
                if not done.cancelled():
                    try:
                        done.result()
                    except Exception as error:
                        log.warning("external result", extra={"event": "external_result_forward", "error_type": type(error).__name__})
            task.add_done_callback(finished)

    async def resume_external_result(row, peer, source):
        from managed_egress import resume
        try:
            result = await resume(root, local, peer, source, row["task_id"],
                                  task_timeout=config.get("task_timeout", 900))
            body = result.get("reply")
            if isinstance(body, str) and body:
                mailbox.store_external_result(row["task_id"], body)
            elif result.get("state") not in {"working", "accepted"}:
                mailbox.record_delivery(row["task_id"], {"state": "uncertain", "stage": "relay"})
            store = stores.get(row["agent"])
            if store:
                context = ServerCallContext(user=Caller(row["owner"]))
                task = await store.get(row["task_id"], context)
                if task and task.status.state not in {TaskState.TASK_STATE_COMPLETED, TaskState.TASK_STATE_FAILED,
                                                     TaskState.TASK_STATE_CANCELED, TaskState.TASK_STATE_REJECTED}:
                    if isinstance(body, str) and body:
                        task.artifacts.append(Artifact(artifact_id=str(uuid.uuid4()), parts=[new_text_part(body)]))
                    task.status.state = TaskState.TASK_STATE_COMPLETED if body else TaskState.TASK_STATE_FAILED
                    await store.save(task, context)
        except asyncio.CancelledError:
            raise
        except Exception:
            mailbox.record_delivery(row["task_id"], {"state": "uncertain", "stage": "relay"})

    async def external_results_loop():
        while True:
            try:
                await forward_external_results()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                log.warning("external result", extra={"event": "external_result_update", "error_type": type(error).__name__})
            await asyncio.sleep(3)

    async def recover():
        for row in mailbox.previous_instance(instance):
            if row.get("mode") == MODE:
                recovered_receipt = mailbox.record_delivery(row["task_id"], {"state": "uncertain", "stage": "relay"})
                store = stores.get(row["agent"])
                if store:
                    context = ServerCallContext(user=Caller(row["owner"]))
                    task = await store.get(row["task_id"], context)
                    if task and task.status.state not in {TaskState.TASK_STATE_COMPLETED, TaskState.TASK_STATE_FAILED,
                                                        TaskState.TASK_STATE_CANCELED, TaskState.TASK_STATE_REJECTED}:
                        task.status.state = (TaskState.TASK_STATE_COMPLETED if recovered_receipt["state"] in {"accepted", "queued"}
                                             else TaskState.TASK_STATE_REJECTED if recovered_receipt["state"] == "not_sent"
                                             else TaskState.TASK_STATE_FAILED)
                        await store.save(task, context)
                continue
            store = stores.get(row["agent"])
            if not store:
                continue
            context = ServerCallContext(user=Caller(row["owner"]))
            task = await store.get(row["task_id"], context)
            if task is None or task.status.state in {
                TaskState.TASK_STATE_COMPLETED, TaskState.TASK_STATE_FAILED,
                TaskState.TASK_STATE_CANCELED, TaskState.TASK_STATE_REJECTED,
            }:
                if task is not None:
                    mailbox.seal(task.id, "done" if task.status.state == TaskState.TASK_STATE_COMPLETED else "failed")
                else:
                    mailbox.seal(row["task_id"], "failed")
                continue
            if row["result"] is not None:
                task.artifacts.append(Artifact(
                    artifact_id=str(uuid.uuid4()), parts=[new_text_part(row["result"])],
                ))
                task.status.state = TaskState.TASK_STATE_COMPLETED
                task.status.message.CopyFrom(new_text_message(row["result"]))
            else:
                task.status.state = TaskState.TASK_STATE_FAILED
                task.status.message.CopyFrom(new_text_message("服务曾中断，请核对原聊天；未重新投递"))
            await store.save(task, context)
            mailbox.finish(task.id, "recovered")

    @asynccontextmanager
    async def lifespan(app):
        maintenance = None
        notifications = None
        results_worker = None
        try:
            # Multiple owner-scoped stores share one official SDK table.
            for store in stores.values():
                await store.initialize()
            await recover()
            if messages:
                with mailbox.connect() as db:
                    external_rows = [dict(row) for row in db.execute(
                        "SELECT * FROM replies WHERE mode='message' AND external_result_state IS NULL")]
                for row in external_rows:
                    peer = peers.get(row["agent"].casefold())
                    if not peer or not (peer.get("transport") == "a2a" or peer["software"] == "a2a"):
                        continue
                    ref = json.loads(row["source_ref"])
                    from access_policy import chat_ref
                    source = next((p for p in peers.values() if chat_ref(p) == ref), None)
                    if source:
                        task = asyncio.create_task(resume_external_result(row, peer, source))
                        resumed_tasks.add(task)
                        task.add_done_callback(resumed_tasks.discard)
                results_worker = asyncio.create_task(external_results_loop())
            await cleaner()
            maintenance = asyncio.create_task(cleanup_loop(cleaner, mailbox.limits.cleanup_interval_seconds))
            from result_watcher import ResultWatcher
            watcher = ResultWatcher(root, local, peers, managed_deliver=bridge.notify_local) if bridge else ResultWatcher(root, local, peers)
            notifications = asyncio.create_task(watcher.run())
            log.info("started", extra={"event": "service_started"})
            yield
        finally:
            if results_worker is not None:
                results_worker.cancel()
                await asyncio.gather(results_worker, return_exceptions=True)
            remaining = tuple(resumed_tasks) + tuple(result_dispatches.values())
            for task in remaining:
                task.cancel()
            if remaining:
                await asyncio.gather(*remaining, return_exceptions=True)
            if notifications is not None:
                notifications.cancel()
                await asyncio.gather(notifications, return_exceptions=True)
            if maintenance is not None:
                maintenance.cancel()
                await asyncio.gather(maintenance, return_exceptions=True)
            try:
                closed = await asyncio.gather(*(handler.aclose() for handler in handlers), return_exceptions=True)
                for result in closed:
                    if isinstance(result, BaseException):
                        log.warning("shutdown", extra={"event": "shutdown_task_error", "error_type": type(result).__name__})
            finally:
                await engine.dispose()
            log.info("stopped", extra={"event": "service_stopped"})

    async def cleaner(now=None):
        await asyncio.to_thread(devices.cleanup, now)
        if stores:
            return await cleanup_once(engine, mailbox, next(iter(stores.values())), now)
        await asyncio.to_thread(mailbox.cleanup, now)
        return 0

    async def receipt_status(request):
        from managed_bridge import owner_of
        sender = request.scope.get("collaboration_source")
        if not messages or sender is None:
            return JSONResponse({"error": "not_found"}, 404)
        message_id = request.path_params["message_id"]
        if len(message_id) > 128:
            return JSONResponse({"error": "not_found"}, 404)
        receipt = await asyncio.to_thread(mailbox.receipt_for, owner_of(sender), message_id)
        if receipt is not None and request.query_params.get("include_result") == "1":
            receipt = {**receipt, "result": mailbox.external_result_for(owner_of(sender), message_id)}
        return JSONResponse(receipt if receipt is not None else {"error": "not_found"},
                            200 if receipt is not None else 404, headers={"Cache-Control": "no-store"})

    routes.append(Route("/collaboration/receipts/{message_id}", receipt_status, methods=["GET"]))

    async def health(request):
        info = {"service": "ai-collaboration-a2a"}
        if request.scope.get("a2a_caller") == "local":
            info.update(instance=instance, agents=list(stores))
        return JSONResponse(info)

    async def public_package(request):
        # The release builder includes public code only. No caller-supplied
        # filename, configuration, runtime database, or credential is served.
        package = root / "runtime/public-update.zip"
        if not package.is_file():
            return JSONResponse({"error": "no_update_package"}, 404)
        return FileResponse(package, media_type="application/zip", filename="ai-collaboration-update.zip",
                            headers={"Cache-Control": "no-store"})

    async def stop(request):
        if request.scope.get("a2a_caller") != "local":
            return JSONResponse({"error": "forbidden"}, 403)
        data = await request.json()
        if not shutdown or data.get("instance") != instance:
            return JSONResponse({"error": "instance_mismatch"}, 409)
        asyncio.get_running_loop().call_later(0.1, shutdown)
        return JSONResponse({"state": "stopping"})

    def authorized_device(request):
        device_id = request.path_params["device_id"]
        return device_id if request.scope.get("a2a_caller") == "device:" + device_id and device_id in device_tokens else None

    async def poll_device(request):
        device_id = authorized_device(request)
        if not device_id:
            return JSONResponse({"error": "forbidden"}, 403)
        if bridge and request.query_params.get("policy") != "2":
            return JSONResponse({"error": "update_receiver_before_delivery"}, 409)
        try:
            wait = float(request.query_params.get("wait", "25"))
            if not 0 <= wait <= 30:
                raise ValueError()
        except ValueError:
            return JSONResponse({"error": "invalid_wait"}, 400)
        until = time.monotonic() + wait
        while True:
            check = (lambda row: bridge.decision(row)["contact"]) if bridge else None
            result = await asyncio.to_thread(devices.next, device_id, can_offer=check,
                                             message_capable=request.query_params.get("message_version") == "1")
            if result["state"] != "empty" or time.monotonic() >= until or await request.is_disconnected():
                return JSONResponse(result, headers={"Cache-Control": "no-store"})
            await asyncio.sleep(.25)

    async def acknowledge_device(request):
        device_id = authorized_device(request)
        if not device_id:
            return JSONResponse({"error": "forbidden"}, 403)
        try:
            data = await request.json()
            if not isinstance(data, dict) or set(data) - {"delivery_id", "state", "scheduled_task_id", "receipt"}:
                raise ValueError()
            delivery_id = data.get("delivery_id")
            if not isinstance(delivery_id, str) or not 0 < len(delivery_id) <= 128:
                raise ValueError()
            if bridge and data.get("state") in {"accepted", "uncertain"}:
                row = mailbox.row(delivery_id)
                if not row or not row["issued_at"] or not row["target_ref"] or json.loads(row["target_ref"])[0] != device_id:
                    return JSONResponse({"error": "delivery_not_authorized"}, 403)
            await asyncio.to_thread(devices.ack, device_id, delivery_id, data.get("state"), data.get("scheduled_task_id"), receipt=data.get("receipt"))
        except (ValueError, TypeError, AttributeError, BridgeError):
            return JSONResponse({"error": "invalid_ack"}, 409)
        return JSONResponse({"state": "accepted"})

    async def reply_device(request):
        device_id = authorized_device(request)
        if bridge and device_id is None:
            bound = bridge.source(request.scope.get("bridge_principal", ""))
            if bound and bound["device_id"] == request.path_params["device_id"] and bound["device_id"] in device_tokens:
                device_id = bound["device_id"]
        if not device_id:
            return JSONResponse({"error": "forbidden"}, 403)
        try:
            data = await request.json()
            address, text = data["address"], data["text"]
            if not isinstance(address, str) or len(address) > 32 or not isinstance(text, str) or not text.strip():
                raise ValueError()
            try:
                delivery = await asyncio.to_thread(devices.sender_for_reply, device_id, address, instance)
            except BridgeError:
                return JSONResponse({"error": "unassigned_reply"}, 403)
            sender_peer = peers.get(delivery["peer"].casefold())
            if not sender_peer or sender_peer["device_id"] != device_id or sender_peer.get("conversation_id") != delivery["conversation_id"]:
                return JSONResponse({"error": "wrong_chat"}, 403)
            claimed = data.get("sender")
            if claimed is not None and (not isinstance(claimed, dict)
                    or not isinstance(claimed.get("alias"), str)
                    or claimed["alias"].casefold() != sender_peer["alias"].casefold()
                    or claimed.get("conversation_id") != sender_peer["conversation_id"]):
                return JSONResponse({"error": "wrong_chat"}, 403)
            if bridge:
                from access_policy import chat_ref
                bound = bridge.source(request.scope.get("bridge_principal", ""))
                if not bound or chat_ref(bound) != chat_ref(sender_peer):
                    return JSONResponse({"error": "unbound_reply"}, 403)
            await asyncio.to_thread(mailbox.submit, address, text, sender_peer, instance=instance, require_delivery=bool(bridge))
        except (ValueError, TypeError, KeyError, AttributeError, BridgeError):
            return JSONResponse({"error": "invalid_reply"}, 409)
        return JSONResponse({"state": "accepted", "to": address})

    routes.append(Route("/health", health))
    routes.append(Route("/package", public_package, methods=["GET"]))
    routes.append(Route("/shutdown", stop, methods=["POST"]))
    routes.append(Route("/devices/{device_id}/next", poll_device, methods=["GET"]))
    routes.append(Route("/devices/{device_id}/ack", acknowledge_device, methods=["POST"]))
    routes.append(Route("/devices/{device_id}/reply", reply_device, methods=["POST"]))
    if bridge:
        async def authorize(request):
            device_id = authorized_device(request)
            if not device_id:
                return JSONResponse({"error": "forbidden"}, 403)
            try:
                data = await request.json()
                delivery_id = data["delivery_id"]
                if not isinstance(delivery_id, str) or not 0 < len(delivery_id) <= 128:
                    raise ValueError()
                result = await asyncio.to_thread(devices.authorize, device_id, delivery_id, instance, bridge.decision)
            except (KeyError, TypeError, ValueError, BridgeError):
                return JSONResponse({"error": "invalid_delivery"}, 400)
            return JSONResponse(result, 200 if result["allowed"] else 403, headers={"Cache-Control": "no-store"})

        async def consume(request):
            try:
                permit = bridge.consume(await request.json())
            except (KeyError, TypeError, ValueError, BridgeError):
                return JSONResponse({"error": "invalid_delivery_permit"}, 403)
            return JSONResponse(permit, headers={"Cache-Control": "no-store"})

        async def reply_local(request):
            sender = request.scope.get("collaboration_source")
            if not sender or sender["device_id"] != local["device_id"]:
                return JSONResponse({"error": "unbound_chat"}, 403)
            try:
                data = await request.json()
                address, body = data["address"], data["text"]
                if not isinstance(address, str) or len(address) > 32 or not isinstance(body, str) or not body.strip():
                    raise ValueError()
                mailbox.submit(address, body, sender, instance=instance, require_delivery=True)
            except (KeyError, TypeError, ValueError, BridgeError):
                return JSONResponse({"error": "unassigned_or_invalid_reply"}, 403)
            return JSONResponse({"state": "accepted", "to": address})

        async def notification(request):
            source = bridge.source(request.scope.get("bridge_principal", ""))
            if not source or source["device_id"] != request.path_params["device_id"] or source["device_id"] not in device_tokens:
                return JSONResponse({"error": "forbidden"}, 403)
            try:
                bridge.reported_notice(source, await request.json())
            except (TypeError, ValueError, KeyError, BridgeError):
                return JSONResponse({"error": "unassigned_notification"}, 403)
            return JSONResponse({"state": "accepted"})

        routes += [Route("/devices/{device_id}/authorize", authorize, methods=["POST"]),
                   Route("/devices/{device_id}/notification", notification, methods=["POST"]),
                   Route("/collaboration/consume", consume, methods=["POST"]),
                   Route("/collaboration/reply", reply_local, methods=["POST"])]
    app = Starlette(routes=routes, lifespan=lifespan)
    app.state.handlers = handlers
    app.state.cleaner = cleaner
    app.state.policy, app.state.bridge = policy, bridge
    return BearerAccess(app, tokens, mailbox.limits, bridge=bridge, legacy_local_only=not bool(public_base))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--instance", required=True)
    args = parser.parse_args()
    local, _ = load_config(args.root)
    configure_logging(args.root)
    # Runtime diagnostics go only to the bounded metadata log. Startup import
    # errors still reach the bootstrap stderr file managed by a2a.ps1.
    sys.stdout = open(os.devnull, "w")
    sys.stderr = open(os.devnull, "w")
    import uvicorn
    def shutdown():
        server.should_exit = True
    server = uvicorn.Server(uvicorn.Config(
        create_app(args.root, args.instance, shutdown=shutdown),
        host=local["a2a"].get("host", "127.0.0.1"), port=local["a2a"].get("port", 8788),
        access_log=False, log_level="warning", log_config=None, timeout_graceful_shutdown=5,
    ))
    server.run()


if __name__ == "__main__":
    main()
