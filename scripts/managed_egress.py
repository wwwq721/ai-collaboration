"""Service-only, policy-gated SDK egress to a verified original A2A chat.

Ordinary clients cannot opt into this route through exchange(). Only metadata
is kept in independent tables of the existing mailbox database.
"""
import asyncio
from contextlib import asynccontextmanager
import hashlib
import json
import math
from pathlib import Path
import time

import httpx
from a2a.client import A2ACardResolver, ClientConfig, ClientFactory
from a2a.helpers import get_message_text, new_text_message
from a2a.types import GetTaskRequest, Role, SendMessageRequest, TaskState
from a2a.utils.errors import TaskNotFoundError

from a2a_client import TERMINAL, INTERRUPTED, origin, task_result, failure_state
from a2a_state import Mailbox
from collaboration_client import validate_external_peer
from host_adapters import ChatRef
from workbuddy import BridgeError


def _identity(peer):
    return json.dumps(ChatRef.from_peer(peer).to_dict(), sort_keys=True, separators=(",", ":"))


async def _bound_task(client, task_id, context_id=None, *, deadline=None):
    request = client.get_task(GetTaskRequest(id=task_id))
    task = await request if deadline is None else await asyncio.wait_for(request, max(.001, deadline-time.time()))
    if task.id != task_id or context_id and task.context_id != context_id:
        raise BridgeError("远端返回的任务或上下文与原绑定不符", "uncertain")
    return task


def _credential(local, alias):
    credentials = local.get("a2a_credentials", {})
    matches = [v for k, v in credentials.items() if isinstance(k, str) and k.casefold() == alias.casefold()]
    token = matches[0].get("token") if len(matches) == 1 and isinstance(matches[0], dict) else None
    if not isinstance(token, str) or not token:
        raise BridgeError("外部 A2A 缺少伙伴独立凭据")
    for name in ("tokens", "chat_tokens", "device_tokens"):
        if token in local.get("a2a", {}).get(name, {}).values():
            raise BridgeError("外部 A2A 必须使用独立伙伴凭据")
    if any(k.casefold() != alias.casefold() and isinstance(v, dict) and v.get("token") == token
           for k, v in credentials.items()):
        raise BridgeError("外部 A2A 伙伴凭据不能与其他伙伴共用")
    return token


def _target_url(local, peer):
    url = validate_external_peer(peer)
    from access_policy import external_a2a_ready
    if not external_a2a_ready(local, peer, peer.get("alias")):
        raise BridgeError("外部 A2A 接入或独立凭据尚未核验")
    network_base = local.get("network", {}).get("base_url", "").rstrip("/")
    if peer.get("local_a2a") or network_base and origin(network_base) == origin(url):
        raise BridgeError("配对中央路由不能作为外部 A2A 直连")
    return url


@asynccontextmanager
async def _verified_client(state, peer, url, token, target_ref, *, deadline=None):
    timeout = 45 if deadline is None else max(.001, min(45, deadline-time.time()))
    async with httpx.AsyncClient(headers={"Authorization": "Bearer " + token}, timeout=timeout,
                                 trust_env=False, follow_redirects=False) as http:
        request = A2ACardResolver(http, base_url=url).get_agent_card()
        card = await request if deadline is None else await asyncio.wait_for(request, max(.001, deadline-time.time()))
        from urllib.parse import urlsplit
        if not card.supported_interfaces or any(origin(i.url) != origin(url) or
                not i.url.startswith("https://") or urlsplit(i.url).username or urlsplit(i.url).password
                or urlsplit(i.url).fragment for i in card.supported_interfaces):
            raise BridgeError("Agent Card 与已核验 HTTPS 站点不同，未转送伙伴凭据")
        state.pin_card(target_ref, url, card, peer.get("verified_at"))
        yield ClientFactory(ClientConfig(httpx_client=http, streaming=False, polling=True,
            supported_protocol_bindings=["JSONRPC"], accepted_output_modes=["text/plain"])).create(card)


def _task_result(task, peer, state):
    result = {**task_result(task, peer["alias"]), "context_id": task.context_id}
    if sum(len(result.get(k, "").encode("utf8")) for k in ("reply", "error")) > state.limits.max_text_bytes:
        return {"state": "failed", "error": "外部 A2A 回件超过正文上限", "task_id": task.id, "context_id": task.context_id}
    return result


def _cache_message_result(state, local_task, result):
    row = state.mailbox.row(local_task)
    if (row and row["mode"] == "message" and result.get("state") in {"completed", "input_required", "auth_required"}
            and isinstance(result.get("reply"), str) and result["reply"].strip()):
        state.mailbox.store_external_result(local_task, result["reply"])


def _task_admitted(task):
    return task.status.state in {
        TaskState.TASK_STATE_SUBMITTED, TaskState.TASK_STATE_WORKING,
        TaskState.TASK_STATE_INPUT_REQUIRED, TaskState.TASK_STATE_AUTH_REQUIRED,
        TaskState.TASK_STATE_COMPLETED,
    }


class EgressState:
    def __init__(self, root):
        self.mailbox = Mailbox(root)
        self.limits = self.mailbox.limits
        with self.mailbox.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS managed_egress_runs(
                    local_task TEXT PRIMARY KEY, source TEXT NOT NULL, target TEXT NOT NULL,
                    url TEXT NOT NULL, text_hash TEXT NOT NULL, state TEXT NOT NULL,
                    remote_task TEXT, context_id TEXT, created REAL NOT NULL, updated REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS managed_egress_cards(
                    target TEXT PRIMARY KEY, url TEXT NOT NULL, card_hash TEXT NOT NULL,
                    verification TEXT NOT NULL DEFAULT 'null', updated REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS managed_egress_direction ON managed_egress_runs(source,target,created);
            """)
            if "verification" not in {row[1] for row in db.execute("PRAGMA table_info(managed_egress_cards)")}:
                db.execute("ALTER TABLE managed_egress_cards ADD COLUMN verification TEXT NOT NULL DEFAULT 'null'")

    def reserve(self, local_task, source, target, url, text, *, independent=False):
        digest = hashlib.sha256(text.encode("utf8")).hexdigest()
        now = time.time()
        with self.mailbox.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            # Unknown/in-flight admissions remain tombstones until reconciled.
            # Capacity exhaustion refuses new work instead of forgetting a
            # potentially executed message and sending it a second time.
            db.execute("DELETE FROM managed_egress_runs WHERE updated<? AND state NOT IN "
                       "('prepared','issued','remote','uncertain')", (now-self.limits.metadata_ttl_seconds,))
            old = db.execute("SELECT * FROM managed_egress_runs WHERE local_task=?", (local_task,)).fetchone()
            if old:
                if any(old[k] != v for k, v in (("source", source), ("target", target), ("url", url), ("text_hash", digest))):
                    raise BridgeError("本地任务与外部原聊天投递绑定不符")
                return dict(old), False, None
            if db.execute("SELECT count(*) FROM managed_egress_runs").fetchone()[0] >= self.limits.max_metadata:
                raise BridgeError("外部 A2A 投递元数据已满")
            if independent and db.execute(
                    "SELECT count(*) FROM managed_egress_runs WHERE state IN ('prepared','issued','remote')"
            ).fetchone()[0] >= self.limits.max_active_tasks:
                raise BridgeError("外部服务的并行投递已达上限")
            blocked = db.execute("SELECT 1 FROM managed_egress_runs WHERE source=? AND target=? "
                                 "AND state IN ('prepared','issued','remote','uncertain') LIMIT 1", (source, target)).fetchone()
            if blocked and not independent:
                raise BridgeError("该原聊天方向已有未确认外部任务，请先核对原任务")
            previous = db.execute("SELECT * FROM managed_egress_runs WHERE source=? AND target=? "
                                  "AND url=? AND remote_task IS NOT NULL ORDER BY created DESC LIMIT 1", (source, target, url)).fetchone()
            suspended = db.execute("SELECT * FROM managed_egress_runs WHERE source=? AND target=? AND url=? "
                                   "AND state IN ('input_required','auth_required') AND remote_task IS NOT NULL",
                                   (source, target, url)).fetchall()
            if len({(row["remote_task"], row["context_id"]) for row in suspended}) > 1:
                previous = None
            db.execute("INSERT INTO managed_egress_runs(local_task,source,target,url,text_hash,state,created,updated) "
                       "VALUES(?,?,?,?,?,'prepared',?,?)", (local_task, source, target, url, digest, now, now))
            return {"state": "prepared", "remote_task": None, "context_id": None, "created": now}, True, dict(previous) if previous else None

    def pin_card(self, target, url, card, verified_at=None):
        routes = sorted((i.protocol_binding, i.url, i.protocol_version) for i in card.supported_interfaces)
        digest = hashlib.sha256(json.dumps(routes, separators=(",", ":")).encode("utf8")).hexdigest()
        verification = json.dumps(verified_at, sort_keys=True)
        with self.mailbox.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT url,card_hash,verification FROM managed_egress_cards WHERE target=?", (target,)).fetchone()
            if old and (old["url"] != url or old["card_hash"] != digest) and old["verification"] == verification:
                raise BridgeError("外部 A2A 目标或 Agent Card 已变化，需要重新核验")
            if not old and db.execute("SELECT count(*) FROM managed_egress_cards").fetchone()[0] >= self.limits.max_metadata:
                raise BridgeError("外部 A2A 卡片元数据已满")
            db.execute("INSERT INTO managed_egress_cards(target,url,card_hash,verification,updated) VALUES(?,?,?,?,?) "
                       "ON CONFLICT(target) DO UPDATE SET url=excluded.url,card_hash=excluded.card_hash,"
                       "verification=excluded.verification,updated=excluded.updated",
                       (target, url, digest, verification, time.time()))

    def update(self, task, state, remote_task=None, context_id=None):
        with self.mailbox.connect() as db:
            db.execute("UPDATE managed_egress_runs SET state=?,remote_task=COALESCE(?,remote_task),"
                       "context_id=COALESCE(?,context_id),updated=? WHERE local_task=?",
                       (state, remote_task, context_id, time.time(), task))

    def existing(self, local_task):
        with self.mailbox.connect() as db:
            row = db.execute("SELECT * FROM managed_egress_runs WHERE local_task=?", (local_task,)).fetchone()
            return dict(row) if row else None


async def deliver(root, local, peer, source, raw_text, local_task_id, approve, task_timeout=900, *, independent=False, on_admission=None):
    """Send once after approve(); duplicates only query their bound remote task."""
    state = None
    attempted = {"attempted": False}
    remote_task, context_id = None, None
    owns_run, fresh = False, True
    try:
        url = _target_url(local, peer)
        if (not isinstance(raw_text, str) or not raw_text.strip() or not isinstance(local_task_id, str)
                or not local_task_id or len(local_task_id) > 512 or not callable(approve)
                or isinstance(task_timeout, bool) or not isinstance(task_timeout, (int, float))
                or not math.isfinite(task_timeout) or not 0 < task_timeout <= 3600):
            raise BridgeError("外部 A2A 投递参数无效")
        target_ref, source_ref = _identity(peer), _identity(source)
        token = _credential(local, peer["alias"])
        state = EgressState(root)
        if len(raw_text.encode("utf8")) > state.limits.max_text_bytes:
            raise BridgeError("外部 A2A 正文超过上限")
        saved, fresh, previous = state.reserve(local_task_id, source_ref, target_ref, url, raw_text, independent=independent)
        owns_run = True
        if independent:
            previous = None
        remote_task, context_id = saved.get("remote_task"), saved.get("context_id")
        if not fresh and not remote_task:
            return {"state": "uncertain" if saved["state"] in {"issued", "remote", "uncertain", "completed"} else "failed",
                    "error": "该任务已尝试投递或正在核验，未重复派题"}
        deadline = saved["created"] + task_timeout if independent else None
        async with _verified_client(state, peer, url, token, target_ref, deadline=deadline) as client:
            task = None
            if not fresh:
                task = await _bound_task(client, remote_task, context_id, deadline=deadline)
            else:
                if previous and previous.get("remote_task"):
                    prior = await _bound_task(client, previous["remote_task"], previous.get("context_id"))
                    if prior.status.state not in TERMINAL | INTERRUPTED:
                        raise BridgeError("原远端任务尚未结束，未派新题")
                    context_id = prior.context_id
                    if prior.status.state in INTERRUPTED:
                        remote_task = prior.id
                message = new_text_message(raw_text, role=Role.ROLE_USER,
                                           context_id=context_id or None, task_id=remote_task or None)
                message.message_id = local_task_id
                message.metadata.update({"collaboration_origin": ChatRef.from_peer(source).to_dict()})
                outgoing = SendMessageRequest(message=message)
                outgoing.configuration.return_immediately = True
                # Persist before the callback: a crash or ambiguous return can
                # never cause a fresh send with this local task identifier.
                state.update(local_task_id, "issued")
                if approve() is not True:
                    raise BridgeError("最终交付前已撤销联系权限，未发送")
                attempted["attempted"] = True
                async for event in client.send_message(outgoing):
                    if event.HasField("message"):
                        attempted["task_received"] = True
                        text = get_message_text(event.message)
                        if len(text.encode("utf8")) > state.limits.max_text_bytes:
                            raise BridgeError("外部 A2A 回件超过正文上限", "uncertain")
                        result = {"state": "completed", "reply": text, "context_id": context_id or ""}
                        _cache_message_result(state, local_task_id, result)
                        state.update(local_task_id, "completed")
                        if on_admission:
                            on_admission({"state": "queued", "stage": "external_service"})
                        return result
                    if event.HasField("task"):
                        attempted["task_received"] = True
                        task = event.task
                        if (remote_task and task.id != remote_task or context_id and task.context_id != context_id):
                            raise BridgeError("远端回执改变了原任务绑定", "uncertain")
                        remote_task, context_id = task.id, task.context_id
                        state.update(local_task_id, "remote", remote_task, context_id)
                        if on_admission and _task_admitted(task):
                            on_admission({"state": "queued", "stage": "external_service", "native_id": task.id})
                if task is None:
                    raise RuntimeError("A2A missing task receipt")
            until = time.monotonic() + (task_timeout if deadline is None else max(0, deadline-time.time()))
            while task.status.state not in TERMINAL | INTERRUPTED:
                if time.monotonic() >= until:
                    state.update(local_task_id, "uncertain", task.id, task.context_id)
                    return {"state": "uncertain", "error": "等待原远端任务超时，未重新派题", "task_id": task.id, "context_id": task.context_id}
                await asyncio.sleep(min(1, max(0, until-time.monotonic())))
                task = await _bound_task(client, task.id, task.context_id, deadline=deadline)
            result = _task_result(task, peer, state)
            _cache_message_result(state, local_task_id, result)
            state.update(local_task_id, result["state"], task.id, task.context_id)
            return result
    except asyncio.CancelledError:
        if state is not None and owns_run:
            state.update(local_task_id, "uncertain" if attempted["attempted"] else "failed", remote_task, context_id)
        raise
    except Exception as error:
        unknown = attempted["task_received"] if "task_received" in attempted else False
        unknown = unknown or attempted["attempted"] and failure_state(attempted, error) == "uncertain"
        result_state = "uncertain" if unknown or not fresh and remote_task and not isinstance(error, TaskNotFoundError) else "failed"
        if state is not None and owns_run:
            state.update(local_task_id, result_state, remote_task if attempted["attempted"] or not fresh else None, context_id)
        return {"state": result_state, "error": (str(error) if isinstance(error, BridgeError) else
                "外部 A2A 未确认（" + type(error).__name__ + "），未重新派题"),
                **({"task_id": remote_task} if remote_task else {}), **({"context_id": context_id} if context_id else {})}


async def resume(root, local, peer, source, local_task_id, task_timeout=900):
    """Recover a previously accepted foreign task using reads of its exact binding."""
    state, owned = None, False
    remote_task, context_id = None, None
    try:
        if (not isinstance(local_task_id, str) or not local_task_id or len(local_task_id) > 512
                or isinstance(task_timeout, bool) or not isinstance(task_timeout, (int, float))
                or not math.isfinite(task_timeout) or not 0 < task_timeout <= 3600):
            raise BridgeError("外部恢复参数无效")
        url = _target_url(local, peer)
        source_ref, target_ref = _identity(source), _identity(peer)
        token = _credential(local, peer["alias"])
        state = EgressState(root)
        saved = state.existing(local_task_id)
        if not saved:
            return {"state": "uncertain", "error": "外部原任务登记尚未找到"}
        if any(saved[key] != value for key, value in (("source", source_ref), ("target", target_ref), ("url", url))):
            raise BridgeError("外部恢复身份与原投递记录不符")
        owned = True
        remote_task, context_id = saved["remote_task"], saved["context_id"]
        if not remote_task or not isinstance(context_id, str) or not context_id:
            return {"state": "uncertain", "error": "原投递缺少可只读核对的远端任务绑定"}
        deadline = saved["created"] + task_timeout
        if not math.isfinite(deadline) or time.time() >= deadline:
            state.update(local_task_id, "uncertain")
            return {"state": "uncertain", "error": "原外部任务确认期限已到", "task_id": remote_task, "context_id": context_id}
        async with _verified_client(state, peer, url, token, target_ref, deadline=deadline) as client:
            task = await _bound_task(client, remote_task, context_id, deadline=deadline)
            while task.status.state not in TERMINAL | INTERRUPTED:
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise TimeoutError()
                await asyncio.sleep(min(1, remaining))
                task = await _bound_task(client, remote_task, context_id, deadline=deadline)
            result = _task_result(task, peer, state)
            _cache_message_result(state, local_task_id, result)
            state.update(local_task_id, result["state"], remote_task, context_id)
            return result
    except asyncio.CancelledError:
        if state is not None and owned:
            state.update(local_task_id, "uncertain", remote_task, context_id)
        raise
    except Exception as error:
        if state is not None and owned:
            state.update(local_task_id, "uncertain", remote_task, context_id)
        return {"state": "uncertain" if owned else "failed",
                "error": str(error) if isinstance(error, BridgeError) else "外部任务只读恢复未确认（" + type(error).__name__ + "）",
                **({"task_id": remote_task} if remote_task else {}),
                **({"context_id": context_id} if context_id else {})}
