"""Text communication using the official A2A Python client."""
import asyncio
import json
from pathlib import Path
import sys
import time
from urllib.parse import quote, urlsplit

import httpx
from a2a.client import A2ACardResolver, ClientConfig, ClientFactory
from a2a.helpers import get_message_text, new_text_message
from a2a.types import GetTaskRequest, Role, SendMessageRequest, TaskState
from a2a.utils.errors import TaskNotFoundError, InvalidParamsError, InvalidRequestError, MethodNotFoundError

from a2a_state import Mailbox

TERMINAL = {
    TaskState.TASK_STATE_COMPLETED, TaskState.TASK_STATE_FAILED,
    TaskState.TASK_STATE_CANCELED, TaskState.TASK_STATE_REJECTED,
}
INTERRUPTED = {TaskState.TASK_STATE_INPUT_REQUIRED, TaskState.TASK_STATE_AUTH_REQUIRED}


def failure_state(request, error):
    """Only explicit refusal or failure before connecting proves no admission."""
    if not request.get("attempted"):
        return "not_sent"
    if request.get("task_received"):
        return "uncertain"
    current, seen = error, set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, (InvalidParamsError, InvalidRequestError, MethodNotFoundError,
                                httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)):
            return "not_sent"
        if isinstance(current, httpx.HTTPStatusError) and current.response.status_code in {401, 403}:
            return "not_sent"
        current = current.__cause__
    return "uncertain"


def task_text(task):
    parts = [part.text for artifact in task.artifacts for part in artifact.parts if part.HasField("text")]
    return "\n".join(parts) if parts else get_message_text(task.status.message)


def task_result(task, alias):
    state = task.status.state
    result = {"state": "accepted", "to": alias, "task_id": task.id}
    if state == TaskState.TASK_STATE_COMPLETED:
        result.update(state="completed", reply=task_text(task))
    elif state in INTERRUPTED:
        result.update(state="input_required" if state == TaskState.TASK_STATE_INPUT_REQUIRED else "auth_required",
                      reply=task_text(task))
    elif state in TERMINAL:
        name = {TaskState.TASK_STATE_CANCELED: "canceled", TaskState.TASK_STATE_REJECTED: "rejected"}.get(state, "failed")
        result.update(state=name, error=task_text(task))
    return result


def origin(url):
    parsed = urlsplit(url)
    return parsed.scheme.lower(), parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80)


def continuity_key(peer):
    """A paired chat's identity survives changing its connection address."""
    return "peer:" + json.dumps(
        [peer.get("device_id", ""), peer.get("host_software", peer.get("software", "a2a")),
         peer.get("conversation_id") or peer.get("peer_id") or peer["alias"].casefold()],
        separators=(",", ":"), ensure_ascii=True,
    )


async def query_receipt(request, job, http=None):
    """Read one sender-owned receipt. An absent record never proves non-delivery."""
    from collaboration_client import validate_central
    from message_delivery import normalize_receipt
    from result_watcher import _message_target
    local, peer = request["local"], request["peer"]
    validate_central(local, peer)
    base = (local.get("network", {}).get("base_url", "") if local.get("platform") == "minis"
            else "http://127.0.0.1:" + str(local.get("a2a", {}).get("port", 8788)))
    url = base.rstrip("/") + "/collaboration/receipts/" + quote(job["id"], safe="")
    if http is None:
        options = {}
        if local.get("network", {}).get("mode") == "paired_ip":
            from network_transport import build_http_options
            options = build_http_options(local, peer)
        else:
            from device_receiver import proxy_url
            proxy = proxy_url(local)
            if proxy:
                options["proxy"] = proxy
        async with httpx.AsyncClient(headers={"Authorization": "Bearer " + request["token"]},
                                     timeout=20, trust_env=False, follow_redirects=False, **options) as client:
            return await query_receipt(request, job, client)
    response = await http.get(url, headers={"Authorization": "Bearer " + request["token"]})
    if response.status_code == 404:
        return {"state": "waiting", "stage": "relay"}
    response.raise_for_status()
    if len(response.content) > 8192:
        raise ValueError("receipt too large")
    result = response.json()
    if (not isinstance(result, dict) or result.get("target_ref") != _message_target(job["peer"])
            or result.get("message_id", job["id"]) != job["id"]):
        raise ValueError("receipt does not belong to this target/message")
    return normalize_receipt(result)


async def exchange_message(request, client, http):
    from collaboration_client import source_token, validate_central
    from result_watcher import PendingResults
    from send import load_config, sender_at
    peer, local = request["peer"], request["local"]
    validate_central(local, peer)
    _, peers = load_config(Path(request["root"]))
    sender = sender_at(local, peers, strict=True)
    from access_policy import chat_ref
    if chat_ref(sender) != chat_ref(request.get("origin") or {}):
        raise ValueError("current host identity does not match prepared sender")
    request["token"] = source_token(local, peers, request.get("origin") or {})
    http.headers["Authorization"] = "Bearer " + request["token"]
    pending = PendingResults(Path(request["root"]))
    if request["mode"] == "status":
        job = pending.latest_message(request["origin"], peer)
        if job is None:
            return {"state": "ready", "to": peer["alias"]}
        try:
            receipt = await query_receipt(request, job, http)
        except Exception:
            receipt = job["receipt"] or {"state": "waiting", "stage": "relay"}
        if receipt["state"] == "waiting" and pending.clock() >= job["deadline"]:
            receipt = {"state": "uncertain", "stage": "relay"}
        pending.record_receipt(job["id"], receipt)
        return pending.receipt_status(job["id"])
    if request["mode"] != "send":
        raise ValueError("message operation is not supported")
    from message_delivery import business_payload
    business_payload(sender, request["text"])
    job_id = pending.prepare_message(request["origin"], peer, request["message_id"], request["text"])
    request["job_id"] = job_id
    message = new_text_message(request["text"], role=Role.ROLE_USER)
    message.message_id = request["message_id"]
    message.metadata.update({"collaboration_mode": "message"})
    if request.get("wait_idle"):
        message.metadata.update({"collaboration_wait_idle": request["wait_idle"]})
    outgoing = SendMessageRequest(message=message)
    outgoing.configuration.return_immediately = True
    try:
        request["attempted"] = True
        async for event in client.send_message(outgoing):
            if event.HasField("message") or event.HasField("task"):
                request["task_received"] = True
            if event.HasField("task"):
                pending.bind(job_id, event.task.id, event.task.context_id)
    except BaseException as error:
        if failure_state(request, error) == "not_sent":
            pending.record_receipt(job_id, {"state": "not_sent", "stage": "relay"})
        # An ambiguous POST remains queryable even without an SDK task id.
        raise
    try:
        receipt = await query_receipt(request, pending.get(job_id), http)
    except Exception:
        receipt = {"state": "waiting", "stage": "relay"}
    pending.record_receipt(job_id, receipt)
    return pending.receipt_status(job_id)


async def exchange(request):
    peer = request["peer"]
    from delivery_ticket import managed
    if managed(request.get("local", {})):
        from collaboration_client import validate_central, source_token
        from send import load_config
        validate_central(request["local"], peer)
        _, peers = load_config(Path(request["root"]))
        request["token"] = source_token(request["local"], peers, request.get("origin") or {})
    url = peer["url"].rstrip("/")
    if urlsplit(url).scheme not in {"http", "https"}:
        raise ValueError("A2A 地址须为 HTTP 或 HTTPS")
    token = request["token"]
    headers = {"Authorization": "Bearer " + token} if token else {}
    mailbox = Mailbox(Path(request["root"]))
    key = continuity_key(peer)
    source = request["source"]
    if request.get("origin"):
        from host_adapters import ChatRef
        source = json.dumps(ChatRef.from_peer(request["origin"]).to_dict(), sort_keys=True, separators=(",", ":"))
    options = {"proxy": request["proxy_url"]} if request.get("proxy_url") else {}
    if request.get("local", {}).get("network", {}).get("mode") == "paired_ip":
        from network_transport import build_http_options
        options = build_http_options(request["local"], peer)
    async with httpx.AsyncClient(headers=headers, timeout=45, trust_env=False, follow_redirects=False, **options) as http:
        card = await A2ACardResolver(http, base_url=url).get_agent_card()
        if token and any(origin(interface.url) != origin(url) for interface in card.supported_interfaces):
            raise ValueError("Agent Card 指向其他站点，未转送访问令牌")
        client = ClientFactory(ClientConfig(
            httpx_client=http, streaming=False, polling=True,
            supported_protocol_bindings=["JSONRPC"], accepted_output_modes=["text/plain"],
        )).create(card)
        from message_delivery import SKILL_ID
        capability = any(skill.id == SKILL_ID for skill in card.skills)
        automatic = False
        if request.get("local", {}).get("platform") == "minis":
            from collaboration_client import validate_central
            try:
                validate_central(request["local"], {**peer, "local_a2a": True})
            except Exception:
                pass
            else:
                automatic = True
                if capability:
                    peer = request["peer"] = {**peer, "local_a2a": True}
        if request["mode"] in {"send", "status"} and (request.get("message_mode") or automatic and capability):
            request["message_mode"] = True
            if not capability:
                raise ValueError("central server has not enabled conversation messages")
            if any(origin(interface.url) != origin(url) for interface in card.supported_interfaces):
                raise ValueError("conversation capability changes the authenticated origin")
            return await exchange_message(request, client, http)
        if request["mode"] == "task-status":
            try:
                return task_result(await client.get_task(GetTaskRequest(id=request["task_id"])), peer["alias"])
            except TaskNotFoundError:
                return {"state": "expired", "to": peer["alias"], "task_id": request["task_id"],
                        "error": "任务已过期或不可访问，请核对对方原聊天"}
        previous = mailbox.outgoing(source, key) if request["mode"] != "task-status" else {"context_id": "", "task_id": ""}
        if not previous["task_id"] and request["mode"] != "task-status":
            previous = mailbox.outgoing(source, url)
            if previous["task_id"]:
                mailbox.save_outgoing(source, key, previous["context_id"], previous["task_id"])
                mailbox.forget_outgoing(source, url)
        task = None
        if previous["task_id"]:
            try:
                task = await client.get_task(GetTaskRequest(id=previous["task_id"]))
            except TaskNotFoundError:
                # Old results may expire on either device. No new message was
                # sent yet, so a fresh user-requested task is safe to start.
                mailbox.forget_outgoing(source, key)
                previous = {"context_id": "", "task_id": ""}
                if request["mode"] == "status":
                    return {"state": "expired", "to": peer["alias"], "error": "上次任务已过期，不再保留结果"}
        if request["mode"] == "status":
            return task_result(task, peer["alias"]) if task else {
                "state": "ready", "to": peer["alias"], "agent": card.name,
            }
        if task and task.status.state not in TERMINAL | INTERRUPTED:
            return {"state": "not_sent", "error": "该伙伴仍有本聊天的未完成任务，请先查询状态",
                    "task_id": task.id}
        message = new_text_message(
            request["text"], role=Role.ROLE_USER,
            context_id=previous["context_id"] or None,
            task_id=(task.id if task and task.status.state in INTERRUPTED
                     and not (managed(request.get("local", {})) and peer.get("local_a2a")) else None),
        )
        message.message_id = request["message_id"]
        if request.get("origin"):
            from host_adapters import ChatRef
            message.metadata.update({"collaboration_origin": ChatRef.from_peer(request["origin"]).to_dict()})
        if managed(request.get("local", {})) and request.get("wait_idle"):
            message.metadata.update({"collaboration_wait_idle": request["wait_idle"]})
        pending = None
        job_id = None
        if request.get("background"):
            from result_watcher import PendingResults
            pending = PendingResults(Path(request["root"]))
            job_id = pending.prepare(request["origin"], peer, message.message_id)
            request["job_id"] = job_id
        try:
            request["attempted"] = True
            outgoing = SendMessageRequest(message=message)
            if pending:
                outgoing.configuration.return_immediately = True
            async for event in client.send_message(outgoing):
                if event.HasField("message"):
                    request["task_received"] = True
                    if pending:
                        pending.mark_inline(job_id)
                    return {"state": "completed", "to": peer["alias"], "reply": get_message_text(event.message)}
                if event.HasField("task"):
                    request["task_received"] = True
                    task = event.task
            if task is None:
                raise RuntimeError("A2A 未返回消息或任务")
            mailbox.save_outgoing(source, key, task.context_id, task.id)
            if pending:
                if task.status.state in TERMINAL | INTERRUPTED:
                    pending.mark_inline(job_id)
                    return task_result(task, peer["alias"])
                pending.bind(job_id, task.id, task.context_id)
                return {**task_result(task, peer["alias"]), "background": True, "job_id": job_id,
                        "notice": "结果由后台自动送回本次发件原聊天，无需轮询或重复派题"}
        except BaseException as error:
            if pending:
                if failure_state(request, error) == "not_sent":
                    pending.mark_not_sent(job_id)
                else:
                    pending.mark_uncertain(job_id)
            raise
        until = time.monotonic() + request["timeout"]
        while task.status.state not in TERMINAL | INTERRUPTED:
            if time.monotonic() >= until:
                break
            await asyncio.sleep(1)
            task = await client.get_task(GetTaskRequest(id=task.id))
        return task_result(task, peer["alias"])


def main():
    request = json.load(sys.stdin)
    try:
        result = asyncio.run(exchange(request))
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return 0
    except Exception as error:
        state = failure_state(request, error)
        if request.get("message_mode"):
            from message_delivery import failure_text, message_summary
            if request.get("mode") == "status":
                state = "uncertain"
            print(json.dumps({"state": state,
                              "error": failure_text(request["peer"]["alias"], message_summary(request.get("text", "")), state)},
                             ensure_ascii=False), flush=True)
            return 2
        print(json.dumps({
            "state": state,
            "error": (("配置、授权或连接前检查未通过，未发送（" if state == "not_sent" else "A2A 连接或协议未确认（")
                      + type(error).__name__ + "），请检查地址、凭据和原任务"),
            **({"job_id": request["job_id"]} if request.get("job_id") else {}),
        }, ensure_ascii=False), flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
