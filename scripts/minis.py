"""Minis native session identity and one-shot follow-up adapter."""
import json
import os
import re
import subprocess

from workbuddy import BridgeError


_MAX_PARENT_HOPS = 16
_SAFE_ID = re.compile(r"^[A-Za-z0-9._:-]{1,200}$")


def _run(argv, runner):
    """Run a CLI command without a shell and with bounded output time."""
    try:
        return runner(
            argv, capture_output=True, text=True, encoding="utf-8", timeout=30,
        )
    except subprocess.TimeoutExpired as error:
        raise BridgeError("Minis CLI 等待超时，请先核对任务，勿自动重发", "uncertain") from error
    except Exception as error:
        # Never echo native stderr/stdout: it may contain prompts or local data.
        raise BridgeError("Minis CLI 无法确认执行结果（" + type(error).__name__ + "）", "uncertain") from error


def _json_result(result, *, uncertain=False):
    try:
        value = json.loads(result.stdout)
    except (TypeError, ValueError):
        state = "uncertain" if uncertain else "not_sent"
        raise BridgeError("Minis CLI 回执不是有效 JSON", state)
    if not isinstance(value, dict):
        state = "uncertain" if uncertain else "not_sent"
        raise BridgeError("Minis CLI 回执格式不受支持", state)
    return value


def _session_record(value):
    """Decode the pinned CLI's status envelope: {ok,tool,action,data}.

    The pinned build wraps status in {ok,tool,action,data}; some live builds
    compact it to {ok,data}. In either shape, the session fields are in data.
    """
    if not isinstance(value, dict):
        return None
    if value.get("ok") is not True:
        return None
    if value.get("tool") not in (None, "minis-sessions-cli"):
        return None
    if value.get("action") not in (None, "status"):
        return None
    record = value.get("data")
    return record if isinstance(record, dict) else None


def _inspect_session(session_id, runner):
    result = _run(["minis-sessions-cli", "status", "--id", session_id, "--compact"], runner)
    if getattr(result, "returncode", 1) != 0:
        raise BridgeError("Minis 未确认该会话存在或可读取")
    payload = _json_result(result)
    record = _session_record(payload)
    if record is None:
        raise BridgeError("Minis 会话状态格式不受支持")
    reported_id = record.get("session_id")
    if reported_id != session_id:
        raise BridgeError("Minis 未能确认请求的会话身份")
    return record


def _identity_chain(session_id, runner):
    """Resolve a hidden child to its trusted top-level conversation."""
    current = session_id
    seen = set()
    for _ in range(_MAX_PARENT_HOPS):
        if current in seen:
            raise BridgeError("Minis 会话父级关系存在循环，无法确认身份")
        seen.add(current)
        record = _inspect_session(current, runner)
        parent = record.get("parent_session_id")
        if parent in (None, ""):
            return current, record
        if not isinstance(parent, str) or not _SAFE_ID.fullmatch(parent) or parent == current:
            raise BridgeError("Minis 会话父级身份无效")
        current = parent
    raise BridgeError("Minis 会话父级层级过深，无法确认身份")


def resolve_conversation(environ=os.environ, runner=subprocess.run):
    """Return the current trusted root session ID from Minis' environment."""
    session_id = environ.get("MINIS_CHAT_SESSION_ID")
    if not isinstance(session_id, str) or not _SAFE_ID.fullmatch(session_id):
        raise BridgeError("未识别到有效的 Minis 当前会话身份")
    conversation_id, _ = _identity_chain(session_id, runner)
    return conversation_id


def _busy_state(record):
    """Return the pinned CLI's authoritative is_running boolean, if present."""
    value = record.get("is_running")
    return value if isinstance(value, bool) else None


def _created_task(payload, expected_label, session_id):
    """Validate minis-scheduled create's exact {created, deliveryTarget} reply."""
    created = payload.get("created")
    delivery = payload.get("deliveryTarget")
    if not isinstance(created, dict) or not isinstance(delivery, dict):
        return None
    task_id = created.get("id")
    if not isinstance(task_id, str) or not task_id:
        return None
    if created.get("label") != expected_label:
        return None
    if created.get("target") != "APPEND_TO:" + session_id:
        return None
    # The pinned CLI echoes the resolved delivery destination. Verify it too,
    # so a successful process exit alone cannot be mistaken for our route.
    if delivery.get("target") != "follow-up" or delivery.get("defaulted") is not False:
        return None
    if delivery.get("sessionId") != session_id:
        return None
    return task_id


def _list_task(list_result, task_id):
    """Find one task by exact ID in minis-scheduled list's raw JSON shape."""
    if getattr(list_result, "returncode", 1) != 0:
        return None, False
    try:
        payload = json.loads(list_result.stdout)
    except (TypeError, ValueError):
        return None, False
    if not isinstance(payload, dict) or not isinstance(payload.get("tasks"), list):
        return None, False
    matches = [task for task in payload["tasks"]
               if isinstance(task, dict) and task.get("id") == task_id]
    if len(matches) > 1:
        return None, False
    if not matches:
        return None, True
    return matches[0], True


def cleanup_task(task_id, message_id, conversation_id, runner=subprocess.run, expired=False):
    """Remove only a finished or expired, uniquely identified A2A one-shot task.

    On normal completion, the caller must already know the original message
    task finished; this method additionally checks the native completion fields.
    On expiry, it may cancel a still-enabled, never-fired alarm after checking
    the exact owned task markers and an idle target. Minis' delete cancels the
    future alarm and removes its row; it cannot stop a model run already handed
    off, so claimed/in-flight runs stay pending. No prompt/error text is returned.
    """
    if (not isinstance(task_id, str) or not _SAFE_ID.fullmatch(task_id)
            or not isinstance(message_id, str) or not _SAFE_ID.fullmatch(message_id)
            or not isinstance(conversation_id, str) or not _SAFE_ID.fullmatch(conversation_id)):
        return {"state": "pending"}

    try:
        session = _inspect_session(conversation_id, runner)
    except BridgeError:
        return {"state": "pending"}
    # True means active; missing/non-boolean means the CLI did not prove idle.
    if session.get("is_running") is not False:
        return {"state": "pending"}

    try:
        listed = runner(
            ["minis-scheduled", "list", "--compact"], capture_output=True,
            text=True, encoding="utf-8", timeout=30,
        )
    except Exception:
        return {"state": "pending"}
    task, list_proven = _list_task(listed, task_id)
    if not list_proven:
        return {"state": "pending"}
    if task is None:
        return {"state": "missing"}

    # `--after` is one-shot. These fields come from taskJson in the pinned
    # source: target is APPEND_TO:<session>, and firedCount is the relative
    # trigger's claimed-fire counter. A disabled task with a claimed fire but
    # no lastFiredAt may be between alarm claim and completion; never delete it.
    expected_label = "a2a-" + message_id
    fired_count = task.get("firedCount")
    markers_match = (task.get("label") == expected_label
                     and task.get("target") == "APPEND_TO:" + conversation_id
                     and task.get("trigger") == "once")
    if not markers_match or isinstance(fired_count, bool) or not isinstance(fired_count, int):
        return {"state": "pending"}

    completed = (task.get("enabled") is False and fired_count >= 1
                 and isinstance(task.get("lastFiredAt"), (int, float))
                 and task.get("lastResultSessionId") == conversation_id)
    expired_unfired = expired and task.get("enabled") is True and fired_count == 0
    if not (completed or expired_unfired):
        return {"state": "pending"}

    try:
        deleted = runner(
            ["minis-scheduled", "delete", "--id", task_id], capture_output=True,
            text=True, encoding="utf-8", timeout=30,
        )
    except Exception:
        return {"state": "pending"}
    if getattr(deleted, "returncode", 1) != 0:
        return {"state": "pending"}
    try:
        receipt = json.loads(deleted.stdout)
    except (TypeError, ValueError):
        return {"state": "pending"}
    if not isinstance(receipt, dict) or receipt.get("deleted") != task_id:
        return {"state": "pending"}
    return {"state": "removed"}


def invoke(local, peer, mode, text="", message_id=None, source="", wait_idle=0,
           environ=os.environ, runner=subprocess.run):
    """Inspect a Minis conversation or schedule one follow-up in that chat.

    ``local``/``source``/``wait_idle`` are accepted for send.py compatibility.
    Minis follow-ups are asynchronous and do not claim that a model run finished.
    """
    del local, source, wait_idle
    target = peer.get("conversation_id") if isinstance(peer, dict) else None
    if not isinstance(target, str) or not _SAFE_ID.fullmatch(target):
        raise BridgeError("Minis 伙伴缺少有效的原聊天地址")

    # A peer address must name the actual root chat. Hidden children are never
    # promoted here; only the local sender identity may follow trusted parents.
    record = _inspect_session(target, runner)
    if record.get("parent_session_id") not in (None, ""):
        raise BridgeError("Minis 目标是隐藏子会话，不能作为登记原聊天")

    if mode == "status":
        result = {"state": "ready", "exists": True}
        busy = _busy_state(record)
        if busy is not None:
            result["busy"] = busy
        return result
    if mode != "send":
        raise BridgeError("Minis 不支持该原生操作")
    if not isinstance(text, str) or not text.strip():
        raise BridgeError("正文不能为空")
    if not isinstance(message_id, str) or not _SAFE_ID.fullmatch(message_id):
        raise BridgeError("Minis 原生投递需要有效的唯一消息号")

    label_id = re.sub(r"[^A-Za-z0-9._:-]", "-", message_id)
    argv = [
        "minis-scheduled", "create", "--target", "follow-up", "--session", target,
        "--after", "1s", "--label", "a2a-" + label_id, "--prompt", text,
    ]
    # From this point the scheduler may have accepted the task even when the
    # process exits abnormally or its receipt is unreadable. Never retry here.
    try:
        result = runner(argv, capture_output=True, text=True, encoding="utf-8", timeout=30)
    except Exception as error:
        raise BridgeError("Minis 定时任务结果不明，请核对原任务，勿自动重发", "uncertain") from error
    if getattr(result, "returncode", 1) != 0:
        raise BridgeError("Minis 定时任务未返回可确认的接纳结果，请核对后再处理", "uncertain")
    payload = _json_result(result, uncertain=True)
    task_id = _created_task(payload, "a2a-" + label_id, target)
    if task_id is None:
        raise BridgeError("Minis 定时任务回执缺少任务号，请核对原任务，勿自动重发", "uncertain")
    return {"state": "accepted", "scheduled_task_id": task_id, "route": "follow-up"}
