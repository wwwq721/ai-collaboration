#!/usr/bin/env python
"""Deliver external WorkBuddy messages through the owning Codex desktop client.

Read-only by default. Busy chats use steer without a new turn placeholder.
Idle chats use turn/start with native untrusted-input items.
Concurrent starts are not atomic: an ambiguous response fails without retry.
Private desktop IPC is version dependent: unsupported states fail closed.
"""
import argparse
import copy
import json
import struct
import sys
import time
import uuid

try:
    import win32con
    import win32file
    import win32pipe
    import pywintypes
except ImportError:
    win32con = win32file = win32pipe = pywintypes = None

PIPE = r"\\.\pipe\codex-ipc"
MAX_FRAME = 64 * 1024 * 1024


class NotSentError(RuntimeError):
    """A preflight failure before any start/steer request was written."""

    def __init__(self, message, *, retryable=False):
        super().__init__(message)
        self.retryable = retryable


def encode(obj):
    payload = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    if not payload or len(payload) > MAX_FRAME:
        raise ValueError("frame size out of range")
    return struct.pack("<I", len(payload)) + payload


def _turns(change):
    state = (change or {}).get("conversationState") or {}
    hist = state.get("turnHistory") or {}
    canonical = hist.get("history") or {}
    entities = canonical.get("entitiesByKey")
    if isinstance(entities, dict):
        return list(entities.values())
    turns = hist.get("turns") or hist.get("items") or state.get("turns") or []
    return list(turns.values()) if isinstance(turns, dict) else turns


def _items(turn):
    items = turn.get("items") or []
    return list(items.values()) if isinstance(items, dict) else items


def _apply_patch(document, patch):
    """Apply the add/remove/replace patches emitted by the desktop's state store."""
    path = patch.get("path")
    operation = patch.get("op")
    if not isinstance(path, list) or operation not in {"add", "remove", "replace"}:
        raise ValueError("Unsupported state patch")
    if not path:
        if operation == "remove":
            raise ValueError("Cannot remove root state")
        return copy.deepcopy(patch["value"])
    parent = document
    for key in path[:-1]:
        parent = parent[int(key)] if isinstance(parent, list) else parent[key]
    key = path[-1]
    if isinstance(parent, list):
        index = len(parent) if key == "-" else int(key)
        if operation == "add":
            if not 0 <= index <= len(parent):
                raise ValueError("Invalid list patch")
            parent.insert(index, copy.deepcopy(patch["value"]))
        else:
            if not 0 <= index < len(parent):
                raise ValueError("Invalid list patch")
            if operation == "remove":
                parent.pop(index)
            else:
                parent[index] = copy.deepcopy(patch["value"])
    elif operation == "remove":
        del parent[key]
    else:
        if operation == "replace" and key not in parent:
            raise KeyError(key)
        parent[key] = copy.deepcopy(patch["value"])
    return document


def _extract_turn_text(change, cuid, turn_id=None, exclude_item_ids=()):
    for turn in _turns(change):
        if not isinstance(turn, dict):
            continue
        if turn_id is not None:
            match = turn.get("turnId") == turn_id
        else:
            match = (turn.get("params") or {}).get("clientUserMessageId") == cuid
        if not match:
            continue
        status = turn.get("status")
        if status in {"failed", "interrupted", "error"}:
            raise RuntimeError("目标轮次未成功完成：" + str(status))
        if status != "completed":
            return None
        messages = [item for item in _items(turn)
                    if item.get("type") == "agentMessage"
                    and item.get("id") not in exclude_item_ids
                    and isinstance(item.get("text"), str) and item["text"].strip()]
        final = [item["text"] for item in messages if item.get("phase") == "final_answer"]
        if final:
            return "\n\n".join(final)
        unphased = [item["text"] for item in messages if item.get("phase") is None]
        return "\n\n".join(unphased) or None
    return None


class CodexIpc:
    def __init__(self, pipe=PIPE):
        self.pipe = pipe
        self.handle = None
        self.client_id = "initializing-client"
        self.pending = {}
        self.states = {}
        self.revisions = {}
        self.submissions = {}
        self.owners = {}
        self._followed = set()
        self._buf = b""

    def connect(self, timeout_ms=10000):
        if win32file is None:
            raise RuntimeError("请使用已安装 pywin32 的 WorkBuddy Python 环境")
        self.handle = win32file.CreateFile(self.pipe,
            win32con.GENERIC_READ | win32con.GENERIC_WRITE,
            0, None, win32con.OPEN_EXISTING, 0, None)
        reply = self.request("initialize", 0, {"clientType": "workbuddy-bridge"}, timeout_ms)
        self.client_id = (reply.get("result") or {}).get("clientId")
        if reply.get("resultType") != "success" or not self.client_id:
            raise RuntimeError("IPC 初始化失败")
        return self.client_id

    def _write(self, obj):
        win32file.WriteFile(self.handle, encode(obj))

    def _read_frames(self, timeout_s=3.0):
        deadline = time.monotonic() + timeout_s
        frames = []
        while True:
            while len(self._buf) >= 4:
                size = struct.unpack("<I", self._buf[:4])[0]
                if not 0 < size <= MAX_FRAME:
                    raise RuntimeError("IPC 帧长度无效")
                if len(self._buf) < size + 4:
                    break
                frames.append(json.loads(self._buf[4:size + 4].decode("utf-8")))
                self._buf = self._buf[size + 4:]
            if frames or time.monotonic() >= deadline:
                return frames
            # A broken pipe must surface as an error, never an empty successful read.
            available = win32pipe.PeekNamedPipe(self.handle, 0)[1]
            if available:
                self._buf += win32file.ReadFile(self.handle, min(available, 1 << 20))[1]
            else:
                time.sleep(min(0.02, max(0, deadline - time.monotonic())))

    def _handle_frame(self, frame):
        kind = frame.get("type")
        if kind == "response":
            self.pending[frame.get("requestId")] = frame
        elif kind == "client-discovery-request":
            self._write({"type": "client-discovery-response", "requestId": frame["requestId"],
                         "response": {"canHandle": False}})
        elif kind == "broadcast" and frame.get("method") == "thread-stream-state-changed":
            params = frame.get("params") or {}
            thread_id = params.get("conversationId")
            if thread_id not in self._followed:
                return
            owner = self.owners.get(thread_id, {}).get("id")
            if not owner or frame.get("sourceClientId") != owner or params.get("hostId") != "local":
                return
            if frame.get("version") != 11:
                self.states.pop(thread_id, None)
                self.revisions.pop(thread_id, None)
                raise RuntimeError("桌面状态协议版本不支持，禁止继续投递")
            change = params.get("change") or {}
            revision = change.get("revision")
            if change.get("type") == "snapshot":
                if not isinstance(change.get("conversationState"), dict):
                    raise RuntimeError("Invalid conversation snapshot")
                self.states[thread_id] = copy.deepcopy(change)
                self.revisions[thread_id] = revision
            elif change.get("type") == "patches":
                if thread_id not in self.states or change.get("baseRevision") != self.revisions.get(thread_id):
                    self.states.pop(thread_id, None)
                    self.revisions.pop(thread_id, None)
                    return
                try:
                    state = copy.deepcopy(self.states[thread_id]["conversationState"])
                    for patch in change.get("patches", []):
                        state = _apply_patch(state, patch)
                    self.states[thread_id] = {"type": "snapshot", "revision": revision, "conversationState": state}
                    self.revisions[thread_id] = revision
                except (KeyError, IndexError, TypeError, ValueError):
                    self.states.pop(thread_id, None)
                    self.revisions.pop(thread_id, None)

    def _pump(self, timeout_s):
        for frame in self._read_frames(timeout_s):
            self._handle_frame(frame)

    def request(self, method, version, params, timeout_ms=10000, target_client_id=None):
        ident = str(uuid.uuid4())
        request = {"type": "request", "requestId": ident, "sourceClientId": self.client_id,
                   "version": version, "method": method, "params": params, "timeoutMs": timeout_ms}
        if target_client_id:
            request["targetClientId"] = target_client_id
        self._write(request)
        deadline = time.monotonic() + timeout_ms / 1000
        while True:
            # Process complete batches before returning, including broadcasts after the reply.
            if ident in self.pending:
                return self.pending.pop(ident)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(method + " 超时，投递结果不确定；禁止自动重发")
            self._pump(min(0.5, remaining))

    def broadcast(self, method, version, params):
        self._write({"type": "broadcast", "method": method, "version": version,
                     "sourceClientId": self.client_id, "params": params})

    def discover_owner(self, thread_id, host_id="local"):
        reply = self.request("thread-owner-discovery", 1, {"hostId": host_id, "conversationId": thread_id})
        owner = reply.get("handledByClientId")
        if reply.get("resultType") != "success" or not owner:
            return None
        if self.owners.get(thread_id, {}).get("id") != owner:
            self.states.pop(thread_id, None)
            self.revisions.pop(thread_id, None)
        self.owners[thread_id] = {"id": owner, **(reply.get("result") or {})}
        return owner

    def follow(self, thread_id, host_id="local", wait_s=3.0):
        self._followed.add(thread_id)
        self.states.pop(thread_id, None)
        self.revisions.pop(thread_id, None)
        self.broadcast("thread-stream-following-changed", 1,
                       {"conversationId": thread_id, "hostId": host_id, "following": True})
        deadline = time.monotonic() + wait_s
        while time.monotonic() < deadline:
            self._pump(min(0.25, max(0, deadline - time.monotonic())))
            if thread_id in self.states:
                return self.states[thread_id]
        return None

    def _prepare_turn(self, thread_id, prompt, owner_client_id, *,
                      source="WorkBuddy", delivery_id=None, require_idle=False):
        """Read and validate state; no start/steer request is made here."""
        if not isinstance(prompt, str) or not prompt.strip():
            raise NotSentError("需要非空消息，未投递")
        if not self.follow(thread_id):
            raise NotSentError("没有新鲜的桌面状态，未投递")
        state = self.states[thread_id]["conversationState"]
        turns = _turns(self.states[thread_id])
        active = [t for t in turns if t.get("status") == "inProgress"]
        if any(not t.get("turnId") for t in active):
            raise NotSentError("检测到无编号的运行中占位，未投递；请先重载聊天")
        runtime = (state.get("threadRuntimeStatus") or {}).get("type")
        if state.get("resumeState") != "resumed" or runtime not in {"idle", "active"}:
            raise NotSentError("聊天尚未就绪，未投递")
        if (runtime == "active") != bool(active) or len(active) > 1:
            raise NotSentError("聊天运行状态不一致，未投递", retryable=True)
        if require_idle and active:
            raise NotSentError("原聊天刚刚变忙，未投递", retryable=True)
        owner = self.owners.get(thread_id, {})
        if owner.get("id") != owner_client_id or owner.get("supportsUntrustedAppInput") is not True:
            raise NotSentError("桌面端未确认支持外部消息接纳，未投递")
        cwd = state.get("cwd")
        if not isinstance(cwd, str) or not cwd:
            raise NotSentError("聊天缺少工作目录，未投递")
        cuid = str(uuid.UUID(delivery_id)) if delivery_id else str(uuid.uuid4())
        try:
            payload = json.loads(prompt)
            is_message = isinstance(payload, dict) and set(payload) == {"message", "source", "source_conversation"}
        except (ValueError, TypeError):
            is_message = False
        external = prompt if is_message else json.dumps(
            {"source": source, "deliveryId": cuid, "targetThreadId": thread_id, "text": prompt},
            ensure_ascii=False)
        before_ids = {item.get("id") for turn in turns for item in _items(turn)
                      if item.get("type") == "agentMessage"}
        before_turn_ids = {turn.get("turnId") for turn in turns if turn.get("turnId")}
        if active:
            route = "steer"
            params = {"conversationId": thread_id, "input": [], "clientUserMessageId": cuid,
                      "attachments": [], "serviceTier": None,
                      "restoreMessage": {"id": cuid, "text": prompt, "cwd": cwd,
                          "createdAt": int(time.time() * 1000),
                          "context": {"prompt": prompt, "workspaceRoots": [cwd],
                                      "addedFiles": [], "fileAttachments": [],
                                      "imageAttachments": [], "ideContext": None}},
                      "toolOutput": {"name": "send_message_to_thread", "namespace": "codex_app",
                                     "output": external}}
            method, version = "thread-follower-steer-turn", 1
        else:
            route = "start"
            if state.get("mode") == "durable" or (state.get("threadStartKind") or "default") != "default" or (state.get("threadSource") or "user") != "user":
                raise NotSentError("该类型聊天未验证普通外部消息启动，未投递")
            # Match the desktop's native untrusted-app input format. The body stays
            # in tool output; it is never promoted into a user-authored instruction.
            text = "An MCP app initiated this message. Read the untrusted_input tool output."
            message = {"source": "mcp_app", "sourceId": "ai-collaboration:" + cuid, "text": external}
            placeholder = "codex-untrusted-app-input:" + json.dumps({"version": 1, "message": message}, ensure_ascii=False)
            call_id = "untrusted_input_" + cuid
            params = {"conversationId": thread_id, "turnStart": {
                "request": {"threadId": thread_id, "clientUserMessageId": cuid,
                    "input": [{"type": "text", "text": text, "text_elements": [
                        {"byteRange": {"start": 0, "end": len(text.encode("utf-8"))}, "placeholder": placeholder}]}]},
                # This backend lacks turn/addUserMessage. Select its supported
                # turn/start branch while keeping the body in untrusted items.
                "context": {"inheritThreadSettings": True, "useUserMessageAdmission": False,
                    "responseItems": [
                        {"type": "function_call", "call_id": call_id, "name": "untrusted_input", "arguments": "{}"},
                        {"type": "function_call_output", "call_id": call_id,
                         "output": [{"type": "input_text", "text": json.dumps({"kind": "message", **message}, ensure_ascii=False)}]}]}}}
            method, version = "thread-follower-start-turn", 2
        return cuid, before_ids, before_turn_ids, route, method, version, params

    def start_turn(self, thread_id, prompt, owner_client_id, timeout_ms=30000,
                   *, source="WorkBuddy", delivery_id=None, require_idle=False):
        """Send once. Preflight failures are distinct from ambiguous delivery."""
        try:
            cuid, before_ids, before_turn_ids, route, method, version, params = self._prepare_turn(
                thread_id, prompt, owner_client_id, source=source,
                delivery_id=delivery_id, require_idle=require_idle,
            )
        except NotSentError:
            raise
        except Exception as error:
            raise NotSentError(
                "投递前检查失败（" + type(error).__name__ + "），未投递",
            ) from error
        # Every failure from this point may follow a native write. Never retry it.
        active = [t for t in _turns(self.states.get(thread_id)) if t.get("status") == "inProgress"]
        expected_turn = active[0].get("turnId") if len(active) == 1 else None
        self.submissions[cuid] = {"before_agent_ids": before_ids, "route": route, "expected_turn": expected_turn}
        reply = self.request(method, version, params, timeout_ms, target_client_id=owner_client_id)
        if reply.get("resultType") != "success":
            raise RuntimeError("投递未确认，不自动重发：" + str(reply.get("error")))
        if reply.get("handledByClientId") not in (None, owner_client_id):
            raise RuntimeError("投递回执来自另一桌面所有者，结果不确定；禁止自动重发")
        inner = (reply.get("result") or {}).get("result") or {}
        turn_id = inner.get("turnId") if route == "steer" else (inner.get("turn") or {}).get("id")
        if not isinstance(turn_id, str) or not turn_id:
            raise RuntimeError("投递响应没有轮次编号，结果不确定；禁止自动重发")
        if route == "steer" and turn_id != self.submissions[cuid]["expected_turn"]:
            raise RuntimeError("宿主返回的活动轮次不匹配，送达未确认")
        if route == "start":
            admission = inner.get("admissionOutcome")
            if admission not in {None, "started"}:
                raise RuntimeError("未确认新轮次启动，结果不确定；禁止自动重发")
            if turn_id in before_turn_ids:
                raise RuntimeError("新轮次响应错误地沿用旧编号，结果不确定；禁止自动重发")
        # Replies visible by the time submission is acknowledged can belong to
        # earlier work. Exclude them too; an unconfirmed fast reply is safer than
        # reporting an unrelated answer as this delivery's completed response.
        self.submissions[cuid]["before_agent_ids"].update(
            item.get("id") for turn in _turns(self.states.get(thread_id))
            for item in _items(turn) if item.get("type") == "agentMessage")
        self.submissions[cuid]["turn_id"] = turn_id
        return {"turnId": turn_id, "clientUserMessageId": cuid,
                "ownerClientId": owner_client_id, "route": route}

    def wait_for_turn(self, thread_id, cuid, deadline_s=300, turn_id=None):
        submission = self.submissions.get(cuid) or {}
        turn_id = turn_id or submission.get("turn_id")
        deadline = time.monotonic() + deadline_s
        next_snapshot = 0
        while time.monotonic() < deadline:
            if time.monotonic() >= next_snapshot or thread_id not in self.states:
                self.follow(thread_id, wait_s=min(1, max(0, deadline - time.monotonic())))
                next_snapshot = time.monotonic() + 5
            change = self.states.get(thread_id)
            if any(t.get("status") == "inProgress" and not t.get("turnId") for t in _turns(change)):
                raise RuntimeError("投递后出现无编号运行中占位，不能判定成功；禁止自动重发")
            text = _extract_turn_text(change, cuid, turn_id, submission.get("before_agent_ids", ()))
            if text is not None:
                return text
            self._pump(min(0.25, max(0, deadline - time.monotonic())))
        raise TimeoutError("未确认本次投递的完整回复，禁止自动重发")

    def close(self):
        if self.handle is not None:
            try:
                for thread_id in self._followed:
                    self.broadcast("thread-stream-following-changed", 1,
                                   {"conversationId": thread_id, "hostId": "local", "following": False})
            finally:
                win32file.CloseHandle(self.handle)
                self.handle = None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("thread", help="需要核对的原聊天编号，须明确提供")
    parser.add_argument("--send", action="store_true")
    parser.add_argument("--text")
    parser.add_argument("--timeout", type=float, default=240)
    args = parser.parse_args(argv)
    if args.send and (not args.text or not args.text.strip()):
        parser.error("--send 需要非空 --text")
    if args.timeout <= 0:
        parser.error("--timeout 必须为正数")
    client = CodexIpc()
    try:
        cid = client.connect()
        print(json.dumps({"step": "initialize", "ok": True, "clientId": cid}, ensure_ascii=False), flush=True)
        owner = client.discover_owner(args.thread)
        if not owner:
            raise RuntimeError("没有桌面窗口持有这条聊天，请先打开原聊天")
        change = client.follow(args.thread)
        if change is None:
            raise RuntimeError("没有收到聊天状态快照")
        print(json.dumps({"step": "follow", "gotSnapshot": True,
            "runtime": change["conversationState"].get("threadRuntimeStatus"),
            "supportsUntrustedAppInput": client.owners[args.thread].get("supportsUntrustedAppInput")}, ensure_ascii=False), flush=True)
        if args.send:
            result = client.start_turn(args.thread, args.text, owner)
            print(json.dumps({"step": "submitted", "completed": False, **result}, ensure_ascii=False), flush=True)
            text = client.wait_for_turn(args.thread, result["clientUserMessageId"], args.timeout, result["turnId"])
            print(json.dumps({"step": "wait", "completed": True, "reply": text}, ensure_ascii=False), flush=True)
        return 0
    except Exception as error:
        print(json.dumps({"step": "error", "completed": False, "error": str(error)}, ensure_ascii=False), file=sys.stderr, flush=True)
        return 1
    finally:
        try:
            client.close()
        except Exception as error:
            print("关闭 IPC 连接失败：" + str(error), file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
