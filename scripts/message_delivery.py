"""Conversation messages and bounded, machine-only delivery receipts."""
import hashlib
import json

from access_policy import chat_ref, compact, registration_alias
from workbuddy import BridgeError


MODE = "message"
SKILL_ID = "conversation-message-v1"
MAX_BODY_BYTES = 65536
MAX_WIRE_BYTES = MAX_BODY_BYTES * 6 + 4096
RECEIPT_STATES = frozenset({"waiting", "accepted", "queued", "uncertain", "not_sent"})
RECEIPT_STAGES = frozenset({"relay", "native_host", "host_queue", "minis_scheduler", "external_service"})


def business_payload(source, text):
    if not isinstance(text, str) or not text.strip() or len(text.encode("utf8")) > MAX_BODY_BYTES:
        raise BridgeError("消息正文须非空且不超过64KiB")
    chat_ref(source)
    alias = registration_alias(source.get("alias"))
    return {"message": text, "source": source["software"], "source_conversation": alias}


def encode_payload(payload):
    if not isinstance(payload, dict) or set(payload) != {"message", "source", "source_conversation"}:
        raise BridgeError("业务来信字段不完整")
    text = payload["message"]
    if not isinstance(text, str) or not text.strip() or len(text.encode("utf8")) > MAX_BODY_BYTES:
        raise BridgeError("业务正文超过允许范围")
    if not isinstance(payload["source"], str) or not payload["source"] or len(payload["source"]) > 200:
        raise BridgeError("消息来源软件无效")
    registration_alias(payload["source_conversation"])
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    if len(encoded.encode("utf8")) > MAX_WIRE_BYTES:
        raise BridgeError("业务消息包装超过上限")
    return encoded


def request_hash(target, text):
    return hashlib.sha256(compact({"target": chat_ref(target), "message": text}).encode("utf8")).hexdigest()


def normalize_receipt(value):
    if not isinstance(value, dict) or value.get("state") not in RECEIPT_STATES:
        raise BridgeError("收件回执无效", "uncertain")
    if value.get("stage") not in RECEIPT_STAGES:
        raise BridgeError("收件回执位置无效", "uncertain")
    state, stage = value["state"], value["stage"]
    if state in {"accepted", "queued"} and stage == "relay":
        raise BridgeError("中转记录不能确认目标收件", "uncertain")
    if state == "accepted" and stage != "native_host":
        raise BridgeError("该回执只确认排队", "uncertain")
    if state == "queued" and stage not in {"host_queue", "minis_scheduler", "external_service"}:
        raise BridgeError("排队位置无效", "uncertain")
    result = {"state": state, "stage": stage}
    native_id = value.get("native_id")
    if native_id is not None:
        if not isinstance(native_id, str) or not native_id or len(native_id) > 200 or any(ord(c) < 32 for c in native_id):
            raise BridgeError("原生收件标记无效", "uncertain")
        result["native_id"] = native_id
    return result


def native_receipt(peer, result):
    if not isinstance(result, dict):
        return {"state": "uncertain", "stage": "relay"}
    if result.get("state") in {"uncertain", "not_sent"}:
        return {"state": result["state"], "stage": "relay"}
    if "receipt" in result:
        receipt = normalize_receipt(result["receipt"])
        if peer["software"] == "minis" and (receipt["state"], receipt["stage"]) != ("queued", "minis_scheduler"):
            raise BridgeError("Minis当前只确认调度排队", "uncertain")
        return receipt
    if result.get("state") == "accepted":
        if peer["software"] == "minis" and result.get("route") == "follow-up" and result.get("scheduled_task_id"):
            return normalize_receipt({"state": "queued", "stage": "minis_scheduler", "native_id": result["scheduled_task_id"]})
        if peer["software"] == "codex" and result.get("route") in {"start", "steer"} and result.get("turn_id"):
            return normalize_receipt({"state": "accepted", "stage": "native_host", "native_id": result["turn_id"]})
        if peer["software"] == "workbuddy" and result.get("route") == "native-queue":
            return {"state": "queued", "stage": "host_queue"}
    return {"state": "uncertain", "stage": "relay"}


def message_summary(text):
    return " ".join(text.split())[:32] if isinstance(text, str) else ""


def failure_text(alias, summary, state="not_sent"):
    target = " ".join(str(alias).split())[:40]
    excerpt = message_summary(summary)
    label = "送达未确认" if state in {"uncertain", "waiting", "expired"} else "发送失败"
    subject = "发给 " + target + (" 的「" + excerpt + "」" if excerpt else "")
    return subject + label + "。"
