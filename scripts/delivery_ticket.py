"""One-use native delivery capabilities owned by the running service.

No capability key or ticket store is written to disk. Creating another store
does not grant access to the service's store. Clients only consume opaque tickets.
"""
import hashlib
import secrets
import threading
import time

from workbuddy import BridgeError


def managed(local):
    return (local.get("a2a", {}).get("managed_ui") is True
            or local.get("network", {}).get("managed_ui") is True)


def managed_pc(local):
    return managed(local) and local.get("platform") != "minis"


def text_hash(text):
    if not isinstance(text, str):
        raise BridgeError("投递正文无效")
    return hashlib.sha256(text.encode("utf8")).hexdigest()


def _ref(target):
    from host_adapters import ChatRef
    return ChatRef.from_peer(target).to_dict()


class DeliveryTickets:
    def __init__(self, clock=time.monotonic, max_tickets=1024):
        self.clock, self.max_tickets = clock, max_tickets
        self._tickets, self._lock = {}, threading.Lock()

    def issue(self, task_id, target, text, message_id, action="task", allow_busy=False, ttl=30):
        if (not isinstance(task_id, str) or not task_id or not isinstance(message_id, str)
                or not message_id or action not in {"task", "notification"}
                or isinstance(ttl, bool) or not isinstance(ttl, (float, int)) or not 0 < ttl <= 660
                or not isinstance(allow_busy, bool)):
            raise BridgeError("投递许可参数无效")
        binding = {"task_id": task_id, "target": _ref(target), "message_id": message_id,
                   "text_hash": text_hash(text), "action": action, "allow_busy": allow_busy}
        with self._lock:
            now = self.clock()
            self._tickets = {k: v for k, v in self._tickets.items() if v["expires"] > now}
            if len(self._tickets) >= self.max_tickets:
                raise BridgeError("一次性投递许可已满")
            ticket = secrets.token_urlsafe(32)
            self._tickets[ticket] = {**binding, "expires": now + ttl}
        return ticket

    def consume(self, ticket, target, message_id, text_hash, action, task_id=None):
        if not isinstance(ticket, str):
            raise BridgeError("缺少一次性投递许可")
        with self._lock:
            binding = self._tickets.pop(ticket, None)
            if (not binding or binding["expires"] <= self.clock()
                    or binding["target"] != _ref(target) or binding["message_id"] != message_id
                    or binding["text_hash"] != text_hash or binding["action"] != action
                    or binding["task_id"] != task_id):
                raise BridgeError("投递许可已失效或与本次投递不符")
        return {"allowed": True, "allow_busy": binding["allow_busy"],
                "task_id": binding["task_id"], "action": binding["action"]}


def consume_local(local, permit, target, message_id, text):
    """Only the running service can recognize the opaque capability."""
    from collaboration_client import local_request
    if not isinstance(permit, dict) or not isinstance(permit.get("ticket"), str):
        raise BridgeError("原生投递需要后台签发的一次性许可")
    result = local_request(local, "/collaboration/consume", {
        "ticket": permit["ticket"], "task_id": permit.get("task_id"),
        "action": permit.get("action", "task"), "target": _ref(target),
        "message_id": message_id, "text_hash": text_hash(text),
    })
    if result.get("allowed") is not True or not isinstance(result.get("allow_busy"), bool):
        raise BridgeError("后台未确认本次原生投递许可")
    return result
