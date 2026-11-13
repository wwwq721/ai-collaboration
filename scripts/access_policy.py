"""Verified original-chat identities; every connected pair may communicate."""
import ipaddress
import json
import re
from urllib.parse import unquote, urlsplit


def chat_ref(peer):
    if hasattr(peer, "to_dict"):
        peer = peer.to_dict()
    if isinstance(peer, (list, tuple)):
        values = list(peer)
    elif isinstance(peer, dict):
        values = [peer.get(k) for k in ("device_id", "software", "conversation_id")]
    else:
        raise ValueError("原聊天身份格式无效")
    if len(values) != 3 or any(not isinstance(v, str) or not v.strip() or len(v) > 200 or any(ord(c) < 32 for c in v) for v in values):
        raise ValueError("原聊天身份不完整")
    return values


def compact(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def registration_alias(value):
    if (not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_\-\u4e00-\u9fff]{1,40}", value)
            or value.upper().startswith("A2A-") or value.casefold() in {"ui", "agents", "devices", "health", "shutdown", "package"}):
        raise ValueError("别名须为1至40个汉字、英数、下划线或连字符且不能使用保留名称")
    return value


def is_external_a2a(peer):
    return peer.get("transport") == "a2a" or peer.get("software") == "a2a"


def external_a2a_ready(local, peer, alias=None, *, require_verified=True):
    """Check only locally configured egress facts; never learns trust from a URL.

    Independent credentials stay in private local.json. Public verification binds
    both the exact endpoint and complete original chat, and expires on any edit.
    This does not claim the endpoint actually wakes its claimed host conversation.
    """
    if not isinstance(peer, dict) or not is_external_a2a(peer):
        return False
    try:
        ref = chat_ref(peer)
        url = peer.get("url")
        if not isinstance(url, str) or not 1 <= len(url) <= 2048 or any(ord(c) <= 32 or ord(c) == 127 for c in url) or any(c in url for c in ("\\", "?", "#")):
            return False
        endpoint = urlsplit(url)
        host = endpoint.hostname
        port = endpoint.port or 443
        if (endpoint.scheme != "https" or not host or endpoint.username is not None or endpoint.password is not None
                or endpoint.query or endpoint.fragment or not 1 <= port <= 65535 or "%" in host):
            return False
        host = host.rstrip(".").lower()
        if host == "localhost" or host.endswith(".localhost") or "metadata" in host.split(".") or host == "instance-data.ec2.internal":
            return False
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            if "." not in host or host.split(".")[-1].isdigit() or not all(re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label) for label in host.split(".")):
                return False
        else:
            # Private/Tailscale addresses remain valid for explicitly verified
            # remote services. Local, link-local and metadata destinations do not.
            if address.is_loopback or address.is_link_local or address.is_multicast or address.is_unspecified or address.is_reserved or str(address) in {"100.100.100.200", "192.0.0.192", "fd00:ec2::254"}:
                return False
            if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
                mapped = address.ipv4_mapped
                if mapped.is_loopback or mapped.is_link_local or mapped.is_unspecified or mapped.is_multicast or mapped.is_reserved:
                    return False
        path = endpoint.path
        for _ in range(4):
            decoded = unquote(path)
            if decoded == path:
                break
            path = decoded
        if "%" in path or "\\" in path or any(ord(c) < 32 or ord(c) == 127 for c in path):
            return False
        parts = [part.casefold() for part in path.split("/") if part]
        if any(part in {".", ".."} for part in parts) or (parts and parts[0] in {"ui", "devices", "health", "shutdown", "package", "admin", "metadata", "latest"}):
            return False
        config = local.get("a2a", {})
        central = config.get("public_base_url", "")
        if central:
            own = urlsplit(central)
            if host == (own.hostname or "").rstrip(".").casefold() and port == (own.port or 443):
                return False
        alias = alias if alias is not None else peer.get("alias")
        if not isinstance(alias, str):
            return False
        credentials = local.get("a2a_credentials", {})
        if not isinstance(credentials, dict):
            return False
        matches = [value for name, value in credentials.items() if isinstance(name, str) and name.casefold() == alias.casefold()]
        if len(matches) != 1 or not isinstance(matches[0], dict):
            return False
        token = matches[0].get("token")
        if not isinstance(token, str) or not 1 <= len(token) <= 4096 or not token.isascii() or any(ord(c) <= 32 or ord(c) == 127 for c in token):
            return False
        for name, credential in credentials.items():
            if isinstance(name, str) and name.casefold() != alias.casefold() and isinstance(credential, dict) and credential.get("token") == token:
                return False
        forbidden = [local.get("network", {}).get("token")]
        for field in ("tokens", "device_tokens", "chat_tokens"):
            values = config.get(field, {})
            if not isinstance(values, dict):
                return False
            forbidden.extend(values.values())
        if token in forbidden:
            return False
        return not require_verified or (peer.get("verified") is True and peer.get("verified_url") == url and peer.get("verified_ref") == ref)
    except (ValueError, TypeError, AttributeError):
        return False



class Policy:
    """Check registration and identity, without per-software or direction rules.

    The historical name is kept for existing delivery callers. The policy never
    opens the old UI/password files or reads legacy direction permission fields.
    """
    def __init__(self, root, local, peers):
        self.local, self.peers = local, peers

    def chat_list(self):
        registered = []
        for alias, peer in self.peers.items():
            try:
                ref = chat_ref(peer)
            except ValueError:
                continue
            name = peer.get("alias", alias)
            if not isinstance(name, str) or not name:
                continue
            registered.append((name, peer, ref))
        paired = self.local.get("a2a", {}).get("device_tokens", {})
        result = []
        for alias, peer, ref in registered:
            unique = (sum(other == ref for _, _, other in registered) == 1
                      and sum(name.casefold() == alias.casefold() for name, _, _ in registered) == 1)
            external = is_external_a2a(peer)
            bound = (external_a2a_ready(self.local, peer, alias) if external else
                     ref[0] == self.local.get("device_id") or ref[0] in paired)
            verified = unique and bound and peer.get("verified") is True
            enabled = peer.get("enabled", True) is True
            result.append({"alias": alias, "device_id": ref[0], "software": ref[1],
                           "conversation_id": ref[2], "ref": ref, "enabled": enabled,
                           "verified": verified, "paired": bool(bound)})
        return result

    def check(self, source_peer, target_peer):
        denied = lambda why: {"contact": False, "busy": False, "reason": why}
        try:
            source, target = chat_ref(source_peer), chat_ref(target_peer)
        except ValueError:
            return denied("原聊天身份不完整")
        if source == target:
            return denied("来源和目标须是不同原聊天")
        identities = {compact(chat["ref"]): chat for chat in self.chat_list()}
        for ref in (source, target):
            chat = identities.get(compact(ref))
            if not chat or not chat["verified"]:
                return denied("原聊天未唯一核验或设备未配对")
            if not chat["enabled"]:
                return denied("原聊天尚未接入")
        # Busy insertion is a host capability, not a permission granted by an AI.
        # Keep the existing default: wait for the original chat to become idle.
        return {"contact": True, "busy": False, "reason": "已接入的原聊天可互相联系"}
