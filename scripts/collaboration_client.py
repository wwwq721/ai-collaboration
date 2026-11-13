"""Managed routing and source-bound credentials; never logs credentials."""
import json
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit
from urllib.request import Request, ProxyHandler, HTTPRedirectHandler, build_opener

from workbuddy import BridgeError
from delivery_ticket import managed


def _same_chat(left, right):
    return all(left.get(k) == right.get(k) for k in ("device_id", "software", "conversation_id"))


def source_alias(peers, origin):
    from host_adapters import _peer_values
    matches = [p for p in _peer_values(peers) if _same_chat(p, origin)
               and p.get("enabled", True) is True]
    if len(matches) != 1:
        raise BridgeError("发件原聊天尚未唯一绑定")
    return matches[0]["alias"]


def _token_at(tokens, alias):
    if not isinstance(tokens, dict):
        return None
    matches = [v for k, v in tokens.items() if isinstance(k, str) and k.casefold() == alias.casefold()]
    return matches[0] if len(matches) == 1 and isinstance(matches[0], str) and matches[0] else None


def source_token(local, peers, origin):
    alias = source_alias(peers, origin)
    config = local.get("network", {}) if local.get("platform") == "minis" else local.get("a2a", {})
    token = _token_at(config.get("chat_tokens"), alias)
    if not token and local.get("platform") == "minis" and not config.get("chat_tokens"):
        from host_adapters import _peer_values
        sources = [p for p in _peer_values(peers) if p.get("device_id") == local.get("device_id")
                   and p.get("enabled", True) is True and p.get("software") in {"minis", "codex", "workbuddy"}]
        if len(sources) == 1 and _same_chat(sources[0], origin):
            token = config.get("token")
    if not isinstance(token, str) or not token:
        raise BridgeError("缺少绑定到发件原聊天的凭据，未发送")
    return token


def central_peer(local, peer):
    """Managed native chats use the existing central service's A2A route."""
    if local.get("platform") == "minis":
        base = local.get("network", {}).get("base_url", "")
        parsed = urlsplit(base)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise BridgeError("管理模式需要已配对的完整 HTTPS 后台地址")
    else:
        config = local.get("a2a", {})
        base = "http://127.0.0.1:" + str(config.get("port", 8788))
    expected = base.rstrip("/") + "/agents/" + quote(peer["alias"], safe="")
    if peer.get("software") == "a2a" or peer.get("transport") == "a2a":
        if peer.get("url", "").rstrip("/") == expected and peer.get("conversation_id"):
            return {**peer, "local_a2a": True}
        validate_external_peer(peer)
        return {**peer, "host_software": peer.get("host_software", peer["software"]),
                "software": "a2a", "local_a2a": True, "url": expected}
    if (local.get("platform") != "minis" and peer.get("device_id") != local.get("device_id")
            and peer.get("device_id") not in local.get("a2a", {}).get("device_tokens", {})):
        raise BridgeError("目标设备尚未完成网络配对")
    return {**peer, "host_software": peer["software"], "software": "a2a", "local_a2a": True,
            "url": expected}


def validate_external_peer(peer):
    """A verified original chat and URL are both required for central egress."""
    from host_adapters import ChatRef
    ChatRef.from_peer(peer)
    from access_policy import chat_ref
    if (peer.get("enabled", True) is not True or peer.get("verified") is not True
            or peer.get("verified_ref") != chat_ref(peer)
            or not (peer.get("transport") == "a2a" or peer.get("software") == "a2a")):
        raise BridgeError("外部 A2A 原聊天尚未核验")
    url = peer.get("url")
    verified = peer.get("verified_url")
    if not isinstance(url, str) or not isinstance(verified, str) or url.rstrip("/") != verified.rstrip("/"):
        raise BridgeError("外部 A2A 地址与已核验地址不符")
    parsed = urlsplit(url)
    try:
        valid_port = parsed.port is None or 1 <= parsed.port <= 65535
    except ValueError:
        valid_port = False
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
            or parsed.query or parsed.fragment or not valid_port):
        raise BridgeError("外部 A2A 需要已核验的完整 HTTPS 地址")
    return url.rstrip("/")


def validate_central(local, peer):
    base = (local.get("network", {}).get("base_url", "") if local.get("platform") == "minis"
            else "http://127.0.0.1:" + str(local.get("a2a", {}).get("port", 8788)))
    expected = base.rstrip("/") + "/agents/" + quote(peer["alias"], safe="")
    if not peer.get("local_a2a") or peer.get("url", "").rstrip("/") != expected:
        raise BridgeError("管理模式拒绝绕过中央后台的 A2A 地址")
    if local.get("platform") == "minis" and urlsplit(base).scheme != "https":
        raise BridgeError("管理模式需要已配对的完整 HTTPS 后台地址")


def local_request(local, path, payload, token=None):
    class NoRedirect(HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    url = "http://127.0.0.1:" + str(local.get("a2a", {}).get("port", 8788)) + path
    request = Request(url, data=json.dumps(payload).encode("utf8"), headers=headers, method="POST")
    try:
        with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=20) as response:
            data = response.read(65537)
        if len(data) > 65536:
            raise ValueError("response size")
        result = json.loads(data)
        if not isinstance(result, dict):
            raise ValueError("response shape")
        return result
    except Exception as error:
        ambiguous = path == "/collaboration/reply" and not (isinstance(error, HTTPError) and 400 <= error.code < 500)
        raise BridgeError("后台未确认本次操作，请核对原任务" if ambiguous else "后台不可达或拒绝本次操作，未发送",
                          "uncertain" if ambiguous else "not_sent") from error
