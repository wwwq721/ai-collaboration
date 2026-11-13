"""Dial one paired tailnet IP while HTTP and TLS retain the logical identity.

No proxy fallback or request retry occurs here. Each transport owns one pool for
one logical HTTPS origin. The production inner HTTPX transport verifies normal
CA trust and the logical server name; test transports may be injected explicitly.
"""
import ipaddress
import json
import subprocess
from urllib.parse import urlsplit

import httpx

from workbuddy import BridgeError


MAX_BODY_BYTES = 512 * 1024 + 16384
TAILNET = ipaddress.ip_network("100.64.0.0/10")


def _name(value):
    if not isinstance(value, str):
        raise BridgeError("配对设备的完整域名无效")
    value = value.rstrip(".").lower()
    try:
        value = value.encode("idna").decode("ascii")
    except UnicodeError:
        raise BridgeError("配对设备的完整域名无效") from None
    labels = value.split(".")
    if (len(labels) < 3 or len(value) > 253 or not value.endswith(".ts.net")
            or any(not label or len(label) > 63 or label.startswith("-")
                   or label.endswith("-") or not all(c.isalnum() or c == "-" for c in label)
                   for label in labels)):
        raise BridgeError("配对设备的完整域名无效")
    return value


def _ip(value):
    try:
        address = ipaddress.ip_address(value)
        if address.version != 4 or address not in TAILNET:
            raise ValueError()
    except (ValueError, TypeError):
        raise BridgeError("配对设备没有唯一有效的 Tailscale IPv4 地址") from None
    return str(address)


def devices_from_payload(payload):
    """Accept exactly one devices list in the CLI's root/data/value envelopes."""
    candidates = []

    def visit(value, depth=0):
        if depth > 4:
            return
        if isinstance(value, str):
            if len(value.encode("utf8")) > 1024 * 1024:
                raise BridgeError("Minis 节点状态超过读取上限")
            try:
                decoded = json.loads(value)
            except ValueError:
                return
            visit(decoded, depth + 1)
            return
        if not isinstance(value, dict):
            return
        if "devices" in value:
            candidates.append(value["devices"])
        for key in ("data", "value"):
            if key in value:
                visit(value[key], depth + 1)

    visit(payload)
    if len(candidates) != 1 or not isinstance(candidates[0], list):
        raise BridgeError("Minis 节点状态格式不明确，未选择连接目标")
    if any(not isinstance(device, dict) for device in candidates[0]):
        raise BridgeError("Minis 节点状态格式不受支持")
    return candidates[0]


def read_minis_devices(runner=subprocess.run):
    try:
        result = runner(
            ["minis-config", "get", "network.tailscale.status"],
            capture_output=True, text=True, encoding="utf-8", timeout=15,
        )
        if result.returncode != 0:
            raise ValueError()
        payload = json.loads(result.stdout)
    except (OSError, ValueError, TypeError, subprocess.TimeoutExpired):
        raise BridgeError("Minis 无法读取当前 Tailscale 节点，未发送请求") from None
    return devices_from_payload(payload)


def resolve_connect_ip(network, resolver=None):
    # A static address is an explicit pairing option, never a guessed fallback.
    if "connect_ip" in network:
        return _ip(network["connect_ip"])
    expected = _name(network.get("device_dns_name"))
    if expected != _name(network.get("server_dns_name")):
        raise BridgeError("配对节点名称与 HTTPS 身份不一致")
    devices = (resolver or read_minis_devices)()
    if not isinstance(devices, list):
        raise BridgeError("Tailscale 节点列表格式不受支持")
    matches = []
    for device in devices:
        if not isinstance(device, dict) or not isinstance(device.get("dnsName"), str):
            continue
        try:
            matching = _name(device["dnsName"]) == expected
        except BridgeError:
            matching = False
        if matching:
            matches.append(device)
    if len(matches) != 1:
        raise BridgeError("配对节点名称未唯一匹配，未发送请求")
    if matches[0].get("online") is not True:
        raise BridgeError("配对设备当前未在线，未发送请求")
    addresses = matches[0].get("ips")
    if not isinstance(addresses, list):
        raise BridgeError("配对设备地址格式不受支持")
    ipv4 = set()
    for address in addresses:
        try:
            value = ipaddress.ip_address(address)
        except (ValueError, TypeError):
            raise BridgeError("配对设备地址格式不受支持") from None
        if value.version == 4:
            ipv4.add(_ip(address))
    if len(ipv4) != 1:
        raise BridgeError("配对设备没有唯一有效的 Tailscale IPv4 地址")
    return ipv4.pop()


class _PairedIdentity:
    def _init_identity(self, server_name, port, connect_ip, max_body_bytes):
        self.server_name = _name(server_name)
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise BridgeError("配对 HTTPS 端口无效")
        if (isinstance(max_body_bytes, bool) or not isinstance(max_body_bytes, int)
                or not 1 <= max_body_bytes <= 1024 * 1024):
            raise BridgeError("网络请求大小上限无效")
        self.port = port
        self.connect_ip = _ip(connect_ip)
        self.max_body_bytes = max_body_bytes
        self.closed = False

    def _check_request(self, request):
        if self.closed:
            raise BridgeError("配对网络连接已关闭")
        url = request.url
        if (url.scheme != "https" or url.username or url.password
                or _name(url.host) != self.server_name or (url.port or 443) != self.port):
            raise BridgeError("请求不属于已配对的 HTTPS 来源，未发送")
        expected = self.server_name + (":" + str(self.port) if self.port != 443 else "")
        hosts = request.headers.get_list("host")
        if len(hosts) != 1 or hosts[0].lower() != expected:
            raise BridgeError("请求 Host 与配对 HTTPS 身份不一致")
        length = request.headers.get("content-length")
        if length is not None:
            try:
                if int(length) < 0 or int(length) > self.max_body_bytes:
                    raise ValueError()
            except ValueError:
                raise BridgeError("网络请求正文超过上限或长度无效") from None

    def _clone(self, request, body):
        return httpx.Request(
            request.method, request.url.copy_with(host=self.connect_ip),
            headers=request.headers, content=body,
            extensions={**request.extensions, "sni_hostname": self.server_name},
        )


class PairedIPTransport(_PairedIdentity, httpx.AsyncBaseTransport):
    def __init__(self, server_name, port, connect_ip, *, max_body_bytes=MAX_BODY_BYTES, inner=None):
        self._init_identity(server_name, port, connect_ip, max_body_bytes)
        self.inner = inner if inner is not None else httpx.AsyncHTTPTransport(retries=0)

    async def handle_async_request(self, request):
        self._check_request(request)
        chunks, size = [], 0
        async for chunk in request.stream:
            size += len(chunk)
            if size > self.max_body_bytes:
                raise BridgeError("网络请求正文超过上限")
            chunks.append(chunk)
        response = await self.inner.handle_async_request(self._clone(request, b"".join(chunks)))
        response.request = request
        return response

    async def aclose(self):
        if not self.closed:
            self.closed = True
            await self.inner.aclose()


class PairedIPSyncTransport(_PairedIdentity, httpx.BaseTransport):
    def __init__(self, server_name, port, connect_ip, *, max_body_bytes=MAX_BODY_BYTES, inner=None):
        self._init_identity(server_name, port, connect_ip, max_body_bytes)
        self.inner = inner if inner is not None else httpx.HTTPTransport(retries=0)

    def handle_request(self, request):
        self._check_request(request)
        chunks, size = [], 0
        for chunk in request.stream:
            size += len(chunk)
            if size > self.max_body_bytes:
                raise BridgeError("网络请求正文超过上限")
            chunks.append(chunk)
        response = self.inner.handle_request(self._clone(request, b"".join(chunks)))
        response.request = request
        return response

    def close(self):
        if not self.closed:
            self.closed = True
            self.inner.close()


def _build(local, peer, resolver, transport_type):
    network = local.get("network", {})
    if not isinstance(network, dict) or network.get("mode") != "paired_ip":
        return {}
    name = _name(network.get("server_dns_name"))
    port = network.get("server_port", 18443)
    target = peer.get("url") if peer else network.get("base_url")
    try:
        parsed = urlsplit(target)
        if (parsed.scheme != "https" or parsed.username or parsed.password
                or _name(parsed.hostname) != name or (parsed.port or 443) != port):
            raise ValueError()
    except (TypeError, ValueError):
        raise BridgeError("目标地址与配对 HTTPS 来源不一致") from None
    return {"transport": transport_type(name, port, resolve_connect_ip(network, resolver))}


def build_http_options(local, peer=None, *, resolver=None):
    """Options for AsyncClient; also pass trust_env=False, follow_redirects=False."""
    return _build(local, peer, resolver, PairedIPTransport)


def build_sync_http_options(local, peer=None, *, resolver=None):
    """Equivalent options for a synchronous HTTPX Client."""
    return _build(local, peer, resolver, PairedIPSyncTransport)
