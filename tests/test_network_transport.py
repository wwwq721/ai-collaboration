"""Paired HTTPS identity, bounded cloning, and real local TLS verification."""
import asyncio
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import ssl
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from network_transport import (
    PairedIPTransport, PairedIPSyncTransport, build_http_options,
    build_sync_http_options, devices_from_payload, read_minis_devices,
    resolve_connect_ip,
)
from workbuddy import BridgeError

HOST = "pc.tailabc.ts.net"
IP = "100.108.215.39"
URL = "https://" + HOST + ":18443"


def network(**extra):
    return {"mode": "paired_ip", "base_url": URL, "server_dns_name": HOST,
            "device_dns_name": HOST, "server_port": 18443, **extra}


def devices(**extra):
    return [{"dnsName": HOST.upper() + ".", "ips": ["fd7a:115c:a1e0::1", IP],
             "online": True, **extra}]


class PairingTests(unittest.TestCase):
    def test_live_minis_config_string_value_envelope(self):
        payload = {"ok": True, "display_name": "Tailscale", "schema": {},
                   "value": json.dumps({"state": "Running", "devices": devices()})}
        self.assertEqual(resolve_connect_ip(network(), lambda: devices_from_payload(payload)), IP)

    def test_selects_full_dns_name_and_ipv4_not_first_device_or_address(self):
        entries = [{"dnsName": "other.tailabc.ts.net", "ips": ["100.80.1.1"]}] + devices()
        self.assertEqual(resolve_connect_ip(network(), lambda: entries), IP)

    def test_unknown_duplicate_offline_or_ambiguous_ip_is_rejected(self):
        for entries in ([], devices() + devices(), devices(online=False),
                        devices(ips=[IP, "100.80.1.2"]), devices(ips=["fd7a::1"]),
                        devices(ips=["192.168.1.1"]), devices(dnsName="pc")):
            with self.subTest(entries=entries), self.assertRaises(BridgeError):
                resolve_connect_ip(network(), lambda: entries)

    def test_static_pairing_must_be_explicit_tailnet_ipv4(self):
        self.assertEqual(resolve_connect_ip(network(connect_ip=IP)), IP)
        for address in ("127.0.0.1", "192.168.1.1", "8.8.8.8", "::1", None):
            with self.subTest(address=address), self.assertRaises(BridgeError):
                resolve_connect_ip(network(connect_ip=address))

    def test_cli_envelopes_must_have_exactly_one_devices_array(self):
        for payload in ({"devices": devices()}, {"data": {"devices": devices()}},
                        {"data": {"value": {"devices": devices()}}}):
            self.assertEqual(devices_from_payload(payload), devices())
        for payload in ({}, {"devices": None}, {"devices": devices(), "data": {"devices": devices()}},
                        {"other": {"devices": devices()}}, {"devices": [None]}):
            with self.subTest(payload=payload), self.assertRaises(BridgeError):
                devices_from_payload(payload)

    def test_cli_reads_only_confirmed_status_command_and_sanitizes_failure(self):
        runner = mock.Mock(return_value=subprocess.CompletedProcess([], 0, json.dumps({"data": {"devices": devices()}})))
        self.assertEqual(read_minis_devices(runner), devices())
        self.assertEqual(runner.call_args.args[0], ["minis-config", "get", "network.tailscale.status"])
        self.assertEqual(runner.call_args.kwargs["timeout"], 15)
        runner.side_effect = subprocess.TimeoutExpired("secret", 15, output="credential")
        with self.assertRaises(BridgeError) as caught:
            read_minis_devices(runner)
        self.assertNotIn("credential", str(caught.exception))

    def test_builder_checks_url_before_resolving_and_returns_separate_pools(self):
        cfg = {"network": network(connect_ip=IP)}
        a = build_http_options(cfg)["transport"]
        b = build_http_options(cfg)["transport"]
        self.assertIsNot(a.inner, b.inner)
        asyncio.run(a.aclose())
        asyncio.run(b.aclose())
        with self.assertRaises(BridgeError):
            build_http_options(cfg, {"url": "https://attacker.example/"})
        self.assertEqual(build_http_options({}), {})
        with build_sync_http_options(cfg)["transport"]:
            pass


class ChunkStream(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b"ab"
        yield b"cd"


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_clone_keeps_http_identity_and_original_response_request(self):
        seen = []

        async def target(request):
            seen.append(request)
            return httpx.Response(200, content=b"ok")

        inner = httpx.MockTransport(target)
        transport = PairedIPTransport(HOST, 18443, IP, inner=inner)
        async with httpx.AsyncClient(transport=transport, trust_env=False, follow_redirects=False) as client:
            response = await client.post(URL + "/agents/Codex/?q=1", content=ChunkStream())
        self.assertEqual(seen[0].url.host, IP)
        self.assertEqual(seen[0].url.port, 18443)
        self.assertEqual(seen[0].headers["host"], HOST + ":18443")
        self.assertEqual(seen[0].extensions["sni_hostname"], HOST)
        self.assertEqual(seen[0].content, b"abcd")
        self.assertEqual(str(response.request.url), URL + "/agents/Codex/?q=1")
        self.assertTrue(transport.closed)

    async def test_rejects_wrong_origin_host_header_and_http_before_inner_send(self):
        handler = mock.Mock(return_value=httpx.Response(200))
        transport = PairedIPTransport(HOST, 18443, IP, inner=httpx.MockTransport(handler))
        async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
            for url, headers in (("http://" + HOST + ":18443/", {}),
                                 ("https://other.tailabc.ts.net:18443/", {}),
                                 ("https://" + HOST + ":443/", {}),
                                 (URL, {"Host": "wrong.tailabc.ts.net"})):
                with self.subTest(url=url), self.assertRaises(BridgeError):
                    await client.get(url, headers=headers)
        handler.assert_not_called()

    async def test_async_stream_and_content_length_limits_precede_network(self):
        handler = mock.Mock(return_value=httpx.Response(200))
        transport = PairedIPTransport(HOST, 18443, IP, max_body_bytes=3, inner=httpx.MockTransport(handler))
        async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
            with self.assertRaises(BridgeError):
                await client.post(URL, content=ChunkStream())
            with self.assertRaises(BridgeError):
                await client.post(URL, content=b"abcd")
        handler.assert_not_called()

    async def test_close_is_forwarded_once_and_closed_transport_rejects(self):
        inner = mock.Mock(spec=httpx.AsyncBaseTransport)
        inner.aclose = mock.AsyncMock()
        transport = PairedIPTransport(HOST, 18443, IP, inner=inner)
        await transport.aclose()
        await transport.aclose()
        inner.aclose.assert_awaited_once()
        with self.assertRaises(BridgeError):
            await transport.handle_async_request(httpx.Request("GET", URL))

    async def test_redirect_does_not_move_credentials(self):
        calls = []

        def target(request):
            calls.append(request)
            return httpx.Response(302, headers={"Location": "https://other.tailabc.ts.net:18443/"})

        async with httpx.AsyncClient(
            transport=PairedIPTransport(HOST, 18443, IP, inner=httpx.MockTransport(target)),
            trust_env=False, follow_redirects=False, headers={"Authorization": "Bearer test-only"},
        ) as client:
            response = await client.get(URL)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(len(calls), 1)


class SyncTests(unittest.TestCase):
    def test_sync_clone_and_response_are_equivalent(self):
        seen = []

        def target(request):
            seen.append(request)
            return httpx.Response(200, content=b"ok")

        transport = PairedIPSyncTransport(HOST, 18443, IP, inner=httpx.MockTransport(target))
        with httpx.Client(transport=transport, trust_env=False) as client:
            response = client.post(URL + "/devices/phone/reply", content=b"payload")
        self.assertEqual(seen[0].url.host, IP)
        self.assertEqual(seen[0].extensions["sni_hostname"], HOST)
        self.assertEqual(seen[0].headers["host"], HOST + ":18443")
        self.assertEqual(seen[0].content, b"payload")
        self.assertEqual(response.request.url.host, HOST)
        self.assertTrue(transport.closed)

    def test_sync_body_limit_rejects_before_send(self):
        target = mock.Mock(return_value=httpx.Response(200))
        with httpx.Client(transport=PairedIPSyncTransport(
            HOST, 18443, IP, max_body_bytes=3, inner=httpx.MockTransport(target),
        ), trust_env=False) as client:
            with self.assertRaises(BridgeError):
                client.post(URL, content=b"abcd")
        target.assert_not_called()


class RealTLSTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        try:
            from cryptography import x509
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import rsa
            from cryptography.x509.oid import NameOID
        except ImportError:
            self.skipTest("optional test-only cryptography unavailable")
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.certpath = Path(self.tmp.name) / "cert.pem"
        keypath = Path(self.tmp.name) / "key.pem"
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, HOST)])
        now = datetime.now(timezone.utc)
        cert = (x509.CertificateBuilder().subject_name(subject).issuer_name(subject)
                .public_key(key.public_key()).serial_number(x509.random_serial_number())
                .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(days=1))
                .add_extension(x509.SubjectAlternativeName([x509.DNSName(HOST)]), critical=False)
                .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
                .sign(key, hashes.SHA256()))
        self.certpath.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        keypath.write_bytes(key.private_bytes(serialization.Encoding.PEM,
                            serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(self.certpath), str(keypath))
        self.sni, self.requests = [], []
        context.set_servername_callback(lambda sock, name, sslctx: self.sni.append(name))

        async def handler(reader, writer):
            try:
                self.requests.append(await reader.readuntil(b"\r\n\r\n"))
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        self.server = await asyncio.start_server(handler, "127.0.0.1", 0, ssl=context)
        self.port = self.server.sockets[0].getsockname()[1]
        from httpcore._backends.auto import AutoBackend
        original = AutoBackend.connect_tcp
        test = self

        async def connect(backend, host, port, **kwargs):
            test.assertEqual((host, port), (IP, 18443))
            return await original(backend, "127.0.0.1", test.port, **kwargs)

        self.patch = mock.patch.object(AutoBackend, "connect_tcp", connect)
        self.patch.start()

    async def asyncTearDown(self):
        if hasattr(self, "server"):
            self.patch.stop()
            self.server.close()
            await self.server.wait_closed()

    async def test_trusted_certificate_checks_logical_name_and_sni(self):
        context = ssl.create_default_context(cafile=str(self.certpath))
        transport = PairedIPTransport(HOST, 18443, IP, inner=httpx.AsyncHTTPTransport(verify=context))
        async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
            response = await client.get(URL)
        self.assertEqual(response.text, "ok")
        self.assertEqual(self.sni, [HOST])
        self.assertIn(("Host: " + HOST + ":18443").encode(), self.requests[0])
        self.assertEqual(response.request.url.host, HOST)

    async def test_untrusted_certificate_is_rejected(self):
        async with httpx.AsyncClient(transport=PairedIPTransport(HOST, 18443, IP), trust_env=False) as client:
            with self.assertRaises(httpx.ConnectError) as caught:
                await client.get(URL)
        self.assertIn("CERTIFICATE_VERIFY_FAILED", str(caught.exception))
        self.assertEqual(self.requests, [])

    async def test_trusted_certificate_for_wrong_hostname_is_rejected(self):
        context = ssl.create_default_context(cafile=str(self.certpath))
        other = "other.tailabc.ts.net"
        transport = PairedIPTransport(other, 18443, IP, inner=httpx.AsyncHTTPTransport(verify=context))
        async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
            with self.assertRaises(httpx.ConnectError) as caught:
                await client.get("https://" + other + ":18443/")
        self.assertIn("CERTIFICATE_VERIFY_FAILED", str(caught.exception))
        self.assertEqual(self.requests, [])


if __name__ == "__main__":
    unittest.main()
