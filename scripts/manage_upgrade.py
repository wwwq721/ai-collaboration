"""Public-file upgrade tooling. Never restarts processes or changes private state.

build --source CANDIDATE --baseline OLD_PUBLIC_ROOT --target mobile --version LABEL --output UPDATE.zip
verify --package UPDATE.zip --sha256 OUT_OF_BAND_HASH --target mobile
prepare --root INSTALLED --package UPDATE.zip --sha256 HASH --target mobile --backup LOCAL_BACKUP
apply --root INSTALLED --package UPDATE.zip --sha256 HASH --target mobile --backup LOCAL_BACKUP --quiescent
restore --root INSTALLED --backup LOCAL_BACKUP --quiescent
fetch --root INSTALLED --output UPDATE.zip --sha256 HASH
inventory --root INSTALLED --target mobile
build --source CANDIDATE --baseline-manifest PUBLIC_HASHES.json --target mobile --version LABEL --output UPDATE.zip
portable --source PUBLIC_ROOT --output PORTABLE.zip [--replace]

inventory prints public hashes only (missing files are null); save its JSON as the
trusted --baseline-manifest input. portable generates editable fictional identity
templates and never reads the source device's local.json, peers.json, or runtime.

Each device requires its own baseline and local backup. A desktop-baseline ZIP must
not be applied to a phone whose public files differ. --quiescent is an operator
assertion: stop new work, check outstanding tasks and safely stop affected
components first. This program cannot infer that boundary or stop its own host.
Only fetch reads local.json, in process memory; never pass credentials as args.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
from urllib.parse import urlsplit
import zipfile

ROOT_PUBLIC = frozenset({"SKILL.md", "send.ps1", "send.sh", "a2a.ps1", "setup.ps1", "setup.sh",
                         "requirements-a2a.txt", "requirements-mobile.txt", "references/setup.md"})
SCRIPT_NAMES = frozenset({
    "a2a_client.py", "a2a_launch.py", "a2a_runtime.py", "a2a_server.py", "a2a_state.py",
    "a2a_transport.py", "access_policy.py", "codex_ipc.py", "collaboration_client.py",
    "delivery_ticket.py", "device_queue.py", "device_receiver.py", "host_adapters.py", "managed_egress.py", "minis.py", "message_delivery.py",
    "network_transport.py", "receiver_manager.py", "receiver.sh", "result_watcher.py", "send.py",
    "setup_mobile.py", "workbuddy.py", "workbuddy_sync.cjs", "manage_upgrade.py", "managed_setup.py", "managed_bridge.py"})
PUBLIC = ROOT_PUBLIC | frozenset("scripts/" + name for name in SCRIPT_NAMES)
TEST_NAMES = frozenset({
    "test_a2a.py", "test_message_receipts.py", "test_message_client.py", "test_conversation_messages.py", "test_message_protocol.py", "test_message_setup.py", "test_access_policy.py", "test_background_delivery.py",
    "test_codex_ipc.py", "test_console.py", "test_device_permissions.py", "test_device_queue.py",
    "test_device_receiver.py", "test_host_adapters.py", "test_manage_upgrade.py", "test_managed_bridge.py",
    "test_managed_client.py", "test_managed_egress.py", "test_managed_setup.py", "test_manager.py", "test_minis.py",
    "test_mobile_routing.py", "test_network_transport.py", "test_receiver_manager.py", "test_result_watcher.py",
    "test_send.py", "test_send_context.py",
    "test_setup_mobile.py", "test_workbuddy_managed.py"})
TESTS = frozenset("tests/" + name for name in TEST_NAMES)
MCP_FALLBACK = "references/mcp-fallback.zip"
MCP_FALLBACK_MANIFEST = "mcp-fallback-manifest.json"
MCP_FALLBACK_FILES = frozenset({
    "scripts/mcp_server.py", "scripts/mcp_actions.py", "scripts/setup_mcp.py",
    "tests/test_mcp_stdio.py", "tests/test_mcp_actions.py", "tests/test_setup_mcp.py",
    "requirements-mcp.txt", "requirements-mcp-windows-lock.txt"})
MANIFEST = "update-manifest.json"
PORTABLE_MANIFEST = "portable-manifest.json"
LOCAL_TEMPLATE = {"machine_name": "本机计算机名", "device_id": "请填写新设备编号",
                  "python": "请填写本机Python绝对路径", "node": "请填写本机Node绝对路径",
                  "workbuddy_install": "请填写本机WorkBuddy安装目录", "workbuddy_config": "请填写本机WorkBuddy用户目录"}
PEERS_TEMPLATE = {"version": 1, "peers": {"示例聊天": {
    "software": "codex", "device_id": "TEMPLATE-REPLACE-DEVICE", "conversation_id": "TEMPLATE-REPLACE-CHAT",
    "alias": "示例聊天", "enabled": False, "verified": False}}}
MAX_ZIP, MAX_FILE, MAX_TOTAL = 16 * 1024 * 1024, 2 * 1024 * 1024, 16 * 1024 * 1024


class UpgradeError(ValueError):
    pass


def digest(data):
    return hashlib.sha256(data).hexdigest()


def allowed(target):
    if target not in ("desktop", "mobile"):
        raise UpgradeError("unknown target")
    return PUBLIC | TESTS | {MCP_FALLBACK} if target == "desktop" else PUBLIC


def check_hash(value):
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise UpgradeError("expected SHA256 must be 64 lowercase hex characters")
    return value


def linked(path):
    return path.is_symlink() or getattr(path, "is_junction", lambda: False)()


def safe_root(root):
    root = Path(root).absolute()
    for part in (root, *root.parents):
        if linked(part):
            raise UpgradeError("linked root is forbidden")
    if not root.is_dir():
        raise UpgradeError("root must already exist")
    return root


def public_path(root, name, target):
    if name not in allowed(target):
        raise UpgradeError("file is outside the explicit public allowlist")
    path = root / name
    for part in (path, *path.parents):
        if part == root.parent:
            break
        if linked(part):
            raise UpgradeError("linked public path is forbidden")
    if path.exists() and (not path.is_file() or path.stat().st_nlink != 1):
        raise UpgradeError("public path must be a regular unshared file")
    return path


def read_public(root, name, target):
    path = public_path(root, name, target)
    if not path.exists():
        return None, None
    if path.stat().st_size > MAX_FILE:
        raise UpgradeError("public file too large")
    data = path.read_bytes()
    if name == MCP_FALLBACK:
        verify_mcp_fallback(data)
    return data, stat.S_IMODE(path.stat().st_mode)


def new_destination(path):
    path = Path(path).absolute()
    for part in (path, *path.parents):
        if linked(part):
            raise UpgradeError("linked output path is forbidden")
    if path.exists():
        raise UpgradeError("output already exists")
    return path


def atomic(path, data, mode=0o600):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".upgrade-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def json_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def inventory(root, target):
    root = safe_root(root)
    files = {}
    for name in sorted(allowed(target)):
        body, _ = read_public(root, name, target)
        files[name] = None if body is None else digest(body)
    return {"format": 1, "target": target, "files": files}


def strict_json(data):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise UpgradeError("duplicate JSON field")
            result[key] = value
        return result
    return json.loads(data, object_pairs_hook=unique)


def verify_mcp_fallback(data):
    """The dormant desktop reference contains exactly eight public source files."""
    if len(data) > MAX_FILE:
        raise UpgradeError("MCP fallback size limit")
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            infos = archive.infolist()
            names = [entry.filename for entry in infos]
            if len(names) != 9 or set(names) != MCP_FALLBACK_FILES | {MCP_FALLBACK_MANIFEST}:
                raise UpgradeError("MCP fallback public file list mismatch")
            if sum(entry.file_size for entry in infos) > MAX_TOTAL:
                raise UpgradeError("MCP fallback expansion limit")
            for entry in infos:
                kind = stat.S_IFMT(entry.external_attr >> 16)
                if (entry.is_dir() or kind not in (0, stat.S_IFREG) or entry.flag_bits & 1
                        or entry.file_size > MAX_FILE):
                    raise UpgradeError("MCP fallback contains unsafe entry")
            manifest = strict_json(archive.read(MCP_FALLBACK_MANIFEST))
            if (not isinstance(manifest, dict) or set(manifest) != {"format", "kind", "files"}
                    or type(manifest["format"]) is not int or manifest["format"] != 1
                    or manifest["kind"] != "mcp-fallback" or not isinstance(manifest["files"], dict)
                    or set(manifest["files"]) != MCP_FALLBACK_FILES):
                raise UpgradeError("MCP fallback manifest mismatch")
            payload = {}
            for name, expected in manifest["files"].items():
                check_hash(expected)
                payload[name] = archive.read(name)
                if digest(payload[name]) != expected:
                    raise UpgradeError("MCP fallback file SHA256 mismatch")
            return manifest, payload
    except (zipfile.BadZipFile, KeyError, TypeError, UnicodeError, json.JSONDecodeError):
        raise UpgradeError("invalid MCP fallback archive") from None


def inventory_manifest(path, target):
    path = Path(path).absolute()
    if any(linked(part) for part in (path, *path.parents)) or not path.is_file() or path.stat().st_size > MAX_FILE:
        raise UpgradeError("invalid baseline inventory file")
    try:
        value = strict_json(path.read_bytes())
    except (ValueError, UnicodeError):
        raise UpgradeError("invalid baseline inventory JSON") from None
    if not isinstance(value, dict) or set(value) != {"format", "target", "files"} or type(value["format"]) is not int or value["format"] != 1 or value["target"] != target or not isinstance(value["files"], dict) or set(value["files"]) != allowed(target):
        raise UpgradeError("baseline inventory must contain exactly the target public file set")
    for sha in value["files"].values():
        if sha is not None:
            check_hash(sha)
    return value["files"]


def build(source, baseline=None, target="mobile", version=None, output=None, baseline_manifest=None):
    source = safe_root(source)
    if (baseline is None) == (baseline_manifest is None):
        raise UpgradeError("choose exactly one physical or inventory baseline")
    hashes = inventory_manifest(baseline_manifest, target) if baseline_manifest is not None else None
    if baseline is not None:
        baseline = safe_root(baseline)
    if not isinstance(version, str) or not re.fullmatch(r"[A-Za-z0-9._-]{1,80}", version):
        raise UpgradeError("version must be a short public label")
    output = new_destination(output)
    if output.suffix.lower() != ".zip":
        raise UpgradeError("public package output must be a ZIP file")
    files, payload = {}, {}
    for name in sorted(allowed(target)):
        data, mode = read_public(source, name, target)
        if data is None:
            raise UpgradeError("candidate is missing required public file: " + name)
        if hashes is None:
            old, old_mode = read_public(baseline, name, target)
            old_sha = None if old is None else digest(old)
        else:
            old_sha = hashes[name]
            # The destination prepare operation saves the real local permission mode.
            old_mode = None if old_sha is None else (0o755 if name.endswith(".sh") else 0o644)
        # Fixed safe modes preserve shell executability across Windows build hosts.
        new_mode = 0o755 if name.endswith(".sh") else 0o644
        files[name] = {"sha256": digest(data), "before": old_sha,
                       "mode": new_mode, "before_mode": old_mode}
        payload[name] = data
    if not files:
        raise UpgradeError("no public files selected")
    manifest = {"format": 1, "version": version, "target": target, "files": files}
    content = io.BytesIO()
    with zipfile.ZipFile(content, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(MANIFEST, json_bytes(manifest))
        for name, data in payload.items():
            archive.writestr(name, data)
    data = content.getvalue()
    if len(data) > MAX_ZIP:
        raise UpgradeError("package too large")
    # Validate the built archive using precisely the receiver's validator.
    verify_bytes(data, digest(data), target)
    atomic(output, data)
    return {"sha256": digest(data), "target": target, "version": version, "files": len(files)}


def portable(source, output, replace=False):
    """Build a clean migration archive, with only generated unverified identity templates."""
    source = safe_root(source)
    output = Path(output).absolute()
    for part in (output, *output.parents):
        if linked(part):
            raise UpgradeError("linked portable output forbidden")
    if output.suffix.lower() != ".zip" or (output.exists() and (not replace or not output.is_file() or output.stat().st_nlink != 1)):
        raise UpgradeError("portable output must be a new ZIP, or explicitly --replace a regular ZIP")
    payload = {}
    for name in sorted(allowed("desktop")):
        body, _ = read_public(source, name, "desktop")
        if body is None:
            raise UpgradeError("portable source is missing required public file: " + name)
        payload[name] = body
    # Generate from constants. Never inspect an installed identity or configuration.
    payload["local.example.json"] = json_bytes(LOCAL_TEMPLATE)
    payload["peers.json"] = json_bytes(PEERS_TEMPLATE)
    manifest = {"format": 1, "kind": "portable", "target": "desktop",
                "files": {name: digest(body) for name, body in payload.items()},
                "templates": {"local.example.json": "模板：在新设备本机填写，不能沿用旧机配置或凭据",
                              "peers.json": "虚构模板：示例聊天已停用且未核验，必须替换设备及原聊天身份、配对后核验"}}
    content = io.BytesIO()
    with zipfile.ZipFile(content, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(PORTABLE_MANIFEST, json_bytes(manifest))
        for name, body in payload.items():
            if len(body) > MAX_FILE:
                raise UpgradeError("portable file too large")
            entry = zipfile.ZipInfo(name)
            entry.create_system = 3
            entry.external_attr = (stat.S_IFREG | (0o755 if name.endswith(".sh") else 0o644)) << 16
            entry.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(entry, body)
    data = content.getvalue()
    if len(data) > MAX_ZIP or sum(map(len, payload.values())) > MAX_TOTAL:
        raise UpgradeError("portable archive size limit")
    atomic(output, data)
    return {"portable": True, "sha256": digest(data), "files": len(payload), "templates": sorted(manifest["templates"])}


def verify_bytes(data, expected_sha256, target):
    check_hash(expected_sha256)
    if len(data) > MAX_ZIP or digest(data) != expected_sha256:
        raise UpgradeError("package SHA256 mismatch or size limit")
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            infos = archive.infolist()
            names = [entry.filename for entry in infos]
            if len(names) != len(set(names)) or len(names) > len(allowed(target)) + 1 or MANIFEST not in names:
                raise UpgradeError("duplicate entries or missing manifest")
            total = 0
            for entry in infos:
                if entry.filename != MANIFEST and entry.filename not in allowed(target):
                    raise UpgradeError("forbidden archive path")
                kind = stat.S_IFMT(entry.external_attr >> 16)
                if entry.is_dir() or kind not in (0, stat.S_IFREG) or entry.flag_bits & 1:
                    raise UpgradeError("archive links/directories/encryption forbidden")
                total += entry.file_size
                if entry.file_size > MAX_FILE or total > MAX_TOTAL:
                    raise UpgradeError("archive expansion limit")
            manifest = strict_json(archive.read(MANIFEST))
            if set(manifest) != {"format", "version", "target", "files"} or manifest["format"] != 1 or manifest["target"] != target:
                raise UpgradeError("unsupported manifest")
            if not isinstance(manifest["version"], str) or not re.fullmatch(r"[A-Za-z0-9._-]{1,80}", manifest["version"]):
                raise UpgradeError("invalid version")
            files = manifest["files"]
            if not isinstance(files, dict) or set(files) != allowed(target) or set(names) != set(files) | {MANIFEST}:
                raise UpgradeError("manifest file list mismatch")
            payload = {}
            for name, info in files.items():
                if name not in allowed(target) or not isinstance(info, dict) or set(info) != {"sha256", "before", "mode", "before_mode"}:
                    raise UpgradeError("invalid file record")
                check_hash(info["sha256"])
                if info["before"] is not None:
                    check_hash(info["before"])
                if info["mode"] != (0o755 if name.endswith(".sh") else 0o644):
                    raise UpgradeError("unsafe public file mode")
                old_mode = info["before_mode"]
                if (info["before"] is None) != (old_mode is None) or (old_mode is not None and (type(old_mode) is not int or not 0 <= old_mode <= 0o777)):
                    raise UpgradeError("invalid baseline mode")
                body = archive.read(name)
                if digest(body) != info["sha256"]:
                    raise UpgradeError("file SHA256 mismatch")
                if name == MCP_FALLBACK:
                    verify_mcp_fallback(body)
                payload[name] = body
            return manifest, payload
    except (zipfile.BadZipFile, KeyError, TypeError, UnicodeError, json.JSONDecodeError) as error:
        raise UpgradeError("invalid update archive") from error


def verify(package, expected_sha256, target):
    path = Path(package).absolute()
    if any(linked(part) for part in path.parents) or linked(path) or not path.is_file() or path.stat().st_size > MAX_ZIP:
        raise UpgradeError("invalid package file")
    return verify_bytes(path.read_bytes(), expected_sha256, target)


def check_baseline(root, manifest):
    for name, info in manifest["files"].items():
        body, _ = read_public(root, name, manifest["target"])
        if (None if body is None else digest(body)) != info["before"]:
            raise UpgradeError("installed public baseline changed: " + name)


def prepare(root, package, expected_sha256, target, backup):
    root = safe_root(root)
    manifest, _ = verify(package, expected_sha256, target)
    check_baseline(root, manifest)
    backup = new_destination(backup)
    # Backup only exact public files. Private config, runtime and databases are never read.
    backup.mkdir(parents=True, mode=0o700)
    old_files = {}
    for name, info in manifest["files"].items():
        body, mode = read_public(root, name, target)
        if (None if body is None else digest(body)) != info["before"]:
            raise UpgradeError("baseline changed while preparing")
        old_files[name] = {**info, "before_mode": mode}
        if body is not None:
            atomic(backup / "files" / name, body, mode & 0o777)
    record = {"format": 1, "root": str(root), "package_sha256": expected_sha256,
              "target": target, "version": manifest["version"], "files": old_files}
    atomic(backup / "backup.json", json_bytes(record))
    return record


def read_backup(root, backup):
    root, backup = safe_root(root), safe_root(backup)
    index = backup / "backup.json"
    if linked(index) or not index.is_file() or index.stat().st_size > MAX_FILE:
        raise UpgradeError("invalid backup record")
    record = strict_json(index.read_bytes())
    if set(record) != {"format","root","package_sha256","target","version","files"} or record.get("format") != 1 or record.get("root") != str(root) or not isinstance(record.get("files"), dict) or not record.get("files"):
        raise UpgradeError("backup belongs to another root")
    target = record.get("target")
    check_hash(record.get("package_sha256"))
    for name, info in record["files"].items():
        public_path(root, name, target)
        if not isinstance(info, dict) or set(info) != {"sha256","before","mode","before_mode"} or info["mode"] != (0o755 if name.endswith(".sh") else 0o644):
            raise UpgradeError("invalid backup file record")
        old_mode=info["before_mode"]
        if (info["before"] is None) != (old_mode is None) or (old_mode is not None and (type(old_mode) is not int or not 0 <= old_mode <= 0o777)):
            raise UpgradeError("invalid backup mode")
        check_hash(info["sha256"])
        if info["before"] is not None:
            check_hash(info["before"])
            body, _ = read_public(backup / "files", name, target)
            if body is None or digest(body) != info["before"]:
                raise UpgradeError("backup file SHA256 mismatch")
    return record


def restore(root, backup, *, quiescent=False):
    if not quiescent:
        raise UpgradeError("operator must confirm quiescent maintenance boundary")
    root = safe_root(root)
    record = read_backup(root, backup)
    # Preflight every path before changing any file. Refuse to erase later manual edits.
    for name, info in record["files"].items():
        body, _ = read_public(root, name, record["target"])
        if (None if body is None else digest(body)) not in (info["sha256"], info["before"]):
            raise UpgradeError("public file changed since upgrade: " + name)
    for name, info in record["files"].items():
        path = public_path(root, name, record["target"])
        if info["before"] is None:
            if path.exists():
                path.unlink()
        else:
            body, _ = read_public(Path(backup) / "files", name, record["target"])
            atomic(path, body, info["before_mode"])
    check_baseline(root, record)
    return {"restored": True, "files": len(record["files"])}


def apply(root, package, expected_sha256, target, backup, *, quiescent=False):
    if not quiescent:
        raise UpgradeError("operator must confirm quiescent maintenance boundary")
    root = safe_root(root)
    manifest, payload = verify(package, expected_sha256, target)
    record = read_backup(root, backup)
    if record["package_sha256"] != expected_sha256 or record["target"] != target or record["version"] != manifest["version"] or set(record["files"]) != set(manifest["files"]):
        raise UpgradeError("backup does not match update")
    for name, info in manifest["files"].items():
        saved = record["files"][name]
        if any(saved[k] != info[k] for k in ("sha256", "before", "mode")):
            raise UpgradeError("backup file contract differs")
    check_baseline(root, manifest)
    replaced = []
    try:
        for name, body in payload.items():
            path = public_path(root, name, target)
            # Recheck immediately before replacement to catch intervening public edits.
            old, _ = read_public(root, name, target)
            if (None if old is None else digest(old)) != manifest["files"][name]["before"]:
                raise UpgradeError("baseline changed during apply")
            atomic(path, body, manifest["files"][name]["mode"])
            replaced.append(name)
        for name, body in payload.items():
            if digest(public_path(root, name, target).read_bytes()) != digest(body):
                raise UpgradeError("post-upgrade verification failed")
    except BaseException:
        # Restore only our successfully replaced public paths; keep backup for interrupted runs.
        for name in reversed(replaced):
            info = record["files"][name]
            path = public_path(root, name, target)
            if path.exists() and digest(path.read_bytes()) == info["sha256"]:
                if info["before"] is None:
                    path.unlink()
                else:
                    body, _ = read_public(Path(backup) / "files", name, target)
                    atomic(path, body, info["before_mode"])
        raise
    return {"applied": True, "version": manifest["version"], "files": len(payload)}


def fetch(root, output, expected_sha256, *, client_factory=None):
    """Only this explicitly invoked operation reads credentials; errors omit all request data."""
    check_hash(expected_sha256)
    root = safe_root(root)
    output = new_destination(output)
    if output.suffix.lower() != ".zip":
        raise UpgradeError("download output must be a ZIP file")
    try:
        local = json.loads((root / "local.json").read_text(encoding="utf-8-sig"))
        network = local["network"]
        url = urlsplit(network["base_url"])
        token = network["token"]
        if url.scheme != "https" or not url.hostname or url.username or url.password or url.query or url.fragment or url.path not in ("", "/") or not isinstance(token, str) or not token:
            raise UpgradeError("invalid HTTPS package configuration")
        from network_transport import build_sync_http_options
        import httpx
        factory = client_factory or httpx.Client
        options = build_sync_http_options(local)
        data = bytearray()
        with factory(trust_env=False, follow_redirects=False, timeout=60, **options) as client:
            with client.stream("GET", network["base_url"].rstrip("/") + "/package", headers={"Authorization": "Bearer " + token, "Accept": "application/zip"}) as response:
                if response.status_code != 200:
                    raise UpgradeError("authenticated package download rejected")
                for chunk in response.iter_bytes(chunk_size=65536):
                    data.extend(chunk)
                    if len(data) > MAX_ZIP:
                        raise UpgradeError("download size limit")
        verify_bytes(bytes(data), expected_sha256, "mobile")
        atomic(output, bytes(data))
    except Exception:
        raise UpgradeError("package fetch failed; no installed files changed") from None
    return {"downloaded": True, "sha256": expected_sha256, "bytes": len(data)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("build", "verify", "prepare", "apply", "restore", "fetch", "inventory", "portable"):
        p = sub.add_parser(command)
        if command == "build":
            for arg in ("source", "version", "output"):
                p.add_argument("--" + arg, required=True)
            group = p.add_mutually_exclusive_group(required=True)
            group.add_argument("--baseline")
            group.add_argument("--baseline-manifest")
        if command == "portable":
            p.add_argument("--source", required=True)
            p.add_argument("--output", required=True)
            p.add_argument("--replace", action="store_true")
        if command in ("build", "verify", "prepare", "apply", "inventory"):
            p.add_argument("--target", required=True, choices=("desktop", "mobile"))
        if command in ("verify", "prepare", "apply"):
            p.add_argument("--package", required=True)
        if command in ("verify", "prepare", "apply", "fetch"):
            p.add_argument("--sha256", dest="expected_sha256", required=True)
        if command in ("prepare", "apply", "restore", "fetch", "inventory"):
            p.add_argument("--root", required=True)
        if command in ("prepare", "apply", "restore"):
            p.add_argument("--backup", required=True)
        if command in ("apply", "restore"):
            p.add_argument("--quiescent", action="store_true")
        if command == "fetch":
            p.add_argument("--output", required=True)
    args = vars(parser.parse_args(argv))
    command = args.pop("command")
    try:
        result = globals()[command](**args)
        if command == "verify":
            result = {"verified": True, "version": result[0]["version"], "files": len(result[1])}
        if command == "prepare":
            result = {"prepared": True, "version": result["version"], "files": len(result["files"])}
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except Exception:
        # Deliberately omit exception repr: HTTP exceptions can contain private headers/URLs.
        print(json.dumps({"ok": False, "error": "operation refused or failed; installed private state untouched"}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
