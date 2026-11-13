"""Verify registered original chats and preserve local credential bindings."""
import argparse
from contextlib import closing, contextmanager
import copy
import ctypes
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import sqlite3
import stat
import tempfile
import time
from urllib.parse import quote, urlsplit
from access_policy import Policy, chat_ref, external_a2a_ready, is_external_a2a


class SetupError(ValueError):
    pass


def _load(path):
    try:
        document = json.loads(path.read_text(encoding="utf-8-sig"))
        if not isinstance(document, dict):
            raise ValueError()
        return document
    except (OSError, ValueError):
        raise SetupError("配置缺失或格式错误；未显示配置内容") from None


def _security(path, template):
    if os.name != "nt":
        mode = stat.S_IMODE(template.stat().st_mode) & 0o700
        os.chmod(path, mode | (0o100 if path.is_dir() else 0))
        return
    # Apply the original file's DACL BEFORE any private bytes enter the new file.
    from ctypes import wintypes
    api = ctypes.WinDLL("advapi32", use_last_error=True)
    get = api.GetFileSecurityW
    get.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.LPVOID, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]
    get.restype = wintypes.BOOL
    needed = wintypes.DWORD()
    get(str(template), 4, None, 0, ctypes.byref(needed))
    if not needed.value:
        raise SetupError("无法读取原私密文件访问权限")
    descriptor = ctypes.create_string_buffer(needed.value)
    if not get(str(template), 4, descriptor, needed, ctypes.byref(needed)):
        raise SetupError("无法读取原私密文件访问权限")
    # Private staging/backup objects inherit no new parent entries. The original
    # target's protection flag and exact DACL are retained by _replace below.
    flags = 4 | 0x80000000
    api.SetFileSecurityW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.LPVOID]
    api.SetFileSecurityW.restype = wintypes.BOOL
    if not api.SetFileSecurityW(str(path), flags, descriptor):
        raise SetupError("无法保护临时私密文件的访问权限")


def _replace(path, temporary):
    if os.name == "nt" and path.exists():
        from ctypes import wintypes
        security = ctypes.WinDLL("advapi32", use_last_error=True)
        security.GetFileSecurityW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.LPVOID, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]
        needed = wintypes.DWORD()
        security.GetFileSecurityW(str(path), 4, None, 0, ctypes.byref(needed))
        original_dacl = ctypes.create_string_buffer(needed.value)
        if not needed.value or not security.GetFileSecurityW(str(path), 4, original_dacl, needed, ctypes.byref(needed)):
            raise SetupError("无法核对替换前的访问权限")
        api = ctypes.WinDLL("kernel32", use_last_error=True)
        replace = api.ReplaceFileW
        replace.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.LPCWSTR,
                            wintypes.DWORD, wintypes.LPVOID, wintypes.LPVOID]
        replace.restype = wintypes.BOOL
        # ReplaceFile retains the target's DACL; never use IGNORE_ACL_ERRORS.
        if not replace(str(path), str(temporary), None, 0, None, None):
            raise SetupError("Windows拒绝保留访问权限的原子替换")
        # Some Windows versions normalize inherited ACE flags during ReplaceFile.
        # Reapply the exact original DACL after the atomic content replacement.
        security.SetFileSecurityW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.LPVOID]
        security.SetFileSecurityW.restype = wintypes.BOOL
        if not security.SetFileSecurityW(str(path), 4, original_dacl):
            raise SetupError("替换后无法恢复原访问权限，须回退")
    else:
        os.replace(temporary, path)


def _write(path, raw, template):
    descriptor, name = tempfile.mkstemp(prefix=".managed-", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            _security(temporary, template)
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        _replace(path, temporary)
    finally:
        if temporary.exists():
            temporary.unlink()


def _json_bytes(document):
    return (json.dumps(document, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


@contextmanager
def _lock(root):
    # A root-level empty lock avoids creating runtime during read-only inspect.
    path = root / ".managed-setup.lock"
    try:
        handle = path.open("x+b")
    except FileExistsError:
        raise SetupError("另一准备命令正在运行；若上次中断请先在本机核查锁文件") from None
    try:
        yield
    finally:
        handle.close()
        path.unlink()


def _transaction(root, changes):
    local_path = root / "local.json"
    originals = {path: path.read_bytes() for path in changes}
    runtime = root / "runtime"
    if runtime.is_symlink() or (runtime / "managed-backups").is_symlink():
        raise SetupError("私密备份目录为链接，须先在本机核对")
    runtime.mkdir(exist_ok=True)
    backup_root = runtime / "managed-backups"
    backup_root.mkdir(exist_ok=True)
    _security(backup_root, local_path)
    group = backup_root / (time.strftime("%Y%m%d-%H%M%S") + "-" + secrets.token_hex(4))
    group.mkdir()
    _security(group, local_path)
    for path, raw in originals.items():
        _write(group / path.name, raw, local_path)
    applied = []
    try:
        for path, document in changes.items():
            _write(path, _json_bytes(document), path)
            applied.append(path)
    except Exception:
        rollback_failed = False
        for path in reversed(list(changes)):
            try:
                _write(path, originals[path], group / path.name)
            except Exception:
                rollback_failed = True
        if rollback_failed:
            raise SetupError("保存中断且自动回退未完成；请在本机使用runtime/managed-backups私密备份恢复，勿启动服务") from None
        raise SetupError("保存失败，已恢复本次已修改文件；未显示配置内容") from None
    return group.relative_to(root).as_posix()


def _configs(root):
    root = Path(root).resolve()
    for name in ("local.json", "peers.json"):
        if (root / name).is_symlink():
            raise SetupError("配置为链接，须先在本机核对真实路径")
    local, book = _load(root / "local.json"), _load(root / "peers.json")
    if book.get("version") != 1 or not isinstance(book.get("peers"), dict) or not isinstance(local.get("device_id"), str) or not local["device_id"]:
        raise SetupError("登记版本或本机身份无效")
    peers = {}
    for alias, peer in book["peers"].items():
        if not isinstance(alias, str) or not alias or not isinstance(peer, dict) or alias.casefold() in peers:
            raise SetupError("聊天别名无效或不唯一")
        peers[alias.casefold()] = {**peer, "alias": alias}
    return root, local, book, peers


def _counts(root):
    """Aggregate-only SQLite queries: never SELECT text, result, history, or receipts."""
    definitions = {
        "a2a-mailbox.db": [("replies", "state IN ('waiting','interrupted') OR (state='done' AND result IS NOT NULL)"),
                            ("device_deliveries", "state IN ('queued','accepted','uncertain') AND finished IS NULL")],
        "pending-results.db": [("pending_results", "state IN ('prepared','waiting','ready','dispatching')")],
        "device-receipts.db": [("receipts", "acked=0 OR state IN ('prepared','dispatching','uncertain') OR (state='accepted' AND reply_received=0)")],
        "a2a-tasks.db": [("tasks", "upper(coalesce(json_extract(status,'$.state'),'')) NOT IN ('COMPLETED','FAILED','CANCELED','CANCELLED','REJECTED','TASK_STATE_COMPLETED','TASK_STATE_FAILED','TASK_STATE_CANCELED','TASK_STATE_REJECTED')")],
    }
    counts, unavailable = {}, []
    for filename, tables in definitions.items():
        path = root / "runtime" / filename
        if not path.exists():
            continue
        if path.is_symlink():
            unavailable.append(filename)
            continue
        try:
            with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)) as db:
                db.execute("PRAGMA query_only=ON")
                known = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if tables[0][0] not in known:
                    unavailable.append(filename)
                for table, condition in tables:
                    if table in known:
                        columns = {row[1] for row in db.execute("PRAGMA table_info(" + table + ")")}
                        if table == "pending_results" and "mode" in columns:
                            condition = "state IN ('prepared','waiting','ready','dispatching')"
                        elif table == "receipts" and "mode" in columns:
                            unconfirmed = "acked=0"
                            if "ack_expired" in columns:
                                unconfirmed += " AND ack_expired=0"
                            condition = ("(" + unconfirmed + ") OR state IN ('prepared','dispatching') "
                                         "OR (mode<>'message' AND (state='uncertain' "
                                         "OR (state='accepted' AND reply_received=0)))")
                        counts[table] = db.execute("SELECT count(*) FROM " + table + " WHERE " + condition).fetchone()[0]
                if filename == "a2a-tasks.db" and "tasks" not in known and known:
                    unavailable.append(filename)
        except (OSError, sqlite3.Error):
            unavailable.append(filename)
    return {"counts": counts, "active": sum(counts.values()), "unavailable": unavailable}


def inspect(root):
    root, local, book, peers = _configs(root)
    return {"chats": Policy(root, local, peers).chat_list(), "queues": _counts(root),
            "message_delivery": local.get("a2a", {}).get("message_delivery") is True
                                or local.get("network", {}).get("message_delivery") is True}


def prepare(root, verified_refs):
    """Bind only exact, previously registered and actually verified chat addresses."""
    root = Path(root).resolve()
    with _lock(root):
        root, local, book, peers = _configs(root)
        try:
            refs = [chat_ref(ref) for ref in verified_refs]
        except (ValueError, TypeError):
            raise SetupError("须提供已经实际核对的完整原聊天身份") from None
        if not refs or len({tuple(ref) for ref in refs}) != len(refs):
            raise SetupError("须逐条提供不重复的已核对原聊天身份")
        for ref in refs:
            matches = [(alias, peer) for alias, peer in book["peers"].items()
                       if all(peer.get(field) == value for field, value in zip(("device_id", "software", "conversation_id"), ref))]
            if len(matches) != 1:
                raise SetupError("指定原聊天未唯一登记；先在本机通讯录登记，未修改")
            alias, peer = matches[0]
            external = is_external_a2a(peer)
            if external:
                if not external_a2a_ready(local, peer, alias, require_verified=False):
                    raise SetupError("外部A2A须已有安全HTTPS目标、完整原聊天及独立目标凭据；未修改或显示凭据")
                peer["verified_url"] = peer["url"]
                peer["verified_ref"] = ref
            bound = ref[0] == local["device_id"] or ref[0] in local.get("a2a", {}).get("device_tokens", {})
            if local.get("platform") == "minis":
                bound = ref[0] == local["device_id"] or bool(local.get("network", {}).get("token"))
            if not bound and not external:
                raise SetupError("指定原聊天的设备未完成现有配对；未修改")
            peer["verified"] = True
            peer["verified_at"] = int(time.time())
        if local.get("platform") != "minis":
            config = local.setdefault("a2a", {})
            if not isinstance(config, dict):
                raise SetupError("本机服务配置格式无效")
            tokens, bindings = config.setdefault("chat_tokens", {}), config.setdefault("chat_bindings", {})
            if not isinstance(tokens, dict) or not isinstance(bindings, dict):
                raise SetupError("来源绑定配置格式无效")
            for alias, peer in book["peers"].items():
                if is_external_a2a(peer) or peer.get("device_id") != local["device_id"] or peer.get("verified") is not True:
                    continue
                ref = dict(zip(("device_id", "software", "conversation_id"), chat_ref(peer)))
                token_keys = [key for key in tokens if key.casefold() == alias.casefold()]
                binding_keys = [key for key in bindings if key.casefold() == alias.casefold()]
                if len(token_keys) > 1 or len(binding_keys) > 1:
                    raise SetupError("已有来源凭据别名不唯一；未修改")
                token_key = token_keys[0] if token_keys else alias
                binding_key = binding_keys[0] if binding_keys else alias
                old_binding = bindings.get(binding_key)
                if token_keys and (not isinstance(tokens[token_key], str) or len(tokens[token_key]) < 32 or old_binding != ref):
                    raise SetupError("已有来源凭据缺少相同聊天绑定或绑定不同；请本人核查，未修改或轮换凭据")
                if old_binding is not None and old_binding != ref:
                    raise SetupError("已有聊天绑定不同；未修改")
                if not token_keys:
                    tokens[token_key] = secrets.token_urlsafe(48)
                bindings[binding_key] = ref
        backup = _transaction(root, {root / "peers.json": book, root / "local.json": local})
        return {"prepared": True, "verified_count": len(refs), "backup": backup,
                "restart_required": True,
                "note": "登记核对完成；重载后已接入的不同原聊天可互相联系"}


@contextmanager
def _stopped(root, local):
    if local.get("platform") == "minis":
        from device_receiver import _instance_lock
        from workbuddy import BridgeError
        try:
            with _instance_lock(root):
                yield
        except (OSError, BridgeError):
            raise SetupError("手机接收器仍在运行或无法确认已停止；未切换") from None
        return
    config = local.get("a2a", {})
    if config.get("host", "127.0.0.1") != "127.0.0.1":
        raise SetupError("服务监听地址不符合本机限制")
    port = config.get("port", 8788)
    if type(port) is not int or not 1 <= port <= 65535:
        raise SetupError("服务端口配置无效")
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        if os.name == "nt":
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        try:
            probe.bind(("127.0.0.1", port))
            probe.listen(1)
        except OSError:
            raise SetupError("服务端口仍被占用；请本人先停服务并核对占用者，未自动停止任何进程") from None
        yield
    finally:
        probe.close()


def _phone_handoffs(root):
    """The narrow roster correction need not wait for an already ACKed business reply."""
    for filename, table in (("device-receipts.db", "receipts"), ("pending-results.db", "pending_results")):
        path = root / "runtime" / filename
        if not path.exists():
            continue
        if path.is_symlink():
            raise SetupError("无法核对手机原生交付状态")
        try:
            with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)) as db:
                db.execute("PRAGMA query_only=ON")
                columns = {row[1] for row in db.execute("PRAGMA table_info(" + table + ")")}
                if not columns or "state" not in columns or table == "receipts" and "acked" not in columns:
                    raise SetupError("无法核对手机原生交付状态")
                condition = "state='dispatching'"
                if table == "receipts":
                    unconfirmed = "acked=0" + (" AND ack_expired=0" if "ack_expired" in columns else "")
                    condition = "state IN ('prepared','dispatching') OR (" + unconfirmed + ")"
                if db.execute("SELECT count(*) FROM " + table + " WHERE " + condition).fetchone()[0]:
                    raise SetupError("存在正在交付或尚未确认的原生交付，未更新来源通讯录")
        except (OSError, sqlite3.Error):
            raise SetupError("无法核对手机原生交付状态") from None


def bind_phone_sources(root, verified_sources):
    """Correct transport-only peer software using an already paired exact source address."""
    if (not isinstance(verified_sources, (list, tuple)) or not verified_sources
            or any(not isinstance(item, (list, tuple)) or len(item) != 4
                   or any(not isinstance(value, str) or not value for value in item) for item in verified_sources)):
        raise SetupError("须列明别名和已核验的完整来源身份")
    with _lock(root):
        root, local, book, peers = _configs(root)
        if local.get("platform") != "minis":
            raise SetupError("来源通讯录迁移仅适用于手机")
        network = local.get("network", {})
        base = network.get("base_url", "").rstrip("/") if isinstance(network, dict) else ""
        parsed = urlsplit(base)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
                or parsed.query or parsed.fragment or parsed.path):
            raise SetupError("来源地址须使用已配对的HTTPS根地址")
        original = copy.deepcopy(book)
        updated = []
        with _stopped(root, local):
            _phone_handoffs(root)
            for alias, device, software, conversation in verified_sources:
                chat_ref([device, software, conversation])
                source = peers.get(alias.casefold())
                if not source or software == "a2a" or device == local["device_id"]:
                    raise SetupError("手机来源须是已登记的电脑原聊天")
                if source.get("device_id") != device or source.get("conversation_id") != conversation:
                    raise SetupError("来源设备或原聊天与已登记身份不同")
                if (source.get("software") not in {"a2a", software}
                        or source.get("host_software") not in {None, software}
                        or source.get("transport") not in {None, "a2a"}):
                    raise SetupError("已有来源软件信息不符，未覆盖")
                endpoint = base + "/agents/" + quote(source["alias"], safe="")
                if not isinstance(source.get("url"), str) or source["url"].rstrip("/") != endpoint:
                    raise SetupError("来源地址与原配对中央端点不同")
                entry = book["peers"][source["alias"]]
                changed = (entry.get("software"), entry.get("transport"), entry.get("host_software")) != (software, "a2a", software)
                entry.update(software=software, transport="a2a", host_software=software)
                if changed:
                    updated.append(source["alias"])
            backup = _transaction(root, {root / "peers.json": book}) if book != original else None
        return {"phone_sources_updated": len(updated), "sources": updated,
                "backup": backup, "restart_required": bool(updated)}


def main():
    parser = argparse.ArgumentParser(description="核对原聊天接入；不接收或显示密钥")
    parser.add_argument("action", choices=("inspect", "prepare", "phone-sources"))
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--verify", nargs=3, action="append", metavar=("DEVICE", "SOFTWARE", "CHAT"))
    parser.add_argument("--bind", nargs=4, action="append", metavar=("ALIAS", "DEVICE", "SOFTWARE", "CHAT"))
    args = parser.parse_args()
    try:
        if args.action == "inspect":
            result = inspect(args.root)
        elif args.action == "prepare":
            result = prepare(args.root, args.verify or [])
        else:
            result = bind_phone_sources(args.root, args.bind or [])
        print(json.dumps(result, ensure_ascii=True))
    except Exception as error:
        safe = str(error) if isinstance(error, SetupError) else "检查或保存失败（" + type(error).__name__ + "）；未显示私密内容"
        print(json.dumps({"error": safe}, ensure_ascii=True))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
