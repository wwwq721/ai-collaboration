"""Isolated public upgrade tests. No real configuration, network, or deployment."""
import io
import json
import os
from pathlib import Path
import stat
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import manage_upgrade as upgrade


def fallback_archive(files):
    manifest = {"format": 1, "kind": "mcp-fallback",
                "files": {name: upgrade.digest(files[name]) for name in sorted(files)}}
    payload = {upgrade.MCP_FALLBACK_MANIFEST: upgrade.json_bytes(manifest),
               **{name: files[name] for name in sorted(files)}}
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name, body in payload.items():
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = (stat.S_IFREG | 0o644) << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, body, compresslevel=9)
    return buffer.getvalue()


class UpgradeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.source, self.installed = self.base / "source", self.base / "installed"
        for root in (self.source, self.installed):
            (root / "scripts").mkdir(parents=True)
        (self.source / "scripts/send.py").write_bytes(b"new public program")
        (self.installed / "scripts/send.py").write_bytes(b"old public program")
        (self.source / "scripts/receiver.sh").write_bytes(b"#!/bin/sh\nexit 0\n")
        (self.source / "ui").mkdir()
        (self.source / "ui/app.js").write_bytes(b"public ui")
        # Construct a complete public tree: incomplete upgrade archives must fail closed.
        for name in upgrade.allowed("desktop"):
            path=self.source/name
            if not path.exists():
                path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b"public fixture")
        self.fallback_files = {name: b"public dormant fixture: " + name.encode("ascii")
                               for name in upgrade.MCP_FALLBACK_FILES}
        (self.source / upgrade.MCP_FALLBACK).write_bytes(fallback_archive(self.fallback_files))
        # Forbidden files deliberately exist in both trees. Build/prepare must ignore them.
        self.private = ("local.json", "peers.json", "runtime/a2a-tasks.db", "runtime/ui-auth.json", "tests/test_private.py", ".a2a-env/secret")
        for root in (self.source, self.installed):
            for name in self.private:
                path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b"fictional private marker")
        self.package, self.backup = self.base / "update.zip", self.base / "backup-v1"
        self.result = upgrade.build(self.source,self.installed,"mobile","v1",self.package)
        self.sha = self.result["sha256"]

    def prepare(self):
        return upgrade.prepare(self.installed,self.package,self.sha,"mobile",self.backup)

    def apply(self):
        return upgrade.apply(self.installed,self.package,self.sha,"mobile",self.backup,quiescent=True)

    def rewrite_zip(self, entries):
        content=io.BytesIO()
        with zipfile.ZipFile(content,"w") as archive:
            for name,body in entries:archive.writestr(name,body)
        data=content.getvalue();self.package.write_bytes(data);self.sha=upgrade.digest(data)

    def test_all_current_public_modules_have_explicit_allowlist_entries(self):
        scripts=Path(__file__).resolve().parents[1]/"scripts"
        current={path.name for path in scripts.iterdir() if path.suffix in (".py",".sh",".cjs")}
        self.assertLessEqual(current,upgrade.SCRIPT_NAMES)
        self.assertLessEqual({"scripts/managed_bridge.py","scripts/managed_setup.py","scripts/manage_upgrade.py"},upgrade.allowed("desktop"))
        retired = {"scripts/ui_api.py", "scripts/activity.py", "ui/index.html", "ui/app.js", "ui/style.css",
                   "tests/test_ui_api.py", "tests/test_activity.py"}
        self.assertTrue(retired.isdisjoint(upgrade.allowed("desktop")))
        self.assertTrue(retired.isdisjoint(upgrade.allowed("mobile")))

    def test_mobile_and_desktop_exclude_retired_ui(self):
        manifest,payload=upgrade.verify(self.package,self.sha,"mobile")
        self.assertEqual(set(payload),upgrade.allowed("mobile"))
        self.assertEqual(len(payload), 36)
        self.assertNotIn(upgrade.MCP_FALLBACK, payload)
        self.assertTrue(upgrade.MCP_FALLBACK_FILES.isdisjoint(payload))
        self.assertTrue({"scripts/managed_bridge.py","scripts/managed_setup.py","scripts/manage_upgrade.py","scripts/access_policy.py"} <= set(payload))
        output=self.base/"desktop.zip"
        result=upgrade.build(self.source,self.installed,"desktop","v1",output)
        self.assertFalse(any(name.startswith("ui/") for name in upgrade.verify(output,result["sha256"],"desktop")[1]))
        for name in self.private:self.assertNotIn(name,manifest["files"])
        with self.assertRaises(upgrade.UpgradeError):upgrade.verify(self.package,self.sha,"desktop")

    def test_prepare_apply_restore_preserves_private_state(self):
        self.prepare()
        self.assertEqual((self.installed/"scripts/send.py").read_bytes(),b"old public program")
        self.apply()
        self.assertEqual((self.installed/"scripts/send.py").read_bytes(),b"new public program")
        if os.name != "nt":self.assertTrue((self.installed/"scripts/receiver.sh").stat().st_mode & stat.S_IXUSR)
        upgrade.restore(self.installed,self.backup,quiescent=True)
        self.assertEqual((self.installed/"scripts/send.py").read_bytes(),b"old public program")
        self.assertFalse((self.installed/"scripts/receiver.sh").exists())
        for name in self.private:self.assertEqual((self.installed/name).read_bytes(),b"fictional private marker")
        self.assertFalse((self.backup/"files/local.json").exists())

    def test_missing_required_code_or_guide_refused(self):
        for name,target in (("scripts/managed_setup.py","mobile"),("scripts/managed_bridge.py","desktop"),("references/setup.md","desktop")):
            path=self.source/name;body=path.read_bytes();path.unlink()
            with self.assertRaises(upgrade.UpgradeError):upgrade.build(self.source,self.installed,target,"v1",self.base/(target+"-missing.zip"))
            path.write_bytes(body)
        with zipfile.ZipFile(self.package) as archive:entries=[(x,archive.read(x)) for x in archive.namelist()]
        manifest=json.loads(dict(entries)[upgrade.MANIFEST]);del manifest["files"]["scripts/managed_setup.py"]
        entries=[(name,upgrade.json_bytes(manifest) if name==upgrade.MANIFEST else body) for name,body in entries if name!="scripts/managed_setup.py"]
        self.rewrite_zip(entries)
        with self.assertRaises(upgrade.UpgradeError):upgrade.verify(self.package,self.sha,"mobile")

    def test_desktop_contains_only_explicit_tests_and_restores_existing_test(self):
        registered = {p.name for p in Path(__file__).parent.glob("test_*.py")}
        self.assertEqual(upgrade.TEST_NAMES, registered)
        path=self.installed/"tests/test_managed_bridge.py";path.parent.mkdir(exist_ok=True);path.write_bytes(b"old test")
        output=self.base/"desktop-tests.zip";backup=self.base/"desktop-backup"
        result=upgrade.build(self.source,self.installed,"desktop","v2",output)
        manifest,payload=upgrade.verify(output,result["sha256"],"desktop")
        self.assertTrue(upgrade.TESTS <= set(payload));self.assertNotIn("tests/test_private.py",payload)
        self.assertFalse(any(name.startswith("tests/") for name in upgrade.allowed("mobile")))
        upgrade.prepare(self.installed,output,result["sha256"],"desktop",backup)
        upgrade.apply(self.installed,output,result["sha256"],"desktop",backup,quiescent=True)
        self.assertEqual(path.read_bytes(),b"public fixture")
        upgrade.restore(self.installed,backup,quiescent=True);self.assertEqual(path.read_bytes(),b"old test")

    def test_inventory_only_public_hashes_with_null_for_missing(self):
        real=Path.read_bytes;seen=[]
        def guarded(path):
            seen.append(str(path.relative_to(self.installed)).replace("\\","/"));return real(path)
        with patch.object(Path,"read_bytes",guarded):value=upgrade.inventory(self.installed,"mobile")
        self.assertEqual(set(value),{"format","target","files"});self.assertEqual(set(value['files']),upgrade.allowed('mobile'))
        self.assertEqual(value['files']['scripts/send.py'],upgrade.digest(b"old public program"));self.assertIsNone(value['files']['scripts/managed_setup.py'])
        self.assertTrue(set(seen) <= upgrade.allowed('mobile'));self.assertNotIn('local.json',seen)

    def test_hash_inventory_build_and_actual_local_baseline_still_checked(self):
        value=upgrade.inventory(self.installed,'mobile');path=self.base/'hashes.json';path.write_bytes(upgrade.json_bytes(value))
        output=self.base/'from-manifest.zip'
        result=upgrade.build(self.source,target='mobile',version='v2',output=output,baseline_manifest=path)
        upgrade.prepare(self.installed,output,result['sha256'],'mobile',self.backup)
        (self.installed/'scripts/send.py').write_bytes(b"changed after inventory")
        with self.assertRaises(upgrade.UpgradeError):upgrade.apply(self.installed,output,result['sha256'],'mobile',self.backup,quiescent=True)
        with self.assertRaises(upgrade.UpgradeError):upgrade.build(self.source,self.installed,'mobile','v3',self.base/'both.zip',baseline_manifest=path)

    def test_inventory_rejects_unknown_missing_and_traversal_fields(self):
        original=upgrade.inventory(self.installed,'mobile');path=self.base/'invalid.json'
        values=[]
        for key in ('../local.json','local.json','tests/test_private.py','scripts/unknown.py'):
            value=json.loads(json.dumps(original));value['files'][key]=None;values.append(value)
        value=json.loads(json.dumps(original));del value['files']['scripts/send.py'];values.append(value)
        value=json.loads(json.dumps(original));value['files']['scripts/send.py']='invalid';values.append(value)
        value=json.loads(json.dumps(original));value['secret']='forbidden';values.append(value)
        value=json.loads(json.dumps(original));value['target']='desktop';values.append(value)
        for value in values:
            path.write_bytes(upgrade.json_bytes(value))
            with self.assertRaises(upgrade.UpgradeError):upgrade.build(self.source,target='mobile',version='bad',output=self.base/'bad.zip',baseline_manifest=path)
        path.write_text('{"format":1,"format":1,"target":"mobile","files":{}}')
        with self.assertRaises(upgrade.UpgradeError):upgrade.inventory_manifest(path,'mobile')

    def test_portable_templates_never_read_installed_private_identities(self):
        (self.source/'local.example.json').write_bytes(b"do not read installed edited example")
        real=Path.read_bytes;seen=[]
        def guarded(path):
            name=str(path.relative_to(self.source)).replace("\\","/");seen.append(name)
            if name not in upgrade.allowed('desktop'):raise AssertionError('private read attempted: '+name)
            return real(path)
        output=self.base/'portable.zip'
        with patch.object(Path,'read_bytes',guarded):result=upgrade.portable(self.source,output)
        with zipfile.ZipFile(output) as archive:
            expected=upgrade.allowed('desktop')|{'local.example.json','peers.json',upgrade.PORTABLE_MANIFEST}
            self.assertEqual(set(archive.namelist()),expected)
            peers=json.loads(archive.read('peers.json'));peer=next(iter(peers['peers'].values()))
            self.assertFalse(peer['verified']);self.assertFalse(peer['enabled']);self.assertTrue(peer['conversation_id'].startswith('TEMPLATE-'))
            manifest=json.loads(archive.read(upgrade.PORTABLE_MANIFEST));self.assertEqual(manifest['kind'],'portable');self.assertEqual(set(manifest['templates']),{'local.example.json','peers.json'})
            for name,sha in manifest['files'].items():self.assertEqual(upgrade.digest(archive.read(name)),sha)
            self.assertNotIn(b'fictional private marker',b''.join(archive.read(n) for n in archive.namelist()))
            self.assertEqual([name for name in archive.namelist() if name.endswith('.zip')], [upgrade.MCP_FALLBACK])
            self.assertTrue(upgrade.MCP_FALLBACK_FILES.isdisjoint(archive.namelist()))
            self.assertFalse(any(name.startswith(('runtime/', '.a2a-env/', '.mcp-env/')) for name in archive.namelist()))
            dormant_manifest, dormant = upgrade.verify_mcp_fallback(archive.read(upgrade.MCP_FALLBACK))
            self.assertEqual(set(dormant), upgrade.MCP_FALLBACK_FILES)
            self.assertEqual(len(dormant_manifest['files']), 8)
        self.assertEqual(result['templates'],['local.example.json','peers.json'])
        with self.assertRaises(upgrade.UpgradeError):upgrade.portable(self.source,output)
        upgrade.portable(self.source,output,replace=True)

    def test_real_dormant_archive_hashes_and_bytes_are_reproducible(self):
        root = Path(__file__).resolve().parents[1]
        raw = (root / upgrade.MCP_FALLBACK).read_bytes()
        manifest, files = upgrade.verify_mcp_fallback(raw)
        self.assertEqual(len(files), 8)
        self.assertTrue(all(upgrade.digest(body) == manifest['files'][name] for name, body in files.items()))
        self.assertEqual(upgrade.digest(fallback_archive(files)), upgrade.digest(raw))
        self.assertTrue(all(not (root / name).exists() for name in upgrade.MCP_FALLBACK_FILES))
        self.assertTrue(upgrade.MCP_FALLBACK_FILES.isdisjoint(upgrade.allowed('desktop')))

    def test_dormant_archive_rejects_private_paths_and_corrupted_hashes(self):
        for name in ('local.json', 'runtime/private.db', '.mcp-env/private', '../private'):
            with self.subTest(name=name):
                raw = fallback_archive({**self.fallback_files, name: b'fictional forbidden data'})
                with self.assertRaises(upgrade.UpgradeError):
                    upgrade.verify_mcp_fallback(raw)
        good = fallback_archive(self.fallback_files)
        content = io.BytesIO()
        with zipfile.ZipFile(io.BytesIO(good)) as original, zipfile.ZipFile(content, 'w') as archive:
            for name in original.namelist():
                archive.writestr(name, b'changed public body' if name == 'scripts/mcp_actions.py' else original.read(name))
        (self.source / upgrade.MCP_FALLBACK).write_bytes(content.getvalue())
        with self.assertRaises(upgrade.UpgradeError):
            upgrade.portable(self.source, self.base / 'bad-portable.zip')
        # Phones do not inspect or carry a dormant desktop-only archive.
        result = upgrade.build(self.source, self.installed, 'mobile', 'phone', self.base / 'phone-only.zip')
        self.assertEqual(result['files'], 36)

    @unittest.skipUnless(os.name=='nt','PowerShell Pack is Windows-specific')
    def test_powershell_pack_uses_portable_with_fictional_root(self):
        powershell=shutil.which('powershell.exe')
        if not powershell:self.skipTest('PowerShell unavailable')
        real_root=Path(__file__).resolve().parents[1]
        shutil.copyfile(real_root/'setup.ps1',self.source/'setup.ps1')
        shutil.copyfile(real_root/'scripts/manage_upgrade.py',self.source/'scripts/manage_upgrade.py')
        result=subprocess.run([powershell,'-NoProfile','-ExecutionPolicy','Bypass','-File',str(self.source/'setup.ps1'),'-Mode','Pack','-PackPython',sys.executable],capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=30)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        output=self.base/'ai-collaboration-portable.zip';self.assertTrue(output.is_file())
        with zipfile.ZipFile(output) as archive:
            self.assertIn(upgrade.PORTABLE_MANIFEST,archive.namelist());self.assertNotIn('local.json',archive.namelist())
            self.assertFalse(next(iter(json.loads(archive.read('peers.json'))['peers'].values()))['verified'])
        for name in self.private:self.assertEqual((self.source/name).read_bytes(),b'fictional private marker')

    def test_wrong_hash_or_corrupt_body_refused(self):
        with self.assertRaises(upgrade.UpgradeError):upgrade.verify(self.package,"0"*64,"mobile")
        with zipfile.ZipFile(self.package) as archive:entries=[(x,archive.read(x)) for x in archive.namelist()]
        entries=[(name,b"corrupt" if name=="scripts/send.py" else body) for name,body in entries]
        self.rewrite_zip(entries)
        with self.assertRaises(upgrade.UpgradeError):upgrade.verify(self.package,self.sha,"mobile")

    def test_baseline_change_refused_before_and_after_prepare(self):
        file=self.installed/"scripts/send.py";file.write_bytes(b"manual edit")
        with self.assertRaises(upgrade.UpgradeError):self.prepare()
        self.assertFalse(self.backup.exists())
        file.write_bytes(b"old public program");self.prepare();file.write_bytes(b"later manual edit")
        with self.assertRaises(upgrade.UpgradeError):self.apply()
        self.assertFalse((self.installed/"scripts/receiver.sh").exists())

    def test_quiescent_required_and_foreign_backup_refused(self):
        self.prepare()
        with self.assertRaises(upgrade.UpgradeError):upgrade.apply(self.installed,self.package,self.sha,"mobile",self.backup)
        other=self.base/"other";other.mkdir()
        with self.assertRaises(upgrade.UpgradeError):upgrade.restore(other,self.backup,quiescent=True)
        with self.assertRaises(upgrade.UpgradeError):upgrade.restore(self.installed,self.backup)

    def test_restore_refuses_later_manual_edits_without_partial_changes(self):
        self.prepare();self.apply();(self.installed/"scripts/send.py").write_bytes(b"post-upgrade edit")
        with self.assertRaises(upgrade.UpgradeError):upgrade.restore(self.installed,self.backup,quiescent=True)
        self.assertTrue((self.installed/"scripts/receiver.sh").exists())

    def test_backup_tamper_refused(self):
        self.prepare();(self.backup/"files/scripts/send.py").write_bytes(b"tamper")
        with self.assertRaises(upgrade.UpgradeError):self.apply()

    def test_forbidden_paths_duplicate_and_link_archive_refused(self):
        original=self.package.read_bytes()
        for bad in ("../local.json","/tmp/code","C:/local.json","scripts\\send.py","local.json","runtime/x","tests/test_x.py","ui/app.js"):
            self.package.write_bytes(original)
            with zipfile.ZipFile(self.package) as archive:entries=[(x,archive.read(x)) for x in archive.namelist()]
            self.rewrite_zip(entries+[(bad,b"bad")])
            with self.assertRaises(upgrade.UpgradeError,msg=bad):upgrade.verify(self.package,self.sha,"mobile")
        self.package.write_bytes(original)
        with zipfile.ZipFile(self.package) as archive:entries=[(x,archive.read(x)) for x in archive.namelist()]
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore");self.rewrite_zip(entries+[("scripts/send.py",b"duplicate")])
        with self.assertRaises(upgrade.UpgradeError):upgrade.verify(self.package,self.sha,"mobile")
        link=zipfile.ZipInfo("scripts/send.py");link.create_system=3;link.external_attr=(stat.S_IFLNK|0o777)<<16
        self.rewrite_zip([(name,body) for name,body in entries if name!="scripts/send.py"]+[(link,b"../../local.json")])
        with self.assertRaises(upgrade.UpgradeError):upgrade.verify(self.package,self.sha,"mobile")

    def test_filesystem_links_refused(self):
        path=self.installed/"scripts/send.py";path.unlink()
        try:path.symlink_to(self.installed/"local.json")
        except (OSError,NotImplementedError):self.skipTest("symlink creation unavailable on this host")
        with self.assertRaises(upgrade.UpgradeError):self.prepare()

    def test_hardlink_refused(self):
        path=self.installed/"scripts/send.py";path.unlink()
        try:os.link(self.installed/"local.json",path)
        except OSError:self.skipTest("hardlink unavailable")
        with self.assertRaises(upgrade.UpgradeError):self.prepare()

    def test_partial_apply_failure_restores_prior_public_file(self):
        self.prepare();real=upgrade.atomic;calls=[]
        def fail_once(path,data,mode=0o600):
            calls.append(path)
            if len(calls)==2:raise OSError("injected replace failure")
            return real(path,data,mode)
        with patch.object(upgrade,"atomic",side_effect=fail_once):
            with self.assertRaises(OSError):self.apply()
        self.assertEqual((self.installed/"scripts/send.py").read_bytes(),b"old public program")
        self.assertFalse((self.installed/"scripts/receiver.sh").exists())
        self.assertFalse((self.installed/"SKILL.md").exists())

    def test_archive_expansion_and_unsafe_mode_refused(self):
        with zipfile.ZipFile(self.package) as archive:entries=[(x,archive.read(x)) for x in archive.namelist()]
        value=json.loads(dict(entries)[upgrade.MANIFEST]);value['files']['scripts/send.py']['mode']=0o4777
        self.rewrite_zip([(name,upgrade.json_bytes(value) if name==upgrade.MANIFEST else body) for name,body in entries])
        with self.assertRaises(upgrade.UpgradeError):upgrade.verify(self.package,self.sha,"mobile")
        self.rewrite_zip([(upgrade.MANIFEST,b"{}"),("scripts/send.py",b"x"*(upgrade.MAX_FILE+1))])
        with self.assertRaises(upgrade.UpgradeError):upgrade.verify(self.package,self.sha,"mobile")

    def test_outputs_cannot_overwrite_or_create_configuration(self):
        with self.assertRaises(upgrade.UpgradeError):
            upgrade.build(self.source,self.installed,"mobile","v1",self.installed/"local.json")
        with self.assertRaises(upgrade.UpgradeError):
            upgrade.build(self.source,self.installed,"mobile","v1",self.base/"missing-local.json")
        with self.assertRaises(upgrade.UpgradeError):self.prepare();self.prepare()
        self.assertFalse((self.base/"missing-local.json").exists())

    def test_backup_mode_tamper_refused(self):
        self.prepare();path=self.backup/"backup.json";record=json.loads(path.read_bytes())
        record["files"]["scripts/send.py"]["before_mode"]=0o4777
        path.write_bytes(upgrade.json_bytes(record))
        with self.assertRaises(upgrade.UpgradeError):self.apply()

    def test_fetch_uses_authenticated_existing_transport_and_no_redirects(self):
        import httpx
        settings=dict(network=dict(base_url="https://fixture.invalid",token="fictional-token"))
        (self.installed/"local.json").write_text(json.dumps(settings),encoding="utf-8")
        seen=[]
        def handle(request):
            seen.append(request)
            self.assertEqual(str(request.url),"https://fixture.invalid/package")
            self.assertEqual(request.headers['authorization'],"Bearer fictional-token")
            return httpx.Response(200,content=self.package.read_bytes())
        def factory(**kwargs):
            self.assertIs(kwargs['trust_env'],False);self.assertIs(kwargs['follow_redirects'],False)
            return httpx.Client(**kwargs,transport=httpx.MockTransport(handle))
        output=self.base/"download.zip"
        with patch('network_transport.build_sync_http_options',return_value={}) as options:
            upgrade.fetch(self.installed,output,self.sha,client_factory=factory);options.assert_called_once()
        self.assertEqual(output.read_bytes(),self.package.read_bytes());self.assertEqual(len(seen),1)
        def redirect_factory(**kwargs):return httpx.Client(**kwargs,transport=httpx.MockTransport(lambda r:httpx.Response(302,headers={"location":"https://elsewhere.invalid/"})))
        with patch('network_transport.build_sync_http_options',return_value={}):
            with self.assertRaises(upgrade.UpgradeError) as error:upgrade.fetch(self.installed,self.base/"redirect.zip",self.sha,client_factory=redirect_factory)
        self.assertNotIn('fictional-token',str(error.exception));self.assertFalse((self.base/"redirect.zip").exists())


if __name__ == '__main__':unittest.main()
