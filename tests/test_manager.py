"""Windows launcher regression: captured tool calls finish while service stays up."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import unittest
import urllib.request

FIXTURE = r'''
import argparse,json,threading
from http.server import BaseHTTPRequestHandler,HTTPServer
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--root');p.add_argument('--instance');a=p.parse_args()
root=Path(a.root);config=json.loads((root/'local.json').read_text())['a2a']
class Handler(BaseHTTPRequestHandler):
    def log_message(self,*args): pass
    def respond(self,data):
        body=json.dumps(data).encode();self.send_response(200)
        self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    def do_GET(self):
        self.respond({'service':'ai-collaboration-a2a','instance':a.instance,'agents':['fixture']})
    def do_POST(self):
        self.rfile.read(int(self.headers['Content-Length']));self.respond({'state':'stopping'})
        threading.Thread(target=self.server.shutdown,daemon=True).start()
server=HTTPServer(('127.0.0.1',config['port']),Handler)
server.serve_forever(poll_interval=.05);server.server_close()
'''


@unittest.skipUnless(os.name == "nt", "Windows service manager")
class ManagerTests(unittest.TestCase):
    def test_concurrent_start_captured_handles_stale_pid_and_graceful_stop(self):
        with tempfile.TemporaryDirectory(prefix="ai bridge ") as temporary:
            root = Path(temporary)
            source = Path(__file__).resolve().parent.parent
            (root / "scripts").mkdir()
            (root / "runtime").mkdir()
            shutil.copy2(source / "a2a.ps1", root / "a2a.ps1")
            shutil.copy2(source / "scripts/a2a_launch.py", root / "scripts/a2a_launch.py")
            (root / "scripts/a2a_server.py").write_text(FIXTURE, encoding="utf8")
            subprocess.run([sys.executable, "-m", "venv", "--without-pip", str(root / ".a2a-env")], check=True, capture_output=True)
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            config = {"python": sys.executable, "a2a": {"host": "127.0.0.1", "port": port, "tokens": {"local": "x"*40}}}
            (root / "local.json").write_text(json.dumps(config), encoding="utf8")
            (root / "runtime/a2a-service.json").write_text(json.dumps({"pid": os.getpid(), "started": "0", "instance": "stale", "port": port}), encoding="utf8")

            def emergency_stop():
                try:
                    request = urllib.request.Request("http://127.0.0.1:"+str(port)+"/shutdown", data=b"{}", method="POST")
                    urllib.request.urlopen(request, timeout=2).close()
                except OSError:
                    pass

            def run(mode):
                process = subprocess.Popen(["powershell.exe", "-NoProfile", "-File", str(root / "a2a.ps1"), "-Mode", mode],
                                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    stdout, stderr = process.communicate(timeout=25)
                except subprocess.TimeoutExpired:
                    emergency_stop()  # Release any inherited capture handles.
                    process.kill()
                    process.communicate(timeout=5)
                    raise
                self.assertEqual(process.returncode, 0, stderr.decode("utf8", errors="replace"))
                return json.loads(stdout)

            try:
                with ThreadPoolExecutor(max_workers=2) as pool:
                    replies = list(pool.map(run, ["Start", "Start"]))
                self.assertEqual([r["state"] for r in replies], ["running", "running"])
                self.assertEqual(run("Status")["state"], "running")
                self.assertEqual(run("Stop")["state"], "stopped")
                self.assertEqual(run("Status")["state"], "stopped")
                self.assertFalse((root / "runtime/a2a-service.json").exists())
            finally:
                emergency_stop()


if __name__ == "__main__":
    unittest.main()
