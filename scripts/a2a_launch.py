"""Launch hidden without inheriting the AI tool's capture pipes on Windows."""
import argparse
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--instance", required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    runtime = root / "runtime"
    runtime.mkdir(exist_ok=True)
    with open(runtime / "a2a-stdin", "wb"):
        pass
    with open(runtime / "a2a-stdin", "rb") as stdin, \
         open(runtime / "a2a-stdout.log", "wb") as stdout, \
         open(runtime / "a2a-stderr.log", "wb") as stderr:
        process = subprocess.Popen(
            [str(root / ".a2a-env/Scripts/python.exe"), "-X", "utf8", "-B",
             str(root / "scripts/a2a_server.py"), "--root", str(root), "--instance", args.instance],
            stdin=stdin, stdout=stdout, stderr=stderr, close_fds=True,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
    print(json.dumps({"pid": process.pid}), flush=True)


if __name__ == "__main__":
    main()
