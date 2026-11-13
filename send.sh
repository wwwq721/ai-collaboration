#!/bin/sh
set -eu
task_root=$(CDPATH= cd -P "$(dirname "$0")" && pwd)
task_python=${PYTHON:-"$task_root/.a2a-env/bin/python"}
if [ ! -x "$task_python" ]; then task_python=python3; fi
exec "$task_python" -X utf8 -B "$task_root/scripts/send.py" "$@"
