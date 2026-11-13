#!/bin/sh
set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
TASK_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
PYTHON_BIN=${PYTHON:-"$TASK_ROOT/.a2a-env/bin/python"}
if [ ! -x "$PYTHON_BIN" ]; then PYTHON_BIN=python3; fi
exec "$PYTHON_BIN" "$SCRIPT_DIR/receiver_manager.py" "$@"
