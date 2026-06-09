#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
. venv_wsl/bin/activate
nohup python3.11 main.py > app.log 2>&1 &
