#!/bin/bash
set -euo pipefail
cd /mnt/c/Users/jaini/OneDrive/Desktop/Proj/distributed-task-queue
python3 -m venv venv_wsl || true
. venv_wsl/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
nohup python3 main.py > app.log 2>&1 &
PID=$!
echo STARTED:$PID
sleep 2
if [ -f app.log ]; then
  tail -n 50 app.log
else
  echo "No app.log yet"
fi
