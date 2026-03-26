#!/usr/bin/env bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$DIR/.pid"

if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "Already running (PID $(cat "$PID_FILE")). Use restart.sh to restart."
  exit 1
fi

cd "$DIR/versions/v1/backend"
PYTHONPATH="$DIR/versions/v1/backend/src" nohup /home/che/anaconda3/bin/python -m uvicorn server:app --host 0.0.0.0 --port 4017 > "$DIR/server.log" 2>&1 &
echo $! > "$PID_FILE"
echo "Started (PID $!). Logs: $DIR/server.log"
