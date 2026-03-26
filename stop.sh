#!/usr/bin/env bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$DIR/.pid"

if [ ! -f "$PID_FILE" ]; then
  echo "Not running (no PID file found)."
  exit 0
fi

PID="$(cat "$PID_FILE")"
if kill -0 "$PID" 2>/dev/null; then
  kill -9 "$PID"
  rm -f "$PID_FILE"
  echo "Stopped (PID $PID)."
else
  echo "Process $PID not found. Cleaning up PID file."
  rm -f "$PID_FILE"
fi
