#!/usr/bin/env bash
set -euo pipefail

PORT="${TRADINGAGENTS_WEB_PORT:-8090}"
PID_FILE="${TRADINGAGENTS_WEB_PID_FILE:-/tmp/tradingagents_web_service.pid}"

stopped=0

if [ -f "$PID_FILE" ]; then
  PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [ -n "$PID" ] && ps -p "$PID" >/dev/null 2>&1; then
    echo "[INFO] Stopping PID from pid file: $PID"
    kill "$PID" || true
    sleep 1
    if ps -p "$PID" >/dev/null 2>&1; then
      echo "[WARN] PID $PID still alive, sending SIGKILL"
      kill -9 "$PID" || true
    fi
    stopped=1
  fi
  rm -f "$PID_FILE"
fi

PIDS_ON_PORT="$(lsof -tiTCP:"$PORT" -sTCP:LISTEN -n -P 2>/dev/null || true)"
if [ -n "$PIDS_ON_PORT" ]; then
  echo "[INFO] Stopping process(es) on port $PORT: $PIDS_ON_PORT"
  for p in $PIDS_ON_PORT; do
    kill "$p" || true
  done
  sleep 1
  stopped=1
fi

if lsof -iTCP:"$PORT" -sTCP:LISTEN -n -P >/dev/null 2>&1; then
  echo "[ERROR] Some process is still listening on port $PORT" >&2
  lsof -iTCP:"$PORT" -sTCP:LISTEN -n -P || true
  exit 1
fi

if [ "$stopped" -eq 1 ]; then
  echo "[OK] TradingAgents Web UI stopped"
else
  echo "[OK] No running web service found on port $PORT"
fi
