#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PORT="${TRADINGAGENTS_WEB_PORT:-8090}"
HOST="${TRADINGAGENTS_WEB_HOST:-127.0.0.1}"
RECONCILE_INTERVAL="${TRADINGAGENTS_WEB_RECONCILE_INTERVAL:-3}"

LOG_FILE="${TRADINGAGENTS_WEB_LOG_FILE:-/tmp/tradingagents_web_service.log}"
PID_FILE="${TRADINGAGENTS_WEB_PID_FILE:-/tmp/tradingagents_web_service.pid}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "[ERROR] python3 not found in PATH" >&2
  exit 1
fi

if lsof -iTCP:"$PORT" -sTCP:LISTEN -n -P >/dev/null 2>&1; then
  echo "[ERROR] Port $PORT is already in use." >&2
  lsof -iTCP:"$PORT" -sTCP:LISTEN -n -P || true
  exit 1
fi

nohup env \
  TRADINGAGENTS_WEB_HOST="$HOST" \
  TRADINGAGENTS_WEB_PORT="$PORT" \
  TRADINGAGENTS_WEB_RECONCILE_INTERVAL="$RECONCILE_INTERVAL" \
  python3 -m cli.web >"$LOG_FILE" 2>&1 < /dev/null &

PID=$!
echo "$PID" > "$PID_FILE"

sleep 1
if ! ps -p "$PID" >/dev/null 2>&1; then
  echo "[ERROR] Web service failed to start. Check log: $LOG_FILE" >&2
  tail -n 120 "$LOG_FILE" || true
  exit 1
fi

echo "[OK] TradingAgents Web UI started"
echo "  PID:  $PID"
echo "  URL:  http://$HOST:$PORT"
echo "  LOG:  $LOG_FILE"
echo "  PIDF: $PID_FILE"
echo
echo "Health check: curl -I http://$HOST:$PORT"
