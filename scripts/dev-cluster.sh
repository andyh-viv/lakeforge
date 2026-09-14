#!/usr/bin/env bash
# Start (or restart) a local Forge dev cluster in the background.
# Usage: scripts/dev-cluster.sh [executors] [slots]
set -euo pipefail
cd "$(dirname "$0")/.."
EXECUTORS="${1:-2}"
SLOTS="${2:-2}"
BIN="${FORGE_BIN:-target/debug/forge}"
LOG="${FORGE_LOG:-/tmp/forge/local.log}"
PIDFILE=/tmp/forge/local.pid

mkdir -p /tmp/forge
if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  kill "$(cat "$PIDFILE")" || true
  sleep 0.5
fi
rm -rf /tmp/forge/local
RUST_LOG="${RUST_LOG:-info,forge_scheduler=debug,forge_executor=debug}" \
  nohup "$BIN" local --executors "$EXECUTORS" --slots "$SLOTS" >"$LOG" 2>&1 &
echo $! >"$PIDFILE"
for _ in $(seq 1 50); do
  if "$BIN" status >/dev/null 2>&1; then
    echo "forge local cluster up (pid $(cat "$PIDFILE"), log $LOG)"
    exit 0
  fi
  sleep 0.2
done
echo "cluster failed to start; see $LOG" >&2
exit 1
