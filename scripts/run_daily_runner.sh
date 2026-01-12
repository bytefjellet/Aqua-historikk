#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/Users/torsteinulvik/Documents/Aqua-historikk"
RUN_SCRIPT="$PROJECT_ROOT/run_daily.sh"

LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

STAMP="$(/bin/date +%Y-%m-%d)"
LOG_FILE="$LOG_DIR/run_daily_${STAMP}.log"

ts() { /bin/date "+%Y-%m-%dT%H:%M:%S%z"; }

LOCK_DIR="/tmp/aqua_historikk_run_daily.lockdir"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "==> $(ts) Runner start"
cd "$PROJECT_ROOT"

if ! /bin/mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "!! $(ts) Another run is already running. Exiting."
  exit 0
fi

cleanup() {
  /bin/rm -rf "$LOCK_DIR"
  echo "==> $(ts) Lock released"
}
trap cleanup EXIT INT TERM

"$RUN_SCRIPT"

echo "==> $(ts) Runner done"
