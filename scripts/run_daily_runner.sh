#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/Users/torsteinulvik/Documents/Aqua-historikk"
RUN_SCRIPT="$PROJECT_ROOT/run_daily.sh"

LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

STAMP="$(/bin/date +%Y-%m-%d)"
LOG_FILE="$LOG_DIR/run_daily_${STAMP}.log"

ts() { /bin/date "+%Y-%m-%dT%H:%M:%S%z"; }

notify_error() {
  /usr/bin/osascript <<EOF
display notification "Daglig kjøring FEILET – sjekk logs/" with title "Aqua-historikk"
EOF
}

on_error() {
  echo "!! $(ts) Runner FAILED"
  notify_error
}

trap on_error ERR


LOCK_DIR="/tmp/aqua_historikk_run_daily.lockdir"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "==> $(ts) Runner start"
cd "$PROJECT_ROOT"

# ---- Lock (macOS-safe) ----
if ! /bin/mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "!! $(ts) Another run is already running. Exiting."
  exit 0
fi

cleanup() {
  /bin/rm -rf "$LOCK_DIR"
  echo "==> $(ts) Lock released"
}
trap cleanup EXIT INT TERM

# ---- Run the actual job ----
"$RUN_SCRIPT"

# ---- Auto-commit & push (ONLY if DB changed) ----
if [[ -n "$(git status --porcelain web/data/aqua.sqlite)" ]]; then
  echo "==> $(ts) Database changed – committing"

  git add web/data/aqua.sqlite
  git commit -m "Update aqua.sqlite ($( /bin/date +%Y-%m-%d ))"

  echo "==> $(ts) Pushing to origin"
  git push
else
  echo "==> $(ts) No database changes – skipping git commit"
fi

echo "==> $(ts) Runner done"
