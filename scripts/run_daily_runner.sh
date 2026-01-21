#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/Users/torsteinulvik/code/Aqua-historikk"
RUN_SCRIPT="$PROJECT_ROOT/run_daily.sh"

LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

STAMP="$(date +%Y-%m-%d)"
LOG_FILE="$LOG_DIR/run_daily_${STAMP}.log"

# Lock med mkdir (atomisk på macOS)
LOCK_DIR="/tmp/aqua_historikk_run_daily.lockdir"

# Retry-konfig
MAX_ATTEMPTS=3
RETRY_SLEEP_SECONDS=300

ts() {
  date "+%Y-%m-%dT%H:%M:%S%z"
}

# Send ALL output (stdout + stderr) både til skjerm og loggfil
exec > >(tee -a "$LOG_FILE") 2>&1

echo "==> $(ts) Runner start"
cd "$PROJECT_ROOT"

cleanup_lock() {
  rmdir "$LOCK_DIR" 2>/dev/null || true
}

attempt=1
while (( attempt <= MAX_ATTEMPTS )); do
  echo "==> $(ts) Attempt ${attempt}/${MAX_ATTEMPTS}"

  # Prøv å ta lock (mkdir er atomisk)
  if mkdir "$LOCK_DIR" 2>/dev/null; then
    # Sørg for at lock alltid fjernes når scriptet avslutter
    trap cleanup_lock EXIT INT TERM

    # Kjør jobben
    if "$RUN_SCRIPT"; then
      echo "==> $(ts) Attempt ${attempt} succeeded"
      echo "==> $(ts) Runner done"
      exit 0
    else
      rc=$?
      echo "!! $(ts) Attempt ${attempt} failed (exit=$rc)"
      # Slipp lock før retry, ellers låser du deg selv ute
      cleanup_lock
      trap - EXIT INT TERM

      if (( attempt < MAX_ATTEMPTS )); then
        echo "==> $(ts) Sleeping ${RETRY_SLEEP_SECONDS}s before retry..."
        sleep "$RETRY_SLEEP_SECONDS"
      fi

      attempt=$((attempt + 1))
      continue
    fi
  else
    echo "!! $(ts) Another run is already running. Exiting."
    exit 0
  fi
done

echo "!! $(ts) Runner FAILED after ${MAX_ATTEMPTS} attempts"
exit 1
