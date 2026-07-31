#!/usr/bin/env bash
# Idempotent farm stack check. It monitors business progress, not only PIDs.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG="$ROOT/logs/ensure-stack.log"
mkdir -p "$ROOT/logs"
export NO_PROXY=127.0.0.1,localhost
export no_proxy=127.0.0.1,localhost
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY all_proxy 2>/dev/null || true

{
  now() { date "+%F %T"; }
  echo "$(now) ensure begin"

  if ! curl -sS --max-time 2 http://127.0.0.1:9224/json/version >/dev/null 2>&1; then
    echo "$(now) chrome down -> start"
    "$ROOT/scripts/start-chrome.sh" || true
  else
    echo "$(now) chrome ok"
  fi

  daemon_pid=""
  if [ -f "$ROOT/daemon.pid" ]; then
    daemon_pid="$(cat "$ROOT/daemon.pid" 2>/dev/null || true)"
  fi
  daemon_ok=0
  if [ -n "$daemon_pid" ] && kill -0 "$daemon_pid" 2>/dev/null; then
    cmd="$(tr '\0' ' ' < "/proc/$daemon_pid/cmdline" 2>/dev/null || true)"
    if echo "$cmd" | grep -q 'farm_runner.py daemon'; then daemon_ok=1; fi
  fi

  business_ok=0
  if [ "$daemon_ok" = 1 ] && [ -f "$ROOT/logs/daemon-journal.jsonl" ]; then
    if python3 - "$ROOT/logs/daemon-journal.jsonl" <<'PY'
import json, sys, time
from datetime import datetime

last_ok = None
fail_streak = 0
try:
    with open(sys.argv[1], encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
            except Exception:
                continue
            if row.get("event") == "cycle_ok":
                last_ok, fail_streak = row, 0
            elif row.get("event") in {"cycle_fail", "cdp_missing"}:
                fail_streak += 1
except OSError:
    raise SystemExit(1)

if not last_ok:
    raise SystemExit(1)
try:
    age = max(0, time.time() - datetime.fromisoformat(last_ok["ts"]).timestamp())
except Exception:
    raise SystemExit(1)
after = last_ok.get("after") or {}
urgent = int(after.get("mature") or 0) > 0 or int(after.get("empty") or 0) > 0
limit = 180 if urgent else 900
raise SystemExit(0 if age <= limit and fail_streak < 5 else 1)
PY
    then
      business_ok=1
    fi
  fi

  if [ "$daemon_ok" = 1 ] && [ "$business_ok" = 1 ]; then
    echo "$(now) daemon business-ok pid=$daemon_pid"
  else
    echo "$(now) daemon business-stale-or-down"
    if command -v supervisorctl >/dev/null 2>&1; then
      supervisorctl -c /personal/pxed/supervisord.conf restart hybgzs-farm || true
      echo "$(now) supervisor restart requested"
    else
      "$ROOT/scripts/run-supervised.sh" >>"$ROOT/logs/supervisor.out.log" 2>&1 &
      echo "$(now) fallback daemon start requested"
    fi
  fi
  echo "$(now) ensure end"
} >>"$LOG" 2>&1