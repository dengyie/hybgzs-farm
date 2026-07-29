#!/usr/bin/env bash
# Supervisor entry for hybgzs-farm. Foreground only — no nohup/background.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# shellcheck disable=SC1091
[ -f "$ROOT/env.sh" ] && source "$ROOT/env.sh"
export NO_PROXY=127.0.0.1,localhost
export no_proxy=127.0.0.1,localhost
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY all_proxy 2>/dev/null || true
export PYTHONPATH=/root/.local/lib/python3.10/site-packages:${PYTHONPATH:-}
export HOME=/data/hybgzs-farm
export XDG_CACHE_HOME=/data/hybgzs-farm/cache
export FARM_CDP=${FARM_CDP:-http://127.0.0.1:9224}
export FARM_LOG_LEVEL=${FARM_LOG_LEVEL:-INFO}
mkdir -p "$ROOT/logs" "$ROOT/cache"

START_CHROME_BIN="$ROOT/scripts/start-chrome.sh"
[ ! -f "$START_CHROME_BIN" ] && START_CHROME_BIN="$ROOT/bin/start-chrome.sh"

# chrome must exist; start if CDP down. Do not kill healthy chrome.
if ! curl -sS --max-time 2 http://127.0.0.1:9224/json/version >/dev/null 2>&1; then
  echo "[run-supervised] chrome 9224 down -> start-chrome"
  "$START_CHROME_BIN" || true
  # wait a bit more for slow CDP
  for i in $(seq 1 10); do
    if curl -sS --max-time 1 http://127.0.0.1:9224/json/version >/dev/null 2>&1; then
      break
    fi
    sleep 0.5
  done
fi
if ! curl -sS --max-time 2 http://127.0.0.1:9224/json/version >/dev/null 2>&1; then
  echo "[run-supervised] chrome CDP still not ready" >&2
  exit 2
fi

# drop stale pidfile so tools do not think an old bg daemon owns us
rm -f "$ROOT/daemon.pid"
echo $$ > "$ROOT/daemon.pid"
# foreground: supervisor tracks this process
exec python3 "$ROOT/farm_runner.py" daemon
