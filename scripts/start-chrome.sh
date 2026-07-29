#!/usr/bin/env bash
# Chrome 自动启动脚本 (9224 轻松农场专用低资源模式)
set -euo pipefail

export NO_PROXY=127.0.0.1,localhost
export no_proxy=127.0.0.1,localhost
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY all_proxy 2>/dev/null || true

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${FARM_CDP_PORT:-9224}"
PROFILE_DIR="${FARM_PROFILE_DIR:-$ROOT/chrome-profile}"
LOG_DIR="${FARM_LOG_DIR:-$ROOT/logs}"

mkdir -p "$LOG_DIR" "$PROFILE_DIR"

if curl -sS --max-time 2 "http://127.0.0.1:$PORT/json/version" >/dev/null 2>&1; then
  echo "chrome $PORT already up"
  exit 0
fi

rm -f "$PROFILE_DIR/SingletonLock" "$PROFILE_DIR/SingletonCookie" "$PROFILE_DIR/SingletonSocket" 2>/dev/null || true

CHROME_BIN=$(command -v google-chrome || command -v chromium || command -v chromium-browser || true)
if [ -z "$CHROME_BIN" ]; then
  echo "google-chrome binary not found"
  exit 1
fi

"$CHROME_BIN" \
  --headless=new --no-sandbox --disable-dev-shm-usage \
  --remote-debugging-port="$PORT" --remote-debugging-address=127.0.0.1 \
  --user-data-dir="$PROFILE_DIR" \
  --no-first-run --no-default-browser-check \
  --window-size=800,450 \
  --disable-gpu --disable-gpu-compositing --disable-gpu-rasterization \
  --disable-software-rasterizer --disable-accelerated-2d-canvas \
  --disable-accelerated-video-decode --disable-accelerated-video-encode \
  --disable-gl-drawing-for-tests --disable-gpu-vsync --use-gl=disabled \
  --renderer-process-limit=1 \
  --js-flags=--max-old-space-size=128 \
  --disk-cache-size=16777216 --mute-audio \
  about:blank >>"$LOG_DIR/chrome-$PORT.log" 2>&1 &

echo $! > "$ROOT/chrome.pid"
echo "started chrome pid=$(cat "$ROOT/chrome.pid") on port $PORT"

for i in $(seq 1 20); do
  if curl -sS --max-time 1 "http://127.0.0.1:$PORT/json/version" >/dev/null 2>&1; then
    echo cdp_ready; exit 0
  fi
  sleep 0.5
done

echo cdp_not_ready; exit 2
