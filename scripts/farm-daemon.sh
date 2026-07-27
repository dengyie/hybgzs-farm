#!/usr/bin/env bash
# VPS/本机挂机入口 — 不 launch/不杀 Chrome；需已有 CDP + 登录态
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export PYTHONUNBUFFERED=1
export FARM_LOG_LEVEL="${FARM_LOG_LEVEL:-INFO}"
# 可选: FARM_CDP=http://127.0.0.1:9222
exec python3 "$ROOT/farm_runner.py" daemon \
  --min-sleep "${FARM_MIN_SLEEP:-60}" \
  --max-sleep "${FARM_MAX_SLEEP:-1800}" \
  --lead "${FARM_LEAD:-45}" \
  --care-every "${FARM_CARE_EVERY:-600}" \
  "$@"
