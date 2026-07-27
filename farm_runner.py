#!/usr/bin/env python3
"""
hybgzs 轻松农场自动化 (cdk.hybgzs.com/entertainment/farm)

设计:
- 默认一次性 CLI；`daemon` 模式 VPS/本机挂机监控
- 复用 headed 已登录 Chrome；自动发现 CDP；不 launch / 不杀 Chrome
- 页面内 fetch 带 Cookie；写操作 DOM 优先 + API 校验
- 智能: care→harvest→recheck→plant；种子按价值/时长评分，库存优先
- 挂机: 按 next_mature + 灾害巡检间隔休眠；每轮连 CDP→干活→断开，控资源

红线:
1. 不杀 Chrome
2. destroyIfFull 默认 false
3. 购种默认关（--allow-buy）
4. CF/人机 headed 等待，不秒退假装成功
"""

from __future__ import annotations

import argparse
import asyncio
import atexit
import fcntl
import json
import logging
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import websockets

FARM_URL = "https://cdk.hybgzs.com/entertainment/farm"
COIN_DIV = 500_000
DEFAULT_LOG_DIR = Path.home() / ".cache" / "hybgzs-farm"
DEFAULT_LOCK = DEFAULT_LOG_DIR / "farm_runner.lock"
# 资源: 小缓冲、短超时、不常驻
CDP_MAX_SIZE = 4 * 1024 * 1024
CDP_OPEN_TIMEOUT = 8
CDP_CALL_TIMEOUT = 15
PAGE_READY_MAX_POLLS = 8

log = logging.getLogger("hybgzs.farm")


def setup_logging(level: str = "INFO", log_file: Optional[str] = None, json_mode: bool = False) -> Path:
    DEFAULT_LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = Path(log_file) if log_file else DEFAULT_LOG_DIR / f"farm-{datetime.now().strftime('%Y%m%d')}.log"
    root = logging.getLogger("hybgzs.farm")
    root.handlers.clear()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # 文件始终详细
    fh = logging.FileHandler(path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    root.addHandler(fh)
    if not json_mode:
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(getattr(logging, level.upper(), logging.INFO))
        sh.setFormatter(fmt)
        root.addHandler(sh)
    return path


class SingleInstance:
    """文件锁：防止并发多开打爆 CDP/CPU。"""

    def __init__(self, path: Path):
        self.path = path
        self.fp = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fp = open(self.path, "w")
        try:
            fcntl.flock(self.fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.fp.close()
            self.fp = None
            return False
        self.fp.write(f"pid={os.getpid()} started={datetime.now().isoformat()}\n")
        self.fp.flush()
        return True

    def release(self):
        if self.fp:
            try:
                fcntl.flock(self.fp.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                self.fp.close()
            except Exception:
                pass
            self.fp = None


def discover_cdp(ports: Optional[list[int]] = None) -> tuple[Optional[str], Optional[str], Optional[int]]:
    # FARM_CDP / COINBOT_CDP 可指定，如 http://127.0.0.1:9223
    env = (os.environ.get("FARM_CDP") or os.environ.get("COINBOT_CDP") or "").strip().rstrip("/")
    if env.startswith("http"):
        try:
            op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with op.open(env + "/json/version", timeout=2.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                ws = data.get("webSocketDebuggerUrl")
                if ws:
                    # port parse
                    port = None
                    try:
                        port = int(env.rsplit(":", 1)[-1])
                    except Exception:
                        port = None
                    return env, ws, port
        except Exception as e:
            log.warning("cdp.env_fail url=%s err=%s — fallback scan", env, e)
    ports = ports or [9222, 9223, 9226, 9333, 9229]
    for p in ports:
        try:
            op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with op.open(f"http://127.0.0.1:{p}/json/version", timeout=1.2) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                ws = data.get("webSocketDebuggerUrl")
                if ws:
                    return f"http://127.0.0.1:{p}", ws, p
        except Exception:
            continue
    return None, None, None


def coin_fmt(raw: Any) -> str:
    try:
        return f"${int(raw) / COIN_DIV:.2f}"
    except Exception:
        return "$?"


def clip(obj: Any, n: int = 400) -> str:
    s = obj if isinstance(obj, str) else json.dumps(obj, ensure_ascii=False, default=str)
    return s if len(s) <= n else s[:n] + f"…(+{len(s)-n})"


def unwrap_api(resp: Optional[dict]) -> tuple[bool, Any, Optional[str]]:
    if not resp:
        return False, None, "empty response"
    if not resp.get("ok") and resp.get("status", 0) not in (200, 201):
        data = resp.get("data")
        err = None
        if isinstance(data, dict):
            err = (data.get("error") or {}).get("message") or data.get("message")
        return False, data, err or resp.get("error") or f"HTTP {resp.get('status')}"
    data = resp.get("data")
    if isinstance(data, dict) and data.get("success") is False:
        err = (data.get("error") or {}).get("message") or data.get("message") or "success=false"
        return False, data, str(err)
    return True, data, None


def extract_list(payload: Any, *keys: str) -> list:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for k in keys:
        v = payload.get(k)
        if isinstance(v, list):
            return v
    d = payload.get("data")
    if isinstance(d, list):
        return d
    if isinstance(d, dict):
        for k in keys:
            v = d.get(k)
            if isinstance(v, list):
                return v
    return []


class FarmClient:
    def __init__(self, browser_ws: str, http_base: str):
        self.browser_ws = browser_ws
        self.http_base = http_base.rstrip("/")
        self.ws = None
        self.sid = None
        self.target_id = None
        self.req_id = 0
        self.created_tab = False
        self._lock = asyncio.Lock()
        self._status_cache: Optional[dict] = None
        self._status_cache_at = 0.0
        self.stats = {
            "cdp_calls": 0,
            "api_calls": 0,
            "t0": time.monotonic(),
        }

    async def connect(self):
        t0 = time.monotonic()
        self.ws = await websockets.connect(
            self.browser_ws,
            max_size=CDP_MAX_SIZE,
            open_timeout=CDP_OPEN_TIMEOUT,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=3,
        )
        tabs = (await self.call_browser("Target.getTargets")).get("targetInfos", [])
        pages = [t for t in tabs if t.get("type") == "page"]
        target = next((t for t in pages if "entertainment/farm" in (t.get("url") or "")), None)
        if not target:
            target = next((t for t in pages if "hybgzs.com" in (t.get("url") or "")), None)
        if target:
            self.target_id = target["targetId"]
            log.info("cdp.reuse_tab id=%s url=%s", self.target_id[:8], (target.get("url") or "")[:80])
        else:
            create_res = await self.call_browser("Target.createTarget", {"url": "about:blank"})
            self.target_id = create_res["targetId"]
            self.created_tab = True
            log.info("cdp.new_tab id=%s", self.target_id[:8])

        attach = await self.call_browser(
            "Target.attachToTarget", {"targetId": self.target_id, "flatten": True}
        )
        self.sid = attach["sessionId"]
        # 资源: 只开 Page+Runtime；不开 Network（避免事件洪泛占 CPU/内存）
        await self.call_tab("Page.enable")
        await self.call_tab("Runtime.enable")
        log.info("cdp.connected ms=%.0f", (time.monotonic() - t0) * 1000)

    async def call_browser(self, method: str, params: Optional[dict] = None) -> dict:
        async with self._lock:
            self.req_id += 1
            rid = self.req_id
            self.stats["cdp_calls"] += 1
            msg: dict[str, Any] = {"id": rid, "method": method}
            if params is not None:
                msg["params"] = params
            await self.ws.send(json.dumps(msg))
            while True:
                raw = await asyncio.wait_for(self.ws.recv(), timeout=CDP_CALL_TIMEOUT)
                data = json.loads(raw)
                if data.get("id") == rid:
                    if "error" in data:
                        log.error("cdp.browser_err method=%s err=%s", method, data["error"])
                        raise RuntimeError(f"CDP browser {method}: {data['error']}")
                    return data.get("result") or {}

    async def call_tab(self, method: str, params: Optional[dict] = None) -> dict:
        async with self._lock:
            self.req_id += 1
            rid = self.req_id
            self.stats["cdp_calls"] += 1
            msg: dict[str, Any] = {"id": rid, "method": method, "sessionId": self.sid}
            if params is not None:
                msg["params"] = params
            await self.ws.send(json.dumps(msg))
            while True:
                raw = await asyncio.wait_for(self.ws.recv(), timeout=CDP_CALL_TIMEOUT)
                data = json.loads(raw)
                if data.get("id") == rid:
                    if "error" in data:
                        log.error("cdp.tab_err method=%s err=%s", method, data["error"])
                        raise RuntimeError(f"CDP tab {method}: {data['error']}")
                    return data.get("result") or {}

    async def evaluate(self, expression: str, await_promise: bool = False) -> Any:
        res = await self.call_tab(
            "Runtime.evaluate",
            {
                "expression": expression,
                "returnByValue": True,
                "awaitPromise": await_promise,
            },
        )
        return (res.get("result") or {}).get("value")

    async def ensure_page_loaded(self) -> bool:
        url = await self.evaluate("location.href")
        if FARM_URL not in str(url):
            log.info("cdp.navigate url=%s", FARM_URL)
            await self.call_tab("Page.navigate", {"url": FARM_URL})
            await asyncio.sleep(1.5)

        closed = await self.evaluate(
            """(() => {
              const btns=[...document.querySelectorAll('button')];
              const known=btns.find(x=>(x.innerText||'').includes('我知道了'));
              if(known){known.click();return 'closed';}
              return 'none';
            })()"""
        )
        if closed == "closed":
            log.info("ui.notice_closed")

        for i in range(PAGE_READY_MAX_POLLS):
            text = str(await self.evaluate("document.body?document.body.innerText.slice(0,2000):''") or "")
            if "获取农场数据失败" in text or "重新登录" in text:
                log.warning("ui.load_fail poll=%s — retry button", i)
                await self.evaluate(
                    """(() => {
                      const b=[...document.querySelectorAll('button')].find(x=>(x.innerText||'').includes('重试'));
                      if(b){b.click();return 'retry';} return 'no';
                    })()"""
                )
                await asyncio.sleep(1.5)
                text = str(await self.evaluate("document.body?document.body.innerText.slice(0,2000):''") or "")
                if "获取农场数据失败" in text or "重新登录" in text:
                    raise RuntimeError("农场数据加载失败/未登录。请在 headed Chrome 登录 cdk.hybgzs.com")
            if "轻松农场" in text or "我的农田" in text or "一键务农" in text:
                log.debug("ui.ready poll=%s", i)
                return True
            await asyncio.sleep(0.5)
        log.warning("ui.ready_timeout — continue anyway")
        return True

    async def fetch_api(self, path: str, method: str = "GET", body: Any = None) -> dict:
        self.stats["api_calls"] += 1
        t0 = time.monotonic()
        js = f"""(async () => {{
            try {{
                const opts = {{
                    method: {json.dumps(method)},
                    headers: {{ 'Content-Type': 'application/json' }},
                    credentials: 'include'
                }};
                const body = {json.dumps(body)};
                if (body !== null && body !== undefined) opts.body = JSON.stringify(body);
                const res = await fetch({json.dumps(path)}, opts);
                const data = await res.json().catch(() => null);
                return {{ status: res.status, ok: res.ok, data }};
            }} catch (e) {{
                return {{ status: 0, ok: false, error: String(e && e.message || e) }};
            }}
        }})()"""
        out = await self.evaluate(js, await_promise=True) or {"ok": False, "status": 0}
        ms = (time.monotonic() - t0) * 1000
        ok, payload, err = unwrap_api(out)
        level = logging.INFO if method != "GET" else logging.DEBUG
        log.log(
            level,
            "api %s %s status=%s ok=%s ms=%.0f err=%s body=%s",
            method,
            path,
            out.get("status"),
            ok,
            ms,
            err,
            clip(payload, 240) if method != "GET" else clip(payload, 120),
        )
        return out

    async def get_farm_status(self, use_cache_s: float = 0.0) -> dict:
        if use_cache_s > 0 and self._status_cache and (time.monotonic() - self._status_cache_at) < use_cache_s:
            log.debug("status.cache_hit age=%.2f", time.monotonic() - self._status_cache_at)
            return self._status_cache

        await self.ensure_page_loaded()
        # 顺序拉取；需要种植时才拉 seeds（由调用方 need_seeds）
        res_crops = await self.fetch_api("/api/farm/crops")
        res_energy = await self.fetch_api("/api/farm/energy/status")
        res_wallet = await self.fetch_api("/api/wallet/balance")
        res_inv = await self.fetch_api("/api/farm/inventory")

        ok_c, crops_payload, err_c = unwrap_api(res_crops)
        if not ok_c:
            raise RuntimeError(f"crops API 失败: {err_c}")

        crops_root = crops_payload if isinstance(crops_payload, dict) else {}
        crops = extract_list(crops_payload, "crops", "data")
        if not crops and isinstance(crops_payload, list):
            crops = crops_payload
        max_slots = 10
        if isinstance(res_crops.get("data"), dict):
            max_slots = int(res_crops["data"].get("maxSlots") or res_crops["data"].get("baseSlots") or 10)
        if isinstance(crops_root, dict):
            max_slots = int(crops_root.get("maxSlots") or crops_root.get("baseSlots") or max_slots)

        ok_e, energy_payload, _ = unwrap_api(res_energy)
        energy = {}
        if ok_e and isinstance(energy_payload, dict):
            energy = energy_payload.get("data") if isinstance(energy_payload.get("data"), dict) else energy_payload

        ok_w, wallet_payload, _ = unwrap_api(res_wallet)
        balance_raw = 0
        if ok_w and isinstance(wallet_payload, dict):
            w = wallet_payload.get("data") if isinstance(wallet_payload.get("data"), dict) else wallet_payload
            if isinstance(w, dict):
                balance_raw = int((w.get("wallet") or {}).get("balance") or w.get("total") or 0)

        ok_i, inv_payload, _ = unwrap_api(res_inv)
        inventory = extract_list(inv_payload, "inventory", "data")
        warehouse = {}
        if isinstance(inv_payload, dict):
            warehouse = inv_payload.get("warehouse") or {}
            if not inventory and isinstance(inv_payload.get("data"), list):
                inventory = inv_payload["data"]

        mature = [c for c in crops if c.get("isMature") and not c.get("isHarvested")]
        growing = [c for c in crops if not c.get("isMature") and not c.get("isHarvested")]
        debuff_crops = []
        for c in growing:
            conds = c.get("conditions") or []
            has = bool(conds) or any(c.get(k) for k in ("thirstyStartedAt", "weedStartedAt", "pestStartedAt"))
            if has:
                debuff_crops.append(c)

        empty_slots = max(0, max_slots - len([c for c in crops if not c.get("isHarvested")]))
        next_mature_sec = None
        if growing:
            rems = [int(c.get("remainingTime") or 0) for c in growing]
            next_mature_sec = min(rems) if rems else None

        inv_qty = {str(i.get("seedId")): int(i.get("quantity") or 0) for i in inventory if i.get("seedId")}
        st = {
            "max_slots": max_slots,
            "planted_count": len(crops),
            "empty_slots": empty_slots,
            "mature_count": len(mature),
            "growing_count": len(growing),
            "debuff_count": len(debuff_crops),
            "energy_current": int(energy.get("currentEnergy") or 0),
            "energy_max": int(energy.get("maxEnergy") or 80),
            "energy": f"{int(energy.get('currentEnergy') or 0)}/{int(energy.get('maxEnergy') or 80)}",
            "balance_raw": balance_raw,
            "balance": coin_fmt(balance_raw),
            "crops": crops,
            "inventory": inventory,
            "inventory_qty": inv_qty,
            "seeds": [],  # lazy
            "warehouse": warehouse,
            "next_mature_sec": next_mature_sec,
            "suggestions": self._suggest(len(mature), len(debuff_crops), empty_slots, inv_qty, warehouse),
        }
        self._status_cache = st
        self._status_cache_at = time.monotonic()
        log.info(
            "status mature=%s growing=%s debuff=%s empty=%s bal=%s energy=%s warehouse=%s/%s next_mature=%s",
            st["mature_count"],
            st["growing_count"],
            st["debuff_count"],
            st["empty_slots"],
            st["balance"],
            st["energy"],
            (warehouse or {}).get("usedCapacity"),
            (warehouse or {}).get("capacity"),
            st["next_mature_sec"],
        )
        return st

    async def load_seeds(self, st: dict) -> list:
        if st.get("seeds"):
            return st["seeds"]
        res = await self.fetch_api("/api/farm/seeds")
        ok, payload, err = unwrap_api(res)
        seeds = []
        if ok:
            if isinstance(payload, dict):
                seeds = payload.get("seeds") or extract_list(payload, "seeds", "data")
            elif isinstance(payload, list):
                seeds = payload
        else:
            log.warning("seeds.load_fail err=%s", err)
        st["seeds"] = seeds or []
        log.info("seeds.loaded n=%s", len(st["seeds"]))
        return st["seeds"]

    def _suggest(self, mature, debuff, empty, inv_qty, warehouse) -> list[str]:
        s = []
        if debuff:
            s.append(f"建议务农：{debuff} 株有灾害")
        if mature:
            s.append(f"建议收菜：{mature} 株成熟")
            used = int((warehouse or {}).get("usedCapacity") or 0)
            cap = int((warehouse or {}).get("capacity") or 0)
            if cap and used >= cap * 0.9:
                s.append(f"仓库将满 {used}/{cap}")
        if empty:
            stock = sum(inv_qty.values())
            if stock >= empty:
                s.append(f"建议补种：空{empty} 库存{stock}")
            elif stock > 0:
                s.append(f"部分补种：空{empty} 库存{stock}（默认不买）")
            else:
                s.append(f"空{empty} 无库存（需 --allow-buy）")
        if not s:
            s.append("无事可做（生长中）")
        return s

    def score_seed(self, seed: dict, inv_qty: dict[str, int]) -> float:
        sid = str(seed.get("id") or seed.get("seedId") or "")
        try:
            gt = max(1, int(seed.get("growthTime") or 1))
            hv = int(seed.get("harvestValue") or 0)
            hq = int(seed.get("harvestQuantity") or 1)
            base = (hv * max(hq, 1)) / gt
        except Exception:
            base = 0.0
        stock = inv_qty.get(sid, 0)
        if stock > 0:
            base *= 1.25
            base += stock * 0.01
        if seed.get("isVipOnly") and stock <= 0:
            base *= 0.05
        if seed.get("isEnabled") is False:
            base = -1.0
        return base

    def plan_planting(
        self,
        empty: int,
        inventory: list,
        seeds: list,
        balance_raw: int,
        allow_buy: bool,
        prefer_seed: Optional[str] = None,
    ) -> list[dict]:
        if empty <= 0:
            return []
        inv_qty = {str(i.get("seedId")): int(i.get("quantity") or 0) for i in inventory if i.get("seedId")}
        seed_by_id: dict[str, dict] = {}
        for s in seeds:
            sid = str(s.get("id") or "")
            if sid:
                seed_by_id[sid] = s
        for i in inventory:
            sid = str(i.get("seedId") or "")
            if sid and sid not in seed_by_id:
                seed_by_id[sid] = {
                    "id": sid,
                    "name": i.get("seedName") or sid,
                    "growthTime": 1800,
                    "harvestValue": i.get("recyclePrice") or 0,
                    "harvestQuantity": 1,
                    "price": 0,
                    "isVipOnly": bool(i.get("isVipOnly")),
                    "isEnabled": True,
                }
        candidates = list(seed_by_id.values())
        if prefer_seed:
            candidates = [s for s in candidates if str(s.get("id")) == prefer_seed] or candidates
        candidates = [s for s in candidates if s.get("isEnabled") is not False]
        candidates.sort(key=lambda s: self.score_seed(s, inv_qty), reverse=True)

        plan = []
        remain = empty
        bal = balance_raw
        for s in candidates:
            if remain <= 0:
                break
            sid = str(s.get("id"))
            stock = inv_qty.get(sid, 0)
            if stock <= 0:
                continue
            use = min(remain, stock)
            plan.append(
                {
                    "seedId": sid,
                    "name": s.get("name") or sid,
                    "quantity": use,
                    "from_stock": use,
                    "need_buy": 0,
                    "cost": 0,
                    "score": round(self.score_seed(s, inv_qty), 4),
                }
            )
            inv_qty[sid] = stock - use
            remain -= use

        if allow_buy and remain > 0:
            buyable = [s for s in candidates if not s.get("isVipOnly") and s.get("isEnabled") is not False]
            buyable.sort(key=lambda s: self.score_seed(s, inv_qty), reverse=True)
            for s in buyable:
                if remain <= 0:
                    break
                sid = str(s.get("id"))
                price = int(s.get("price") or 0)
                if price <= 0:
                    use = remain
                    plan.append(
                        {
                            "seedId": sid,
                            "name": s.get("name") or sid,
                            "quantity": use,
                            "from_stock": 0,
                            "need_buy": 0,
                            "cost": 0,
                            "score": round(self.score_seed(s, inv_qty), 4),
                        }
                    )
                    remain = 0
                    break
                can_buy = bal // price
                use = min(remain, can_buy)
                if use <= 0:
                    continue
                cost = use * price
                plan.append(
                    {
                        "seedId": sid,
                        "name": s.get("name") or sid,
                        "quantity": use,
                        "from_stock": 0,
                        "need_buy": use,
                        "cost": cost,
                        "score": round(self.score_seed(s, inv_qty), 4),
                    }
                )
                bal -= cost
                remain -= use

        merged: dict[str, dict] = {}
        for p in plan:
            m = merged.setdefault(
                p["seedId"],
                {
                    "seedId": p["seedId"],
                    "name": p["name"],
                    "quantity": 0,
                    "from_stock": 0,
                    "need_buy": 0,
                    "cost": 0,
                    "score": p.get("score"),
                },
            )
            m["quantity"] += p["quantity"]
            m["from_stock"] += p["from_stock"]
            m["need_buy"] += p["need_buy"]
            m["cost"] += p["cost"]
        out = list(merged.values())
        log.info("plant.plan empty=%s remain=%s steps=%s detail=%s", empty, remain, len(out), clip(out, 300))
        return out

    async def care_all(self) -> dict:
        log.info("action.care start")
        click_res = await self.evaluate(
            """(() => {
              const btn = document.querySelector('[aria-label="一键务农"], [data-testid="care-actions-desktop"], [data-testid="care-actions-mobile"]');
              if (btn && !btn.disabled) { btn.click(); return 'clicked_dom'; }
              return 'disabled_or_not_found';
            })()"""
        )
        if click_res == "clicked_dom":
            log.info("action.care via=DOM")
            await asyncio.sleep(1.2)
            self._status_cache = None
            return {"success": True, "via": "DOM"}
        log.info("action.care via=API")
        res = await self.fetch_api("/api/farm/care/all", method="POST", body={})
        ok, payload, err = unwrap_api(res)
        self._status_cache = None
        return {"success": ok, "via": "API", "payload": payload, "error": err}

    async def harvest_all(self, destroy_if_full: bool = False) -> dict:
        log.info("action.harvest start destroyIfFull=%s", destroy_if_full)
        click_res = await self.evaluate(
            r"""(() => {
              const btns=[...document.querySelectorAll('button')];
              const b=btns.find(x=>{
                const t=(x.innerText||'').replace(/\s+/g,'');
                return !x.disabled && (t.includes('一键收获') || t.includes('收获('));
              });
              if(b){b.click();return 'clicked_dom:'+ (b.innerText||'').trim().slice(0,40);}
              return 'no';
            })()"""
        )
        if str(click_res).startswith("clicked_dom"):
            log.info("action.harvest dom=%s", click_res)
            await asyncio.sleep(1.0)

        res = await self.fetch_api(
            "/api/farm/harvest-all",
            method="POST",
            body={"destroyIfFull": bool(destroy_if_full)},
        )
        ok, payload, err = unwrap_api(res)
        data = payload.get("data") if isinstance(payload, dict) else payload
        code = None
        if isinstance(payload, dict):
            code = (payload.get("error") or {}).get("code")
        if code == "WAREHOUSE_FULL":
            log.warning("action.harvest warehouse_full — refuse destroy")
        log.info("action.harvest done ok=%s err=%s data=%s", ok, err, clip(data, 300))
        self._status_cache = None
        return {"success": ok, "via": "API", "payload": payload, "error": err, "data": data}

    async def plant_batch(self, seed_id: str, quantity: int) -> dict:
        log.info("action.plant seed=%s qty=%s", seed_id, quantity)
        res = await self.fetch_api(
            "/api/farm/plant-batch",
            method="POST",
            body={"seedId": seed_id, "quantity": int(quantity)},
        )
        ok, payload, err = unwrap_api(res)
        data = payload.get("data") if isinstance(payload, dict) else payload
        self._status_cache = None
        if ok:
            n = int((data or {}).get("plantedCount") or 0) if isinstance(data, dict) else 0
            log.info("action.plant ok plantedCount=%s purchased=%s", n, (data or {}).get("purchasedCount") if isinstance(data, dict) else None)
        else:
            log.error("action.plant FAIL seed=%s qty=%s err=%s payload=%s", seed_id, quantity, err, clip(payload, 200))
        return {"success": ok, "payload": payload, "error": err, "data": data}

    async def plant_smart(
        self,
        allow_buy: bool = False,
        prefer_seed: Optional[str] = None,
        dry_run: bool = False,
        status: Optional[dict] = None,
        empty_override: Optional[int] = None,
    ) -> dict:
        st = status or await self.get_farm_status()
        empty = empty_override if empty_override is not None else st["empty_slots"]
        if empty <= 0:
            log.info("action.plant skip reason=full")
            return {"success": True, "planted": 0, "plan": [], "reason": "full"}

        await self.load_seeds(st)
        plan = self.plan_planting(
            empty, st["inventory"], st["seeds"], st["balance_raw"], allow_buy=allow_buy, prefer_seed=prefer_seed
        )
        if not plan:
            log.warning("action.plant no_plan allow_buy=%s", allow_buy)
            return {"success": False, "planted": 0, "plan": [], "reason": "no_plan"}

        for p in plan:
            log.info(
                "plant.step %s x%s stock=%s buy=%s cost=%s score=%s",
                p["name"],
                p["quantity"],
                p["from_stock"],
                p["need_buy"],
                coin_fmt(p["cost"]),
                p.get("score"),
            )
        if dry_run:
            return {"success": True, "planted": 0, "plan": plan, "dry_run": True}

        total = 0
        results = []
        for p in plan:
            r = await self.plant_batch(p["seedId"], p["quantity"])
            results.append({"seedId": p["seedId"], **{k: r.get(k) for k in ("success", "error", "data")}})
            if r.get("success"):
                data = r.get("data") or {}
                total += int(data.get("plantedCount") or p["quantity"] or 0)
            await asyncio.sleep(0.4)
        log.info("action.plant done planted=%s", total)
        return {"success": total > 0, "planted": total, "plan": plan, "results": results}

    async def run_pipeline(
        self,
        destroy_if_full: bool = False,
        allow_buy: bool = False,
        prefer_seed: Optional[str] = None,
        dry_run: bool = False,
    ) -> dict:
        log.info("pipeline.start dry_run=%s allow_buy=%s destroy=%s", dry_run, allow_buy, destroy_if_full)
        before = await self.get_farm_status()
        self.print_status(before)
        report: dict[str, Any] = {
            "before": {
                "mature": before["mature_count"],
                "debuff": before["debuff_count"],
                "empty": before["empty_slots"],
                "next_mature_sec": before["next_mature_sec"],
            },
            "care": None,
            "harvest": None,
            "plant": None,
            "after": None,
        }

        did_write = False
        if before["debuff_count"] > 0:
            if dry_run:
                log.info("pipeline.care dry-run debuff=%s", before["debuff_count"])
                report["care"] = {"dry_run": True, "debuff": before["debuff_count"]}
            else:
                report["care"] = await self.care_all()
                did_write = True
                await asyncio.sleep(0.5)
        else:
            log.info("pipeline.care skip")

        # 无写操作则复用 before，少打 1 轮 API
        mid = await self.get_farm_status() if did_write else before
        harvested_n = 0
        if mid["mature_count"] > 0:
            if dry_run:
                log.info("pipeline.harvest dry-run mature=%s", mid["mature_count"])
                report["harvest"] = {"dry_run": True, "mature": mid["mature_count"]}
                harvested_n = mid["mature_count"]
            else:
                report["harvest"] = await self.harvest_all(destroy_if_full=destroy_if_full)
                did_write = True
                hdata = (report["harvest"] or {}).get("data") or {}
                if isinstance(hdata, dict):
                    harvested_n = int(
                        hdata.get("harvestedCount")
                        or len(hdata.get("harvestedCropIds") or [])
                        or 0
                    )
                # 收菜后服务端状态可能滞后：轮询直到出现空位或超时
                await asyncio.sleep(0.8)
        else:
            log.info("pipeline.harvest skip")

        after_h = await self.get_farm_status() if did_write else mid
        empty_for_plant = after_h["empty_slots"]

        # 关键：收成功但 empty 仍为 0 → 重拉最多 5 次；仍 0 则用 harvested_n 估算空位强种
        if not dry_run and harvested_n > 0 and empty_for_plant <= 0:
            log.warning(
                "pipeline.plant empty_lag harvested=%s empty=%s — retry status",
                harvested_n,
                empty_for_plant,
            )
            for i in range(5):
                await asyncio.sleep(0.7)
                after_h = await self.get_farm_status()
                empty_for_plant = after_h["empty_slots"]
                log.info("pipeline.plant lag_poll=%s empty=%s planted=%s", i, empty_for_plant, after_h["planted_count"])
                if empty_for_plant > 0:
                    break
            if empty_for_plant <= 0:
                # API 已收但 crops 仍满：按收获数强制补种名额（plant-batch 只填空地）
                guess = min(harvested_n, after_h.get("max_slots") or harvested_n)
                log.warning("pipeline.plant force_empty_override=%s (status lag)", guess)
                empty_for_plant = guess

        if dry_run and (mid["mature_count"] > 0 or harvested_n > 0):
            empty_for_plant = after_h["empty_slots"] + (harvested_n or mid["mature_count"])
            log.info("pipeline.plant dry-run assume_empty=%s", empty_for_plant)

        if empty_for_plant > 0:
            report["plant"] = await self.plant_smart(
                allow_buy=allow_buy,
                prefer_seed=prefer_seed,
                dry_run=dry_run,
                status=after_h,
                empty_override=empty_for_plant,
            )
            if not dry_run and report["plant"].get("planted"):
                did_write = True
            # 种完再确认：若仍有空位且有库存，再补一轮
            if not dry_run:
                after_p = await self.get_farm_status()
                if after_p["empty_slots"] > 0:
                    stock = sum((after_p.get("inventory_qty") or {}).values())
                    if stock > 0 or allow_buy:
                        log.warning(
                            "pipeline.plant second_pass empty=%s stock=%s",
                            after_p["empty_slots"],
                            stock,
                        )
                        r2 = await self.plant_smart(
                            allow_buy=allow_buy,
                            prefer_seed=prefer_seed,
                            dry_run=False,
                            status=after_p,
                        )
                        report["plant_second"] = r2
                        if r2.get("planted"):
                            did_write = True
                            prev = int((report.get("plant") or {}).get("planted") or 0)
                            if isinstance(report.get("plant"), dict):
                                report["plant"]["planted"] = prev + int(r2.get("planted") or 0)
                    else:
                        log.warning("pipeline.plant still_empty no_stock — need --allow-buy or buy seeds")
                after_h = after_p
        else:
            log.info("pipeline.plant skip empty=0 harvested_n=%s", harvested_n)
            report["plant"] = {"success": True, "planted": 0, "reason": "no_empty"}

        after = await self.get_farm_status() if did_write else after_h
        report["after"] = {
            "mature": after["mature_count"],
            "debuff": after["debuff_count"],
            "empty": after["empty_slots"],
            "planted": after["planted_count"],
            "growing": after["growing_count"],
            "balance": after["balance"],
            "next_mature_sec": after["next_mature_sec"],
            "suggestions": after["suggestions"],
        }
        report["resource"] = self.resource_snapshot()
        log.info(
            "pipeline.done resource=%s after=%s",
            report["resource"],
            clip(report["after"], 200),
        )
        self.print_status(after)
        return report

    def resource_snapshot(self) -> dict:
        return {
            "elapsed_s": round(time.monotonic() - self.stats["t0"], 2),
            "cdp_calls": self.stats["cdp_calls"],
            "api_calls": self.stats["api_calls"],
            "rss_mb": self._rss_mb(),
        }

    def _rss_mb(self) -> Optional[float]:
        try:
            import resource

            # macOS ru_maxrss is bytes
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            if sys.platform == "darwin":
                return round(rss / (1024 * 1024), 2)
            return round(rss / 1024, 2)
        except Exception:
            return None

    def print_status(self, st: dict):
        nm = st.get("next_mature_sec")
        if nm is None:
            eta = "-"
        elif nm <= 0:
            eta = "已可收"
        else:
            eta = f"{nm // 60}分{nm % 60}秒"
        wh = st.get("warehouse") or {}
        used, cap = wh.get("usedCapacity"), wh.get("capacity")
        wh_s = f"{used}/{cap}" if used is not None and cap is not None else "-"
        print("\n========== 轻松农场状态 ==========")
        print(f"余额       : {st['balance']}")
        print(f"体力       : {st['energy']}")
        print(f"地块       : {st['planted_count']}/{st['max_slots']} (空 {st['empty_slots']})")
        print(f"作物       : 待收 {st['mature_count']} | 生长 {st['growing_count']} | 灾害 {st['debuff_count']}")
        print(f"下次成熟   : {eta}")
        print(f"仓库       : {wh_s}")
        inv = st.get("inventory") or []
        if inv:
            inv_s = ", ".join(f"{i.get('seedName') or i.get('seedId')}×{i.get('quantity')}" for i in inv[:8])
            print(f"种子库存   : {inv_s}")
        else:
            print("种子库存   : (空)")
        print("建议       :")
        for s in st.get("suggestions") or []:
            print(f"  - {s}")
        rs = self.resource_snapshot()
        print(f"资源       : {rs['elapsed_s']}s cdp={rs['cdp_calls']} api={rs['api_calls']} rss={rs['rss_mb']}MB")
        print("=================================\n")

    async def close(self):
        # 若我们创建了临时 tab，关掉以省 Chrome 渲染资源；用户原有 farm 标签保留
        if self.created_tab and self.target_id:
            try:
                await self.call_browser("Target.closeTarget", {"targetId": self.target_id})
                log.info("cdp.closed_temp_tab id=%s", self.target_id[:8])
            except Exception as e:
                log.warning("cdp.close_tab_fail %s", e)
        if self.ws:
            try:
                await asyncio.wait_for(self.ws.close(), timeout=3)
            except Exception:
                pass
            self.ws = None
        log.info("cdp.disconnected stats=%s", self.resource_snapshot())



def compute_sleep_sec(
    st: dict,
    *,
    min_s: float,
    max_s: float,
    lead_s: float,
    care_every_s: float,
    force_s: Optional[float] = None,
) -> float:
    """挂机休眠：成熟前提前 lead 醒来；生长中不超过 care_every 以便扫灾害。"""
    if force_s is not None:
        return max(min_s, min(max_s, float(force_s)))
    # 有活立刻短歇再扫（防 API 刚写完状态滞后）
    if st.get("mature_count", 0) > 0 or st.get("debuff_count", 0) > 0 or st.get("empty_slots", 0) > 0:
        return max(min_s, min(30.0, max_s))
    nm = st.get("next_mature_sec")
    if nm is None:
        # 全空且无 ETA：可能未种上或异常，别睡太久
        return max(min_s, min(care_every_s, max_s))
    # 距成熟 lead 秒前醒来
    until_harvest = max(0.0, float(nm) - float(lead_s))
    # 生长期间定期 care 巡检
    wait = min(until_harvest, float(care_every_s))
    return max(min_s, min(max_s, wait))


def append_journal(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    row = dict(row)
    row.setdefault("ts", datetime.now(timezone.utc).isoformat())
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


async def run_daemon(args) -> int:
    """
    囤囤鼠式挂机：
    循环 connect → status/pipeline → disconnect → sleep(ETA)
    不常驻占着 CDP 事件流；每轮短连接，VPS 友好。
    """
    min_s = float(getattr(args, "min_sleep", 60))
    max_s = float(getattr(args, "max_sleep", 1800))
    lead_s = float(getattr(args, "lead", 45))
    care_every_s = float(getattr(args, "care_every", 600))
    max_cycles = int(getattr(args, "max_cycles", 0)) or 0
    journal = Path(getattr(args, "journal", "") or (DEFAULT_LOG_DIR / "daemon-journal.jsonl"))
    stop = asyncio.Event()

    def _stop(signum, frame):
        log.warning("daemon.signal %s — graceful stop", signum)
        stop.set()

    import signal as signal_mod
    for sig in (signal_mod.SIGINT, signal_mod.SIGTERM):
        try:
            signal_mod.signal(sig, _stop)
        except Exception:
            pass

    cycle = 0
    consecutive_fail = 0
    log.info(
        "daemon.start min_sleep=%s max_sleep=%s lead=%s care_every=%s journal=%s",
        min_s, max_s, lead_s, care_every_s, journal,
    )
    append_journal(journal, {"event": "daemon_start", "pid": os.getpid(), "min_s": min_s, "max_s": max_s})

    while not stop.is_set():
        cycle += 1
        if max_cycles and cycle > max_cycles:
            log.info("daemon.max_cycles reached %s", max_cycles)
            break
        cycle_t0 = time.monotonic()
        http, ws, port = discover_cdp()
        if not ws:
            consecutive_fail += 1
            backoff = min(max_s, min_s * min(consecutive_fail, 8))
            log.error("daemon.cdp_missing fail=%s sleep=%.0fs", consecutive_fail, backoff)
            append_journal(journal, {"event": "cdp_missing", "cycle": cycle, "sleep": backoff})
            try:
                await asyncio.wait_for(stop.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass
            continue

        client = FarmClient(ws, http or f"http://127.0.0.1:{port}")
        sleep_s = care_every_s
        report = None
        try:
            await client.connect()
            # 有活干或巡检：跑流水线（内部会 status）
            report = await client.run_pipeline(
                destroy_if_full=args.destroy_if_full,
                allow_buy=args.allow_buy,
                prefer_seed=args.seed,
                dry_run=args.dry_run,
            )
            after = report.get("after") or {}
            # 构造最小 st 给 sleep
            st_sleep = {
                "mature_count": after.get("mature", 0),
                "debuff_count": after.get("debuff", 0),
                "empty_slots": after.get("empty", 0),
                "next_mature_sec": after.get("next_mature_sec"),
            }
            sleep_s = compute_sleep_sec(
                st_sleep, min_s=min_s, max_s=max_s, lead_s=lead_s, care_every_s=care_every_s
            )
            consecutive_fail = 0
            append_journal(
                journal,
                {
                    "event": "cycle_ok",
                    "cycle": cycle,
                    "port": port,
                    "before": report.get("before"),
                    "after": after,
                    "care": bool(report.get("care")),
                    "harvest": bool(report.get("harvest")),
                    "plant": (report.get("plant") or {}).get("planted"),
                    "sleep_s": sleep_s,
                    "resource": report.get("resource"),
                    "elapsed_s": round(time.monotonic() - cycle_t0, 2),
                },
            )
            log.info(
                "daemon.cycle=%s ok sleep=%.0fs next_mature=%s mature=%s debuff=%s empty=%s",
                cycle,
                sleep_s,
                after.get("next_mature_sec"),
                after.get("mature"),
                after.get("debuff"),
                after.get("empty"),
            )
        except Exception as e:
            consecutive_fail += 1
            sleep_s = min(max_s, min_s * min(consecutive_fail, 6))
            log.exception("daemon.cycle_fail cycle=%s err=%s sleep=%.0fs", cycle, e, sleep_s)
            append_journal(
                journal,
                {
                    "event": "cycle_fail",
                    "cycle": cycle,
                    "error": str(e),
                    "sleep_s": sleep_s,
                    "fail": consecutive_fail,
                },
            )
        finally:
            try:
                await client.close()
            except Exception:
                pass

        if stop.is_set():
            break
        # 可中断睡眠
        log.info("daemon.sleep %.0fs (ctrl+c 结束)", sleep_s)
        try:
            await asyncio.wait_for(stop.wait(), timeout=sleep_s)
        except asyncio.TimeoutError:
            pass

    append_journal(journal, {"event": "daemon_stop", "pid": os.getpid(), "cycles": cycle})
    log.info("daemon.exit cycles=%s", cycle)
    return 0


async def main_async(args) -> int:
    if args.mode == "daemon":
        return await run_daemon(args)

    http, ws, port = discover_cdp()
    if not ws:
        log.error("cdp.not_found — 启动带 --remote-debugging-port 的 headed Chrome")
        return 2
    log.info("cdp.found port=%s", port)

    client = FarmClient(ws, http or f"http://127.0.0.1:{port}")
    code = 0
    try:
        await client.connect()
        if args.mode == "status":
            st = await client.get_farm_status()
            if args.json:
                out = {k: v for k, v in st.items() if k != "crops"}
                out["crops_n"] = len(st.get("crops") or [])
                out["resource"] = client.resource_snapshot()
                print(json.dumps(out, ensure_ascii=False, default=str))
            else:
                client.print_status(st)
        elif args.mode == "care":
            r = await client.care_all()
            st = await client.get_farm_status()
            if args.json:
                print(json.dumps({"care": r, "status_summary": {
                    "mature": st["mature_count"], "debuff": st["debuff_count"], "empty": st["empty_slots"]
                }, "resource": client.resource_snapshot()}, ensure_ascii=False, default=str))
            else:
                client.print_status(st)
            if not r.get("success"):
                code = 1
        elif args.mode == "harvest":
            r = await client.harvest_all(destroy_if_full=args.destroy_if_full)
            st = await client.get_farm_status()
            if args.json:
                print(json.dumps({"harvest": r, "resource": client.resource_snapshot()}, ensure_ascii=False, default=str))
            else:
                client.print_status(st)
            if not r.get("success"):
                code = 1
        elif args.mode == "plant":
            r = await client.plant_smart(
                allow_buy=args.allow_buy, prefer_seed=args.seed, dry_run=args.dry_run
            )
            st = await client.get_farm_status()
            if args.json:
                print(json.dumps({"plant": r, "resource": client.resource_snapshot()}, ensure_ascii=False, default=str))
            else:
                client.print_status(st)
            if not r.get("success") and r.get("reason") not in ("full",):
                code = 1
        elif args.mode == "run":
            r = await client.run_pipeline(
                destroy_if_full=args.destroy_if_full,
                allow_buy=args.allow_buy,
                prefer_seed=args.seed,
                dry_run=args.dry_run,
            )
            if args.json:
                print(json.dumps(r, ensure_ascii=False, default=str))
        else:
            log.error("unknown mode")
            code = 2
    except Exception as e:
        log.exception("fatal %s", e)
        if args.json:
            print(json.dumps({"error": str(e)}, ensure_ascii=False))
        code = 1
    finally:
        await client.close()
    return code


def main():
    p = argparse.ArgumentParser(description="hybgzs 轻松农场智能自动化")
    p.add_argument("mode", nargs="?", default="status", choices=["status", "run", "care", "harvest", "plant", "daemon"])
    p.add_argument("--destroy-if-full", action="store_true")
    p.add_argument("--allow-buy", action="store_true")
    p.add_argument("--seed", default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--json", action="store_true")
    p.add_argument("--log-level", default=os.environ.get("FARM_LOG_LEVEL", "INFO"))
    p.add_argument("--log-file", default=os.environ.get("FARM_LOG_FILE"))
    p.add_argument("--no-lock", action="store_true", help="允许并发（不推荐）")
    # daemon 挂机
    p.add_argument("--min-sleep", type=float, default=float(os.environ.get("FARM_MIN_SLEEP", "60")),
                   help="挂机最短休眠秒（默认60）")
    p.add_argument("--max-sleep", type=float, default=float(os.environ.get("FARM_MAX_SLEEP", "1800")),
                   help="挂机最长休眠秒（默认1800=30m）")
    p.add_argument("--lead", type=float, default=float(os.environ.get("FARM_LEAD", "45")),
                   help="成熟前提前醒来秒（默认45）")
    p.add_argument("--care-every", type=float, default=float(os.environ.get("FARM_CARE_EVERY", "600")),
                   help="无成熟时灾害巡检间隔秒（默认600）")
    p.add_argument("--max-cycles", type=int, default=int(os.environ.get("FARM_MAX_CYCLES", "0")),
                   help="挂机最多轮数，0=无限")
    p.add_argument("--journal", default=os.environ.get("FARM_JOURNAL", ""),
                   help="daemon JSONL 日志路径")
    args = p.parse_args()

    log_path = setup_logging(args.log_level, args.log_file, json_mode=args.json)
    log.info("boot mode=%s pid=%s log=%s", args.mode, os.getpid(), log_path)
    if args.mode == "daemon":
        log.info("daemon.config min=%s max=%s lead=%s care_every=%s",
                 args.min_sleep, args.max_sleep, args.lead, args.care_every)

    lock = SingleInstance(DEFAULT_LOCK)
    if not args.no_lock:
        if not lock.acquire():
            log.error("another farm_runner is running (lock %s)", DEFAULT_LOCK)
            sys.exit(3)
        atexit.register(lock.release)

    try:
        code = asyncio.run(main_async(args))
    finally:
        lock.release()
    log.info("exit code=%s", code)
    sys.exit(code)


if __name__ == "__main__":
    main()
