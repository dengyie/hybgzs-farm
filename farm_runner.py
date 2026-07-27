#!/usr/bin/env python3
"""
hybgzs 轻松农场自动化 (cdk.hybgzs.com/entertainment/farm)

红线:
1. 复用 headed CDP（自动发现端口），不 launch / 不杀 Chrome
2. 默认 destroyIfFull=false（不毁菜）
3. 默认只种背包库存，不自动扣币买种（--allow-buy 才可）
4. CF/人机：页面等待，不秒退

智能点:
- 种子按 单位时间价值 harvestValue/growthTime 排序，库存优先
- 可多种子填空：先耗库存高价值，再可选购种
- run: care → harvest → recheck → plant
- status: 下次成熟 ETA / 仓库占用 / 动作建议
- 写操作后复检状态验收
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any, Optional

import websockets

FARM_URL = "https://cdk.hybgzs.com/entertainment/farm"
COIN_DIV = 500_000


def discover_cdp(ports: Optional[list[int]] = None) -> tuple[Optional[str], Optional[str], Optional[int]]:
    ports = ports or [9222, 9223, 9226, 9333, 9229]
    for p in ports:
        try:
            op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with op.open(f"http://127.0.0.1:{p}/json/version", timeout=1.5) as resp:
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


def unwrap_api(resp: Optional[dict]) -> tuple[bool, Any, Optional[str]]:
    """Return (ok, payload, err_msg). payload prefers nested data."""
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
    # nested data
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
    def __init__(self, browser_ws: str, verbose: bool = True):
        self.browser_ws = browser_ws
        self.verbose = verbose
        self.ws = None
        self.sid = None
        self.target_id = None
        self.req_id = 0
        self.created_tab = False
        self._lock = asyncio.Lock()

    def log(self, *a):
        if self.verbose:
            print(*a)

    async def connect(self):
        self.ws = await websockets.connect(
            self.browser_ws, max_size=32 * 1024 * 1024, open_timeout=12
        )
        tabs = (await self.call_browser("Target.getTargets")).get("targetInfos", [])
        pages = [t for t in tabs if t.get("type") == "page"]
        # prefer exact farm page
        target = next(
            (t for t in pages if "entertainment/farm" in (t.get("url") or "")),
            None,
        )
        if not target:
            target = next((t for t in pages if "hybgzs.com" in (t.get("url") or "")), None)
        if target:
            self.target_id = target["targetId"]
            self.log(f"[CDP] 复用标签: {self.target_id[:8]}… {(target.get('url') or '')[:60]}")
        else:
            create_res = await self.call_browser("Target.createTarget", {"url": "about:blank"})
            self.target_id = create_res["targetId"]
            self.created_tab = True
            self.log(f"[CDP] 创建临时标签: {self.target_id[:8]}…")

        attach = await self.call_browser(
            "Target.attachToTarget", {"targetId": self.target_id, "flatten": True}
        )
        self.sid = attach["sessionId"]
        await self.call_tab("Page.enable")
        await self.call_tab("Runtime.enable")
        await self.call_tab("Network.enable")

    async def call_browser(self, method: str, params: Optional[dict] = None) -> dict:
        async with self._lock:
            self.req_id += 1
            rid = self.req_id
            msg: dict[str, Any] = {"id": rid, "method": method}
            if params is not None:
                msg["params"] = params
            await self.ws.send(json.dumps(msg))
            while True:
                data = json.loads(await asyncio.wait_for(self.ws.recv(), timeout=25))
                if data.get("id") == rid:
                    if "error" in data:
                        raise RuntimeError(f"CDP browser {method}: {data['error']}")
                    return data.get("result") or {}

    async def call_tab(self, method: str, params: Optional[dict] = None) -> dict:
        async with self._lock:
            self.req_id += 1
            rid = self.req_id
            msg: dict[str, Any] = {
                "id": rid,
                "method": method,
                "sessionId": self.sid,
            }
            if params is not None:
                msg["params"] = params
            await self.ws.send(json.dumps(msg))
            while True:
                data = json.loads(await asyncio.wait_for(self.ws.recv(), timeout=25))
                if data.get("id") == rid:
                    if "error" in data:
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
            self.log(f"[CDP] 导航 {FARM_URL}")
            await self.call_tab("Page.navigate", {"url": FARM_URL})
            await asyncio.sleep(2.5)

        await self.evaluate(
            """(() => {
              const btns=[...document.querySelectorAll('button')];
              const known=btns.find(x=>(x.innerText||'').includes('我知道了'));
              if(known){known.click();return 'closed';}
              return 'none';
            })()"""
        )

        for _ in range(15):
            text = str(await self.evaluate("document.body?document.body.innerText:''") or "")
            if "获取农场数据失败" in text or "重新登录" in text:
                # one retry click
                await self.evaluate(
                    """(() => {
                      const b=[...document.querySelectorAll('button')].find(x=>(x.innerText||'').includes('重试'));
                      if(b){b.click();return 'retry';} return 'no';
                    })()"""
                )
                await asyncio.sleep(2)
                text = str(await self.evaluate("document.body?document.body.innerText:''") or "")
                if "获取农场数据失败" in text or "重新登录" in text:
                    raise RuntimeError("农场数据加载失败 / 未登录。请在 headed Chrome 登录 cdk.hybgzs.com")
            if "轻松农场" in text or "我的农田" in text or "一键务农" in text:
                return True
            await asyncio.sleep(0.8)
        return True

    async def fetch_api(self, path: str, method: str = "GET", body: Any = None) -> dict:
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
        return await self.evaluate(js, await_promise=True) or {"ok": False, "status": 0}

    async def get_farm_status(self) -> dict:
        await self.ensure_page_loaded()
        # websockets 单连接不可并发 recv，顺序拉取
        res_crops = await self.fetch_api("/api/farm/crops")
        res_energy = await self.fetch_api("/api/farm/energy/status")
        res_wallet = await self.fetch_api("/api/wallet/balance")
        res_inv = await self.fetch_api("/api/farm/inventory")
        res_seeds = await self.fetch_api("/api/farm/seeds")
        res_plots = {"ok": True, "status": 200, "data": {"success": True, "data": {}}}

        ok_c, crops_payload, err_c = unwrap_api(res_crops)
        if not ok_c:
            raise RuntimeError(f"crops API 失败: {err_c}")

        # crops payload: {success, data:[], crops:[], maxSlots,...} OR list in data
        crops_root = crops_payload if isinstance(crops_payload, dict) else {}
        crops = extract_list(crops_payload, "crops", "data")
        if not crops and isinstance(crops_payload, list):
            crops = crops_payload
        max_slots = int(crops_root.get("maxSlots") or crops_root.get("baseSlots") or 10)
        if isinstance(crops_payload, dict) and isinstance(crops_payload.get("data"), dict):
            max_slots = int(crops_payload["data"].get("maxSlots") or max_slots)

        # if top-level maxSlots on HTTP wrapper path
        if res_crops and isinstance(res_crops.get("data"), dict):
            max_slots = int(res_crops["data"].get("maxSlots") or max_slots)

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

        ok_s, seeds_payload, _ = unwrap_api(res_seeds)
        seeds = []
        if ok_s:
            if isinstance(seeds_payload, dict):
                seeds = seeds_payload.get("seeds") or extract_list(seeds_payload, "seeds", "data")
            elif isinstance(seeds_payload, list):
                seeds = seeds_payload

        ok_p, plots_payload, _ = unwrap_api(res_plots)
        plots = plots_payload.get("data") if isinstance(plots_payload, dict) and isinstance(plots_payload.get("data"), dict) else (plots_payload if isinstance(plots_payload, dict) else {})

        mature = [c for c in crops if c.get("isMature") and not c.get("isHarvested")]
        growing = [c for c in crops if not c.get("isMature") and not c.get("isHarvested")]
        debuff_crops = []
        for c in growing:
            conds = c.get("conditions") or []
            has = bool(conds) or any(
                c.get(k) for k in ("thirstyStartedAt", "weedStartedAt", "pestStartedAt")
            )
            if has:
                debuff_crops.append(c)

        empty_slots = max(0, max_slots - len([c for c in crops if not c.get("isHarvested")]))
        # if all mature about to harvest, empty after harvest would be max_slots
        next_mature_sec = None
        if growing:
            rems = [int(c.get("remainingTime") or 0) for c in growing]
            next_mature_sec = min(rems) if rems else None

        inv_qty = {str(i.get("seedId")): int(i.get("quantity") or 0) for i in inventory if i.get("seedId")}

        return {
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
            "mature": mature,
            "growing": growing,
            "inventory": inventory,
            "inventory_qty": inv_qty,
            "seeds": seeds,
            "warehouse": warehouse,
            "plots": plots,
            "next_mature_sec": next_mature_sec,
            "suggestions": self._suggest(
                len(mature), len(debuff_crops), empty_slots, inv_qty, warehouse
            ),
        }

    def _suggest(self, mature, debuff, empty, inv_qty, warehouse) -> list[str]:
        s = []
        if debuff:
            s.append(f"建议务农：{debuff} 株有灾害")
        if mature:
            s.append(f"建议收菜：{mature} 株成熟")
            used = int((warehouse or {}).get("usedCapacity") or 0)
            cap = int((warehouse or {}).get("capacity") or 0)
            if cap and used >= cap * 0.9:
                s.append(f"仓库将满 {used}/{cap}，收菜可能 WAREHOUSE_FULL")
        if empty:
            stock = sum(inv_qty.values())
            if stock >= empty:
                s.append(f"建议补种：{empty} 空位，库存种子 {stock}")
            elif stock > 0:
                s.append(f"建议部分补种：空位 {empty}，库存仅 {stock}（默认不买种）")
            else:
                s.append(f"空位 {empty} 但无库存种子（需 --allow-buy 才购种）")
        if not s:
            s.append("无事可做（生长中）")
        return s

    def score_seed(self, seed: dict, inv_qty: dict[str, int]) -> float:
        """Higher is better. Inventory bonus; VIP without stock penalized unless allowed later."""
        sid = str(seed.get("id") or seed.get("seedId") or "")
        try:
            gt = max(1, int(seed.get("growthTime") or 1))
            hv = int(seed.get("harvestValue") or 0)
            hq = int(seed.get("harvestQuantity") or 1)
            # value per second * harvest qty weight
            base = (hv * max(hq, 1)) / gt
        except Exception:
            base = 0.0
        stock = inv_qty.get(sid, 0)
        if stock > 0:
            base *= 1.25  # prefer inventory
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
        """Return list of {seedId, name, quantity, from_stock, need_buy, cost}."""
        if empty <= 0:
            return []
        inv_qty = {
            str(i.get("seedId")): int(i.get("quantity") or 0)
            for i in inventory
            if i.get("seedId")
        }
        seed_by_id = {}
        for s in seeds:
            sid = str(s.get("id") or "")
            if sid:
                seed_by_id[sid] = s
        # synthesize seed meta from inventory if missing in /seeds
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

        # only enabled
        candidates = [s for s in candidates if s.get("isEnabled") is not False]
        candidates.sort(key=lambda s: self.score_seed(s, inv_qty), reverse=True)

        plan = []
        remain = empty
        bal = balance_raw

        # Phase 1: inventory only
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
                }
            )
            inv_qty[sid] = stock - use
            remain -= use

        # Phase 2: optional buy cheapest/high score non-vip
        if allow_buy and remain > 0:
            buyable = [
                s
                for s in candidates
                if not s.get("isVipOnly") and int(s.get("price") or 0) >= 0 and s.get("isEnabled") is not False
            ]
            buyable.sort(key=lambda s: self.score_seed(s, inv_qty), reverse=True)
            for s in buyable:
                if remain <= 0:
                    break
                sid = str(s.get("id"))
                price = int(s.get("price") or 0)
                if price <= 0:
                    # free? treat as plant without buy
                    use = remain
                    plan.append(
                        {
                            "seedId": sid,
                            "name": s.get("name") or sid,
                            "quantity": use,
                            "from_stock": 0,
                            "need_buy": 0,
                            "cost": 0,
                        }
                    )
                    remain = 0
                    break
                can_buy = bal // price if price else 0
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
                    }
                )
                bal -= cost
                remain -= use

        # merge same seedId
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
                },
            )
            m["quantity"] += p["quantity"]
            m["from_stock"] += p["from_stock"]
            m["need_buy"] += p["need_buy"]
            m["cost"] += p["cost"]
        return list(merged.values())

    async def care_all(self) -> dict:
        self.log("[操作] 一键务农…")
        click_res = await self.evaluate(
            """(() => {
              const btn = document.querySelector('[aria-label="一键务农"], [data-testid="care-actions-desktop"], [data-testid="care-actions-mobile"]');
              if (btn && !btn.disabled) { btn.click(); return 'clicked_dom'; }
              return 'disabled_or_not_found';
            })()"""
        )
        if click_res == "clicked_dom":
            self.log("[操作] DOM 点击「一键务农」")
            await asyncio.sleep(2.5)
            return {"success": True, "via": "DOM"}
        self.log("[操作] DOM 不可用 → API /api/farm/care/all")
        res = await self.fetch_api("/api/farm/care/all", method="POST", body={})
        ok, payload, err = unwrap_api(res)
        self.log(f"[API] care/all ok={ok} err={err} body={json.dumps(payload, ensure_ascii=False)[:300]}")
        return {"success": ok, "via": "API", "payload": payload, "error": err}

    async def harvest_all(self, destroy_if_full: bool = False) -> dict:
        self.log(f"[操作] 一键收菜 destroyIfFull={destroy_if_full}…")
        # try DOM first
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
            self.log(f"[操作] DOM {click_res}")
            await asyncio.sleep(2.0)
            # 不信任 DOM 单独成功，继续 API 或状态验收

        res = await self.fetch_api(
            "/api/farm/harvest-all",
            method="POST",
            body={"destroyIfFull": bool(destroy_if_full)},
        )
        ok, payload, err = unwrap_api(res)
        code = None
        if isinstance(payload, dict):
            code = (payload.get("error") or {}).get("code")
            data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        else:
            data = payload
        if code == "WAREHOUSE_FULL" or (isinstance(payload, dict) and (payload.get("error") or {}).get("code") == "WAREHOUSE_FULL"):
            self.log("⚠️ 仓库已满，拒绝毁菜（未开 --destroy-if-full）")
        self.log(
            f"[API] harvest-all ok={ok} err={err} data={json.dumps(data, ensure_ascii=False)[:400]}"
        )
        return {"success": ok, "via": "API", "payload": payload, "error": err, "data": data}

    async def plant_batch(self, seed_id: str, quantity: int) -> dict:
        res = await self.fetch_api(
            "/api/farm/plant-batch",
            method="POST",
            body={"seedId": seed_id, "quantity": int(quantity)},
        )
        ok, payload, err = unwrap_api(res)
        data = payload.get("data") if isinstance(payload, dict) else payload
        self.log(
            f"[API] plant-batch {seed_id} x{quantity} ok={ok} err={err} data={json.dumps(data, ensure_ascii=False)[:300]}"
        )
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
            self.log("[提示] 无空位")
            return {"success": True, "planted": 0, "plan": [], "reason": "full"}

        plan = self.plan_planting(
            empty,
            st["inventory"],
            st["seeds"],
            st["balance_raw"],
            allow_buy=allow_buy,
            prefer_seed=prefer_seed,
        )
        if not plan:
            self.log("[提示] 无可用种植方案（无库存且未 --allow-buy / 余额不足）")
            return {"success": False, "planted": 0, "plan": [], "reason": "no_plan"}

        self.log("[计划] 种植方案:")
        for p in plan:
            self.log(
                f"  - {p['name']}({p['seedId']}) x{p['quantity']} "
                f"库存{p['from_stock']} 购{p['need_buy']} 成本{coin_fmt(p['cost'])}"
            )
        if dry_run:
            return {"success": True, "planted": 0, "plan": plan, "dry_run": True}

        total = 0
        results = []
        for p in plan:
            r = await self.plant_batch(p["seedId"], p["quantity"])
            results.append(r)
            if r.get("success"):
                data = r.get("data") or {}
                total += int(data.get("plantedCount") or p["quantity"] or 0)
            await asyncio.sleep(0.8)
        return {"success": total > 0, "planted": total, "plan": plan, "results": results}

    async def run_pipeline(
        self,
        destroy_if_full: bool = False,
        allow_buy: bool = False,
        prefer_seed: Optional[str] = None,
        dry_run: bool = False,
    ) -> dict:
        self.log(">>> 智能流水线 Care → Harvest → Recheck → Plant <<<")
        before = await self.get_farm_status()
        self.print_status(before)
        report = {
            "before": {
                "mature": before["mature_count"],
                "debuff": before["debuff_count"],
                "empty": before["empty_slots"],
            },
            "care": None,
            "harvest": None,
            "plant": None,
            "after": None,
        }

        if before["debuff_count"] > 0:
            if dry_run:
                self.log(f"[dry-run] 将务农 debuff={before['debuff_count']}")
                report["care"] = {"dry_run": True}
            else:
                report["care"] = await self.care_all()
                await asyncio.sleep(1.5)
        else:
            self.log("[跳过] 无需务农")

        # re-read mature after care
        mid = await self.get_farm_status()
        if mid["mature_count"] > 0:
            if dry_run:
                self.log(f"[dry-run] 将收菜 mature={mid['mature_count']}")
                report["harvest"] = {"dry_run": True, "mature": mid["mature_count"]}
            else:
                report["harvest"] = await self.harvest_all(destroy_if_full=destroy_if_full)
                await asyncio.sleep(1.5)
        else:
            self.log("[跳过] 无可收作物")

        after_h = await self.get_farm_status()
        # dry-run: 假设已收成熟株 → 空位 = 原空位 + 成熟数
        empty_for_plant = after_h["empty_slots"]
        if dry_run and mid["mature_count"] > 0:
            empty_for_plant = after_h["empty_slots"] + mid["mature_count"]
            self.log(f"[dry-run] 假设收后空位={empty_for_plant}")

        if empty_for_plant > 0:
            if dry_run and empty_for_plant != after_h["empty_slots"]:
                # 临时覆盖 empty 做规划
                fake = dict(after_h)
                fake["empty_slots"] = empty_for_plant
                plan = self.plan_planting(
                    empty_for_plant,
                    after_h["inventory"],
                    after_h["seeds"],
                    after_h["balance_raw"],
                    allow_buy=allow_buy,
                    prefer_seed=prefer_seed,
                )
                self.log("[dry-run] 种植方案:")
                for p in plan:
                    self.log(
                        f"  - {p['name']} x{p['quantity']} 库存{p['from_stock']} 购{p['need_buy']}"
                    )
                report["plant"] = {"success": True, "planted": 0, "plan": plan, "dry_run": True}
            else:
                report["plant"] = await self.plant_smart(
                    allow_buy=allow_buy, prefer_seed=prefer_seed, dry_run=dry_run
                )
        else:
            self.log("[跳过] 无空位可种")
            report["plant"] = {"success": True, "planted": 0, "reason": "no_empty"}

        after = await self.get_farm_status()
        report["after"] = {
            "mature": after["mature_count"],
            "debuff": after["debuff_count"],
            "empty": after["empty_slots"],
            "planted": after["planted_count"],
            "balance": after["balance"],
            "next_mature_sec": after["next_mature_sec"],
            "suggestions": after["suggestions"],
        }
        self.log(">>> 流水线结束 <<<")
        self.print_status(after)
        return report

    def print_status(self, st: dict):
        nm = st.get("next_mature_sec")
        if nm is None:
            eta = "-"
        elif nm <= 0:
            eta = "已可收 / 即将可收"
        else:
            eta = f"{nm // 60}分{nm % 60}秒"
        wh = st.get("warehouse") or {}
        used = wh.get("usedCapacity")
        cap = wh.get("capacity")
        wh_s = f"{used}/{cap}" if used is not None and cap is not None else "-"
        print("\n========== 轻松农场状态 ==========")
        print(f"余额       : {st['balance']}")
        print(f"体力       : {st['energy']}")
        print(f"地块       : {st['planted_count']}/{st['max_slots']} (空 {st['empty_slots']})")
        print(
            f"作物       : 待收 {st['mature_count']} | 生长 {st['growing_count']} | 灾害 {st['debuff_count']}"
        )
        print(f"下次成熟   : {eta}")
        print(f"仓库       : {wh_s}")
        inv = st.get("inventory") or []
        if inv:
            inv_s = ", ".join(
                f"{i.get('seedName') or i.get('seedId')}×{i.get('quantity')}" for i in inv[:8]
            )
            print(f"种子库存   : {inv_s}")
        else:
            print("种子库存   : (空)")
        print("建议       :")
        for s in st.get("suggestions") or []:
            print(f"  - {s}")
        print("=================================\n")

    async def close(self):
        # do not auto-close user tabs; only close if we created blank temp and navigated? keep tab for login continuity
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass


async def main_async(args):
    http, ws, port = discover_cdp()
    if not ws:
        print("[错误] 未发现 CDP。请启动带 --remote-debugging-port 的 headed Chrome 并登录农场。")
        sys.exit(2)
    print(f"[CDP] 端口 {port}")

    client = FarmClient(ws, verbose=not args.json)
    code = 0
    try:
        await client.connect()
        if args.mode == "status":
            st = await client.get_farm_status()
            if args.json:
                print(json.dumps(st, ensure_ascii=False, default=str))
            else:
                client.print_status(st)
        elif args.mode == "care":
            r = await client.care_all()
            st = await client.get_farm_status()
            if args.json:
                print(json.dumps({"care": r, "status": st}, ensure_ascii=False, default=str))
            else:
                client.print_status(st)
            if not r.get("success"):
                code = 1
        elif args.mode == "harvest":
            r = await client.harvest_all(destroy_if_full=args.destroy_if_full)
            st = await client.get_farm_status()
            if args.json:
                print(json.dumps({"harvest": r, "status": st}, ensure_ascii=False, default=str))
            else:
                client.print_status(st)
            if not r.get("success"):
                code = 1
        elif args.mode == "plant":
            r = await client.plant_smart(
                allow_buy=args.allow_buy,
                prefer_seed=args.seed,
                dry_run=args.dry_run,
            )
            st = await client.get_farm_status()
            if args.json:
                print(json.dumps({"plant": r, "status": st}, ensure_ascii=False, default=str))
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
            print("unknown mode")
            code = 2
    except Exception as e:
        print(f"[错误] {e}")
        code = 1 if not args.json else 1
        if args.json:
            print(json.dumps({"error": str(e)}, ensure_ascii=False))
    finally:
        await client.close()
    sys.exit(code)


def main():
    p = argparse.ArgumentParser(description="hybgzs 轻松农场智能自动化")
    p.add_argument(
        "mode",
        nargs="?",
        default="status",
        choices=["status", "run", "care", "harvest", "plant"],
        help="status | run | care | harvest | plant",
    )
    p.add_argument("--destroy-if-full", action="store_true", help="仓库满时允许毁菜（默认关）")
    p.add_argument("--allow-buy", action="store_true", help="库存不足时允许扣币买种（默认关）")
    p.add_argument("--seed", default=None, help="优先种子 id，如 carrot")
    p.add_argument("--dry-run", action="store_true", help="只规划不写操作")
    p.add_argument("--json", action="store_true", help="JSON 输出")
    args = p.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
