#!/usr/bin/env python3
"""
hybgzs 轻松农场自动化脚本 (cdk.hybgzs.com/entertainment/farm)

规范与红线：
1. 连已登录 headed Chrome (自动发现 CDP 端口 9222/9223 等)，禁止杀/重启 Chrome。
2. 绝对不默认 destroyIfFull（保护仓库）。
3. 绝对不默认花黑白币购种（仅种背包库存）。
4. 遇到 Cloudflare 或人机验证，保持页面并等待/提示，不盲目报错。
"""

import sys
import os
import json
import time
import argparse
import asyncio
import urllib.request
import websockets

FARM_URL = "https://cdk.hybgzs.com/entertainment/farm"

def discover_cdp():
    ports = [9222, 9223, 9226, 9333, 9229]
    for p in ports:
        try:
            url = f"http://127.0.0.1:{p}/json/version"
            req = urllib.request.Request(url)
            op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with op.open(req, timeout=1.5) as resp:
                data = json.loads(resp.read().decode('utf-8'))
                if "webSocketDebuggerUrl" in data:
                    print(f"[CDP] 发现可用 Chrome CDP 端口: {p}")
                    return f"http://127.0.0.1:{p}", data["webSocketDebuggerUrl"]
        except Exception:
            pass
    return None, None

class FarmClient:
    def __init__(self, browser_ws):
        self.browser_ws = browser_ws
        self.ws = None
        self.sid = None
        self.target_id = None
        self.req_id = 0

    async def connect(self):
        self.ws = await websockets.connect(self.browser_ws, max_size=32*1024*1024, open_timeout=10)
        
        # 查找已有 farm 标签页
        tabs = await self.call_browser("Target.getTargets")
        target_info = None
        for t in tabs.get("targetInfos", []):
            if t.get("type") == "page" and "hybgzs.com" in t.get("url", ""):
                target_info = t
                break
        
        if target_info:
            self.target_id = target_info["targetId"]
            print(f"[CDP] 复用已有农场标签页: {self.target_id}")
        else:
            create_res = await self.call_browser("Target.createTarget", {"url": "about:blank"})
            self.target_id = create_res["targetId"]
            print(f"[CDP] 创建新标签页: {self.target_id}")

        attach_res = await self.call_browser("Target.attachToTarget", {"targetId": self.target_id, "flatten": True})
        self.sid = attach_res["sessionId"]
        
        await self.call_tab("Page.enable")
        await self.call_tab("Runtime.enable")
        await self.call_tab("Network.enable")

    async def call_browser(self, method, params=None):
        self.req_id += 1
        msg = {"id": self.req_id, "method": method}
        if params: msg["params"] = params
        await self.ws.send(json.dumps(msg))
        while True:
            raw = await self.ws.recv()
            data = json.loads(raw)
            if data.get("id") == self.req_id:
                if "error" in data:
                    raise RuntimeError(f"CDP Browser Error ({method}): {data['error']}")
                return data.get("result", {})

    async def call_tab(self, method, params=None):
        self.req_id += 1
        msg = {"id": self.req_id, "method": method, "sessionId": self.sid}
        if params: msg["params"] = params
        await self.ws.send(json.dumps(msg))
        while True:
            raw = await self.ws.recv()
            data = json.loads(raw)
            if data.get("id") == self.req_id:
                if "error" in data:
                    raise RuntimeError(f"CDP Tab Error ({method}): {data['error']}")
                return data.get("result", {})

    async def evaluate(self, expression, await_promise=False):
        res = await self.call_tab("Runtime.evaluate", {
            "expression": expression,
            "returnByValue": True,
            "awaitPromise": await_promise
        })
        return res.get("result", {}).get("value")

    async def ensure_page_loaded(self):
        url = await self.evaluate("location.href")
        if FARM_URL not in str(url):
            print(f"[CDP] 导航至 {FARM_URL}")
            await self.call_tab("Page.navigate", {"url": FARM_URL})
            await asyncio.sleep(3)
        
        # 检查并关闭公告弹窗
        close_notice_js = """(() => {
            const btns = [...document.querySelectorAll('button')];
            const known = btns.find(x => (x.innerText || '').includes('我知道了'));
            if (known) { known.click(); return 'closed_notice'; }
            return 'no_notice';
        })()"""
        await self.evaluate(close_notice_js)

        # 检查是否获取数据失败（登录态失效 / 401）
        for _ in range(10):
            text = await self.evaluate("document.body ? document.body.innerText : ''")
            if "获取农场数据失败" in str(text) or "重新登录" in str(text):
                raise RuntimeError("农场页面数据加载失败！请检查登录状态或刷新 Chrome 页面。")
            if "轻松农场" in str(text) or "我的农田" in str(text):
                return True
            await asyncio.sleep(1)
        return True

    async def fetch_api(self, path, method="GET", body=None):
        """通过页面 Context 的 fetch 执行 API 请求，自带 auth 状态与 Turnstile"""
        js = f"""(async () => {{
            try {{
                const opts = {{
                    method: '{method}',
                    headers: {{ 'Content-Type': 'application/json' }}
                }};
                if ({json.dumps(body)}) {{
                    opts.body = JSON.stringify({json.dumps(body)});
                }}
                const res = await fetch('{path}', opts);
                const data = await res.json().catch(() => null);
                return {{ status: res.status, ok: res.ok, data }};
            }} catch (e) {{
                return {{ status: 0, ok: false, error: e.message }};
            }}
        }})()"""
        return await self.evaluate(js, await_promise=True)

    async def get_farm_status(self):
        await self.ensure_page_loaded()
        res_crops = await self.fetch_api("/api/farm/crops")
        res_energy = await self.fetch_api("/api/farm/energy/status")
        res_wallet = await self.fetch_api("/api/wallet/balance")
        res_inv = await self.fetch_api("/api/farm/inventory")

        crops_data = res_crops.get("data", {}) if res_crops and res_crops.get("ok") else {}
        energy_data = res_energy.get("data", {}).get("data", {}) if res_energy and res_energy.get("ok") else {}
        wallet_data = res_wallet.get("data", {}).get("data", {}) if res_wallet and res_wallet.get("ok") else {}
        inv_data = res_inv.get("data", {}) if res_inv and res_inv.get("ok") else {}

        crops = crops_data.get("crops") or crops_data.get("data") or []
        max_slots = crops_data.get("maxSlots", 10)
        
        mature_count = sum(1 for c in crops if c.get("isMature"))
        debuff_count = sum(1 for c in crops if not c.get("isMature") and c.get("conditions"))
        growing_count = sum(1 for c in crops if not c.get("isMature"))
        empty_slots = max(0, max_slots - len(crops))

        balance_raw = wallet_data.get("wallet", {}).get("balance", 0)
        balance_display = f"${balance_raw / 500000:.2f}"

        # 背包种子
        inventory = inv_data.get("inventory") or inv_data.get("data") or []
        
        return {
            "max_slots": max_slots,
            "planted_count": len(crops),
            "empty_slots": empty_slots,
            "mature_count": mature_count,
            "debuff_count": debuff_count,
            "growing_count": growing_count,
            "energy": f"{energy_data.get('currentEnergy', 0)}/{energy_data.get('maxEnergy', 80)}",
            "balance": balance_display,
            "crops": crops,
            "inventory": inventory
        }

    async def care_all(self):
        print("[操作] 开始一键务农 (Care All)...")
        # 优先触发 DOM 点击，若无反应回退 API
        click_res = await self.evaluate("""(() => {
            const btn = document.querySelector('[aria-label="一键务农"], [data-testid="care-actions-desktop"]');
            if (btn && !btn.disabled) { btn.click(); return 'clicked_dom'; }
            return 'disabled_or_not_found';
        })()""")
        
        if click_res == "clicked_dom":
            print("[操作] 成功点击 UI「一键务农」按钮")
            await asyncio.sleep(2)
            return {"success": True, "via": "DOM"}
        else:
            print("[操作] UI 按钮不可用或未找到，调用 API /api/farm/care/all...")
            res = await self.fetch_api("/api/farm/care/all", method="POST", body={})
            print(f"[API 结果] {res}")
            return res

    async def harvest_all(self, destroy_if_full=False):
        print(f"[操作] 开始一键收菜 (Harvest All, destroyIfFull={destroy_if_full})...")
        res = await self.fetch_api("/api/farm/harvest-all", method="POST", body={"destroyIfFull": destroy_if_full})
        print(f"[API 结果] {res}")
        if res.get("data", {}).get("error", {}).get("code") == "WAREHOUSE_FULL":
            print("⚠️ [警告] 仓库已满！未设置 destroyIfFull=True，拒绝摧毁作物。")
        return res

    async def plant_inventory_seeds(self):
        print("[操作] 检查空地与背包种子准备补种...")
        status = await self.get_farm_status()
        empty = status["empty_slots"]
        if empty <= 0:
            print("[提示] 菜地已满，无需种植。")
            return

        inventory = status["inventory"]
        # 过滤出数量 > 0 的种子
        available_seeds = [item for item in inventory if item.get("quantity", 0) > 0 and item.get("seedId")]
        
        if not available_seeds:
            print("[提示] 背包中没有可用种子库存，按照安全策略不出资购买新种子。")
            return

        # 默认使用背包中数量最多的种子
        available_seeds.sort(key=lambda x: x.get("quantity", 0), reverse=True)
        target_seed = available_seeds[0]
        seed_id = target_seed["seedId"]
        seed_name = target_seed.get("seedName", seed_id)
        qty_to_plant = min(empty, target_seed["quantity"])

        print(f"[操作] 使用背包库存种子【{seed_name}】补种 {qty_to_plant} 块空地...")
        res = await self.fetch_api("/api/farm/plant-batch", method="POST", body={
            "seedId": seed_id,
            "quantity": qty_to_plant
        })
        print(f"[API 结果] {res}")
        return res

    async def close(self):
        if self.ws:
            await self.ws.close()

async def main_async(args):
    http_url, ws_url = discover_cdp()
    if not ws_url:
        print("[错误] 未能成功发现 CDP WebSocket 端口。请确保 Chrome 已启动并开启了 Remote Debugging。")
        sys.exit(2)

    client = FarmClient(ws_url)
    try:
        await client.connect()

        if args.mode == "status":
            status = await client.get_farm_status()
            print("\n========== 轻松农场状态快照 ==========")
            print(f"黑白币余额 : {status['balance']}")
            print(f"偷菜体力   : {status['energy']}")
            print(f"农田槽位   : 已用 {status['planted_count']} / 上限 {status['max_slots']} (空闲 {status['empty_slots']})")
            print(f"作物状态   : 待收 {status['mature_count']} | 生长中 {status['growing_count']} | 需务农 {status['debuff_count']}")
            print("======================================")

        elif args.mode == "care":
            await client.care_all()

        elif args.mode == "harvest":
            await client.harvest_all(destroy_if_full=args.destroy_if_full)

        elif args.mode == "plant":
            await client.plant_inventory_seeds()

        elif args.mode == "run":
            print(">>> 启动轻松农场全自动流水线 (Care -> Harvest -> Plant) <<<")
            status = await client.get_farm_status()
            print(f"[状态] 待收获: {status['mature_count']}, 待务农: {status['debuff_count']}, 空位: {status['empty_slots']}")
            
            if status['debuff_count'] > 0:
                await client.care_all()
                await asyncio.sleep(2)
            
            if status['mature_count'] > 0:
                await client.harvest_all(destroy_if_full=args.destroy_if_full)
                await asyncio.sleep(2)
            
            await client.plant_inventory_seeds()
            print(">>> 农场流水线执行完毕 <<<")

    finally:
        await client.close()

def main():
    parser = argparse.ArgumentParser(description="hybgzs 轻松农场自动化脚本")
    parser.add_argument("mode", nargs="?", default="status", choices=["status", "run", "care", "harvest", "plant"],
                        help="运行模式: status(状态), run(自动流程), care(务农), harvest(收菜), plant(种背包种子)")
    parser.add_argument("--destroy-if-full", action="store_true", help="当仓库爆满时强制摧毁多余收获 (默认关)")
    args = parser.parse_args()

    asyncio.run(main_async(args))

if __name__ == "__main__":
    main()
