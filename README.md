# hybgzs-farm

黑与白福利站轻松农场自动化：`https://cdk.hybgzs.com/entertainment/farm`

## 能力

- 自动发现 headed CDP（9222/9223…），复用登录态，不杀 Chrome
- **智能流水线** `run`：务农 → 收菜 → 复检 → 按价值补种
- 选种：`harvestValue/growthTime` 单位时间价值，**库存优先**，可选 `--allow-buy`
- `status`：余额/体力/待收/灾害/空位/仓库/下次成熟 ETA/动作建议
- 默认不毁仓、不扣币买种

## 命令

```bash
cd /Users/mango/project/hermes/hybgzs-farm
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python3 farm_runner.py status
python3 farm_runner.py run
python3 farm_runner.py run --dry-run
python3 farm_runner.py care
python3 farm_runner.py harvest
python3 farm_runner.py plant
python3 farm_runner.py plant --seed carrot
python3 farm_runner.py plant --allow-buy   # 明确允许扣币
python3 farm_runner.py run --json
```

## 红线

1. 不 launch / 不杀 Chrome  
2. `destroyIfFull` 默认 false  
3. 购种默认关闭（`--allow-buy`）  
4. CF/人机：headed 等待，不假装成功  

## VPS

Clone 本仓，在 VPS 上对接带登录 Cookie 的 headed/CDP Chrome 即可：

```bash
git clone https://github.com/dengyie/hybgzs-farm.git
cd hybgzs-farm && python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python3 farm_runner.py run
```

## 日志与资源

- 日志目录：`~/.cache/hybgzs-farm/farm-YYYYMMDD.log`
- 级别：`--log-level DEBUG|INFO|WARNING` 或环境变量 `FARM_LOG_LEVEL`
- 单实例锁：`~/.cache/hybgzs-farm/farm_runner.lock`（防并发打爆 CDP）
- 默认**一次性进程**：跑完断开 WebSocket；不开 Network 事件监听；临时 tab 会关
- 状态行带 `资源: Xs cdp=N api=M rss=MB`

```bash
python3 farm_runner.py status --log-level DEBUG
tail -f ~/.cache/hybgzs-farm/farm-$(date +%Y%m%d).log
```
