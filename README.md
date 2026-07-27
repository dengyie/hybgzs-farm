# hybgzs-farm

黑与白福利站轻松农场自动化 / **挂机守护**  
`https://cdk.hybgzs.com/entertainment/farm`

GitHub: https://github.com/dengyie/hybgzs-farm

## 两种用法

| 模式 | 命令 | 场景 |
|------|------|------|
| 一次性 | `python3 farm_runner.py run` | 手动收一波 |
| **挂机** | `python3 farm_runner.py daemon` | VPS/本机 7×24，类似囤囤鼠 daemon |

## 挂机设计（像囤囤鼠，但更省）

```
loop:
  发现 CDP → 短连接
  care → harvest → plant（有活才写）
  写 journal JSONL
  断开 CDP          ← 休眠期间不占 WS/事件
  sleep(min(成熟-lead, care_every)) 可被 SIGTERM 打断
```

- **成熟前提前 `--lead` 秒醒来收菜**
- **生长期按 `--care-every` 巡检灾害**（默认 10 分钟）
- 最短/最长休眠：`--min-sleep` / `--max-sleep`（默认 60s ~ 30min）
- 单实例 flock，防多开
- 不 launch / 不杀 Chrome；需已有 headed/CDP 登录态

## 本机快速挂机

```bash
cd /Users/mango/project/hermes/hybgzs-farm
pip install -r requirements.txt   # websockets

# 前台挂机（看日志）
python3 farm_runner.py daemon

# 或
./scripts/farm-daemon.sh

# 只跑 2 轮自测
python3 farm_runner.py daemon --max-cycles 2 --min-sleep 5 --care-every 30
```

## VPS 部署

```bash
git clone https://github.com/dengyie/hybgzs-farm.git /opt/hybgzs-farm
cd /opt/hybgzs-farm
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# Chrome/Chromium 需 --remote-debugging-port=9222 且 cookie 已登录
export FARM_CDP=http://127.0.0.1:9222

# 前台
.venv/bin/python farm_runner.py daemon

# 或 systemd：见 scripts/farm-daemon.service.example
```

## 一次性命令

```bash
python3 farm_runner.py status
python3 farm_runner.py run
python3 farm_runner.py care|harvest|plant
python3 farm_runner.py plant --allow-buy   # 明确扣币
python3 farm_runner.py run --dry-run
```

## 日志

| 文件 | 内容 |
|------|------|
| `~/.cache/hybgzs-farm/farm-YYYYMMDD.log` | 结构化运行日志 |
| `~/.cache/hybgzs-farm/daemon-journal.jsonl` | 每轮摘要（挂机） |
| `~/.cache/hybgzs-farm/farm_runner.lock` | 单实例锁 |

```bash
tail -f ~/.cache/hybgzs-farm/farm-$(date +%Y%m%d).log
tail -f ~/.cache/hybgzs-farm/daemon-journal.jsonl
```

## 红线

1. 不杀 / 不擅自重启用户 Chrome  
2. 默认不 `destroyIfFull`、不自动购种  
3. CF/人机：等 headed 登录恢复，失败退避休眠  
4. 挂机休眠期断开 CDP，降低 CPU/内存  

## 环境变量

| 变量 | 含义 |
|------|------|
| `FARM_CDP` / `COINBOT_CDP` | CDP HTTP 基址 |
| `FARM_LOG_LEVEL` | INFO/DEBUG |
| `FARM_MIN_SLEEP` / `FARM_MAX_SLEEP` | 休眠上下限 |
| `FARM_LEAD` | 成熟提前量秒 |
| `FARM_CARE_EVERY` | 灾害巡检间隔 |
