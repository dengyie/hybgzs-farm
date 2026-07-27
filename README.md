# hybgzs-farm

黑与白福利站（`cdk.hybgzs.com/entertainment/farm`）轻松农场自动化脚本。

## 功能

- **自动发现 CDP**：复用已登录 Chrome（无头/有头），不重启、不杀进程、不硬写死 9222。
- **一键务农 (care)**：自动处理 浇水/除草/杀虫 去除 debuff。
- **一键收菜 (harvest)**：先务农后收获（`destroyIfFull=false` 保护仓库）。
- **智能种菜 (plant)**：识别空地，优先使用背包库存种子填充，防自动扣币。
- **命令行操作**：支持 `status`, `run`, `care`, `harvest`, `plant` 模式。

## 安全与红线

1. **绝对禁杀 Chrome**：遇 CF 人机在 Chrome 前台手动过验证，脚本等待不崩溃。
2. **零扣币安全**：默认仅种植库存种子，购买新种子须显式声明参数。
3. **零毁菜安全**：仓库爆满时拒绝强制摧毁收获，保障资产。

## 使用方法

```bash
cd /Users/mango/project/hermes/hybgzs-farm
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 查看农场状态
python3 farm_runner.py status

# 执行完整流水线（务农 -> 收菜 -> 补种库存种子）
python3 farm_runner.py run

# 仅务农
python3 farm_runner.py care

# 仅收菜
python3 farm_runner.py harvest

# 仅补种（只用库存）
python3 farm_runner.py plant
```
