# 接手审计报告与整改清单

> 审计日期：2026-07-25 · 分支：`15min` · 方式：只读审计后按清单整改
> 本文档记录接手时的代码库状态、风险结论与整改进度，后续接手者请以此为起点。

## 一、总体结论

项目整体可运行：`pytest tests/` 27 个用例全部通过，全量字节码编译无错，MySQL
持久化迁移（`core/database.py`）与停机补结算（`core/reconciliation.py`）质量扎实、
有测试覆盖。

真正的风险不是"没写完"，而是**三处"看起来写完了其实是假的"**：

1. `execution/` 目录下约 1500 行伪装成生产实现的死代码（假市场 stub、坏 import、
   过期的下单语义），不在实盘路径上，但极易误导接手者接线后造成资金损失。
2. 风控引擎账户余额恒为默认 `$100`（`set_account_balance()` 无调用方、
   `ACCOUNT_BALANCE_USD` 未进 `.env.example`），日亏/回撤闸门形同虚设。
3. 绩效面板基于硬编码 `$1000` 虚构本金、纯内存、重启归零，驾驶舱"账户权益"不可信。

叠加 Docker 默认 `--live` + `LIVE_CONFIRM=true` 免确认直接实盘，P0 整改主题是
**"让实盘模式配得上实盘"**。

## 二、关键事实备忘

- 实盘下单真实路径：Nautilus 原生 Polymarket adapter
  （`bot/runner.py` 构造 `PolymarketExecClientConfig`，
  `bot/strategy.py` `order_factory.market(..., quote_quantity=True)` + `submit_order`）。
  `execution/` 下除 `risk_engine.py` 外均不在此路径上。
- 持久化已完全迁移至 MySQL（`core/database.py` 拒绝非 MySQL/MariaDB）。根目录
  `feature_store.db` / `signal_recordings.db` / `paper_trades.json` 仅被一次性迁移脚本
  `scripts/migrate_to_mysql.py` 只读使用，运行时不再读写。
- 补结算：`reconcile_trades.py` 只捞 `outcome='PENDING'`，写回带乐观锁
  （`settle_pending` 的 `WHERE outcome='PENDING'`）。
- Redis `btc_trading:simulation_mode` 可在运行中切换模拟/实盘，
  是绕过 `main.py` preflight 的旁路。
- 本地 `main` 分支比 `origin/main` 多 1 个未推送提交（`14b5e06`）。

## 三、按优先级整改清单

| # | 优先级 | 任务 | 关键位置 | 状态 |
|---|--------|------|----------|------|
| 1 | 🔴 P0 | 风控接真实余额：同步调用 `set_account_balance()`；`.env.example` 补 `ACCOUNT_BALANCE_USD` 等缺失变量；`MAX_POSITION_USD` 成为独立闸门 | `execution/risk_engine.py` | ✅ 本次完成 |
| 2 | 🔴 P0 | 处置 `execution/` 死代码：DEPRECATED 标记；修坏 import（`nautilus_polymarket_integration` 不存在）；移除 `metrics_exporter` 无用实例化 | `execution/execution_engine.py:262` | ✅ 本次完成 |
| 3 | 🔴 P0 | 收紧实盘开关：`.env.example` 补 `LIVE_CONFIRM`/`BOT_ARGS`/`DASHBOARD_BIND_ADDRESS` 并显著提示 Docker 默认即实盘（复核更正：`redis_control.py` 切 live 原本就有 `yes` 二次确认，无需改动） | `docker-compose.yml:18-19,36` | ✅ 本次完成 |
| 4 | 🟠 P1 | 修停机全量 upsert 覆盖补结算的竞态：`upsert` 不得把已结算记录覆写回 PENDING | `bot/strategy.py`（on_stop 保存）、`core/database.py` | ✅ 本次完成 |
| 5 | 🟠 P1 | `SETTLEMENT_UNRESOLVED`/`SETTLEMENT_FALLBACK` 记录纳入补结算重算范围 | `core/database.py`、`core/reconciliation.py` | ✅ 本次完成 |
| 6 | 🟠 P1 | PerformanceTracker 初始本金从 env 读取，替代硬编码 $1000 | `monitoring/performance_tracker.py` | ✅ 本次完成 |
| 7 | 🟡 P2 | 修文档漂移：AGENTS.md 分阶段检查死链（`test_nautilus.py`/`test_strategy.py`）、`.env.example` 死配置 `MAX_POSITION_SIZE`、不存在的 `check_wallet.py`、`REDIS_DB` 默认值不一致 | `scripts/`、`AGENTS.md` | ✅ 本次完成 |
| 8 | 🟡 P2 | supervisor 加指数退避与最大连续崩溃次数 | `supervisor.py` | ✅ 本次完成 |
| 9 | 🟢 P3 | 仓库卫生：untrack 47 个 `.pyc` 与 3 个本地数据快照（磁盘文件保留）；修 dashboard"待接入"过期文案。遗留：推送 main 滞留提交 `14b5e06`（对外操作，待人工确认）；删除 `metrics_exporter` 内嵌的第二份 dashboard HTML | `git ls-files` | ⏳ 部分完成 |
| 10 | 🟢 P3 | 添加 CI：pytest 门禁 | — | ❌ 按接手方决定不引入 CI；提交前手动运行 `python -m pytest tests/` |
| 11 | 🟢 P3 | 文档规范落地：`docs/README.md`（索引 + 规范），本文档迁入 `docs/`，AGENTS.md 增加文档规范章节 | `docs/README.md` | ✅ 本次完成 |

## 四、遗留风险（本次未整改，接手者注意）

- **结算三级降级**（`bot/strategy.py` `_resolve_settlement` 一带）：Chainlink 不可用时
  逐级退化到"按入场价平仓"，会产生盈亏为零的假记录。本次已让这类记录可被
  `reconcile_trades.py` 重算（清单 #5），但降级逻辑本身未动。
- **停机预算 10s，二次 Ctrl+C 走 `os._exit(130)`** 跳过 MySQL flush（`bot/shutdown.py`），
  属有意设计权衡；配合 #4 的修复后数据风险已降低，但仍建议避免连续两次 Ctrl+C。
- **驾驶舱 HTTP 无鉴权**：裸机运行绑定 `0.0.0.0:8000`（`metrics_exporter.py`）。
  Docker 下由 `DASHBOARD_BIND_ADDRESS`（默认 127.0.0.1）兜底；公网部署必须加反代鉴权。
- **`metrics_exporter.py` 内嵌旧版 dashboard HTML 兜底副本**（约 230 行），与
  `monitoring/dashboard.html` 会漂移，建议后续删除兜底改为明确报错。
- **无 lint 配置**：建议引入 ruff 并逐步收敛。

## 五、审计过程记录（摘要）

- 文档一致性：AGENTS.md 所列脚本/参数大体属实；发现死链 2 处、`check_wallet.py`
  引用失效、"无 pytest"表述过时（`tests/` 已有 593 行用例）。
- 未完成实现：全仓仅 1 处 TODO（`polymarket_client.py:160` 假市场 stub，无调用方）；
  更多问题是死代码与过期文案（详见第一节）。
- 近期提交主线：MySQL 迁移 → 查询优化 → 停机补结算工具 → 实时驾驶舱 → 收尾。
- 测试/构建/lint：pytest 27 通过；无 CI；无 lint 配置；AGENTS.md 五步检查中 2 步死链，
  其余 3 步依赖网络未在审计中运行。
