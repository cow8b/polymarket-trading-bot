# Repository Guidelines

## 项目结构与模块组织

本仓库是 Python 实现的 Polymarket BTC 15 分钟交易机器人。主 CLI 入口是 `main.py`；`supervisor.py` 用于长时间运行和自动重启。核心策略、数据摄取、结算和 Nautilus 集成位于 `core/`。机器人编排代码在 `bot/`，执行与风控在 `execution/`，数据源适配器在 `data_sources/`，监控与 Grafana 支持在 `monitoring/` 和 `infra/grafana/`，学习反馈逻辑在 `feedback/`。工具脚本和分阶段检查脚本位于 `scripts/`。测试分布在 `tests/`、模块级 `test_*.py` 文件以及脚本化检查中。

## 构建、测试与开发命令

创建隔离环境并安装依赖：

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

实盘前先使用安全模式本地运行：

```bash
python main.py --test-mode      # 快速模拟循环
python main.py --simulation     # 正常 15 分钟纸面交易模式
python supervisor.py --live     # 实盘交易；涉及真实资金
```

Docker 部署只包含应用容器；MySQL、Redis 使用 `.env` 中配置的外部服务。默认启动实盘：

```bash
docker compose build            # 构建应用镜像并安装依赖
docker compose up -d            # 后台启动，默认 python main.py --live
docker compose logs -f bot      # 查看日志
docker compose restart bot      # 重启应用容器
docker compose down             # 停止并移除容器
```

临时模拟运行使用 `BOT_ARGS=--simulation docker compose up`；需要终端 TUI 时使用
`BOT_ARGS="--live --tui" docker compose up`。Docker 部署默认设置
`LIVE_CONFIRM=true`，因此不会卡在实盘确认输入。

先运行无网络单元测试，再按顺序运行分阶段检查（需要网络）：

```bash
python -m pytest tests/                  # 无网络单元测试（持久化、补结算、分析）
python scripts/test_data_sources.py test
python scripts/test_ingestion.py test
python scripts/test_execution.py test
```

历史上的 Phase 3/4 检查（`scripts/test_nautilus.py`、`scripts/test_strategy.py`）
的目标文件在目录重构后已不存在，脚本保留为弃用提示；对应验证改由
`pytest tests/` 与 `python main.py --test-mode` 覆盖。

使用 `python scripts/view_trades.py` 查看模拟交易历史。
机器人停机后，先使用
`python scripts/reconcile_trades.py --dry-run --mode paper` 预览遗留待结算交易；
只有确认官方结果与盈亏正确后，才使用 `--apply` 写回 MySQL。该命令不会下单或自动赎回。

## 代码风格与命名约定

使用惯用 Python 风格，采用 4 空格缩进。可行时添加类型标注；入口点和复杂集成应有清晰 docstring。函数、变量和模块使用 `snake_case`，类使用 `PascalCase`。代码注释优先使用简体中文，除非周边上下文已经明确使用英文。保持副作用显式，尤其是实盘交易、补丁应用、Redis 模式控制、文件写入和日志写入。导入顺序遵循现有模式：标准库、第三方依赖、本地模块。

## 测试指南

`tests/` 下是可直接用 `python -m pytest tests/` 运行的无网络单元测试（持久化仓库、补结算、分析与驾驶舱指标），提交前必须保持全绿。脚本化分阶段检查（见上）覆盖需要网络的数据源与执行链路。新增行为时，在相关模块附近或 `tests/` 下添加聚焦的 `test_*.py`。交易逻辑应覆盖模拟安全行为和关键风控约束，例如入场价格限制、价差过滤、冷却时间和单市场最大交易次数。

完整的测试、审核与验收流程（变更风险分级 L1/L2/L3、资金路径审查清单、模拟验收与小额实盘灰度标准）见 `docs/TESTING_ACCEPTANCE.md`；任何合入/上线判定以该文档为准。

## 文档规范

除根目录的 `README*` 与本文件外，所有项目文档统一放在 `docs/` 下，并在
`docs/README.md` 的索引表登记。完整规范（命名、语言、开头约定、与代码同步的
要求）见 `docs/README.md`。改变行为的 PR 必须同步更新受影响文档；文档中引用的
脚本、参数、路径必须真实存在。接手基线与风险清单见 `docs/HANDOVER_AUDIT.md`。

## 提交与 Pull Request 规范

近期提交历史以简短祈使句为主，偶尔使用 `feat:`、`docs:` 等 Conventional Commit 前缀。建议使用简洁信息，例如 `fix: 优化策略后台任务停机流程` 或 `docs: 更新模拟运行说明`。PR 应说明行为变化、列出已运行的验证命令、标注是否影响实盘交易或配置，并关联相关 issue。只有终端 UI、Grafana 或仪表盘变更需要附截图。

## Agent 强制协作要求

每次修改仓库文件后，最终回复必须提供建议的 git 提交信息，便于协作者快速理解本次变更范围。提交信息应简短、具体，并优先使用 Conventional Commit 风格，描述部分使用简体中文，例如 `fix: 优化策略后台任务停机流程`。如果一次变更多个主题，可以提供 1 条推荐提交信息和必要的备选拆分建议。

## 安全与配置提示

不要提交 `.env`、私钥、Polymarket API 凭据、Redis 密钥、生成的日志或本地数据库快照。配置从 `.env.example` 开始。先使用模拟模式验证，再考虑实盘；`--live` 和 `supervisor.py --live` 都应视为具有生产风险的命令。
