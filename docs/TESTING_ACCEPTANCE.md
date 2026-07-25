# 测试・审核・验收规范

> 本文档规定代码变更从开发完成到合入/上线的验证流程：测试怎么跑、审核看什么、
> 验收怎么算通过。适用于所有代码与配置变更；文档-only 变更仅需第四节的文档检查。
> 制定日期：2026-07-25。

## 一、变更风险分级

先给变更定级，级别决定后续测试与验收的深度：

| 级别 | 定义 | 典型目录/文件 |
|------|------|--------------|
| **L1 低** | 不影响运行行为：文档、注释、日志文案、工具脚本输出格式 | `docs/`、`README*`、`scripts/view_trades.py` 展示层 |
| **L2 中** | 影响运行行为，但不直接触碰资金与结算：信号处理器参数、驾驶舱/指标、数据摄取、监控 | `core/strategy/processors/`、`monitoring/`、`core/ingestion/`、`data_sources/` |
| **L3 高（资金路径）** | 直接影响下单、风控、结算、持久化写入、模式切换、停机流程 | `bot/strategy.py`、`bot/runner.py`、`bot/shutdown.py`、`execution/risk_engine.py`、`core/database.py`、`core/reconciliation.py`、`core/settlement/`、`scripts/redis_control.py`、`docker-compose.yml`、`.env.example` 中的交易/风控项 |

拿不准就按高一级处理。触碰多个级别的变更按最高级别走。

## 二、测试规范（提交者执行）

### 所有级别（必须）

```bash
python -m pytest tests/          # 必须全绿，一个失败都不允许合入
```

- 新增或修改行为必须附带聚焦的 `test_*.py` 用例（放 `tests/` 或相关模块旁），
  覆盖正常路径 + 至少一个边界/失败路径。
- 修 bug 必须先写一个能复现该 bug 的失败用例，再修到绿。
- 测试必须无网络、无外部服务依赖（数据库用内存 SQLite，见
  `tests/test_database.py` 的做法）；需要网络的验证放到分阶段检查里。

### L2 及以上（按改动面选跑，需网络）

```bash
python scripts/test_data_sources.py test   # 改了 data_sources/
python scripts/test_ingestion.py test      # 改了 core/ingestion/
python scripts/test_execution.py test      # 改了 execution/
```

### L3 额外（模拟冒烟，必须）

```bash
python main.py --test-mode        # 快速模拟循环，至少完整跑 2 个决策周期
```

观察点（任一不满足即未通过）：
1. 启动无异常栈；日志出现 `READY TO TRADE`；
2. 改了风控/余额相关：日志出现 `Risk engine balance set to $...` 且数值合理；
3. 至少产生一笔纸面交易并走完 入场 → 出场/结算 闭环，`outcome` 落库不为空；
4. Ctrl+C 一次能在 10 秒内干净退出，无 `Traceback`。

## 三、审核规范（Reviewer 执行）

PR 说明必须包含（缺一退回）：**行为变化、已运行的验证命令及结果、变更级别
（L1/L2/L3）、是否影响实盘交易或配置**。UI/驾驶舱/Grafana 变更附截图。

### 通用审查项

- 测试是否真的覆盖了改动的行为，而不是为凑数写的形式用例；
- 文档同步：引用的脚本/参数/路径真实存在，`docs/README.md` 索引已登记，
  `.env.example` 与代码读取的环境变量一致（本仓库历史教训见
  [HANDOVER_AUDIT.md](HANDOVER_AUDIT.md)）；
- 无新增死代码；废弃代码必须带 DEPRECATED 头说明替代路径。

### L3 强制逐条过（资金路径审查清单）

- [ ] 下单语义：金额单位是 USD 还是股数？`quote_quantity` 语义与
      `patches/market_orders.py` 一致？
- [ ] 风控闸门：改动是否绕过 `validate_new_position`？限额是否仍独立于
      `MARKET_BUY_USD`？
- [ ] 持久化并发：写库是否可能覆盖补结算结果（`upsert` 的
      PROVISIONAL_OUTCOMES 守卫）？停机路径（`on_stop`/`shutdown.py`）是否受影响？
- [ ] 结算降级：新逻辑在 Chainlink/盘口不可用时落到哪个分支？产生的记录
      能否被 `reconcile_trades.py` 重算？
- [ ] 模式切换：模拟/实盘判断（Redis `simulation_mode`）在新代码路径上是否仍生效？
- [ ] 副作用显式：真实下单、文件写入、Redis 写入、配置变更在代码里一眼可见，
      无隐藏开关。

## 四、验收规范（合入/上线判定)

### L1：文档与无行为变更

- `pytest` 全绿 + 文档检查（索引登记、链接有效、无敏感信息）即可合入。

### L2：普通代码变更

- 第二节测试全部通过 + 审核通过即可合入；
- 涉及驾驶舱/指标的，合入前在 `--simulation` 下打开 dashboard 目视确认无
  空白卡片、无 JS 报错。

### L3：资金路径变更（三步走，逐步放行）

1. **模拟验收**：`python main.py --simulation` 连续运行 ≥ 4 个完整 15 分钟
   市场周期。通过标准：无异常栈；每笔交易在 MySQL `trade_history` 中记录
   完整（entry/exit/outcome/pnl 无空洞）；
   `python scripts/reconcile_trades.py --dry-run --mode paper` 无 ERROR/INVALID。
2. **小额实盘灰度**：临时将 `MARKET_BUY_USD` 调到最小可下单额（$1），实盘
   运行 ≥ 2 个市场周期。通过标准：订单提交-成交-结算闭环正常；
   `scripts/view_trades.py` 中盈亏与 Polymarket 页面一致；风控日志中的余额
   与真实钱包一致。
3. **恢复正常参数上线**，24 小时内保持对日志与驾驶舱的观察；发现异常立即
   `docker compose down`（或 Ctrl+C）回滚到上一提交重启。

验收执行记录（跑了哪些命令、观察到什么）写进 PR 评论，作为可追溯的验收凭证；
产物文件按第五节留档入库。

## 五、验收产物留档

验收不只留在 PR 评论里——产物文件随验收提交入库，目录约定：

```
docs/acceptance/<YYYYMMDD-HHMM>-<主题slug>/
├── report.html      # 策略统计分析报告（必须，见下）
├── stats.json       # 机器可读统计摘要，用于两次验收之间对比
├── pytest.txt       # pytest 输出（tail 即可）
└── notes.md         # 可选：观察记录、reconcile --dry-run 输出、灰度结论
```

### 策略统计分析报告（策略/资金路径变更必须）

凡触碰策略逻辑或 L3 资金路径的验收，必须附图表化的统计分析报告，用报告
生成器从真实交易记录产出：

```bash
# 模拟验收后（读 paper 记录）：
python scripts/acceptance_report.py --mode paper --subject <主题> --level L3
# 小额实盘灰度后（读 live 记录）：
python scripts/acceptance_report.py --mode live --subject <主题> --level L3
```

报告为单文件 HTML（内联 SVG、明暗双模式、无外部依赖），包含并以此为
验收查验点：

| 图表/区块 | 验收看什么 |
|-----------|-----------|
| 统计卡（笔数/胜率/累计盈亏/盈利因子/最大回撤） | 与变更预期一致，无异常量级 |
| 权益曲线 | 无异常跳变；趋势与日志一致 |
| 每笔盈亏 | 单笔亏损不超风控上限；无连续同因大额亏损 |
| 结果分布 | PENDING/UNRESOLVED 无堆积（停机后应≈0） |
| 入场价区间胜率 | 与入场价带（MIN/MAX_ENTRY_PRICE）设计一致 |
| 平仓原因分布 | SETTLEMENT_FALLBACK/UNRESOLVED 占比接近 0 |
| 交易明细表 | 抽查 3 笔与 Polymarket 页面/日志核对 |

对比性验收（如调整信号阈值）应同时保留变更前基线目录，`stats.json`
逐项对比写进 `notes.md`。

### 留档规则

- **临时/中间产物一律放 `runtime/` 下**（验收试跑用 `runtime/acceptance/`，
  已在 `.gitignore` 中忽略），不要写到系统 `/tmp` ——保证产物在项目内可查、
  不因系统清理丢失，也不误入版本库：

  ```bash
  # 试跑/预览（临时，不入库）：
  python scripts/acceptance_report.py --mode paper --subject <主题> --out runtime/acceptance
  # 正式留档（默认输出 docs/acceptance/，随 PR 入库）：
  python scripts/acceptance_report.py --mode paper --subject <主题> --level L3
  ```

- 产物目录随验收 PR 提交入库（`docs/acceptance/` 不进 `.gitignore`）；
- 报告内不得包含私钥、API 凭据、`.env` 值（报告生成器只读交易记录，
  正常不会引入，合入前扫一眼确认）；
- 同一验收多轮执行只保留最终轮 + 首轮基线，中间轮删除，避免仓库膨胀。

### 回滚标准（任一命中立即回滚）

- 实盘出现非预期下单（方向、金额、市场与信号日志不符）；
- `trade_history` 出现无法解释的空洞或重复；
- 停机后 `reconcile_trades.py --dry-run` 出现本不该有的 PENDING 堆积；
- 风控余额与真实钱包持续偏离。
