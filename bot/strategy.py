"""
bot.strategy — IntegratedBTCStrategy: the main Nautilus trading strategy.

Subscribes to Polymarket BTC 15-min UP/DOWN markets, runs a full 6-step ML
decision loop, and routes orders through paper-trading or live Nautilus
execution depending on the current simulation mode.
"""
from __future__ import annotations

import asyncio
import math
import os
import random
import threading
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional

import httpx

from loguru import logger
from nautilus_trader.adapters.polymarket.common.constants import POLYMARKET_VENUE
from nautilus_trader.model.currencies import pUSD
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy

from bot.models import (
    MARKET_INTERVAL_SECONDS,
    QUOTE_MIN_SPREAD,
    QUOTE_STABILITY_REQUIRED,
    LiveTrade,
    PaperTrade,
    _make_stub_signal,
    interp_exit_fracs,
    is_book_sane,
)
from core.analytics import calculate_prediction_edge, calculate_strategy_edge
from core.database import OrderLifecycleRepository, TradeHistoryRepository
from core.recording import get_signal_recorder
from core.settlement import get_settlement_tracker
from core.strategy.fusion import get_fusion_engine
from core.strategy.ml_engine import get_ml_engine
from core.strategy.processors.cvd_orderbook import CVDOrderBookProcessor
from core.strategy.processors.deribit_pcr import DeribitPCRProcessor
from core.strategy.processors.divergence import PriceDivergenceProcessor
from core.strategy.processors.funding_oi import FundingRateOIProcessor
from core.strategy.processors.liquidation import LiquidationProcessor
from core.strategy.processors.ohlcv_momentum import OHLCVMomentumProcessor
from core.strategy.processors.orderbook import OrderBookImbalanceProcessor
from core.strategy.processors.sentiment import SentimentProcessor
from core.strategy.processors.spike import SpikeDetectionProcessor
from core.strategy.processors.tick_velocity import TickVelocityProcessor
from execution.risk_engine import get_risk_engine
from feedback.learning_engine import get_learning_engine
from monitoring.metrics_exporter import get_grafana_exporter
from monitoring.performance_tracker import get_performance_tracker

try:
    from monitoring.terminal_ui import tui_event
except ImportError:
    def tui_event(*args, **kwargs):  # noqa: E303
        pass


class IntegratedBTCStrategy(Strategy):
    """
    Integrated BTC 15-min trading strategy.

    Lifecycle
    ---------
    1. ``on_start`` — loads all BTC 15-min instruments, subscribes, starts
       background threads (WebSocket streams, settlement tracker, Grafana).
    2. ``on_quote_tick`` — buffers ticks; when inside the trade window fires
       ``_make_trading_decision_sync``. Open positions (live and paper) exit
       via TP / SL / TIME-EXIT on every tick; survivors settle at market end.
    3. ``_make_trading_decision`` — the 6-step ML decision loop.
    4. ``on_stop`` — saves paper trades.
    """

    def __init__(
        self,
        redis_client=None,
        enable_grafana: bool = True,
        test_mode: bool = False,
        simulation: bool = False,
    ):
        super().__init__()

        self.bot_start_time = datetime.now(timezone.utc)
        self.restart_after_minutes = 90
        self._stopping = False

        self.instrument_id: Optional[InstrumentId] = None
        self.redis_client = redis_client
        # Initialise from the CLI flag so the simulation state is correct even
        # when Redis is offline or slow to respond.
        self.current_simulation_mode = simulation

        self.all_btc_instruments: List[Dict] = []
        self.current_instrument_index: int = -1
        self.next_switch_time: Optional[datetime] = None

        self._stable_tick_count = 0
        self._market_stable = False
        self._last_instrument_switch = None

        self.last_trade_time = -1
        self._waiting_for_market_open = False
        self._last_bid_ask = None

        # Async instrument loading state. Nautilus loads Polymarket instruments
        # in a background task; on_start() can run before the cache is populated.
        # Track retries so the timer loop can re-attempt without spamming logs.
        self._instruments_loaded: bool = False
        self._instrument_load_attempts: int = 0
        self._max_instrument_load_attempts: int = 60  # ~10 min at 10s ticks

        # Heartbeat tracking — surfaces "what is the bot doing right now?"
        # in the main log every HEARTBEAT_SECS so users can tell it's alive
        # and see exactly when the next trade attempt will happen.
        self._last_heartbeat_ts: float = 0.0
        self._heartbeat_secs: int = 30
        self._tick_count_since_last_heartbeat: int = 0

        # Decision-cycle logging — groups the 6-step loop into one block.
        self._decision_cycle_counter: int = 0
        self._current_cycle_id: Optional[int] = None
        self._current_cycle_outcome: str = ""

        # 内置驾驶舱只消费真实数据。行情采集与策略线程隔离，避免前端轮询
        # 触发外部网络请求或阻塞交易决策。
        self._dashboard_lock = threading.RLock()
        self._dashboard_stop_event = threading.Event()
        self._dashboard_thread: Optional[threading.Thread] = None
        self._grafana_thread: Optional[threading.Thread] = None
        self._dashboard_market_data: Dict[str, Any] = {
            "updated_at": None,
            "candles_updated_at": None,
            "orderbook_updated_at": None,
            "eth_price": None,
            "candles": [],
            "orderbook": {},
            "last_error": "",
        }
        self._quote_updated_at: Optional[str] = None
        self._decision_state: Dict[str, Any] = {
            "cycle_id": None,
            "status": "idle",
            "current_stage": None,
            "outcome": "",
            "updated_at": None,
            "stages": [],
        }

        # ── Live position / exit management ─────────────────────────────────
        # Pending BUY orders awaiting their first fill report.
        self._pending_orders: Dict[str, dict] = {}
        # Open positions, keyed by entry client_order_id, that the live
        # stop-loss / take-profit handler watches on every quote tick.
        self._open_positions: Dict[str, dict] = {}
        # Outstanding exit (SELL) orders, mapping exit client_id -> entry id.
        self._pending_exits: Dict[str, str] = {}

        # Risk / exit math is *payoff-relative* for binary outcome tokens:
        #   TP = entry + take_profit_frac * (1 - entry)   # frac of upside
        #   SL = entry - stop_loss_frac   * entry          # frac of capital
        # Old behaviour ("+X% of token price") was broken for tokens near
        # 1.0 because TP clamped to 0.99 and fired immediately at fill.
        #
        # ENABLE_STOP_LOSS defaults to TRUE: without it a losing trade rides to
        # binary settlement and loses ~100% of its stake. The SL clips that
        # downside at STOP_LOSS_PCT of capital. Set ENABLE_STOP_LOSS=false to
        # revert to hold-to-settlement behaviour.
        self._stop_loss_enabled = os.getenv(
            "ENABLE_STOP_LOSS", "true"
        ).strip().lower() in ("1", "true", "yes", "on")
        try:
            self._stop_loss_frac = max(0.0, min(1.0, float(os.getenv("STOP_LOSS_PCT", "0.50"))))
        except (TypeError, ValueError):
            self._stop_loss_frac = 0.50
        try:
            self._take_profit_frac = max(0.0, min(1.0, float(os.getenv("TAKE_PROFIT_PCT", "0.40"))))
        except (TypeError, ValueError):
            self._take_profit_frac = 0.40
        # Don't try to exit in the last few seconds before settlement —
        # FAK rejects + settlement happen too fast to be useful.
        self._exit_cutoff_seconds = 30

        # ── Entry filters ────────────────────────────────────────────────
        # Refuse to bet when the token price is at an extreme (terrible
        # R:R on binary outcome tokens) or when the bid-ask spread is so
        # wide that crossing it eats most of the expected edge.
        try:
            self._min_entry_price = max(0.01, min(0.99, float(os.getenv("MIN_ENTRY_PRICE", "0.20"))))
        except (TypeError, ValueError):
            self._min_entry_price = 0.20
        try:
            self._max_entry_price = max(self._min_entry_price + 0.01, min(0.99, float(os.getenv("MAX_ENTRY_PRICE", "0.98"))))
        except (TypeError, ValueError):
            self._max_entry_price = 0.98
        try:
            self._max_spread_pct = max(0.0, float(os.getenv("MAX_SPREAD_PCT", "0.05")))
        except (TypeError, ValueError):
            self._max_spread_pct = 0.05

        # 按入场价线性插值的 SL/TP 端点（.env.example 描述已久、此前从未
        # 实现的矩阵）。四个变量齐备才启用；否则沿用固定
        # STOP_LOSS_PCT / TAKE_PROFIT_PCT 的旧行为。
        def _frac_env(name: str) -> Optional[float]:
            raw = (os.getenv(name) or "").strip()
            if not raw:
                return None
            try:
                return max(0.0, min(1.0, float(raw)))
            except (TypeError, ValueError):
                logger.warning(f"Invalid {name}={raw!r}; SL/TP 插值端点被忽略")
                return None

        endpoints = (
            _frac_env("SL_PCT_AT_MIN_ENTRY"),
            _frac_env("SL_PCT_AT_MAX_ENTRY"),
            _frac_env("TP_PCT_AT_MIN_ENTRY"),
            _frac_env("TP_PCT_AT_MAX_ENTRY"),
        )
        self._sl_tp_endpoints = endpoints if all(v is not None for v in endpoints) else None
        if self._sl_tp_endpoints:
            logger.info(
                "SL/TP 按入场价插值已启用: "
                f"SL {endpoints[0]:.0%}→{endpoints[1]:.0%}, "
                f"TP {endpoints[2]:.0%}→{endpoints[3]:.0%} "
                f"(带 [{self._min_entry_price:.2f}, {self._max_entry_price:.2f}])"
            )

        # ── 出场检查护栏 ─────────────────────────────────────────────────
        # Polymarket 的 NO 侧订单簿经常只剩 $0.01 级别的钓鱼单，裸 bid 会让
        # 止损在开仓瞬间被虚假触发并按垃圾价"成交"（2026-07-25 模拟盘 6 笔
        # 全灭的根因）。两道护栏：
        #   1) 点差超过 MAX_EXIT_SPREAD 的 tick 视为无效盘口，整体跳过；
        #   2) SL/TP 需连续 EXIT_CONFIRM_TICKS 个有效 tick 确认才触发。
        try:
            self._max_exit_spread = max(0.01, min(0.90, float(os.getenv("MAX_EXIT_SPREAD", "0.12"))))
        except (TypeError, ValueError):
            self._max_exit_spread = 0.12
        try:
            self._exit_confirm_ticks = max(1, int(float(os.getenv("EXIT_CONFIRM_TICKS", "2"))))
        except (TypeError, ValueError):
            self._exit_confirm_ticks = 2

        # ── Trade-window / cooldown ──────────────────────────────────────
        # Entry window: seconds 780-870 of each 15-min market (13:00 – 14:30).
        # The 90-second window lets the strategy enter once the market trend is
        # fully established, then exit via the forced time-based sell at 14:30.
        try:
            self._trade_window_start = max(0, int(float(os.getenv("TRADE_WINDOW_SEC_START", "780"))))
        except (TypeError, ValueError):
            self._trade_window_start = 780
        try:
            self._trade_window_end = max(
                self._trade_window_start + 30,
                int(float(os.getenv("TRADE_WINDOW_SEC_END", "870"))),
            )
        except (TypeError, ValueError):
            self._trade_window_end = 870
        try:
            self._entry_cooldown_sec = max(0, int(float(os.getenv("ENTRY_COOLDOWN_SEC", "90"))))
        except (TypeError, ValueError):
            self._entry_cooldown_sec = 90

        # Unix-ts of the most recent entry attempt; used by on_quote_tick
        # to gate re-entries inside the wider trade window.
        self._last_entry_ts: float = 0.0

        # Cooldown applied after a pre-trade filter rejection (band /
        # spread / liquidity guard). Prevents the trade-window block
        # from re-emitting on every tick (~10/s) when price stays out
        # of band — the old code set _last_entry_ts=0 here which spammed
        # the log with ~3000 banner emissions in 20 seconds.
        try:
            self._filter_reject_cooldown_sec = max(
                0, int(float(os.getenv("FILTER_REJECT_COOLDOWN_SEC", "15")))
            )
        except (TypeError, ValueError):
            self._filter_reject_cooldown_sec = 15

        # Late-entry cutoff — refuse new entries this close to settlement.
        # Set to 30s: entries are valid up to second 870 (14:30), which is
        # the same moment the forced time-based exit fires.
        try:
            self._late_entry_cutoff_sec = max(
                0, int(float(os.getenv("LATE_ENTRY_CUTOFF_SEC", "30")))
            )
        except (TypeError, ValueError):
            self._late_entry_cutoff_sec = 30

        # ── Per-market constraints ───────────────────────────────────────
        # LOCK_MARKET_DIRECTION blocks the "win then fade" anti-pattern:
        # after a winning LONG the fusion frequently flips BEARISH (mean-
        # reversion / divergence signals) and the bot opens an opposite
        # SHORT on the same market. In 15-min binary markets that
        # contrarian trade usually loses because the trend continues to
        # settlement, so the net P&L for the market becomes negative.
        self._lock_market_direction = os.getenv(
            "LOCK_MARKET_DIRECTION", "true"
        ).strip().lower() in ("1", "true", "yes", "on")
        try:
            self._max_trades_per_market = max(
                1, int(float(os.getenv("MAX_TRADES_PER_MARKET", "3")))
            )
        except (TypeError, ValueError):
            self._max_trades_per_market = 3
        try:
            self._max_chase_delta = max(
                0.0, float(os.getenv("MAX_CHASE_DELTA", "0.12"))
            )
        except (TypeError, ValueError):
            self._max_chase_delta = 0.12
        # 逆势共识门槛：买入低于 ceiling 的弱方 = 对赌市场隐含 ≥60% 的
        # 共识，要求至少 min_signals 个信号同向才放行（2026-07-26 实测：
        # 该子类 11 笔 2 胜 8 负转负，其中信号数 <4 的 5 笔全部亏损）。
        # 仅作用于 fusion 回退路径；ML 激活后的入场不受限。
        try:
            self._contrarian_price_ceiling = max(
                0.0, min(0.5, float(os.getenv("CONTRARIAN_PRICE_CEILING", "0.40")))
            )
        except (TypeError, ValueError):
            self._contrarian_price_ceiling = 0.40
        try:
            self._contrarian_min_signals = max(
                1, int(float(os.getenv("CONTRARIAN_MIN_SIGNALS", "4")))
            )
        except (TypeError, ValueError):
            self._contrarian_min_signals = 4

        # Per-market state, keyed by market slug.
        #   _market_direction:   slug -> "long"|"short"  (first trade's direction)
        #   _market_trade_count: slug -> int            (entries submitted)
        #   _market_last_entry_held_price: slug -> float
        #       Held-token price (YES for LONG, NO for SHORT) of the most
        #       recent entry — used by the anti-chase guard.
        self._market_direction: Dict[str, str] = {}
        self._market_trade_count: Dict[str, int] = {}
        self._market_last_entry_held_price: Dict[str, float] = {}

        # ── Live realised-P&L tracking ──────────────────────────────────────
        # 已平仓实盘交易会与模拟交易一起写入 MySQL。
        self.live_trades: List[LiveTrade] = []
        self._live_session_num: int = 0
        # Hard cap (seconds after market end) before we give up waiting for a
        # definitive settlement price and resolve at the last seen bid.
        self._settle_grace_seconds: int = 600  # 10 min
        # Paper trades resolve faster: Chainlink (if configured) usually settles
        # within ~30s of market end, so we only briefly wait before falling
        # back to Coinbase spot. Shorter still in test mode (1-min markets).

        self._tick_buffer: deque = deque(maxlen=500)
        self._yes_token_id: Optional[str] = None
        self._no_token_id: Optional[str] = None

        # ── Signal processors ─────────────────────────────────────────────────
        try:
            _spike_threshold = max(0.01, float(os.getenv("SPIKE_THRESHOLD", "0.08")))
        except (TypeError, ValueError):
            _spike_threshold = 0.08
        self.spike_detector = SpikeDetectionProcessor(
            spike_threshold=_spike_threshold,
            lookback_periods=20,
        )
        self.sentiment_processor = SentimentProcessor(
            extreme_fear_threshold=25,
            extreme_greed_threshold=75,
        )
        try:
            _divergence_threshold = max(0.01, float(os.getenv("DIVERGENCE_THRESHOLD", "0.06")))
        except (TypeError, ValueError):
            _divergence_threshold = 0.06
        self.divergence_processor = PriceDivergenceProcessor(
            divergence_threshold=_divergence_threshold,
        )
        self.orderbook_processor = OrderBookImbalanceProcessor(
            imbalance_threshold=0.30,
            min_book_volume=50.0,
        )
        # 阈值为绝对概率差（processor 已从相对变化改为绝对差口径）。
        self.tick_velocity_processor = TickVelocityProcessor(
            velocity_threshold_60s=0.008,
            velocity_threshold_30s=0.005,
        )
        self.deribit_pcr_processor = DeribitPCRProcessor(
            bullish_pcr_threshold=1.20,
            bearish_pcr_threshold=0.70,
            max_days_to_expiry=2,
            cache_seconds=300,
        )
        self.liquidation_processor = LiquidationProcessor(
            window_seconds=60,
            min_usd_threshold=500_000,
            imbalance_threshold=0.65,
        )
        self.funding_oi_processor = FundingRateOIProcessor(
            bullish_funding_threshold=-0.0003,
            bearish_funding_threshold=0.0005,
            oi_change_threshold=0.02,
            cache_seconds=300,
        )
        self.cvd_ob_processor = CVDOrderBookProcessor(
            cvd_window_seconds=900,
            cvd_threshold_usd=5_000_000,
            ob_imbalance_threshold=0.30,
        )
        self.ohlcv_momentum_processor = OHLCVMomentumProcessor(
            rsi_overbought=68.0,
            rsi_oversold=32.0,
            cache_seconds=120,
        )

        # ── Signal fusion weights (must sum to 1.0 for active processors) ───
        self.fusion_engine = get_fusion_engine()
        self.fusion_engine.set_weight("OrderBookImbalance", 0.30)
        self.fusion_engine.set_weight("TickVelocity",       0.25)
        self.fusion_engine.set_weight("PriceDivergence",    0.18)
        self.fusion_engine.set_weight("SpikeDetection",     0.12)
        self.fusion_engine.set_weight("DeribitPCR",         0.10)
        self.fusion_engine.set_weight("SentimentAnalysis",  0.05)
        # Processors still run for ML features / Grafana but do not vote in fusion
        self.fusion_engine.set_weight("OHLCVMomentum",      0.0)
        self.fusion_engine.set_weight("CVDOrderBook",       0.0)
        self.fusion_engine.set_weight("Liquidations",       0.0)
        self.fusion_engine.set_weight("FundingRateOI",      0.0)

        # ── Supporting systems ────────────────────────────────────────────────
        self.risk_engine = get_risk_engine()
        # Real-wallet balance sync state: last pUSD reading pushed into the
        # risk engine, and the monotonic deadline for the next portfolio check.
        self._risk_balance_last_synced: Optional[Decimal] = None
        self._risk_balance_next_check: float = 0.0
        self.performance_tracker = get_performance_tracker()
        self.learning_engine = get_learning_engine()
        self.trade_history_repository = TradeHistoryRepository()
        self.order_lifecycle_repository = OrderLifecycleRepository()
        self._order_stats_by_mode: Dict[str, Dict[str, dict]] = {}
        self._latest_prediction_edge: Dict[str, Any] = {
            "available": False,
            "reason": "尚无通过风控的交易信号",
        }
        for order_mode in ("paper", "live"):
            self._refresh_order_stats(order_mode)
        self.ml_engine = get_ml_engine()
        self.settlement_tracker = get_settlement_tracker()
        # Records every decision cycle (all processor signals + fused result +
        # ML p_up) and resolves the real BTC outcome later, enabling an
        # offline fused-signal backtest (see backtest/replay_recordings.py).
        self.signal_recorder = get_signal_recorder(
            price_fn=self.settlement_tracker.get_current_btc_price
        )

        # Override the ML engine's min_edge with the env-configured value.
        # Once ML is active (>= min_samples outcomes recorded), the bot
        # only fires when |p_up - poly_price| >= MIN_ML_EDGE. Default 0.10
        # means "bet only when our prediction is at least 10 percentage
        # points away from Polymarket's implied price".
        try:
            ml_edge_env = float(os.getenv("MIN_ML_EDGE", "0.10"))
            if ml_edge_env >= 0:
                self.ml_engine.min_edge = ml_edge_env
        except (TypeError, ValueError):
            pass
        logger.info(
            f"ML edge gate: bet only when |p_up - poly_price| >= "
            f"{self.ml_engine.min_edge:.0%} (active once "
            f"ML has {self.ml_engine.min_samples} samples; currently "
            f"{self.ml_engine._sample_count})"
        )

        self.grafana_exporter = get_grafana_exporter() if enable_grafana else None

        # ── Price history ─────────────────────────────────────────────────────
        self.price_history: list = []
        self.max_history = 100
        self.paper_trades: List[PaperTrade] = []
        self._load_trade_history()
        # Open paper positions live in ``_open_positions`` with is_paper=True so
        # exit timing, sizing, and settlement mirror the live path.

        self.test_mode = test_mode
        self._last_learning_optimization = datetime.now(timezone.utc)
        self._learning_interval_hours: float = (5 / 60) if test_mode else (7 * 24)

        if test_mode:
            logger.info("=" * 80)
            logger.info("  TEST MODE ACTIVE - Trading every minute!")
            logger.info("=" * 80)

        logger.info("=" * 80)
        logger.info("INTEGRATED BTC STRATEGY INITIALIZED")
        logger.info("  All signal processors ready")
        logger.info("  Risk engine ready")
        logger.info("  ML engine ready")
        logger.info("  $1 per trade maximum")
        logger.info("=" * 80)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _refresh_order_stats(self, mode: str) -> None:
        """订单事件后刷新小型聚合缓存，页面轮询不直接查询数据库。"""
        try:
            self._order_stats_by_mode[mode] = {
                "entry": self.order_lifecycle_repository.stats(mode, "entry"),
                "exit": self.order_lifecycle_repository.stats(mode, "exit"),
            }
        except Exception as exc:
            logger.warning(f"无法刷新 {mode} 订单统计: {exc}")
            self._order_stats_by_mode.setdefault(mode, {"entry": {}, "exit": {}})

    def _record_order_submitted(
        self,
        *,
        mode: str,
        order_id: str,
        role: str,
        market_slug: str,
        direction: str,
        requested_usd: float = 0.0,
        requested_qty: float = 0.0,
    ) -> None:
        try:
            self.order_lifecycle_repository.record_submitted(
                mode=mode,
                order_id=order_id,
                role=role,
                market_slug=market_slug,
                direction=direction,
                requested_usd=requested_usd,
                requested_qty=requested_qty,
            )
            self._refresh_order_stats(mode)
        except Exception as exc:
            logger.warning(f"无法记录订单提交 {order_id}: {exc}")

    def _record_order_fill(
        self,
        *,
        mode: str,
        order_id: str,
        role: str,
        market_slug: str,
        direction: str,
        filled_qty: float,
        filled_notional_usd: float,
    ) -> None:
        try:
            self.order_lifecycle_repository.record_fill(
                mode=mode,
                order_id=order_id,
                role=role,
                market_slug=market_slug,
                direction=direction,
                filled_qty=filled_qty,
                filled_notional_usd=filled_notional_usd,
            )
            self._refresh_order_stats(mode)
        except Exception as exc:
            logger.warning(f"无法记录订单成交 {order_id}: {exc}")

    def _record_order_terminal(
        self,
        *,
        mode: str,
        order_id: str,
        role: str,
        status: str,
        reason: str = "",
    ) -> None:
        try:
            self.order_lifecycle_repository.record_terminal(
                mode=mode,
                order_id=order_id,
                role=role,
                status=status,
                reason=reason,
            )
            self._refresh_order_stats(mode)
        except Exception as exc:
            logger.warning(f"无法记录订单终态 {order_id}: {exc}")

    def _load_trade_history(self) -> None:
        """从 MySQL 恢复交易，用于累计统计和重启后复盘。

        历史 PENDING 记录只作为审计记录恢复，不重新创建活动仓位；活动仓位
        需要交易所订单状态和实时订阅，盲目恢复会造成重复退出订单。
        """
        loaders = (
            ("paper", PaperTrade, "paper_trades"),
            ("live", LiveTrade, "live_trades"),
        )
        for trade_type, model, attr in loaders:
            try:
                raw = self.trade_history_repository.load(trade_type)
                restored = []
                for item in raw:
                    try:
                        restored.append(model.from_dict(item))
                    except Exception as exc:
                        logger.warning(f"跳过损坏的 {trade_type} 历史交易: {exc}")
                setattr(self, attr, restored)
                logger.info(f"已从 MySQL 恢复 {len(restored)} 条 {trade_type} 交易记录")
            except Exception as exc:
                logger.error(f"无法从 MySQL 恢复 {trade_type} 交易: {exc}")
                raise
        self._live_session_num = max(
            (trade.session_trade_num for trade in self.live_trades),
            default=0,
        )

    def _seconds_to_next_15min_boundary(self) -> float:
        now_ts = datetime.now(timezone.utc).timestamp()
        next_boundary = (math.floor(now_ts / MARKET_INTERVAL_SECONDS) + 1) * MARKET_INTERVAL_SECONDS
        return next_boundary - now_ts

    def _is_quote_valid(self, bid, ask) -> bool:
        if bid is None or ask is None:
            return False
        try:
            b, a = float(bid), float(ask)
        except (TypeError, ValueError):
            return False
        return QUOTE_MIN_SPREAD <= b < 1.0 and QUOTE_MIN_SPREAD <= a < 1.0

    def _reset_stability(self, reason: str = "") -> None:
        if self._market_stable:
            logger.warning(f"Market stability RESET{' – ' + reason if reason else ''}")
        self._market_stable = False
        self._stable_tick_count = 0

    def _simulated_entry_price(
        self,
        direction: str,
        bid: Decimal,
        ask: Decimal,
        poly_mid: Decimal,
    ) -> Decimal:
        """Mirror live market BUY: LONG pays YES ask; SHORT pays NO ask ≈ 1 − YES bid."""
        if direction == "long":
            px = ask if ask > 0 else poly_mid
        else:
            px = Decimal("1") - bid if bid > 0 else (Decimal("1") - poly_mid)
        return max(Decimal("0.01"), min(Decimal("0.99"), px))

    def _compute_exit_levels(
        self,
        fill_price: Decimal,
        *,
        stop_loss_enabled: Optional[bool] = None,
        stop_loss_frac: Optional[float] = None,
        take_profit_frac: Optional[float] = None,
    ) -> tuple:
        """Payoff-relative TP/SL levels shared by live fills and paper simulation."""
        sl_enabled = (
            self._stop_loss_enabled if stop_loss_enabled is None else stop_loss_enabled
        )
        if (
            self._sl_tp_endpoints
            and stop_loss_frac is None
            and take_profit_frac is None
        ):
            # 低价入场宽 SL/耐心 TP，高价入场紧 SL/快 TP
            sl_v, tp_v = interp_exit_fracs(
                float(fill_price),
                self._min_entry_price,
                self._max_entry_price,
                *self._sl_tp_endpoints,
            )
            sl_frac, tp_frac = Decimal(str(sl_v)), Decimal(str(tp_v))
        else:
            sl_frac = Decimal(str(
                self._stop_loss_frac if stop_loss_frac is None else stop_loss_frac
            ))
            tp_frac = Decimal(str(
                self._take_profit_frac if take_profit_frac is None else take_profit_frac
            ))

        remaining_upside = max(Decimal("0"), Decimal("1") - fill_price)
        take_profit = min(Decimal("0.99"), fill_price + tp_frac * remaining_upside)
        if take_profit - fill_price < Decimal("0.01"):
            take_profit = min(Decimal("0.99"), fill_price + Decimal("0.01"))

        if sl_enabled:
            stop_loss = max(Decimal("0.01"), fill_price - sl_frac * fill_price)
            if fill_price - stop_loss < Decimal("0.01"):
                stop_loss = max(Decimal("0.01"), fill_price - Decimal("0.01"))
        else:
            stop_loss = Decimal("0")

        return stop_loss, take_profit, sl_enabled

    def _held_trade_instrument(self, direction: str):
        """Instrument id for the token the bot buys (YES for long, NO for short)."""
        if direction == "long":
            return getattr(self, "_yes_instrument_id", None) or self.instrument_id
        return getattr(self, "_no_instrument_id", None)

    # ── Redis ─────────────────────────────────────────────────────────────────

    async def check_simulation_mode(self) -> bool:
        if not self.redis_client:
            return self.current_simulation_mode
        try:
            sim_mode = self.redis_client.get("btc_trading:simulation_mode")
            if sim_mode is not None:
                redis_simulation = sim_mode == "1"
                if redis_simulation != self.current_simulation_mode:
                    self.current_simulation_mode = redis_simulation
                    mode_text = "SIMULATION" if redis_simulation else "LIVE TRADING"
                    logger.warning(f"Trading mode changed to: {mode_text}")
                    if not redis_simulation:
                        logger.warning("LIVE TRADING ACTIVE - Real money at risk!")
                return redis_simulation
        except Exception as e:
            logger.warning(f"Failed to check Redis simulation mode: {e}")
        return self.current_simulation_mode

    # ── Strategy lifecycle ────────────────────────────────────────────────────

    @staticmethod
    def _dashboard_book_levels(levels: list, *, reverse: bool) -> list:
        """规范化并排序盘口档位，最多保留五档真实数据。"""
        rows = []
        for level in levels or []:
            try:
                price = float(level.get("price", 0.0))
                size = float(level.get("size", 0.0))
            except (AttributeError, TypeError, ValueError):
                continue
            if price <= 0 or size <= 0:
                continue
            rows.append({
                "price": price,
                "size": size,
                "notional": price * size,
            })
        rows.sort(key=lambda item: item["price"], reverse=reverse)
        return rows[:5]

    def _fetch_dashboard_market_data(self, client: httpx.Client, *, include_candles: bool) -> bool:
        """抓取驾驶舱使用的公开行情；失败时保留最后一次成功快照。

        返回本轮是否有抓取错误（供调用方统计连败并重建连接池）。"""
        now_iso = datetime.now(timezone.utc).isoformat()
        updates: Dict[str, Any] = {"updated_at": now_iso, "last_error": ""}
        errors: List[str] = []

        if include_candles:
            try:
                response = client.get(
                    "https://api.binance.com/api/v3/klines",
                    params={"symbol": "BTCUSDT", "interval": "1m", "limit": 100},
                )
                response.raise_for_status()
                candles = []
                for row in response.json():
                    if not isinstance(row, list) or len(row) < 6:
                        continue
                    candles.append({
                        "time": int(row[0] // 1000),
                        "open": float(row[1]),
                        "high": float(row[2]),
                        "low": float(row[3]),
                        "close": float(row[4]),
                        "volume": float(row[5]),
                    })
                updates.update({
                    "candles": candles,
                    "candles_updated_at": now_iso,
                })
            except Exception as exc:
                errors.append(f"BTC K线: {exc!r}")
            try:
                eth_response = client.get(
                    "https://api.binance.com/api/v3/ticker/price",
                    params={"symbol": "ETHUSDT"},
                )
                eth_response.raise_for_status()
                updates["eth_price"] = float(eth_response.json()["price"])
            except Exception as exc:
                errors.append(f"ETH行情: {exc!r}")

        yes_token = str(getattr(self, "_yes_token_id", None) or "")
        no_token = str(getattr(self, "_no_token_id", None) or "")
        market_slug = ""
        if 0 <= self.current_instrument_index < len(self.all_btc_instruments):
            market_slug = str(
                self.all_btc_instruments[self.current_instrument_index].get("slug", "")
            )

        books: Dict[str, Any] = {}
        for outcome, token_id in (("up", yes_token), ("down", no_token)):
            if not token_id:
                continue
            try:
                book_response = client.get(
                    "https://clob.polymarket.com/book",
                    params={"token_id": token_id},
                )
                book_response.raise_for_status()
                raw_book = book_response.json()
                books[outcome] = {
                    "token_id": token_id,
                    "bids": self._dashboard_book_levels(raw_book.get("bids", []), reverse=True),
                    "asks": self._dashboard_book_levels(raw_book.get("asks", []), reverse=False),
                }
            except Exception as exc:
                errors.append(f"{outcome.upper()}盘口: {exc!r}")
        if books:
            updates.update({
                "orderbook": {
                    "market_slug": market_slug,
                    "updated_at": now_iso,
                    **books,
                },
                "orderbook_updated_at": now_iso,
            })
        updates["last_error"] = "; ".join(errors)

        with self._dashboard_lock:
            self._dashboard_market_data.update(updates)
        return bool(errors)

    def _dashboard_data_loop(self) -> None:
        """后台轮询真实 K 线和 Polymarket 盘口。

        连续失败时重建 httpx.Client：断网会把长连接池整体毁掉（TLS 会话
        失效），网络恢复后复用旧池只会继续失败——2026-07-26 断网 6.5h 后
        盘口冻结 15h（页面价与持仓 last_bid 矛盾）的根因。"""
        proxy_url = (os.getenv("POLYMARKET_PROXY_URL") or "").strip() or None
        client_kwargs: Dict[str, Any] = {"timeout": 6.0}
        if proxy_url:
            client_kwargs["proxy"] = proxy_url
        next_candles_at = 0.0
        fail_streak = 0
        client = httpx.Client(**client_kwargs)
        try:
            while not self._dashboard_stop_event.is_set():
                now = time.monotonic()
                try:
                    include_candles = now >= next_candles_at
                    had_error = self._fetch_dashboard_market_data(
                        client, include_candles=include_candles
                    )
                    if include_candles:
                        next_candles_at = now + 15.0
                    self._publish_dashboard_state()
                    fail_streak = fail_streak + 1 if had_error else 0
                except Exception as exc:
                    fail_streak += 1
                    with self._dashboard_lock:
                        self._dashboard_market_data["last_error"] = repr(exc)
                    logger.debug(f"驾驶舱行情刷新失败: {exc!r}")
                if fail_streak >= 5:
                    logger.warning(
                        f"驾驶舱行情连续失败 {fail_streak} 轮，重建 HTTP 连接池"
                    )
                    try:
                        client.close()
                    except Exception:
                        pass
                    client = httpx.Client(**client_kwargs)
                    fail_streak = 0
                self._dashboard_stop_event.wait(2.0)
        finally:
            try:
                client.close()
            except Exception:
                pass

    def _publish_dashboard_state(self) -> None:
        if self.grafana_exporter and hasattr(self.grafana_exporter, "update_live_state"):
            try:
                self.grafana_exporter.update_live_state(self.get_dashboard_snapshot())
            except Exception:
                pass

    def _set_decision_stage(self, stage: str, title: str, *, status: str = "active") -> None:
        """发布真实策略阶段，供流程图驱动节点与连线。"""
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._dashboard_lock:
            stages = [dict(item) for item in self._decision_state.get("stages", [])]
            if stages and stages[-1].get("stage") == stage and stages[-1].get("status") == "active":
                stages[-1].update({
                    "title": title,
                    "status": status,
                    "timestamp": now_iso,
                })
            else:
                for item in stages:
                    if item.get("status") == "active":
                        item["status"] = "completed"
                stages.append({
                    "stage": stage,
                    "title": title,
                    "status": status,
                    "timestamp": now_iso,
                })
            self._decision_state.update({
                "status": "running",
                "current_stage": stage,
                "updated_at": now_iso,
                "stages": stages[-20:],
            })
        if self.grafana_exporter and hasattr(self.grafana_exporter, "record_dashboard_event"):
            self.grafana_exporter.record_dashboard_event(
                "decision_stage",
                title,
                cycle_id=self._current_cycle_id,
                stage=stage,
                status=status,
                is_simulation=self.current_simulation_mode,
            )
        self._publish_dashboard_state()

    def on_start(self) -> None:
        self._stopping = False
        logger.info("=" * 80)
        logger.info("INTEGRATED BTC STRATEGY STARTED")
        logger.info("=" * 80)

        # First attempt at the instrument cache. The Polymarket adapter
        # populates the cache asynchronously, so an empty result here is
        # expected on a fresh start; the timer loop will keep retrying.
        if not self._load_all_btc_instruments():
            logger.warning(
                "Instrument cache empty at startup — timer loop will retry "
                "until BTC 15-min markets become available."
            )

        if self.instrument_id:
            try:
                quote = self.cache.quote_tick(self.instrument_id)
                if quote and quote.bid_price and quote.ask_price:
                    current_price = (quote.bid_price + quote.ask_price) / 2
                    self.price_history.append(current_price)
                    logger.info(f"Initial price: ${float(current_price):.4f}")
            except Exception as e:
                logger.debug(f"No initial price yet: {e}")

        if len(self.price_history) < 20:
            self._generate_synthetic_history(target_count=20, existing_count=len(self.price_history))

        self.run_in_executor(self._start_timer_loop)

        if self.grafana_exporter:
            self._grafana_thread = threading.Thread(
                target=self._start_grafana_sync,
                daemon=True,
                name="grafana-exporter-loop",
            )
            self._grafana_thread.start()
            self._dashboard_stop_event.clear()
            self._dashboard_thread = threading.Thread(
                target=self._dashboard_data_loop,
                daemon=True,
                name="dashboard-market-data",
            )
            self._dashboard_thread.start()

        self.liquidation_processor.start_stream()
        self.cvd_ob_processor.start_stream()
        self.settlement_tracker.start_tracking()
        self.signal_recorder.start()
        logger.info("Liquidation stream started")
        logger.info("CVD aggTrade stream started")
        logger.info("Settlement tracker started")
        logger.info("Signal recorder started")
        logger.info(
            f"ML engine active: {self.ml_engine.is_active} "
            f"(samples={self.ml_engine._sample_count}/{self.ml_engine.min_samples})"
        )

        logger.info("=" * 80)
        if self.test_mode:
            logger.info("Strategy active — TEST MODE: full 15-min market is tradeable")
        else:
            logger.info(
                f"Strategy active — trade window {self._trade_window_start}-"
                f"{self._trade_window_end}s into each 15-min market "
                f"(minutes {self._trade_window_start/60:.1f}-{self._trade_window_end/60:.1f})"
            )
        logger.info(f"Price history: {len(self.price_history)} points")
        if len(self.price_history) >= 20:
            logger.info("READY TO TRADE")
        else:
            logger.warning(f"Need more history ({len(self.price_history)}/20)")
        logger.info("=" * 80)

    def _generate_synthetic_history(self, target_count: int = 20, existing_count: int = 0) -> None:
        base_price = self.price_history[-1] if self.price_history else Decimal("0.5")
        needed = target_count - existing_count
        if needed <= 0:
            return
        for _ in range(needed):
            change = Decimal(str(random.uniform(-0.03, 0.03)))
            new_price = max(Decimal("0.01"), min(Decimal("0.99"), base_price * (Decimal("1") + change)))
            self.price_history.append(new_price)
            base_price = new_price

    # ── Instrument loading ────────────────────────────────────────────────────

    def _load_all_btc_instruments(self, *, quiet: bool = False) -> bool:
        """
        Scan the Nautilus instrument cache for BTC 15-min markets and bind the
        active one. Returns True if any markets were found, False otherwise.

        Nautilus populates the instrument cache asynchronously, so this must be
        callable repeatedly until it succeeds. ``quiet=True`` suppresses the
        per-attempt log lines used by the retry loop.
        """
        instruments = list(self.cache.instruments())

        # Fallback: if the strategy cache is empty but the data client's
        # instrument provider has already loaded instruments, push them into
        # the cache so the rest of this method can find them. This guards
        # against the race where the data client's `_connect()` populated its
        # own provider but the data engine hasn't forwarded the items yet
        # (the Polymarket adapter's `_send_all_instruments_to_data_engine` is
        # only called once, on connect).
        if not instruments:
            provider_total, provider_added = self._refresh_cache_from_providers()
            if not quiet and provider_total:
                logger.info(
                    f"Cache empty but providers hold {provider_total} instruments; "
                    f"added {provider_added} to strategy cache."
                )
            if provider_added:
                instruments = list(self.cache.instruments())

        if not quiet:
            logger.info(f"Loading BTC instruments from {len(instruments)} total...")

        now = datetime.now(timezone.utc)
        current_timestamp = int(now.timestamp())

        btc_instruments = []

        for instrument in instruments:
            try:
                info = getattr(instrument, "info", None) or {}
                if not info:
                    continue
                question = (info.get("question") or "").lower()
                slug = (info.get("market_slug") or "").lower()

                if not (("btc" in question or "btc" in slug) and "15m" in slug):
                    continue

                try:
                    market_timestamp = int(slug.split("-")[-1])
                except (ValueError, IndexError):
                    continue

                end_timestamp = market_timestamp + 900
                if end_timestamp <= current_timestamp:
                    continue

                raw_id = str(instrument.id)
                without_suffix = raw_id.split(".")[0] if "." in raw_id else raw_id
                token_id = (
                    without_suffix.split("-")[-1]
                    if "-" in without_suffix
                    else without_suffix
                )

                # Polymarket binary markets expose two CLOB tokens per slug,
                # one per outcome. Trust info["outcome"] when present; fall
                # back to insertion order only as a last resort.
                outcome = (info.get("outcome") or "").strip().lower()

                btc_instruments.append({
                    "instrument": instrument,
                    "slug": slug,
                    "start_time": datetime.fromtimestamp(market_timestamp, tz=timezone.utc),
                    "end_time": datetime.fromtimestamp(end_timestamp, tz=timezone.utc),
                    "market_timestamp": market_timestamp,
                    "end_timestamp": end_timestamp,
                    "time_diff_minutes": (market_timestamp - current_timestamp) / 60,
                    "token_id": token_id,
                    "outcome": outcome,
                })
            except Exception:
                continue

        if not btc_instruments:
            if not quiet:
                logger.warning("No BTC 15-min instruments in cache yet")
            return False

        # Group both tokens (YES/NO) under their shared slug.
        grouped: Dict[str, dict] = {}
        for inst in btc_instruments:
            slug = inst["slug"]
            entry = grouped.get(slug)
            if entry is None:
                entry = {
                    **inst,
                    "yes_instrument_id": None,
                    "no_instrument_id": None,
                    "yes_token_id": None,
                    "no_token_id": None,
                }
                grouped[slug] = entry

            outcome = inst["outcome"]
            inst_id = inst["instrument"].id
            tok_id = inst["token_id"]

            if outcome == "yes" or outcome == "up":
                entry["yes_instrument_id"] = inst_id
                entry["yes_token_id"] = tok_id
            elif outcome == "no" or outcome == "down":
                entry["no_instrument_id"] = inst_id
                entry["no_token_id"] = tok_id
            else:
                # Outcome metadata missing — fall back to first/second seen.
                if entry["yes_instrument_id"] is None:
                    entry["yes_instrument_id"] = inst_id
                    entry["yes_token_id"] = tok_id
                else:
                    entry["no_instrument_id"] = inst_id
                    entry["no_token_id"] = tok_id

        btc_instruments = sorted(grouped.values(), key=lambda x: x["market_timestamp"])

        logger.info("=" * 80)
        logger.info(f"FOUND {len(btc_instruments)} BTC 15-MIN MARKETS:")
        for i, inst in enumerate(btc_instruments):
            is_active = inst["time_diff_minutes"] <= 0 and inst["end_timestamp"] > current_timestamp
            status = "ACTIVE" if is_active else ("FUTURE" if inst["time_diff_minutes"] > 0 else "PAST")
            yes_marker = "Y" if inst.get("yes_instrument_id") else "-"
            no_marker = "N" if inst.get("no_instrument_id") else "-"
            logger.info(
                f"  [{i}] {inst['slug']}: {status} [{yes_marker}{no_marker}] "
                f"(starts {inst['start_time'].strftime('%H:%M:%S')}, "
                f"ends {inst['end_time'].strftime('%H:%M:%S')})"
            )
        logger.info("=" * 80)

        self.all_btc_instruments = btc_instruments

        # Pick the market to bind. Critically, DON'T bind a market that is
        # already too far into its cycle to trade — that just burns a full
        # 15-min wait with "no quotes yet" before the bot can do anything
        # useful. If the live market has no usable trade window left, bind the
        # next FUTURE market in waiting mode so we're subscribed and ready the
        # moment it opens (and actually catch its trade window).
        active_idx = None
        for i, inst in enumerate(btc_instruments):
            if inst["time_diff_minutes"] <= 0 and inst["end_timestamp"] > current_timestamp:
                active_idx = i
                break

        future_markets = [inst for inst in btc_instruments if inst["time_diff_minutes"] > 0]
        nearest_future_idx = (
            btc_instruments.index(min(future_markets, key=lambda x: x["time_diff_minutes"]))
            if future_markets
            else None
        )

        if active_idx is not None and self._market_has_usable_window(
            btc_instruments[active_idx], current_timestamp
        ):
            self._bind_market(active_idx, waiting=False)
        elif nearest_future_idx is not None:
            if active_idx is not None:
                logger.info(
                    f"Active market {btc_instruments[active_idx]['slug']} has no usable "
                    f"trade window left — binding next market and waiting for open."
                )
            self._bind_market(nearest_future_idx, waiting=True)
        elif active_idx is not None:
            # No future market available yet; fall back to the live one.
            self._bind_market(active_idx, waiting=False)
        else:
            self._bind_market(len(btc_instruments) - 1, waiting=True)

        self._instruments_loaded = True
        return True

    def _market_has_usable_window(self, market: dict, now_ts: float) -> bool:
        """True if ``market`` still has enough of its trade window left to act.

        Guards against binding to an about-to-settle market (the root cause of
        repeated "no quotes yet / window CLOSED" startups). Requires that the
        current time is before the end of the trade window with a small lead,
        and not already inside the late-entry cutoff before settlement.
        """
        try:
            start_ts = int(market.get("market_timestamp") or 0)
            end_ts = int(market.get("end_timestamp") or 0)
        except (TypeError, ValueError):
            return False
        if not start_ts or not end_ts:
            return False

        if self.test_mode:
            # Whole market is tradeable in test mode; just need some time left.
            return (end_ts - now_ts) > self._late_entry_cutoff_sec

        elapsed = now_ts - start_ts
        # Need at least a small lead (30s) of trade window remaining...
        window_time_left = self._trade_window_end - elapsed
        # ...and we must not already be inside the late-entry cutoff.
        secs_to_settle = end_ts - now_ts
        return window_time_left >= 30 and secs_to_settle > self._late_entry_cutoff_sec

    def _refresh_cache_from_providers(self) -> tuple[int, int]:
        """
        Pull instruments from any data/exec client's instrument provider and
        push them into the strategy ``cache``.

        Returns ``(total_in_providers, newly_added_to_cache)``.

        Nautilus's Polymarket adapter calls
        ``_send_all_instruments_to_data_engine`` once at connect time. If that
        runs before the data engine is fully wired, the cache stays empty even
        though the provider holds the instruments. This helper closes that
        race by re-publishing the provider's contents on demand.

        Falls back to a direct Gamma API fetch if no provider returned
        instruments — guards against the Nautilus data/exec client failing to
        invoke ``provider.initialize()`` for any reason.
        """
        total = 0
        added = 0
        try:
            trader = getattr(self, "trader", None)
            data_engine = getattr(trader, "data_engine", None) if trader else None
            exec_engine = getattr(trader, "exec_engine", None) if trader else None

            providers = []
            for engine in (data_engine, exec_engine):
                if engine is None:
                    continue
                # Nautilus engines expose clients via the private `_clients`
                # mapping (Cython attribute). Fall back to iterating any
                # public ``registered_clients`` collection if available.
                client_objs = []
                clients_attr = getattr(engine, "_clients", None)
                if isinstance(clients_attr, dict):
                    client_objs = list(clients_attr.values())
                elif hasattr(engine, "registered_clients"):
                    try:
                        reg = engine.registered_clients
                        if isinstance(reg, dict):
                            client_objs = list(reg.values())
                        else:
                            for cid in reg:
                                client_objs.append(
                                    clients_attr.get(cid) if isinstance(clients_attr, dict) else None
                                )
                    except Exception:
                        client_objs = []

                for client in client_objs:
                    if client is None:
                        continue
                    prov = getattr(client, "_instrument_provider", None) or getattr(
                        client, "instrument_provider", None
                    )
                    if prov is not None and prov not in providers:
                        providers.append(prov)

            for prov in providers:
                try:
                    items = prov.get_all().values()
                except Exception:
                    continue
                for inst in items:
                    total += 1
                    if self.cache.instrument(inst.id) is None:
                        try:
                            self.cache.add_instrument(inst)
                            added += 1
                        except Exception as exc:
                            logger.debug(
                                f"Could not add {inst.id} to cache: {exc}"
                            )
        except Exception as exc:
            logger.debug(f"_refresh_cache_from_providers failed: {exc}")

        # Bulletproof fallback: if no provider yielded instruments, fetch
        # them directly via Gamma API and register with the cache. This
        # bypasses Nautilus's adapter entirely in case ``_connect()`` never
        # triggered the provider's ``initialize()``.
        if added == 0 and total == 0:
            direct_added = self._load_instruments_via_gamma_direct()
            added += direct_added
            total += direct_added

        return total, added

    def _load_instruments_via_gamma_direct(self) -> int:
        """
        Fetch BTC 15-min markets straight from Gamma API and parse them into
        Nautilus ``BinaryOption`` instruments, registering each with the
        strategy cache.

        This sidesteps any plumbing issues in the Polymarket adapter's
        instrument-provider initialisation path and runs in the strategy's
        own timer-loop thread.

        Returns the number of instruments newly added to the cache.
        """
        try:
            import httpx
            from nautilus_trader.adapters.polymarket.common.gamma_markets import (
                normalize_gamma_market_to_clob_format,
            )
            from nautilus_trader.adapters.polymarket.common.parsing import (
                parse_polymarket_instrument,
            )

            base_url = os.getenv(
                "GAMMA_API_URL", "https://gamma-api.polymarket.com"
            ).rstrip("/")

            now = datetime.now(timezone.utc)
            unix_interval_start = (int(now.timestamp()) // 900) * 900
            slugs = [
                f"btc-updown-15m-{unix_interval_start + (i * 900)}"
                for i in range(-1, 97)
            ]

            chunk_size = 50
            markets: List[dict] = []
            seen: set[str] = set()

            with httpx.Client(timeout=60.0) as client:
                for start in range(0, len(slugs), chunk_size):
                    chunk = slugs[start : start + chunk_size]
                    params = {
                        "active": "true",
                        "closed": "false",
                        "archived": "false",
                        "slug": chunk,
                        "limit": 100,
                    }
                    resp = client.get(f"{base_url}/markets", params=params)
                    if resp.status_code != 200:
                        logger.warning(
                            f"Direct Gamma fetch failed: HTTP {resp.status_code} "
                            f"{resp.text[:200]}"
                        )
                        continue
                    rows = resp.json()
                    if isinstance(rows, dict):
                        rows = rows.get("data") or []
                    for market in rows or []:
                        cid = market.get("conditionId")
                        if cid and cid in seen:
                            continue
                        if cid:
                            seen.add(cid)
                        markets.append(market)

            if not markets:
                logger.warning("Direct Gamma fetch returned zero markets")
                return 0

            ts_init = self.clock.timestamp_ns()
            added = 0
            errors = 0

            for market in markets:
                try:
                    normalized = normalize_gamma_market_to_clob_format(market)
                    for token_info in normalized.get("tokens") or []:
                        token_id = token_info.get("token_id")
                        if not token_id:
                            continue
                        outcome = token_info.get("outcome") or ""

                        token_market_info = dict(normalized)
                        token_market_info["outcome"] = outcome
                        token_market_info["token_id"] = token_id

                        instrument = parse_polymarket_instrument(
                            market_info=token_market_info,
                            token_id=token_id,
                            outcome=outcome,
                            ts_init=ts_init,
                        )
                        if self.cache.instrument(instrument.id) is None:
                            self.cache.add_instrument(instrument)
                            added += 1
                except Exception as exc:
                    errors += 1
                    logger.debug(
                        f"Direct-load skip for {market.get('slug', '?')}: {exc}"
                    )

            logger.info(
                f"Direct Gamma load: {len(markets)} markets parsed, "
                f"{added} instruments added to cache, {errors} errors."
            )
            return added

        except Exception as exc:
            logger.error(f"Direct Gamma load failed: {exc}")
            return 0

    def _bind_market(self, index: int, *, waiting: bool) -> None:
        """Bind the strategy to a market entry by index in ``all_btc_instruments``."""
        if not (0 <= index < len(self.all_btc_instruments)):
            return

        market = self.all_btc_instruments[index]
        self.current_instrument_index = index
        # Default subscription/instrument target is the YES token when present.
        self.instrument_id = market.get("yes_instrument_id") or market["instrument"].id
        self._yes_instrument_id = market.get("yes_instrument_id") or market["instrument"].id
        self._no_instrument_id = market.get("no_instrument_id")
        self._yes_token_id = market.get("yes_token_id") or market.get("token_id")
        self._no_token_id = market.get("no_token_id")

        # 跨市场状态清零：上一个市场结算前收敛到 ~0/~1 的价格若残留在缓冲
        # 里，TickVelocity/Spike 会在新市场 ~0.5 开盘时算出巨幅假动量，每次
        # 换盘吐一发高置信度错向信号（2026-07-25 双审计确认的换盘污染）。
        self._tick_buffer.clear()
        self.price_history.clear()
        try:
            self.divergence_processor.reset_history()
        except Exception:
            pass

        if waiting:
            self.next_switch_time = market["start_time"]
            self._waiting_for_market_open = True
            logger.info(f"NO CURRENT MARKET — waiting for: {market['slug']}")
        else:
            self.next_switch_time = market["end_time"]
            self._waiting_for_market_open = False
            logger.info(f"CURRENT MARKET: {market['slug']} (index {index})")
            logger.info(f"  Next switch at: {self.next_switch_time.strftime('%H:%M:%S')}")

        try:
            self.subscribe_quote_ticks(self.instrument_id)
            logger.info(f"  Subscribed to: {self.instrument_id}")
        except Exception as e:
            logger.warning(f"Failed to subscribe quote ticks for {self.instrument_id}: {e}")

    def _switch_to_next_market(self) -> bool:
        if not self.all_btc_instruments:
            logger.error("No instruments loaded!")
            return False

        next_index = self.current_instrument_index + 1
        if next_index >= len(self.all_btc_instruments):
            logger.warning("No more markets — will restart bot")
            return False

        next_market = self.all_btc_instruments[next_index]
        now = datetime.now(timezone.utc)

        if now < next_market["start_time"]:
            logger.info(f"Waiting for next market at {next_market['start_time'].strftime('%H:%M:%S')}")
            return False

        logger.info("[MARKET] Switched to next 15-min window")

        self._bind_market(next_index, waiting=False)

        self._stable_tick_count = QUOTE_STABILITY_REQUIRED
        self._market_stable = True
        self.last_trade_time = -1
        logger.info("  Trade timer reset — will trade on next tick")
        return True

    # ── Heartbeat / liveness ─────────────────────────────────────────────────

    def _emit_heartbeat(self, now: datetime) -> None:
        """
        Print a single-line status snapshot every ``self._heartbeat_secs``.

        Tells the user at a glance:
          - bot is alive (timer loop is running)
          - which market is bound
          - elapsed time within the current 15-min market
          - seconds until the next trade-window opens (or "OPEN NOW")
          - last observed mid-price and tick rate since previous heartbeat
        """
        now_ts = now.timestamp()
        if now_ts - self._last_heartbeat_ts < self._heartbeat_secs:
            return
        # Skip the very first heartbeat until we have something meaningful.
        first_call = self._last_heartbeat_ts == 0.0
        elapsed_since = now_ts - self._last_heartbeat_ts if not first_call else 0.0
        self._last_heartbeat_ts = now_ts

        if not (0 <= self.current_instrument_index < len(self.all_btc_instruments)):
            logger.info(
                f"[heartbeat] uptime={self._uptime_str()} "
                f"instruments_loaded={self._instruments_loaded} "
                f"waiting_for_market=True"
            )
            return

        market = self.all_btc_instruments[self.current_instrument_index]
        market_start_ts = int(market["market_timestamp"])
        market_end_ts = int(market["end_timestamp"])
        elapsed_in_market = now_ts - market_start_ts

        # Trade window — keep in sync with on_quote_tick.
        if self.test_mode:
            win_start, win_end = 0, 900
        else:
            win_start, win_end = self._trade_window_start, self._trade_window_end

        if elapsed_in_market < 0:
            window_status = (
                f"market opens in {abs(elapsed_in_market):.0f}s "
                f"({market['start_time'].strftime('%H:%M:%S')} UTC)"
            )
        elif elapsed_in_market < win_start:
            window_status = (
                f"TRADE WINDOW opens in {win_start - elapsed_in_market:.0f}s"
            )
        elif elapsed_in_market < win_end:
            window_status = (
                f"TRADE WINDOW OPEN ({win_end - elapsed_in_market:.0f}s left)"
            )
        elif elapsed_in_market < (market_end_ts - market_start_ts):
            window_status = (
                f"window CLOSED — market settles in "
                f"{market_end_ts - now_ts:.0f}s"
            )
        else:
            window_status = "market EXPIRED — switching soon"

        if self._last_bid_ask:
            bid, ask = self._last_bid_ask
            mid = (bid + ask) / 2
            quote_str = (
                f"bid=${float(bid):.4f} ask=${float(ask):.4f} mid=${float(mid):.4f}"
            )
        else:
            quote_str = "no quotes yet"

        if not first_call and elapsed_since > 0:
            rate = self._tick_count_since_last_heartbeat / elapsed_since
            tick_str = f"{self._tick_count_since_last_heartbeat} ticks ({rate:.1f}/s)"
        else:
            tick_str = f"{self._tick_count_since_last_heartbeat} ticks"
        self._tick_count_since_last_heartbeat = 0

        positions_str = (
            f"open={len(self._open_positions)} "
            f"pending={len(self._pending_orders)}"
        )

        logger.info(
            f"[heartbeat] {market['slug']} | "
            f"min {elapsed_in_market/60:.1f}/15 | "
            f"{window_status} | {quote_str} | {tick_str} | {positions_str}"
        )

    def _uptime_str(self) -> str:
        seconds = (datetime.now(timezone.utc) - self.bot_start_time).total_seconds()
        if seconds < 60:
            return f"{seconds:.0f}s"
        if seconds < 3600:
            return f"{seconds/60:.1f}m"
        return f"{seconds/3600:.2f}h"

    @staticmethod
    def _fmt_mmss(seconds: float) -> str:
        """Format a duration as MM:SS for the dashboard countdown."""
        secs = max(0, int(round(seconds)))
        return f"{secs // 60:02d}:{secs % 60:02d}"

    def get_dashboard_snapshot(self) -> dict:
        """Return live state for the terminal UI dashboard."""
        now = datetime.now(timezone.utc)
        now_ts = now.timestamp()
        with self._dashboard_lock:
            market_data = {
                **self._dashboard_market_data,
                "candles": list(self._dashboard_market_data.get("candles", [])),
                "orderbook": dict(self._dashboard_market_data.get("orderbook", {})),
            }
            decision_state = {
                **self._decision_state,
                "stages": [dict(item) for item in self._decision_state.get("stages", [])],
            }

        market_slug = "—"
        next_window = "—"
        market_start_iso = None
        market_end_iso = None
        market_label = "BTC 15M"
        trade_window_open = False
        waiting_for_market = self._waiting_for_market_open
        price_to_beat: Optional[float] = None

        if 0 <= self.current_instrument_index < len(self.all_btc_instruments):
            market = self.all_btc_instruments[self.current_instrument_index]
            market_slug = market.get("slug", "—")
            market_start_ts = int(market["market_timestamp"])
            market_end_ts = int(market["end_timestamp"])
            market_start_iso = datetime.fromtimestamp(market_start_ts, tz=timezone.utc).isoformat()
            market_end_iso = datetime.fromtimestamp(market_end_ts, tz=timezone.utc).isoformat()
            market_label = "BTC 15M"
            elapsed = now_ts - market_start_ts

            if self.test_mode:
                win_start, win_end = 0, 900
            else:
                win_start, win_end = self._trade_window_start, self._trade_window_end

            if elapsed < 0:
                next_window = f"opens in {self._fmt_mmss(abs(elapsed))}"
            elif elapsed < win_start:
                next_window = f"opens in {self._fmt_mmss(win_start - elapsed)}"
            elif elapsed < win_end:
                trade_window_open = True
                next_window = f"OPEN ({self._fmt_mmss(win_end - elapsed)} left)"
            elif elapsed < (market_end_ts - market_start_ts):
                next_window = f"settle in {self._fmt_mmss(market_end_ts - now_ts)}"
            else:
                next_window = "switching soon"

            # 开盘基准 = 市场起始那一分钟的 1m K 线开盘价。不能用本市场
            # 首笔交易的入场现货——交易在第 5 分钟后才入场，拿它当"开盘
            # 价"会让方向显示与盘口矛盾（2026-07-26 实例：显示 +58 涨、
            # 盘口 DOWN 0.77 且买 DOWN 获胜——真基准在更高位）。仅当
            # K 线不可用（行情断流）时才退回交易入场价兜底。
            candles = market_data.get("candles", [])
            if candles:
                nearest = min(
                    candles,
                    key=lambda bar: abs(float(bar.get("time", 0)) - market_start_ts),
                )
                if abs(float(nearest.get("time", 0)) - market_start_ts) <= 90:
                    price_to_beat = float(nearest.get("open", 0.0) or 0.0) or None
            if price_to_beat is None:
                for trade in reversed(self.paper_trades + self.live_trades):
                    if trade.market_slug == market_slug and trade.btc_spot_price:
                        price_to_beat = float(trade.btc_spot_price)
                        break

        candles = market_data.get("candles", [])
        btc_price = (
            float(candles[-1].get("close", 0.0) or 0.0)
            if candles
            else None
        )

        up_price = down_price = None
        yes_bid = yes_ask = None
        if self._last_bid_ask:
            bid, ask = self._last_bid_ask
            yes_bid = float(bid)
            yes_ask = float(ask)

        orderbook = market_data.get("orderbook", {})
        # 陈旧护栏：REST 盘口超过 30s 未更新（抓取线程故障/断网）时整体
        # 弃用，回退 tick 派生价——冻结的盘口价会与持仓 last_bid 互相矛盾，
        # 让持仓收益看起来"算错了"（2026-07-26 实例：盘口冻结 15 小时）。
        ob_ts = market_data.get("orderbook_updated_at")
        orderbook_stale = True
        if ob_ts:
            try:
                orderbook_stale = (
                    datetime.now(timezone.utc) - datetime.fromisoformat(str(ob_ts))
                ).total_seconds() > 30.0
            except Exception:
                orderbook_stale = True
        if orderbook_stale:
            orderbook = {}
        up_book = orderbook.get("up", {}) if isinstance(orderbook, dict) else {}
        down_book = orderbook.get("down", {}) if isinstance(orderbook, dict) else {}
        up_asks = up_book.get("asks", []) if isinstance(up_book, dict) else []
        down_asks = down_book.get("asks", []) if isinstance(down_book, dict) else []
        if up_asks:
            up_price = float(up_asks[0].get("price", 0.0) or 0.0) or None
        if down_asks:
            down_price = float(down_asks[0].get("price", 0.0) or 0.0) or None
        # REST 盘口不可用时用实时 tick 派生，保证价格与持仓估值同源。
        if up_price is None and yes_ask is not None:
            up_price = yes_ask
        if down_price is None and yes_bid is not None:
            down_price = round(1.0 - yes_bid, 4)

        open_count = len(self._open_positions)
        pending_count = len(self._pending_orders)
        positions = []
        for entry_id, pos in self._open_positions.items():
            try:
                entry_price_f = float(pos.get("entry_price", 0.0) or 0.0)
            except Exception:
                entry_price_f = 0.0
            try:
                qty_f = float(pos.get("filled_qty", 0.0) or 0.0)
            except Exception:
                qty_f = 0.0
            try:
                size_usd_f = float(pos.get("size_usd", 0.0) or 0.0)
            except Exception:
                size_usd_f = 0.0
            try:
                last_bid_f = float(pos.get("last_bid", 0.0) or 0.0)
            except Exception:
                last_bid_f = 0.0
            unrealized = qty_f * (last_bid_f - entry_price_f) if last_bid_f > 0 else 0.0
            positions.append({
                "id": str(entry_id),
                "market_slug": str(pos.get("market_slug", market_slug) or market_slug),
                "direction": str(pos.get("direction", "")).upper(),
                "label": str(pos.get("label", "")),
                "size_usd": size_usd_f,
                "entry_price": entry_price_f,
                "qty_tokens": qty_f,
                "last_bid": last_bid_f,
                "unrealized_pnl": unrealized,
                "signal_score": float(pos.get("signal_score", 0.0) or 0.0),
                "signal_confidence": float(pos.get("signal_confidence", 0.0) or 0.0),
                "exit_in_flight": bool(pos.get("exit_in_flight", False)),
                "is_simulation": bool(pos.get("is_paper", False)),
            })
        if open_count:
            pos = self._open_positions[next(iter(self._open_positions))]
            direction = pos.get("direction", "?").upper()
            position_summary = f"{direction} ×{open_count} (pending={pending_count})"
            signal = direction
            conf = pos.get("signal_confidence", 0.0)
            confidence = f"{float(conf):.1%}" if conf else "—"
        else:
            position_summary = "None" if pending_count == 0 else f"pending={pending_count}"
            signal = "—"
            confidence = "—"

        # 当前模式的持久化交易账本是驾驶舱唯一收益来源。不能使用传统
        # long/short 绩效公式，因为 SHORT 在这里是买入 NO 代币。
        all_trades = (
            list(self.paper_trades)
            if self.current_simulation_mode
            else list(self.live_trades)
        )
        settled = [t for t in all_trades if t.outcome in ("WIN", "LOSS")]
        wins = sum(1 for t in settled if t.outcome == "WIN")
        win_rate = (wins / len(settled) * 100) if settled else 0.0
        total_pnl = sum(float(t.pnl_usd) for t in all_trades)
        strategy_edge = calculate_strategy_edge(all_trades, window=50)
        total_volume_usd = sum(
            float(t.size_usd)
            + (
                float(t.filled_qty) * float(t.exit_price)
                if t.outcome in ("WIN", "LOSS", "BREAKEVEN", "UNRESOLVED")
                else 0.0
            )
            for t in all_trades
        )
        unrealized_pnl = sum(float(item["unrealized_pnl"]) for item in positions)
        starting_balance = float(getattr(self.risk_engine, "_starting_balance", 0.0) or 0.0)
        wallet_balance = starting_balance + total_pnl + unrealized_pnl
        running_balance = starting_balance
        peak_balance = starting_balance
        max_drawdown_pct = 0.0
        for trade in sorted(settled, key=lambda item: item.timestamp):
            running_balance += float(trade.pnl_usd)
            peak_balance = max(peak_balance, running_balance)
            if peak_balance > 0:
                max_drawdown_pct = max(
                    max_drawdown_pct,
                    (peak_balance - running_balance) / peak_balance * 100,
                )
        # 全量口径的账本聚合：trade_history 只截最近 200 笔限制 payload，
        # 期望/盈利因子/平均持仓若由 exporter 从切片重算，超过 200 笔后会
        # 与全量口径的胜率/收益自相矛盾，因此在这里按全量算好下发。
        pnls_settled = [float(t.pnl_usd) for t in settled]
        gross_win = sum(p for p in pnls_settled if p > 0)
        gross_loss = -sum(p for p in pnls_settled if p < 0)
        ledger_expectancy = (sum(pnls_settled) / len(pnls_settled)) if pnls_settled else 0.0
        ledger_profit_factor = (
            gross_win / gross_loss if gross_loss > 1e-9
            else (99.99 if gross_win > 0 else 0.0)
        )
        # 仅统计确有平仓时间的交易，历史遗留行（无 closed_at）不拉低均值
        hold_samples = [
            (t.closed_at - t.timestamp).total_seconds()
            for t in settled
            if getattr(t, "closed_at", None)
        ]
        ledger_avg_hold = (sum(hold_samples) / len(hold_samples)) if hold_samples else 0.0

        trade_history = []
        for trade in all_trades[-200:]:
            row = trade.to_dict()
            row["mode"] = "simulation" if self.current_simulation_mode else "live"
            row["restored"] = True
            trade_history.append(row)

        risk = self.risk_engine.get_risk_summary()
        ml_stats = self.ml_engine.get_stats()
        settle_stats = self.settlement_tracker.get_stats()

        bot_start = self.bot_start_time.astimezone().strftime("%Y-%m-%d %H:%M")

        return {
            "market_slug": market_slug,
            "market_label": market_label,
            "market_start": market_start_iso,
            "market_end": market_end_iso,
            "mode": "simulation" if self.current_simulation_mode else "live",
            "quote_updated_at": self._quote_updated_at,
            "btc_price": btc_price,
            "eth_price": market_data.get("eth_price"),
            "price_to_beat": price_to_beat,
            "up_price": up_price,
            "down_price": down_price,
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "candles": market_data.get("candles", []),
            "candles_updated_at": market_data.get("candles_updated_at"),
            "orderbook": orderbook,
            "orderbook_stale": orderbook_stale,
            "orderbook_updated_at": market_data.get("orderbook_updated_at"),
            "market_data_error": market_data.get("last_error", ""),
            "decision": decision_state,
            "position_summary": position_summary,
            "positions": positions,
            "signal": signal,
            "confidence": confidence,
            "next_window": next_window,
            "trade_window_open": trade_window_open,
            # 交易窗口在 15 分钟周期内的秒数区间，供回合钟绘制行为分区
            "trade_window_sec": [
                0 if self.test_mode else self._trade_window_start,
                900 if self.test_mode else self._trade_window_end,
            ],
            "waiting_for_market": waiting_for_market,
            "instruments_loaded": self._instruments_loaded,
            "open_positions": open_count,
            "rpc_ok": settle_stats.get("rpc_configured", False) or settle_stats.get("chainlink_connected", False),
            "rpc_label": "Chainlink RTDS" if settle_stats.get("chainlink_connected") else "REST fallback",
            "websocket_ok": self._instruments_loaded and not self._waiting_for_market_open,
            "ml_active": ml_stats.get("is_active", False),
            "ml_samples": ml_stats.get("sample_count", 0),
            "ml_min_samples": ml_stats.get("min_samples", 0),
            "settlement_running": self.signal_recorder.is_running,
            "streams_ok": True,
            "completed_trades": len(settled),
            "trade_history_total": len(all_trades),
            "wins": wins,
            "win_rate": win_rate,
            "total_pnl": total_pnl,
            "total_volume_usd": total_volume_usd,
            "prediction_edge": dict(self._latest_prediction_edge),
            "strategy_edge": strategy_edge,
            "order_stats": dict(
                self._order_stats_by_mode.get(
                    "paper" if self.current_simulation_mode else "live",
                    {"entry": {}, "exit": {}},
                )
            ),
            "unrealized_pnl": unrealized_pnl,
            "starting_balance": starting_balance,
            "wallet_balance": wallet_balance,
            "trade_history": trade_history,
            "drawdown_pct": max_drawdown_pct,
            "peak_balance": peak_balance,
            "ledger_expectancy_usd": ledger_expectancy,
            "ledger_profit_factor": ledger_profit_factor,
            "ledger_avg_hold_seconds": ledger_avg_hold,
            "bot_start": bot_start,
        }

    # ── Highlighted order/trade banners ─────────────────────────────────────
    #
    # All real-order events go through these banners so they stand out from
    # the routine quote / metrics / heartbeat noise. Every banner is:
    #   - Two solid ``#``-char borders, preceded and followed by a blank line
    #   - Tagged ``>>> ORDER ...`` (or similar) so it's grep-friendly
    #   - Logged at WARNING/SUCCESS/ERROR levels which loguru colourises
    #     more brightly than routine INFO output
    #
    # ASCII-only on purpose — Windows console sessions default to cp1252
    # and choke on box-drawing characters. ``#`` and ``-`` look heavy
    # enough to stand out against the metric-update noise.
    #
    # ``lines`` is a list of ``(label, value)`` tuples. Pass ``("", "")``
    # to insert a blank separator row inside the banner.

    _BANNER_WIDTH: int = 80
    _STEP_BOX_WIDTH: int = 78

    # Brief description shown at the top of each staged log block.
    _STAGE_DESCRIPTIONS: Dict[str, str] = {
        "DECISION START": (
            "Open a new decision cycle for this quote tick."
        ),
        "CONTEXT": (
            "Fetch external data: spot, funding, CVD, liquidations, OHLCV."
        ),
        "FILTER": (
            "Cheap pre-trade guards — skip before heavy work if rules fail."
        ),
        "STEP 1": (
            "Run all signal processors and fuse them into one consensus."
        ),
        "STEP 2": (
            "Build the feature vector and run XGBoost to predict P(BTC UP)."
        ),
        "STEP 3": (
            "Compare ML probability vs Polymarket implied odds (YES price)."
        ),
        "STEP 4": (
            "Apply ML edge threshold or fusion fallback to choose bet/skip."
        ),
        "STEP 5": (
            "Final gates, save ML features, register settlement tracking."
        ),
        "STEP 6": (
            "Submit a live order or open a simulated paper position."
        ),
        "SETTLE": (
            "Resolve a closed position against settlement and record P&L."
        ),
    }

    def _begin_decision_cycle(self, poly_price: float, is_simulation: bool) -> int:
        """Open a visually distinct decision-cycle block in the logs."""
        self._decision_cycle_counter += 1
        self._current_cycle_id = self._decision_cycle_counter
        self._current_cycle_outcome = "running"
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._dashboard_lock:
            self._decision_state = {
                "cycle_id": self._current_cycle_id,
                "status": "running",
                "current_stage": "DECISION START",
                "outcome": "",
                "updated_at": now_iso,
                "stages": [{
                    "stage": "DECISION START",
                    "title": "MODE",
                    "status": "active",
                    "timestamp": now_iso,
                }],
            }
        mode = "SIM" if is_simulation else "LIVE"
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        sep = "═" * self._STEP_BOX_WIDTH
        logger.info("")
        logger.info(sep)
        logger.info(
            f"  DECISION CYCLE #{self._current_cycle_id}  ·  {mode}  ·  "
            f"{ts}  ·  poly={poly_price:.4f}"
        )
        logger.info(sep)
        return self._current_cycle_id

    def _set_cycle_outcome(self, outcome: str) -> None:
        """Record how this decision cycle ended (shown in the cycle footer)."""
        self._current_cycle_outcome = outcome

    def _end_decision_cycle(self, is_simulation: bool) -> None:
        """Close the decision-cycle block."""
        if self._current_cycle_id is None:
            return
        mode = "SIM" if is_simulation else "LIVE"
        outcome = self._current_cycle_outcome or "ended"
        logger.info(
            f"└─ {mode} CYCLE #{self._current_cycle_id} END ── {outcome} "
            + "─" * max(0, self._STEP_BOX_WIDTH - 28 - len(str(self._current_cycle_id)) - len(outcome))
        )
        logger.info("")
        self._current_cycle_id = None

    def _stage_description(self, step: str) -> str:
        """Lookup the human-readable blurb for a stage key."""
        return self._STAGE_DESCRIPTIONS.get(step, "")

    def _finish_decision_cycle(self, is_simulation: bool, outcome: str) -> None:
        """Record outcome and close the grouped decision-cycle log block."""
        self._set_cycle_outcome(outcome)
        failed = any(token in outcome.upper() for token in ("SKIP", "BLOCK", "REJECT", "FAIL"))
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._dashboard_lock:
            stages = [dict(item) for item in self._decision_state.get("stages", [])]
            for item in stages:
                if item.get("status") == "active":
                    item["status"] = "blocked" if failed else "completed"
            self._decision_state.update({
                "status": "blocked" if failed else "completed",
                "current_stage": None,
                "outcome": outcome,
                "updated_at": now_iso,
                "stages": stages,
            })
        if self.grafana_exporter and hasattr(self.grafana_exporter, "record_dashboard_event"):
            self.grafana_exporter.record_dashboard_event(
                "decision_finished",
                outcome,
                cycle_id=self._current_cycle_id,
                status="blocked" if failed else "completed",
                is_simulation=is_simulation,
            )
        self._publish_dashboard_state()
        self._end_decision_cycle(is_simulation)

    def _publish_order_metrics(
        self,
        *,
        direction: str,
        size_usd: float,
        poly_price: float,
        held_entry_price: float,
        signal,
        ml_p_up: Optional[float],
        ml_edge: float,
        metadata: dict,
        market_slug: str,
        is_simulation: bool,
        fused,
    ) -> None:
        """Push pre-submit order details to Prometheus for Grafana."""
        try:
            held = max(0.01, min(0.99, held_entry_price))
            bid = ask = None
            if self._last_bid_ask:
                bid = float(self._last_bid_ask[0])
                ask = float(self._last_bid_ask[1])
                executable = ask if direction == "long" else 1.0 - bid
                held = max(0.01, min(0.99, executable))
            qty = float(size_usd) / held if held > 0 else 0.0
            prediction_edge = calculate_prediction_edge(
                direction=direction,
                p_up=ml_p_up,
                executable_entry_price=held,
            )
            prediction_edge.update(
                {
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "market_slug": market_slug,
                    "mode": "paper" if is_simulation else "live",
                }
            )
            with self._dashboard_lock:
                self._latest_prediction_edge = prediction_edge
            if not self.grafana_exporter:
                return
            secs_to_settle = None
            if 0 <= self.current_instrument_index < len(self.all_btc_instruments):
                end_ts = self.all_btc_instruments[self.current_instrument_index].get(
                    "end_timestamp"
                )
                if end_ts:
                    secs_to_settle = float(end_ts) - time.time()
            fusion_score = (
                float(fused.score)
                if fused is not None
                else float(getattr(signal, "score", 0.0) or 0.0)
            )
            self.grafana_exporter.update_order_metrics(
                direction=direction,
                size_usd=float(size_usd),
                entry_price=held,
                poly_yes_price=float(poly_price),
                qty_tokens=qty,
                signal_score=float(getattr(signal, "score", 0.0) or 0.0),
                signal_confidence=float(getattr(signal, "confidence", 0.0) or 0.0),
                ml_edge=float(ml_edge or 0.0),
                ml_p_up=ml_p_up,
                fusion_score=fusion_score,
                btc_spot=float(metadata.get("spot_price", 0.0) or 0.0),
                bid_price=bid,
                ask_price=ask,
                seconds_to_settle=secs_to_settle,
                market_slug=market_slug,
                is_simulation=is_simulation,
            )
            # 此处只是执行前指标快照，不是交易所委托。订单事件必须等到
            # submit_order() 成功后再发布；模拟模式则只发布实际模拟成交。
        except Exception:
            pass

    def _log_event_banner(
        self,
        level: str,
        tag: str,
        title: str,
        lines: List[tuple],
    ) -> None:
        """Emit a high-contrast multi-line banner around an order/trade event."""
        log = {
            "info": logger.info,
            "warning": logger.warning,
            "success": getattr(logger, "success", logger.info),
            "error": logger.error,
        }.get(level, logger.info)

        border = "#" * self._BANNER_WIDTH
        sep = "#" + "-" * (self._BANNER_WIDTH - 2) + "#"

        # Compute label width for clean alignment (cap to keep value column).
        label_width = min(
            12,
            max((len(lbl) for lbl, _ in lines if lbl), default=0),
        )

        log("")
        log(border)
        log(f"### >>> {tag}: {title}")
        log(sep)
        for label, value in lines:
            if label == "" and value == "":
                log("#")
                continue
            if label:
                log(f"#  {label:<{label_width}}  {value}")
            else:
                log(f"#  {value}")
        log(border)
        log("")

    def _log_step(
        self,
        step: str,
        title: str,
        lines: List[tuple] = None,
        *,
        is_simulation: bool = True,
        level: str = "info",
        description: Optional[str] = None,
    ) -> None:
        """Emit a grouped, labelled block for one stage of the decision loop.

        Format::

            ┌─[SIM │ CYCLE #12 │ STEP 1 │ SIGNALS + FUSION]────────────── ...
            │  ▸ Run all signal processors and fuse them into one consensus.
            │
            │  Signals fired          3  (2 bullish / 1 bearish)
            │    SpikeDetection       BULLISH  conf=82%  score=74.0
            └────────────────────────────────────────────────────────────── ...
        """
        self._set_decision_stage(step, title)
        mode_tag = "SIM" if is_simulation else "LIVE"
        cycle_part = (
            f"CYCLE #{self._current_cycle_id} │ "
            if self._current_cycle_id is not None
            else ""
        )
        header_core = f"[{mode_tag} │ {cycle_part}{step} │ {title}]"
        header = "┌─" + header_core
        header = header + "─" * max(0, self._STEP_BOX_WIDTH - len(header))

        desc = (description or self._stage_description(step)).strip()
        log = {
            "info": logger.info,
            "warning": logger.warning,
            "success": getattr(logger, "success", logger.info),
            "error": logger.error,
        }.get(level, logger.info)

        log(header)
        if desc:
            log(f"│  ▸ {desc}")
            log("│")
        for label, value in (lines or []):
            if label:
                log(f"│  {label:<22} {value}")
            elif value:
                log(f"│  {value}")
            else:
                log("│")
        log("└" + "─" * self._STEP_BOX_WIDTH)

    # ── Timer loop ────────────────────────────────────────────────────────────

    def _start_timer_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._timer_loop())
        finally:
            loop.close()

    async def _timer_loop(self) -> None:
        while not self._stopping:
            uptime_minutes = (
                (datetime.now(timezone.utc) - self.bot_start_time).total_seconds() / 60
            )
            if uptime_minutes >= self.restart_after_minutes:
                logger.warning("=" * 80)
                logger.warning("MARKET REFRESH — fetching new BTC 15-min instruments in-place")
                logger.warning("=" * 80)
                try:
                    added = self._load_instruments_via_gamma_direct()
                    logger.info(
                        f"Refresh: +{added} new instruments fetched via direct Gamma load"
                    )
                    self._load_all_btc_instruments(quiet=True)
                    # Reset the cycle so we refresh again in another
                    # `restart_after_minutes` minutes. No process restart.
                    self.bot_start_time = datetime.now(timezone.utc)
                except Exception as e:
                    logger.error(f"Market refresh failed: {e}")
                    # Don't burn the loop spamming refreshes — push the
                    # next attempt out by 5 min and keep trading.
                    self.bot_start_time = datetime.now(timezone.utc) - timedelta(
                        minutes=max(0, self.restart_after_minutes - 5)
                    )

            # Retry instrument loading until Nautilus has populated the cache.
            if not self._instruments_loaded:
                self._instrument_load_attempts += 1
                # Log the first few attempts then go quiet to avoid log spam.
                quiet = self._instrument_load_attempts > 3
                if self._load_all_btc_instruments(quiet=quiet):
                    logger.info(
                        f"Instruments loaded after {self._instrument_load_attempts} "
                        f"attempt(s) — strategy is ready to trade."
                    )
                elif self._instrument_load_attempts == self._max_instrument_load_attempts:
                    logger.error(
                        f"Failed to load BTC 15-min instruments after "
                        f"{self._max_instrument_load_attempts} attempts. "
                        f"Check Polymarket connectivity and Gamma API patches."
                    )
                elif self._instrument_load_attempts > self._max_instrument_load_attempts:
                    # Stop retrying but keep the loop alive for stop/restart logic.
                    pass

            now = datetime.now(timezone.utc)

            # Push the real Polymarket USDC balance into the risk engine so
            # drawdown / daily-loss limits track actual capital, not defaults.
            self._sync_risk_engine_balance()

            # Heartbeat: report bot status every _heartbeat_secs so the user
            # can see it's alive and how long until the next trade window.
            self._emit_heartbeat(now)

            # Settle positions whose underlying market has already resolved.
            # Polymarket auto-resolves binary markets at end_timestamp; the
            # held token pays $1 to the winner / $0 to the loser. We compute
            # the realised P&L from a definitive settlement source (the
            # SettlementTracker's Chainlink outcome) and fall back to the
            # last observed bid for the held token if Chainlink is offline.
            self._settle_open_positions(now)

            if self.next_switch_time and now >= self.next_switch_time:
                if self._waiting_for_market_open:
                    logger.info("=" * 80)
                    logger.info(f"WAITING MARKET NOW OPEN: {now.strftime('%H:%M:%S')} UTC")
                    logger.info("=" * 80)
                    if 0 <= self.current_instrument_index < len(self.all_btc_instruments):
                        current_market = self.all_btc_instruments[self.current_instrument_index]
                        self.next_switch_time = current_market["end_time"]
                        logger.info(f"  Market ends at {self.next_switch_time.strftime('%H:%M:%S')} UTC")
                    self._waiting_for_market_open = False
                    self._market_stable = True
                    self._stable_tick_count = QUOTE_STABILITY_REQUIRED
                    self.last_trade_time = -1
                    logger.info("  MARKET OPEN — ready to trade on next tick")
                else:
                    self._switch_to_next_market()

            # Scheduled learning-engine weight optimisation
            hours_since_learn = (
                (datetime.now(timezone.utc) - self._last_learning_optimization).total_seconds() / 3600
            )
            if hours_since_learn >= self._learning_interval_hours:
                logger.info("=" * 60)
                logger.info("SCHEDULED: Running learning engine weight optimisation")
                logger.info("=" * 60)
                try:
                    new_weights = await self.learning_engine.optimize_weights()
                    self._last_learning_optimization = datetime.now(timezone.utc)
                    logger.info(
                        f"Learning engine complete: {len(new_weights)} weights updated. "
                        f"Next run in {self._learning_interval_hours:.1f}h"
                    )
                except Exception as _le:
                    logger.warning(f"Learning engine scheduled run failed: {_le}")

            for _ in range(10):
                if self._stopping:
                    break
                await asyncio.sleep(1)

        logger.info("Timer loop stopped")

    # ── Risk-engine balance sync ──────────────────────────────────────────────

    def _sync_risk_engine_balance(self) -> None:
        """
        Mirror the real Polymarket USDC balance into the risk engine.

        The Polymarket exec client reports the wallet's collateral balance as
        an AccountState in ``pUSD``; without this sync the risk engine falls
        back to the ``ACCOUNT_BALANCE_USD`` env default and its daily-loss /
        drawdown gates are computed against fictional capital.

        Only pushes when the wallet reading actually changed, so in paper mode
        (constant wallet) the risk engine keeps tracking simulated P&L instead
        of being reset every minute. Throttled to one portfolio read per 60s.
        """
        # 模拟盘不同步：venue 纸面账户的余额是 Nautilus 的虚构默认值
        #（实测 $100），与配置的模拟本金无关。用它重定基会把驾驶舱
        # "账户权益"的基数从 ACCOUNT_BALANCE_USD 污染成 $100
        #（2026-07-26 实例：权益显示 $121 而实际应为 ~$1021）。
        if self.current_simulation_mode:
            return
        now_mono = time.monotonic()
        if now_mono < self._risk_balance_next_check:
            return
        self._risk_balance_next_check = now_mono + 60.0
        try:
            account = self.portfolio.account(POLYMARKET_VENUE)
            if account is None:
                return
            free = account.balance_free(pUSD)
            if free is None:
                return
            balance = free.as_decimal()
            if balance <= 0:
                return
            if balance == self._risk_balance_last_synced:
                return
            first_sync = self._risk_balance_last_synced is None
            self.risk_engine.set_account_balance(balance, reset_peak=first_sync)
            if first_sync:
                # 首次拿到真实余额时同步重设绩效面板本金，使驾驶舱
                # "账户权益"与 ROI 基于真实资金而非默认值。
                self.performance_tracker.set_initial_capital(balance)
            self._risk_balance_last_synced = balance
        except Exception as e:
            logger.debug(f"Risk balance sync skipped: {e}")

    # ── Quote tick handler ────────────────────────────────────────────────────

    def on_quote_tick(self, tick: QuoteTick) -> None:
        try:
            bid, ask = tick.bid_price, tick.ask_price
            if bid is None or ask is None:
                return

            try:
                bid_decimal = bid.as_decimal()
                ask_decimal = ask.as_decimal()
            except Exception:
                return
            self._quote_updated_at = datetime.now(timezone.utc).isoformat()

            # Live / paper exit check on every tick for held tokens.
            try:
                self._check_position_exits(tick.instrument_id, bid_decimal, ask_decimal)
            except Exception as e:
                logger.warning(f"Exit check failed: {e}")

            # Paper SHORT (NO token): derive NO bid/ask from YES when NO stream
            # is quiet but YES ticks are flowing on the active market.
            yes_id = getattr(self, "_yes_instrument_id", None) or self.instrument_id
            no_id = getattr(self, "_no_instrument_id", None)
            if no_id and tick.instrument_id == yes_id:
                derived_bid = max(Decimal("0.01"), Decimal("1") - ask_decimal)
                derived_ask = max(Decimal("0.01"), Decimal("1") - bid_decimal)
                try:
                    self._check_position_exits(no_id, derived_bid, derived_ask)
                except Exception as e:
                    logger.warning(f"Derived NO exit check failed: {e}")

            # Everything below is signal/decision logic for the active market
            # only. Position exits already ran above for any held instrument.
            if self.instrument_id is None or tick.instrument_id != self.instrument_id:
                self._publish_dashboard_state()
                return

            now = datetime.now(timezone.utc)

            mid_price = (bid_decimal + ask_decimal) / 2
            self.price_history.append(mid_price)
            if len(self.price_history) > self.max_history:
                self.price_history.pop(0)

            self._last_bid_ask = (bid_decimal, ask_decimal)
            self._publish_dashboard_state()
            self._tick_buffer.append({"ts": now, "price": mid_price})
            self._tick_count_since_last_heartbeat += 1

            if not self._market_stable:
                self._stable_tick_count += 1
                if self._stable_tick_count >= QUOTE_STABILITY_REQUIRED:
                    self._market_stable = True
                    logger.info(
                        f"Market STABLE after {self._stable_tick_count} tick(s)"
                    )
                else:
                    return

            if self._waiting_for_market_open:
                return

            if not (0 <= self.current_instrument_index < len(self.all_btc_instruments)):
                return

            current_market = self.all_btc_instruments[self.current_instrument_index]
            market_start_ts = current_market["market_timestamp"]

            elapsed_secs = now.timestamp() - market_start_ts
            if elapsed_secs < 0:
                return

            sub_interval = int(elapsed_secs // MARKET_INTERVAL_SECONDS)
            seconds_into_sub = elapsed_secs % MARKET_INTERVAL_SECONDS

            # Trade window: configurable via TRADE_WINDOW_SEC_{START,END}.
            # Default 780-870s (13:00–14:30) — enter once the market trend is
            # fully established; forced time-exit fires at second 870.
            if self.test_mode:
                # In test mode the whole 15-min market is the trade window.
                window_start, window_end = 0, 900
            else:
                window_start = self._trade_window_start
                window_end = self._trade_window_end

            if not (window_start <= seconds_into_sub < window_end):
                return

            # Late-entry cutoff — don't open new positions in the last
            # `_late_entry_cutoff_sec` of the market. FAK rejects spike
            # near settlement and the binary outcome is about to pay out.
            current_slug = current_market.get("slug", "")
            now_ts = now.timestamp()
            market_end_ts = int(current_market.get("end_timestamp", 0) or 0)
            if market_end_ts and (market_end_ts - now_ts) < self._late_entry_cutoff_sec:
                return

            # MAX_TRADES_PER_MARKET — hard cap (safety net on top of
            # cooldown + direction lock).
            if (
                self._market_trade_count.get(current_slug, 0)
                >= self._max_trades_per_market
            ):
                return

            # Re-entry gating: instead of "one trade per market", allow
            # multiple entries inside the window subject to:
            #   1. cooldown since last entry attempt has elapsed, AND
            #   2. no open position is already running on this market.
            cooldown_ok = (now_ts - self._last_entry_ts) >= self._entry_cooldown_sec
            has_open_on_market = any(
                p.get("market_slug") == current_slug
                for p in self._open_positions.values()
            )
            has_pending_on_market = any(
                p.get("market_slug") == current_slug
                for p in self._pending_orders.values()
            )

            if not cooldown_ok or has_open_on_market or has_pending_on_market:
                return

            # Reserve the cooldown slot *now* so concurrent ticks during
            # the async decision call don't double-fire.
            self._last_entry_ts = now_ts

            logger.info("=" * 80)
            logger.info(f"TRADE WINDOW: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            logger.info(f"  Market:   {current_market['slug']}")
            logger.info(
                f"  Sub-interval #{sub_interval} "
                f"({seconds_into_sub:.1f}s in = {seconds_into_sub/60:.1f} min)"
            )
            logger.info(
                f"  Price: ${float(mid_price):,.4f} | "
                f"Bid: ${float(bid_decimal):,.4f} | "
                f"Ask: ${float(ask_decimal):,.4f}"
            )
            logger.info(
                f"  Trend: {'STRONG' if float(mid_price) > 0.60 or float(mid_price) < 0.40 else 'WEAK'}"
            )
            logger.info("=" * 80)

            self.run_in_executor(lambda: self._make_trading_decision_sync(float(mid_price)))

        except Exception as e:
            logger.error(f"Error processing quote tick: {e}")

    # ── Trading decision ──────────────────────────────────────────────────────

    def _make_trading_decision_sync(self, current_price: float) -> None:
        """Synchronous wrapper — called from executor thread."""
        if self._stopping:
            return
        price_decimal = Decimal(str(current_price))
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._make_trading_decision(price_decimal))
        finally:
            loop.close()

    async def _fetch_market_context(self, current_price: Decimal) -> tuple:
        """Fetch real external data to populate all signal-processor metadata."""
        current_price_float = float(current_price)

        recent_prices = [float(p) for p in self.price_history[-20:]]
        sma_20 = sum(recent_prices) / len(recent_prices)
        deviation = (current_price_float - sma_20) / sma_20
        momentum = (
            (current_price_float - float(self.price_history[-5])) / float(self.price_history[-5])
            if len(self.price_history) >= 5
            else 0.0
        )
        variance = sum((p - sma_20) ** 2 for p in recent_prices) / len(recent_prices)
        volatility = math.sqrt(variance)

        metadata: dict = {
            "deviation": deviation,
            "momentum": momentum,
            "volatility": volatility,
            "tick_buffer": list(self._tick_buffer),
            "yes_token_id": self._yes_token_id,
        }

        # Fear & Greed
        try:
            from data_sources.news_social import NewsSocialDataSource
            news_source = NewsSocialDataSource()
            await news_source.connect()
            fg = await news_source.get_fear_greed_index()
            await news_source.disconnect()
            if fg and "value" in fg:
                metadata["sentiment_score"] = float(fg["value"])
                metadata["sentiment_classification"] = fg.get("classification", "")
                logger.debug(
                    f"Fear & Greed: {metadata['sentiment_score']:.0f} "
                    f"({metadata['sentiment_classification']})"
                )
            else:
                logger.debug("Fear & Greed fetch returned no data")
        except Exception as e:
            logger.debug(f"Could not fetch Fear & Greed: {e}")

        # Coinbase spot price
        try:
            from data_sources.coinbase import CoinbaseDataSource
            coinbase = CoinbaseDataSource()
            await coinbase.connect()
            spot = await coinbase.get_current_price()
            await coinbase.disconnect()
            if spot:
                metadata["spot_price"] = float(spot)
                logger.debug(f"Coinbase spot: ${float(spot):,.2f}")
            else:
                logger.debug("Coinbase price fetch returned None")
        except Exception as e:
            logger.debug(f"Could not fetch Coinbase spot price: {e}")

        logger.debug(
            f"Market context — deviation={deviation:.2%}, "
            f"momentum={momentum:.2%}, volatility={volatility:.4f}, "
            f"sentiment={'%.0f' % metadata['sentiment_score'] if 'sentiment_score' in metadata else 'N/A'}, "
            f"spot=${'%.2f' % metadata['spot_price'] if 'spot_price' in metadata else 'N/A'}"
        )

        # Liquidation snapshot
        liq_snap = self.liquidation_processor._get_window_snapshot()
        liq_total = liq_snap["long_liq_usd"] + liq_snap["short_liq_usd"]
        liq_imbalance = (
            (liq_snap["long_liq_usd"] - liq_snap["short_liq_usd"]) / liq_total
            if liq_total > 0
            else 0.0
        )
        metadata["liq_imbalance"] = liq_imbalance
        metadata["liq_total_usd"] = liq_total

        # CVD snapshot
        cvd_snap = self.cvd_ob_processor._compute_cvd()
        metadata["cvd_delta_usd"] = cvd_snap["cvd_delta"]

        # Binance spot order book (for Binance-side CVD imbalance feature)
        ob_snap = self.cvd_ob_processor._fetch_order_book()
        if ob_snap:
            metadata["ob_imbalance"] = ob_snap["imbalance"]

        # Polymarket CLOB order-book imbalance (poly_ob_imbalance ML feature).
        # Fetched independently from the OrderBook signal processor so the
        # feature vector always has this value regardless of signal threshold.
        if self._yes_token_id:
            try:
                poly_book = self.orderbook_processor.fetch_order_book(self._yes_token_id)
                if poly_book:
                    bid_vol = self.orderbook_processor._parse_levels(poly_book.get("bids", []))
                    ask_vol = self.orderbook_processor._parse_levels(poly_book.get("asks", []))
                    total = bid_vol + ask_vol
                    if total > 0:
                        metadata["poly_ob_imbalance"] = (bid_vol - ask_vol) / total
            except Exception as _pob_e:
                logger.debug(f"Polymarket OB fetch skipped: {_pob_e}")

        # Funding + OI
        try:
            fi_data = self.funding_oi_processor._fetch_data()
            if fi_data:
                metadata["funding_rate"] = fi_data["funding_rate"]
                metadata["oi_change"] = fi_data["oi_change"]
        except Exception:
            pass

        # OHLCV
        try:
            klines = self.ohlcv_momentum_processor._fetch_klines()
            if klines:
                for key in ("vol_regime", "rsi", "macd_line", "macd_signal",
                            "pct_b", "ret1", "ret3", "ret5", "ret15"):
                    metadata[key] = klines[key]
        except Exception:
            pass

        return metadata, liq_imbalance, cvd_snap

    async def _make_trading_decision(self, current_price: Decimal) -> None:
        """
        6-step ML decision loop.

        Step 1  Collect all 8 features → feature vector
        Step 2  XGBoost model → p(BTC UP)
        Step 3  Compare vs Polymarket implied odds
        Step 4  Edge check → bet if mispriced
        Step 5  Register with settlement tracker
        Step 6  Weekly retrain triggered by settlement tracker
        """
        if self._stopping:
            return
        is_simulation = await self.check_simulation_mode()
        if self._stopping:
            return

        if len(self.price_history) < 20:
            logger.warning(f"Not enough price history ({len(self.price_history)}/20)")
            return

        poly_price = float(current_price)
        self._begin_decision_cycle(poly_price, is_simulation)

        self._log_step(
            "DECISION START", "MODE",
            [
                ("Mode",          "SIMULATION (paper)" if is_simulation else "*** LIVE TRADING ***"),
                ("Poly price",    f"{poly_price:.4f}"),
                ("Price history", f"{len(self.price_history)} ticks"),
            ],
            is_simulation=is_simulation,
            level="info" if is_simulation else "warning",
        )

        # Helper: schedule the next entry attempt `delay` seconds from
        # now (instead of "right now" which busy-loops at full tick rate
        # whenever the filter keeps rejecting on the same price).
        now_ts_for_cooldown = datetime.now(timezone.utc).timestamp()

        def _backoff_after_filter_reject() -> None:
            # Backdate _last_entry_ts so that the next on_quote_tick
            # attempt is in `_filter_reject_cooldown_sec` seconds rather
            # than on the very next tick.
            self._last_entry_ts = now_ts_for_cooldown - max(
                0, self._entry_cooldown_sec - self._filter_reject_cooldown_sec
            )

        # ── Pre-trade filters (cheap; bail out before any external IO) ──
        # 1. Entry-price band — refuse the bet when the token is already
        #    pricing the outcome near-certain. R:R is junk at extremes.
        if not (self._min_entry_price <= poly_price <= self._max_entry_price):
            self._log_step(
                "FILTER", "PRICE BAND — SKIP",
                [
                    ("Price",    f"{poly_price:.4f}"),
                    ("Band",     f"[{self._min_entry_price:.2f}, {self._max_entry_price:.2f}]"),
                    ("Reason",   "price outside bet band — R:R junk at extremes"),
                    ("Retry in", f"{self._filter_reject_cooldown_sec}s"),
                ],
                is_simulation=is_simulation,
            )
            _backoff_after_filter_reject()
            self._finish_decision_cycle(is_simulation, "SKIP — price outside bet band")
            return

        # 2. Spread filter — buying through a wide spread eats the edge.
        last_tick = getattr(self, "_last_bid_ask", None)
        if last_tick:
            last_bid_f = float(last_tick[0])
            last_ask_f = float(last_tick[1])
            mid_f = (last_bid_f + last_ask_f) / 2
            if mid_f > 0:
                spread_pct = (last_ask_f - last_bid_f) / mid_f
                if spread_pct > self._max_spread_pct:
                    self._log_step(
                        "FILTER", "SPREAD — SKIP",
                        [
                            ("Spread",   f"{spread_pct:.2%}  >  max {self._max_spread_pct:.2%}"),
                            ("Bid/Ask",  f"${last_bid_f:.4f} / ${last_ask_f:.4f}"),
                            ("Reason",   "wide spread eats the edge"),
                            ("Retry in", f"{self._filter_reject_cooldown_sec}s"),
                        ],
                        is_simulation=is_simulation,
                    )
                    _backoff_after_filter_reject()
                    self._finish_decision_cycle(is_simulation, "SKIP — spread too wide")
                    return

        metadata, liq_imbalance, cvd_snap = await self._fetch_market_context(current_price)
        self._log_step(
            "CONTEXT", "MARKET DATA",
            [
                ("BTC spot",    f"${metadata.get('spot_price', 0):,.0f}" if metadata.get("spot_price") else "n/a"),
                ("Liq imbalance", f"{liq_imbalance:+.3f}"),
                ("CVD delta",   f"${cvd_snap['cvd_delta']/1e6:+.1f}M"),
                ("Funding rate", f"{metadata.get('funding_rate', 0):.5%}"),
                ("Vol regime",  str(metadata.get("vol_regime", "N/A"))),
            ],
            is_simulation=is_simulation,
        )
        signals = self._process_signals(current_price, metadata)

        # ── Grafana: push per-processor metrics ──────────────────────────────
        if self.grafana_exporter:
            try:
                for sig in signals:
                    _dir = str(getattr(sig, "direction", "neutral")).lower()
                    _dir = "bullish" if "bull" in _dir else ("bearish" if "bear" in _dir else "neutral")
                    self.grafana_exporter.update_signal_processor(
                        name=sig.source,
                        score=float(getattr(sig, "score", 0.0) or 0.0),
                        confidence=float(getattr(sig, "confidence", 0.0) or 0.0),
                        direction=_dir,
                        metadata=sig.metadata or {},
                    )
            except Exception:
                pass
            # 无论处理器是否触发信号，都推送一次市场指标——否则驾驶舱的
            # RSI/资金费率/CVD/恐惧贪婪四格只有在对应处理器开火时才有值。
            # 独立 try：单个 signal 的异常不应吞掉指标推送；只读处理器的
            # K 线缓存（不主动拉取），Binance 故障时不产生额外阻塞请求。
            try:
                ohlcv = self.ohlcv_momentum_processor._cached_klines or {}
                self.grafana_exporter.update_market_indicators(
                    rsi=ohlcv.get("rsi"),
                    macd_histogram=(
                        ohlcv["macd_line"] - ohlcv["macd_signal"]
                        if "macd_line" in ohlcv and "macd_signal" in ohlcv
                        else None
                    ),
                    funding_rate=metadata.get("funding_rate"),
                    cvd_delta=metadata.get("cvd_delta_usd"),
                    fear_greed=metadata.get("sentiment_score"),
                )
            except Exception:
                pass

        for sig in signals:
            if sig.source == "TickVelocity":
                metadata["velocity_60s"] = sig.metadata.get("velocity_60s") or 0.0
                metadata["velocity_30s"] = sig.metadata.get("velocity_30s") or 0.0
                break

        # min_signals=2：融合分是占比制，单处理器独自开火时 score 恒为 100，
        # 任何单点信号 bug 都会被放大成"高置信度"方向（双审计确认的传导链）。
        fused = self.fusion_engine.fuse_signals(signals, min_signals=2, min_score=40.0)

        # ── Grafana: push fusion metrics ──────────────────────────────────────
        if self.grafana_exporter and fused:
            try:
                _fdir = str(getattr(fused, "direction", "neutral")).lower()
                self.grafana_exporter.update_fusion_metrics(
                    score=float(fused.score),
                    confidence=float(fused.confidence),
                    num_signals=int(fused.num_signals),
                    direction=("bullish" if "bull" in _fdir else ("bearish" if "bear" in _fdir else "neutral")),
                )
            except Exception:
                pass

        if signals:
            n_bull = sum(1 for s in signals if "BULLISH" in str(s.direction).upper())
            n_bear = sum(1 for s in signals if "BEARISH" in str(s.direction).upper())
            sig_lines: List[tuple] = [
                ("Signals fired",  f"{len(signals)}  ({n_bull} bullish / {n_bear} bearish)"),
            ]
            for _s in signals:
                _dir = str(getattr(_s, "direction", "")).replace("SignalDirection.", "").upper()
                _conf = getattr(_s, "confidence", 0.0) or 0.0
                _score = getattr(_s, "score", 0.0) or 0.0
                sig_lines.append((
                    f"  {_s.source}",
                    f"{_dir:<8}  conf={float(_conf):.2%}  score={float(_score):.1f}",
                ))
            if fused:
                fused_dir = fused.direction.value.upper()
                arrow = "▲" if "BULL" in fused_dir else "▼"
                sig_lines.append(("", ""))
                sig_lines.append((
                    "Fused result",
                    f"{arrow} {fused_dir}  score={fused.score:.1f}  conf={fused.confidence:.2%}",
                ))
            else:
                sig_lines.append(("Fused result", "NONE  (below min_score=40)"))
        else:
            sig_lines = [
                ("Signals fired", "0 — proceeding to ML / trend filter"),
            ]
            if fused:
                sig_lines.append(("Fused result", f"{fused.direction.value.upper()}"))
            else:
                sig_lines.append(("Fused result", "NONE"))

        self._log_step(
            "STEP 1", "SIGNALS + FUSION",
            sig_lines,
            is_simulation=is_simulation,
        )
        if fused:
            fused_dir = str(fused.direction.value).upper()
            if "NEUTRAL" not in fused_dir:
                tui_event(
                    "FUSION",
                    f"{fused.num_signals} signals → {fused_dir} "
                    f"score={fused.score:.1f} conf={fused.confidence:.1%}",
                    slug="S1",
                )

        # STEP 2 — ML model
        flat_metadata = {
            k: float(v) if hasattr(v, "__float__") else v
            for k, v in metadata.items()
            if not isinstance(v, (list, dict))
        }
        feature_vector = self.ml_engine.build_feature_vector(
            metadata=flat_metadata,
            poly_price=poly_price,
        )

        ml_p_up: Optional[float] = None
        if self.ml_engine.is_active and feature_vector is not None:
            ml_p_up = self.ml_engine.predict(feature_vector)
            implied_edge = abs(ml_p_up - poly_price) if ml_p_up is not None else 0.0
            # ── Grafana: push ML metrics ──────────────────────────────────────
            if self.grafana_exporter and ml_p_up is not None:
                try:
                    self.grafana_exporter.update_ml_metrics(
                        edge=implied_edge,
                        prediction=float(ml_p_up),
                    )
                except Exception:
                    pass
            self._log_step(
                "STEP 2", "ML MODEL",
                [
                    ("Status",    f"ACTIVE  ({self.ml_engine._sample_count} samples)"),
                    ("p(UP)",     f"{ml_p_up:.4f}"),
                    ("Poly price", f"{poly_price:.4f}"),
                    ("Implied edge", f"{implied_edge:.4f}  (min={self.ml_engine.min_edge:.4f})"),
                ],
                is_simulation=is_simulation,
            )
            tui_event(
                "ML",
                f"p(UP)={ml_p_up:.2%} edge={implied_edge:.2%}",
                slug="S2",
            )
            gap = ml_p_up - poly_price
            self._log_step(
                "STEP 3", "MARKET vs ML",
                [
                    ("ML p(UP)",      f"{ml_p_up:.4f}"),
                    ("Poly (implied)", f"{poly_price:.4f}"),
                    ("Gap (ML−Poly)", f"{gap:+.4f}"),
                    ("Interpretation", (
                        "ML sees higher UP prob than market"
                        if gap > 0.01
                        else "ML sees lower UP prob than market"
                        if gap < -0.01
                        else "ML roughly agrees with market"
                    )),
                ],
                is_simulation=is_simulation,
            )
        else:
            self._log_step(
                "STEP 2", "ML MODEL",
                [
                    ("Status",    f"WARMING UP  "
                                  f"({self.ml_engine._sample_count}/{self.ml_engine.min_samples} samples needed)"),
                    ("p(UP)",     "N/A — falling back to fusion direction"),
                ],
                is_simulation=is_simulation,
            )
            self._log_step(
                "STEP 3", "MARKET vs ML",
                [
                    ("ML p(UP)",      "N/A — model warming up"),
                    ("Poly (implied)", f"{poly_price:.4f}"),
                    ("Note",          "STEP 4 will use fusion fallback rules"),
                ],
                is_simulation=is_simulation,
            )

        # Record the full cycle snapshot (all processor signals + fused result +
        # ML p_up) for offline fused-signal backtesting. This runs on EVERY
        # evaluated cycle — even when no trade is ultimately placed — so the
        # recorded dataset captures the strategy's behaviour across all regimes.
        try:
            if 0 <= self.current_instrument_index < len(self.all_btc_instruments):
                _mkt = self.all_btc_instruments[self.current_instrument_index]
                self.signal_recorder.record_cycle(
                    market_slug=_mkt.get("slug", "unknown"),
                    market_start_ts=_mkt.get("market_timestamp"),
                    market_end_ts=_mkt.get("end_timestamp"),
                    poly_price=poly_price,
                    btc_spot=metadata.get("spot_price"),
                    signals=signals,
                    fused=fused,
                    ml_p_up=ml_p_up,
                    metadata=flat_metadata,
                )
        except Exception as _rec_e:
            logger.debug(f"Signal recording skipped: {_rec_e}")

        # STEPS 3 + 4 — Edge check / fallback trend filter
        # The market-order patch reads MARKET_BUY_USD at submit time, so the
        # strategy must use the same value for risk validation and logging
        # otherwise the risk engine and the actual fill amount disagree.
        try:
            position_size_usd = max(0.01, float(os.getenv("MARKET_BUY_USD", "1.0")))
        except (TypeError, ValueError):
            position_size_usd = 1.0
        POSITION_SIZE_USD = Decimal(str(position_size_usd))
        direction: Optional[str] = None
        bet_edge: float = 0.0

        if ml_p_up is not None:
            should_bet, ml_direction, _edge = self.ml_engine.should_bet(
                p_up=ml_p_up,
                poly_price=poly_price,
            )
            if not should_bet:
                self._log_step(
                    "STEP 4", "EDGE CHECK — NO BET",
                    [
                        ("ML p(UP)",   f"{ml_p_up:.4f}"),
                        ("Poly price", f"{poly_price:.4f}"),
                        ("Gap",        f"{abs(ml_p_up - poly_price):.4f}  <  min={self.ml_engine.min_edge:.4f}"),
                        ("Decision",   "SKIP — insufficient edge"),
                    ],
                    is_simulation=is_simulation,
                )
                if feature_vector is not None:
                    slug = (
                        self.all_btc_instruments[self.current_instrument_index]["slug"]
                        if self.current_instrument_index >= 0
                        else "unknown"
                    )
                    self.ml_engine.record_trade(
                        market_slug=slug,
                        poly_price=poly_price,
                        feature_vector=feature_vector,
                    )
                self._finish_decision_cycle(is_simulation, "SKIP — insufficient ML edge")
                return
            direction = ml_direction
            bet_edge = _edge
            self._log_step(
                "STEP 4", "EDGE CHECK — BET",
                [
                    ("ML p(UP)",   f"{ml_p_up:.4f}"),
                    ("Poly price", f"{poly_price:.4f}"),
                    ("Edge",       f"{bet_edge:.4f}  ≥  min={self.ml_engine.min_edge:.4f}"),
                    ("Direction",  f"{'▲ LONG (YES)' if direction == 'long' else '▼ SHORT (NO)'}"),
                ],
                is_simulation=is_simulation,
                level="success" if is_simulation else "warning",
            )
            trend = "UP" if direction == "long" else "DOWN"
            tui_event(
                "DECISION",
                f"Trend {trend} {ml_p_up * 100:.2f}% → "
                f"{'LONG' if direction == 'long' else 'SHORT'}",
                slug="S4",
            )
        else:
            # ML is warming up — fall back to the fused signal direction.
            # CRITICAL: the bet direction comes from the FUSION DIRECTION,
            # not from price extremity. The old behaviour ("if poly>0.60 go
            # LONG") was firing AGAINST a bearish consensus near the top of
            # the market and against a bullish consensus near the bottom —
            # i.e. it was systematically buying tops and selling bottoms.
            #
            # Require ≥2 signals agreeing (min_signals=2) with consensus
            # score ≥55 to filter out weak/noisy fallback trades.
            fallback_fused = self.fusion_engine.fuse_signals(
                signals, min_signals=2, min_score=55.0
            ) if signals else None

            if fallback_fused is None:
                self._log_step(
                    "STEP 4", "FALLBACK — NO CONSENSUS — SKIP",
                    [
                        ("ML",      "warming up — no model yet"),
                        ("Signals", f"{len(signals)} fired, insufficient fusion consensus"),
                        ("Decision","SKIP"),
                    ],
                    is_simulation=is_simulation,
                )
                if feature_vector is not None:
                    slug = (
                        self.all_btc_instruments[self.current_instrument_index]["slug"]
                        if self.current_instrument_index >= 0
                        else "unknown"
                    )
                    self.ml_engine.record_trade(
                        market_slug=slug,
                        poly_price=poly_price,
                        feature_vector=feature_vector,
                    )
                self._finish_decision_cycle(is_simulation, "SKIP — no fusion consensus")
                return

            fused_dir = str(fallback_fused.direction).upper()
            if "BULLISH" in fused_dir:
                direction = "long"
                self._log_step(
                    "STEP 4", "FALLBACK — FUSION BET",
                    [
                        ("Fusion",    f"▲ BULLISH  score={fallback_fused.score:.1f}  conf={fallback_fused.confidence:.2%}"),
                        ("Poly price", f"{poly_price:.4f}"),
                        ("Direction", "▲ LONG (YES)"),
                    ],
                    is_simulation=is_simulation,
                    level="success" if is_simulation else "warning",
                )
                tui_event(
                    "DECISION",
                    f"Trend UP {poly_price * 100:.2f}% → LONG",
                    slug="S4",
                )
            elif "BEARISH" in fused_dir:
                direction = "short"
                self._log_step(
                    "STEP 4", "FALLBACK — FUSION BET",
                    [
                        ("Fusion",    f"▼ BEARISH  score={fallback_fused.score:.1f}  conf={fallback_fused.confidence:.2%}"),
                        ("Poly price", f"{poly_price:.4f}"),
                        ("Direction", "▼ SHORT (NO)"),
                    ],
                    is_simulation=is_simulation,
                    level="success" if is_simulation else "warning",
                )
                tui_event(
                    "DECISION",
                    f"Trend DOWN {(1 - poly_price) * 100:.2f}% → SHORT",
                    slug="S4",
                )
            else:
                self._log_step(
                    "STEP 4", "FALLBACK — NEUTRAL — SKIP",
                    [
                        ("Fusion dir", fused_dir),
                        ("Decision",  "SKIP — neutral/unknown direction"),
                    ],
                    is_simulation=is_simulation,
                )
                if feature_vector is not None:
                    slug = (
                        self.all_btc_instruments[self.current_instrument_index]["slug"]
                        if self.current_instrument_index >= 0
                        else "unknown"
                    )
                    self.ml_engine.record_trade(
                        market_slug=slug,
                        poly_price=poly_price,
                        feature_vector=feature_vector,
                    )
                self._finish_decision_cycle(is_simulation, "SKIP — neutral fusion direction")
                return

        # ── Per-market direction lock + anti-chase guard ────────────────
        # Resolve the market slug once for the gates below.
        active_slug = (
            self.all_btc_instruments[self.current_instrument_index]["slug"]
            if 0 <= self.current_instrument_index < len(self.all_btc_instruments)
            else ""
        )

        # Held-token price for this entry: LONG buys YES at poly_price,
        # SHORT buys NO at 1 - poly_price.
        held_entry_price = poly_price if direction == "long" else (1.0 - poly_price)

        # 1. Direction lock — once the first trade on this market is in,
        #    refuse opposite-direction entries on the same market. This
        #    blocks the "win then fade" pattern that was burning every
        #    same-market opposite-side re-entry observed in the logs.
        if active_slug and self._lock_market_direction:
            locked_dir = self._market_direction.get(active_slug)
            if locked_dir and locked_dir != direction:
                self._log_step(
                    "STEP 5", "EXECUTION GATE — DIRECTION LOCK",
                    [
                        ("Market",   active_slug),
                        ("Locked to", locked_dir.upper()),
                        ("Requested", direction.upper()),
                        ("Decision", "SKIP — opposite direction blocked on this market"),
                    ],
                    is_simulation=is_simulation,
                )
                if feature_vector is not None:
                    self.ml_engine.record_trade(
                        market_slug=active_slug,
                        poly_price=poly_price,
                        feature_vector=feature_vector,
                    )
                self._finish_decision_cycle(is_simulation, "SKIP — direction lock")
                return

        # 2. Anti-chase guard — refuse re-entry on the same market at a
        #    price more than MAX_CHASE_DELTA cents WORSE than the last
        #    entry on this market. Prevents climbing into a runaway move.
        if active_slug and self._max_chase_delta > 0:
            last_held = self._market_last_entry_held_price.get(active_slug)
            if last_held is not None:
                # Worse = paying more for the same exposure.
                delta = held_entry_price - last_held
                if delta > self._max_chase_delta:
                    self._log_step(
                        "STEP 5", "EXECUTION GATE — ANTI-CHASE",
                        [
                            ("Direction",  direction.upper()),
                            ("Entry price", f"{held_entry_price:.4f}"),
                            ("Last entry",  f"{last_held:.4f}"),
                            ("Chase delta", f"{delta:+.4f}  >  max {self._max_chase_delta:.2f}"),
                            ("Decision",   "SKIP — price chased too far"),
                        ],
                        is_simulation=is_simulation,
                    )
                    if feature_vector is not None:
                        self.ml_engine.record_trade(
                            market_slug=active_slug,
                            poly_price=poly_price,
                            feature_vector=feature_vector,
                        )
                    self._finish_decision_cycle(is_simulation, "SKIP — anti-chase")
                    return

        # 3. 逆势共识门槛 — 低价弱方是对市场共识的对赌，需要更广的信号
        #    同向支持（配置见 __init__；ML 激活后 fused 为 None 时不适用）。
        if (
            fused is not None
            and held_entry_price < self._contrarian_price_ceiling
            and int(getattr(fused, "num_signals", 0) or 0) < self._contrarian_min_signals
        ):
            n_sig = int(getattr(fused, "num_signals", 0) or 0)
            self._log_step(
                "STEP 5", "EXECUTION GATE — CONTRARIAN CONSENSUS",
                [
                    ("Entry price", f"{held_entry_price:.4f}  <  {self._contrarian_price_ceiling:.2f}"),
                    ("Signals",     f"{n_sig}  <  需 {self._contrarian_min_signals}"),
                    ("Decision",    "SKIP — 逆势单信号共识不足"),
                ],
                is_simulation=is_simulation,
            )
            if feature_vector is not None:
                self.ml_engine.record_trade(
                    market_slug=active_slug,
                    poly_price=poly_price,
                    feature_vector=feature_vector,
                )
            self._finish_decision_cycle(is_simulation, "SKIP — contrarian consensus")
            return

        # Risk engine
        is_valid, error = self.risk_engine.validate_new_position(
            size=POSITION_SIZE_USD,
            direction=direction,
            current_price=current_price,
        )
        if not is_valid:
            self._log_step(
                "STEP 5", "EXECUTION GATE — RISK ENGINE",
                [
                    ("Decision", "BLOCKED"),
                    ("Reason",   str(error)),
                ],
                is_simulation=is_simulation,
                level="warning",
            )
            self._finish_decision_cycle(is_simulation, "BLOCKED — risk engine")
            return

        # Liquidity guard
        last_tick = getattr(self, "_last_bid_ask", None)
        if last_tick:
            last_bid, last_ask = last_tick
            MIN_LIQ = Decimal("0.02")
            if direction == "long" and last_ask <= MIN_LIQ:
                self._log_step(
                    "STEP 5", "EXECUTION GATE — LIQUIDITY",
                    [
                        ("Side",     "LONG — need ask"),
                        ("Ask",      f"{float(last_ask):.4f}"),
                        ("Min liq",  f"{float(MIN_LIQ):.4f}"),
                        ("Retry in", f"{self._filter_reject_cooldown_sec}s"),
                    ],
                    is_simulation=is_simulation,
                    level="warning",
                )
                _backoff_after_filter_reject()
                self._finish_decision_cycle(is_simulation, "SKIP — no ask liquidity")
                return
            if direction == "short" and last_bid <= MIN_LIQ:
                self._log_step(
                    "STEP 5", "EXECUTION GATE — LIQUIDITY",
                    [
                        ("Side",     "SHORT — need bid"),
                        ("Bid",      f"{float(last_bid):.4f}"),
                        ("Min liq",  f"{float(MIN_LIQ):.4f}"),
                        ("Retry in", f"{self._filter_reject_cooldown_sec}s"),
                    ],
                    is_simulation=is_simulation,
                    level="warning",
                )
                _backoff_after_filter_reject()
                self._finish_decision_cycle(is_simulation, "SKIP — no bid liquidity")
                return

        # STEP 5 — persist features + register settlement before order
        trade_id: Optional[int] = None
        step5_lines: List[tuple] = [
            ("Direction lock", "PASS"),
            ("Anti-chase",     "PASS"),
            ("Risk engine",    "PASS"),
            ("Liquidity",      "PASS"),
        ]
        if feature_vector is not None:
            slug = (
                self.all_btc_instruments[self.current_instrument_index]["slug"]
                if self.current_instrument_index >= 0
                else "unknown"
            )
            trade_id = self.ml_engine.record_trade(
                market_slug=slug,
                poly_price=poly_price,
                feature_vector=feature_vector,
            )
            step5_lines.append(("ML features", f"saved  trade_id={trade_id}"))
        else:
            step5_lines.append(("ML features", "skipped — no feature vector"))

        settlement_line = "not registered"
        if trade_id is not None and self.current_instrument_index >= 0:
            market_info = self.all_btc_instruments[self.current_instrument_index]
            self.settlement_tracker.register_trade(
                trade_id=trade_id,
                market_slug=market_info["slug"],
                market_start_ts=market_info["market_timestamp"],
                market_end_ts=market_info["end_timestamp"],
                direction=direction,
                poly_price=poly_price,
            )
            settlement_line = (
                f"registered  trade_id={trade_id}  "
                f"closes {market_info['end_time'].strftime('%H:%M:%S')} UTC"
            )
        step5_lines.append(("Settlement", settlement_line))
        self._log_step(
            "STEP 5", "PRE-EXECUTION — ALL GATES PASSED",
            step5_lines,
            is_simulation=is_simulation,
            level="success",
        )

        signal_for_logging = fused if fused is not None else _make_stub_signal(direction, ml_p_up)

        if self._stopping:
            self._finish_decision_cycle(is_simulation, "STOPPED — shutdown requested")
            return

        # Record direction lock + last-entry held price + count BEFORE
        # the actual order goes out, so any concurrent tick that fires
        # during submission cannot bypass these gates.
        if active_slug:
            self._market_direction.setdefault(active_slug, direction)
            self._market_last_entry_held_price[active_slug] = held_entry_price
            self._market_trade_count[active_slug] = (
                self._market_trade_count.get(active_slug, 0) + 1
            )

        self._publish_order_metrics(
            direction=direction,
            size_usd=float(POSITION_SIZE_USD),
            poly_price=poly_price,
            held_entry_price=held_entry_price,
            signal=signal_for_logging,
            ml_p_up=ml_p_up,
            ml_edge=bet_edge if ml_p_up is not None else 0.0,
            metadata=flat_metadata,
            market_slug=active_slug,
            is_simulation=is_simulation,
            fused=fused,
        )

        if is_simulation:
            await self._record_paper_trade(
                signal_for_logging, POSITION_SIZE_USD, current_price, direction,
                ml_p_up=ml_p_up if ml_p_up is not None else 0.0,
                ml_edge=bet_edge if ml_p_up is not None else 0.0,
                metadata=flat_metadata,
                ml_trade_id=trade_id,
            )
            self._finish_decision_cycle(is_simulation, "TRADE OPENED (simulation)")
        else:
            await self._place_real_order(
                signal_for_logging,
                POSITION_SIZE_USD,
                current_price,
                direction,
                ml_trade_id=trade_id,
            )
            self._finish_decision_cycle(is_simulation, "ORDER SUBMITTED (live)")

    # ── Paper trading ─────────────────────────────────────────────────────────

    async def _record_paper_trade(
        self,
        signal,
        position_size,
        current_price: Decimal,
        direction: str,
        ml_p_up: float = 0.0,
        ml_edge: float = 0.0,
        metadata: dict = None,
        ml_trade_id: Optional[int] = None,
    ) -> None:
        """Open a simulated position using the same economics as live trading.

        Entry fills at the ask (LONG → YES ask, SHORT → NO ask ≈ 1 − YES bid).
        Exits are managed by ``_check_position_exits`` (TP / SL / TIME-EXIT)
        with simulated market sells at the bid. Positions still open at market
        end settle through ``_settle_open_positions`` — same path as live.
        """
        if metadata is None:
            metadata = {}

        now = datetime.now(timezone.utc)

        slug = ""
        market_end_ts = 0
        if 0 <= self.current_instrument_index < len(self.all_btc_instruments):
            market = self.all_btc_instruments[self.current_instrument_index]
            slug = market.get("slug", "")
            try:
                market_end_ts = int(market.get("end_timestamp") or 0)
            except (TypeError, ValueError):
                market_end_ts = 0

        trade_instrument_id = self._held_trade_instrument(direction)
        if trade_instrument_id is None:
            logger.warning("NO token instrument not found — cannot simulate SHORT. Skipping.")
            return

        poly_price = float(current_price)
        last_tick = getattr(self, "_last_bid_ask", None)
        if last_tick:
            bid_dec, ask_dec = last_tick
        else:
            bid_dec = ask_dec = current_price

        fill_price = self._simulated_entry_price(direction, bid_dec, ask_dec, current_price)

        # 成交价复检入场带：决策通过与实际成交之间市场可能已大幅移动
        #（2026-07-26 实例：SHORT 决策时 mid 0.40 在带内，30 秒暴跌后
        # 成交在 $0.81，穿透 [0.25, 0.75] 带）。带外直接放弃本次开仓。
        fill_f = float(fill_price)
        if not (self._min_entry_price <= fill_f <= self._max_entry_price):
            logger.warning(
                f"放弃纸面开仓：成交价 ${fill_f:.4f} 超出入场带 "
                f"[{self._min_entry_price:.2f}, {self._max_entry_price:.2f}]（快市穿透）"
            )
            return
        size_usd = float(position_size)
        try:
            max_usd = max(0.01, float(os.getenv("MARKET_BUY_USD", str(size_usd))))
        except (TypeError, ValueError):
            max_usd = size_usd
        size_usd = max_usd
        fill_qty = size_usd / float(fill_price) if float(fill_price) > 0 else 0.0

        trade_id = f"paper_{int(now.timestamp() * 1000)}"
        session_num = len(self.paper_trades) + 1
        entry_spot = float(metadata.get("spot_price", 0.0) or 0.0)

        num_signals = signal.num_signals if hasattr(signal, "num_signals") else 1
        signal_sources = (
            [s.source for s in signal.contributing_signals]
            if hasattr(signal, "contributing_signals") and signal.contributing_signals
            else [str(signal.direction).replace("SignalDirection.", "")]
        )

        label = "YES (UP)" if direction == "long" else "NO (DOWN)"

        paper_trade = PaperTrade(
            trade_id=trade_id,
            timestamp=now,
            direction=direction.upper(),
            size_usd=size_usd,
            entry_price=float(fill_price),
            exit_price=float(fill_price),
            pnl_usd=0.0,
            pnl_pct=0.0,
            outcome="PENDING",
            signal_score=signal.score,
            signal_confidence=signal.confidence,
            num_signals=num_signals,
            ml_p_up=ml_p_up,
            ml_edge=ml_edge,
            market_slug=slug,
            btc_spot_price=entry_spot,
            vol_regime=str(metadata.get("vol_regime", "") or ""),
            funding_rate=float(metadata.get("funding_rate", 0.0) or 0.0),
            filled_qty=fill_qty,
            close_reason="",
            ml_trade_id=ml_trade_id,
            session_trade_num=session_num,
        )
        self.paper_trades.append(paper_trade)
        self._record_order_submitted(
            mode="paper",
            order_id=trade_id,
            role="entry",
            market_slug=slug,
            direction=direction,
            requested_usd=size_usd,
            requested_qty=fill_qty,
        )
        self._record_order_fill(
            mode="paper",
            order_id=trade_id,
            role="entry",
            market_slug=slug,
            direction=direction,
            filled_qty=fill_qty,
            filled_notional_usd=size_usd,
        )

        try:
            self.risk_engine.add_position(
                position_id=trade_id,
                size=Decimal(str(size_usd)),
                entry_price=fill_price,
                direction=direction,
                long_token=True,
            )
        except Exception as e:
            logger.warning(f"Failed to record paper fill in risk engine: {e}")

        stop_loss, take_profit, sl_enabled = self._compute_exit_levels(fill_price)

        position = {
            "instrument_id": trade_instrument_id,
            "direction": direction,
            "label": label,
            "size_usd": size_usd,
            "entry_price": fill_price,
            "filled_qty": Decimal(str(fill_qty)),
            "stop_loss": stop_loss,
            "stop_loss_enabled": sl_enabled,
            "take_profit": take_profit,
            "market_end_ts": market_end_ts,
            "market_slug": slug,
            "ml_trade_id": ml_trade_id,
            "signal_score": float(signal.score),
            "signal_confidence": float(signal.confidence),
            "exit_in_flight": False,
            "exit_order_id": None,
            "opened_at": now,
            "last_bid": None,
            "last_bid_ts": None,
            "last_bid_post_settle": None,
            "last_bid_post_settle_ts": None,
            "is_paper": True,
            "paper_trade": paper_trade,
            "signal_sources": signal_sources,
            "ml_p_up": ml_p_up,
            "ml_edge": ml_edge,
            "num_signals": num_signals,
            "entry_spot": entry_spot,
        }
        self._open_positions[trade_id] = position

        if self.grafana_exporter:
            try:
                self.grafana_exporter.increment_order_counter("filled")
                self.grafana_exporter.record_dashboard_event(
                    "paper_fill",
                    "模拟买入成交",
                    trade_id=trade_id,
                    market_slug=slug,
                    side="BUY",
                    direction=direction.upper(),
                    qty_tokens=fill_qty,
                    fill_price=float(fill_price),
                    notional_usd=size_usd,
                    is_simulation=True,
                )
            except Exception:
                pass
        self._publish_dashboard_state()

        if trade_instrument_id != self.instrument_id:
            try:
                self.subscribe_quote_ticks(trade_instrument_id)
            except Exception as e:
                logger.warning(
                    f"Could not subscribe to {trade_instrument_id} for paper "
                    f"exit monitoring: {e}"
                )

        end_str = (
            datetime.fromtimestamp(market_end_ts, tz=timezone.utc).strftime("%H:%M:%S")
            if market_end_ts else "unknown"
        )
        dir_arrow = "▲" if direction == "long" else "▼"
        risk_str = (
            f"SL=-{self._stop_loss_frac:.0%}  " if self._stop_loss_enabled else "SL=DISABLED  "
        ) + f"TP=+{self._take_profit_frac:.0%}  exit_cutoff={self._exit_cutoff_seconds}s"
        self._log_step(
            "STEP 6", f"[SIM] TRADE #{session_num} OPENED — {dir_arrow} {direction.upper()}",
            [
                ("Market",     f"{slug or 'unknown'}"),
                ("Settles at", f"{end_str} UTC"),
                ("Outcome",    label),
                ("Entry fill", f"${float(fill_price):.4f}  (poly mid={poly_price:.4f})"),
                ("Shares / notional", f"{fill_qty:.4f} 股  =  ${size_usd:.2f}"),
                ("BTC spot",   f"${entry_spot:,.0f}"),
                ("ML p(UP)",   f"{ml_p_up:.4f}  edge={ml_edge:.4f}"),
                ("Signal",     f"score={signal.score:.1f}  conf={signal.confidence:.2%}  n={num_signals}"),
                ("Sources",    ", ".join(signal_sources) or "n/a"),
                ("Risk",       risk_str),
                ("Status",     "OPEN — same exit rules as live"),
            ],
            is_simulation=True,
            level="success",
        )
        tui_event(
            "ORDER",
            f"{dir_arrow} {direction.upper()} ${size_usd:.2f}",
            slug="S5",
            activity=True,
        )

        self._save_paper_trades([paper_trade])

    def _close_paper_position(
        self,
        entry_id: str,
        position: dict,
        exit_price: Decimal,
        close_reason: str,
    ) -> None:
        """Record a closed paper position — mirrors ``_close_live_position``."""
        try:
            self.risk_engine.remove_position(entry_id, exit_price=exit_price)
        except Exception as e:
            logger.warning(f"Failed to remove paper position from risk engine: {e}")

        pt: PaperTrade = position["paper_trade"]
        entry_price_dec = (
            position["entry_price"]
            if isinstance(position["entry_price"], Decimal)
            else Decimal(str(position["entry_price"]))
        )
        qty_dec = (
            position["filled_qty"]
            if isinstance(position["filled_qty"], Decimal)
            else Decimal(str(position["filled_qty"]))
        )

        entry_price_f = float(entry_price_dec)
        exit_price_f = float(exit_price)
        qty_f = float(qty_dec)

        realized = qty_f * (exit_price_f - entry_price_f)
        pnl_pct = (exit_price_f - entry_price_f) / entry_price_f if entry_price_f > 0 else 0.0

        if realized > 1e-6:
            outcome = "WIN"
        elif realized < -1e-6:
            outcome = "LOSS"
        else:
            outcome = "BREAKEVEN"
        if close_reason == "SETTLEMENT_UNRESOLVED":
            outcome = "UNRESOLVED"

        now = datetime.now(timezone.utc)
        opened_at = position.get("opened_at") or pt.timestamp

        pt.exit_price = exit_price_f
        pt.pnl_usd = realized
        pt.pnl_pct = pnl_pct
        pt.outcome = outcome
        pt.close_reason = close_reason
        pt.closed_at = now

        try:
            self.performance_tracker.record_trade(
                trade_id=pt.trade_id,
                direction=position["direction"],
                entry_price=entry_price_dec,
                exit_price=exit_price,
                size=Decimal(str(position["size_usd"])),
                entry_time=opened_at,
                exit_time=now,
                signal_score=position.get("signal_score", 0.0),
                signal_confidence=position.get("signal_confidence", 0.0),
                metadata={
                    "simulated": True,
                    "long_token": True,
                    "num_signals": position.get("num_signals", 1),
                    "fusion_score": position.get("signal_score", 0.0),
                    "ml_p_up": position.get("ml_p_up", 0.0),
                    "ml_edge": position.get("ml_edge", 0.0),
                    "market_slug": position.get("market_slug", ""),
                    "close_reason": close_reason,
                    "signal_sources": position.get("signal_sources", []),
                },
            )
        except Exception as e:
            logger.warning(f"Failed to record paper trade in PerformanceTracker: {e}")

        if hasattr(self, "grafana_exporter") and self.grafana_exporter and outcome in ("WIN", "LOSS"):
            try:
                self.grafana_exporter.increment_trade_counter(won=(outcome == "WIN"))
                self.grafana_exporter.record_trade_duration((now - opened_at).total_seconds())
            except Exception:
                pass

        if self.grafana_exporter:
            try:
                self.grafana_exporter.record_dashboard_event(
                    "paper_exit_filled",
                    "模拟卖出成交",
                    trade_id=pt.trade_id,
                    market_slug=pt.market_slug,
                    side="SELL",
                    direction=position.get("direction", ""),
                    qty_tokens=qty_f,
                    fill_price=exit_price_f,
                    entry_price=entry_price_f,
                    notional_usd=qty_f * exit_price_f,
                    realized_pnl=realized,
                    close_reason=close_reason,
                    outcome=outcome,
                    is_simulation=True,
                )
            except Exception:
                pass
        self._publish_dashboard_state()

        settled = [t for t in self.paper_trades if t.outcome in ("WIN", "LOSS")]
        wins = sum(1 for t in settled if t.outcome == "WIN")
        win_rate = wins / len(settled) if settled else 0.0
        total_pnl = sum(
            t.pnl_usd for t in self.paper_trades if t.outcome in ("WIN", "LOSS", "BREAKEVEN")
        )

        outcome_icon = {
            "WIN":        "✔ WIN",
            "LOSS":       "✘ LOSS",
            "BREAKEVEN":  "= BREAKEVEN",
            "UNRESOLVED": "? UNRESOLVED",
        }.get(outcome, outcome)
        outcome_level = {"WIN": "success", "LOSS": "warning"}.get(outcome, "info")
        dir_arrow = "▲" if pt.direction == "LONG" else "▼"
        hold_secs = (now - opened_at).total_seconds()
        hold_str = f"{hold_secs:.0f}s" if hold_secs < 90 else f"{hold_secs/60:.1f}m"

        reason_label = {
            "EXIT_TP": "TAKE-PROFIT",
            "EXIT_STOP": "STOP-LOSS",
            "TIME-EXIT": "TIME-EXIT",
            "SETTLEMENT": "SETTLEMENT",
            "SETTLEMENT_FALLBACK": "SETTLEMENT (fallback bid)",
            "SETTLEMENT_UNRESOLVED": "SETTLEMENT (unresolved)",
        }.get(close_reason, close_reason)

        self._log_step(
            "SETTLE", f"[SIM] TRADE #{pt.session_trade_num} CLOSED — {outcome_icon}",
            [
                ("Direction",   f"{dir_arrow} {pt.direction}"),
                ("Market",      pt.market_slug or "unknown"),
                ("Close reason", reason_label),
                ("Share price", f"${entry_price_f:.4f}  →  ${exit_price_f:.4f}"),
                ("Shares",      f"{qty_f:.4f} 股"),
                ("Hold",        hold_str),
                ("P&L",         f"${realized:+.4f}  ({pnl_pct * 100:+.2f}%)"),
                ("", ""),
                ("Session",     f"{wins}/{len(settled)} wins  ({win_rate:.1%})"),
                ("Cumul P&L",   f"${total_pnl:+.4f}"),
                ("Open trades", str(len(self._open_positions))),
            ],
            is_simulation=True,
            level=outcome_level,
        )

        self._save_paper_trades([pt])

    def _simulate_paper_exit(
        self,
        entry_id: str,
        position: dict,
        bid: Decimal,
        reason: str,
    ) -> None:
        """Simulate a market SELL at the current bid — same triggers as live."""
        if position.get("exit_in_flight"):
            return

        exit_price = max(Decimal("0.01"), bid)
        position["exit_in_flight"] = True

        reason_map = {
            "STOP-LOSS": "EXIT_STOP",
            "TAKE-PROFIT": "EXIT_TP",
            "TIME-EXIT": "TIME-EXIT",
        }
        close_reason = reason_map.get(reason, "EXIT_MANUAL")

        exit_id = f"PAPER-EXIT-{entry_id}-{int(time.time() * 1000)}"
        qty_float = float(position.get("filled_qty", 0.0) or 0.0)
        self._record_order_submitted(
            mode="paper",
            order_id=exit_id,
            role="exit",
            market_slug=str(position.get("market_slug", "")),
            direction=str(position.get("direction", "")),
            requested_qty=qty_float,
        )
        self._record_order_fill(
            mode="paper",
            order_id=exit_id,
            role="exit",
            market_slug=str(position.get("market_slug", "")),
            direction=str(position.get("direction", "")),
            filled_qty=qty_float,
            filled_notional_usd=qty_float * float(exit_price),
        )

        self._open_positions.pop(entry_id, None)
        self._close_paper_position(entry_id, position, exit_price, close_reason)

    def _save_paper_trades(
        self, trades: Optional[List[PaperTrade]] = None
    ) -> None:
        try:
            selected = self.paper_trades if trades is None else trades
            self.trade_history_repository.upsert(
                "paper", [trade.to_dict() for trade in selected]
            )
        except Exception as e:
            logger.error(f"无法将模拟交易写入 MySQL: {e}")

    # ── Real order ────────────────────────────────────────────────────────────

    async def _place_real_order(
        self,
        signal,
        position_size,
        current_price: Decimal,
        direction: str,
        ml_trade_id: Optional[int] = None,
    ) -> None:
        if not self.instrument_id:
            logger.error("No instrument available — instrument cache not yet loaded")
            return

        try:
            side = OrderSide.BUY

            if direction == "long":
                trade_instrument_id = getattr(self, "_yes_instrument_id", None) or self.instrument_id
                trade_label = "YES (UP)"
            else:
                no_id = getattr(self, "_no_instrument_id", None)
                if no_id is None:
                    logger.warning("NO token instrument not found — cannot bet DOWN. Skipping.")
                    return
                trade_instrument_id = no_id
                trade_label = "NO (DOWN)"

            instrument = self.cache.instrument(trade_instrument_id)
            if not instrument:
                logger.error(f"Instrument not in cache: {trade_instrument_id}")
                return

            # Resolve the USD amount once and use it as the order quantity
            # below. Polymarket / Nautilus V2 expect BUY market orders to be
            # quote-denominated: ``amount`` is USD to spend, not tokens.
            try:
                max_usd_amount = max(
                    0.01,
                    float(os.getenv("MARKET_BUY_USD", str(float(position_size)))),
                )
            except (TypeError, ValueError):
                max_usd_amount = float(position_size)

            # microUSDC precision — matches the instrument's size_increment
            # and Polymarket's collateral granularity.
            usd_precision = max(instrument.size_precision, 6)
            usd_qty = Quantity(round(max_usd_amount, usd_precision), precision=usd_precision)

            timestamp_ms = int(time.time() * 1000)
            # Plain alphanumeric/dash id only — some venues reject special chars.
            usd_label = str(int(round(max_usd_amount * 100))).zfill(4)
            unique_id = f"BTC-15M-{usd_label}-{timestamp_ms}"

            market_end_ts = 0
            market_slug = ""
            if 0 <= self.current_instrument_index < len(self.all_btc_instruments):
                cur = self.all_btc_instruments[self.current_instrument_index]
                market_end_ts = int(cur.get("end_timestamp", 0))
                market_slug = str(cur.get("slug", ""))

            signal_score = float(getattr(signal, "score", 0.0) or 0.0)
            signal_conf = float(getattr(signal, "confidence", 0.0) or 0.0)

            # 必须先登记再提交。IOC 市价单可能立即成交，如果反过来写，
            # on_order_filled 会找不到订单元数据，从而无法创建对应持仓。
            self._pending_orders[unique_id] = {
                "side": "BUY",
                "instrument_id": trade_instrument_id,
                "direction": direction,
                "size_usd": max_usd_amount,
                "expected_price": float(current_price),
                "ref_price": float(current_price),
                "label": trade_label,
                "market_end_ts": market_end_ts,
                "market_slug": market_slug,
                "ml_trade_id": ml_trade_id,
                "signal_score": signal_score,
                "signal_confidence": signal_conf,
                "stop_loss_frac": self._stop_loss_frac,
                "stop_loss_enabled": self._stop_loss_enabled,
                "take_profit_frac": self._take_profit_frac,
                "submitted_at": datetime.now(timezone.utc),
            }

            order = self.order_factory.market(
                instrument_id=trade_instrument_id,
                order_side=side,
                quantity=usd_qty,
                client_order_id=ClientOrderId(unique_id),
                quote_quantity=True,
                time_in_force=TimeInForce.IOC,
            )

            self.submit_order(order)
            self._record_order_submitted(
                mode="live",
                order_id=unique_id,
                role="entry",
                market_slug=market_slug,
                direction=direction,
                requested_usd=max_usd_amount,
            )
            self._track_order_event("placed")
            if self.grafana_exporter:
                self.grafana_exporter.record_dashboard_event(
                    "order_submitted",
                    "实盘买入委托已提交",
                    client_id=unique_id,
                    market_slug=market_slug,
                    side="BUY",
                    direction=direction.upper(),
                    requested_usd=max_usd_amount,
                    reference_price=float(current_price),
                    is_simulation=False,
                )

            # Subscribe to ticks for the held token *before* the fill arrives so
            # the live stop-loss handler starts seeing prices immediately.
            if trade_instrument_id != self.instrument_id:
                try:
                    self.subscribe_quote_ticks(trade_instrument_id)
                except Exception as e:
                    logger.warning(
                        f"Could not subscribe to {trade_instrument_id} for exit "
                        f"monitoring: {e}"
                    )

            # ── Build highlighted banner ─────────────────────────────────────
            # Pull a snapshot of signal + market context so the user sees
            # *why* the bot fired this trade, not just the order itself.
            signal_score_v = float(getattr(signal, "score", 0.0) or 0.0)
            signal_conf_v = float(getattr(signal, "confidence", 0.0) or 0.0)
            num_signals_v = (
                int(getattr(signal, "num_signals", 0) or 0)
                if hasattr(signal, "num_signals")
                else 0
            )
            sig_dir_v = str(getattr(signal, "direction", "")).replace(
                "SignalDirection.", ""
            )
            contributing = []
            if hasattr(signal, "contributing_signals") and signal.contributing_signals:
                try:
                    contributing = sorted({s.source for s in signal.contributing_signals})
                except Exception:
                    contributing = []

            market_slug_v = ""
            mins_left_v: Optional[float] = None
            if 0 <= self.current_instrument_index < len(self.all_btc_instruments):
                cur = self.all_btc_instruments[self.current_instrument_index]
                market_slug_v = str(cur.get("slug", ""))
                end_ts = int(cur.get("end_timestamp", 0))
                if end_ts:
                    mins_left_v = (end_ts - time.time()) / 60.0

            if self._last_bid_ask:
                bid_v, ask_v = self._last_bid_ask
                quote_v = (
                    f"bid=${float(bid_v):.4f}  ask=${float(ask_v):.4f}  "
                    f"mid=${float((bid_v + ask_v) / 2):.4f}"
                )
            else:
                quote_v = "n/a"

            ml_engine_state = (
                "active" if getattr(self.ml_engine, "active", False) else "inactive"
            )
            session_total = (
                len(self.live_trades) if hasattr(self, "live_trades") else 0
            )
            session_pnl = (
                sum(t.pnl_usd for t in self.live_trades)
                if hasattr(self, "live_trades")
                else 0.0
            )

            dir_arrow_live = "▲" if direction == "long" else "▼"
            risk_str = (
                f"SL=-{self._stop_loss_frac:.0%}  " if self._stop_loss_enabled else "SL=DISABLED  "
            ) + f"TP=+{self._take_profit_frac:.0%}  exit_cutoff={self._exit_cutoff_seconds}s"
            self._log_step(
                "STEP 6",
                f"[LIVE] ORDER PLACED — {dir_arrow_live} BUY {trade_label}  ${max_usd_amount:.2f}",
                [
                    ("Order ID",   unique_id),
                    ("Market",     market_slug_v or "(unknown)"),
                    ("Token",      str(trade_instrument_id)),
                    ("", ""),
                    ("Side",       f"BUY  ({trade_label})"),
                    ("Notional",   f"${max_usd_amount:.2f} USD  (IOC)"),
                    ("Ref price",  f"${float(current_price):.4f}"),
                    ("Quote",      quote_v),
                    ("Mkt close",  f"{mins_left_v:+.1f} min" if mins_left_v is not None else "n/a"),
                    ("", ""),
                    ("Signal",     f"{sig_dir_v}  score={signal_score_v:.1f}  conf={signal_conf_v:.1%}  n={num_signals_v}"),
                    ("Sources",    ", ".join(contributing) if contributing else "(fallback)"),
                    ("ML engine",  ml_engine_state),
                    ("Risk",       risk_str),
                    ("", ""),
                    ("Session",    f"trades={session_total}  pnl=${session_pnl:+.4f}  open={len(self._open_positions)}"),
                ],
                is_simulation=False,
                level="warning",
            )
            tui_event(
                "ORDER",
                f"{dir_arrow_live} {direction.upper()} ${max_usd_amount:.2f}",
                slug="S5",
                activity=True,
            )

            # NOTE: do not call _track_order_event("placed") again here —
            # we already counted it right after submit_order() above. The
            # old code double-counted every placed order.

        except Exception as e:
            if "unique_id" in locals():
                self._pending_orders.pop(unique_id, None)
            logger.error(f"Error placing real order: {e}")
            import traceback
            traceback.print_exc()
            self._track_order_event("rejected", reason=str(e)[:160])

    # ── Signal processing ─────────────────────────────────────────────────────

    def _process_signals(self, current_price: Decimal, metadata: dict = None) -> list:
        signals = []
        if metadata is None:
            metadata = {}

        proc_meta = {
            k: Decimal(str(v)) if isinstance(v, float) else v
            for k, v in metadata.items()
        }

        spike_signal = self.spike_detector.process(current_price, self.price_history, proc_meta)
        if spike_signal:
            signals.append(spike_signal)

        if "sentiment_score" in proc_meta:
            s = self.sentiment_processor.process(current_price, self.price_history, proc_meta)
            if s:
                signals.append(s)

        if "spot_price" in proc_meta:
            s = self.divergence_processor.process(current_price, self.price_history, proc_meta)
            if s:
                signals.append(s)

        if proc_meta.get("yes_token_id"):
            s = self.orderbook_processor.process(current_price, self.price_history, proc_meta)
            if s:
                signals.append(s)

        if proc_meta.get("tick_buffer"):
            s = self.tick_velocity_processor.process(current_price, self.price_history, proc_meta)
            if s:
                signals.append(s)

        pcr_signal = self.deribit_pcr_processor.process(current_price, self.price_history, proc_meta)
        if pcr_signal:
            signals.append(pcr_signal)

        liq_signal = self.liquidation_processor.process(current_price, self.price_history, proc_meta)
        if liq_signal:
            signals.append(liq_signal)

        fi_signal = self.funding_oi_processor.process(current_price, self.price_history, proc_meta)
        if fi_signal:
            signals.append(fi_signal)

        cvd_signal = self.cvd_ob_processor.process(current_price, self.price_history, proc_meta)
        if cvd_signal:
            signals.append(cvd_signal)

        ohlcv_signal = self.ohlcv_momentum_processor.process(current_price, self.price_history, proc_meta)
        if ohlcv_signal:
            signals.append(ohlcv_signal)

        return signals

    # ── Order events ──────────────────────────────────────────────────────────

    def _track_order_event(
        self, event_type: str, *, reason: str = "", client_id: str = ""
    ) -> None:
        try:
            pt = self.performance_tracker
            if hasattr(pt, "record_order_event"):
                pt.record_order_event(event_type)
            elif hasattr(pt, "increment_counter"):
                pt.increment_counter(event_type)
            elif hasattr(pt, "increment_order_counter"):
                pt.increment_order_counter(event_type)
            else:
                logger.debug(f"PerformanceTracker: no order-counter method for '{event_type}'")
        except Exception as e:
            logger.warning(f"Failed to track order event '{event_type}': {e}")
        try:
            if self.grafana_exporter:
                if event_type in ("filled", "rejected"):
                    self.grafana_exporter.increment_order_counter(event_type)
                if event_type in ("rejected", "denied") and hasattr(self.grafana_exporter, "record_dashboard_event"):
                    # 带上拒单原因与订单号，否则驾驶舱日志只剩一行光秃秃的"订单被拒绝"
                    self.grafana_exporter.record_dashboard_event(
                        f"order_{event_type}",
                        f"Order {event_type}",
                        reason=reason or "",
                        client_id=client_id or "",
                    )
        except Exception:
            pass

    def on_order_filled(self, event) -> None:
        client_id = str(event.client_order_id)
        try:
            fill_price = Decimal(str(float(event.last_px)))
            fill_qty = Decimal(str(float(event.last_qty)))
        except Exception:
            fill_price = Decimal("0")
            fill_qty = Decimal("0")

        # Slippage / wait-time context relative to the entry submit, when we
        # have it (entry orders only — exits don't track ref price).
        is_exit = client_id in self._pending_exits
        pending_meta = self._pending_orders.get(client_id, {}) if not is_exit else {}
        exit_entry_id = self._pending_exits.get(client_id) if is_exit else None
        exit_position = self._open_positions.get(exit_entry_id, {}) if exit_entry_id else {}
        event_meta = exit_position if is_exit else pending_meta
        ref_px = float(pending_meta.get("ref_price", 0.0) or 0.0)
        slip_str = "n/a"
        if not is_exit and ref_px > 0 and fill_price > 0:
            slip_bps = (float(fill_price) - ref_px) / ref_px * 10_000
            slip_str = f"{slip_bps:+.1f} bps  (ref=${ref_px:.4f})"

        submitted_at = pending_meta.get("submitted_at")
        latency_str = "n/a"
        if submitted_at:
            try:
                latency_str = (
                    f"{(datetime.now(timezone.utc) - submitted_at).total_seconds():.2f}s"
                )
            except Exception:
                pass

        notional = float(fill_price) * float(fill_qty)
        self._record_order_fill(
            mode="live",
            order_id=client_id,
            role="exit" if is_exit else "entry",
            market_slug=str(event_meta.get("market_slug", "")),
            direction=str(event_meta.get("direction", "")),
            filled_qty=float(fill_qty),
            filled_notional_usd=notional,
        )

        self._log_event_banner(
            level="success" if not is_exit else "info",
            tag="ORDER FILLED" if not is_exit else "EXIT FILLED",
            title=(
                f"{'BUY' if not is_exit else 'SELL'}  "
                f"qty={float(fill_qty):.4f}  @ ${float(fill_price):.4f}  "
                f"= ${notional:.4f}"
            ),
            lines=[
                ("Order ID",  client_id),
                ("Fill px",   f"${float(fill_price):.4f}"),
                ("Fill shares", f"{float(fill_qty):.6f} 股"),
                ("Notional",  f"${notional:.4f}"),
                ("Slippage",  slip_str),
                ("Latency",   f"{latency_str}  (submit → fill)"),
            ],
        )
        self._track_order_event("filled")
        try:
            if self.grafana_exporter and hasattr(self.grafana_exporter, "record_dashboard_event"):
                self.grafana_exporter.record_dashboard_event(
                    "exit_filled" if is_exit else "order_filled",
                    "卖出成交" if is_exit else "买入成交",
                    client_id=client_id,
                    qty_tokens=float(fill_qty),
                    fill_price=float(fill_price),
                    notional_usd=notional,
                    side="SELL" if is_exit else "BUY",
                    direction=str(event_meta.get("direction", "")).upper(),
                    market_slug=str(event_meta.get("market_slug", "")),
                    entry_id=str(exit_entry_id or client_id),
                    is_simulation=False,
                )
        except Exception:
            pass

        # Branch on whether this fill closed an existing position (SELL) or
        # opened a new one (BUY).
        entry_id = self._pending_exits.pop(client_id, None)
        if entry_id is not None:
            self._handle_exit_fill(entry_id, client_id, fill_price)
            return

        pending = self._pending_orders.pop(client_id, None)
        if pending is not None:
            self._handle_entry_fill(client_id, pending, fill_price, fill_qty)

    def _handle_entry_fill(
        self,
        client_id: str,
        pending: dict,
        fill_price: Decimal,
        fill_qty: Decimal,
    ) -> None:
        """Track a freshly-opened position so the exit handler can monitor it."""
        try:
            self.risk_engine.add_position(
                position_id=client_id,
                size=Decimal(str(pending["size_usd"])),
                entry_price=fill_price,
                direction=pending["direction"],
                long_token=True,
            )
        except Exception as e:
            logger.warning(f"Failed to record live fill in risk engine: {e}")

        # Payoff-relative TP/SL — shared with paper simulation via
        # ``_compute_exit_levels``.
        # 插值启用时按真实成交价现算（下单时快照的固定值无法预知 fill
        # price）；未启用时沿用快照，保持"下单时配置固化"的原语义。
        sl_enabled = bool(pending.get("stop_loss_enabled", self._stop_loss_enabled))
        use_interp = self._sl_tp_endpoints is not None
        stop_loss, take_profit, sl_enabled = self._compute_exit_levels(
            fill_price,
            stop_loss_enabled=sl_enabled,
            stop_loss_frac=None if use_interp else pending.get("stop_loss_frac", self._stop_loss_frac),
            take_profit_frac=None if use_interp else pending.get("take_profit_frac", self._take_profit_frac),
        )
        if use_interp:
            sl_v, tp_v = interp_exit_fracs(
                float(fill_price),
                self._min_entry_price,
                self._max_entry_price,
                *self._sl_tp_endpoints,
            )
            sl_frac = Decimal(str(sl_v)) if sl_enabled else Decimal("0")
            tp_frac = Decimal(str(tp_v))
        else:
            sl_frac = Decimal(str(
                pending.get("stop_loss_frac",
                    pending.get("stop_loss_pct", self._stop_loss_frac))
            )) if sl_enabled else Decimal("0")
            tp_frac = Decimal(str(
                pending.get("take_profit_frac",
                    pending.get("take_profit_pct", self._take_profit_frac))
            ))

        position = {
            "instrument_id": pending["instrument_id"],
            "direction": pending["direction"],
            "label": pending.get("label", ""),
            "size_usd": float(pending["size_usd"]),
            "entry_price": fill_price,
            "filled_qty": fill_qty,
            "stop_loss": stop_loss,
            "stop_loss_enabled": sl_enabled,
            "take_profit": take_profit,
            "market_end_ts": int(pending.get("market_end_ts", 0)),
            "market_slug": pending.get("market_slug", ""),
            "ml_trade_id": pending.get("ml_trade_id"),
            "signal_score": float(pending.get("signal_score", 0.0) or 0.0),
            "signal_confidence": float(pending.get("signal_confidence", 0.0) or 0.0),
            "exit_in_flight": False,
            "exit_order_id": None,
            "opened_at": datetime.now(timezone.utc),
            # Latest bid seen for the held token; updated on every quote tick
            # by ``_check_position_exits``. Used as a settlement-price source
            # when the market auto-resolves (no manual exit).
            "last_bid": None,
            "last_bid_ts": None,
            "last_bid_post_settle": None,
            "last_bid_post_settle_ts": None,
        }
        self._open_positions[client_id] = position
        self._publish_dashboard_state()

        notional_usd = float(pending.get("size_usd", 0.0))
        market_slug_v = str(pending.get("market_slug", "") or "(unknown)")
        mkt_end_ts = int(pending.get("market_end_ts", 0) or 0)
        mins_left_v = (
            (mkt_end_ts - time.time()) / 60.0 if mkt_end_ts else None
        )

        self._log_event_banner(
            level="success",
            tag="POSITION OPEN",
            title=(
                f"{pending.get('label', '')}  qty={float(fill_qty):.4f}  "
                f"@ ${float(fill_price):.4f}"
            ),
            lines=[
                ("Entry ID",  client_id),
                ("Market",    market_slug_v),
                ("Direction", str(pending.get("direction", "")).upper()),
                ("Entry px",  f"${float(fill_price):.4f}"),
                ("Shares",    f"{float(fill_qty):.6f} 股"),
                ("Notional",  f"${notional_usd:.2f}"),
                ("", ""),
                (
                    "Stop-loss",
                    (
                        f"${float(stop_loss):.4f}  "
                        f"(-{float(sl_frac):.0%} of capital, "
                        f"${float(fill_price - stop_loss):.4f} below entry)"
                        if sl_enabled
                        else "DISABLED  (rides to TP or settlement)"
                    ),
                ),
                (
                    "Take-prof",
                    f"${float(take_profit):.4f}  "
                    f"(+{float(tp_frac):.0%} of upside, ${float(take_profit - fill_price):.4f} above entry)",
                ),
                (
                    "Mkt close",
                    f"{mins_left_v:+.1f} min" if mins_left_v is not None else "n/a",
                ),
                (
                    "Signal",
                    f"score={float(pending.get('signal_score', 0.0)):.1f}  "
                    f"conf={float(pending.get('signal_confidence', 0.0)):.1%}",
                ),
            ],
        )

    def _handle_exit_fill(
        self,
        entry_id: str,
        exit_id: str,
        fill_price: Decimal,
    ) -> None:
        """An exit (SELL) order filled — close out the underlying position."""
        position = self._open_positions.pop(entry_id, None)
        if position is None:
            logger.warning(f"Exit fill {exit_id} had no matching open position {entry_id}")
            return

        # Decide whether this exit was driven by stop-loss, take-profit, or a
        # generic mid-market sell. ``fill_price`` may differ slightly from the
        # trigger threshold (slippage), so use small tolerances.
        try:
            sl = position["stop_loss"]
            tp = position["take_profit"]
            if fill_price >= tp:
                close_reason = "EXIT_TP"
            elif fill_price <= sl:
                close_reason = "EXIT_STOP"
            else:
                close_reason = "EXIT_MANUAL"
        except Exception:
            close_reason = "EXIT_MANUAL"

        self._close_live_position(
            entry_id=entry_id,
            position=position,
            exit_price=fill_price,
            exit_order_id=exit_id,
            close_reason=close_reason,
        )

    # ── Live realised-P&L recording ─────────────────────────────────────────

    def _close_live_position(
        self,
        entry_id: str,
        position: dict,
        exit_price: Decimal,
        exit_order_id: Optional[str],
        close_reason: str,
    ) -> None:
        """Record a closed live position's realised P&L in every consumer.

        - Removes the position from the risk engine
        - Appends a ``LiveTrade`` to ``self.live_trades`` and persists it
        - Forwards the trade to the global PerformanceTracker so cumulative
          metrics (win rate, ROI, Sharpe, drawdown) include live results
        """
        try:
            self.risk_engine.remove_position(entry_id, exit_price=exit_price)
        except Exception as e:
            logger.warning(f"Failed to remove position from risk engine: {e}")

        entry_price_dec = position["entry_price"] if isinstance(position["entry_price"], Decimal) \
            else Decimal(str(position["entry_price"]))
        qty_dec = position["filled_qty"] if isinstance(position["filled_qty"], Decimal) \
            else Decimal(str(position["filled_qty"]))

        entry_price_f = float(entry_price_dec)
        exit_price_f = float(exit_price)
        qty_f = float(qty_dec)

        realized = qty_f * (exit_price_f - entry_price_f)
        pnl_pct = (exit_price_f - entry_price_f) / entry_price_f if entry_price_f > 0 else 0.0

        if realized > 1e-6:
            outcome = "WIN"
        elif realized < -1e-6:
            outcome = "LOSS"
        else:
            outcome = "BREAKEVEN"
        if close_reason == "SETTLEMENT_UNRESOLVED":
            outcome = "UNRESOLVED"

        self._live_session_num += 1
        opened_at = position.get("opened_at") or datetime.now(timezone.utc)
        closed_at = datetime.now(timezone.utc)

        live_trade = LiveTrade(
            trade_id=entry_id,
            ml_trade_id=position.get("ml_trade_id"),
            timestamp=opened_at,
            closed_at=closed_at,
            direction=str(position.get("direction", "")).upper(),
            label=position.get("label", ""),
            market_slug=position.get("market_slug", ""),
            size_usd=float(position.get("size_usd", 0.0)),
            filled_qty=qty_f,
            entry_price=entry_price_f,
            exit_price=exit_price_f,
            pnl_usd=realized,
            pnl_pct=pnl_pct,
            outcome=outcome,
            close_reason=close_reason,
            entry_order_id=entry_id,
            exit_order_id=exit_order_id,
            session_trade_num=self._live_session_num,
        )
        self.live_trades.append(live_trade)

        # Mirror into the global performance tracker so Grafana, summaries,
        # and the supervisor dashboard reflect live performance.
        try:
            self.performance_tracker.record_trade(
                trade_id=entry_id,
                direction=str(position.get("direction", "long")),
                entry_price=entry_price_dec,
                exit_price=Decimal(str(exit_price_f)),
                size=Decimal(str(position.get("size_usd", 0.0))),
                entry_time=opened_at,
                exit_time=closed_at,
                signal_score=float(position.get("signal_score", 0.0) or 0.0),
                signal_confidence=float(position.get("signal_confidence", 0.0) or 0.0),
                metadata={
                    "simulated": False,
                    "long_token": True,
                    "close_reason": close_reason,
                    "market_slug": position.get("market_slug", ""),
                    "label": position.get("label", ""),
                    "filled_qty": qty_f,
                    "ml_trade_id": position.get("ml_trade_id"),
                },
            )
        except Exception as e:
            logger.warning(f"Failed to record live trade in PerformanceTracker: {e}")

        # Live-session running totals (analogous to the simulation block).
        wins = sum(1 for t in self.live_trades if t.outcome == "WIN")
        losses = sum(1 for t in self.live_trades if t.outcome == "LOSS")
        total_pnl = sum(t.pnl_usd for t in self.live_trades)

        if hasattr(self, "grafana_exporter") and self.grafana_exporter and outcome in ("WIN", "LOSS"):
            try:
                self.grafana_exporter.increment_trade_counter(won=(outcome == "WIN"))
                self.grafana_exporter.record_trade_duration(
                    (closed_at - opened_at).total_seconds()
                )
            except Exception:
                pass

        if self.grafana_exporter:
            try:
                self.grafana_exporter.record_dashboard_event(
                    "position_closed",
                    "实盘仓位已平仓",
                    trade_id=entry_id,
                    market_slug=position.get("market_slug", ""),
                    side="SELL",
                    direction=position.get("direction", ""),
                    qty_tokens=qty_f,
                    entry_price=entry_price_f,
                    fill_price=exit_price_f,
                    notional_usd=qty_f * exit_price_f,
                    realized_pnl=realized,
                    close_reason=close_reason,
                    outcome=outcome,
                    is_simulation=False,
                )
            except Exception:
                pass
        self._publish_dashboard_state()

        marker = {
            "EXIT_TP": "TAKE-PROFIT",
            "EXIT_STOP": "STOP-LOSS",
            "EXIT_MANUAL": "MANUAL EXIT",
            "SETTLEMENT": "SETTLED",
            "SETTLEMENT_FALLBACK": "SETTLED (fallback)",
            "SETTLEMENT_UNRESOLVED": "UNRESOLVED",
        }.get(close_reason, close_reason)

        # Hold duration + win-rate context for the banner.
        try:
            hold_secs = (closed_at - opened_at).total_seconds()
            hold_str = (
                f"{hold_secs:.0f}s" if hold_secs < 90 else f"{hold_secs/60:.1f}m"
            )
        except Exception:
            hold_str = "n/a"
        total = max(1, wins + losses)
        win_rate = (wins / total) * 100.0

        banner_level = "success" if outcome == "WIN" else (
            "error" if outcome == "LOSS" else "warning"
        )

        self._log_event_banner(
            level=banner_level,
            tag=f"TRADE CLOSED #{self._live_session_num}",
            title=(
                f"{outcome}  ({marker})  "
                f"P&L ${realized:+.4f}  ({pnl_pct*100:+.2f}%)"
            ),
            lines=[
                ("Trade ID",  entry_id),
                ("Market",    live_trade.market_slug or "(unknown)"),
                ("Direction", live_trade.direction or "(?)"),
                ("Exit code", marker),
                ("", ""),
                ("Entry px",  f"${entry_price_f:.4f}"),
                ("Exit px",   f"${exit_price_f:.4f}"),
                ("Shares",    f"{qty_f:.6f} 股"),
                ("Notional",  f"${live_trade.size_usd:.2f}"),
                ("Hold",      hold_str),
                ("", ""),
                ("P&L",       f"${realized:+.4f}  ({pnl_pct*100:+.2f}%)"),
                (
                    "Session",
                    f"{wins}W/{losses}L  (winrate {win_rate:.1f}%)  "
                    f"cum=${total_pnl:+.4f}  open={len(self._open_positions)}",
                ),
                (
                    "Capital",
                    f"${float(self.performance_tracker.current_capital):.4f}",
                ),
            ],
        )

        self._save_live_trades([live_trade])

    def _save_live_trades(
        self, trades: Optional[List[LiveTrade]] = None
    ) -> None:
        """将已平仓实盘交易以单个事务持久化到 MySQL。"""
        try:
            selected = self.live_trades if trades is None else trades
            self.trade_history_repository.upsert(
                "live", [trade.to_dict() for trade in selected]
            )
        except Exception as e:
            logger.warning(f"无法将实盘交易写入 MySQL: {e}")

    def _settle_open_positions(self, now: datetime) -> None:
        """Resolve realised P&L for positions whose market has already ended.

        Resolution sources, in order of preference:
          1. ``SettlementTracker.get_resolved_outcome(ml_trade_id)`` — definitive
             1.0 / 0.0 settlement based on Chainlink BTC/USD
          2. Last bid observed *after* market_end_ts (Polymarket's CLOB writes
             1.0 / 0.0 quotes after on-chain resolution)
          3. After ``_settle_grace_seconds`` (default 10 min), a fallback to
             the most recent bid even if it isn't at an extreme; the trade is
             marked ``UNRESOLVED`` so the user can review it.
        """
        if not self._open_positions:
            return

        now_ts = int(now.timestamp())

        for entry_id, position in list(self._open_positions.items()):
            end_ts = int(position.get("market_end_ts") or 0)
            if not end_ts or now_ts < end_ts:
                continue
            if position.get("exit_in_flight"):
                # Manual exit is still in flight — wait for its fill report
                # rather than double-counting at settlement.
                continue

            ml_trade_id = position.get("ml_trade_id")
            settle_price: Optional[Decimal] = None
            close_reason = "SETTLEMENT"

            # Source 1: Chainlink-backed outcome from the settlement tracker.
            if ml_trade_id is not None:
                try:
                    resolved = self.settlement_tracker.get_resolved_outcome(ml_trade_id)
                except Exception:
                    resolved = None
                if resolved is not None:
                    direction = str(position.get("direction", "long"))
                    won = (resolved["outcome"] == 1 and direction == "long") or \
                          (resolved["outcome"] == 0 and direction == "short")
                    settle_price = Decimal("1") if won else Decimal("0")

            # Source 2: last post-settlement bid clamped to {0, 1} when the
            # CLOB has clearly resolved.
            if settle_price is None:
                ps_bid = position.get("last_bid_post_settle")
                if ps_bid is not None:
                    if Decimal(str(ps_bid)) >= Decimal("0.95"):
                        settle_price = Decimal("1")
                    elif Decimal(str(ps_bid)) <= Decimal("0.05"):
                        settle_price = Decimal("0")

            # Source 3: grace-period fallback.
            if settle_price is None:
                if (now_ts - end_ts) >= self._settle_grace_seconds:
                    fallback_bid = (
                        position.get("last_bid_post_settle")
                        or position.get("last_bid")
                    )
                    if fallback_bid is not None:
                        settle_price = Decimal(str(fallback_bid))
                        close_reason = "SETTLEMENT_FALLBACK"
                    else:
                        # No price at all — close at entry, mark unresolved.
                        settle_price = position["entry_price"] if isinstance(
                            position["entry_price"], Decimal
                        ) else Decimal(str(position["entry_price"]))
                        close_reason = "SETTLEMENT_UNRESOLVED"
                else:
                    # Still within grace window — try again on the next tick.
                    continue

            self._open_positions.pop(entry_id, None)
            if position.get("is_paper"):
                self._close_paper_position(
                    entry_id=entry_id,
                    position=position,
                    exit_price=settle_price,
                    close_reason=close_reason,
                )
            else:
                self._close_live_position(
                    entry_id=entry_id,
                    position=position,
                    exit_price=settle_price,
                    exit_order_id=None,
                    close_reason=close_reason,
                )

    def on_order_denied(self, event) -> None:
        client_id = str(getattr(event, "client_order_id", "?"))
        reason = str(getattr(event, "reason", "(unknown)"))
        self._record_order_terminal(
            mode="live",
            order_id=client_id,
            role="exit" if client_id in self._pending_exits else "entry",
            status="DENIED",
            reason=reason,
        )
        tui_event("REJECT", reason[:72], slug="S5", level="ERROR", activity=True)
        self._log_event_banner(
            level="error",
            tag="ORDER DENIED",
            title=f"client_id={client_id}",
            lines=[
                ("Order ID", client_id),
                ("Reason",   str(getattr(event, "reason", "(unknown)"))),
                ("Action",   "discarded; nothing further submitted"),
            ],
        )
        self._track_order_event(
            "rejected",
            reason=str(getattr(event, "reason", "")),
            client_id=str(getattr(event, "client_order_id", "")),
        )
        self._discard_pending_order(event)

    def on_order_rejected(self, event) -> None:
        client_id = str(getattr(event, "client_order_id", "?"))
        reason = str(getattr(event, "reason", ""))
        self._record_order_terminal(
            mode="live",
            order_id=client_id,
            role="exit" if client_id in self._pending_exits else "entry",
            status="REJECTED",
            reason=reason,
        )
        tui_event("REJECT", (reason or "Order rejected")[:72], slug="S5", level="ERROR", activity=True)
        is_fak = any(
            kw in reason.lower() for kw in ("no orders found", "fak", "no match")
        )
        if is_fak:
            self._last_entry_ts = 0.0
            note = "no liquidity (FAK) — cooldown cleared; will retry on next tick"
        else:
            note = "venue rejected order — see reason"

        self._log_event_banner(
            level="error",
            tag="ORDER REJECTED",
            title=f"client_id={client_id}  ({'FAK' if is_fak else 'venue'})",
            lines=[
                ("Order ID", client_id),
                ("Reason",   reason or "(none)"),
                ("Action",   note),
            ],
        )
        self._track_order_event("rejected", reason=reason, client_id=client_id)
        self._discard_pending_order(event)

    def on_order_canceled(self, event) -> None:
        """记录 IOC 未成交或部分成交后的取消终态。"""
        client_id = str(getattr(event, "client_order_id", "?"))
        self._record_order_terminal(
            mode="live",
            order_id=client_id,
            role="exit" if client_id in self._pending_exits else "entry",
            status="CANCELED",
            reason=str(getattr(event, "reason", "") or ""),
        )
        self._discard_pending_order(event)

    def on_order_expired(self, event) -> None:
        """记录订单过期终态并释放待处理状态。"""
        client_id = str(getattr(event, "client_order_id", "?"))
        self._record_order_terminal(
            mode="live",
            order_id=client_id,
            role="exit" if client_id in self._pending_exits else "entry",
            status="EXPIRED",
            reason=str(getattr(event, "reason", "") or ""),
        )
        self._discard_pending_order(event)

    def _discard_pending_order(self, event) -> None:
        """Clean up state for an order that was denied/rejected before any fill.

        Handles both entry (BUY) and exit (SELL) orders so a failed exit lets
        the next quote tick try again instead of hanging forever.
        """
        try:
            client_id = str(getattr(event, "client_order_id", ""))
        except Exception:
            return

        # Failed entry: drop pending metadata.
        if client_id in self._pending_orders:
            self._pending_orders.pop(client_id, None)
            return

        # Failed exit: clear in-flight flag so the position retries on the
        # next eligible tick.
        entry_id = self._pending_exits.pop(client_id, None)
        if entry_id is not None and entry_id in self._open_positions:
            position = self._open_positions[entry_id]
            position["exit_in_flight"] = False
            position["exit_order_id"] = None
            logger.warning(
                f"Exit order {client_id} failed for position {entry_id} — "
                f"will retry on next tick"
            )

    # ── Live position exits ──────────────────────────────────────────────────

    def _check_position_exits(
        self,
        instrument_id,
        bid: Decimal,
        ask: Decimal,
    ) -> None:
        """Inspect every open position on this instrument; submit exits if hit."""
        if not self._open_positions:
            return

        # 护栏 1：点差过宽或价格越界的 tick 视为无效盘口（NO 侧订单簿常
        # 只剩钓鱼单）。直接跳过——既不触发出场，也不把垃圾 bid 写进
        # last_bid（结算收割器会用它计算已实现盈亏）。
        if not is_book_sane(bid, ask, self._max_exit_spread):
            return

        now_ts = int(datetime.now(timezone.utc).timestamp())

        for entry_id, position in list(self._open_positions.items()):
            if position["instrument_id"] != instrument_id:
                continue

            # Always refresh the most recent bid for this position. The
            # post-settlement bid is what the settlement reaper uses to
            # determine realised P&L when no manual exit fired.
            position["last_bid"] = bid
            position["last_bid_ts"] = now_ts
            end_ts = position.get("market_end_ts", 0)
            if end_ts and now_ts >= end_ts:
                position["last_bid_post_settle"] = bid
                position["last_bid_post_settle_ts"] = now_ts

            if position["exit_in_flight"]:
                continue
            if position["filled_qty"] <= 0:
                continue

            # Force-sell at the 14:30 mark (30s before settlement).
            # The entry window closes at second 870, so any open position
            # must be exited here rather than held to binary settlement.
            if end_ts and (end_ts - now_ts) <= self._exit_cutoff_seconds:
                if not position["exit_in_flight"]:
                    logger.info(
                        f"TIME-EXIT triggered for {position.get('label', '')} "
                        f"— forced sell at 14:30 ({self._exit_cutoff_seconds}s before settlement)"
                    )
                    if position.get("is_paper"):
                        self._simulate_paper_exit(entry_id, position, bid, "TIME-EXIT")
                    else:
                        self._submit_exit_order(entry_id, position, "TIME-EXIT")
                continue

            stop_loss = position["stop_loss"]
            take_profit = position["take_profit"]
            sl_enabled = bool(position.get("stop_loss_enabled", True))

            trigger: Optional[str] = None
            # Only consider stop-loss when it's enabled AND the SL price is
            # strictly positive (disabled positions store stop_loss=0 which
            # would otherwise match any bid <= 0 case).
            if sl_enabled and stop_loss > 0 and bid <= stop_loss:
                trigger = "STOP-LOSS"
            elif bid >= take_profit:
                trigger = "TAKE-PROFIT"

            if trigger is None:
                position["exit_confirm_streak"] = 0
                continue

            # 护栏 2：连续确认——单个异常 tick（漏网的钓鱼单）不足以出场。
            streak = int(position.get("exit_confirm_streak", 0)) + 1
            position["exit_confirm_streak"] = streak
            if streak < self._exit_confirm_ticks:
                continue

            logger.warning(
                f"{trigger} TRIGGERED for {position.get('label', '')} "
                f"(entry=${float(position['entry_price']):.4f} "
                f"bid=${float(bid):.4f} stop=${float(stop_loss):.4f} "
                f"tp=${float(take_profit):.4f})"
            )

            self._simulate_paper_exit(entry_id, position, bid, trigger) \
                if position.get("is_paper") \
                else self._submit_exit_order(entry_id, position, trigger)

    def _submit_exit_order(self, entry_id: str, position: dict, reason: str) -> None:
        """Submit a market SELL for the held token quantity to close ``position``."""
        instrument_id = position["instrument_id"]
        instrument = self.cache.instrument(instrument_id)
        if not instrument:
            logger.error(f"Cannot exit {entry_id}: instrument {instrument_id} not in cache")
            return

        precision = instrument.size_precision
        # Polymarket market SELL is base-denominated (token quantity) per the
        # adapter patch, so send the exact filled quantity.
        try:
            qty_float = round(float(position["filled_qty"]), precision)
        except Exception:
            qty_float = float(position["filled_qty"])

        if qty_float <= 0:
            logger.error(f"Cannot exit {entry_id}: zero quantity")
            return

        try:
            qty = Quantity(qty_float, precision=precision)
        except Exception as e:
            logger.error(f"Failed to build Quantity for exit: {e}")
            return

        timestamp_ms = int(time.time() * 1000)
        # Keep a deterministic prefix so the exit can be correlated back to the
        # entry in audit logs.
        suffix = entry_id.split("-")[-1][-6:] if "-" in entry_id else "000000"
        exit_id = f"EXIT-{suffix}-{timestamp_ms}"

        try:
            order = self.order_factory.market(
                instrument_id=instrument_id,
                order_side=OrderSide.SELL,
                quantity=qty,
                client_order_id=ClientOrderId(exit_id),
                quote_quantity=False,
                time_in_force=TimeInForce.IOC,
            )
            self.submit_order(order)
        except Exception as e:
            logger.error(f"Failed to submit exit order: {e}")
            return

        position["exit_in_flight"] = True
        position["exit_order_id"] = exit_id
        self._pending_exits[exit_id] = entry_id
        self._record_order_submitted(
            mode="live",
            order_id=exit_id,
            role="exit",
            market_slug=str(position.get("market_slug", "")),
            direction=str(position.get("direction", "")),
            requested_qty=qty_float,
        )

        # Compute current unrealised P&L for the banner.
        entry_px_f = float(
            position["entry_price"]
            if not isinstance(position["entry_price"], Decimal)
            else position["entry_price"]
        )
        last_bid = position.get("last_bid")
        last_bid_f = float(last_bid) if last_bid is not None else 0.0
        unrealised = (
            qty_float * (last_bid_f - entry_px_f) if last_bid_f > 0 else 0.0
        )
        try:
            hold_secs = (
                datetime.now(timezone.utc) - position["opened_at"]
            ).total_seconds()
            hold_str = (
                f"{hold_secs:.0f}s" if hold_secs < 90 else f"{hold_secs/60:.1f}m"
            )
        except Exception:
            hold_str = "n/a"

        reason_marker = {
            "EXIT_TP":  "TAKE-PROFIT",
            "EXIT_STOP": "STOP-LOSS",
            "EXIT_MANUAL": "MANUAL EXIT",
        }.get(reason, reason)

        self._log_event_banner(
            level="warning",
            tag="EXIT ORDER",
            title=(
                f"SELL  {reason_marker}  qty={qty_float:.4f}  "
                f"unrealised=${unrealised:+.4f}"
            ),
            lines=[
                ("Entry ID",   entry_id),
                ("Exit ID",    exit_id),
                ("Market",     str(position.get("market_slug", "")) or "(unknown)"),
                ("Direction",  str(position.get("direction", "")).upper()),
                ("Token",      str(instrument_id)),
                ("", ""),
                ("Entry px",   f"${entry_px_f:.4f}"),
                (
                    "Last bid",
                    f"${last_bid_f:.4f}" if last_bid_f > 0 else "n/a",
                ),
                ("Stop-loss",  f"${float(position.get('stop_loss', 0)):.4f}"),
                ("Take-prof",  f"${float(position.get('take_profit', 0)):.4f}"),
                ("Shares",     f"{qty_float:.6f} 股"),
                ("Notional",   f"${float(position.get('size_usd', 0)):.2f}"),
                ("", ""),
                ("Reason",     reason),
                ("Held",       hold_str),
                ("Unrealised", f"${unrealised:+.4f}"),
            ],
        )
        self._track_order_event("placed")

    # ── Grafana / stop ────────────────────────────────────────────────────────

    def _start_grafana_sync(self) -> None:
        """Start the Grafana metrics server and keep the update loop running.

        ``GrafanaMetricsExporter.start()`` schedules ``_update_loop`` via
        ``asyncio.create_task``, which only works inside a running event loop.
        Running ``start()`` alone with ``run_until_complete`` exits immediately
        and orphans the task. Instead we start the HTTP server synchronously,
        then drive ``_update_loop`` directly so this thread stays alive.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            # Start the HTTP server (synchronous part of start()).
            loop.run_until_complete(self.grafana_exporter.start())
            logger.info("Grafana metrics started on port 8000")
            # Now keep the update loop alive on this dedicated thread.
            loop.run_until_complete(self.grafana_exporter._update_loop())
        except Exception as e:
            logger.error(f"Failed to start Grafana: {e}")
        finally:
            loop.close()

    def on_stop(self) -> None:
        self._stopping = True
        self._dashboard_stop_event.set()
        logger.info("Integrated BTC strategy stopped")

        for name, stop_fn in (
            ("liquidation stream", self.liquidation_processor.stop_stream),
            ("CVD stream", self.cvd_ob_processor.stop_stream),
            ("settlement tracker", self.settlement_tracker.stop_tracking),
            ("signal recorder", self.signal_recorder.stop),
        ):
            try:
                stop_fn()
                logger.info(f"Stopped {name}")
            except Exception as e:
                logger.warning(f"Failed to stop {name}: {e}")

        # Final settlement sweep for any position whose market has already ended.
        if self._open_positions:
            now = datetime.now(timezone.utc)
            self._settle_open_positions(now)

        p_wins = sum(1 for t in self.paper_trades if t.outcome == "WIN")
        p_losses = sum(1 for t in self.paper_trades if t.outcome == "LOSS")
        p_pending = sum(1 for t in self.paper_trades if t.outcome == "PENDING")
        p_pnl = sum(
            t.pnl_usd for t in self.paper_trades if t.outcome in ("WIN", "LOSS", "BREAKEVEN")
        )
        logger.info(
            f"Paper trades recorded: {len(self.paper_trades)} "
            f"({p_wins}W / {p_losses}L / {p_pending} pending)  "
            f"cumulative P&L=${p_pnl:+.4f}"
        )
        if self.live_trades:
            wins = sum(1 for t in self.live_trades if t.outcome == "WIN")
            losses = sum(1 for t in self.live_trades if t.outcome == "LOSS")
            total = sum(t.pnl_usd for t in self.live_trades)
            logger.info(
                f"Live trades recorded: {len(self.live_trades)} "
                f"({wins}W / {losses}L)  cumulative P&L=${total:+.4f}"
            )
            self._save_live_trades()
        if self.grafana_exporter:
            try:
                self.grafana_exporter.stop_sync()
            except Exception:
                pass

        for name, thread, timeout in (
            ("驾驶舱行情线程", self._dashboard_thread, 7.0),
            ("指标更新线程", self._grafana_thread, 3.0),
        ):
            if thread is None or not thread.is_alive() or threading.current_thread() is thread:
                continue
            thread.join(timeout=timeout)
            if thread.is_alive():
                logger.warning(f"{name} 未在超时前退出")
