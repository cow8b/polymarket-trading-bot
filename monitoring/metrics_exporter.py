"""
monitoring.metrics_exporter
============================
Prometheus-format metrics HTTP server for Grafana dashboards.

Exposes port 8000 (default) with:
  /metrics   — Prometheus scrape endpoint
  /health    — JSON health check
  /api/v1/*  — Grafana API probe responses (prevents 405 errors)

Metrics covered
---------------
Portfolio / P&L
  trading_current_capital, trading_total_pnl, trading_roi,
  trading_daily_roi, trading_weekly_roi, trading_monthly_roi,
  trading_unrealized_pnl

Risk-adjusted returns
  trading_sharpe_ratio, trading_sortino_ratio, trading_calmar_ratio,
  trading_kelly_fraction, trading_recovery_factor

Drawdown / capital
  trading_max_drawdown, trading_max_drawdown_usd, trading_peak_capital

Trade statistics
  trading_win_rate, trading_profit_factor, trading_expectancy_usd,
  trading_avg_win_usd, trading_avg_loss_usd, trading_best_trade_usd,
  trading_worst_trade_usd, trading_avg_hold_seconds,
  trading_consecutive_wins, trading_consecutive_losses,
  trading_avg_trades_per_day, trading_pnl_variance

Signal / ML
  trading_avg_signal_score, trading_avg_signal_confidence,
  trading_fusion_score, trading_fusion_confidence, trading_fusion_num_signals,
  trading_ml_edge_score, trading_ml_prediction

Execution
  trading_open_positions, trading_total_exposure, trading_risk_utilization,
  trading_orders_placed_total, trading_orders_filled_total,
  trading_orders_rejected_total, trading_trades_closed_total,
  trading_winning_trades_total, trading_losing_trades_total,
  trading_trade_duration_seconds (histogram)

Per-processor metrics (label: processor=<name>)
  signal_processor_score, signal_processor_confidence, signal_processor_direction,
  signal_processor_fires_total

Processor-specific metadata gauges
  signal_ohlcv_rsi, signal_ohlcv_macd_histogram,
  signal_tick_velocity_30s, signal_tick_velocity_60s,
  signal_cvd_delta, signal_orderbook_bid_ask_ratio,
  signal_liquidation_cascade_volume, signal_spike_magnitude,
  signal_divergence_score, signal_funding_rate, signal_oi_change_pct,
  signal_pcr_value, signal_fear_greed_index

Environment
-----------
METRICS_UPDATE_INTERVAL  Seconds between portfolio gauge refreshes (default 1, range 1–60).
                         Match Prometheus scrape_interval and Grafana dashboard refresh for
                         lowest lag (e.g. all set to 1 or 2).
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
import time
import urllib.parse
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional

# ── Windows guard for prometheus_client ──────────────────────────────────────
# Something in the runtime dep tree installs a META-PATH FINDER that
# synthesises an empty `resource` module; that module lacks `getpagesize`,
# causing an AttributeError inside prometheus_client's process_collector.
# Pre-install a stub before importing prometheus_client to avoid this.
if sys.platform == "win32":
    _existing = sys.modules.get("resource")
    if _existing is None or not hasattr(_existing, "getpagesize"):
        import types as _types

        _stub = _types.ModuleType("resource")
        _stub.getpagesize = lambda: 4096
        sys.modules["resource"] = _stub
        del _types, _stub

from loguru import logger
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Info,
    REGISTRY,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from monitoring.performance_tracker import get_performance_tracker
from execution.risk_engine import get_risk_engine
from execution.execution_engine import get_execution_engine

# All 10 signal processor names (canonical labels used in Prometheus)
PROCESSOR_NAMES = [
    "OHLCVMomentum",
    "TickVelocity",
    "CVDOrderBook",
    "OrderBookImbalance",
    "Liquidations",
    "SpikeDetection",
    "PriceDivergence",
    "FundingRateOI",
    "DeribitPCR",
    "SentimentAnalysis",
]


def _dashboard_html() -> str:
    """Return the built-in dashboard HTML, falling back to the embedded page."""
    path = Path(__file__).with_name("dashboard.html")
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return DASHBOARD_HTML


class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP handler for Prometheus metrics and Grafana API probes."""

    exporter: Optional["GrafanaMetricsExporter"] = None

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path in ("/", ""):
            self._respond(200, "text/html", b"""
            <html><head><title>Polymarket Bot Metrics</title>
            <style>body{font-family:sans-serif;margin:2em;background:#111;color:#eee;}
            a{color:#58a6ff;}h1{color:#f0b429;}</style></head><body>
            <h1>&#128200; Polymarket AI Trading Bot</h1>
            <p><a href="/dashboard">/dashboard</a> &mdash; Built-in live dashboard</p>
            <p><a href="/metrics">/metrics</a> &mdash; Prometheus scrape target</p>
            <p><a href="/api/dashboard/snapshot">/api/dashboard/snapshot</a> &mdash; Dashboard JSON snapshot</p>
            <p><a href="/health">/health</a> &mdash; JSON liveness probe</p>
            </body></html>""")
        elif parsed.path == "/dashboard":
            self._respond(200, "text/html; charset=utf-8", _dashboard_html().encode("utf-8"))
        elif parsed.path == "/health":
            self._respond(200, "application/json", b'{"status":"healthy"}')
        elif parsed.path == "/api/dashboard/snapshot":
            if not self.exporter:
                self._respond(503, "application/json", b'{"error":"exporter unavailable"}', cors=True)
                return
            body = json.dumps(
                self.exporter.dashboard_snapshot(),
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            self._respond(200, "application/json", body, cors=True)
        elif parsed.path == "/api/dashboard/history":
            if not self.exporter:
                self._respond(503, "application/json", b'{"error":"exporter unavailable"}', cors=True)
                return
            query = urllib.parse.parse_qs(parsed.query)
            try:
                limit = int(query.get("limit", ["600"])[0])
            except (TypeError, ValueError):
                limit = 600
            body = json.dumps(
                self.exporter.dashboard_history(limit=limit),
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            self._respond(200, "application/json", body, cors=True)
        elif parsed.path == "/metrics":
            try:
                data = generate_latest(REGISTRY)
                self._respond(200, CONTENT_TYPE_LATEST, data, cors=True)
            except Exception as e:
                logger.error(f"Error generating metrics: {e}")
                self._respond(500, "text/plain", f"Error: {e}".encode())
        elif parsed.path.startswith("/api/v1/"):
            body = (
                b'{"status":"success","data":[]}'
                if "labels" in parsed.path
                else (
                    b'{"status":"success","data":{"resultType":"vector","result":[]}}'
                    if "query" in parsed.path
                    else b'{"status":"success"}'
                )
            )
            self._respond(200, "application/json", body, cors=True)
        else:
            self._respond(404, "text/plain", b"Not Found")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith("/api/v1/") or parsed.path == "/metrics":
            self.do_GET()
        else:
            self._respond(404, "text/plain", b"Not Found")

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Accept, Content-Type")
        self.send_header("Access-Control-Max-Age", "86400")
        self.end_headers()

    def _respond(
        self,
        code: int,
        content_type: str,
        body: bytes,
        cors: bool = False,
    ) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        if cors:
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        try:
            if len(args) >= 2:
                status_code = int(args[1]) if str(args[1]).isdigit() else 0
                if status_code >= 400:
                    logger.debug(f"Metrics server: {format % args}")
        except Exception:
            pass


class GrafanaMetricsExporter:
    """
    Prometheus metrics exporter for Grafana dashboards.

    Call ``update_signal_processor(name, score, confidence, direction, metadata)``
    from your strategy whenever a processor fires to keep per-processor gauges live.
    Call ``update_fusion_metrics(score, confidence, num_signals, consensus)`` after
    each fusion pass, and ``update_ml_metrics(edge, prediction)`` after each ML
    inference.
    """

    def __init__(self, port: int = 8000, update_interval: int = 1):
        self.port = port
        self.update_interval = update_interval

        self.performance = get_performance_tracker()
        self.risk = get_risk_engine()
        self.execution = get_execution_engine()

        self._setup_metrics()
        self._history_lock = threading.Lock()
        self._history = deque(maxlen=_dashboard_history_points())
        self._live_state_lock = threading.Lock()
        self._live_state: Dict[str, Any] = {}
        self._events_lock = threading.Lock()
        self._dashboard_events = deque(maxlen=200)
        self._dashboard_event_sequence = 0
        self._is_running = False
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._state_path = Path(
            os.getenv("DASHBOARD_STATE_PATH", "runtime/dashboard/state.json")
        )
        self._persist_lock = threading.Lock()
        self._last_persist_at = 0.0
        self._last_history_at = 0.0
        self._load_dashboard_state()

        logger.info(
            f"Initialized Grafana Metrics Exporter "
            f"(port {port}, update_interval={update_interval}s)"
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Metric registration
    # ──────────────────────────────────────────────────────────────────────────

    def _setup_metrics(self) -> None:
        # ── Portfolio / P&L ──────────────────────────────────────────────────
        self.total_pnl          = Gauge("trading_total_pnl",             "Total realized P&L USD")
        self.unrealized_pnl     = Gauge("trading_unrealized_pnl",        "Unrealized P&L USD")
        self.roi                = Gauge("trading_roi",                    "Total ROI percent")
        self.daily_roi          = Gauge("trading_daily_roi",              "Today ROI percent")
        self.weekly_roi         = Gauge("trading_weekly_roi",             "7-day ROI percent")
        self.monthly_roi        = Gauge("trading_monthly_roi",            "30-day ROI percent")
        self.current_capital    = Gauge("trading_current_capital",        "Current capital USD")
        self.peak_capital       = Gauge("trading_peak_capital",           "Peak capital USD")

        # ── Risk-adjusted returns ────────────────────────────────────────────
        self.sharpe_ratio       = Gauge("trading_sharpe_ratio",           "Sharpe ratio (annualised)")
        self.sortino_ratio      = Gauge("trading_sortino_ratio",          "Sortino ratio (annualised)")
        self.calmar_ratio       = Gauge("trading_calmar_ratio",           "Calmar ratio (ROI / max drawdown)")
        self.kelly_fraction     = Gauge("trading_kelly_fraction",         "Kelly criterion optimal fraction")
        self.recovery_factor    = Gauge("trading_recovery_factor",        "Net profit / max drawdown USD")

        # ── Drawdown ─────────────────────────────────────────────────────────
        self.max_drawdown       = Gauge("trading_max_drawdown",           "Max drawdown percent")
        self.max_drawdown_usd   = Gauge("trading_max_drawdown_usd",       "Max drawdown USD")

        # ── Trade statistics ─────────────────────────────────────────────────
        self.win_rate           = Gauge("trading_win_rate",               "Win rate percent")
        self.profit_factor      = Gauge("trading_profit_factor",          "Gross profit / gross loss")
        self.expectancy_usd     = Gauge("trading_expectancy_usd",         "Expected P&L per trade USD")
        self.avg_win_usd        = Gauge("trading_avg_win_usd",            "Average winning trade USD")
        self.avg_loss_usd       = Gauge("trading_avg_loss_usd",           "Average losing trade USD (abs)")
        self.best_trade_usd     = Gauge("trading_best_trade_usd",         "Best single trade USD")
        self.worst_trade_usd    = Gauge("trading_worst_trade_usd",        "Worst single trade USD")
        self.avg_hold_seconds   = Gauge("trading_avg_hold_seconds",       "Average hold duration seconds")
        self.consecutive_wins   = Gauge("trading_consecutive_wins",       "Current consecutive winning trades")
        self.consecutive_losses = Gauge("trading_consecutive_losses",     "Current consecutive losing trades")
        self.avg_trades_per_day = Gauge("trading_avg_trades_per_day",     "Average trades per day")
        self.pnl_variance       = Gauge("trading_pnl_variance",           "P&L variance (trade-level)")

        # ── Execution counters ───────────────────────────────────────────────
        self.total_trades       = Counter("trading_trades_closed",        "Total closed trades")
        self.winning_trades     = Counter("trading_winning_trades",       "Winning trades")
        self.losing_trades      = Counter("trading_losing_trades",        "Losing trades")
        self.orders_placed      = Counter("trading_orders_placed",        "Orders placed")
        self.orders_filled      = Counter("trading_orders_filled",        "Orders filled")
        self.orders_rejected    = Counter("trading_orders_rejected",      "Orders rejected")
        self.trade_duration     = Histogram(
            "trading_trade_duration_seconds",
            "Trade duration seconds",
            buckets=[60, 300, 600, 900, 1800, 3600, 7200, 14400],
        )

        # ── Position / risk ───────────────────────────────────────────────────
        self.open_positions     = Gauge("trading_open_positions",         "Open positions count")
        self.total_exposure     = Gauge("trading_total_exposure",         "Total exposure USD")
        self.risk_utilization   = Gauge("trading_risk_utilization",       "Risk utilisation percent")

        # ── Signal / fusion / ML ─────────────────────────────────────────────
        self.avg_signal_score       = Gauge("trading_avg_signal_score",       "Average signal score 0-100")
        self.avg_signal_confidence  = Gauge("trading_avg_signal_confidence",  "Average signal confidence 0-1")
        self.fusion_score           = Gauge("trading_fusion_score",           "Latest fusion composite score 0-100")
        self.fusion_confidence      = Gauge("trading_fusion_confidence",      "Latest fusion confidence 0-1")
        self.fusion_num_signals     = Gauge("trading_fusion_num_signals",     "Signals contributing to last fusion")
        self.ml_edge_score          = Gauge("trading_ml_edge_score",          "Latest ML edge vs market price")
        self.ml_prediction          = Gauge("trading_ml_prediction",          "Latest ML p(UP) prediction 0-1")

        # ── Per-processor gauges (labeled) ────────────────────────────────────
        self.proc_score         = Gauge(
            "signal_processor_score",
            "Latest signal score 0-100 per processor",
            ["processor"],
        )
        self.proc_confidence    = Gauge(
            "signal_processor_confidence",
            "Latest signal confidence 0-1 per processor",
            ["processor"],
        )
        self.proc_direction     = Gauge(
            "signal_processor_direction",
            "Latest signal direction: 1=bullish, -1=bearish, 0=neutral",
            ["processor"],
        )
        self.proc_fires         = Counter(
            "signal_processor_fires",
            "Total signals fired per processor",
            ["processor"],
        )

        # ── Processor-specific metadata gauges ───────────────────────────────
        self.ohlcv_rsi              = Gauge("signal_ohlcv_rsi",                 "OHLCV RSI value 0-100")
        self.ohlcv_macd_histogram   = Gauge("signal_ohlcv_macd_histogram",      "OHLCV MACD histogram value")
        self.tick_velocity_30s      = Gauge("signal_tick_velocity_30s",         "Tick velocity 30-second window")
        self.tick_velocity_60s      = Gauge("signal_tick_velocity_60s",         "Tick velocity 60-second window")
        self.cvd_delta              = Gauge("signal_cvd_delta",                 "Cumulative volume delta (CVD)")
        self.orderbook_bid_ask      = Gauge("signal_orderbook_bid_ask_ratio",   "Polymarket CLOB bid/ask imbalance ratio")
        self.liquidation_volume     = Gauge("signal_liquidation_cascade_volume","Liquidation cascade volume USD")
        self.spike_magnitude        = Gauge("signal_spike_magnitude",           "Spike detection deviation magnitude")
        self.divergence_score_g     = Gauge("signal_divergence_score",          "Price divergence score")
        self.funding_rate           = Gauge("signal_funding_rate",              "Binance perp funding rate")
        self.oi_change_pct          = Gauge("signal_oi_change_pct",             "Open interest change percent")
        self.pcr_value              = Gauge("signal_pcr_value",                 "Deribit put/call ratio")
        self.fear_greed_index       = Gauge("signal_fear_greed_index",          "Fear & Greed index 0-100")

        # ── Latest order (updated immediately before each submission) ─────────
        self.last_order_direction   = Gauge(
            "trading_last_order_direction",
            "Last order direction: 1=UP/YES (long), -1=DOWN/NO (short), 0=none",
        )
        self.last_order_size_usd    = Gauge("trading_last_order_size_usd",    "Last order notional USD")
        self.last_order_entry_price = Gauge("trading_last_order_entry_price", "Held-token entry price")
        self.last_order_poly_yes    = Gauge("trading_last_order_poly_yes_price", "Polymarket YES price at entry")
        self.last_order_qty_tokens  = Gauge("trading_last_order_qty_tokens",  "Token quantity")
        self.last_order_bid         = Gauge("trading_last_order_bid_price",   "Best bid at entry")
        self.last_order_ask         = Gauge("trading_last_order_ask_price",   "Best ask at entry")
        self.last_order_spread_pct  = Gauge("trading_last_order_spread_pct",  "Bid-ask spread percent at entry")
        self.last_order_signal_score = Gauge("trading_last_order_signal_score", "Signal score at entry 0-100")
        self.last_order_signal_conf  = Gauge("trading_last_order_signal_confidence", "Signal confidence 0-1")
        self.last_order_ml_edge      = Gauge("trading_last_order_ml_edge",     "ML edge at entry")
        self.last_order_ml_p_up      = Gauge("trading_last_order_ml_p_up",     "ML p(UP) at entry")
        self.last_order_btc_spot     = Gauge("trading_last_order_btc_spot_usd", "BTC spot USD at entry")
        self.last_order_secs_settle  = Gauge("trading_last_order_seconds_to_settle", "Seconds until settlement")
        self.last_order_is_sim       = Gauge("trading_last_order_is_simulation", "1=simulation 0=live")
        self.last_order_fusion_score = Gauge("trading_last_order_fusion_score", "Fusion score at entry")
        self.last_order_info         = Info(
            "trading_last_order",
            "Metadata for the most recently submitted order",
        )
        self.orders_submitted        = Counter(
            "trading_orders_submitted_total",
            "Orders submitted by direction and mode",
            ["direction", "mode"],
        )

        # ── Initialise all labeled time-series so they appear immediately ─────
        for name in PROCESSOR_NAMES:
            self.proc_score.labels(processor=name).set(0)
            self.proc_confidence.labels(processor=name).set(0)
            self.proc_direction.labels(processor=name).set(0)

        logger.info("Prometheus metrics initialised — %d metric families registered", 65)

    def update_order_metrics(
        self,
        *,
        direction: str,
        size_usd: float,
        entry_price: float,
        poly_yes_price: float,
        qty_tokens: float,
        signal_score: float = 0.0,
        signal_confidence: float = 0.0,
        ml_edge: float = 0.0,
        ml_p_up: Optional[float] = None,
        fusion_score: float = 0.0,
        btc_spot: float = 0.0,
        bid_price: Optional[float] = None,
        ask_price: Optional[float] = None,
        seconds_to_settle: Optional[float] = None,
        market_slug: str = "",
        is_simulation: bool = True,
    ) -> None:
        """Publish order details to Prometheus before submission (Grafana UI)."""
        try:
            is_long = direction == "long"
            dir_val = 1.0 if is_long else -1.0
            side = "YES" if is_long else "NO"
            outcome = "UP" if is_long else "DOWN"
            mode = "simulation" if is_simulation else "live"

            self.last_order_direction.set(dir_val)
            self.last_order_size_usd.set(size_usd)
            self.last_order_entry_price.set(entry_price)
            self.last_order_poly_yes.set(poly_yes_price)
            self.last_order_qty_tokens.set(qty_tokens)
            self.last_order_signal_score.set(signal_score)
            self.last_order_signal_conf.set(signal_confidence)
            self.last_order_ml_edge.set(ml_edge)
            if ml_p_up is not None:
                self.last_order_ml_p_up.set(float(ml_p_up))
            self.last_order_fusion_score.set(fusion_score)
            if btc_spot > 0:
                self.last_order_btc_spot.set(btc_spot)
            self.last_order_is_sim.set(1.0 if is_simulation else 0.0)

            if bid_price is not None:
                self.last_order_bid.set(bid_price)
            if ask_price is not None:
                self.last_order_ask.set(ask_price)
            if bid_price is not None and ask_price is not None:
                mid = (bid_price + ask_price) / 2
                if mid > 0:
                    self.last_order_spread_pct.set((ask_price - bid_price) / mid * 100)

            if seconds_to_settle is not None:
                self.last_order_secs_settle.set(max(0.0, seconds_to_settle))

            self.last_order_info.info({
                "direction": outcome,
                "side": side,
                "mode": mode,
                "market": market_slug or "unknown",
            })
            self.orders_submitted.labels(direction=outcome.lower(), mode=mode).inc()
            self.increment_order_counter("placed")

            logger.debug(
                f"Order metrics: {outcome} {side} ${size_usd:.2f} @ {entry_price:.4f} "
                f"qty={qty_tokens:.4f} market={market_slug}"
            )
        except Exception as e:
            logger.debug(f"update_order_metrics error: {e}")

    # ──────────────────────────────────────────────────────────────────────────
    # Periodic update (called from _update_loop every update_interval seconds)
    # ──────────────────────────────────────────────────────────────────────────

    def update_metrics(self) -> None:
        try:
            perf = self.performance.calculate_metrics()

            # Portfolio
            self.total_pnl.set(float(perf.total_pnl))
            self.unrealized_pnl.set(float(perf.unrealized_pnl))
            self.roi.set(perf.roi * 100)
            self.current_capital.set(float(self.performance.current_capital))
            self.peak_capital.set(float(self.performance._peak_capital))

            # Rolling ROI
            self.daily_roi.set(self.performance.get_rolling_roi(days=1) * 100)
            self.weekly_roi.set(self.performance.get_rolling_roi(days=7) * 100)
            self.monthly_roi.set(self.performance.get_rolling_roi(days=30) * 100)

            # Risk-adjusted
            self.sharpe_ratio.set(perf.sharpe_ratio)
            self.sortino_ratio.set(self.performance.calculate_sortino_ratio())
            self.calmar_ratio.set(self.performance.calculate_calmar_ratio())
            self.kelly_fraction.set(self.performance.calculate_kelly_fraction())
            self.recovery_factor.set(self.performance.calculate_recovery_factor())

            # Drawdown
            self.max_drawdown.set(perf.max_drawdown * 100)
            self.max_drawdown_usd.set(
                float(self.performance._peak_capital - self.performance.current_capital)
            )

            # Trade stats
            self.win_rate.set(perf.win_rate * 100)
            self.avg_hold_seconds.set(perf.avg_hold_time)
            self.avg_signal_score.set(perf.avg_signal_score)
            self.avg_signal_confidence.set(perf.avg_signal_confidence)

            dist = self.performance.get_win_loss_distribution()
            self.profit_factor.set(float(dist.get("profit_factor") or 0.0))
            self.avg_win_usd.set(float(dist["wins"]["avg"]))
            self.avg_loss_usd.set(abs(float(dist["losses"]["avg"])))
            self.best_trade_usd.set(float(dist["wins"]["max"]))
            self.worst_trade_usd.set(float(dist["losses"]["max"]))

            if perf.total_trades > 0:
                self.expectancy_usd.set(float(perf.total_pnl / perf.total_trades))

            # Streaks and variance
            streaks = self.performance.get_streak_info()
            self.consecutive_wins.set(streaks["current_wins"])
            self.consecutive_losses.set(streaks["current_losses"])
            self.pnl_variance.set(self.performance.calculate_pnl_variance())
            self.avg_trades_per_day.set(self.performance.calculate_avg_trades_per_day())

            # Position / risk
            self.open_positions.set(perf.open_positions)
            self.total_exposure.set(float(perf.total_exposure))
            risk_summary = self.risk.get_risk_summary()
            if risk_summary:
                self.risk_utilization.set(
                    risk_summary["exposure"]["utilization_pct"]
                )

            logger.debug("Portfolio metrics updated")
            self._record_dashboard_history()
        except Exception as e:
            logger.error(f"Error updating portfolio metrics: {e}")

    # ──────────────────────────────────────────────────────────────────────────
    # Per-processor update API (called by strategy on every signal fire)
    # ──────────────────────────────────────────────────────────────────────────

    def update_signal_processor(
        self,
        name: str,
        score: float,
        confidence: float,
        direction: str,  # "bullish" | "bearish" | "neutral"
        metadata: Dict[str, Any] = None,
    ) -> None:
        """Update per-processor Prometheus gauges.

        Call this every time a signal processor fires (even if the signal was
        filtered out by risk/ML) so Grafana always has fresh per-processor data.
        """
        try:
            self.proc_score.labels(processor=name).set(score)
            self.proc_confidence.labels(processor=name).set(confidence)
            dir_val = 1.0 if direction == "bullish" else (-1.0 if direction == "bearish" else 0.0)
            self.proc_direction.labels(processor=name).set(dir_val)
            self.proc_fires.labels(processor=name).inc()

            if metadata:
                self._apply_processor_metadata(name, metadata)
            self._record_dashboard_history()
        except Exception as e:
            logger.debug(f"update_signal_processor({name}) error: {e}")

    def _apply_processor_metadata(self, name: str, md: Dict[str, Any]) -> None:
        """Route processor-specific metadata fields to dedicated gauges."""
        try:
            n = name.lower()
            if "ohlcv" in n or "momentum" in n:
                if "rsi" in md:
                    self.ohlcv_rsi.set(float(md["rsi"]))
                if "macd_histogram" in md:
                    self.ohlcv_macd_histogram.set(float(md["macd_histogram"]))
            elif "tick" in n or "velocity" in n:
                if "velocity_30s" in md:
                    self.tick_velocity_30s.set(float(md["velocity_30s"]))
                if "velocity_60s" in md:
                    self.tick_velocity_60s.set(float(md["velocity_60s"]))
            elif "cvd" in n:
                if "cvd_delta" in md:
                    self.cvd_delta.set(float(md["cvd_delta"]))
            elif "orderbook" in n or "imbalance" in n:
                if "bid_ask_ratio" in md:
                    self.orderbook_bid_ask.set(float(md["bid_ask_ratio"]))
            elif "liquidat" in n:
                if "cascade_volume" in md:
                    self.liquidation_volume.set(float(md["cascade_volume"]))
                elif "volume" in md:
                    self.liquidation_volume.set(float(md["volume"]))
            elif "spike" in n:
                if "magnitude" in md:
                    self.spike_magnitude.set(float(md["magnitude"]))
                elif "spike_magnitude" in md:
                    self.spike_magnitude.set(float(md["spike_magnitude"]))
            elif "divergence" in n:
                if "divergence_score" in md:
                    self.divergence_score_g.set(float(md["divergence_score"]))
                elif "score" in md:
                    self.divergence_score_g.set(float(md["score"]))
            elif "funding" in n or "oi" in n:
                if "funding_rate" in md:
                    self.funding_rate.set(float(md["funding_rate"]))
                if "oi_change_pct" in md:
                    self.oi_change_pct.set(float(md["oi_change_pct"]))
            elif "pcr" in n or "deribit" in n:
                if "pcr" in md:
                    self.pcr_value.set(float(md["pcr"]))
                elif "put_call_ratio" in md:
                    self.pcr_value.set(float(md["put_call_ratio"]))
            elif "sentiment" in n:
                if "fear_greed_index" in md:
                    self.fear_greed_index.set(float(md["fear_greed_index"]))
                elif "sentiment_score" in md:
                    self.fear_greed_index.set(float(md["sentiment_score"]))
        except Exception as e:
            logger.debug(f"_apply_processor_metadata({name}) error: {e}")

    # ──────────────────────────────────────────────────────────────────────────
    # Fusion / ML update API
    # ──────────────────────────────────────────────────────────────────────────

    def update_fusion_metrics(
        self,
        score: float,
        confidence: float,
        num_signals: int,
        direction: str = "neutral",
    ) -> None:
        """Call after each SignalFusionEngine.fuse_signals() pass."""
        try:
            self.fusion_score.set(score)
            self.fusion_confidence.set(confidence)
            self.fusion_num_signals.set(num_signals)
            self._record_dashboard_history()
        except Exception as e:
            logger.debug(f"update_fusion_metrics error: {e}")

    def update_ml_metrics(self, edge: float, prediction: float) -> None:
        """Call after each MLEngine inference."""
        try:
            self.ml_edge_score.set(edge)
            self.ml_prediction.set(prediction)
            self._record_dashboard_history()
        except Exception as e:
            logger.debug(f"update_ml_metrics error: {e}")

    # ──────────────────────────────────────────────────────────────────────────
    # Event counters (called by strategy on order/trade events)
    # ──────────────────────────────────────────────────────────────────────────

    def increment_trade_counter(self, won: bool) -> None:
        self.total_trades.inc()
        if won:
            self.winning_trades.inc()
        else:
            self.losing_trades.inc()

    def record_trade_duration(self, duration_seconds: float) -> None:
        self.trade_duration.observe(duration_seconds)

    def increment_order_counter(self, status: str) -> None:
        if status == "placed":
            self.orders_placed.inc()
        elif status == "filled":
            self.orders_filled.inc()
        elif status == "rejected":
            self.orders_rejected.inc()

    def update_live_state(self, state: Dict[str, Any]) -> None:
        """Publish non-Prometheus live strategy state for the built-in dashboard."""
        try:
            with self._live_state_lock:
                self._live_state = dict(state or {})
            self._record_dashboard_history()
        except Exception as e:
            logger.debug(f"update_live_state error: {e}")

    def record_dashboard_event(self, event_type: str, message: str, **payload: Any) -> None:
        """Append a real strategy/order event for the built-in dashboard log."""
        try:
            with self._events_lock:
                self._dashboard_event_sequence += 1
                event = {
                    "sequence": self._dashboard_event_sequence,
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "type": event_type,
                    "message": message,
                    "payload": payload,
                }
                self._dashboard_events.append(event)
            self._persist_dashboard_state(force=True)
        except Exception as e:
            logger.debug(f"record_dashboard_event error: {e}")

    def _load_dashboard_state(self) -> None:
        """恢复驾驶舱时间序列和事件，保证重启后仍可复盘。"""
        if not self._state_path.exists():
            return
        try:
            raw = json.loads(self._state_path.read_text(encoding="utf-8"))
            history = raw.get("history", []) if isinstance(raw, dict) else []
            events = raw.get("events", []) if isinstance(raw, dict) else []
            if isinstance(history, list):
                # 旧版本会按每个行情 tick 采样。重启加载时按秒合并，防止
                # 数千个重复点在几分钟内挤掉真正需要复盘的时间范围。
                compacted: Dict[str, dict] = {}
                for index, item in enumerate(history):
                    if not isinstance(item, dict):
                        continue
                    ts = str(item.get("ts", ""))
                    key = ts[:19] if ts else f"legacy-{index}"
                    compacted[key] = item
                self._history.extend(compacted.values())
            if isinstance(events, list):
                self._dashboard_events.extend(item for item in events if isinstance(item, dict))
            self._dashboard_event_sequence = max(
                [int(item.get("sequence", 0) or 0) for item in self._dashboard_events] + [0]
            )
            logger.info(
                f"已恢复驾驶舱历史：{len(self._history)} 个采样，"
                f"{len(self._dashboard_events)} 条事件"
            )
        except Exception as exc:
            logger.warning(f"无法恢复驾驶舱状态 {self._state_path}: {exc}")

    def _persist_dashboard_state(self, *, force: bool = False) -> None:
        """原子保存驾驶舱状态；高频行情最多每 5 秒写盘一次。"""
        now = time.monotonic()
        if not force and now - self._last_persist_at < 5.0:
            return
        if not self._persist_lock.acquire(blocking=False):
            return
        try:
            with self._history_lock:
                history = list(self._history)
            with self._events_lock:
                events = list(self._dashboard_events)
                sequence = self._dashboard_event_sequence
            payload = {
                "version": 1,
                "saved_at": datetime.now(timezone.utc).isoformat(),
                "event_sequence": sequence,
                "history": history,
                "events": events,
            }
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._state_path.with_suffix(".json.tmp")
            tmp.write_text(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
            tmp.replace(self._state_path)
            self._last_persist_at = now
        except Exception as exc:
            logger.warning(f"无法保存驾驶舱状态 {self._state_path}: {exc}")
        finally:
            self._persist_lock.release()

    # ──────────────────────────────────────────────────────────────────────────
    # Built-in dashboard JSON API
    # ──────────────────────────────────────────────────────────────────────────

    def dashboard_snapshot(self) -> Dict[str, Any]:
        """Return a browser-friendly snapshot using the same metrics as Grafana."""
        metrics = self._collect_metric_samples()
        return self._snapshot_from_metrics(metrics)

    def dashboard_history(self, limit: int = 600) -> Dict[str, Any]:
        """Return recent in-memory dashboard samples for browser time-series charts."""
        limit = max(1, min(limit, self._history.maxlen or 600))
        with self._history_lock:
            points = list(self._history)[-limit:]
        snapshot = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "points": points,
        }
        return snapshot

    def _record_dashboard_history(self) -> None:
        """Append one compact dashboard sample for the built-in front-end."""
        now = time.monotonic()
        if now - self._last_history_at < max(1.0, float(self.update_interval)):
            return
        self._last_history_at = now
        try:
            snapshot = self.dashboard_snapshot()
            with self._live_state_lock:
                live_state = dict(self._live_state)
            compact_live_state = {
                key: live_state.get(key)
                for key in (
                    "btc_price",
                    "eth_price",
                    "up_price",
                    "down_price",
                    "market_slug",
                    "market_start",
                    "market_end",
                    "quote_updated_at",
                    "orderbook_updated_at",
                    "mode",
                    "total_volume_usd",
                )
            }
            point = {
                "ts": snapshot["timestamp"],
                "portfolio": snapshot["portfolio"],
                "risk": snapshot["risk"],
                "trade_stats": snapshot["trade_stats"],
                "fusion_ml": snapshot["fusion_ml"],
                "execution": snapshot["execution"],
                "latest_order": snapshot["latest_order"],
                "live_state": compact_live_state,
                "processor_indicators": snapshot["processor_indicators"],
                "processor_scores": {
                    name: data.get("score", 0.0)
                    for name, data in snapshot["processors"].items()
                },
                "processor_directions": {
                    name: data.get("direction", 0.0)
                    for name, data in snapshot["processors"].items()
                },
            }
            with self._history_lock:
                self._history.append(point)
            self._persist_dashboard_state()
        except Exception as e:
            logger.debug(f"Dashboard history update failed: {e}")

    def _collect_metric_samples(self) -> Dict[str, Any]:
        """Collect Prometheus samples into a simple metric-name map."""
        metrics: Dict[str, Any] = {}
        for family in REGISTRY.collect():
            for sample in family.samples:
                name = sample.name
                labels = dict(sample.labels or {})
                value = float(sample.value)
                if labels:
                    metrics.setdefault(name, []).append({
                        "labels": labels,
                        "value": value,
                    })
                else:
                    metrics[name] = value
        return metrics

    def _snapshot_from_metrics(self, m: Dict[str, Any]) -> Dict[str, Any]:
        def v(name: str, default: float = 0.0) -> float:
            raw = m.get(name, default)
            if isinstance(raw, list):
                return default
            try:
                return float(raw)
            except (TypeError, ValueError):
                return default

        def labeled(name: str, label: str, label_value: str, default: float = 0.0) -> float:
            for item in m.get(name, []) if isinstance(m.get(name), list) else []:
                if item.get("labels", {}).get(label) == label_value:
                    return float(item.get("value", default))
            return default

        processors = {}
        for name in PROCESSOR_NAMES:
            processors[name] = {
                "score": labeled("signal_processor_score", "processor", name),
                "confidence": labeled("signal_processor_confidence", "processor", name),
                "direction": labeled("signal_processor_direction", "processor", name),
                "fires_total": labeled("signal_processor_fires_total", "processor", name),
            }

        orders_by_direction: Dict[str, float] = {}
        for item in m.get("trading_orders_submitted_total", []) if isinstance(m.get("trading_orders_submitted_total"), list) else []:
            direction = item.get("labels", {}).get("direction", "unknown")
            orders_by_direction[direction] = orders_by_direction.get(direction, 0.0) + float(item.get("value", 0.0))

        with self._live_state_lock:
            live_state = dict(self._live_state)
        with self._events_lock:
            dashboard_events = list(self._dashboard_events)

        snapshot = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "live_state": live_state,
            "dashboard_events": dashboard_events,
            "portfolio": {
                "current_capital": v("trading_current_capital"),
                "total_pnl": v("trading_total_pnl"),
                "unrealized_pnl": v("trading_unrealized_pnl"),
                "roi": v("trading_roi"),
                "daily_roi": v("trading_daily_roi"),
                "weekly_roi": v("trading_weekly_roi"),
                "monthly_roi": v("trading_monthly_roi"),
            },
            "risk": {
                "sharpe_ratio": v("trading_sharpe_ratio"),
                "sortino_ratio": v("trading_sortino_ratio"),
                "calmar_ratio": v("trading_calmar_ratio"),
                "kelly_fraction": v("trading_kelly_fraction"),
                "max_drawdown": v("trading_max_drawdown"),
                "max_drawdown_usd": v("trading_max_drawdown_usd"),
                "peak_capital": v("trading_peak_capital"),
                "recovery_factor": v("trading_recovery_factor"),
                "risk_utilization": v("trading_risk_utilization"),
            },
            "trade_stats": {
                "win_rate": v("trading_win_rate"),
                "profit_factor": v("trading_profit_factor"),
                "expectancy_usd": v("trading_expectancy_usd"),
                "avg_win_usd": v("trading_avg_win_usd"),
                "avg_loss_usd": v("trading_avg_loss_usd"),
                "avg_win_loss_ratio": v("trading_avg_win_usd") / max(v("trading_avg_loss_usd"), 0.0001),
                "consecutive_wins": v("trading_consecutive_wins"),
                "consecutive_losses": v("trading_consecutive_losses"),
                "avg_hold_seconds": v("trading_avg_hold_seconds"),
                "avg_trades_per_day": v("trading_avg_trades_per_day"),
            },
            "processors": processors,
            "processor_indicators": {
                "ohlcv_rsi": v("signal_ohlcv_rsi"),
                "ohlcv_macd_histogram": v("signal_ohlcv_macd_histogram"),
                "cvd_delta": v("signal_cvd_delta"),
                "tick_velocity_30s": v("signal_tick_velocity_30s"),
                "tick_velocity_60s": v("signal_tick_velocity_60s"),
                "orderbook_bid_ask_ratio": v("signal_orderbook_bid_ask_ratio"),
                "liquidation_cascade_volume": v("signal_liquidation_cascade_volume"),
                "spike_magnitude": v("signal_spike_magnitude"),
                "divergence_score": v("signal_divergence_score"),
                "funding_rate": v("signal_funding_rate"),
                "oi_change_pct": v("signal_oi_change_pct"),
                "pcr_value": v("signal_pcr_value"),
                "fear_greed_index": v("signal_fear_greed_index"),
            },
            "fusion_ml": {
                "fusion_score": v("trading_fusion_score"),
                "fusion_confidence": v("trading_fusion_confidence"),
                "fusion_num_signals": v("trading_fusion_num_signals"),
                "ml_edge_score": v("trading_ml_edge_score"),
                "ml_prediction": v("trading_ml_prediction"),
            },
            "execution": {
                "trades_closed_total": v("trading_trades_closed_total"),
                "winning_trades_total": v("trading_winning_trades_total"),
                "losing_trades_total": v("trading_losing_trades_total"),
                "open_positions": v("trading_open_positions"),
                "total_exposure": v("trading_total_exposure"),
                "orders_placed_total": v("trading_orders_placed_total"),
                "orders_filled_total": v("trading_orders_filled_total"),
                "orders_rejected_total": v("trading_orders_rejected_total"),
                "orders_by_direction": orders_by_direction,
            },
            "latest_order": {
                "direction": v("trading_last_order_direction"),
                "size_usd": v("trading_last_order_size_usd"),
                "entry_price": v("trading_last_order_entry_price"),
                "poly_yes_price": v("trading_last_order_poly_yes_price"),
                "qty_tokens": v("trading_last_order_qty_tokens"),
                "seconds_to_settle": v("trading_last_order_seconds_to_settle"),
                "btc_spot_usd": v("trading_last_order_btc_spot_usd"),
                "ml_edge": v("trading_last_order_ml_edge"),
                "signal_score": v("trading_last_order_signal_score"),
                "signal_confidence": v("trading_last_order_signal_confidence"),
                "bid_price": v("trading_last_order_bid_price"),
                "ask_price": v("trading_last_order_ask_price"),
                "spread_pct": v("trading_last_order_spread_pct"),
                "is_simulation": v("trading_last_order_is_simulation"),
            },
        }
        # 策略账本保存的是实际持有 YES/NO 代币后的成交结果，是驾驶舱
        # 收益、胜率和仓位的权威来源。Prometheus PerformanceTracker 仍可
        # 服务通用指标，但不能覆盖二元代币账本。
        if "total_pnl" in live_state:
            ledger_pnl = float(live_state.get("total_pnl", 0.0) or 0.0)
            ledger_unrealized = float(live_state.get("unrealized_pnl", 0.0) or 0.0)
            starting_balance = float(live_state.get("starting_balance", 0.0) or 0.0)
            wallet_balance = float(
                live_state.get(
                    "wallet_balance",
                    starting_balance + ledger_pnl + ledger_unrealized,
                ) or 0.0
            )
            snapshot["portfolio"].update({
                "current_capital": wallet_balance,
                "total_pnl": ledger_pnl,
                "unrealized_pnl": ledger_unrealized,
                "roi": (ledger_pnl / starting_balance * 100) if starting_balance else 0.0,
            })
            snapshot["trade_stats"]["win_rate"] = float(
                live_state.get("win_rate", 0.0) or 0.0
            )
            history_count = len(live_state.get("trade_history", []) or [])
            completed = int(live_state.get("completed_trades", 0) or 0)
            wins = int(live_state.get("wins", 0) or 0)
            positions = live_state.get("positions", []) or []
            snapshot["execution"].update({
                "trades_closed_total": completed,
                "winning_trades_total": wins,
                "losing_trades_total": max(0, completed - wins),
                "open_positions": len(positions),
                "total_exposure": sum(float(p.get("size_usd", 0.0) or 0.0) for p in positions),
                "orders_filled_total": max(
                    snapshot["execution"]["orders_filled_total"], history_count
                ),
            })
        return snapshot

    # ──────────────────────────────────────────────────────────────────────────
    # Server lifecycle
    # ──────────────────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the HTTP metrics server (update loop driven separately)."""
        if self._is_running:
            logger.warning("Metrics exporter already running")
            return
        try:
            MetricsHandler.exporter = self
            self._server = ThreadingHTTPServer(("0.0.0.0", self.port), MetricsHandler)
            self._server.daemon_threads = True
            self._thread = threading.Thread(
                target=self._server.serve_forever, daemon=True
            )
            self._thread.start()
            self._is_running = True
            logger.info(f"Metrics server started on port {self.port}")
        except Exception as e:
            logger.error(f"Failed to start metrics server: {e}")

    async def _update_loop(self) -> None:
        while self._is_running:
            try:
                self.update_metrics()
                await asyncio.sleep(self.update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Metrics update loop error: {e}")
                await asyncio.sleep(self.update_interval)

    def stop_sync(self) -> None:
        """同步停止 HTTP 服务，允许从已有事件循环的策略回调中调用。"""
        self._is_running = False
        self._persist_dashboard_state(force=True)
        if self._server:
            self._server.shutdown()
            self._server.server_close()
        if (
            self._thread is not None
            and self._thread.is_alive()
            and threading.current_thread() is not self._thread
        ):
            self._thread.join(timeout=2.0)
        logger.info("Metrics exporter stopped")

    async def stop(self) -> None:
        """异步兼容入口；实际停机过程不需要 await。"""
        self.stop_sync()


def _dashboard_history_points(default: int = 3600) -> int:
    """Read DASHBOARD_HISTORY_POINTS from env; clamp to a practical memory bound."""
    raw = os.getenv("DASHBOARD_HISTORY_POINTS", str(default))
    try:
        value = int(raw)
    except ValueError:
        logger.warning(f"Invalid DASHBOARD_HISTORY_POINTS={raw!r}; using {default}")
        return default
    return max(60, min(86400, value))


DASHBOARD_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Polymarket AI Bot — Full Analytics</title>
  <style>
    :root {
      --bg: #0b1020;
      --panel: #111827;
      --panel-2: #0f172a;
      --text: #e5e7eb;
      --muted: #94a3b8;
      --line: #243044;
      --good: #22c55e;
      --bad: #ef4444;
      --warn: #f59e0b;
      --blue: #38bdf8;
      --violet: #a78bfa;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: radial-gradient(circle at top left, #14213f 0, var(--bg) 34rem);
      color: var(--text);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    header {
      position: sticky;
      top: 0;
      z-index: 10;
      border-bottom: 1px solid var(--line);
      background: rgba(11, 16, 32, 0.88);
      backdrop-filter: blur(12px);
      padding: 18px 24px;
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: center;
    }
    h1 { margin: 0; font-size: 20px; letter-spacing: .2px; }
    .sub { color: var(--muted); font-size: 13px; margin-top: 4px; }
    main { padding: 22px; max-width: 1780px; margin: 0 auto; }
    section { margin-bottom: 28px; }
    h2 { font-size: 16px; font-weight: 700; margin: 0 0 12px; color: #f8fafc; }
    .grid { display: grid; grid-template-columns: repeat(6, minmax(160px, 1fr)); gap: 12px; }
    .grid.four { grid-template-columns: repeat(4, minmax(180px, 1fr)); }
    .grid.five { grid-template-columns: repeat(5, minmax(160px, 1fr)); }
    .grid.two { grid-template-columns: repeat(2, minmax(280px, 1fr)); }
    .card {
      background: linear-gradient(180deg, rgba(17,24,39,.96), rgba(15,23,42,.96));
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      min-height: 92px;
      box-shadow: 0 10px 32px rgba(0,0,0,.18);
    }
    .label { color: var(--muted); font-size: 12px; line-height: 1.25; }
    .value { font-size: 25px; font-weight: 750; margin-top: 8px; white-space: nowrap; }
    .unit { color: var(--muted); font-size: 13px; margin-left: 3px; }
    .gauge .track { height: 8px; background: #1f2937; border-radius: 999px; overflow: hidden; margin-top: 12px; }
    .gauge .bar { height: 100%; width: 0%; background: linear-gradient(90deg, var(--blue), var(--good)); border-radius: 999px; transition: width .25s; }
    canvas { width: 100%; height: 230px; display: block; }
    .chart { min-height: 300px; }
    .pill {
      display: inline-flex; align-items: center; gap: 6px;
      padding: 6px 10px; border: 1px solid var(--line); border-radius: 999px;
      color: var(--muted); font-size: 12px; background: rgba(15,23,42,.8);
    }
    .ok { color: var(--good); }
    .bad { color: var(--bad); }
    .warn { color: var(--warn); }
    .processor { min-height: 120px; }
    .dir { margin-top: 8px; font-size: 12px; color: var(--muted); }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    td { padding: 8px 0; border-bottom: 1px solid rgba(148,163,184,.12); }
    td:last-child { text-align: right; color: #f8fafc; font-weight: 650; }
    @media (max-width: 1180px) {
      .grid, .grid.four, .grid.five { grid-template-columns: repeat(2, minmax(160px, 1fr)); }
      .grid.two { grid-template-columns: 1fr; }
    }
    @media (max-width: 640px) {
      main { padding: 14px; }
      .grid, .grid.four, .grid.five { grid-template-columns: 1fr; }
      header { display: block; }
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Polymarket AI Bot — Full Analytics</h1>
      <div class="sub">内置仪表盘 · 数据源与 Grafana 相同，来自 /metrics 同一套指标</div>
    </div>
    <div class="pill">状态：<span id="status" class="warn">连接中</span> · 更新时间：<span id="updated">—</span></div>
  </header>
  <main id="app"></main>
  <script>
    const PROCESSORS = ["OHLCVMomentum","TickVelocity","CVDOrderBook","OrderBookImbalance","Liquidations","SpikeDetection","PriceDivergence","FundingRateOI","DeribitPCR","SentimentAnalysis"];
    const app = document.getElementById("app");
    const fmt = {
      usd: v => "$" + n(v, 2),
      pct: v => n(v, 2) + "%",
      num: (v, d=2) => n(v, d),
      int: v => n(v, 0),
      prob: v => n((v || 0) * 100, 1) + "%",
      sec: v => {
        v = Math.max(0, Number(v || 0));
        if (v < 60) return n(v, 0) + "s";
        if (v < 3600) return n(v / 60, 1) + "m";
        return n(v / 3600, 2) + "h";
      },
    };
    function n(v, d=2) {
      v = Number(v || 0);
      return v.toLocaleString(undefined, {maximumFractionDigits:d, minimumFractionDigits:d});
    }
    function get(obj, path, fallback=0) {
      return path.split(".").reduce((o, k) => (o && o[k] !== undefined ? o[k] : undefined), obj) ?? fallback;
    }
    function card(label, value, unit="", cls="") {
      return `<div class="card ${cls}"><div class="label">${label}</div><div class="value">${value}<span class="unit">${unit}</span></div></div>`;
    }
    function gauge(label, value, max=100, formatter=fmt.num) {
      const pct = Math.max(0, Math.min(100, (Number(value || 0) / max) * 100));
      return `<div class="card gauge"><div class="label">${label}</div><div class="value">${formatter(value)}</div><div class="track"><div class="bar" style="width:${pct}%"></div></div></div>`;
    }
    function section(title, html) {
      return `<section><h2>${title}</h2>${html}</section>`;
    }
    function directionText(v) {
      v = Number(v || 0);
      if (v > 0) return `<span class="ok">Bullish / UP</span>`;
      if (v < 0) return `<span class="bad">Bearish / DOWN</span>`;
      return `<span class="warn">Neutral</span>`;
    }
    function render(s) {
      const p = s.portfolio, r = s.risk, t = s.trade_stats, f = s.fusion_ml, e = s.execution, o = s.latest_order, ind = s.processor_indicators;
      const procCards = PROCESSORS.map(name => {
        const x = s.processors[name] || {};
        return `<div class="card processor gauge"><div class="label">${name}</div><div class="value">${fmt.num(x.score)}</div><div class="track"><div class="bar" style="width:${Math.max(0, Math.min(100, x.score || 0))}%"></div></div><div class="dir">方向：${directionText(x.direction)} · 置信度 ${fmt.prob(x.confidence)} · 触发 ${fmt.int(x.fires_total)}</div></div>`;
      }).join("");
      app.innerHTML =
        section("⚡ Portfolio Overview", `<div class="grid">${card("Capital", fmt.usd(p.current_capital))}${card("Total P&L", fmt.usd(p.total_pnl), "", p.total_pnl >= 0 ? "ok" : "bad")}${card("ROI %", fmt.pct(p.roi))}${card("Daily ROI %", fmt.pct(p.daily_roi))}${card("7-day ROI %", fmt.pct(p.weekly_roi))}${card("30-day ROI %", fmt.pct(p.monthly_roi))}</div>`) +
        section("📐 Risk-Adjusted Metrics", `<div class="grid four">${gauge("Sharpe Ratio", r.sharpe_ratio, 5)}${gauge("Sortino Ratio", r.sortino_ratio, 5)}${gauge("Calmar Ratio", r.calmar_ratio, 5)}${gauge("Kelly Fraction", r.kelly_fraction, 1, fmt.prob)}</div>`) +
        section("📉 Drawdown & Capital", `<div class="grid four">${gauge("Max Drawdown %", r.max_drawdown, 100, fmt.pct)}${card("Max Drawdown USD", fmt.usd(r.max_drawdown_usd))}${card("Peak Capital", fmt.usd(r.peak_capital))}${card("Recovery Factor", fmt.num(r.recovery_factor))}</div>`) +
        section("🎲 Trade Statistics", `<div class="grid five">${gauge("Win Rate %", t.win_rate, 100, fmt.pct)}${card("Profit Factor", fmt.num(t.profit_factor))}${card("Expectancy / Trade", fmt.usd(t.expectancy_usd))}${card("Avg Win / Avg Loss", fmt.num(t.avg_win_loss_ratio))}${card("Streaks", `${fmt.int(t.consecutive_wins)}W / ${fmt.int(t.consecutive_losses)}L`)}</div>`) +
        section("🤖 Signal Processors — Live Scores", `<div class="grid five">${procCards}</div>`) +
        section("📊 Signal Processor — Direction & Confidence", `<div class="grid two">${chartCard("processorScores", "All Processor Scores")}${chartCard("processorDirections", "Processor Direction")}</div>`) +
        section("🔬 Processor-Specific Indicators", `<div class="grid five">${card("OHLCV RSI", fmt.num(ind.ohlcv_rsi))}${card("MACD Histogram", fmt.num(ind.ohlcv_macd_histogram, 4))}${card("CVD Delta", fmt.num(ind.cvd_delta, 0))}${card("Tick Velocity 30s / 60s", `${fmt.num(ind.tick_velocity_30s, 4)} / ${fmt.num(ind.tick_velocity_60s, 4)}`)}${card("Bid/Ask Ratio", fmt.num(ind.orderbook_bid_ask_ratio))}${card("Liquidation Volume", fmt.usd(ind.liquidation_cascade_volume))}${card("Spike Magnitude", fmt.num(ind.spike_magnitude, 4))}${card("Funding / OI", `${fmt.num(ind.funding_rate, 5)} / ${fmt.pct(ind.oi_change_pct)}`)}${card("Deribit PCR", fmt.num(ind.pcr_value))}${card("Fear/Greed", fmt.num(ind.fear_greed_index))}</div>`) +
        section("🧠 ML Engine & Fusion", `<div class="grid four">${gauge("Fusion Score", f.fusion_score, 100)}${gauge("Fusion Confidence", f.fusion_confidence, 1, fmt.prob)}${gauge("ML Edge Score", f.ml_edge_score, 1, fmt.prob)}${gauge("ML p(UP)", f.ml_prediction, 1, fmt.prob)}</div><div class="grid two" style="margin-top:12px">${chartCard("fusion", "Fusion Score & ML Edge vs time")}${chartCard("signals", "Signals per fusion pass")}</div>`) +
        section("📦 Execution & Order Flow", `<div class="grid">${card("Trades", fmt.int(e.trades_closed_total))}${card("Wins", fmt.int(e.winning_trades_total))}${card("Losses", fmt.int(e.losing_trades_total))}${card("Open Positions", fmt.int(e.open_positions))}${card("Exposure USD", fmt.usd(e.total_exposure))}${gauge("Risk Utilisation %", r.risk_utilization, 100, fmt.pct)}</div>`) +
        section("🎯 Latest Order", `<div class="grid five">${card("Direction", directionText(o.direction))}${card("Order size", fmt.usd(o.size_usd))}${card("Entry price", fmt.num(o.entry_price, 4))}${card("Poly YES price", fmt.num(o.poly_yes_price, 4))}${card("Token qty", fmt.num(o.qty_tokens, 4))}${card("Time to settle", fmt.sec(o.seconds_to_settle))}${card("BTC spot @ entry", fmt.usd(o.btc_spot_usd))}${card("ML edge @ entry", fmt.prob(o.ml_edge))}${card("Signal score / conf", `${fmt.num(o.signal_score)} / ${fmt.prob(o.signal_confidence)}`)}${card("Bid / Ask / Spread", `${fmt.num(o.bid_price, 4)} / ${fmt.num(o.ask_price, 4)} / ${fmt.pct(o.spread_pct)}`)}${card("Mode", o.is_simulation ? "SIM" : "LIVE")}</div><div class="grid two" style="margin-top:12px">${chartCard("orders", "Order entry price & size over time")}${tableCard("Order Flow", [["Placed", e.orders_placed_total],["Filled", e.orders_filled_total],["Rejected", e.orders_rejected_total],["UP orders", get(e, "orders_by_direction.up")],["DOWN orders", get(e, "orders_by_direction.down")]])}</div>`) +
        section("📈 Time Series — Capital, P&L, Drawdown", `<div class="grid two">${chartCard("capital", "Capital & P&L trajectory")}${chartCard("risk", "Sharpe / Sortino / Calmar vs time")}${chartCard("roi", "ROI breakdown")}${chartCard("winDrawdown", "Win rate & max drawdown")}</div>`);
    }
    function chartCard(id, title) {
      return `<div class="card chart"><div class="label">${title}</div><canvas id="${id}"></canvas></div>`;
    }
    function tableCard(title, rows) {
      return `<div class="card"><div class="label">${title}</div><table>${rows.map(r => `<tr><td>${r[0]}</td><td>${fmt.num(r[1], 0)}</td></tr>`).join("")}</table></div>`;
    }
    function drawLine(canvas, series, colors) {
      if (!canvas) return;
      const rect = canvas.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;
      canvas.width = rect.width * dpr;
      canvas.height = rect.height * dpr;
      const ctx = canvas.getContext("2d");
      ctx.scale(dpr, dpr);
      ctx.clearRect(0,0,rect.width,rect.height);
      ctx.strokeStyle = "#243044";
      ctx.lineWidth = 1;
      for (let i=0;i<4;i++) {
        const y = 20 + i * ((rect.height-36)/3);
        ctx.beginPath(); ctx.moveTo(0,y); ctx.lineTo(rect.width,y); ctx.stroke();
      }
      const values = series.flatMap(s => s.values).filter(v => Number.isFinite(v));
      const min = Math.min(...values, 0), max = Math.max(...values, 1);
      const span = Math.max(1e-9, max - min);
      series.forEach((s, idx) => {
        ctx.strokeStyle = colors[idx % colors.length];
        ctx.lineWidth = 2;
        ctx.beginPath();
        s.values.forEach((v, i) => {
          const x = series[0].values.length <= 1 ? 0 : i * rect.width / (series[0].values.length - 1);
          const y = rect.height - 16 - ((v - min) / span) * (rect.height - 34);
          if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
        });
        ctx.stroke();
        ctx.fillStyle = colors[idx % colors.length];
        ctx.fillText(s.name, 10, 16 + idx * 14);
      });
    }
    function drawCharts(history) {
      const pts = history.points || [];
      const arr = fn => pts.map(fn).map(v => Number(v || 0));
      const colors = ["#38bdf8","#22c55e","#f59e0b","#a78bfa","#ef4444","#14b8a6"];
      drawLine(document.getElementById("capital"), [{name:"Capital",values:arr(p=>p.portfolio.current_capital)},{name:"PnL",values:arr(p=>p.portfolio.total_pnl)}], colors);
      drawLine(document.getElementById("risk"), [{name:"Sharpe",values:arr(p=>p.risk.sharpe_ratio)},{name:"Sortino",values:arr(p=>p.risk.sortino_ratio)},{name:"Calmar",values:arr(p=>p.risk.calmar_ratio)}], colors);
      drawLine(document.getElementById("roi"), [{name:"Daily",values:arr(p=>p.portfolio.daily_roi)},{name:"Weekly",values:arr(p=>p.portfolio.weekly_roi)},{name:"Monthly",values:arr(p=>p.portfolio.monthly_roi)}], colors);
      drawLine(document.getElementById("winDrawdown"), [{name:"Win%",values:arr(p=>p.trade_stats.win_rate)},{name:"MaxDD",values:arr(p=>p.risk.max_drawdown)}], colors);
      drawLine(document.getElementById("fusion"), [{name:"Fusion",values:arr(p=>p.fusion_ml.fusion_score)},{name:"ML Edge",values:arr(p=>p.fusion_ml.ml_edge_score)},{name:"ML pUP",values:arr(p=>p.fusion_ml.ml_prediction)}], colors);
      drawLine(document.getElementById("signals"), [{name:"Signals",values:arr(p=>p.fusion_ml.fusion_num_signals)}], colors);
      drawLine(document.getElementById("orders"), [{name:"Entry",values:arr(p=>p.latest_order?.entry_price || 0)},{name:"Size",values:arr(p=>p.latest_order?.size_usd || 0)}], colors);
      drawLine(document.getElementById("processorScores"), PROCESSORS.slice(0,6).map(name => ({name, values: arr(p => (p.processor_scores || {})[name])})), colors);
      drawLine(document.getElementById("processorDirections"), PROCESSORS.slice(0,6).map(name => ({name, values: arr(p => (p.processor_directions || {})[name])})), colors);
    }
    async function refresh() {
      try {
        const [snap, hist] = await Promise.all([
          fetch("/api/dashboard/snapshot", {cache:"no-store"}).then(r => r.json()),
          fetch("/api/dashboard/history?limit=900", {cache:"no-store"}).then(r => r.json())
        ]);
        render(snap);
        drawCharts(hist);
        document.getElementById("status").textContent = "在线";
        document.getElementById("status").className = "ok";
        document.getElementById("updated").textContent = new Date(snap.timestamp).toLocaleTimeString();
      } catch (e) {
        document.getElementById("status").textContent = "离线";
        document.getElementById("status").className = "bad";
      }
    }
    refresh();
    setInterval(refresh, 2000);
    addEventListener("resize", () => refresh());
  </script>
</body>
</html>"""


_grafana_exporter_instance: Optional[GrafanaMetricsExporter] = None


def _metrics_update_interval_from_env(default: int = 1) -> int:
    """Read METRICS_UPDATE_INTERVAL from env; clamp to 1–60 seconds."""
    raw = os.getenv("METRICS_UPDATE_INTERVAL", str(default))
    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            f"Invalid METRICS_UPDATE_INTERVAL={raw!r}; using {default}s"
        )
        return default
    if value < 1 or value > 60:
        clamped = max(1, min(60, value))
        logger.warning(
            f"METRICS_UPDATE_INTERVAL={value} out of range; using {clamped}s"
        )
        return clamped
    return value


def get_grafana_exporter() -> GrafanaMetricsExporter:
    global _grafana_exporter_instance
    if _grafana_exporter_instance is None:
        _grafana_exporter_instance = GrafanaMetricsExporter(
            update_interval=_metrics_update_interval_from_env(),
        )
    return _grafana_exporter_instance
