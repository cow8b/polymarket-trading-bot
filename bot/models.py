"""
bot.models — Shared dataclasses, constants, and helper functions used by the
trading strategy and runner.
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional


# ── Trading-window constants ──────────────────────────────────────────────────
QUOTE_STABILITY_REQUIRED: int = 3       # valid ticks before market is considered stable
QUOTE_MIN_SPREAD: float = 0.001         # bid AND ask must be at least this
MARKET_INTERVAL_SECONDS: int = 900      # 15-minute markets


@dataclass
class PaperTrade:
    """Simulation trade with full context for post-session analytics."""

    # Core trade fields
    trade_id: str
    timestamp: datetime
    direction: str          # "LONG" | "SHORT"
    size_usd: float
    entry_price: float      # held-token price at entry (0-1)
    exit_price: float       # held-token settlement price (0 or 1; entry while PENDING)
    pnl_usd: float
    pnl_pct: float
    outcome: str            # "PENDING" | "WIN" | "LOSS" | "BREAKEVEN" | "UNRESOLVED"

    # Signal context
    signal_score: float
    signal_confidence: float
    num_signals: int = 0
    ml_p_up: float = 0.0    # ML model p(UP) at time of trade
    ml_edge: float = 0.0    # abs(ml_p_up - poly_price)

    # Market context
    market_slug: str = ""
    btc_spot_price: float = 0.0
    vol_regime: str = ""
    funding_rate: float = 0.0

    # Settlement context (mirrors LiveTrade so analytics treat paper/live alike)
    filled_qty: float = 0.0         # tokens held = size_usd / entry_price
    close_reason: str = ""          # "" while open; EXIT_TP | EXIT_STOP | TIME-EXIT | SETTLEMENT | ...
    closed_at: Optional[datetime] = None  # 平仓时间；缺失时持仓时长统计无法计算
    ml_trade_id: Optional[int] = None

    # Session tracking
    session_trade_num: int = 0

    def to_dict(self) -> dict:
        return {
            "trade_id": self.trade_id,
            "timestamp": self.timestamp.isoformat(),
            "direction": self.direction,
            "size_usd": self.size_usd,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "pnl_usd": round(self.pnl_usd, 6),
            "pnl_pct": round(self.pnl_pct * 100, 4),
            "outcome": self.outcome,
            "signal_score": self.signal_score,
            "signal_confidence": self.signal_confidence,
            "num_signals": self.num_signals,
            "ml_p_up": round(self.ml_p_up, 4),
            "ml_edge": round(self.ml_edge, 4),
            "market_slug": self.market_slug,
            "btc_spot_price": self.btc_spot_price,
            "vol_regime": self.vol_regime,
            "funding_rate": round(self.funding_rate, 6),
            "filled_qty": round(self.filled_qty, 6),
            "close_reason": self.close_reason,
            "closed_at": self.closed_at.isoformat() if self.closed_at else None,
            "ml_trade_id": self.ml_trade_id,
            "session_trade_num": self.session_trade_num,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "PaperTrade":
        """从持久化 JSON 恢复交易；文件中的 pnl_pct 使用百分数。"""
        return cls(
            trade_id=str(raw.get("trade_id", "")),
            timestamp=datetime.fromisoformat(str(raw["timestamp"]).replace("Z", "+00:00")),
            direction=str(raw.get("direction", "")).upper(),
            size_usd=float(raw.get("size_usd", 0.0) or 0.0),
            entry_price=float(raw.get("entry_price", 0.0) or 0.0),
            exit_price=float(raw.get("exit_price", 0.0) or 0.0),
            pnl_usd=float(raw.get("pnl_usd", 0.0) or 0.0),
            pnl_pct=float(raw.get("pnl_pct", 0.0) or 0.0) / 100.0,
            outcome=str(raw.get("outcome", "PENDING")),
            signal_score=float(raw.get("signal_score", 0.0) or 0.0),
            signal_confidence=float(raw.get("signal_confidence", 0.0) or 0.0),
            num_signals=int(raw.get("num_signals", 0) or 0),
            ml_p_up=float(raw.get("ml_p_up", 0.0) or 0.0),
            ml_edge=float(raw.get("ml_edge", 0.0) or 0.0),
            market_slug=str(raw.get("market_slug", "")),
            btc_spot_price=float(raw.get("btc_spot_price", 0.0) or 0.0),
            vol_regime=str(raw.get("vol_regime", "")),
            funding_rate=float(raw.get("funding_rate", 0.0) or 0.0),
            filled_qty=float(raw.get("filled_qty", 0.0) or 0.0),
            close_reason=str(raw.get("close_reason", "")),
            closed_at=(
                datetime.fromisoformat(str(raw["closed_at"]).replace("Z", "+00:00"))
                if raw.get("closed_at")
                else None
            ),
            ml_trade_id=raw.get("ml_trade_id"),
            session_trade_num=int(raw.get("session_trade_num", 0) or 0),
        )


@dataclass
class LiveTrade:
    """Realised live trade — recorded once a live position is closed.

    A live trade is closed in one of three ways:
      * ``EXIT_STOP``    — manual SELL fired by stop-loss
      * ``EXIT_TP``      — manual SELL fired by take-profit
      * ``SETTLEMENT``   — Polymarket auto-resolved at the 15-min boundary
                          (token paid 1.0 to the winner / 0.0 to the loser)
    """

    trade_id: str           # entry client_order_id
    ml_trade_id: Optional[int]
    timestamp: datetime
    closed_at: datetime
    direction: str          # "LONG" | "SHORT"
    label: str              # "YES (UP)" | "NO (DOWN)"
    market_slug: str

    size_usd: float         # USD notional of the BUY entry
    filled_qty: float       # Polymarket tokens held
    entry_price: float      # held-token price at entry (0-1)
    exit_price: float       # held-token price at exit (0-1)
    pnl_usd: float          # qty * (exit - entry)
    pnl_pct: float          # (exit - entry) / entry

    outcome: str            # "WIN" | "LOSS" | "BREAKEVEN" | "UNRESOLVED"
    close_reason: str       # "EXIT_STOP" | "EXIT_TP" | "SETTLEMENT" | ...

    # Identifiers from the venue
    entry_order_id: Optional[str] = None
    exit_order_id: Optional[str] = None

    # Session tracking
    session_trade_num: int = 0

    def to_dict(self) -> dict:
        return {
            "trade_id": self.trade_id,
            "ml_trade_id": self.ml_trade_id,
            "timestamp": self.timestamp.isoformat(),
            "closed_at": self.closed_at.isoformat(),
            "direction": self.direction,
            "label": self.label,
            "market_slug": self.market_slug,
            "size_usd": round(self.size_usd, 6),
            "filled_qty": round(self.filled_qty, 6),
            "entry_price": round(self.entry_price, 6),
            "exit_price": round(self.exit_price, 6),
            "pnl_usd": round(self.pnl_usd, 6),
            "pnl_pct": round(self.pnl_pct * 100, 4),
            "outcome": self.outcome,
            "close_reason": self.close_reason,
            "entry_order_id": self.entry_order_id,
            "exit_order_id": self.exit_order_id,
            "session_trade_num": self.session_trade_num,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "LiveTrade":
        """从持久化 JSON 恢复已平仓实盘交易。"""
        return cls(
            trade_id=str(raw.get("trade_id", "")),
            ml_trade_id=raw.get("ml_trade_id"),
            timestamp=datetime.fromisoformat(str(raw["timestamp"]).replace("Z", "+00:00")),
            closed_at=datetime.fromisoformat(str(raw["closed_at"]).replace("Z", "+00:00")),
            direction=str(raw.get("direction", "")).upper(),
            label=str(raw.get("label", "")),
            market_slug=str(raw.get("market_slug", "")),
            size_usd=float(raw.get("size_usd", 0.0) or 0.0),
            filled_qty=float(raw.get("filled_qty", 0.0) or 0.0),
            entry_price=float(raw.get("entry_price", 0.0) or 0.0),
            exit_price=float(raw.get("exit_price", 0.0) or 0.0),
            pnl_usd=float(raw.get("pnl_usd", 0.0) or 0.0),
            pnl_pct=float(raw.get("pnl_pct", 0.0) or 0.0) / 100.0,
            outcome=str(raw.get("outcome", "UNRESOLVED")),
            close_reason=str(raw.get("close_reason", "")),
            entry_order_id=raw.get("entry_order_id"),
            exit_order_id=raw.get("exit_order_id"),
            session_trade_num=int(raw.get("session_trade_num", 0) or 0),
        )


def _make_stub_signal(direction: str, ml_p_up: Optional[float] = None):
    """
    Minimal signal stub for paper-trade logging when the ML model fires but no
    individual signal processor produced a fused result.  Avoids crashes in
    ``_record_paper_trade`` which expects ``.direction``, ``.score``,
    ``.confidence``, and ``.num_signals``.
    """
    from core.strategy.processors.base import SignalDirection
    from dataclasses import dataclass as _dc

    @_dc
    class _Stub:
        direction: object
        score: float
        confidence: float
        num_signals: int = 0

    d = SignalDirection.BULLISH if direction == "long" else SignalDirection.BEARISH
    conf = ml_p_up if ml_p_up is not None else 0.60
    return _Stub(direction=d, score=conf * 100, confidence=conf)


def is_book_sane(bid, ask, max_spread: float) -> bool:
    """出场检查用的盘口有效性判定。

    Polymarket 的 NO 侧订单簿经常只剩 $0.01 级别的钓鱼单（流动性集中在
    YES 侧），裸 bid 会让止损在开仓瞬间被虚假触发并按垃圾价成交。点差
    超过 ``max_spread``（绝对值）或价格越界的 tick 视为无效盘口。
    """
    try:
        b, a = float(bid), float(ask)
    except (TypeError, ValueError):
        return False
    return 0.0 < b < 1.0 and a >= b and (a - b) <= max_spread


def interp_exit_fracs(
    entry_price: float,
    min_entry: float,
    max_entry: float,
    sl_at_min: float,
    sl_at_max: float,
    tp_at_min: float,
    tp_at_max: float,
) -> tuple:
    """按入场价在 [min_entry, max_entry] 带内线性插值 SL/TP 比例。

    低价入场 → 宽止损 + 耐心止盈（不对称上行值得等）；高价入场 → 紧止损 +
    快止盈（距 1.0 空间小，须尽快锁定）。入场价越出带按端点截断。
    这是 .env.example 中描述已久的 SL/TP 矩阵的实际实现。
    """
    lo, hi = float(min_entry), float(max_entry)
    e = min(max(float(entry_price), lo), hi)
    t = 0.0 if hi <= lo else (e - lo) / (hi - lo)
    sl = sl_at_min + (sl_at_max - sl_at_min) * t
    tp = tp_at_min + (tp_at_max - tp_at_min) * t
    return sl, tp
