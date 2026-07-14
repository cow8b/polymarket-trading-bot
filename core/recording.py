"""
core.recording
==============
Persists every strategy decision cycle (processor signals, fused vote, ML p_up)
for offline fused-signal backtesting. A background thread resolves the real
BTC outcome once each market's end timestamp passes.
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from loguru import logger
from sqlalchemy import and_, func, insert, select, update
from sqlalchemy.engine import Engine

from core.database import initialize_database, signal_cycles
from core.strategy.fusion import FusedSignal
from core.strategy.processors.base import TradingSignal

RESOLVE_INTERVAL_SEC = float(os.getenv("SIGNAL_RECORDING_RESOLVE_SEC", "15"))


def _serialize_signal(sig: TradingSignal) -> Dict[str, Any]:
    return {
        "source": sig.source,
        "direction": getattr(sig.direction, "value", str(sig.direction)),
        "confidence": float(sig.confidence),
        "score": float(sig.score),
        "signal_type": getattr(sig.signal_type, "value", str(sig.signal_type)),
        "strength": int(getattr(sig.strength, "value", sig.strength)),
        "metadata": {
            k: float(v) if hasattr(v, "__float__") else v
            for k, v in (sig.metadata or {}).items()
        },
        "timestamp": sig.timestamp.isoformat() if sig.timestamp else None,
    }


def _serialize_fused(fused: Optional[FusedSignal]) -> Optional[Dict[str, Any]]:
    if fused is None:
        return None
    return {
        "direction": getattr(fused.direction, "value", str(fused.direction)),
        "confidence": float(fused.confidence),
        "score": float(fused.score),
        "num_signals": int(fused.num_signals),
        "weights": fused.weights,
        "metadata": fused.metadata or {},
        "timestamp": fused.timestamp.isoformat() if fused.timestamp else None,
    }


class SignalRecorder:
    """Records decision cycles and resolves BTC market outcomes in the background."""

    def __init__(
        self,
        price_fn: Optional[Callable[[], Optional[float]]] = None,
        engine: Optional[Engine] = None,
    ):
        self._price_fn = price_fn
        self.engine = initialize_database(engine=engine)
        self._lock = threading.Lock()
        self._stats_lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._stop_event = threading.Event()
        self._total_cycles = 0
        self._pending_cycles = 0
        self._resolved_cycles = 0
        self._refresh_stats_from_db()

    def _refresh_stats_from_db(self) -> None:
        """启动时从 MySQL 初始化一次计数，运行期间由写入路径维护。"""
        with self.engine.connect() as conn:
            total, resolved = conn.execute(
                select(
                    func.count(),
                    func.count(signal_cycles.c.outcome),
                ).select_from(signal_cycles)
            ).one()
        with self._stats_lock:
            self._total_cycles = int(total or 0)
            self._resolved_cycles = int(resolved or 0)
            self._pending_cycles = self._total_cycles - self._resolved_cycles

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._resolve_loop,
            name="signal-recorder",
            daemon=True,
        )
        self._thread.start()
        logger.info("SignalRecorder started (storage=MySQL)")

    def stop(self) -> None:
        self._running = False
        self._stop_event.set()
        if (
            self._thread is not None
            and self._thread.is_alive()
            and threading.current_thread() is not self._thread
        ):
            self._thread.join(timeout=2.0)

    def record_cycle(
        self,
        *,
        market_slug: str,
        market_start_ts: Optional[float],
        market_end_ts: Optional[float],
        poly_price: float,
        btc_spot: Optional[float],
        signals: List[TradingSignal],
        fused: Optional[FusedSignal],
        ml_p_up: Optional[float],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload = {
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "market_slug": market_slug,
            "market_start_ts": market_start_ts,
            "market_end_ts": market_end_ts,
            "poly_price": float(poly_price),
            "btc_spot": float(btc_spot) if btc_spot is not None else None,
            "ml_p_up": float(ml_p_up) if ml_p_up is not None else None,
            "signals_json": json.dumps([_serialize_signal(s) for s in signals]),
            "fused_json": json.dumps(_serialize_fused(fused)),
            "metadata_json": json.dumps(metadata or {}),
        }
        with self._lock:
            with self.engine.begin() as conn:
                conn.execute(insert(signal_cycles), payload)
            with self._stats_lock:
                self._total_cycles += 1
                self._pending_cycles += 1

    def _resolve_loop(self) -> None:
        while self._running:
            try:
                self._resolve_pending()
            except Exception as exc:
                logger.debug(f"SignalRecorder resolve loop error: {exc}")
            self._stop_event.wait(RESOLVE_INTERVAL_SEC)

    def _resolve_pending(self) -> None:
        if self._price_fn is None:
            return
        with self._stats_lock:
            if self._pending_cycles <= 0:
                return

        now = time.time()
        exit_price = self._price_fn()
        if exit_price is None:
            return

        with self._lock:
            resolved_cycle_count = 0
            with self.engine.begin() as conn:
                rows = conn.execute(
                    select(
                        signal_cycles.c.market_slug,
                        signal_cycles.c.market_start_ts,
                        signal_cycles.c.market_end_ts,
                    )
                    .where(
                        signal_cycles.c.outcome.is_(None),
                        signal_cycles.c.market_end_ts.is_not(None),
                        signal_cycles.c.market_end_ts <= now,
                    )
                    .group_by(
                        signal_cycles.c.market_slug,
                        signal_cycles.c.market_start_ts,
                        signal_cycles.c.market_end_ts,
                    )
                ).mappings().all()

                for row in rows:
                    market_match = [signal_cycles.c.market_slug == row["market_slug"]]
                    for column, value in (
                        (signal_cycles.c.market_start_ts, row["market_start_ts"]),
                        (signal_cycles.c.market_end_ts, row["market_end_ts"]),
                    ):
                        market_match.append(column.is_(None) if value is None else column == value)

                    entry_price = conn.execute(
                        select(signal_cycles.c.btc_spot)
                        .where(
                            and_(*market_match),
                            signal_cycles.c.btc_spot.is_not(None),
                        )
                        .order_by(signal_cycles.c.id.asc())
                        .limit(1)
                    ).scalar_one_or_none()

                    entry_price = (
                        float(entry_price) if entry_price is not None else exit_price
                    )
                    outcome = 1 if exit_price > entry_price else 0
                    resolved_at = datetime.now(timezone.utc).isoformat()

                    result = conn.execute(
                        update(signal_cycles)
                        .where(and_(*market_match), signal_cycles.c.outcome.is_(None))
                        .values(
                            btc_entry=entry_price,
                            btc_exit=exit_price,
                            outcome=outcome,
                            resolved_at=resolved_at,
                        )
                    )
                    if result.rowcount:
                        resolved_cycle_count += result.rowcount

            # 事务成功提交后再更新缓存，避免数据库回滚时内存计数提前变化。
            if resolved_cycle_count:
                with self._stats_lock:
                    self._pending_cycles = max(
                        0, self._pending_cycles - resolved_cycle_count
                    )
                    self._resolved_cycles += resolved_cycle_count
            if rows:
                logger.info(
                    f"SignalRecorder resolved {len(rows)} market(s) "
                    f"(exit={exit_price:.2f})"
                )

    def get_stats(self, *, refresh: bool = False) -> Dict[str, Any]:
        """返回内存计数；诊断场景可显式 refresh=True 与数据库重新同步。"""
        if refresh:
            self._refresh_stats_from_db()
        with self._stats_lock:
            total = self._total_cycles
            pending = self._pending_cycles
            resolved = self._resolved_cycles
        return {
            "storage": "mysql",
            "total_cycles": total,
            "pending_resolution": pending,
            "resolved_cycles": resolved,
            "running": self._running,
        }


_recorder_instance: Optional[SignalRecorder] = None


def get_signal_recorder(
    price_fn: Optional[Callable[[], Optional[float]]] = None,
) -> SignalRecorder:
    global _recorder_instance
    if _recorder_instance is None:
        _recorder_instance = SignalRecorder(price_fn=price_fn)
    elif price_fn is not None and _recorder_instance._price_fn is None:
        _recorder_instance._price_fn = price_fn
    return _recorder_instance
