"""core.strategy.processors.tick_velocity — Polymarket probability velocity processor."""
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from core.strategy.processors.base import (
    BaseSignalProcessor,
    SignalDirection,
    SignalStrength,
    SignalType,
    TradingSignal,
)


class TickVelocityProcessor(BaseSignalProcessor):
    """
    Measures how fast the Polymarket UP probability is moving in the last 60 s.

    速度使用**绝对概率差**（curr − past）而非相对变化：概率是 0-1 的有界量，
    相对差会让低价端的敏感度虚高十几倍（0.06 时 1 美分 = ±17%，0.90 时同样
    1 美分只有 1%），而低价端恰是盘口最薄、最易被钓鱼单推动 mid 的地方。
    """

    def __init__(
        self,
        velocity_threshold_60s: float = 0.008,
        velocity_threshold_30s: float = 0.005,
        min_ticks: int = 5,
        min_confidence: float = 0.55,
    ):
        super().__init__("TickVelocity")
        self.velocity_threshold_60s = velocity_threshold_60s
        self.velocity_threshold_30s = velocity_threshold_30s
        self.min_ticks = min_ticks
        self.min_confidence = min_confidence
        logger.info(
            f"Initialized Tick Velocity Processor (abs prob diff): "
            f"60s={velocity_threshold_60s:.3f}, 30s={velocity_threshold_30s:.3f}"
        )

    def _get_price_at(
        self, tick_buffer: List[Dict], seconds_ago: float, now: datetime
    ) -> Tuple[Optional[float], Optional[datetime]]:
        """返回目标时刻附近（±15s）最近的 tick 价格及其时间戳。

        时间戳一并返回，供调用方判断 30s/60s 是否解析到了同一根 tick——
        tick 稀疏时两者会撞车，此时加速度是伪量（恒等于速度）。"""
        target = now - timedelta(seconds=seconds_ago)
        best: Optional[float] = None
        best_ts: Optional[datetime] = None
        best_diff = float("inf")
        for tick in tick_buffer:
            ts = tick["ts"]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            diff = abs((ts - target).total_seconds())
            if diff < best_diff:
                best_diff = diff
                best = float(tick["price"])
                best_ts = ts
        if best_diff <= 15:
            return best, best_ts
        return None, None

    def process(
        self,
        current_price: Decimal,
        historical_prices: list,
        metadata: Dict[str, Any] = None,
    ) -> Optional[TradingSignal]:
        if not self.is_enabled or not metadata:
            return None

        tick_buffer = metadata.get("tick_buffer")
        if not tick_buffer or len(tick_buffer) < self.min_ticks:
            return None

        now = datetime.now(timezone.utc)
        curr = float(current_price)

        price_60s, ts_60s = self._get_price_at(tick_buffer, 60, now)
        price_30s, ts_30s = self._get_price_at(tick_buffer, 30, now)

        if price_60s is None and price_30s is None:
            return None

        # 绝对概率差（见类 docstring）。
        vel_60s = (curr - price_60s) if price_60s is not None else None
        vel_30s = (curr - price_30s) if price_30s is not None else None

        # 30s/60s 解析到同一根 tick 时加速度无意义（恒等于速度、必然同号，
        # 会无条件吃到"同向加速"置信度奖励）——视为未知。
        acceleration = 0.0
        if (
            vel_60s is not None
            and vel_30s is not None
            and ts_60s is not None
            and ts_30s is not None
            and ts_60s != ts_30s
        ):
            vel_first_30s = vel_60s - vel_30s
            acceleration = vel_30s - vel_first_30s

        primary_vel = vel_30s if vel_30s is not None else vel_60s
        threshold = (
            self.velocity_threshold_30s
            if vel_30s is not None
            else self.velocity_threshold_60s
        )

        if primary_vel is None or abs(primary_vel) < threshold:
            return None

        direction = SignalDirection.BULLISH if primary_vel > 0 else SignalDirection.BEARISH
        abs_vel = abs(primary_vel)

        if abs_vel >= 0.020:
            strength = SignalStrength.VERY_STRONG
        elif abs_vel >= 0.012:
            strength = SignalStrength.STRONG
        elif abs_vel >= 0.008:
            strength = SignalStrength.MODERATE
        else:
            strength = SignalStrength.WEAK

        confidence = min(0.82, 0.55 + (abs_vel / threshold - 1) * 0.12)

        accel_same_dir = (acceleration > 0 and primary_vel > 0) or (
            acceleration < 0 and primary_vel < 0
        )
        if accel_same_dir and abs(acceleration) > 0.003:
            confidence = min(0.88, confidence + 0.06)

        if vel_60s is not None and vel_30s is not None and (vel_60s > 0) != (vel_30s > 0):
            confidence *= 0.80

        if confidence < self.min_confidence:
            return None

        signal = TradingSignal(
            timestamp=datetime.now(),
            source=self.name,
            signal_type=SignalType.MOMENTUM,
            direction=direction,
            strength=strength,
            confidence=confidence,
            current_price=current_price,
            metadata={
                "velocity_60s": round(vel_60s, 6) if vel_60s is not None else None,
                "velocity_30s": round(vel_30s, 6) if vel_30s is not None else None,
                "acceleration": round(acceleration, 6),
                "ticks_in_buffer": len(tick_buffer),
            },
        )
        self._record_signal(signal)
        logger.info(
            f"TickVelocity {direction.value.upper()}: "
            f"vel={primary_vel:+.4f}, accel={acceleration:+.5f}, "
            f"conf={confidence:.2%}"
        )
        return signal
