"""2026-07-25 信号处理器双审计修复的回归测试（无网络）。

覆盖：TickVelocity 绝对概率差口径 + 同 tick 加速度去伪；DeribitPCR falsy
陷阱 + 看跌置信度天花板对称化；Sentiment 弱带对称；Divergence 缺数据弃权。
"""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from core.strategy.processors.base import SignalDirection, SignalStrength
from core.strategy.processors.deribit_pcr import DeribitPCRProcessor
from core.strategy.processors.divergence import PriceDivergenceProcessor
from core.strategy.processors.sentiment import SentimentProcessor
from core.strategy.processors.tick_velocity import TickVelocityProcessor


def _buffer(now, *points):
    """(seconds_ago, price) 列表 → tick_buffer。"""
    return [
        {"ts": now - timedelta(seconds=s), "price": p}
        for s, p in points
    ]


class TickVelocityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.proc = TickVelocityProcessor(
            velocity_threshold_60s=0.008, velocity_threshold_30s=0.005
        )
        self.now = datetime.now(timezone.utc)

    def test_velocity_is_absolute_probability_diff(self) -> None:
        # 低价端 1 美分变动：旧的相对口径会算出 +20% 直接 VERY_STRONG；
        # 绝对口径应为 +0.01 → 仅 MODERATE。
        buf = _buffer(self.now, (1, 0.05), (2, 0.05), (3, 0.05), (4, 0.05), (30, 0.05))
        sig = self.proc.process(Decimal("0.06"), [], {"tick_buffer": buf})
        self.assertIsNotNone(sig)
        self.assertAlmostEqual(sig.metadata["velocity_30s"], 0.01)
        self.assertEqual(sig.strength, SignalStrength.MODERATE)

    def test_same_tick_acceleration_is_neutralised(self) -> None:
        # 只有一根 45s 前的 tick 同时命中 30s/60s 目标（±15s 容忍边界）——
        # 加速度是伪量，必须为 0 且不吃"同向加速"置信度奖励。
        # process 内部会重新取 now，需冻结时钟避免毫秒漂移把 45s 挤出容忍窗。
        from unittest.mock import patch as _patch

        fixed = self.now

        class _FrozenDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return fixed if tz else fixed.replace(tzinfo=None)

        buf = _buffer(self.now, (1, 0.52), (2, 0.52), (3, 0.52), (4, 0.52), (45, 0.50))
        with _patch("core.strategy.processors.tick_velocity.datetime", _FrozenDatetime):
            sig = self.proc.process(Decimal("0.52"), [], {"tick_buffer": buf})
        self.assertIsNotNone(sig)
        self.assertEqual(sig.metadata["acceleration"], 0.0)
        self.assertAlmostEqual(sig.confidence, 0.82)  # 封顶但无 +0.06 → 不是 0.88

    def test_distinct_ticks_yield_real_acceleration(self) -> None:
        buf = _buffer(
            self.now,
            (1, 0.52), (2, 0.52), (3, 0.52),
            (30, 0.50), (60, 0.49),
        )
        sig = self.proc.process(Decimal("0.53"), [], {"tick_buffer": buf})
        self.assertIsNotNone(sig)
        self.assertNotEqual(sig.metadata["acceleration"], 0.0)
        self.assertEqual(sig.direction, SignalDirection.BULLISH)


class DeribitPCRTests(unittest.TestCase):
    def setUp(self) -> None:
        self.proc = DeribitPCRProcessor(
            bullish_pcr_threshold=1.20, bearish_pcr_threshold=0.70
        )

    def test_zero_short_pcr_not_swallowed(self) -> None:
        # short_pcr=0.0（近月全 call，极度贪婪）是最强逆向看跌读数；
        # 旧的 `or` 短路会静默回退到 overall_pcr=1.0（无信号区）。
        sig = self.proc._generate_signal(
            Decimal("0.5"), {"short_pcr": 0.0, "overall_pcr": 1.0}
        )
        self.assertIsNotNone(sig)
        self.assertEqual(sig.direction, SignalDirection.BEARISH)
        self.assertEqual(sig.strength, SignalStrength.VERY_STRONG)

    def test_bearish_confidence_can_reach_cap(self) -> None:
        # 旧线性刻度下看跌 extremeness 封顶 1.0、置信度够不到 0.80。
        sig = self.proc._generate_signal(Decimal("0.5"), {"short_pcr": 0.20})
        self.assertIsNotNone(sig)
        self.assertAlmostEqual(sig.confidence, 0.80)

    def test_strength_thresholds_ratio_symmetric(self) -> None:
        bull = self.proc._generate_signal(Decimal("0.5"), {"short_pcr": 1.61})
        bear = self.proc._generate_signal(Decimal("0.5"), {"short_pcr": 0.52})
        self.assertEqual(bull.strength, SignalStrength.VERY_STRONG)
        self.assertEqual(bear.strength, SignalStrength.VERY_STRONG)


class SentimentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.proc = SentimentProcessor(
            extreme_fear_threshold=25, extreme_greed_threshold=75
        )

    def _sig(self, score):
        return self.proc.process(Decimal("0.5"), [], {"sentiment_score": score})

    def test_weak_bands_symmetric(self) -> None:
        self.assertEqual(self._sig(39).direction, SignalDirection.BULLISH)
        self.assertEqual(self._sig(61).direction, SignalDirection.BEARISH)
        # 中性区 40-60 两侧等宽（旧实现 40-65，弱看涨带比弱看跌带宽 5 分）。
        self.assertIsNone(self._sig(40))
        self.assertIsNone(self._sig(50))
        self.assertIsNone(self._sig(60))

    def test_degenerate_thresholds_no_crash(self) -> None:
        proc = SentimentProcessor(extreme_fear_threshold=0, extreme_greed_threshold=100)
        sig = proc.process(Decimal("0.5"), [], {"sentiment_score": 100})
        self.assertIsNotNone(sig)  # clamp 后不再除零


class DivergenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.proc = PriceDivergenceProcessor()

    def test_missing_spot_abstains(self) -> None:
        # 旧实现回退用 poly 动量（与 TickVelocity 重复计票）。
        sig = self.proc.process(Decimal("0.80"), [], {"momentum": 0.05})
        self.assertIsNone(sig)

    def test_insufficient_history_abstains(self) -> None:
        # 头两个样本动量未知，不得按 0.0 通过"无确认动量"闸门放行 fade。
        for _ in range(2):
            sig = self.proc.process(Decimal("0.80"), [], {"spot_price": 64000.0})
            self.assertIsNone(sig)

    def test_fade_fires_with_flat_spot(self) -> None:
        for _ in range(3):
            sig = self.proc.process(Decimal("0.80"), [], {"spot_price": 64000.0})
        self.assertIsNotNone(sig)
        self.assertEqual(sig.direction, SignalDirection.BEARISH)

    def test_reset_history_clears(self) -> None:
        for _ in range(3):
            self.proc.process(Decimal("0.80"), [], {"spot_price": 64000.0})
        self.proc.reset_history()
        sig = self.proc.process(Decimal("0.80"), [], {"spot_price": 64000.0})
        self.assertIsNone(sig)


if __name__ == "__main__":
    unittest.main()
