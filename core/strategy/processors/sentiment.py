"""core.strategy.processors.sentiment — Sentiment (Fear & Greed) signal processor."""
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional

from loguru import logger

from core.strategy.processors.base import (
    BaseSignalProcessor,
    SignalDirection,
    SignalStrength,
    SignalType,
    TradingSignal,
)


class SentimentProcessor(BaseSignalProcessor):
    """
    Contrarian signals from the Fear & Greed index.

    Extreme Fear  (0–25)  → BULLISH (buy the fear)
    Extreme Greed (75–100) → BEARISH (fade the greed)
    """

    def __init__(
        self,
        extreme_fear_threshold: float = 25,
        extreme_greed_threshold: float = 75,
        min_confidence: float = 0.50,
    ):
        super().__init__("SentimentAnalysis")
        # clamp 到开区间，防止 extremeness 分母为 0（fear=0 或 greed=100）。
        self.extreme_fear = min(max(float(extreme_fear_threshold), 1.0), 49.0)
        self.extreme_greed = min(max(float(extreme_greed_threshold), 51.0), 99.0)
        # 弱信号带从阈值向中性区各延伸 15 分，两侧对称（旧实现硬编码
        # 25-40 vs 65-75，看涨带宽 15 / 看跌带宽 10，结构性偏多）。
        self._weak_bull_ceiling = self.extreme_fear + 15.0
        self._weak_bear_floor = self.extreme_greed - 15.0
        self.min_confidence = min_confidence
        logger.info(
            f"Initialized Sentiment Processor: fear<{extreme_fear_threshold}, "
            f"greed>{extreme_greed_threshold}"
        )

    def process(
        self,
        current_price: Decimal,
        historical_prices: list,
        metadata: Dict[str, Any] = None,
    ) -> Optional[TradingSignal]:
        if not self.is_enabled:
            return None
        if not metadata or "sentiment_score" not in metadata:
            return None

        try:
            score = min(max(float(metadata["sentiment_score"]), 0.0), 100.0)
        except (TypeError, ValueError):
            return None

        if score <= self.extreme_fear:
            direction = SignalDirection.BULLISH
            extremeness = (self.extreme_fear - score) / self.extreme_fear
            if extremeness >= 0.8:
                strength, confidence = SignalStrength.VERY_STRONG, 0.85
            elif extremeness >= 0.5:
                strength, confidence = SignalStrength.STRONG, 0.75
            else:
                strength, confidence = SignalStrength.MODERATE, 0.65
            logger.info(f"Extreme fear {score:.0f} → contrarian BULLISH")

        elif score >= self.extreme_greed:
            direction = SignalDirection.BEARISH
            extremeness = (score - self.extreme_greed) / (100 - self.extreme_greed)
            if extremeness >= 0.8:
                strength, confidence = SignalStrength.VERY_STRONG, 0.85
            elif extremeness >= 0.5:
                strength, confidence = SignalStrength.STRONG, 0.75
            else:
                strength, confidence = SignalStrength.MODERATE, 0.65
            logger.info(f"Extreme greed {score:.0f} → contrarian BEARISH")

        elif score < self._weak_bull_ceiling:
            direction = SignalDirection.BULLISH
            strength, confidence = SignalStrength.WEAK, 0.55

        elif score > self._weak_bear_floor:
            direction = SignalDirection.BEARISH
            strength, confidence = SignalStrength.WEAK, 0.55

        else:
            return None  # 两弱带之间为中性区（默认 40-60）——无信号

        if confidence < self.min_confidence:
            return None

        signal = TradingSignal(
            timestamp=datetime.now(),
            source=self.name,
            signal_type=SignalType.SENTIMENT_SHIFT,
            direction=direction,
            strength=strength,
            confidence=confidence,
            current_price=current_price,
            metadata={
                "sentiment_score": score,
                "sentiment_classification": metadata.get("sentiment_classification", "unknown"),
            },
        )
        self._record_signal(signal)
        return signal
