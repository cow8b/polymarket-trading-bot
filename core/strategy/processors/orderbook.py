"""core.strategy.processors.orderbook — Polymarket CLOB order-book imbalance processor."""
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import httpx
from loguru import logger

from core.strategy.processors.base import (
    BaseSignalProcessor,
    SignalDirection,
    SignalStrength,
    SignalType,
    TradingSignal,
)

CLOB_BASE = "https://clob.polymarket.com"


class OrderBookImbalanceProcessor(BaseSignalProcessor):
    """
    Detects order-book imbalance on the Polymarket CLOB for the YES token.

    以盘口最优 N 档、价带内的份额（share）计算：
    imbalance = (bid_shares - ask_shares) / (bid_shares + ask_shares)
      > +0.30  → BULLISH (heavy buy pressure)
      < -0.30  → BEARISH (heavy sell pressure)
    """

    def __init__(
        self,
        imbalance_threshold: float = 0.30,
        wall_threshold: float = 0.20,
        min_book_volume: float = 50.0,
        min_confidence: float = 0.55,
        top_levels: int = 10,
    ):
        super().__init__("OrderBookImbalance")
        self.imbalance_threshold = imbalance_threshold
        self.wall_threshold = wall_threshold
        self.min_book_volume = min_book_volume
        self.min_confidence = min_confidence
        self.top_levels = top_levels
        logger.info(
            f"Initialized OrderBook Processor: "
            f"imbalance={imbalance_threshold:.0%}, "
            f"wall={wall_threshold:.0%}, "
            f"min_vol=${min_book_volume:.0f}"
        )

    def fetch_order_book(self, token_id: str) -> Optional[Dict]:
        try:
            with httpx.Client(timeout=5.0) as client:
                resp = client.get(f"{CLOB_BASE}/book", params={"token_id": token_id})
                resp.raise_for_status()
                return resp.json()
        except Exception as e:
            logger.warning(f"OrderBook fetch failed for {token_id[:16]}…: {e}")
            return None

    @staticmethod
    def _level_price(level: Dict) -> float:
        try:
            return float(level.get("price", 0))
        except (ValueError, TypeError):
            return 0.0

    def _nearest_levels(self, levels: List[Dict], *, is_bid: bool) -> List[Dict]:
        """取靠近盘口的 top_levels 档。

        CLOB /book 返回 bids 升序、asks 降序——最优价在数组末尾。旧实现
        直接 levels[:N]，拿到的是离盘口最远的 N 档钓鱼单（0.001 的 bid、
        0.999 的 ask），失衡信号因此长期错向（2026-07-25 模拟盘 6 笔全
        SHORT 的根因之一）。"""
        ordered = sorted(levels, key=self._level_price, reverse=is_bid)
        return ordered[: self.top_levels]

    def _band_share_volume(self, levels: List[Dict], band: float = 0.15) -> float:
        """以该侧最优价为基准，统计价带内各档的份额（share）总量。

        失衡必须按份额而非美元额（price×size）计——ask 价恒高于 bid，
        美元加权会结构性放大 ask 侧、把信号推向偏空。价带过滤掉漏进
        top 档的深水钓鱼单。"""
        if not levels:
            return 0.0
        touch = self._level_price(levels[0])
        total = 0.0
        for level in levels:
            try:
                price = float(level.get("price", 0))
                size = float(level.get("size", 0))
            except (ValueError, TypeError):
                continue
            if abs(price - touch) <= band:
                total += size
        return total

    def _parse_levels_usd(self, levels: List[Dict]) -> float:
        total = 0.0
        for level in levels:
            try:
                total += float(level.get("price", 0)) * float(level.get("size", 0))
            except (ValueError, TypeError):
                continue
        return total

    def _detect_wall(self, levels: List[Dict], total_volume: float) -> Optional[float]:
        if total_volume <= 0:
            return None
        for level in levels:
            try:
                order_usd = float(level.get("price", 0)) * float(level.get("size", 0))
                if order_usd / total_volume >= self.wall_threshold:
                    return order_usd
            except (ValueError, TypeError):
                continue
        return None

    def process(
        self,
        current_price: Decimal,
        historical_prices: list,
        metadata: Dict[str, Any] = None,
    ) -> Optional[TradingSignal]:
        if not self.is_enabled or not metadata:
            return None

        token_id = metadata.get("yes_token_id")
        if not token_id:
            return None

        try:
            book = self.fetch_order_book(token_id)
            if not book:
                return None

            top_bids = self._nearest_levels(book.get("bids", []), is_bid=True)
            top_asks = self._nearest_levels(book.get("asks", []), is_bid=False)

            # 流动性门槛沿用美元口径（top 档），失衡改用份额口径。
            usd_volume = self._parse_levels_usd(top_bids) + self._parse_levels_usd(top_asks)
            if usd_volume < self.min_book_volume:
                return None

            bid_volume = self._band_share_volume(top_bids)
            ask_volume = self._band_share_volume(top_asks)
            total_volume = bid_volume + ask_volume
            if total_volume <= 0:
                return None

            imbalance = (bid_volume - ask_volume) / total_volume
            logger.info(
                f"OrderBook: bids={bid_volume:.0f}sh, asks={ask_volume:.0f}sh, "
                f"(${usd_volume:.0f} top-book) imbalance={imbalance:+.3f}"
            )

            if abs(imbalance) < self.imbalance_threshold:
                return None

            direction = (
                SignalDirection.BULLISH if imbalance > 0 else SignalDirection.BEARISH
            )
            abs_imb = abs(imbalance)
            if abs_imb >= 0.70:
                strength = SignalStrength.VERY_STRONG
            elif abs_imb >= 0.50:
                strength = SignalStrength.STRONG
            elif abs_imb >= 0.35:
                strength = SignalStrength.MODERATE
            else:
                strength = SignalStrength.WEAK

            confidence = min(0.85, 0.55 + abs_imb * 0.40)
            bid_wall = self._detect_wall(top_bids, usd_volume)
            ask_wall = self._detect_wall(top_asks, usd_volume)
            wall_side = bid_wall if direction == SignalDirection.BULLISH else ask_wall
            if wall_side:
                confidence = min(0.90, confidence + 0.05)

            if confidence < self.min_confidence:
                return None

            signal = TradingSignal(
                timestamp=datetime.now(),
                source=self.name,
                signal_type=SignalType.VOLUME_SURGE,
                direction=direction,
                strength=strength,
                confidence=confidence,
                current_price=current_price,
                metadata={
                    "bid_shares": round(bid_volume, 2),
                    "ask_shares": round(ask_volume, 2),
                    "total_shares": round(total_volume, 2),
                    "top_book_usd": round(usd_volume, 2),
                    "imbalance": round(imbalance, 4),
                    "bid_wall_usd": round(bid_wall, 2) if bid_wall else None,
                    "ask_wall_usd": round(ask_wall, 2) if ask_wall else None,
                },
            )
            self._record_signal(signal)
            logger.info(
                f"OrderBook {direction.value.upper()}: "
                f"imbalance={imbalance:+.3f}, conf={confidence:.2%}"
            )
            return signal

        except Exception as e:
            logger.warning(f"OrderBookImbalance error: {e}")
            return None
