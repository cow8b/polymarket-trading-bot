"""OrderBookImbalance 处理器的无网络单元测试。

复现 2026-07-25 事故：CLOB /book 返回 bids 升序、asks 降序（最优价在数组
末尾），旧实现取 levels[:N] 拿到的是离盘口最远的钓鱼单，且按美元额加权，
导致失衡信号长期高置信度偏空（6/6 全 SHORT）。
"""
from __future__ import annotations

import unittest
from decimal import Decimal
from unittest.mock import patch

from core.strategy.processors.base import SignalDirection
from core.strategy.processors.orderbook import OrderBookImbalanceProcessor


def clob_book(bids, asks):
    """按 CLOB 原始顺序构造订单簿：bids 升序、asks 降序（最优在末尾）。"""
    return {
        "bids": [{"price": str(p), "size": str(s)} for p, s in sorted(bids)],
        "asks": [{"price": str(p), "size": str(s)} for p, s in sorted(asks, reverse=True)],
    }


class NearestLevelsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.proc = OrderBookImbalanceProcessor(top_levels=3)

    def test_picks_touch_not_junk(self) -> None:
        book = clob_book(
            bids=[(0.001, 999), (0.01, 500), (0.26, 40), (0.27, 60)],
            asks=[(0.999, 999), (0.99, 800), (0.29, 30), (0.28, 50)],
        )
        tb = self.proc._nearest_levels(book["bids"], is_bid=True)
        ta = self.proc._nearest_levels(book["asks"], is_bid=False)
        self.assertEqual(float(tb[0]["price"]), 0.27)   # 最优 bid 在前
        self.assertEqual(float(ta[0]["price"]), 0.28)   # 最优 ask 在前
        self.assertNotIn("0.001", [b["price"] for b in tb])
        self.assertNotIn("0.999", [a["price"] for a in ta])


class ImbalanceDirectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.proc = OrderBookImbalanceProcessor(min_book_volume=1.0)

    def _run(self, book):
        with patch.object(self.proc, "fetch_order_book", return_value=book):
            return self.proc.process(
                Decimal("0.5"), [], metadata={"yes_token_id": "tok"}
            )

    def test_bid_heavy_book_is_bullish(self) -> None:
        # 盘口附近买方份额显著更大；深水处放上巨额钓鱼 ask 不应翻转方向。
        book = clob_book(
            bids=[(0.49, 900), (0.48, 600), (0.01, 50)],
            asks=[(0.51, 200), (0.52, 100), (0.99, 8000)],
        )
        sig = self._run(book)
        self.assertIsNotNone(sig)
        self.assertEqual(sig.direction, SignalDirection.BULLISH)
        self.assertGreater(sig.metadata["imbalance"], 0.3)

    def test_ask_heavy_book_is_bearish(self) -> None:
        book = clob_book(
            bids=[(0.49, 150), (0.48, 100), (0.001, 9000)],
            asks=[(0.51, 900), (0.52, 700), (0.99, 40)],
        )
        sig = self._run(book)
        self.assertIsNotNone(sig)
        self.assertEqual(sig.direction, SignalDirection.BEARISH)
        self.assertLess(sig.metadata["imbalance"], -0.3)

    def test_dollar_weighting_bias_regression(self) -> None:
        # 事故场景：份额均衡（1000 vs 1000）但 ask 价更高。美元加权会给出
        # 偏空信号；份额口径应视为均衡、不产生信号。
        book = clob_book(
            bids=[(0.30, 1000)],
            asks=[(0.70, 1000)],
        )
        sig = self._run(book)
        self.assertIsNone(sig)

    def test_thin_book_gated(self) -> None:
        proc = OrderBookImbalanceProcessor(min_book_volume=50.0)
        book = clob_book(bids=[(0.49, 10)], asks=[(0.51, 5)])
        with patch.object(proc, "fetch_order_book", return_value=book):
            sig = proc.process(Decimal("0.5"), [], metadata={"yes_token_id": "tok"})
        self.assertIsNone(sig)


if __name__ == "__main__":
    unittest.main()
