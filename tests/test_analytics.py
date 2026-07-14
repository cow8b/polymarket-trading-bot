"""驾驶舱指标口径的无网络测试。"""
from __future__ import annotations

import unittest

from core.analytics import calculate_prediction_edge, calculate_strategy_edge


class PredictionEdgeTests(unittest.TestCase):
    def test_up_and_down_use_symmetric_held_probability(self) -> None:
        up = calculate_prediction_edge(
            direction="long", p_up=0.65, executable_entry_price=0.55
        )
        down = calculate_prediction_edge(
            direction="short", p_up=0.35, executable_entry_price=0.55
        )
        self.assertTrue(up["available"])
        self.assertAlmostEqual(up["edge_pct"], down["edge_pct"])
        self.assertAlmostEqual(up["edge_points_pct"], 10.0)

    def test_negative_edge_keeps_sign(self) -> None:
        result = calculate_prediction_edge(
            direction="long", p_up=0.45, executable_entry_price=0.55
        )
        self.assertLess(result["edge_pct"], 0)

    def test_missing_probability_is_unavailable(self) -> None:
        result = calculate_prediction_edge(
            direction="long", p_up=None, executable_entry_price=0.55
        )
        self.assertFalse(result["available"])


class StrategyEdgeTests(unittest.TestCase):
    def test_uses_settled_pnl_over_entry_notional(self) -> None:
        result = calculate_strategy_edge(
            [
                {"outcome": "WIN", "pnl_usd": 5.0, "size_usd": 10.0},
                {"outcome": "LOSS", "pnl_usd": -2.0, "size_usd": 10.0},
                {"outcome": "PENDING", "pnl_usd": 99.0, "size_usd": 10.0},
            ]
        )
        self.assertTrue(result["available"])
        self.assertEqual(result["sample_size"], 2)
        self.assertAlmostEqual(result["edge_pct"], 15.0)
        self.assertAlmostEqual(result["expectancy_usd"], 1.5)

    def test_window_uses_most_recent_settled_trades(self) -> None:
        trades = [
            {"outcome": "LOSS", "pnl_usd": -10.0, "size_usd": 10.0},
            {"outcome": "WIN", "pnl_usd": 5.0, "size_usd": 10.0},
            {"outcome": "WIN", "pnl_usd": 5.0, "size_usd": 10.0},
        ]
        result = calculate_strategy_edge(trades, window=2)
        self.assertEqual(result["sample_size"], 2)
        self.assertAlmostEqual(result["edge_pct"], 50.0)

    def test_zero_samples_are_not_reported_as_zero_edge(self) -> None:
        result = calculate_strategy_edge([], window=50)
        self.assertFalse(result["available"])
        self.assertIsNone(result["edge_pct"])


if __name__ == "__main__":
    unittest.main()
