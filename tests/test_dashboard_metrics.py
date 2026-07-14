"""驾驶舱快照对持久化订单统计的映射测试。"""
from __future__ import annotations

import threading
import unittest
from collections import deque

from monitoring.metrics_exporter import GrafanaMetricsExporter


class DashboardSnapshotTests(unittest.TestCase):
    @staticmethod
    def exporter_with_state(state: dict) -> GrafanaMetricsExporter:
        exporter = GrafanaMetricsExporter.__new__(GrafanaMetricsExporter)
        exporter._live_state_lock = threading.Lock()
        exporter._events_lock = threading.Lock()
        exporter._live_state = state
        exporter._dashboard_events = deque()
        return exporter

    def test_trade_history_count_does_not_fabricate_fill_rate(self) -> None:
        state = {
            "total_pnl": -1.0,
            "unrealized_pnl": 0.0,
            "starting_balance": 100.0,
            "win_rate": 40.0,
            "completed_trades": 10,
            "wins": 4,
            "positions": [],
            "trade_history": [{"trade_id": str(index)} for index in range(10)],
            "order_stats": {
                "entry": {
                    "submitted_total": 0,
                    "filled_orders": 0,
                    "rejected_orders": 0,
                    "fill_rate_pct": None,
                    "full_fill_rate_pct": None,
                    "notional_fill_rate_pct": None,
                    "stats_since": None,
                },
                "exit": {},
            },
        }
        snapshot = self.exporter_with_state(state)._snapshot_from_metrics({})
        execution = snapshot["execution"]
        self.assertEqual(execution["orders_filled_total"], 10)
        self.assertEqual(execution["entry_orders_filled_total"], 0)
        self.assertIsNone(execution["entry_fill_rate_pct"])

    def test_entry_fill_rate_uses_order_fact_table(self) -> None:
        state = {
            "total_pnl": 0.0,
            "starting_balance": 100.0,
            "positions": [],
            "trade_history": [],
            "order_stats": {
                "entry": {
                    "submitted_total": 4,
                    "filled_orders": 3,
                    "rejected_orders": 1,
                    "fill_rate_pct": 75.0,
                    "full_fill_rate_pct": 50.0,
                    "notional_fill_rate_pct": 60.0,
                    "stats_since": "2026-07-14T00:00:00+00:00",
                },
                "exit": {"submitted_total": 2, "filled_orders": 1},
            },
        }
        snapshot = self.exporter_with_state(state)._snapshot_from_metrics({})
        execution = snapshot["execution"]
        self.assertEqual(execution["entry_orders_submitted_total"], 4)
        self.assertEqual(execution["entry_orders_filled_total"], 3)
        self.assertEqual(execution["entry_fill_rate_pct"], 75.0)
        self.assertEqual(execution["exit_orders_filled_total"], 1)


if __name__ == "__main__":
    unittest.main()
