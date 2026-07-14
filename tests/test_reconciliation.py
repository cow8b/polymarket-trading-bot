"""停机交易补结算的无网络单元测试。"""
from __future__ import annotations

import unittest
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from core.database import TradeHistoryRepository, metadata
from core.reconciliation import (
    MarketResolution,
    OfflineTradeReconciler,
    held_outcome_index,
    parse_market_resolution,
)


class FakeMarketProvider:
    def __init__(self, resolution: MarketResolution):
        self.resolution = resolution
        self.calls = 0

    def fetch_resolution(self, _slug: str) -> MarketResolution:
        self.calls += 1
        return self.resolution


class ReconciliationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        metadata.create_all(self.engine)
        self.repository = TradeHistoryRepository(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()

    @staticmethod
    def resolution(*, winner: int = 0, resolved: bool = True) -> MarketResolution:
        prices = (1.0, 0.0) if winner == 0 else (0.0, 1.0)
        return MarketResolution(
            slug="btc-updown-15m-1783999800",
            outcomes=("Up", "Down"),
            prices=prices,
            winning_index=winner if resolved else None,
            closed=True,
            resolution_status="resolved" if resolved else "proposed",
            end_at=datetime(2026, 7, 14, 3, 45, tzinfo=timezone.utc),
        )

    def pending_trade(self) -> dict:
        return {
            "trade_id": "paper_1",
            "timestamp": "2026-07-14T03:41:22+00:00",
            "direction": "SHORT",
            "market_slug": "btc-updown-15m-1783999800",
            "size_usd": 10.0,
            "entry_price": 0.25,
            "filled_qty": 40.0,
            "exit_price": 0.25,
            "pnl_usd": 0.0,
            "pnl_pct": 0.0,
            "outcome": "PENDING",
        }

    def test_parse_gamma_string_arrays(self) -> None:
        parsed = parse_market_resolution(
            {
                "slug": "btc-updown-15m-1783999800",
                "closed": True,
                "umaResolutionStatus": "resolved",
                "outcomes": '["Up", "Down"]',
                "outcomePrices": '["1", "0"]',
                "endDate": "2026-07-14T03:45:00Z",
            }
        )
        self.assertTrue(parsed.is_resolved)
        self.assertEqual(parsed.winning_outcome, "Up")

    def test_direction_maps_to_polymarket_outcome(self) -> None:
        self.assertEqual(held_outcome_index({"direction": "LONG"}, ("Up", "Down")), 0)
        self.assertEqual(held_outcome_index({"direction": "SHORT"}, ("Up", "Down")), 1)
        self.assertEqual(held_outcome_index({"direction": "LONG"}, ("Yes", "No")), 0)

    def test_dry_run_recalculates_loss_without_writing(self) -> None:
        self.repository.upsert("paper", [self.pending_trade()])
        reconciler = OfflineTradeReconciler(
            self.repository,
            FakeMarketProvider(self.resolution(winner=0)),
            now_fn=lambda: datetime(2026, 7, 14, 4, 0, tzinfo=timezone.utc),
        )
        result = reconciler.reconcile_once(("paper",), apply=False)[0]

        self.assertEqual(result.status, "READY")
        self.assertAlmostEqual(result.pnl_usd or 0.0, -10.0)
        self.assertEqual(self.repository.load_pending("paper")[0]["outcome"], "PENDING")

    def test_apply_settles_once_and_preserves_profit_formula(self) -> None:
        self.repository.upsert("paper", [self.pending_trade()])
        provider = FakeMarketProvider(self.resolution(winner=1))
        reconciler = OfflineTradeReconciler(
            self.repository,
            provider,
            now_fn=lambda: datetime(2026, 7, 14, 4, 0, tzinfo=timezone.utc),
        )
        result = reconciler.reconcile_once(("paper",), apply=True)[0]

        self.assertEqual(result.status, "UPDATED")
        settled = self.repository.load("paper")[0]
        self.assertEqual(settled["outcome"], "WIN")
        self.assertEqual(settled["exit_price"], 1.0)
        self.assertAlmostEqual(settled["pnl_usd"], 30.0)
        self.assertEqual(reconciler.reconcile_once(("paper",), apply=True), [])

    def test_unresolved_market_is_not_written(self) -> None:
        self.repository.upsert("paper", [self.pending_trade()])
        reconciler = OfflineTradeReconciler(
            self.repository,
            FakeMarketProvider(self.resolution(resolved=False)),
            now_fn=lambda: datetime(2026, 7, 14, 4, 0, tzinfo=timezone.utc),
        )
        result = reconciler.reconcile_once(("paper",), apply=True)[0]

        self.assertEqual(result.status, "WAITING")
        self.assertEqual(self.repository.load_pending("paper")[0]["outcome"], "PENDING")

    def test_same_market_is_fetched_only_once(self) -> None:
        first = self.pending_trade()
        second = {**first, "trade_id": "paper_2"}
        self.repository.upsert("paper", [first, second])
        provider = FakeMarketProvider(self.resolution(winner=0))
        reconciler = OfflineTradeReconciler(
            self.repository,
            provider,
            now_fn=lambda: datetime(2026, 7, 14, 4, 0, tzinfo=timezone.utc),
        )
        reconciler.reconcile_once(("paper",), apply=False)
        self.assertEqual(provider.calls, 1)


if __name__ == "__main__":
    unittest.main()
