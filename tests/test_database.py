"""MySQL 持久化仓库的无网络单元测试。"""
from __future__ import annotations

import os
import time
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, event
from sqlalchemy.pool import StaticPool

from core.database import (
    DashboardStateRepository,
    DatabaseConfigurationError,
    DatabaseSettings,
    OrderLifecycleRepository,
    TradeHistoryRepository,
    metadata,
)
from core.recording import SignalRecorder


class DatabaseSettingsTests(unittest.TestCase):
    def test_password_is_safely_encoded_in_url(self) -> None:
        env = {
            "DB_TYPE": "mysql",
            "DB_HOST": "127.0.0.1",
            "DB_PORT": "3306",
            "DB_USERNAME": "bot",
            "DB_PASSWORD": "p@ss:/word",
            "DB_DATABASE": "poly_bot",
        }
        with patch.dict(os.environ, env, clear=True):
            url = DatabaseSettings.from_env().url()
        self.assertEqual(url.password, "p@ss:/word")
        self.assertNotIn("p@ss:/word", url.render_as_string(hide_password=True))

    def test_rejects_non_mysql_database(self) -> None:
        with patch.dict(os.environ, {"DB_TYPE": "sqlite"}, clear=True):
            with self.assertRaises(DatabaseConfigurationError):
                DatabaseSettings.from_env()


class RepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_trade_history_replace_is_atomic_snapshot(self) -> None:
        repository = TradeHistoryRepository(self.engine)
        repository.replace(
            "paper",
            [
                {
                    "trade_id": "paper_1",
                    "timestamp": "2026-07-14T10:00:00+00:00",
                    "outcome": "PENDING",
                    "filled_qty": 20.0,
                }
            ],
        )
        self.assertEqual(repository.load("paper")[0]["filled_qty"], 20.0)

        repository.replace("paper", [])
        self.assertEqual(repository.load("paper"), [])

    def test_trade_history_upsert_only_updates_target_trade(self) -> None:
        repository = TradeHistoryRepository(self.engine)
        repository.replace(
            "paper",
            [
                {
                    "trade_id": "paper_1",
                    "timestamp": "2026-07-14T10:00:00+00:00",
                    "outcome": "PENDING",
                },
                {
                    "trade_id": "paper_2",
                    "timestamp": "2026-07-14T10:01:00+00:00",
                    "outcome": "PENDING",
                },
            ],
        )
        repository.upsert(
            "paper",
            [
                {
                    "trade_id": "paper_2",
                    "timestamp": "2026-07-14T10:01:00+00:00",
                    "outcome": "WIN",
                }
            ],
        )
        trades = repository.load("paper")
        self.assertEqual(len(trades), 2)
        self.assertEqual(trades[0]["outcome"], "PENDING")
        self.assertEqual(trades[1]["outcome"], "WIN")

    def test_settle_pending_is_conditional_and_idempotent(self) -> None:
        repository = TradeHistoryRepository(self.engine)
        pending = {
            "trade_id": "paper_1",
            "timestamp": "2026-07-14T10:00:00+00:00",
            "outcome": "PENDING",
        }
        repository.upsert("paper", [pending])

        settled = {**pending, "outcome": "WIN", "pnl_usd": 10.0}
        self.assertTrue(repository.settle_pending("paper", settled))
        self.assertFalse(repository.settle_pending("paper", {**settled, "pnl_usd": -10.0}))
        self.assertEqual(repository.load("paper")[0]["pnl_usd"], 10.0)

    def test_load_pending_filters_in_database(self) -> None:
        repository = TradeHistoryRepository(self.engine)
        repository.upsert(
            "paper",
            [
                {
                    "trade_id": "pending",
                    "timestamp": "2026-07-14T10:00:00+00:00",
                    "outcome": "PENDING",
                },
                {
                    "trade_id": "settled",
                    "timestamp": "2026-07-14T10:01:00+00:00",
                    "outcome": "WIN",
                },
            ],
        )
        self.assertEqual(
            [row["trade_id"] for row in repository.load_pending("paper")],
            ["pending"],
        )

    def test_order_lifecycle_tracks_partial_and_full_fill(self) -> None:
        repository = OrderLifecycleRepository(self.engine)
        repository.record_submitted(
            mode="live",
            order_id="entry-1",
            role="entry",
            requested_usd=10.0,
        )
        repository.record_fill(
            mode="live",
            order_id="entry-1",
            role="entry",
            filled_qty=10.0,
            filled_notional_usd=5.0,
        )
        partial = repository.stats("live", "entry")
        self.assertEqual(partial["filled_orders"], 1)
        self.assertEqual(partial["fully_filled_orders"], 0)
        self.assertEqual(partial["fill_rate_pct"], 100.0)
        self.assertEqual(partial["notional_fill_rate_pct"], 50.0)

        repository.record_fill(
            mode="live",
            order_id="entry-1",
            role="entry",
            filled_qty=10.0,
            filled_notional_usd=5.0,
        )
        complete = OrderLifecycleRepository(self.engine).stats("live", "entry")
        self.assertEqual(complete["fully_filled_orders"], 1)
        self.assertEqual(complete["full_fill_rate_pct"], 100.0)
        self.assertEqual(complete["notional_fill_rate_pct"], 100.0)

    def test_order_stats_isolate_mode_role_and_rejections(self) -> None:
        repository = OrderLifecycleRepository(self.engine)
        repository.record_submitted(
            mode="live", order_id="entry-1", role="entry", requested_usd=10.0
        )
        repository.record_terminal(
            mode="live",
            order_id="entry-1",
            role="entry",
            status="REJECTED",
            reason="no liquidity",
        )
        repository.record_submitted(
            mode="live", order_id="exit-1", role="exit", requested_qty=20.0
        )
        repository.record_fill(
            mode="live",
            order_id="exit-1",
            role="exit",
            filled_qty=20.0,
            filled_notional_usd=8.0,
        )
        repository.record_submitted(
            mode="paper", order_id="paper-1", role="entry", requested_usd=10.0
        )
        repository.record_fill(
            mode="paper",
            order_id="paper-1",
            role="entry",
            filled_qty=20.0,
            filled_notional_usd=10.0,
        )

        live_entry = repository.stats("live", "entry")
        self.assertEqual(live_entry["submitted_total"], 1)
        self.assertEqual(live_entry["filled_orders"], 0)
        self.assertEqual(live_entry["rejected_orders"], 1)
        self.assertEqual(repository.stats("live", "exit")["filled_orders"], 1)
        self.assertEqual(repository.stats("paper", "entry")["filled_orders"], 1)

    def test_partial_fill_remains_a_fill_after_cancel(self) -> None:
        repository = OrderLifecycleRepository(self.engine)
        repository.record_submitted(
            mode="live", order_id="partial", role="entry", requested_usd=10.0
        )
        repository.record_fill(
            mode="live",
            order_id="partial",
            role="entry",
            filled_qty=5.0,
            filled_notional_usd=2.0,
        )
        repository.record_terminal(
            mode="live",
            order_id="partial",
            role="entry",
            status="CANCELED",
        )
        stats = repository.stats("live", "entry")
        self.assertEqual(stats["filled_orders"], 1)
        self.assertEqual(stats["fully_filled_orders"], 0)
        self.assertEqual(stats["fill_rate_pct"], 100.0)
        self.assertEqual(stats["notional_fill_rate_pct"], 20.0)

    def test_dashboard_state_round_trip(self) -> None:
        repository = DashboardStateRepository(self.engine)
        state = {"history": [{"ts": "now"}], "events": [{"type": "BUY"}]}
        repository.save(state)
        self.assertEqual(repository.load(), state)

    def test_signal_recorder_stats_do_not_query_database_repeatedly(self) -> None:
        recorder = SignalRecorder(engine=self.engine)
        statements = []

        def capture_statement(*args) -> None:
            statements.append(str(args[2]))

        event.listen(self.engine, "before_cursor_execute", capture_statement)
        try:
            for _ in range(10):
                recorder.get_stats()
        finally:
            event.remove(self.engine, "before_cursor_execute", capture_statement)

        self.assertEqual(statements, [])

    def test_signal_recorder_skips_resolution_query_without_pending_rows(self) -> None:
        price_calls = []
        recorder = SignalRecorder(
            engine=self.engine,
            price_fn=lambda: price_calls.append(True) or 100_000.0,
        )
        statements = []

        def capture_statement(*args) -> None:
            statements.append(str(args[2]))

        event.listen(self.engine, "before_cursor_execute", capture_statement)
        try:
            recorder._resolve_pending()
        finally:
            event.remove(self.engine, "before_cursor_execute", capture_statement)

        self.assertEqual(price_calls, [])
        self.assertEqual(statements, [])

    def test_signal_recorder_updates_cached_counts_on_write_and_resolution(self) -> None:
        recorder = SignalRecorder(engine=self.engine, price_fn=lambda: 101.0)
        recorder.record_cycle(
            market_slug="btc-test",
            market_start_ts=time.time() - 60,
            market_end_ts=time.time() - 1,
            poly_price=0.5,
            btc_spot=100.0,
            signals=[],
            fused=None,
            ml_p_up=None,
        )
        self.assertEqual(recorder.get_stats()["pending_resolution"], 1)

        recorder._resolve_pending()
        stats = recorder.get_stats()
        self.assertEqual(stats["total_cycles"], 1)
        self.assertEqual(stats["pending_resolution"], 0)
        self.assertEqual(stats["resolved_cycles"], 1)


if __name__ == "__main__":
    unittest.main()
