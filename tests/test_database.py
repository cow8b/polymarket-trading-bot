"""MySQL 持久化仓库的无网络单元测试。"""
from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from core.database import (
    DashboardStateRepository,
    DatabaseConfigurationError,
    DatabaseSettings,
    TradeHistoryRepository,
    metadata,
)


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

    def test_dashboard_state_round_trip(self) -> None:
        repository = DashboardStateRepository(self.engine)
        state = {"history": [{"ts": "now"}], "events": [{"type": "BUY"}]}
        repository.save(state)
        self.assertEqual(repository.load(), state)


if __name__ == "__main__":
    unittest.main()
