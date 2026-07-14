"""初始化 MySQL，并从旧 SQLite/JSON 文件迁移历史数据。

用法：
    python scripts/migrate_to_mysql.py

脚本只会向空的目标表导入旧数据，不会覆盖已有 MySQL 记录，也不会删除
本地旧文件。重复执行是安全的。
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List

from sqlalchemy import Double, func, insert, inspect, select

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.database import (
    DashboardStateRepository,
    TradeHistoryRepository,
    dashboard_events,
    dashboard_history,
    dashboard_states,
    initialize_database,
    ml_feature_trades,
    signal_cycles,
    trade_history,
)


def _destination_count(engine, table, *conditions) -> int:
    stmt = select(func.count()).select_from(table)
    if conditions:
        stmt = stmt.where(*conditions)
    with engine.connect() as conn:
        return int(conn.execute(stmt).scalar_one())


def _read_sqlite_rows(path: Path, table_name: str) -> List[Dict]:
    if not path.exists():
        return []
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        if not exists:
            return []
        return [dict(row) for row in conn.execute(f'SELECT * FROM "{table_name}"')]


def _insert_chunks(engine, table, rows: Iterable[Dict], chunk_size: int = 500) -> int:
    items = list(rows)
    with engine.begin() as conn:
        for start in range(0, len(items), chunk_size):
            conn.execute(insert(table), items[start:start + chunk_size])
    return len(items)


def upgrade_numeric_precision(engine) -> None:
    """将早期 MySQL FLOAT 列原地升级为 DOUBLE，避免行情精度损失。"""
    if engine.dialect.name not in {"mysql", "mariadb"}:
        return
    inspector = inspect(engine)
    upgraded = 0
    for table in (signal_cycles, ml_feature_trades):
        actual_columns = {item["name"]: item for item in inspector.get_columns(table.name)}
        for column in table.columns:
            if not isinstance(column.type, Double):
                continue
            actual = actual_columns.get(column.name)
            if actual is None or actual["type"].__class__.__name__.upper() != "FLOAT":
                continue
            nullable = "NULL" if column.nullable else "NOT NULL"
            with engine.begin() as conn:
                conn.exec_driver_sql(
                    f"ALTER TABLE `{table.name}` MODIFY COLUMN `{column.name}` DOUBLE {nullable}"
                )
            upgraded += 1
    if upgraded:
        print(f"- 已将 {upgraded} 个 FLOAT 列升级为 DOUBLE")


def migrate_signal_cycles(engine) -> int:
    if _destination_count(engine, signal_cycles):
        print("- signal_cycles 已有数据，跳过旧 SQLite")
        return 0
    rows = _read_sqlite_rows(ROOT_DIR / "signal_recordings.db", "signal_cycles")
    if not rows:
        print("- 未发现旧 signal_recordings.db 数据")
        return 0
    allowed = set(signal_cycles.c.keys())
    count = _insert_chunks(
        engine,
        signal_cycles,
        ({key: value for key, value in row.items() if key in allowed} for row in rows),
    )
    print(f"- 已迁移 {count} 条策略信号周期")
    return count


def migrate_ml_features(engine) -> int:
    if _destination_count(engine, ml_feature_trades):
        print("- ml_feature_trades 已有数据，跳过旧 SQLite")
        return 0
    rows = _read_sqlite_rows(ROOT_DIR / "feature_store.db", "trades")
    if not rows:
        print("- 未发现旧 feature_store.db 数据")
        return 0
    now = datetime.now(timezone.utc).isoformat()
    allowed = set(ml_feature_trades.c.keys())
    normalized = []
    for row in rows:
        item = {key: value for key, value in row.items() if key in allowed}
        item["created_at"] = str(item.get("created_at") or now)
        normalized.append(item)
    count = _insert_chunks(engine, ml_feature_trades, normalized)
    print(f"- 已迁移 {count} 条 ML 特征样本")
    return count


def _load_json_list(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError(f"{path.name} 根节点必须是数组")
    return [item for item in raw if isinstance(item, dict)]


def migrate_trade_history(engine) -> int:
    repository = TradeHistoryRepository(engine)
    total = 0
    for trade_type, filename in (("paper", "paper_trades.json"), ("live", "live_trades.json")):
        existing = _destination_count(
            engine,
            trade_history,
            trade_history.c.trade_type == trade_type,
        )
        if existing:
            print(f"- {trade_type} 交易已有 {existing} 条，跳过旧 JSON")
            continue
        rows = _load_json_list(ROOT_DIR / filename)
        if not rows:
            print(f"- 未发现旧 {filename} 数据")
            continue
        count = repository.replace(trade_type, rows)
        total += count
        print(f"- 已迁移 {count} 条 {trade_type} 交易")
    return total


def migrate_dashboard_state(engine) -> int:
    if _destination_count(engine, dashboard_history) or _destination_count(
        engine, dashboard_events
    ):
        print("- 驾驶舱增量历史已有数据，跳过旧状态文件")
        return 0

    raw = None
    with engine.connect() as conn:
        legacy_payload = conn.execute(
            select(dashboard_states.c.payload_json).where(
                dashboard_states.c.state_key == "main"
            )
        ).scalar_one_or_none()
    if legacy_payload:
        candidate = json.loads(legacy_payload)
        if isinstance(candidate, dict) and (
            candidate.get("history") or candidate.get("events")
        ):
            raw = candidate

    path = ROOT_DIR / "runtime" / "dashboard" / "state.json"
    if raw is None and path.exists():
        candidate = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(candidate, dict):
            raw = candidate
    if raw is None:
        print("- 未发现旧驾驶舱状态文件")
        return 0

    DashboardStateRepository(
        engine,
        history_limit=max(1, len(raw.get("history", []))),
        event_limit=max(1, len(raw.get("events", []))),
    ).save(raw)
    print(
        f"- 已迁移驾驶舱历史 {len(raw.get('history', []))} 条、"
        f"事件 {len(raw.get('events', []))} 条"
    )
    return 1


def main() -> None:
    print("正在初始化 MySQL 表结构……")
    engine = initialize_database(force=True)
    upgrade_numeric_precision(engine)
    migrated = 0
    migrated += migrate_signal_cycles(engine)
    migrated += migrate_ml_features(engine)
    migrated += migrate_trade_history(engine)
    migrated += migrate_dashboard_state(engine)
    print(f"MySQL 初始化完成，本次迁移 {migrated} 条/组记录。")


if __name__ == "__main__":
    main()
