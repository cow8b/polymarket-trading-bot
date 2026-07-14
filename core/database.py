"""项目统一的 MySQL 连接、表结构与持久化仓库。"""
from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    case,
    Column,
    Double,
    func,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    delete,
    inspect,
    insert,
    select,
    update,
)
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.engine import Engine, URL

load_dotenv()


class DatabaseConfigurationError(RuntimeError):
    """数据库配置缺失或不支持。"""


class DatabaseSchemaError(RuntimeError):
    """数据库表结构尚未初始化。"""


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return max(minimum, int(raw))
    except ValueError as exc:
        raise DatabaseConfigurationError(f"{name} 必须是整数") from exc


@dataclass(frozen=True)
class DatabaseSettings:
    """从环境变量读取的 MySQL 连接配置。"""

    host: str
    port: int
    username: str
    password: str
    database: str
    echo: bool
    pool_size: int
    max_overflow: int
    pool_recycle: int
    pool_timeout: int
    auto_create_tables: bool

    @classmethod
    def from_env(cls) -> "DatabaseSettings":
        db_type = (os.getenv("DB_TYPE") or "mysql").strip().lower()
        if db_type not in {"mysql", "mariadb"}:
            raise DatabaseConfigurationError(
                f"DB_TYPE={db_type!r} 不受支持；当前持久化层只支持 MySQL/MariaDB"
            )

        required = {
            "DB_HOST": os.getenv("DB_HOST"),
            "DB_USERNAME": os.getenv("DB_USERNAME"),
            "DB_PASSWORD": os.getenv("DB_PASSWORD"),
            "DB_DATABASE": os.getenv("DB_DATABASE"),
        }
        missing = [key for key, value in required.items() if value is None or not value.strip()]
        if missing:
            raise DatabaseConfigurationError(
                "缺少 MySQL 配置：" + ", ".join(missing)
            )

        return cls(
            host=required["DB_HOST"].strip(),
            port=_env_int("DB_PORT", 3306, 1),
            username=required["DB_USERNAME"].strip(),
            password=required["DB_PASSWORD"],
            database=required["DB_DATABASE"].strip(),
            echo=_env_bool("DB_ECHO", False),
            pool_size=_env_int("DB_POOL_SIZE", 10, 1),
            max_overflow=_env_int("DB_MAX_OVERFLOW", 10),
            pool_recycle=_env_int("DB_POOL_RECYCLE", 3600, 1),
            pool_timeout=_env_int("DB_POOL_TIMEOUT", 30, 1),
            auto_create_tables=_env_bool("DB_AUTO_CREATE_TABLES", False),
        )

    def url(self) -> URL:
        # URL.create 会正确转义密码中的 @、:、/ 等字符。
        return URL.create(
            "mysql+pymysql",
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            query={"charset": "utf8mb4"},
        )


metadata = MetaData()
long_text = Text().with_variant(LONGTEXT(), "mysql")
bigint_primary_key = BigInteger().with_variant(Integer, "sqlite")

signal_cycles = Table(
    "signal_cycles",
    metadata,
    Column("id", bigint_primary_key, primary_key=True, autoincrement=True),
    Column("recorded_at", String(40), nullable=False),
    Column("market_slug", String(255), nullable=False),
    Column("market_start_ts", Double),
    Column("market_end_ts", Double),
    Column("poly_price", Double),
    Column("btc_spot", Double),
    Column("ml_p_up", Double),
    Column("signals_json", long_text, nullable=False),
    Column("fused_json", long_text),
    Column("metadata_json", long_text),
    Column("btc_entry", Double),
    Column("btc_exit", Double),
    Column("outcome", Integer),
    Column("resolved_at", String(40)),
    Index("idx_signal_cycles_pending", "outcome", "market_end_ts"),
    Index("idx_signal_cycles_market", "market_slug", "market_start_ts", "market_end_ts"),
)

ML_FEATURE_NAMES = [
    "rsi", "macd_line", "macd_signal", "pct_b", "ret1", "ret3", "ret5",
    "ret15", "vol_regime", "cvd_delta_norm", "ob_imbalance", "funding_rate",
    "oi_change", "liq_imbalance", "liq_total_norm", "tick_vel_60s",
    "tick_vel_30s", "poly_ob_imbalance", "spot_momentum", "poly_prob",
    "hour_sin", "hour_cos", "is_ny_open", "is_asia_open", "is_dead_zone",
]

ml_feature_trades = Table(
    "ml_feature_trades",
    metadata,
    Column("id", bigint_primary_key, primary_key=True, autoincrement=True),
    Column("timestamp", String(40), nullable=False),
    Column("market_slug", String(255)),
    Column("poly_price", Double, nullable=False),
    *(Column(name, Double) for name in ML_FEATURE_NAMES),
    Column("outcome", Integer),
    Column("chainlink_entry", Double),
    Column("chainlink_exit", Double),
    Column("created_at", String(40), nullable=False),
    Index("idx_ml_feature_trades_outcome", "outcome", "id"),
)

trade_history = Table(
    "trade_history",
    metadata,
    Column("trade_type", String(16), primary_key=True),
    Column("trade_id", String(191), primary_key=True),
    Column("timestamp", String(40), nullable=False),
    Column("outcome", String(32), nullable=False),
    Column("payload_json", long_text, nullable=False),
    Column("updated_at", String(40), nullable=False),
    Index("idx_trade_history_timeline", "trade_type", "timestamp"),
)

order_lifecycle = Table(
    "order_lifecycle",
    metadata,
    Column("mode", String(16), primary_key=True),
    Column("order_id", String(191), primary_key=True),
    Column("role", String(16), nullable=False),
    Column("market_slug", String(255), nullable=False, default=""),
    Column("direction", String(16), nullable=False, default=""),
    Column("status", String(32), nullable=False),
    Column("requested_usd", Double, nullable=False, default=0.0),
    Column("requested_qty", Double, nullable=False, default=0.0),
    Column("filled_notional_usd", Double, nullable=False, default=0.0),
    Column("filled_qty", Double, nullable=False, default=0.0),
    Column("fill_count", Integer, nullable=False, default=0),
    Column("rejection_reason", Text),
    Column("submitted_at", String(40), nullable=False),
    Column("first_fill_at", String(40)),
    Column("last_fill_at", String(40)),
    Column("terminal_at", String(40)),
    Column("updated_at", String(40), nullable=False),
    Index("idx_order_lifecycle_stats", "mode", "role", "status", "submitted_at"),
)

dashboard_states = Table(
    "dashboard_states",
    metadata,
    Column("state_key", String(64), primary_key=True),
    Column("payload_json", long_text, nullable=False),
    Column("updated_at", String(40), nullable=False),
)

dashboard_history = Table(
    "dashboard_history",
    metadata,
    Column("sample_ts", String(40), primary_key=True),
    Column("payload_json", long_text, nullable=False),
)

dashboard_events = Table(
    "dashboard_events",
    metadata,
    Column("sequence", bigint_primary_key, primary_key=True),
    Column("event_ts", String(40), nullable=False),
    Column("payload_json", long_text, nullable=False),
    Index("idx_dashboard_events_ts", "event_ts"),
)


_engine: Optional[Engine] = None
_engine_lock = threading.Lock()
_schema_ready = False


def get_database_engine() -> Engine:
    """返回进程级 MySQL 连接池；首次调用时才创建。"""
    global _engine
    if _engine is not None:
        return _engine
    with _engine_lock:
        if _engine is None:
            settings = DatabaseSettings.from_env()
            _engine = create_engine(
                settings.url(),
                echo=settings.echo,
                pool_pre_ping=True,
                pool_size=settings.pool_size,
                max_overflow=settings.max_overflow,
                pool_recycle=settings.pool_recycle,
                pool_timeout=settings.pool_timeout,
                future=True,
            )
    return _engine


def initialize_database(*, force: bool = False, engine: Optional[Engine] = None) -> Engine:
    """检查表结构；显式 force 或配置允许时创建缺失表。"""
    global _schema_ready
    if engine is None and _schema_ready and not force:
        return get_database_engine()

    settings = DatabaseSettings.from_env() if engine is None else None
    # 建库属于部署动作，仅显式迁移命令（force=True）执行；机器人启动只建表。
    if settings is not None and force:
        admin_engine = create_engine(
            settings.url().set(database=None),
            pool_pre_ping=True,
            future=True,
        )
        database_name = settings.database.replace("`", "``")
        try:
            with admin_engine.begin() as conn:
                conn.exec_driver_sql(
                    f"CREATE DATABASE IF NOT EXISTS `{database_name}` "
                    "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
        finally:
            admin_engine.dispose()

    db_engine = engine or get_database_engine()
    if force or (settings is not None and settings.auto_create_tables):
        metadata.create_all(db_engine)

    existing = set(inspect(db_engine).get_table_names())
    missing = sorted(set(metadata.tables) - existing)
    if missing:
        raise DatabaseSchemaError(
            "MySQL 缺少数据表："
            + ", ".join(missing)
            + "；请先运行 python scripts/migrate_to_mysql.py"
        )
    if engine is None:
        _schema_ready = True
    return db_engine


def dispose_database_engine() -> None:
    """测试或停机时释放连接池。"""
    global _engine, _schema_ready
    with _engine_lock:
        if _engine is not None:
            _engine.dispose()
            _engine = None
        _schema_ready = False


class TradeHistoryRepository:
    """模拟与实盘成交历史的 MySQL 仓库。"""

    def __init__(self, engine: Optional[Engine] = None):
        self.engine = initialize_database(engine=engine)

    def load(self, trade_type: str) -> List[Dict[str, Any]]:
        stmt = (
            select(trade_history.c.payload_json)
            .where(trade_history.c.trade_type == trade_type)
            .order_by(trade_history.c.timestamp, trade_history.c.trade_id)
        )
        with self.engine.connect() as conn:
            payloads = conn.execute(stmt).scalars().all()
        return [json.loads(payload) for payload in payloads]

    def load_pending(
        self,
        trade_type: str,
        trade_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """只读取待结算交易，避免补结算命令扫描完整历史。"""
        stmt = select(trade_history.c.payload_json).where(
            trade_history.c.trade_type == trade_type,
            trade_history.c.outcome == "PENDING",
        )
        if trade_id:
            stmt = stmt.where(trade_history.c.trade_id == trade_id)
        stmt = stmt.order_by(trade_history.c.timestamp, trade_history.c.trade_id)
        with self.engine.connect() as conn:
            payloads = conn.execute(stmt).scalars().all()
        return [json.loads(payload) for payload in payloads]

    @staticmethod
    def _serialize_rows(
        trade_type: str, trades: Iterable[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for item in trades:
            trade_id = str(item.get("trade_id", "")).strip()
            if not trade_id:
                continue
            rows.append(
                {
                    "trade_type": trade_type,
                    "trade_id": trade_id,
                    "timestamp": str(item.get("timestamp") or now),
                    "outcome": str(item.get("outcome") or "UNKNOWN"),
                    "payload_json": json.dumps(item, ensure_ascii=False, separators=(",", ":")),
                    "updated_at": now,
                }
            )
        return rows

    def replace(self, trade_type: str, trades: Iterable[Dict[str, Any]]) -> int:
        rows = self._serialize_rows(trade_type, trades)
        with self.engine.begin() as conn:
            conn.execute(delete(trade_history).where(trade_history.c.trade_type == trade_type))
            if rows:
                conn.execute(insert(trade_history), rows)
        return len(rows)

    def upsert(self, trade_type: str, trades: Iterable[Dict[str, Any]]) -> int:
        """仅写入新增或变化的交易，避免交易越多时反复重写整张表。"""
        rows = self._serialize_rows(trade_type, trades)
        with self.engine.begin() as conn:
            for row in rows:
                result = conn.execute(
                    update(trade_history)
                    .where(
                        trade_history.c.trade_type == row["trade_type"],
                        trade_history.c.trade_id == row["trade_id"],
                    )
                    .values(
                        timestamp=row["timestamp"],
                        outcome=row["outcome"],
                        payload_json=row["payload_json"],
                        updated_at=row["updated_at"],
                    )
                )
                if result.rowcount == 0:
                    conn.execute(insert(trade_history), row)
        return len(rows)

    def settle_pending(self, trade_type: str, trade: Dict[str, Any]) -> bool:
        """仅在记录仍为 PENDING 时写入结算结果，防止并发覆盖。"""
        rows = self._serialize_rows(trade_type, [trade])
        if not rows:
            return False
        row = rows[0]
        with self.engine.begin() as conn:
            result = conn.execute(
                update(trade_history)
                .where(
                    trade_history.c.trade_type == row["trade_type"],
                    trade_history.c.trade_id == row["trade_id"],
                    trade_history.c.outcome == "PENDING",
                )
                .values(
                    timestamp=row["timestamp"],
                    outcome=row["outcome"],
                    payload_json=row["payload_json"],
                    updated_at=row["updated_at"],
                )
            )
        return result.rowcount == 1


class OrderLifecycleRepository:
    """订单生命周期事实表；用于可重启恢复的执行质量统计。"""

    TERMINAL_STATUSES = {"FILLED", "REJECTED", "DENIED", "CANCELED", "EXPIRED"}

    def __init__(self, engine: Optional[Engine] = None):
        self.engine = initialize_database(engine=engine)

    @staticmethod
    def _now() -> str:
        from datetime import datetime, timezone

        return datetime.now(timezone.utc).isoformat()

    def record_submitted(
        self,
        *,
        mode: str,
        order_id: str,
        role: str,
        market_slug: str = "",
        direction: str = "",
        requested_usd: float = 0.0,
        requested_qty: float = 0.0,
        submitted_at: Optional[str] = None,
    ) -> None:
        """登记一次订单提交；重复事件只补充元数据，不重置成交状态。"""
        now = self._now()
        row = {
            "mode": mode,
            "order_id": order_id,
            "role": role,
            "market_slug": market_slug,
            "direction": direction,
            "status": "SUBMITTED",
            "requested_usd": max(0.0, float(requested_usd or 0.0)),
            "requested_qty": max(0.0, float(requested_qty or 0.0)),
            "filled_notional_usd": 0.0,
            "filled_qty": 0.0,
            "fill_count": 0,
            "submitted_at": submitted_at or now,
            "updated_at": now,
        }
        with self.engine.begin() as conn:
            existing = conn.execute(
                select(order_lifecycle.c.status).where(
                    order_lifecycle.c.mode == mode,
                    order_lifecycle.c.order_id == order_id,
                )
            ).scalar_one_or_none()
            if existing is None:
                conn.execute(insert(order_lifecycle), row)
                return
            conn.execute(
                update(order_lifecycle)
                .where(
                    order_lifecycle.c.mode == mode,
                    order_lifecycle.c.order_id == order_id,
                )
                .values(
                    role=role,
                    market_slug=market_slug,
                    direction=direction,
                    requested_usd=row["requested_usd"],
                    requested_qty=row["requested_qty"],
                    updated_at=now,
                )
            )

    def record_fill(
        self,
        *,
        mode: str,
        order_id: str,
        role: str,
        filled_qty: float,
        filled_notional_usd: float,
        market_slug: str = "",
        direction: str = "",
    ) -> None:
        """累加部分成交；达到请求数量或金额时标记为完整成交。"""
        now = self._now()
        qty_delta = max(0.0, float(filled_qty or 0.0))
        notional_delta = max(0.0, float(filled_notional_usd or 0.0))
        with self.engine.begin() as conn:
            existing = conn.execute(
                select(order_lifecycle).where(
                    order_lifecycle.c.mode == mode,
                    order_lifecycle.c.order_id == order_id,
                ).with_for_update()
            ).mappings().one_or_none()
            if existing is None:
                conn.execute(
                    insert(order_lifecycle),
                    {
                        "mode": mode,
                        "order_id": order_id,
                        "role": role,
                        "market_slug": market_slug,
                        "direction": direction,
                        "status": "FILLED",
                        "requested_usd": 0.0,
                        "requested_qty": 0.0,
                        "filled_notional_usd": notional_delta,
                        "filled_qty": qty_delta,
                        "fill_count": 1,
                        "submitted_at": now,
                        "first_fill_at": now,
                        "last_fill_at": now,
                        "terminal_at": now,
                        "updated_at": now,
                    },
                )
                return

            total_qty = float(existing["filled_qty"] or 0.0) + qty_delta
            total_notional = float(existing["filled_notional_usd"] or 0.0) + notional_delta
            requested_qty = float(existing["requested_qty"] or 0.0)
            requested_usd = float(existing["requested_usd"] or 0.0)
            complete = (
                (requested_qty > 0 and total_qty >= requested_qty * 0.995)
                or (requested_qty <= 0 and requested_usd > 0 and total_notional >= requested_usd * 0.995)
                or (requested_qty <= 0 and requested_usd <= 0)
            )
            values: Dict[str, Any] = {
                "filled_qty": total_qty,
                "filled_notional_usd": total_notional,
                "fill_count": int(existing["fill_count"] or 0) + 1,
                "status": "FILLED" if complete else "PARTIAL",
                "first_fill_at": existing["first_fill_at"] or now,
                "last_fill_at": now,
                "updated_at": now,
            }
            if complete:
                values["terminal_at"] = now
            conn.execute(
                update(order_lifecycle)
                .where(
                    order_lifecycle.c.mode == mode,
                    order_lifecycle.c.order_id == order_id,
                )
                .values(**values)
            )

    def record_terminal(
        self,
        *,
        mode: str,
        order_id: str,
        role: str,
        status: str,
        reason: str = "",
    ) -> None:
        """登记拒绝、取消等终态；已有成交数量不会被清空。"""
        normalized = status.strip().upper()
        if normalized not in self.TERMINAL_STATUSES:
            raise ValueError(f"不支持的订单终态：{status}")
        now = self._now()
        with self.engine.begin() as conn:
            result = conn.execute(
                update(order_lifecycle)
                .where(
                    order_lifecycle.c.mode == mode,
                    order_lifecycle.c.order_id == order_id,
                )
                .values(
                    status=normalized,
                    rejection_reason=reason or None,
                    terminal_at=now,
                    updated_at=now,
                )
            )
            if result.rowcount == 0:
                conn.execute(
                    insert(order_lifecycle),
                    {
                        "mode": mode,
                        "order_id": order_id,
                        "role": role,
                        "market_slug": "",
                        "direction": "",
                        "status": normalized,
                        "requested_usd": 0.0,
                        "requested_qty": 0.0,
                        "filled_notional_usd": 0.0,
                        "filled_qty": 0.0,
                        "fill_count": 0,
                        "rejection_reason": reason or None,
                        "submitted_at": now,
                        "terminal_at": now,
                        "updated_at": now,
                    },
                )

    def stats(self, mode: str, role: str = "entry") -> Dict[str, Any]:
        """聚合指定模式与订单角色；仅在订单事件或启动恢复时调用。"""
        stmt = select(
            func.count().label("submitted_total"),
            func.coalesce(
                func.sum(
                    case(
                        (
                            (order_lifecycle.c.filled_qty > 0)
                            | (order_lifecycle.c.filled_notional_usd > 0),
                            1,
                        ),
                        else_=0,
                    )
                ),
                0,
            ).label("filled_orders"),
            func.coalesce(
                func.sum(case((order_lifecycle.c.status == "FILLED", 1), else_=0)), 0
            ).label("fully_filled_orders"),
            func.coalesce(
                func.sum(
                    case(
                        (order_lifecycle.c.status.in_(("REJECTED", "DENIED")), 1),
                        else_=0,
                    )
                ),
                0,
            ).label("rejected_orders"),
            func.coalesce(func.sum(order_lifecycle.c.requested_usd), 0.0).label(
                "requested_notional_usd"
            ),
            func.coalesce(func.sum(order_lifecycle.c.filled_notional_usd), 0.0).label(
                "filled_notional_usd"
            ),
            func.min(order_lifecycle.c.submitted_at).label("stats_since"),
        ).where(
            order_lifecycle.c.mode == mode,
            order_lifecycle.c.role == role,
        )
        with self.engine.connect() as conn:
            row = conn.execute(stmt).mappings().one()
        submitted = int(row["submitted_total"] or 0)
        filled = int(row["filled_orders"] or 0)
        full = int(row["fully_filled_orders"] or 0)
        requested = float(row["requested_notional_usd"] or 0.0)
        filled_notional = float(row["filled_notional_usd"] or 0.0)
        return {
            "mode": mode,
            "role": role,
            "submitted_total": submitted,
            "filled_orders": filled,
            "fully_filled_orders": full,
            "rejected_orders": int(row["rejected_orders"] or 0),
            "fill_rate_pct": (filled / submitted * 100.0) if submitted else None,
            "full_fill_rate_pct": (full / submitted * 100.0) if submitted else None,
            "notional_fill_rate_pct": (
                min(100.0, filled_notional / requested * 100.0)
                if requested > 0
                else None
            ),
            "requested_notional_usd": requested,
            "filled_notional_usd": filled_notional,
            "stats_since": row["stats_since"],
        }


class DashboardStateRepository:
    """驾驶舱快照仓库；时间序列按增量行保存，避免反复写入大 JSON。"""

    def __init__(
        self,
        engine: Optional[Engine] = None,
        history_limit: int = 3600,
        event_limit: int = 200,
    ):
        self.engine = initialize_database(engine=engine)
        self.history_limit = max(1, history_limit)
        self.event_limit = max(1, event_limit)
        self._last_history_ts: Optional[str] = None
        self._last_event_sequence: Optional[int] = None
        self._last_pruned_at = 0.0

    def load(self, state_key: str = "main") -> Optional[Dict[str, Any]]:
        stmt = select(dashboard_states.c.payload_json).where(
            dashboard_states.c.state_key == state_key
        )
        with self.engine.connect() as conn:
            payload = conn.execute(stmt).scalar_one_or_none()
            history_payloads = conn.execute(
                select(dashboard_history.c.payload_json)
                .order_by(dashboard_history.c.sample_ts.desc())
                .limit(self.history_limit)
            ).scalars().all()
            event_payloads = conn.execute(
                select(dashboard_events.c.payload_json)
                .order_by(dashboard_events.c.sequence.desc())
                .limit(self.event_limit)
            ).scalars().all()

        state = json.loads(payload) if payload else {}
        if history_payloads:
            state["history"] = [json.loads(item) for item in reversed(history_payloads)]
            self._last_history_ts = str(state["history"][-1].get("ts") or "") or None
        if event_payloads:
            state["events"] = [json.loads(item) for item in reversed(event_payloads)]
            try:
                self._last_event_sequence = max(
                    int(item.get("sequence", 0) or 0) for item in state["events"]
                )
            except (TypeError, ValueError):
                self._last_event_sequence = None
        return state or None

    def save(self, payload: Dict[str, Any], state_key: str = "main") -> None:
        from datetime import datetime, timezone
        history = [item for item in payload.get("history", []) if isinstance(item, dict)]
        events = [item for item in payload.get("events", []) if isinstance(item, dict)]
        state_payload = dict(payload)
        # 大数组拆分为增量行，主状态行只保留版本和保存时间等小型元数据。
        state_payload.pop("history", None)
        state_payload.pop("events", None)
        now = datetime.now(timezone.utc).isoformat()
        row = {
            "state_key": state_key,
            "payload_json": json.dumps(
                state_payload, ensure_ascii=False, separators=(",", ":")
            ),
            "updated_at": now,
        }
        next_history_ts: Optional[str] = None
        next_event_sequence: Optional[int] = None
        pruned_at: Optional[float] = None
        with self.engine.begin() as conn:
            if self._last_history_ts is None:
                from sqlalchemy import func

                self._last_history_ts = conn.execute(
                    select(func.max(dashboard_history.c.sample_ts))
                ).scalar_one_or_none()
            new_history = []
            for item in history:
                sample_ts = str(item.get("ts") or "")
                if sample_ts and (
                    self._last_history_ts is None or sample_ts > self._last_history_ts
                ):
                    new_history.append(
                        {
                            "sample_ts": sample_ts,
                            "payload_json": json.dumps(
                                item, ensure_ascii=False, separators=(",", ":")
                            ),
                        }
                    )
            if new_history:
                conn.execute(insert(dashboard_history), new_history)
                next_history_ts = new_history[-1]["sample_ts"]

            if self._last_event_sequence is None:
                from sqlalchemy import func

                self._last_event_sequence = conn.execute(
                    select(func.max(dashboard_events.c.sequence))
                ).scalar_one_or_none()
            new_events = []
            for item in events:
                try:
                    sequence = int(item.get("sequence", 0) or 0)
                except (TypeError, ValueError):
                    sequence = 0
                if sequence <= 0:
                    sequence = int(self._last_event_sequence or 0) + len(new_events) + 1
                if (
                    self._last_event_sequence is not None
                    and sequence <= self._last_event_sequence
                ):
                    continue
                new_events.append(
                    {
                        "sequence": sequence,
                        "event_ts": str(item.get("ts") or now),
                        "payload_json": json.dumps(
                            item, ensure_ascii=False, separators=(",", ":")
                        ),
                    }
                )
            if new_events:
                conn.execute(insert(dashboard_events), new_events)
                next_event_sequence = new_events[-1]["sequence"]

            result = conn.execute(
                update(dashboard_states)
                .where(dashboard_states.c.state_key == state_key)
                .values(
                    payload_json=row["payload_json"],
                    updated_at=row["updated_at"],
                )
            )
            if result.rowcount == 0:
                conn.execute(insert(dashboard_states), row)

            # 裁剪不是实时语义，每分钟执行一次即可，避免每 5 秒做两次边界查询。
            now_monotonic = time.monotonic()
            if now_monotonic - self._last_pruned_at >= 60.0:
                history_cutoff = conn.execute(
                    select(dashboard_history.c.sample_ts)
                    .order_by(dashboard_history.c.sample_ts.desc())
                    .offset(self.history_limit - 1)
                    .limit(1)
                ).scalar_one_or_none()
                if history_cutoff is not None:
                    conn.execute(
                        delete(dashboard_history).where(
                            dashboard_history.c.sample_ts < history_cutoff
                        )
                    )

                event_cutoff = conn.execute(
                    select(dashboard_events.c.sequence)
                    .order_by(dashboard_events.c.sequence.desc())
                    .offset(self.event_limit - 1)
                    .limit(1)
                ).scalar_one_or_none()
                if event_cutoff is not None:
                    conn.execute(
                        delete(dashboard_events).where(
                            dashboard_events.c.sequence < event_cutoff
                        )
                    )
                pruned_at = now_monotonic

        # 事务提交成功后才推进水位，失败时下一轮仍会重试相同增量。
        if next_history_ts is not None:
            self._last_history_ts = next_history_ts
        if next_event_sequence is not None:
            self._last_event_sequence = next_event_sequence
        if pruned_at is not None:
            self._last_pruned_at = pruned_at
