"""停机期间遗留交易的独立补结算服务。"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Protocol, Sequence
from urllib.parse import quote

import httpx

from core.database import TradeHistoryRepository


class MarketLookupError(RuntimeError):
    """Gamma 市场查询失败或响应结构异常。"""


@dataclass(frozen=True)
class MarketResolution:
    slug: str
    outcomes: tuple[str, ...]
    prices: tuple[float, ...]
    winning_index: Optional[int]
    closed: bool
    resolution_status: str
    end_at: Optional[datetime]

    @property
    def winning_outcome(self) -> Optional[str]:
        if self.winning_index is None:
            return None
        return self.outcomes[self.winning_index]

    @property
    def is_resolved(self) -> bool:
        return (
            self.closed
            and self.resolution_status == "resolved"
            and self.winning_index is not None
        )


@dataclass
class ReconciliationResult:
    trade_type: str
    trade_id: str
    market_slug: str
    status: str
    message: str
    direction: str = ""
    held_outcome: str = ""
    winning_outcome: str = ""
    pnl_usd: Optional[float] = None
    payload: Optional[Dict[str, Any]] = None


class MarketResolutionProvider(Protocol):
    def fetch_resolution(self, slug: str) -> MarketResolution:
        ...


def _as_list(value: Any, field: str) -> List[Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise MarketLookupError(f"{field} 不是有效 JSON") from exc
    if not isinstance(value, list):
        raise MarketLookupError(f"{field} 不是数组")
    return value


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes"}


def _parse_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_market_resolution(raw: Dict[str, Any]) -> MarketResolution:
    """解析 Gamma 市场，并只接受唯一的 1/0 二元结算结果。"""
    slug = str(raw.get("slug") or "").strip()
    if not slug:
        raise MarketLookupError("市场响应缺少 slug")

    outcomes = tuple(str(item).strip() for item in _as_list(raw.get("outcomes"), "outcomes"))
    price_values = _as_list(raw.get("outcomePrices"), "outcomePrices")
    try:
        prices = tuple(float(item) for item in price_values)
    except (TypeError, ValueError) as exc:
        raise MarketLookupError("outcomePrices 包含非数字值") from exc
    if len(outcomes) < 2 or len(outcomes) != len(prices):
        raise MarketLookupError("outcomes 与 outcomePrices 数量不一致")

    winners = [index for index, price in enumerate(prices) if price >= 0.99]
    losers_are_zero = all(
        price <= 0.01 for index, price in enumerate(prices) if index not in winners
    )
    winning_index = winners[0] if len(winners) == 1 and losers_are_zero else None
    return MarketResolution(
        slug=slug,
        outcomes=outcomes,
        prices=prices,
        winning_index=winning_index,
        closed=_as_bool(raw.get("closed")),
        resolution_status=str(raw.get("umaResolutionStatus") or "").strip().lower(),
        end_at=_parse_datetime(raw.get("endDate")),
    )


class GammaMarketClient:
    """只读查询 Polymarket Gamma 市场结果。"""

    def __init__(self, timeout: float = 15.0):
        base_url = os.getenv("GAMMA_API_URL", "https://gamma-api.polymarket.com")
        self.base_url = base_url.rstrip("/")
        client_args: Dict[str, Any] = {
            "timeout": timeout,
            "headers": {"User-Agent": "polymarket-trading-bot/reconciler"},
        }
        proxy_url = (os.getenv("POLYMARKET_PROXY_URL") or "").strip()
        if proxy_url:
            client_args["proxy"] = proxy_url
        self._client = httpx.Client(**client_args)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "GammaMarketClient":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def fetch_resolution(self, slug: str) -> MarketResolution:
        try:
            response = self._client.get(
                f"{self.base_url}/markets/slug/{quote(slug, safe='')}"
            )
            response.raise_for_status()
            raw = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise MarketLookupError(f"Gamma 查询失败：{exc}") from exc
        if not isinstance(raw, dict):
            raise MarketLookupError("Gamma 返回的市场不是对象")
        return parse_market_resolution(raw)


def _normalise_label(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", value.upper())


def held_outcome_index(trade: Dict[str, Any], outcomes: Sequence[str]) -> Optional[int]:
    """根据持仓标签和 LONG/SHORT 方向映射实际持有的结果股。"""
    normalised = [_normalise_label(item) for item in outcomes]
    label = _normalise_label(str(trade.get("label") or ""))
    if label:
        matches = [index for index, item in enumerate(normalised) if item and item in label]
        if len(matches) == 1:
            return matches[0]

    direction = str(trade.get("direction") or "").strip().upper()
    preferred = {
        "LONG": ("UP", "YES"),
        "UP": ("UP", "YES"),
        "SHORT": ("DOWN", "NO"),
        "DOWN": ("DOWN", "NO"),
    }.get(direction, ())
    matches = [
        index for index, item in enumerate(normalised) if item in preferred
    ]
    return matches[0] if len(matches) == 1 else None


def _slug_end_at(slug: str) -> Optional[datetime]:
    match = re.search(r"-(\d{10})$", slug)
    if not match:
        return None
    start_at = datetime.fromtimestamp(int(match.group(1)), tz=timezone.utc)
    return start_at + timedelta(minutes=15)


class OfflineTradeReconciler:
    """扫描 MySQL 中的 PENDING/UNRESOLVED 记录，按官方结果重算并可选写回。

    UNRESOLVED 是机器人在所有结算源（Chainlink/盘口 bid）都不可用时按入场价
    平账的降级记录；这里允许它与 PENDING 一样按 Gamma 官方结果重算。
    """

    def __init__(
        self,
        repository: TradeHistoryRepository,
        market_provider: MarketResolutionProvider,
        now_fn: Optional[Callable[[], datetime]] = None,
    ):
        self.repository = repository
        self.market_provider = market_provider
        self.now_fn = now_fn or (lambda: datetime.now(timezone.utc))

    def reconcile_once(
        self,
        trade_types: Iterable[str],
        *,
        trade_id: Optional[str] = None,
        older_than_minutes: float = 0.0,
        apply: bool = False,
    ) -> List[ReconciliationResult]:
        pending: List[tuple[str, Dict[str, Any]]] = []
        for trade_type in trade_types:
            pending.extend(
                (trade_type, trade)
                for trade in self.repository.load_pending(trade_type, trade_id)
            )

        cache: Dict[str, MarketResolution | Exception] = {}
        results: List[ReconciliationResult] = []
        now = self.now_fn().astimezone(timezone.utc)
        for trade_type, trade in pending:
            results.append(
                self._reconcile_trade(
                    trade_type,
                    trade,
                    cache=cache,
                    now=now,
                    older_than_minutes=older_than_minutes,
                    apply=apply,
                )
            )
        return results

    def _reconcile_trade(
        self,
        trade_type: str,
        trade: Dict[str, Any],
        *,
        cache: Dict[str, MarketResolution | Exception],
        now: datetime,
        older_than_minutes: float,
        apply: bool,
    ) -> ReconciliationResult:
        trade_id = str(trade.get("trade_id") or "")
        slug = str(trade.get("market_slug") or "").strip()
        base = dict(
            trade_type=trade_type,
            trade_id=trade_id,
            market_slug=slug,
            direction=str(trade.get("direction") or ""),
        )
        if not trade_id or not slug:
            return ReconciliationResult(
                **base, status="INVALID", message="缺少 trade_id 或 market_slug"
            )

        if slug not in cache:
            try:
                cache[slug] = self.market_provider.fetch_resolution(slug)
            except Exception as exc:  # 单个市场失败不能中断整批处理。
                cache[slug] = exc
        resolution = cache[slug]
        if isinstance(resolution, Exception):
            return ReconciliationResult(
                **base, status="ERROR", message=str(resolution)
            )

        market_end = resolution.end_at or _slug_end_at(slug)
        if market_end is not None:
            eligible_at = market_end + timedelta(minutes=older_than_minutes)
            if now < eligible_at:
                remaining = max(0, int((eligible_at - now).total_seconds()))
                return ReconciliationResult(
                    **base,
                    status="WAITING",
                    message=f"市场结束后仍需等待 {remaining} 秒",
                )
        if not resolution.is_resolved:
            return ReconciliationResult(
                **base,
                status="WAITING",
                message=(
                    f"市场尚未明确结算（closed={resolution.closed}, "
                    f"status={resolution.resolution_status or '-'}, "
                    f"prices={list(resolution.prices)}）"
                ),
            )

        held_index = held_outcome_index(trade, resolution.outcomes)
        if held_index is None:
            return ReconciliationResult(
                **base,
                status="INVALID",
                message=f"无法将方向映射到结果 {list(resolution.outcomes)}",
            )

        try:
            entry_price = float(trade.get("entry_price") or 0.0)
            quantity = float(trade.get("filled_qty") or 0.0)
            size_usd = float(trade.get("size_usd") or 0.0)
        except (TypeError, ValueError):
            entry_price = quantity = size_usd = 0.0
        if entry_price <= 0 or entry_price > 1:
            return ReconciliationResult(
                **base, status="INVALID", message=f"买入价无效：{entry_price}"
            )
        if quantity <= 0 and size_usd > 0:
            quantity = size_usd / entry_price
        if quantity <= 0:
            return ReconciliationResult(
                **base, status="INVALID", message="持仓股数无效"
            )

        exit_price = 1.0 if held_index == resolution.winning_index else 0.0
        pnl_usd = quantity * (exit_price - entry_price)
        pnl_pct = (exit_price - entry_price) / entry_price
        if pnl_usd > 1e-9:
            outcome = "WIN"
        elif pnl_usd < -1e-9:
            outcome = "LOSS"
        else:
            outcome = "BREAKEVEN"

        reconciled_at = now.isoformat()
        updated = dict(trade)
        updated.update(
            {
                "filled_qty": round(quantity, 6),
                "exit_price": exit_price,
                "pnl_usd": round(pnl_usd, 6),
                "pnl_pct": round(pnl_pct * 100, 4),
                "outcome": outcome,
                "close_reason": "OFFLINE_SETTLEMENT",
                "closed_at": reconciled_at,
                "reconciliation": {
                    "source": "polymarket_gamma",
                    "reconciled_at": reconciled_at,
                    "market_end_at": market_end.isoformat() if market_end else None,
                    "winning_outcome": resolution.winning_outcome,
                    "outcome_prices": list(resolution.prices),
                },
            }
        )
        status = "READY"
        message = "可结算（预览，不写数据库）"
        if apply:
            if self.repository.settle_pending(trade_type, updated):
                status = "UPDATED"
                message = "已写回 MySQL"
            else:
                status = "SKIPPED"
                message = "记录已被其他进程结算，未覆盖"
        return ReconciliationResult(
            **base,
            status=status,
            message=message,
            held_outcome=resolution.outcomes[held_index],
            winning_outcome=resolution.winning_outcome or "",
            pnl_usd=pnl_usd,
            payload=updated,
        )
