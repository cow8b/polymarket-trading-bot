"""驾驶舱交易指标的纯计算函数。"""
from __future__ import annotations

from typing import Any, Dict, Iterable


def _field(item: Any, name: str, default: Any = None) -> Any:
    if isinstance(item, dict):
        return item.get(name, default)
    return getattr(item, name, default)


def calculate_prediction_edge(
    *,
    direction: str,
    p_up: float | None,
    executable_entry_price: float,
) -> Dict[str, Any]:
    """计算持有结果股的成交前毛优势，不假设手续费或滑点。"""
    normalized = direction.strip().lower()
    try:
        probability_up = float(p_up) if p_up is not None else None
        entry = float(executable_entry_price)
    except (TypeError, ValueError):
        probability_up = None
        entry = 0.0
    if normalized not in {"long", "short", "up", "down"}:
        return {"available": False, "reason": "方向无效"}
    if probability_up is None or not 0.0 <= probability_up <= 1.0:
        return {"available": False, "reason": "模型概率不可用"}
    if not 0.0 < entry < 1.0:
        return {"available": False, "reason": "可成交价格不可用"}

    is_up = normalized in {"long", "up"}
    held_probability = probability_up if is_up else 1.0 - probability_up
    edge_per_share = held_probability - entry
    return {
        "available": True,
        "direction": "UP" if is_up else "DOWN",
        "held_probability": held_probability,
        "executable_entry_price": entry,
        "edge_points_pct": edge_per_share * 100.0,
        "edge_pct": edge_per_share / entry * 100.0,
        "costs_included": False,
    }


def calculate_strategy_edge(
    trades: Iterable[Any],
    *,
    window: int = 50,
) -> Dict[str, Any]:
    """按已结算交易计算滚动已实现收益率和单笔期望。"""
    eligible = [
        item
        for item in trades
        if str(_field(item, "outcome", "")).upper() in {"WIN", "LOSS", "BREAKEVEN"}
    ]
    selected = eligible[-max(1, int(window)):]
    pnl = sum(float(_field(item, "pnl_usd", 0.0) or 0.0) for item in selected)
    notional = sum(float(_field(item, "size_usd", 0.0) or 0.0) for item in selected)
    sample_size = len(selected)
    return {
        "available": sample_size > 0 and notional > 0,
        "window": max(1, int(window)),
        "sample_size": sample_size,
        "settled_total": len(eligible),
        "realized_pnl_usd": pnl,
        "entry_notional_usd": notional,
        "edge_pct": (pnl / notional * 100.0) if notional > 0 else None,
        "expectancy_usd": (pnl / sample_size) if sample_size else None,
        "costs_included": False,
        "sample_sufficient": sample_size >= 30,
    }
