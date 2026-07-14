#!/usr/bin/env python3
"""独立补结算命令：处理机器人停机后遗留的 PENDING 交易。"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Iterable, List

from rich.console import Console
from rich.table import Table

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.database import TradeHistoryRepository
from core.reconciliation import (
    GammaMarketClient,
    OfflineTradeReconciler,
    ReconciliationResult,
)

console = Console()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="按 Polymarket 官方市场结果补结算 MySQL 中的待处理交易；不会下单或赎回。"
    )
    write_mode = parser.add_mutually_exclusive_group()
    write_mode.add_argument(
        "--dry-run",
        action="store_true",
        help="只预览（默认），不修改数据库",
    )
    write_mode.add_argument(
        "--apply",
        action="store_true",
        help="将唯一明确的结算结果写回 MySQL",
    )
    parser.add_argument(
        "--mode",
        choices=("paper", "live", "both"),
        default="paper",
        help="处理模拟、实盘或两类记录（默认：paper）",
    )
    parser.add_argument("--trade-id", help="仅处理指定交易 ID")
    parser.add_argument(
        "--older-than",
        type=float,
        default=0.0,
        metavar="MINUTES",
        help="仅处理市场结束至少指定分钟后的记录（默认：0）",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="持续轮询；Ctrl+C 可干净退出",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=30.0,
        metavar="SECONDS",
        help="持续轮询间隔（默认：30）",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        metavar="SECONDS",
        help="Gamma 单次请求超时（默认：15）",
    )
    args = parser.parse_args()
    if args.older_than < 0:
        parser.error("--older-than 不能小于 0")
    if args.interval <= 0:
        parser.error("--interval 必须大于 0")
    if args.timeout <= 0:
        parser.error("--timeout 必须大于 0")
    return args


def trade_types(mode: str) -> Iterable[str]:
    return ("paper", "live") if mode == "both" else (mode,)


def print_results(results: List[ReconciliationResult], apply: bool) -> None:
    table = Table(
        title="停机交易补结算" + ("（写入）" if apply else "（只读预览）"),
        show_lines=False,
    )
    table.add_column("类型", style="cyan", no_wrap=True)
    table.add_column("交易 ID", no_wrap=True)
    table.add_column("市场")
    table.add_column("持仓/结果", no_wrap=True)
    table.add_column("盈亏", justify="right", no_wrap=True)
    table.add_column("状态", no_wrap=True)
    table.add_column("说明")
    styles = {
        "READY": "yellow",
        "UPDATED": "green",
        "WAITING": "dim",
        "SKIPPED": "yellow",
        "INVALID": "red",
        "ERROR": "red",
    }
    for item in results:
        pnl = "-" if item.pnl_usd is None else f"${item.pnl_usd:+.4f}"
        table.add_row(
            item.trade_type,
            item.trade_id,
            item.market_slug,
            f"{item.held_outcome or '-'} / {item.winning_outcome or '-'}",
            pnl,
            f"[{styles.get(item.status, 'white')}]{item.status}[/]",
            item.message,
        )
    if results:
        console.print(table)
    else:
        console.print("[green]没有符合条件的 PENDING 交易。[/green]")

    counts = {status: sum(item.status == status for item in results) for status in {
        item.status for item in results
    }}
    if counts:
        summary = "，".join(f"{key}={value}" for key, value in sorted(counts.items()))
        console.print(f"汇总：{summary}")


def main() -> int:
    args = parse_args()
    apply = bool(args.apply)
    try:
        repository = TradeHistoryRepository()
        with GammaMarketClient(timeout=args.timeout) as market_client:
            reconciler = OfflineTradeReconciler(repository, market_client)
            while True:
                results = reconciler.reconcile_once(
                    trade_types(args.mode),
                    trade_id=args.trade_id,
                    older_than_minutes=args.older_than,
                    apply=apply,
                )
                print_results(results, apply)
                if not args.watch:
                    return 1 if any(item.status == "ERROR" for item in results) else 0
                time.sleep(args.interval)
    except KeyboardInterrupt:
        console.print("\n[yellow]已停止补结算轮询。[/yellow]")
        return 130
    except Exception as exc:
        console.print(f"[red]补结算命令启动失败：[/red]{exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
