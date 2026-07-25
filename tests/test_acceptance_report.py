"""验收报告生成器（scripts/acceptance_report.py）的无网络单元测试。"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.acceptance_report import compute_stats, generate_report


def _trade(i: int, pnl: float, outcome: str, entry: float = 0.5, reason: str = "TAKE_PROFIT"):
    return {
        "trade_id": f"t{i}",
        "timestamp": f"2026-07-25T10:{i:02d}:00+00:00",
        "closed_at": f"2026-07-25T10:{i:02d}:30+00:00",
        "direction": "LONG",
        "entry_price": entry,
        "exit_price": entry + pnl / 10,
        "size_usd": 10.0,
        "filled_qty": 10.0 / entry,
        "pnl_usd": pnl,
        "outcome": outcome,
        "close_reason": reason,
        "market_slug": f"btc-updown-15m-{i}",
    }


SAMPLE = [
    _trade(1, 4.0, "WIN", entry=0.35),
    _trade(2, -3.0, "LOSS", entry=0.55, reason="STOP_LOSS"),
    _trade(3, 6.0, "WIN", entry=0.42),
    _trade(4, -2.0, "LOSS", entry=0.61, reason="SETTLEMENT"),
    _trade(5, 0.0, "BREAKEVEN", entry=0.50),
    _trade(6, 0.0, "PENDING", entry=0.48),
]


class ComputeStatsTests(unittest.TestCase):
    def test_core_metrics(self) -> None:
        stats = compute_stats(SAMPLE)
        self.assertEqual(stats["total"], 6)
        self.assertEqual(stats["settled_count"], 5)
        self.assertEqual(stats["wins"], 2)
        self.assertEqual(stats["losses"], 2)
        self.assertAlmostEqual(stats["win_rate"], 40.0)
        self.assertAlmostEqual(stats["total_pnl"], 5.0)
        self.assertAlmostEqual(stats["profit_factor"], 2.0)
        # 权益曲线 4 → 1 → 7 → 5 → 5：峰值 7，谷 5，最大回撤 3（4→1）
        self.assertEqual(stats["equity"], [4.0, 1.0, 7.0, 5.0, 5.0])
        self.assertAlmostEqual(stats["max_drawdown"], 3.0)
        self.assertEqual(stats["outcome_counts"]["PENDING"], 1)

    def test_empty_input_does_not_crash(self) -> None:
        stats = compute_stats([])
        self.assertEqual(stats["total"], 0)
        self.assertEqual(stats["win_rate"], 0.0)
        self.assertEqual(stats["max_drawdown"], 0.0)


class GenerateReportTests(unittest.TestCase):
    def test_report_and_stats_files_written(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "20260725-test"
            report = generate_report(
                {"paper": SAMPLE}, out, subject="risk-balance", level="L3", commit="abc1234"
            )
            html = report.read_text(encoding="utf-8")
            # 关键区块齐全：标题、统计卡、四张图、明细表、tooltip 层
            for marker in (
                "策略验收报告", "risk-balance", "权益曲线", "每笔盈亏",
                "结果分布", "入场价区间胜率", "平仓原因分布", "交易明细",
                "<svg", "data-tt", "id='tt'",
            ):
                self.assertIn(marker, html)
            # 单序列图无图例；盈亏图有盈利/亏损图例
            self.assertIn("盈利", html)
            summary = json.loads((out / "stats.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["paper"]["total"], 6)
            self.assertAlmostEqual(summary["paper"]["total_pnl"], 5.0)

    def test_empty_modes_render_placeholders(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = generate_report({"live": []}, Path(tmp) / "empty")
            html = report.read_text(encoding="utf-8")
            self.assertIn("暂无交易", html)


if __name__ == "__main__":
    unittest.main()
