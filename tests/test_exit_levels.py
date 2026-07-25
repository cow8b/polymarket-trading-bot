"""SL/TP 按入场价插值（bot.models.interp_exit_fracs）的无网络单元测试。

对照 .env 注释中按带 [0.30, 0.65] 校准的矩阵：
  entry 0.30 → sl 40% tp 70%；entry 0.65 → sl 22% tp 50%；中间线性。
"""
from __future__ import annotations

import unittest

from decimal import Decimal

from bot.models import interp_exit_fracs, is_book_sane

ENDPOINTS = dict(sl_at_min=0.40, sl_at_max=0.22, tp_at_min=0.70, tp_at_max=0.50)


class InterpExitFracsTests(unittest.TestCase):
    def test_endpoints_match_documented_matrix(self) -> None:
        sl, tp = interp_exit_fracs(0.30, 0.30, 0.65, **ENDPOINTS)
        self.assertAlmostEqual(sl, 0.40)
        self.assertAlmostEqual(tp, 0.70)
        sl, tp = interp_exit_fracs(0.65, 0.30, 0.65, **ENDPOINTS)
        self.assertAlmostEqual(sl, 0.22)
        self.assertAlmostEqual(tp, 0.50)

    def test_midband_interpolation(self) -> None:
        # 矩阵中间行 entry 0.50 → sl≈30% tp≈60%（线性近似，允许 ±2pp）
        sl, tp = interp_exit_fracs(0.50, 0.30, 0.65, **ENDPOINTS)
        self.assertAlmostEqual(sl, 0.2971, places=3)
        self.assertAlmostEqual(tp, 0.5857, places=3)

    def test_out_of_band_clamped_to_endpoints(self) -> None:
        self.assertEqual(
            interp_exit_fracs(0.10, 0.30, 0.65, **ENDPOINTS),
            interp_exit_fracs(0.30, 0.30, 0.65, **ENDPOINTS),
        )
        self.assertEqual(
            interp_exit_fracs(0.95, 0.30, 0.65, **ENDPOINTS),
            interp_exit_fracs(0.65, 0.30, 0.65, **ENDPOINTS),
        )

    def test_degenerate_band_uses_min_endpoint(self) -> None:
        sl, tp = interp_exit_fracs(0.5, 0.5, 0.5, **ENDPOINTS)
        self.assertAlmostEqual(sl, 0.40)
        self.assertAlmostEqual(tp, 0.70)


class IsBookSaneTests(unittest.TestCase):
    """出场护栏：2026-07-25 模拟盘 6 笔全灭的根因是 NO 侧钓鱼单盘口。"""

    def test_normal_book_passes(self) -> None:
        self.assertTrue(is_book_sane(0.27, 0.28, 0.12))
        self.assertTrue(is_book_sane(Decimal("0.72"), Decimal("0.73"), 0.12))

    def test_junk_lowball_book_rejected(self) -> None:
        # 实测事故盘口：持仓 NO@0.73，NO 侧仅剩 $0.01 钓鱼 bid。
        self.assertFalse(is_book_sane(0.01, 0.73, 0.12))
        self.assertFalse(is_book_sane(0.10, 0.37, 0.12))

    def test_settlement_collapse_book_passes(self) -> None:
        # 临近结算输方 token 真实塌价：点差窄，属于合法止损场景。
        self.assertTrue(is_book_sane(0.02, 0.04, 0.12))
        # 赢方 token 挂满 1.00 的 ask 也不应被拒。
        self.assertTrue(is_book_sane(0.99, 1.00, 0.12))

    def test_degenerate_prices_rejected(self) -> None:
        self.assertFalse(is_book_sane(0.0, 0.05, 0.12))
        self.assertFalse(is_book_sane(1.0, 1.0, 0.12))
        self.assertFalse(is_book_sane(0.30, 0.20, 0.12))  # 倒挂
        self.assertFalse(is_book_sane(None, 0.5, 0.12))


if __name__ == "__main__":
    unittest.main()
