"""SL/TP 按入场价插值（bot.models.interp_exit_fracs）的无网络单元测试。

对照 .env 注释中按带 [0.30, 0.65] 校准的矩阵：
  entry 0.30 → sl 40% tp 70%；entry 0.65 → sl 22% tp 50%；中间线性。
"""
from __future__ import annotations

import unittest

from bot.models import interp_exit_fracs

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


if __name__ == "__main__":
    unittest.main()
