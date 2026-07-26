#!/usr/bin/env python3
"""check_wallet.py — Polymarket 钱包连通性自检（只读，不下单）。

逐项验证实盘交易的前置条件：
  1. .env 配置齐全（PK / FUNDER / SIG_TYPE）
  2. 私钥可派生签名地址（不打印私钥本身）
  3. CLOB API 认证可用（创建/派生 L2 凭据）
  4. CLOB 视角下 funder 名下的 USDC 可用余额 > 0
  5. 交易合约的 USDC 授权额度 > 0

用法：python scripts/check_wallet.py
退出码：0 = 全部通过，可以实盘；1 = 存在阻断项。
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

OK, BAD, WARN = "✔", "✘", "⚠"
failures: list[str] = []


def report(ok: bool, label: str, detail: str = "", *, warn_only: bool = False) -> None:
    mark = OK if ok else (WARN if warn_only else BAD)
    print(f"  {mark} {label}" + (f"  —  {detail}" if detail else ""))
    if not ok and not warn_only:
        failures.append(label)


def main() -> int:
    print("── Polymarket 钱包连通性自检 ──────────────────────────")

    # 1. 配置齐全性
    pk = (os.getenv("POLYMARKET_PK") or "").strip()
    funder = (os.getenv("POLYMARKET_FUNDER") or "").strip()
    try:
        sig_type = int(os.getenv("POLYMARKET_SIG_TYPE", "0") or 0)
    except ValueError:
        sig_type = -1
    report(bool(pk), "POLYMARKET_PK 已配置")
    report(bool(funder), "POLYMARKET_FUNDER 已配置", funder or "缺失")
    sig_label = {0: "EOA(0)", 1: "POLY_PROXY/Magic(1)", 2: "POLY_GNOSIS_SAFE(2)"}.get(
        sig_type, f"非法值 {sig_type}"
    )
    report(sig_type in (0, 1, 2), "POLYMARKET_SIG_TYPE", sig_label)
    if sig_type in (1, 2) and not funder:
        report(False, "sig_type=1/2 需要 FUNDER（代理钱包地址）")
    if failures:
        _summary()
        return 1

    # 2. 私钥 → 签名地址（本地运算，不联网）
    try:
        from eth_account import Account

        signer = Account.from_key(pk).address
        report(True, "私钥可派生签名地址", signer)
    except Exception as e:
        report(False, "私钥无效", repr(e))
        _summary()
        return 1

    # 3. CLOB API 认证
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
    except ImportError as e:
        report(False, "py_clob_client 未安装", repr(e))
        _summary()
        return 1

    try:
        client = ClobClient(
            "https://clob.polymarket.com",
            key=pk,
            chain_id=137,
            signature_type=sig_type,
            funder=funder or None,
        )
        client.set_api_creds(client.create_or_derive_api_creds())
        report(True, "CLOB API 认证（L2 凭据派生）")
    except Exception as e:
        report(False, "CLOB API 认证失败", repr(e))
        _summary()
        return 1

    # 4/5. 余额与授权
    try:
        res = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
    except Exception as e:
        report(False, "余额查询失败", repr(e))
        _summary()
        return 1

    try:
        balance_usdc = float(res.get("balance", "0")) / 1e6
    except (TypeError, ValueError):
        balance_usdc = 0.0
    report(
        balance_usdc > 0,
        "funder 名下 USDC 可用余额",
        f"${balance_usdc:.2f}"
        + ("" if balance_usdc > 0 else "  → FUNDER 地址不对，或资金在别的代理钱包"),
    )

    allowances = res.get("allowances") or {}
    n_approved = sum(1 for v in allowances.values() if float(v or 0) > 0)
    report(
        n_approved > 0,
        f"交易合约 USDC 授权（{n_approved}/{len(allowances)} 个合约已授权）",
        "" if n_approved else "→ 该代理从未在链上授权过交易合约（UI 交易过的代理通常已有授权）",
    )

    # 附注：市场买单金额与余额的关系
    try:
        buy_usd = float(os.getenv("MARKET_BUY_USD", "0") or 0)
        if buy_usd > 0:
            report(
                balance_usdc >= buy_usd,
                f"余额覆盖单笔下单额 MARKET_BUY_USD=${buy_usd:.2f}",
                warn_only=True,
            )
    except ValueError:
        pass

    _summary()
    return 1 if failures else 0


def _summary() -> None:
    print("──────────────────────────────────────────────────────")
    if failures:
        print(f"结论：{BAD} 存在 {len(failures)} 个阻断项，暂不可实盘：{'；'.join(failures)}")
    else:
        print(f"结论：{OK} 全部通过，钱包链路可用。")


if __name__ == "__main__":
    sys.exit(main())
