"""scripts/acceptance_report.py — 生成验收报告（策略统计分析图，自包含 HTML）。

从 MySQL `trade_history` 读取交易记录，产出一份可留档的验收报告，包含：
权益曲线、每笔盈亏、结果分布、入场价区间胜率、平仓原因分布与交易明细表。
报告是单文件 HTML（内联 SVG，无外部依赖），直接存入 `docs/acceptance/` 即可
作为验收产物提交，见 docs/TESTING_ACCEPTANCE.md。

用法：
    python scripts/acceptance_report.py --mode both --subject risk-balance --level L3
    python scripts/acceptance_report.py --mode paper --last 200 --out docs/acceptance

不依赖 matplotlib；图表配色经 dataviz 校验（蓝/红分歧对，明暗两套均通过
CVD 与对比度检查）。
"""
from __future__ import annotations

import argparse
import html
import json
import math
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

SETTLED = ("WIN", "LOSS", "BREAKEVEN")


# ── 统计计算 ──────────────────────────────────────────────────────────────────

def _f(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def _sort_key(trade: Dict[str, Any]) -> str:
    return str(trade.get("closed_at") or trade.get("timestamp") or "")


def compute_stats(trades: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """从交易 payload 列表计算验收所需的全部统计量。"""
    ordered = sorted(trades, key=_sort_key)
    settled = [t for t in ordered if str(t.get("outcome")) in SETTLED]

    wins = [t for t in settled if t.get("outcome") == "WIN"]
    losses = [t for t in settled if t.get("outcome") == "LOSS"]
    pnls = [_f(t.get("pnl_usd")) for t in settled]
    total_pnl = sum(pnls)
    gross_win = sum(p for p in pnls if p > 0)
    gross_loss = -sum(p for p in pnls if p < 0)

    # 权益曲线（相对起点的累计盈亏）与最大回撤
    equity: List[float] = []
    running = 0.0
    for p in pnls:
        running += p
        equity.append(running)
    peak = 0.0
    max_dd = 0.0
    for value in equity:
        peak = max(peak, value)
        max_dd = max(max_dd, peak - value)

    # 结果分布（含未结算/降级状态，验收要求它们无堆积）
    outcome_counts: Dict[str, int] = {}
    for t in ordered:
        key = str(t.get("outcome") or "UNKNOWN")
        outcome_counts[key] = outcome_counts.get(key, 0) + 1

    # 入场价 0.1 分桶胜率（仅官方结算的交易）
    bands: Dict[str, Dict[str, int]] = {}
    for t in settled:
        entry = _f(t.get("entry_price"))
        if not 0 < entry <= 1:
            continue
        low = min(int(entry * 10) / 10, 0.9)
        label = f"{low:.1f}–{low + 0.1:.1f}"
        bucket = bands.setdefault(label, {"total": 0, "wins": 0})
        bucket["total"] += 1
        bucket["wins"] += 1 if t.get("outcome") == "WIN" else 0

    reason_counts: Dict[str, int] = {}
    for t in settled:
        key = str(t.get("close_reason") or "UNKNOWN")
        reason_counts[key] = reason_counts.get(key, 0) + 1

    return {
        "trades": ordered,
        "settled": settled,
        "total": len(ordered),
        "settled_count": len(settled),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": (len(wins) / len(settled) * 100) if settled else 0.0,
        "total_pnl": total_pnl,
        "avg_pnl": (total_pnl / len(settled)) if settled else 0.0,
        "profit_factor": (gross_win / gross_loss) if gross_loss > 0 else float("inf") if gross_win > 0 else 0.0,
        "max_drawdown": max_dd,
        "equity": equity,
        "pnls": pnls,
        "outcome_counts": outcome_counts,
        "entry_bands": dict(sorted(bands.items())),
        "reason_counts": dict(sorted(reason_counts.items(), key=lambda kv: -kv[1])),
    }


# ── SVG 绘图助手（遵循 dataviz 标记规范）──────────────────────────────────────

def _esc(text: Any) -> str:
    return html.escape(str(text), quote=True)


def _nice_ticks(low: float, high: float, count: int = 4) -> List[float]:
    if high <= low:
        high = low + 1
    span = high - low
    step_raw = span / count
    magnitude = 10 ** math.floor(math.log10(step_raw))
    for mult in (1, 2, 2.5, 5, 10):
        step = mult * magnitude
        if step >= step_raw:
            break
    start = math.floor(low / step) * step
    ticks = []
    value = start
    while value <= high + step / 2:
        ticks.append(round(value, 10))
        value += step
    return ticks


def _bar_path(x: float, y: float, w: float, h: float, up: bool) -> str:
    """4px 圆角在数据端、基线端为直角的柱形路径。"""
    r = min(4.0, w / 2, abs(h) / 2)
    if h <= 0:
        return ""
    if up:  # 数据端在上
        return (
            f"M{x:.2f},{y + h:.2f} v{-(h - r):.2f} q0,{-r:.2f} {r:.2f},{-r:.2f} "
            f"h{w - 2 * r:.2f} q{r:.2f},0 {r:.2f},{r:.2f} v{h - r:.2f} z"
        )
    return (  # 数据端在下（负值柱）
        f"M{x:.2f},{y:.2f} v{h - r:.2f} q0,{r:.2f} {r:.2f},{r:.2f} "
        f"h{w - 2 * r:.2f} q{r:.2f},0 {r:.2f},{-r:.2f} v{-(h - r):.2f} z"
    )


def _svg_frame(width: int, height: int, body: str) -> str:
    return (
        f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" '
        f'role="img" xmlns="http://www.w3.org/2000/svg">{body}</svg>'
    )


def equity_curve_svg(equity: List[float]) -> str:
    """权益曲线：单序列折线 + 10% 面积淡washes + 端点直标。"""
    W, H, ML, MR, MT, MB = 760, 220, 56, 70, 14, 26
    if len(equity) < 2:
        return '<p class="empty">交易样本不足，无法绘制权益曲线。</p>'
    plot_w, plot_h = W - ML - MR, H - MT - MB
    lo, hi = min(min(equity), 0.0), max(max(equity), 0.0)
    ticks = _nice_ticks(lo, hi)
    lo, hi = min(lo, ticks[0]), max(hi, ticks[-1])

    def sx(i: int) -> float:
        return ML + plot_w * i / (len(equity) - 1)

    def sy(v: float) -> float:
        return MT + plot_h * (1 - (v - lo) / (hi - lo))

    grid, labels = [], []
    for t in ticks:
        y = sy(t)
        grid.append(f'<line x1="{ML}" y1="{y:.1f}" x2="{ML + plot_w}" y2="{y:.1f}" class="grid"/>')
        labels.append(f'<text x="{ML - 8}" y="{y + 4:.1f}" class="tick" text-anchor="end">{t:+.2f}</text>')

    pts = " ".join(f"{sx(i):.1f},{sy(v):.1f}" for i, v in enumerate(equity))
    area = f"{ML},{sy(0):.1f} {pts} {ML + plot_w},{sy(0):.1f}"
    hover = "".join(
        f'<circle cx="{sx(i):.1f}" cy="{sy(v):.1f}" r="9" class="hit" '
        f'data-tt="第 {i + 1} 笔结算后：{v:+.2f} USD"/>'
        for i, v in enumerate(equity)
    )
    end_x, end_y = sx(len(equity) - 1), sy(equity[-1])
    body = (
        "".join(grid)
        + f'<line x1="{ML}" y1="{sy(0):.1f}" x2="{ML + plot_w}" y2="{sy(0):.1f}" class="axis"/>'
        + f'<polygon points="{area}" class="area"/>'
        + f'<polyline points="{pts}" class="line"/>'
        + f'<circle cx="{end_x:.1f}" cy="{end_y:.1f}" r="4.5" class="dot"/>'
        + f'<text x="{end_x + 10:.1f}" y="{end_y + 4:.1f}" class="endlab">{equity[-1]:+.2f}</text>'
        + "".join(labels)
        + hover
    )
    return _svg_frame(W, H, body)


def pnl_bars_svg(pnls: List[float], trades: Sequence[Dict[str, Any]]) -> str:
    """每笔盈亏：正负分歧配色（蓝正红负），2px 间隔，基线直角。"""
    W, H, ML, MR, MT, MB = 760, 220, 56, 16, 14, 26
    if not pnls:
        return '<p class="empty">暂无已结算交易。</p>'
    plot_w, plot_h = W - ML - MR, H - MT - MB
    lo, hi = min(min(pnls), 0.0), max(max(pnls), 0.0)
    ticks = _nice_ticks(lo, hi)
    lo, hi = min(lo, ticks[0]), max(hi, ticks[-1])

    def sy(v: float) -> float:
        return MT + plot_h * (1 - (v - lo) / (hi - lo))

    n = len(pnls)
    slot = plot_w / n
    bw = max(2.0, min(24.0, slot - 2.0))  # 柱宽 ≤24px，留 2px 表面间隔
    grid, labels = [], []
    for t in ticks:
        y = sy(t)
        grid.append(f'<line x1="{ML}" y1="{y:.1f}" x2="{ML + plot_w}" y2="{y:.1f}" class="grid"/>')
        labels.append(f'<text x="{ML - 8}" y="{y + 4:.1f}" class="tick" text-anchor="end">{t:+.2f}</text>')
    y0 = sy(0)
    bars = []
    for i, p in enumerate(pnls):
        x = ML + slot * i + (slot - bw) / 2
        h = abs(sy(p) - y0)
        if h < 0.5:
            h = 0.5
        cls = "pos" if p >= 0 else "neg"
        y = sy(p) if p >= 0 else y0
        tid = str(trades[i].get("trade_id", "")) if i < len(trades) else ""
        path = _bar_path(x, y, bw, h, up=(p >= 0))
        bars.append(f'<path d="{path}" class="{cls}" data-tt="{_esc(tid)}：{p:+.2f} USD"/>')
    body = (
        "".join(grid)
        + "".join(bars)
        + f'<line x1="{ML}" y1="{y0:.1f}" x2="{ML + plot_w}" y2="{y0:.1f}" class="axis"/>'
        + "".join(labels)
    )
    legend = (
        '<div class="legend"><span><i class="sw pos"></i>盈利</span>'
        '<span><i class="sw neg"></i>亏损</span></div>'
    )
    return legend + _svg_frame(W, H, body)


def hbar_svg(counts: Dict[str, Any], value_fn=None, suffix: str = "") -> str:
    """横向单色柱：结果分布 / 平仓原因 / 入场价区间胜率共用。"""
    if not counts:
        return '<p class="empty">暂无数据。</p>'
    rows = list(counts.items())
    W, ML, MR, RH = 760, 150, 90, 26
    H = len(rows) * RH + 12
    plot_w = W - ML - MR
    values = [(value_fn(v) if value_fn else float(v)) for _, v in rows]
    vmax = max(max(values), 1e-9)
    parts = []
    for i, ((label, raw), value) in enumerate(zip(rows, values)):
        y = 6 + i * RH
        bw = max(1.0, plot_w * value / vmax)
        r = min(4.0, bw / 2, 10.0)
        path = (
            f"M{ML},{y:.1f} h{bw - r:.2f} q{r:.2f},0 {r:.2f},{r:.2f} v{20 - 2 * r:.2f} "
            f"q0,{r:.2f} {-r:.2f},{r:.2f} h{-(bw - r):.2f} z"
        )
        shown = f"{value:.1f}{suffix}" if suffix else f"{int(value)}"
        parts.append(
            f'<text x="{ML - 8}" y="{y + 14:.1f}" class="tick" text-anchor="end">{_esc(label)}</text>'
            f'<path d="{path}" class="pos" data-tt="{_esc(f"{label}：{shown}")}"/>'
            f'<text x="{ML + bw + 8:.1f}" y="{y + 14:.1f}" class="endlab">{shown}</text>'
        )
    parts.append(f'<line x1="{ML}" y1="0" x2="{ML}" y2="{H}" class="axis"/>')
    return _svg_frame(W, H, "".join(parts))


# ── HTML 组装 ─────────────────────────────────────────────────────────────────

_CSS = """
:root { color-scheme: light;
  --surface:#fcfcfb; --page:#f9f9f7; --ink:#0b0b0b; --ink2:#52514e; --muted:#898781;
  --grid:#e1e0d9; --axis:#c3c2b7; --series:#2a78d6; --neg:#e34948;
  --good:#006300; --bad:#d03b3b; --ring:rgba(11,11,11,0.10); }
@media (prefers-color-scheme: dark) { :root { color-scheme: dark;
  --surface:#1a1a19; --page:#0d0d0d; --ink:#ffffff; --ink2:#c3c2b7; --muted:#898781;
  --grid:#2c2c2a; --axis:#383835; --series:#3987e5; --neg:#e66767;
  --good:#0ca30c; --bad:#d03b3b; --ring:rgba(255,255,255,0.10); } }
* { box-sizing:border-box; }
body { margin:0; padding:24px; background:var(--page); color:var(--ink);
  font:14px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif; }
h1 { font-size:20px; margin:0 0 4px; } h2 { font-size:15px; margin:28px 0 8px; }
.meta { color:var(--ink2); font-size:13px; margin-bottom:16px; }
.meta code { background:var(--surface); border:1px solid var(--ring); border-radius:4px; padding:0 4px; }
.card { background:var(--surface); border:1px solid var(--ring); border-radius:10px; padding:16px; margin-bottom:8px; }
.tiles { display:grid; grid-template-columns:repeat(auto-fit,minmax(130px,1fr)); gap:8px; }
.tile { background:var(--surface); border:1px solid var(--ring); border-radius:10px; padding:12px 14px; }
.tile .k { color:var(--ink2); font-size:12px; } .tile .v { font-size:24px; font-weight:600; margin-top:2px; }
.tile .v.up { color:var(--good); } .tile .v.down { color:var(--bad); }
svg { display:block; } .grid { stroke:var(--grid); stroke-width:1; }
.axis { stroke:var(--axis); stroke-width:1; }
.tick { fill:var(--muted); font-size:11px; font-variant-numeric:tabular-nums; }
.endlab { fill:var(--ink2); font-size:11px; font-variant-numeric:tabular-nums; }
.line { fill:none; stroke:var(--series); stroke-width:2; stroke-linejoin:round; stroke-linecap:round; }
.area { fill:var(--series); opacity:.1; }
.dot { fill:var(--series); stroke:var(--surface); stroke-width:2; }
.pos { fill:var(--series); } .neg { fill:var(--neg); }
.hit { fill:transparent; }
.legend { display:flex; gap:16px; color:var(--ink2); font-size:12px; margin-bottom:6px; }
.sw { display:inline-block; width:10px; height:10px; border-radius:3px; margin-right:5px; }
.sw.pos { background:var(--series); } .sw.neg { background:var(--neg); }
table { width:100%; border-collapse:collapse; font-size:12px; font-variant-numeric:tabular-nums; }
th,td { text-align:left; padding:6px 8px; border-bottom:1px solid var(--grid); }
th { color:var(--ink2); font-weight:600; } td.num,th.num { text-align:right; }
td .up { color:var(--good); } td .down { color:var(--bad); }
.empty { color:var(--muted); }
#tt { position:fixed; display:none; pointer-events:none; background:var(--ink); color:var(--page);
  padding:4px 8px; border-radius:6px; font-size:12px; z-index:9; max-width:320px; }
"""

_JS = """
const tt = document.getElementById('tt');
document.querySelectorAll('[data-tt]').forEach(el => {
  el.addEventListener('mousemove', e => {
    tt.textContent = el.dataset.tt; tt.style.display = 'block';
    tt.style.left = Math.min(e.clientX + 12, innerWidth - tt.offsetWidth - 8) + 'px';
    tt.style.top = (e.clientY + 14) + 'px';
  });
  el.addEventListener('mouseleave', () => tt.style.display = 'none');
});
"""


def _tiles(stats: Dict[str, Any]) -> str:
    pnl = stats["total_pnl"]
    pf = stats["profit_factor"]
    pf_txt = "∞" if pf == float("inf") else f"{pf:.2f}"
    pnl_cls = "up" if pnl > 0 else "down" if pnl < 0 else ""
    return (
        '<div class="tiles">'
        f'<div class="tile"><div class="k">交易笔数（已结算/总）</div><div class="v">{stats["settled_count"]}/{stats["total"]}</div></div>'
        f'<div class="tile"><div class="k">胜率</div><div class="v">{stats["win_rate"]:.1f}%</div></div>'
        f'<div class="tile"><div class="k">累计盈亏</div><div class="v {pnl_cls}">{pnl:+.2f}</div></div>'
        f'<div class="tile"><div class="k">单笔均值</div><div class="v">{stats["avg_pnl"]:+.2f}</div></div>'
        f'<div class="tile"><div class="k">盈利因子</div><div class="v">{pf_txt}</div></div>'
        f'<div class="tile"><div class="k">最大回撤</div><div class="v down">-{stats["max_drawdown"]:.2f}</div></div>'
        "</div>"
    )


def _trades_table(trades: Sequence[Dict[str, Any]], limit: int = 50) -> str:
    rows = []
    for t in list(trades)[-limit:]:
        pnl = _f(t.get("pnl_usd"))
        cls = "up" if pnl > 0 else "down" if pnl < 0 else ""
        rows.append(
            "<tr>"
            f"<td>{_esc(t.get('trade_id', ''))}</td>"
            f"<td>{_esc(str(t.get('closed_at') or t.get('timestamp') or '')[:19])}</td>"
            f"<td>{_esc(t.get('direction', ''))}</td>"
            f"<td class='num'>{_f(t.get('entry_price')):.3f}</td>"
            f"<td class='num'>{_f(t.get('exit_price')):.3f}</td>"
            f"<td class='num'>{_f(t.get('size_usd')):.2f}</td>"
            f"<td class='num'><span class='{cls}'>{pnl:+.2f}</span></td>"
            f"<td>{_esc(t.get('outcome', ''))}</td>"
            f"<td>{_esc(t.get('close_reason', ''))}</td>"
            "</tr>"
        )
    if not rows:
        return '<p class="empty">暂无交易。</p>'
    return (
        "<table><thead><tr><th>trade_id</th><th>时间</th><th>方向</th>"
        "<th class='num'>入场价</th><th class='num'>出场价</th><th class='num'>金额</th>"
        "<th class='num'>盈亏</th><th>结果</th><th>平仓原因</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def _mode_section(mode: str, stats: Dict[str, Any]) -> str:
    band_rates = {
        label: bucket for label, bucket in stats["entry_bands"].items()
    }
    return (
        f"<h2>{'纸面交易' if mode == 'paper' else '实盘交易'}（{mode}）</h2>"
        + _tiles(stats)
        + '<h2>权益曲线（累计盈亏，USD）</h2><div class="card">'
        + equity_curve_svg(stats["equity"]) + "</div>"
        + '<h2>每笔盈亏（按结算顺序）</h2><div class="card">'
        + pnl_bars_svg(stats["pnls"], stats["settled"]) + "</div>"
        + '<h2>结果分布</h2><div class="card">'
        + hbar_svg(stats["outcome_counts"]) + "</div>"
        + '<h2>入场价区间胜率（%）</h2><div class="card">'
        + hbar_svg(
            band_rates,
            value_fn=lambda b: (b["wins"] / b["total"] * 100) if b["total"] else 0.0,
            suffix="%",
        )
        + "</div>"
        + '<h2>平仓原因分布</h2><div class="card">'
        + hbar_svg(stats["reason_counts"]) + "</div>"
        + '<h2>交易明细（最近 50 笔）</h2><div class="card">'
        + _trades_table(stats["trades"]) + "</div>"
    )


def render_report(
    stats_by_mode: Dict[str, Dict[str, Any]],
    *,
    subject: str = "",
    level: str = "",
    commit: str = "",
    generated_at: Optional[str] = None,
) -> str:
    generated_at = generated_at or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    meta_bits = [f"生成时间 {generated_at}"]
    if subject:
        meta_bits.append(f"验收主题 <code>{_esc(subject)}</code>")
    if level:
        meta_bits.append(f"变更级别 <code>{_esc(level)}</code>")
    if commit:
        meta_bits.append(f"提交 <code>{_esc(commit)}</code>")
    sections = "".join(_mode_section(mode, stats) for mode, stats in stats_by_mode.items())
    return (
        "<!doctype html><html lang='zh'><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        f"<title>验收报告 {_esc(subject)}</title><style>{_CSS}</style></head><body>"
        "<h1>策略验收报告</h1>"
        f"<div class='meta'>{' · '.join(meta_bits)}</div>"
        + sections
        + "<div id='tt'></div>"
        + f"<script>{_JS}</script></body></html>"
    )


def generate_report(
    trades_by_mode: Dict[str, Sequence[Dict[str, Any]]],
    out_dir: Path,
    *,
    subject: str = "",
    level: str = "",
    commit: str = "",
) -> Path:
    """计算统计并将 report.html 与 stats.json 写入 out_dir。"""
    stats_by_mode = {mode: compute_stats(trades) for mode, trades in trades_by_mode.items()}
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "report.html"
    report_path.write_text(
        render_report(stats_by_mode, subject=subject, level=level, commit=commit),
        encoding="utf-8",
    )
    # 机器可读的统计摘要，便于对比两次验收
    summary = {
        mode: {
            k: v
            for k, v in stats.items()
            if k not in ("trades", "settled", "equity", "pnls")
        }
        | {"profit_factor": (None if stats["profit_factor"] == float("inf") else stats["profit_factor"])}
        for mode, stats in stats_by_mode.items()
    }
    (out_dir / "stats.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return report_path


# ── CLI ───────────────────────────────────────────────────────────────────────

def _git_commit() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, cwd=ROOT, timeout=5,
        ).stdout.strip()
    except Exception:
        return ""


def main() -> None:
    parser = argparse.ArgumentParser(description="生成策略验收报告（图表 + 统计）")
    parser.add_argument("--mode", choices=["paper", "live", "both"], default="both")
    parser.add_argument("--subject", default="", help="验收主题（用于目录名与报告标题）")
    parser.add_argument("--level", default="", help="变更级别 L1/L2/L3")
    parser.add_argument("--last", type=int, default=0, help="只取最近 N 笔（0=全部）")
    parser.add_argument(
        "--out", default=str(ROOT / "docs" / "acceptance"),
        help="输出根目录（默认 docs/acceptance/）",
    )
    args = parser.parse_args()

    from core.database import TradeHistoryRepository  # 延迟导入，避免无 DB 环境下模块不可用

    repository = TradeHistoryRepository()
    modes = ["paper", "live"] if args.mode == "both" else [args.mode]
    trades_by_mode: Dict[str, List[Dict[str, Any]]] = {}
    for mode in modes:
        trades = repository.load(mode)
        if args.last > 0:
            trades = sorted(trades, key=_sort_key)[-args.last:]
        trades_by_mode[mode] = trades

    slug = re.sub(r"[^a-zA-Z0-9_-]+", "-", args.subject).strip("-") or "acceptance"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")
    out_dir = Path(args.out) / f"{stamp}-{slug}"
    report = generate_report(
        trades_by_mode, out_dir,
        subject=args.subject, level=args.level, commit=_git_commit(),
    )
    total = sum(len(v) for v in trades_by_mode.values())
    print(f"验收报告已生成：{report}（{total} 笔交易）")
    print("请将该目录随验收 PR 一并提交，见 docs/TESTING_ACCEPTANCE.md 第五节。")


if __name__ == "__main__":
    main()
