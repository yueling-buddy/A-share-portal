#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A股盘中选股日报生成器。

口径（用户 2026-09-04 确认）：
  底池 = 现有 RPS/FIP 体系「严格强势平滑」（综合RPS≥90 + FIP50/120/250 全<0 + 总市值≥50亿）
       + 所属板块 6 月强度 ≥ 阈值（默认 80，即全市场前 20%）
  排序 = 盘中打分：当日涨幅 35% + 量比 25% + 距250日高点 20% + 板块6月强度 20%
        （各因子在候选池内取横截面百分位后加权）
  输出 = data/pick_latest.json + 根目录 pick.html（Top5 配基本面卡片）

用法：
  python scripts/pick_daily.py                # 刷新数据 + 筛选 + 渲染
  python scripts/pick_daily.py --no-refresh   # 跳过 refresh.py，直接用现有 data/*.json
  python scripts/pick_daily.py --render-only  # 只重渲染 HTML（补完基本面后）
  python scripts/pick_daily.py --sector-str 75 --top 20
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
RPS_JSON = DATA / "rps.json"
SECTOR_JSON = DATA / "sector_rps.json"
META_JSON = DATA / "meta.json"
PICK_JSON = DATA / "pick_latest.json"
FUND_JSON = DATA / "fundamentals.json"
OUT_HTML = ROOT / "pick.html"

WEIGHTS = {"chg": 0.35, "vol": 0.25, "dist": 0.20, "sector": 0.20}


# ---------------------------------------------------------------- utils
def load_json(path: Path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def dump_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)


def pct_rank(values: list[float]) -> list[float]:
    """把一组数值转成 0-100 的横截面百分位（并列取平均名次）。"""
    n = len(values)
    if n <= 1:
        return [100.0] * n
    order = sorted(range(n), key=lambda i: values[i])
    out = [0.0] * n
    i = 0
    while i < n:
        j = i
        while j + 1 < n and values[order[j + 1]] == values[order[i]]:
            j += 1
        avg_rank = (i + j) / 2.0
        pr = 100.0 * avg_rank / (n - 1)
        for k in range(i, j + 1):
            out[order[k]] = pr
        i = j + 1
    return out


def is_limit_up(code: str, chg: float) -> bool:
    """涨停判定：主板/中小板 10%，创业板(300)/科创板(688) 20%。"""
    if chg is None:
        return False
    limit = 19.8 if code.startswith(("300", "688")) else 9.8
    return chg >= limit


def fnum(v, nd=2, dash="—"):
    return dash if v is None else f"{v:.{nd}f}"


# ---------------------------------------------------------------- 大盘温度
def market_temperature(asof: str) -> dict:
    """DPWD 大盘温度（上证指数）。失败不中断，返回空 dict。"""
    try:
        sys.path.insert(0, str(ROOT.parent / "rps_fip_package"))
        import numpy as np  # noqa: F401
        import pandas as pd
        import dpwd

        import akshare as ak

        df = ak.stock_zh_index_daily(symbol="sh000001")
        s = pd.Series(df["close"].values, index=pd.to_datetime(df["date"]))
        s = s[s.index <= pd.Timestamp(asof)]
        out = dpwd.compute_dpwd(s)
        reading = dpwd.latest_reading(out)
        reading["source"] = "上证指数(新浪源) + DPWD"
        return reading
    except Exception as e:  # 网络/依赖异常都不该中断选股
        return {"error": f"{type(e).__name__}: {e}"}


# ---------------------------------------------------------------- 筛选打分
def screen(args) -> dict:
    rps = load_json(RPS_JSON)
    meta = load_json(META_JSON, {})
    sec = load_json(SECTOR_JSON, {})
    if not rps:
        raise SystemExit("data/rps.json 读取失败，请先跑 scripts/refresh.py --mode intraday")

    rows = rps.get("data", [])
    rmeta = rps.get("meta", {})
    sec_rows = sec.get("data", [])
    smap = {s["industry"]: s for s in sec_rows}

    # 1) 底池：严格强势平滑 + 市值 ≥ 50 亿
    base = [
        r for r in rows
        if r.get("smooth_strength")
        and (r.get("market_cap_yi") or 0) >= args.min_mcap
        and not r.get("halted")
    ]
    n_base = len(base)

    # 2) 板块过滤
    pool = []
    for r in base:
        s = smap.get(r.get("industry"))
        if not s:
            continue
        if (s.get("str6m") or 0) < args.sector_str:
            continue
        r = dict(r)
        r["_sector"] = s
        pool.append(r)

    # 3) 涨跌停标记，默认剔除涨停（10 点追不进）
    for r in pool:
        r["_limit_up"] = is_limit_up(r["code"], r.get("chg_pct"))
    n_limit_up = sum(1 for r in pool if r["_limit_up"])
    if not args.allow_limit_up:
        pool = [r for r in pool if not r["_limit_up"]]

    if not pool:
        raise SystemExit("候选池为空：可放宽 --sector-str / --min-mcap")

    # 4) 盘中打分
    chgs = [r.get("chg_pct") or 0.0 for r in pool]
    vols = [r.get("vol_ratio20") or 0.0 for r in pool]
    dists = [-abs(r.get("dist_high_250") or 99.0) for r in pool]
    secs = [r["_sector"].get("str6m") or 0.0 for r in pool]
    p_chg, p_vol, p_dist, p_sec = (
        pct_rank(chgs), pct_rank(vols), pct_rank(dists), pct_rank(secs),
    )
    for i, r in enumerate(pool):
        r["score"] = round(
            WEIGHTS["chg"] * p_chg[i]
            + WEIGHTS["vol"] * p_vol[i]
            + WEIGHTS["dist"] * p_dist[i]
            + WEIGHTS["sector"] * p_sec[i],
            1,
        )
        r["p_chg"], r["p_vol"], r["p_dist"], r["p_sec"] = (
            round(p_chg[i], 1), round(p_vol[i], 1), round(p_dist[i], 1), round(p_sec[i], 1),
        )

    pool.sort(key=lambda r: (-r["score"], -(r.get("chg_pct") or 0)))
    for i, r in enumerate(pool, 1):
        r["rank"] = i

    # 5) 板块 TOP（按 6 月强度）
    sec_top = sorted(sec_rows, key=lambda s: -(s.get("str6m") or 0))[:6]

    def slim(r: dict) -> dict:
        s = r["_sector"]
        return {
            "rank": r["rank"],
            "code": r["code"],
            "name": r["name"],
            "industry": r.get("industry"),
            "close": r.get("close"),
            "chg_pct": r.get("chg_pct"),
            "vol_ratio20": r.get("vol_ratio20"),
            "dist_high_250": r.get("dist_high_250"),
            "amount_yi": r.get("amount_yi"),
            "market_cap_yi": r.get("market_cap_yi"),
            "composite_rps": r.get("composite_rps"),
            "rps50": r.get("rps50"),
            "rps120": r.get("rps120"),
            "rps250": r.get("rps250"),
            "fip50": r.get("fip50"),
            "fip120": r.get("fip120"),
            "fip250": r.get("fip250"),
            "vs_ma50": r.get("vs_ma50"),
            "rsi14": r.get("rsi14"),
            "sector_str6m": s.get("str6m"),
            "sector_rank1m": s.get("rank1m"),
            "sector_rank1m_1w_ago": s.get("rank1m_1w_ago"),
            "score": r["score"],
            "p_chg": r["p_chg"],
            "p_vol": r["p_vol"],
            "p_dist": r["p_dist"],
            "p_sec": r["p_sec"],
        }

    cands = [slim(r) for r in pool[: args.top]]
    fund_top5 = [c["code"] for c in cands[:5]]

    return {
        "meta": {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "asof": rmeta.get("asof"),
            "quote_time": rmeta.get("quote_time"),
            "universe": rmeta.get("count"),
            "n_base": n_base,
            "n_pool": len(pool),
            "n_limit_up_excluded": n_limit_up,
            "params": {
                "min_mcap_yi": args.min_mcap,
                "sector_str6m_min": args.sector_str,
                "weights": WEIGHTS,
                "top": args.top,
                "allow_limit_up": args.allow_limit_up,
            },
            "dpwd": market_temperature(str(rmeta.get("asof") or datetime.now().date())),
        },
        "candidates": cands,
        "fund_top5": fund_top5,
        "sectors_top": [
            {
                "industry": s["industry"],
                "str6m": s.get("str6m"),
                "str3m": s.get("str3m"),
                "str1m": s.get("str1m"),
                "rank1m": s.get("rank1m"),
                "rank1m_1w_ago": s.get("rank1m_1w_ago"),
                "ret1m_cw": s.get("ret1m_cw"),
                "n_strong": s.get("n_strong"),
                "n_stocks": s.get("n_stocks"),
                "top_name": s.get("top_name"),
            }
            for s in sec_top
        ],
    }


# ---------------------------------------------------------------- 渲染
CSS = """
*{box-sizing:border-box}
body{margin:0;background:#0f1216;color:#e6e8ea;font:14px/1.6 -apple-system,"Segoe UI","Microsoft YaHei",sans-serif}
.wrap{max-width:1180px;margin:0 auto;padding:24px 20px 60px}
h1{font-size:24px;margin:0 0 4px}
.sub{color:#8b939c;font-size:13px;margin-bottom:20px}
.card{background:#171b21;border:1px solid #232a33;border-radius:10px;padding:16px 18px;margin-bottom:18px}
.card h2{font-size:16px;margin:0 0 12px;color:#cfd6de;display:flex;align-items:center;gap:8px}
.badge{font-size:12px;padding:2px 8px;border-radius:20px;background:#243040;color:#7fb2ff}
.kpis{display:flex;flex-wrap:wrap;gap:12px}
.kpi{flex:1 1 150px;background:#11161c;border:1px solid #232a33;border-radius:8px;padding:12px 14px}
.kpi .k{font-size:12px;color:#8b939c}
.kpi .v{font-size:20px;font-weight:600;margin-top:2px}
.up{color:#ff5c5c}.down{color:#25c56b}.warn{color:#f0b429}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:8px 8px;border-bottom:1px solid #232a33;text-align:right;white-space:nowrap}
th{color:#8b939c;font-weight:500;text-align:right;position:sticky;top:0;background:#171b21}
td.l,th.l{text-align:left}
tbody tr:hover{background:#1d232c}
.tblbox{max-height:none;overflow:auto}
.pill{display:inline-block;font-size:12px;padding:1px 7px;border-radius:4px;background:#2a3340;color:#9fb0c4}
.pill.top{background:#3a2f16;color:#f0b429}
.fund{border-left:3px solid #3d7eff;padding-left:14px;margin-bottom:20px}
.fund .h{display:flex;align-items:baseline;gap:10px;flex-wrap:wrap}
.fund .nm{font-size:17px;font-weight:600}
.fund .cd{color:#8b939c;font-size:13px}
.fund dl{display:grid;grid-template-columns:78px 1fr;gap:4px 10px;margin:10px 0 0}
.fund dt{color:#8b939c;font-size:13px}
.fund dd{margin:0;font-size:13px}
.note{color:#8b939c;font-size:12px;line-height:1.8}
.miss{color:#f0b429;font-size:13px}
a{color:#7fb2ff}
"""

FUND_FIELDS = [
    ("business", "公司主营"),
    ("position", "行业地位"),
    ("financial", "最新财务"),
    ("logic", "上涨逻辑"),
    ("risk", "风险点"),
]


def render(pick: dict) -> str:
    m = pick["meta"]
    dp = m.get("dpwd") or {}
    fund = load_json(FUND_JSON, {}) or {}
    cands = pick["candidates"]
    top5 = pick["fund_top5"]

    def zone_cls(z: str) -> str:
        if "AGGRESSIVE" in z or "ACTIVE" in z:
            return "up"
        if "MODERATE" in z:
            return "warn"
        return "down"

    temp = dp.get("TEMP10")
    kpis = [
        ("行情时间", m.get("quote_time") or m.get("asof") or "—", ""),
        ("大盘温度 TEMP10", fnum(temp, 1, "—"), zone_cls(dp.get("zone", ""))),
        ("温度区带", dp.get("zone") or "—", zone_cls(dp.get("zone", ""))),
        ("底池 / 候选", f'{m.get("n_base", "—")} / {m.get("n_pool", "—")}', ""),
        ("剔除涨停", str(m.get("n_limit_up_excluded", 0)), ""),
    ]
    kpi_html = "".join(
        f'<div class="kpi"><div class="k">{k}</div><div class="v {cls}">{v}</div></div>'
        for k, v, cls in kpis
    )

    rows = []
    for c in cands:
        score_cls = "warn" if c["score"] >= 80 else ""
        tag = '<span class="pill top">TOP5</span>' if c["code"] in top5 else ""
        chg_cls = "up" if (c["chg_pct"] or 0) > 0 else ("down" if (c["chg_pct"] or 0) < 0 else "")
        rows.append(
            "<tr>"
            f'<td class="l">{c["rank"]}</td>'
            f'<td class="l">{c["code"]}</td>'
            f'<td class="l">{c["name"]} {tag}</td>'
            f'<td class="l">{c.get("industry") or "—"}</td>'
            f'<td>{fnum(c.get("close"))}</td>'
            f'<td class="{chg_cls}">{fnum(c.get("chg_pct"))}%</td>'
            f'<td>{fnum(c.get("vol_ratio20"))}</td>'
            f'<td>{fnum(c.get("dist_high_250"))}%</td>'
            f'<td>{fnum(c.get("composite_rps"), 1)}</td>'
            f'<td>{fnum(c.get("fip50"), 3)}</td>'
            f'<td>{fnum(c.get("sector_str6m"), 1)}</td>'
            f'<td>{fnum(c.get("market_cap_yi"), 0)}亿</td>'
            f'<td class="{score_cls}"><b>{fnum(c.get("score"), 1)}</b></td>'
            "</tr>"
        )

    fund_cards = []
    for code in top5:
        c = next((x for x in cands if x["code"] == code), None)
        if not c:
            continue
        f = fund.get(code) or {}
        if f:
            dl = "".join(
                f"<dt>{label}</dt><dd>{f.get(key) or '—'}</dd>" for key, label in FUND_FIELDS
            )
            body = f"<dl>{dl}</dl>"
            stamp = f'<span class="pill">基本面更新 {f.get("updated", "—")}</span>'
        else:
            body = '<p class="miss">基本面卡片待补充（下次运行自动补齐）</p>'
            stamp = ""
        fund_cards.append(
            f'<div class="card fund"><div class="h"><span class="nm">{c["name"]}</span>'
            f'<span class="cd">{code} · {c.get("industry") or "—"}</span>'
            f'<span class="badge">综合RPS {fnum(c.get("composite_rps"), 1)}</span>'
            f'<span class="badge">盘中分 {fnum(c.get("score"), 1)}</span>{stamp}</div>{body}</div>'
        )

    sec_rows = []
    for s in pick.get("sectors_top", []):
        d = s.get("rank1m_1w_ago")
        delta = ""
        if d is not None and s.get("rank1m") is not None:
            diff = d - s["rank1m"]
            delta = f'<span class="{"up" if diff > 0 else ("down" if diff < 0 else "")}">{"↑" if diff > 0 else ("↓" if diff < 0 else "→")}{abs(diff)}</span>'
        sec_rows.append(
            "<tr>"
            f'<td class="l">{s["industry"]}</td>'
            f'<td>{fnum(s.get("str6m"), 1)}</td>'
            f'<td>{fnum(s.get("str3m"), 1)}</td>'
            f'<td>{fnum(s.get("str1m"), 1)}</td>'
            f'<td>{s.get("rank1m") or "—"} {delta}</td>'
            f'<td>{fnum(s.get("ret1m_cw"))}%</td>'
            f'<td>{s.get("n_strong", 0)}/{s.get("n_stocks", 0)}</td>'
            f'<td class="l">{s.get("top_name") or "—"}</td>'
            "</tr>"
        )

    p = m.get("params", {})
    w = p.get("weights", WEIGHTS)
    return f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>A股盘中选股日报 · {m.get("asof")}</title>
<style>{CSS}</style></head><body><div class="wrap">
<h1>A股盘中选股日报</h1>
<div class="sub">动量底池 + 当日盘口打分 · 数据截至 {m.get("quote_time") or m.get("asof")} · 生成于 {m.get("generated_at")}</div>

<div class="card"><h2>大盘状态</h2><div class="kpis">{kpi_html}</div></div>

<div class="card"><h2>今日候选（按盘中综合分排序）</h2>
<div class="tblbox"><table>
<thead><tr>
<th class="l">#</th><th class="l">代码</th><th class="l">名称</th><th class="l">行业</th>
<th>现价</th><th>当日涨幅</th><th>量比20</th><th>距250日高</th><th>综合RPS</th><th>FIP50</th>
<th>板块6月强度</th><th>市值</th><th>盘中分</th>
</tr></thead><tbody>{''.join(rows)}</tbody></table></div></div>

<div class="card"><h2>Top 5 基本面</h2>{''.join(fund_cards) or '<p class="miss">暂无</p>'}</div>

<div class="card"><h2>强势板块（6月强度 TOP6）</h2>
<div class="tblbox"><table>
<thead><tr><th class="l">行业</th><th>6月强度</th><th>3月强度</th><th>1月强度</th><th>1月排名</th><th>1月涨幅</th><th>强势/总数</th><th class="l">板块龙头</th></tr></thead>
<tbody>{''.join(sec_rows)}</tbody></table></div></div>

<div class="card"><h2>口径说明</h2><div class="note">
<b>底池</b>：综合 RPS（0.3×RPS50 + 0.3×RPS120 + 0.4×RPS250）≥ 90，且 FIP50/FIP120/FIP250 全部 &lt; 0（上涨日多于下跌日、走势平滑），总市值 ≥ {p.get("min_mcap_yi", 50)} 亿元；已剔除 ST、退市整理期、停牌股。<br>
<b>板块过滤</b>：所属行业 6 月强度 ≥ {p.get("sector_str6m_min", 80)}（全市场前 20%），强度为横截面百分位。<br>
<b>盘中打分</b>：当日涨幅 {w.get("chg")} + 量比(20日) {w.get("vol")} + 距250日高点 {w.get("dist")} + 板块6月强度 {w.get("sector")}，各因子先在候选池内取横截面百分位再加权，满分 100。<br>
<b>剔除</b>：涨停股（10 点无法买入）默认剔除，本期剔除 {m.get("n_limit_up_excluded", 0)} 只。<br>
<b>大盘温度</b>：DPWD（TEMP10），≥85 进攻、75–85 中性、&lt;75 防御；温度 &lt; 70 时按既有策略应清仓观望，仅作参考。<br>
<span class="warn">本页为量化规则筛选结果，不构成投资建议，据此操作风险自负。</span>
</div></div>
</div></body></html>"""


# ---------------------------------------------------------------- main
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="all", choices=["all", "screen", "render-only"])
    ap.add_argument("--no-refresh", action="store_true", help="不调用 refresh.py，直接用现有 data/*.json")
    ap.add_argument("--sector-str", type=float, default=80.0, help="板块 6 月强度下限（百分位）")
    ap.add_argument("--min-mcap", type=float, default=50.0, help="总市值下限（亿元）")
    ap.add_argument("--top", type=int, default=20, help="候选表输出条数")
    ap.add_argument("--allow-limit-up", action="store_true", help="保留涨停股")
    args = ap.parse_args()

    if args.mode in ("all", "screen"):
        if not args.no_refresh:
            print("[1/3] 刷新实时行情与指标 ...", flush=True)
            r = subprocess.run(
                [sys.executable, str(ROOT / "scripts" / "refresh.py"), "--mode", "intraday"],
                cwd=str(ROOT),
            )
            if r.returncode != 0:
                print("  ! refresh.py 退出码非 0，仍继续用现有数据", flush=True)
        print("[2/3] 筛选与打分 ...", flush=True)
        pick = screen(args)
        dump_json(PICK_JSON, pick)
        m = pick["meta"]
        print(f"  底池 {m['n_base']} → 候选 {m['n_pool']}（剔除涨停 {m['n_limit_up_excluded']}）"
              f"｜TEMP10 {(m.get('dpwd') or {}).get('TEMP10', '—')}")
        for c in pick["candidates"][:10]:
            print(f"  {c['rank']:>2}. {c['code']} {c['name']:<8} {c['score']:>5} "
                  f"涨幅{c['chg_pct']:+}% 量比{c['vol_ratio20']} {c['industry']}")
        missing = [c for c in pick["fund_top5"] if c not in (load_json(FUND_JSON) or {})]
        print("  Top5 待补基本面: " + (", ".join(missing) if missing else "无（已全部缓存）"))
    else:
        pick = load_json(PICK_JSON)
        if not pick:
            raise SystemExit("data/pick_latest.json 不存在，先跑 screen")

    if args.mode in ("all", "render-only"):
        print("[3/3] 渲染 pick.html ...", flush=True)
        OUT_HTML.write_text(render(pick), encoding="utf-8")
        print(f"  已写出 {OUT_HTML}（{OUT_HTML.stat().st_size/1024:.0f} KB）")


if __name__ == "__main__":
    main()
