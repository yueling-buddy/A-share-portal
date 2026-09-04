#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A股盘中选股日报生成器。

口径（用户 2026-09-04 最终确认，硬条件需全部满足才入选）：
  1. 综合 RPS > 85
  2. FIP250 ≤ 0
  3. 总市值 > 50 亿元
  4. 量比 > 1.5（分时归一化口径：当日累计量/已交易分钟 ÷ 前 5 日均量/240。
     10 点只有 30 分钟数据时不会被系统性低估；rps.json 里的 vol_ratio20 是
     「当日累计量/20日均量」的全日口径，盘中直接拿来筛会几乎全军覆没，故不采用）
  5. 股价高于 MA50 且低于 1.3 倍 MA50（即 0 < vs_ma50 < 30%）
  6. 距 250 日新高 ≥ -15%
  另：剔除停牌；涨停股默认剔除（10 点买不进）。
  默认按综合 RPS 降序排列，--sort 可切 chg（当日涨幅）/ score（盘中综合分）。

用法：
  python scripts/pick_daily.py                # 刷新数据 + 筛选 + 渲染
  python scripts/pick_daily.py --no-refresh   # 跳过 refresh.py，直接用现有 data/*.json
  python scripts/pick_daily.py --render-only  # 只重渲染 HTML（补完基本面后）
  python scripts/pick_daily.py --rps-min 90 --vol-min 2 --sort chg
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
CACHE = DATA / "kline_cache.parquet"
PICK_JSON = DATA / "pick_latest.json"
FUND_JSON = DATA / "fundamentals.json"
OUT_HTML = ROOT / "pick.html"

WEIGHTS = {"chg": 0.40, "vol": 0.20, "dist": 0.20, "sector": 0.20}


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


def num(v, default=None):
    return v if isinstance(v, (int, float)) else default


# ---------------------------------------------------------------- 量比（分时归一化）
def elapsed_minutes(quote_time: str | None) -> int:
    """A股当日已交易分钟数：9:30-11:30 共 120 分，13:00-15:00 共 120 分，合计 240。"""
    if not quote_time:
        return 0
    try:
        t = datetime.strptime(quote_time.strip()[-8:], "%H:%M:%S")
    except Exception:
        return 0
    m = t.hour * 60 + t.minute
    open_m, noon_m, pm_m, close_m = 9 * 60 + 30, 11 * 60 + 30, 13 * 60, 15 * 60
    if m <= open_m:
        return 0
    if m <= noon_m:
        return m - open_m
    if m <= pm_m:
        return 120
    if m >= close_m:
        return 240
    return 120 + (m - pm_m)


def load_vol_baseline(asof: str | None) -> tuple[dict, str | None]:
    """从 kline_cache 取每只股票最近 5 个已收盘交易日的均量（股）。

    只取 date < 计算日 的 bar，避免把盘中那根未完成的量算进基准。
    返回 (dict[code] -> 均量, 缓存最后日期)。
    """
    try:
        import pandas as pd

        df = pd.read_parquet(CACHE, columns=["date", "code", "volume"])
        last_date = str(df["date"].max().date())
        if asof:
            df = df[df["date"] < pd.Timestamp(asof)]
        df = df.sort_values(["code", "date"]).groupby("code").tail(5)
        return df.groupby("code")["volume"].mean().to_dict(), last_date
    except Exception:
        return {}, None


# ---------------------------------------------------------------- 大盘温度
def market_temperature(asof: str) -> dict:
    """DPWD 大盘温度（上证指数）。失败不中断，返回空 dict。"""
    try:
        sys.path.insert(0, str(ROOT.parent / "rps_fip_package"))
        import pandas as pd
        import dpwd
        import akshare as ak

        df = ak.stock_zh_index_daily(symbol="sh000001")
        s = pd.Series(df["close"].values, index=pd.to_datetime(df["date"]))
        s = s[s.index <= pd.Timestamp(asof)]
        reading = dpwd.latest_reading(dpwd.compute_dpwd(s))
        reading["source"] = "上证指数(新浪源) + DPWD"
        return reading
    except Exception as e:  # 网络/依赖异常都不该中断选股
        return {"error": f"{type(e).__name__}: {e}"}


# ---------------------------------------------------------------- 筛选打分
def screen(args) -> dict:
    rps = load_json(RPS_JSON)
    sec = load_json(SECTOR_JSON, {})
    if not rps:
        raise SystemExit("data/rps.json 读取失败，请先跑 scripts/refresh.py --mode intraday")

    rows = rps.get("data", [])
    rmeta = rps.get("meta", {})
    asof = rmeta.get("asof")
    quote_time = rmeta.get("quote_time")
    smap = {s["industry"]: s for s in sec.get("data", [])}

    mins = elapsed_minutes(quote_time)
    baseline, cache_last = load_vol_baseline(asof)
    vr_mode = "分时归一化（当日累计量/已交易分钟 ÷ 前5日均量/240）"

    # 1) 逐条硬条件（记录漏斗，便于回看是哪一档卡掉的）
    funnel = []

    def step(label: str, pool: list) -> list:
        funnel.append([label, len(pool)])
        return pool

    pool = step("全市场有效横截面", [r for r in rows if not r.get("halted")])

    def keep(pred) -> list:
        nonlocal pool
        pool = [r for r in pool if pred(r)]
        return pool

    keep(lambda r: (num(r.get("composite_rps"), 0) or 0) > args.rps_min)
    step(f"综合RPS > {args.rps_min:g}", pool)
    keep(lambda r: (num(r.get("fip250"), 1) or 1) <= args.fip_max)
    step(f"FIP250 ≤ {args.fip_max:g}", pool)
    keep(lambda r: (num(r.get("market_cap_yi"), 0) or 0) > args.min_mcap)
    step(f"总市值 > {args.min_mcap:g}亿", pool)
    keep(lambda r: (num(r.get("vs_ma50"), -999) or -999) > 0)
    step("股价 > MA50", pool)
    keep(lambda r: (num(r.get("vs_ma50"), 999) or 999) < args.ma50_ceil_pct)
    step(f"股价 < {1 + args.ma50_ceil_pct / 100:.2g}× MA50", pool)
    keep(lambda r: (num(r.get("dist_high_250"), -999) or -999) >= args.dist_high_min)
    step(f"距250日新高 ≥ {args.dist_high_min:g}%", pool)

    # 量比（这一步需要算，单独处理）
    n_before_vol = len(pool)
    vol_ok = []
    for r in pool:
        vol_mn = num(r.get("volume_mn"))          # 百万股（当日累计）
        base = baseline.get(r["code"])
        if base and base > 0 and vol_mn and mins >= 5:
            vr = (vol_mn * 1e6 / mins) / (base / 240.0)
        else:
            vr = num(r.get("vol_ratio20"))        # 兜底：用全日口径
        r["vol_ratio"] = round(vr, 2) if isinstance(vr, (int, float)) else None
        if isinstance(vr, (int, float)) and vr > args.vol_min:
            vol_ok.append(r)
    pool = vol_ok
    step(f"量比 > {args.vol_min:g}", pool)
    n_limit_up = sum(1 for r in pool if is_limit_up(r["code"], num(r.get("chg_pct"))))
    if not args.allow_limit_up:
        pool = [r for r in pool if not is_limit_up(r["code"], num(r.get("chg_pct")))]
    step(f"剔除涨停（{n_limit_up} 只）" if n_limit_up else "剔除涨停", pool)

    if not pool:
        raise SystemExit(
            "候选池为空。漏斗：" + " → ".join(f"{k}={v}" for k, v in funnel)
            + "（可放宽 --rps-min / --vol-min / --dist-high-min）"
        )

    # 2) 盘中综合分（仅作参考列，不参与硬条件）
    for r in pool:
        r["_sector"] = smap.get(r.get("industry")) or {}
    chgs = [num(r.get("chg_pct"), 0.0) or 0.0 for r in pool]
    vols = [num(r.get("vol_ratio"), 0.0) or 0.0 for r in pool]
    dists = [-abs(num(r.get("dist_high_250"), 99.0) or 99.0) for r in pool]
    secs = [num(r["_sector"].get("str6m"), 0.0) or 0.0 for r in pool]
    p_chg, p_vol, p_dist, p_sec = pct_rank(chgs), pct_rank(vols), pct_rank(dists), pct_rank(secs)
    for i, r in enumerate(pool):
        r["score"] = round(
            WEIGHTS["chg"] * p_chg[i] + WEIGHTS["vol"] * p_vol[i]
            + WEIGHTS["dist"] * p_dist[i] + WEIGHTS["sector"] * p_sec[i], 1)
        r["p_chg"], r["p_vol"] = round(p_chg[i], 1), round(p_vol[i], 1)

    key = {
        "rps": lambda r: -(num(r.get("composite_rps"), 0) or 0),
        "chg": lambda r: -(num(r.get("chg_pct"), 0) or 0),
        "score": lambda r: -r["score"],
    }[args.sort]
    pool.sort(key=lambda r: (key(r), -(num(r.get("composite_rps"), 0) or 0)))
    for i, r in enumerate(pool, 1):
        r["rank"] = i

    def slim(r: dict) -> dict:
        s = r["_sector"]
        return {
            "rank": r["rank"], "code": r["code"], "name": r["name"],
            "industry": r.get("industry"), "close": r.get("close"),
            "chg_pct": r.get("chg_pct"), "vol_ratio": r.get("vol_ratio"),
            "vol_ratio20": r.get("vol_ratio20"), "amount_yi": r.get("amount_yi"),
            "dist_high_250": r.get("dist_high_250"), "vs_ma50": r.get("vs_ma50"),
            "market_cap_yi": r.get("market_cap_yi"),
            "composite_rps": r.get("composite_rps"),
            "rps50": r.get("rps50"), "rps120": r.get("rps120"), "rps250": r.get("rps250"),
            "fip50": r.get("fip50"), "fip120": r.get("fip120"), "fip250": r.get("fip250"),
            "rsi14": r.get("rsi14"), "ret1m": r.get("ret1m"), "ret3m": r.get("ret3m"),
            "sector_str6m": s.get("str6m"), "sector_rank1m": s.get("rank1m"),
            "score": r["score"], "p_chg": r["p_chg"], "p_vol": r["p_vol"],
        }

    cands = [slim(r) for r in pool[: args.top]]
    fund_top5 = [c["code"] for c in cands[:5]]
    sec_top = sorted(sec.get("data", []), key=lambda s: -(num(s.get("str6m"), 0) or 0))[:6]

    return {
        "meta": {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "asof": asof,
            "quote_time": quote_time,
            "universe": rmeta.get("count"),
            "n_pool": len(pool),
            "n_limit_up_excluded": n_limit_up,
            "n_before_vol": n_before_vol,
            "elapsed_minutes": mins,
            "vol_ratio_mode": vr_mode,
            "cache_last_date": cache_last,
            "sort": args.sort,
            "funnel": funnel,
            "params": {
                "rps_min": args.rps_min, "fip_max": args.fip_max,
                "min_mcap_yi": args.min_mcap, "vol_min": args.vol_min,
                "ma50_ceil_pct": args.ma50_ceil_pct, "dist_high_min": args.dist_high_min,
                "top": args.top, "allow_limit_up": args.allow_limit_up,
                "weights": WEIGHTS,
            },
            "dpwd": market_temperature(str(asof or datetime.now().date())),
        },
        "candidates": cands,
        "fund_top5": fund_top5,
        "sectors_top": [
            {"industry": s["industry"], "str6m": s.get("str6m"), "str3m": s.get("str3m"),
             "str1m": s.get("str1m"), "rank1m": s.get("rank1m"),
             "rank1m_1w_ago": s.get("rank1m_1w_ago"), "ret1m_cw": s.get("ret1m_cw"),
             "n_strong": s.get("n_strong"), "n_stocks": s.get("n_stocks"),
             "top_name": s.get("top_name")}
            for s in sec_top
        ],
    }


# ---------------------------------------------------------------- 渲染
CSS = """
*{box-sizing:border-box}
body{margin:0;background:#0f1216;color:#e6e8ea;font:14px/1.6 -apple-system,"Segoe UI","Microsoft YaHei",sans-serif}
.wrap{max-width:1400px;margin:0 auto;padding:24px 20px 60px}
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
th,td{padding:8px 6px;border-bottom:1px solid #232a33;text-align:right;white-space:nowrap}
th{color:#8b939c;font-weight:500;text-align:right;position:sticky;top:0;background:#171b21}
td.l,th.l{text-align:left}
tbody tr:hover{background:#1d232c}
.tblbox{overflow-x:auto}
.pill{display:inline-block;font-size:12px;padding:1px 7px;border-radius:4px;background:#2a3340;color:#9fb0c4}
.pill.top{background:#3a2f16;color:#f0b429}
.fund{border-left:3px solid #3d7eff;padding-left:14px;margin-bottom:20px}
.fund .h{display:flex;align-items:baseline;gap:10px;flex-wrap:wrap}
.fund .nm{font-size:17px;font-weight:600}
.fund .cd{color:#8b939c;font-size:13px}
.fund dl{display:grid;grid-template-columns:78px 1fr;gap:4px 10px;margin:10px 0 0}
.fund dt{color:#8b939c;font-size:13px}
.fund dd{margin:0;font-size:13px}
.note{color:#8b939c;font-size:12px;line-height:1.9}
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
        ("入选数量", str(m.get("n_pool", "—")), ""),
        ("已交易分钟", str(m.get("elapsed_minutes", "—")), ""),
    ]
    kpi_html = "".join(
        f'<div class="kpi"><div class="k">{k}</div><div class="v {cls}">{v}</div></div>'
        for k, v, cls in kpis
    )

    rows = []
    for c in cands:
        tag = '<span class="pill top">TOP5</span>' if c["code"] in top5 else ""
        chg = num(c.get("chg_pct"))
        chg_cls = "up" if (chg or 0) > 0 else ("down" if (chg or 0) < 0 else "")
        vr = num(c.get("vol_ratio"))
        rows.append(
            "<tr>"
            f'<td class="l">{c["rank"]}</td>'
            f'<td class="l">{c["code"]}</td>'
            f'<td class="l">{c["name"]} {tag}</td>'
            f'<td class="l">{c.get("industry") or "—"}</td>'
            f'<td>{fnum(c.get("close"))}</td>'
            f'<td class="{chg_cls}">{fnum(chg)}%</td>'
            f'<td class="{"warn" if (vr or 0) >= 2 else ""}">{fnum(vr)}</td>'
            f'<td>{fnum(c.get("dist_high_250"))}%</td>'
            f'<td>{fnum(c.get("vs_ma50"))}%</td>'
            f'<td>{fnum(c.get("composite_rps"), 1)}</td>'
            f'<td>{fnum(c.get("fip250"), 3)}</td>'
            f'<td>{fnum(c.get("market_cap_yi"), 0)}亿</td>'
            f'<td>{fnum(c.get("sector_str6m"), 1)}</td>'
            f'<td><b>{fnum(c.get("score"), 1)}</b></td>'
            "</tr>"
        )

    fund_cards = []
    for code in top5:
        c = next((x for x in cands if x["code"] == code), None)
        if not c:
            continue
        f = fund.get(code) or {}
        if f:
            dl = "".join(f"<dt>{label}</dt><dd>{f.get(key) or '—'}</dd>"
                         for key, label in FUND_FIELDS)
            body, stamp = f"<dl>{dl}</dl>", f'<span class="pill">基本面更新 {f.get("updated", "—")}</span>'
        else:
            body = '<p class="miss">基本面卡片待补充（下次运行自动补齐）</p>'
            stamp = ""
        fund_cards.append(
            f'<div class="card fund"><div class="h"><span class="nm">{c["name"]}</span>'
            f'<span class="cd">{code} · {c.get("industry") or "—"}</span>'
            f'<span class="badge">综合RPS {fnum(c.get("composite_rps"), 1)}</span>'
            f'<span class="badge">量比 {fnum(c.get("vol_ratio"))}</span>{stamp}</div>{body}</div>'
        )

    sec_rows = []
    for s in pick.get("sectors_top", []):
        d = s.get("rank1m_1w_ago")
        delta = ""
        if d is not None and s.get("rank1m") is not None:
            diff = d - s["rank1m"]
            delta = (f'<span class="{"up" if diff > 0 else ("down" if diff < 0 else "")}">'
                     f'{"↑" if diff > 0 else ("↓" if diff < 0 else "→")}{abs(diff)}</span>')
        sec_rows.append(
            "<tr>"
            f'<td class="l">{s["industry"]}</td>'
            f'<td>{fnum(s.get("str6m"), 1)}</td><td>{fnum(s.get("str3m"), 1)}</td>'
            f'<td>{fnum(s.get("str1m"), 1)}</td><td>{s.get("rank1m") or "—"} {delta}</td>'
            f'<td>{fnum(s.get("ret1m_cw"))}%</td>'
            f'<td>{s.get("n_strong", 0)}/{s.get("n_stocks", 0)}</td>'
            f'<td class="l">{s.get("top_name") or "—"}</td>'
            "</tr>"
        )

    funnel_html = " → ".join(f"{k} <b>{v}</b>" for k, v in m.get("funnel", []))
    p = m.get("params", {})
    w = p.get("weights", WEIGHTS)
    return f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>A股盘中选股日报 · {m.get("asof")}</title>
<style>{CSS}</style></head><body><div class="wrap">
<h1>A股盘中选股日报</h1>
<div class="sub">硬条件全过才入选 · 数据截至 {m.get("quote_time") or m.get("asof")} · 生成于 {m.get("generated_at")}</div>

<div class="card"><h2>大盘状态</h2><div class="kpis">{kpi_html}</div></div>

<div class="card"><h2>今日入选（按{m.get("sort")}降序）</h2>
<div class="tblbox"><table>
<thead><tr>
<th class="l">#</th><th class="l">代码</th><th class="l">名称</th><th class="l">行业</th>
<th>现价</th><th>当日涨幅</th><th>量比</th><th>距250日高</th><th>vs MA50</th>
<th>综合RPS</th><th>FIP250</th><th>市值</th><th>板块6月强度</th><th>盘中分</th>
</tr></thead><tbody>{''.join(rows)}</tbody></table></div>
<div class="note" style="margin-top:10px">筛选漏斗：{funnel_html}</div></div>

<div class="card"><h2>Top 5 基本面</h2>{''.join(fund_cards) or '<p class="miss">暂无</p>'}</div>

<div class="card"><h2>强势板块（6月强度 TOP6）</h2>
<div class="tblbox"><table>
<thead><tr><th class="l">行业</th><th>6月强度</th><th>3月强度</th><th>1月强度</th><th>1月排名</th><th>1月涨幅</th><th>强势/总数</th><th class="l">板块龙头</th></tr></thead>
<tbody>{''.join(sec_rows)}</tbody></table></div></div>

<div class="card"><h2>选股标准与口径</h2><div class="note">
<b>硬条件（全部满足）</b>：① 综合 RPS &gt; {p.get("rps_min", 85):g}（0.3×RPS50 + 0.3×RPS120 + 0.4×RPS250）；
② FIP250 ≤ {p.get("fip_max", 0):g}（区间内下跌日数占比不高于上涨日数）；
③ 总市值 &gt; {p.get("min_mcap_yi", 50):g} 亿元；
④ 量比 &gt; {p.get("vol_min", 1.5):g}；
⑤ 股价在 MA50 上方，但不超过 {1 + p.get("ma50_ceil_pct", 30) / 100:.2g} 倍 MA50（防止追高偏离过大）；
⑥ 距 250 日新高 ≥ {p.get("dist_high_min", -15):g}%。<br>
<b>量比口径</b>：{m.get("vol_ratio_mode")}。当前已交易 {m.get("elapsed_minutes")} 分钟，基准取 K 线缓存中
计算日之前最近 5 个交易日均量（缓存末日 {m.get("cache_last_date") or "—"}）。
用「当日累计量 ÷ 20 日均量」的全日口径在盘中会系统性偏低，故未采用。<br>
<b>剔除</b>：停牌股；涨停股（10 点无法买入）默认剔除，本期剔除 {m.get("n_limit_up_excluded", 0)} 只。<br>
<b>盘中分（参考列，不参与筛选）</b>：当日涨幅 {w.get("chg")} + 量比 {w.get("vol")} + 距250日高点 {w.get("dist")} + 板块6月强度 {w.get("sector")}，各因子在入选池内取横截面百分位后加权，满分 100。<br>
<b>大盘温度</b>：DPWD（TEMP10），≥85 进攻、75–85 中性、&lt;75 防御；既有策略在 TEMP10 &lt; 70 时应清仓观望，此处仅作参考。<br>
<span class="warn">本页为量化规则筛选结果，不构成投资建议，据此操作风险自负。</span>
</div></div>
</div></body></html>"""


# ---------------------------------------------------------------- main
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="all", choices=["all", "screen", "render-only"])
    ap.add_argument("--no-refresh", action="store_true", help="不调用 refresh.py，直接用现有 data/*.json")
    ap.add_argument("--rps-min", type=float, default=85.0, help="综合 RPS 下限（严格大于）")
    ap.add_argument("--fip-max", type=float, default=0.0, help="FIP250 上限（小于等于）")
    ap.add_argument("--min-mcap", type=float, default=50.0, help="总市值下限（亿元，严格大于）")
    ap.add_argument("--vol-min", type=float, default=1.5, help="量比下限（分时归一化口径，严格大于）")
    ap.add_argument("--ma50-ceil-pct", type=float, default=30.0, help="股价相对 MA50 的最大正偏离%%（对应 1.3 倍）")
    ap.add_argument("--dist-high-min", type=float, default=-15.0, help="距 250 日新高的最小百分比（≥）")
    ap.add_argument("--top", type=int, default=20, help="输出条数")
    ap.add_argument("--sort", default="rps", choices=["rps", "chg", "score"], help="排序键")
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
        print("  漏斗: " + " → ".join(f"{k}={v}" for k, v in m["funnel"]))
        print(f"  入选 {m['n_pool']} 只｜已交易 {m['elapsed_minutes']} 分钟"
              f"｜TEMP10 {(m.get('dpwd') or {}).get('TEMP10', '—')}")
        for c in pick["candidates"][:10]:
            print(f"  {c['rank']:>2}. {c['code']} {c['name']:<8} RPS{c['composite_rps']:.1f} "
                  f"量比{c['vol_ratio']} 涨幅{c['chg_pct']:+}% 距高{c['dist_high_250']}% "
                  f"vsMA50 {c['vs_ma50']}% {c['industry']}")
        missing = [c for c in pick["fund_top5"] if c not in (load_json(FUND_JSON) or {})]
        print("  Top5 待补基本面: " + (", ".join(missing) if missing else "无（已全部缓存）"))
    else:
        pick = load_json(PICK_JSON)
        if not pick:
            raise SystemExit("data/pick_latest.json 不存在，先跑 screen")

    if args.mode in ("all", "render-only"):
        print("[3/3] 渲染 pick.html ...", flush=True)
        OUT_HTML.write_text(render(pick), encoding="utf-8")
        print(f"  已写出 {OUT_HTML}（{OUT_HTML.stat().st_size / 1024:.0f} KB）")


if __name__ == "__main__":
    main()
