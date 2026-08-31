#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A股动量选股盘 - 数据刷新脚本（标准、可云端运行）。

三种模式：
  --mode bootstrap   一次性构建本地 K 线缓存 data/kline_cache.parquet（最近 300 交易日）。
                     --seed-local 时从本机 rps_fip_package/data/kline/*.parquet 直接整合
                     （秒级，免去盘中云端 1.8h 的 akshare 全量拉取）；否则逐只 akshare 拉取。
  --mode intraday    盘中刷新：加载缓存 -> 拉一次全市场实时行情(stock_zh_a_spot) ->
                     用实时价更新"当天那根" -> 重算 RPS/FIP -> 写 data/rps.json 等。不写回缓存。
  --mode close       收盘定稿：同 intraday，但把当日完成的日线写回 kline_cache.parquet（持久化历史）。

数据源：
  - akshare stock_zh_a_saily（新浪前复权日线，bootstrap 用）
  - akshare stock_zh_a_spot（新浪全市场实时行情，intraday 用）
  - akshare stock_info_a_code_name（全市场代码+名称）
  - 仓库内稳定元信息：data/codes.csv（选股盘）、data/industry.csv（行业）、data/mcap.csv（市值）

产物（仓库根，GitHub Pages 源 = main /(root)）：
  - index.html        看板页面（由 index.template.html 渲染）
  - data/rps.json     主数据：{meta, data:[每只票完整记录]}
  - data/fip.json     FIP 专项导出
  - data/meta.json    元信息（冗余兼容）

设计原则：
  - 所有路径相对仓库根（脚本位于 scripts/，仓库根 = parent.parent）
  - 接口失败重试 3 次、间隔 5 秒
  - 单只/单步异常写入 data/error.log，不中断整体；最终用成功数据计算横截面
  - 禁止未来函数：所有指标仅用截至"计算时刻"的已知数据（盘中用实时价当收盘，属已知）
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
import sys
import time
import traceback
from datetime import date, datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

try:
    import akshare as ak
except Exception as e:  # pragma: no cover
    print("[FATAL] 未安装 akshare，请先 pip install -r scripts/requirements.txt:", e, file=sys.stderr)
    sys.exit(2)

ROOT = Path(__file__).resolve().parent.parent          # 仓库根（a-share-dashboard）
DATA_DIR = ROOT / "data"
TEMPLATE = ROOT / "index.template.html"
WATCHLIST_CSV = DATA_DIR / "codes.csv"                 # 选股盘（股票池）
INDUSTRY_CSV = DATA_DIR / "industry.csv"
MCAP_CSV = DATA_DIR / "mcap.csv"
RPS_JSON = DATA_DIR / "rps.json"
FIP_JSON = DATA_DIR / "fip.json"
META_JSON = DATA_DIR / "meta.json"
SECTOR_JSON = DATA_DIR / "sector_rps.json"
INDEX_HTML = ROOT / "index.html"
ERROR_LOG = DATA_DIR / "error.log"
KLINE_CACHE = DATA_DIR / "kline_cache.parquet"         # 运行时缓存（盘中用）
KLINE_SEED = DATA_DIR / "kline_cache_seed.parquet"     # 提交到仓库的冷启动种子
LOCAL_KLINE_DIR = ROOT.parent / "rps_fip_package" / "data" / "kline"  # 本机已有缓存

WINDOWS = (50, 120, 250)
N_TRADING_DAYS = 300                                   # 保留最近 N 个交易日
RETRY = 3
RETRY_WAIT = 5                                         # 秒
BJ = ZoneInfo("Asia/Shanghai")

DATA_DIR.mkdir(parents=True, exist_ok=True)
# 每次运行重置 error.log，仅保留本次异常
try:
    ERROR_LOG.write_text("", encoding="utf-8")
except Exception:
    pass


def log_err(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}\n"
    try:
        with ERROR_LOG.open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass
    print("ERROR:", msg, file=sys.stderr)


def with_retry(fn, what: str):
    """执行 fn，失败重试 RETRY 次、间隔 RETRY_WAIT 秒；全失败返回 None。"""
    last = None
    for i in range(RETRY):
        try:
            return fn()
        except Exception as e:
            last = e
            log_err(f"重试 {i + 1}/{RETRY} 失败 [{what}]: {repr(e)[:200]}")
            if i < RETRY - 1:
                time.sleep(RETRY_WAIT)
    return None


# ---------------- 指标计算（禁止未来函数） ----------------
def finite(value, digits=4):
    if value is None or pd.isna(value) or not np.isfinite(value):
        return None
    return round(float(value), digits)


def pct_return(values: pd.Series, sessions: int):
    """截至最新交易日的 N 日收益率（无未来数据）。"""
    if len(values) <= sessions:
        return None
    base = values.iloc[-sessions - 1]
    last = values.iloc[-1]
    if not np.isfinite(base) or base == 0 or not np.isfinite(last):
        return None
    return (last / base - 1.0) * 100.0


def rsi14(close: pd.Series):
    if len(close) < 16:
        return None
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean().iloc[-1]
    loss = (-delta.clip(upper=0)).rolling(14).mean().iloc[-1]
    if not np.isfinite(gain) or not np.isfinite(loss):
        return None
    if loss == 0:
        return 100.0
    rs = gain / loss
    return 100.0 - 100.0 / (1.0 + rs)


def weekly_macd_hist(frame: pd.DataFrame):
    if len(frame) < 160:
        return None
    s = frame.set_index("date")["close"].resample("W-FRI").last().dropna()
    if len(s) < 35:
        return None
    ema12 = s.ewm(span=12, adjust=False).mean()
    ema26 = s.ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    return float((dif - dea).iloc[-1])


def fip_value(close: pd.Series, window: int):
    """FIP = sign(N日收益) × (下跌日数−上涨日数) / N（截至最新，无未来）。"""
    if len(close) <= window:
        return None
    daily = close.pct_change().iloc[-window:]
    period_ret = close.iloc[-1] / close.iloc[-window - 1] - 1.0
    up = int((daily > 0).sum())
    down = int((daily < 0).sum())
    return float(np.sign(period_ret) * (down - up) / window)


def prefix(code: str) -> str:
    if code.startswith(("8", "4", "9")):
        return "bj" + code
    if code.startswith("6"):
        return "sh" + code
    return "sz" + code


# ---------------- 元信息 ----------------
def load_meta():
    """加载仓库内稳定元信息。任一缺失仅记日志不中断。"""
    industry, mcap, watch_codes, watch_names = {}, {}, None, {}
    if INDUSTRY_CSV.exists():
        try:
            with open(INDUSTRY_CSV, encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    c = (row.get("code") or "").strip()
                    v = (row.get("industry") or "").strip()
                    if c:
                        industry[c] = v or None
        except Exception as e:
            log_err(f"读取 industry.csv 失败: {repr(e)[:200]}")
    if MCAP_CSV.exists():
        try:
            m = pd.read_csv(MCAP_CSV, dtype={"code": str})
            m["code"] = m["code"].str.zfill(6)
            mcap = dict(zip(m["code"], m["mcap"]))
        except Exception as e:
            log_err(f"读取 mcap.csv 失败: {repr(e)[:200]}")
    if WATCHLIST_CSV.exists():
        try:
            w = pd.read_csv(WATCHLIST_CSV, dtype={"code": str})
            w["code"] = w["code"].str.zfill(6)
            watch_codes = set(w["code"].tolist())
            watch_names = dict(zip(w["code"], w["name"].fillna("")))
        except Exception as e:
            log_err(f"读取 codes.csv(选股盘)失败: {repr(e)[:200]}")
    return industry, mcap, watch_codes, watch_names


# ---------------- 单只指标 ----------------
def metrics_from_df(df: pd.DataFrame, code: str, name: str, industry, mcap_map, watch_codes):
    """从已含"当天那根"的日线 DataFrame 计算单只全部指标（无未来函数）。"""
    df = df.tail(N_TRADING_DAYS).copy()
    for c in ("open", "high", "low", "close", "volume"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["close"])
    if df.empty:
        return None
    close = df["close"].astype(float)
    last = float(close.iloc[-1])
    prev = float(close.iloc[-2]) if len(close) > 1 else np.nan
    chg = (last / prev - 1.0) * 100 if np.isfinite(prev) and prev != 0 else np.nan

    ma = {n: close.rolling(n).mean().iloc[-1] if len(close) >= n else np.nan for n in (20, 50, 120, 200)}
    vol20 = df["volume"].tail(20).mean()
    vol_ratio = float(df["volume"].iloc[-1] / vol20) if np.isfinite(vol20) and vol20 > 0 else np.nan
    prior_high = df["high"].shift(1).rolling(250).max().iloc[-1] if len(df) >= 251 else np.nan
    dist_high = (last / prior_high - 1.0) * 100 if np.isfinite(prior_high) and prior_high > 0 else np.nan
    dr = close.pct_change().dropna()
    volatility20 = dr.tail(20).std(ddof=0) * math.sqrt(250) * 100 if len(dr) >= 20 else np.nan

    dt_idx = pd.to_datetime(df["date"])
    ytd_part = df[dt_idx.dt.year == dt_idx.iloc[-1].year]
    ytd = (last / float(ytd_part["close"].iloc[0]) - 1.0) * 100 if len(ytd_part) else np.nan
    history = df.tail(120)
    history_dates = [pd.Timestamp(x).strftime("%Y-%m-%d") for x in history["date"]]

    row = {
        "code": code,
        "name": str(name or ""),
        "industry": industry.get(code) or None,
        "asof": pd.Timestamp(df["date"].iloc[-1]).strftime("%Y-%m-%d"),
        "sessions": int(len(df)),
        "close": finite(last, 3),
        "chg_pct": finite(chg, 2),
        "market_cap_yi": finite(mcap_map.get(code), 2),
        "volume_mn": finite(df["volume"].iloc[-1] / 1e6, 2),
        "amount_yi": finite(df["volume"].iloc[-1] * last / 1e8, 2),
        "vol_ratio20": finite(vol_ratio, 2),
        "dist_high_250": finite(dist_high, 2),
        "volatility20": finite(volatility20, 2),
        "rsi14": finite(rsi14(close), 2),
        "macd_weekly": finite(weekly_macd_hist(df), 4),
        "ma20": finite(ma[20], 3),
        "ma50": finite(ma[50], 3),
        "ma120": finite(ma[120], 3),
        "ma200": finite(ma[200], 3),
        "vs_ma50": finite((last / ma[50] - 1.0) * 100 if np.isfinite(ma[50]) and ma[50] else np.nan, 2),
        "vs_ma200": finite((last / ma[200] - 1.0) * 100 if np.isfinite(ma[200]) and ma[200] else np.nan, 2),
        "ma50_ma200": finite(ma[50] / ma[200] if np.isfinite(ma[50]) and np.isfinite(ma[200]) and ma[200] else np.nan, 4),
        "ret1w": finite(pct_return(close, 5), 2),
        "ret1m": finite(pct_return(close, 20), 2),
        "ret3m": finite(pct_return(close, 60), 2),
        "ret6m": finite(pct_return(close, 120), 2),
        "ret1y": finite(pct_return(close, 250), 2),
        "ret_ytd": finite(ytd, 2),
        "fip50": finite(fip_value(close, 50), 4),
        "fip120": finite(fip_value(close, 120), 4),
        "fip250": finite(fip_value(close, 250), 4),
        "history": {
            "dates": history_dates,
            "close": [finite(x, 3) for x in history["close"]],
            "ma20": [finite(x, 3) for x in history["close"].rolling(20).mean()],
            "ma50": [finite(x, 3) for x in history["close"].rolling(50).mean()],
        },
    }
    for w in WINDOWS:
        row[f"ret{w}"] = finite(pct_return(close, w), 4)
    return row


def compute_all(frames: dict, names: dict, industry_map, mcap_map, watch_codes):
    """对全市场 frames 计算指标 + 横截面 RPS（无未来函数）。返回 (records, asof)。"""
    rows = []
    asof_counts = {}
    total = len(frames)
    done = 0
    for code, df in frames.items():
        try:
            row = metrics_from_df(df, code, names.get(code, ""), industry_map, mcap_map, watch_codes)
        except Exception as e:
            log_err(f"指标计算失败 [{code}]: {repr(e)[:160]}")
            continue
        if row is None:
            continue
        rows.append(row)
        asof_counts[row["asof"]] = asof_counts.get(row["asof"], 0) + 1
        done += 1
        if done % 500 == 0:
            print(f"已处理 {done}/{total}")

    if not rows:
        log_err("未生成任何股票数据")
        return [], None
    frame = pd.DataFrame(rows)
    latest = max(asof_counts, key=asof_counts.get)
    frame = frame[frame["asof"] == latest].copy()
    for w in WINDOWS:
        col = f"ret{w}"
        frame[f"rps{w}"] = frame[col].rank(pct=True, method="average") * 100.0
    frame["composite_rps"] = 0.3 * frame["rps50"] + 0.3 * frame["rps120"] + 0.4 * frame["rps250"]
    frame["fip_negative_count"] = (frame[["fip50", "fip120", "fip250"]] < 0).sum(axis=1)
    frame["triple_rps90"] = (frame[["rps50", "rps120", "rps250"]] >= 90).all(axis=1)
    frame["trend_bull"] = (frame["close"] > frame["ma50"]) & (frame["ma50"] > frame["ma120"]) & (frame["ma120"] > frame["ma200"])
    frame["smooth_strength"] = (frame["composite_rps"] >= 90) & (frame["fip_negative_count"] == 3) & (frame["market_cap_yi"] >= 50)
    frame["in_watchlist"] = frame["code"].isin(watch_codes) if watch_codes else False
    for c in [f"rps{w}" for w in WINDOWS] + ["composite_rps"]:
        frame[c] = frame[c].round(2)

    records = frame.replace({np.nan: None}).to_dict(orient="records")
    records.sort(key=lambda r: (r.get("composite_rps") is not None, r.get("composite_rps") or -1), reverse=True)
    return records, latest


# ---------------- 板块（行业）RPS 聚合 ----------------
SECTOR_MIN_STOCKS = 3                              # 行业内至少多少只股票才参与排名（防单票噪声）
RET_COLS = {                                       # 板块收益窗口：股票 ret 列 -> 展示标签
    # 与个股 RPS 完全平行的窗口（用于板块 RPS 横截面排名）
    "ret50": "50D", "ret120": "120D", "ret250": "250D",
    # 展示用区间收益（市值加权板块收益，对齐美股选股盘板块面板）
    "ret1w": "1W", "ret1m": "1M", "ret3m": "3M", "ret6m": "6M", "ret_ytd": "YTD", "ret1y": "1Y",
}
SECTOR_WIN = (50, 120, 250)                         # 板块 RPS 窗口，与个股 RPS 口径一致


def compute_sector(records, asof):
    """从个股 records 聚合出板块（行业）RPS。

    两类口径并存：
      - 板块 RPS（市值加权）= 各行业「市值加权收益」在全部行业中的横截面百分位 ×100，
                            与个股 RPS 定义完全平行（=美股选股盘"市值加权板块收益"的 RPS 版）。
      - 板块 RPS（等权）  = 各行业「等权收益」的百分位，作为对照。
    同时输出行业成分股数、市值加权/等权区间收益、平均个股 RPS、强势股数、领涨股等。
    返回 (sector_records, asof)；无有效行业时返回 ([], asof)。
    """
    if not records:
        return [], asof
    df = pd.DataFrame(records)
    df = df[df["industry"].notna() & (df["industry"].astype(str).str.len() > 0)].copy()
    if df.empty:
        return [], asof
    for c in ["composite_rps", "rps50", "rps120", "rps250", "market_cap_yi"] + list(RET_COLS):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    counts = df.groupby("industry")["code"].count()
    valid_inds = counts[counts >= SECTOR_MIN_STOCKS].index
    df = df[df["industry"].isin(valid_inds)].copy()
    if df.empty:
        log_err(f"板块聚合：无行业满足 >= {SECTOR_MIN_STOCKS} 只成分股")
        return [], asof

    rows = []
    for ind, g in df.groupby("industry"):
        n = len(g)
        mc = pd.to_numeric(g["market_cap_yi"], errors="coerce").fillna(0.0)
        tot_mc = float(mc.sum())
        w = (mc / tot_mc) if tot_mc > 0 else None

        # 区间收益：市值加权 vs 等权
        ew_ret, cw_ret = {}, {}
        for k in RET_COLS:
            ew_ret[k] = float(g[k].mean(skipna=True)) if g[k].notna().any() else np.nan
            if w is not None and tot_mc > 0:
                cw_ret[k] = float((g[k].fillna(0.0) * w).sum())
            else:
                cw_ret[k] = ew_ret[k]
        # 个股 RPS 的均值（市值加权 vs 等权）：反映"板块内个股平均强度"
        ew_rps, cw_rps = {}, {}
        for s in SECTOR_WIN:
            c = f"rps{s}"
            ew_rps[s] = float(g[c].mean(skipna=True)) if g[c].notna().any() else np.nan
            if w is not None and tot_mc > 0:
                cw_rps[s] = float((g[c].fillna(0.0) * w).sum())
            else:
                cw_rps[s] = ew_rps[s]

        avg_comp = float(g["composite_rps"].mean(skipna=True)) if g["composite_rps"].notna().any() else np.nan
        n_strong = int((g["composite_rps"] >= 90).sum())
        top = g.sort_values("composite_rps", ascending=False, na_position="last").iloc[0]
        rows.append({
            "industry": ind,
            "n_stocks": int(n),
            "total_mcap_yi": tot_mc if tot_mc > 0 else None,
            "avg_mcap_yi": float(mc.mean()) if n > 0 else None,
            # 板块 RPS（市值加权）：个股 RPS 的市值加权均值
            "cw_rps50": cw_rps[50], "cw_rps120": cw_rps[120], "cw_rps250": cw_rps[250],
            "cw_composite_rps": 0.3 * cw_rps[50] + 0.3 * cw_rps[120] + 0.4 * cw_rps[250],
            # 板块 RPS（等权）：个股 RPS 的等权均值
            "ew_rps50": ew_rps[50], "ew_rps120": ew_rps[120], "ew_rps250": ew_rps[250],
            "ew_composite_rps": 0.3 * ew_rps[50] + 0.3 * ew_rps[120] + 0.4 * ew_rps[250],
            # 板块区间收益（市值加权 / 等权）
            "ret50_cw": cw_ret["ret50"], "ret120_cw": cw_ret["ret120"], "ret250_cw": cw_ret["ret250"],
            "ret1w_cw": cw_ret["ret1w"], "ret1m_cw": cw_ret["ret1m"], "ret3m_cw": cw_ret["ret3m"],
            "ret6m_cw": cw_ret["ret6m"], "ret_ytd_cw": cw_ret["ret_ytd"], "ret1y_cw": cw_ret["ret1y"],
            "ret50_ew": ew_ret["ret50"], "ret120_ew": ew_ret["ret120"], "ret250_ew": ew_ret["ret250"],
            "ret1w_ew": ew_ret["ret1w"], "ret1m_ew": ew_ret["ret1m"], "ret3m_ew": ew_ret["ret3m"],
            "ret6m_ew": ew_ret["ret6m"], "ret_ytd_ew": ew_ret["ret_ytd"], "ret1y_ew": ew_ret["ret1y"],
            "avg_composite_rps": avg_comp,
            "n_strong": n_strong,
            "top_code": str(top["code"]), "top_name": str(top["name"]),
            "top_composite_rps": None if pd.isna(top["composite_rps"]) else float(top["composite_rps"]),
        })

    sdf = pd.DataFrame(rows)
    # 板块 RPS（横截面百分位版）：以"市值加权区间收益"在行业间排名，得到真正的相对强度排名
    for k in RET_COLS:
        c = k + "_cw"
        if c in sdf.columns:
            sdf[f"rps_{k}_cw"] = sdf[c].rank(pct=True, method="average") * 100.0
    sdf["sector_composite_rps"] = (
        0.3 * sdf["rps_ret50_cw"] + 0.3 * sdf["rps_ret120_cw"] + 0.4 * sdf["rps_ret250_cw"]
    ) if {"rps_ret50_cw", "rps_ret120_cw", "rps_ret250_cw"}.issubset(sdf.columns) else np.nan

    # 保留 4 位小数（RPS 类）
    rps_cols = [c for c in sdf.columns if c.startswith(("cw_rps", "ew_rps", "rps_ret", "sector_composite_rps"))]
    rps_cols += ["cw_composite_rps", "ew_composite_rps", "avg_composite_rps", "top_composite_rps"]
    for c in rps_cols:
        if c in sdf.columns:
            sdf[c] = sdf[c].round(2)
    ret_cols = [c for c in sdf.columns if c.endswith(("_cw", "_ew")) and c.startswith("ret")]
    for c in ret_cols:
        sdf[c] = sdf[c].round(2)
    sdf = sdf.sort_values("sector_composite_rps", ascending=False, na_position="last")
    return sdf.replace({np.nan: None}).to_dict(orient="records"), asof


def write_sector(records, asof, source_desc):
    if not records:
        log_err("无板块数据可写（sector_rps.json 未生成）")
        return
    meta = {
        "asof": asof,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(records),
        "min_stocks_per_sector": SECTOR_MIN_STOCKS,
        "source": source_desc,
        "formula": {
            "板块RPS_市值加权": "各行业「市值加权区间收益」在全部行业中的横截面百分位×100；综合=0.3×RPS50+0.3×RPS120+0.4×RPS250",
            "板块RPS_等权": "各行业「等权区间收益」的百分位（对照口径）",
            "市值加权收益": "行业内成分股按总市值加权平均的区间收益率",
            "等权收益": "行业内成分股简单平均的区间收益率",
        },
    }
    SECTOR_JSON.write_text(
        json.dumps({"meta": meta, "data": records}, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    print(f"已生成 {SECTOR_JSON}（{len(records)} 个行业）")


# ---------------- 数据获取 ----------------
def fetch_universe():
    def _call():
        df = ak.stock_info_a_code_name()
        df = df.dropna(subset=["code"])
        df["code"] = df["code"].astype(str).str.zfill(6)
        return df
    df = with_retry(_call, "stock_info_a_code_name")
    if df is None or df.empty:
        log_err("获取全市场列表失败")
        return None
    return df


def fetch_daily(symbol: str):
    def _call():
        df = ak.stock_zh_a_daily(symbol=symbol, adjust="qfq")
        if df is None or df.empty:
            return None
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        for c in ("open", "high", "low", "close", "volume", "amount"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["date", "close"]).sort_values("date").drop_duplicates("date")
        return df
    return with_retry(_call, f"stock_zh_a_daily:{symbol}")


def fetch_spot():
    """全市场实时行情（新浪，一次返回）。失败返回 None。"""
    def _call():
        df = ak.stock_zh_a_spot()
        if df is None or df.empty:
            return None
        df = df.copy()
        # 新浪 spot 的"代码"带交易所前缀（sh600519/sz000001/bj920000），
        # 须提取末 6 位纯数字，才能与缓存的纯数字 code 对齐
        df["code"] = df["代码"].astype(str).str.extract(r"(\d{6})$")[0]
        df = df.dropna(subset=["code"])
        return df
    return with_retry(_call, "stock_zh_a_spot")


# ---------------- 缓存构建 ----------------
def build_cache_from_local():
    """从本机 rps_fip_package/data/kline/*.parquet 整合为 kline_cache.parquet（秒级）。"""
    if not LOCAL_KLINE_DIR.is_dir():
        log_err(f"本机 kline 目录不存在: {LOCAL_KLINE_DIR}")
        return False
    files = sorted(LOCAL_KLINE_DIR.glob("*.parquet"))
    if not files:
        log_err("本机 kline 无文件")
        return False
    parts = []
    for fp in files:
        try:
            d = pd.read_parquet(fp)
            if d is None or d.empty:
                continue
            code = fp.stem
            d = d.copy()
            d["code"] = code
            for c in ("open", "high", "low", "close", "volume"):
                if c in d.columns:
                    d[c] = pd.to_numeric(d[c], errors="coerce")
            d = d.dropna(subset=["close"])
            if d.empty:
                continue
            d["date"] = pd.to_datetime(d["date"])
            d = d.sort_values("date").tail(N_TRADING_DAYS)
            parts.append(d[["date", "open", "high", "low", "close", "volume", "code"]])
        except Exception as e:
            log_err(f"读取 {fp.name} 失败: {repr(e)[:160]}")
    if not parts:
        log_err("本机 kline 整合失败")
        return False
    cache = pd.concat(parts, ignore_index=True)
    cache.to_parquet(KLINE_CACHE, index=False)
    print(f"本地种子缓存构建完成：{cache['code'].nunique()} 只，写入 {KLINE_CACHE}")
    return True


def build_cache_from_akshare(limit=None):
    """逐只 akshare 拉取（云端冷启动 fallback，较慢）。"""
    uni = fetch_universe()
    if uni is None:
        return False
    codes = uni["code"].tolist()
    names = dict(zip(uni["code"], uni["name"].fillna("")))
    if limit:
        codes = codes[:limit]
    parts = []
    total = len(codes)
    for i, code in enumerate(codes, 1):
        df = fetch_daily(prefix(code))
        if df is None or df.empty:
            continue
        df = df.copy()
        df["code"] = code
        df = df.sort_values("date").tail(N_TRADING_DAYS)
        parts.append(df[["date", "open", "high", "low", "close", "volume", "code"]])
        if i % 500 == 0:
            print(f"已拉取 {i}/{total}")
    if not parts:
        log_err("akshare 拉取为空")
        return False
    cache = pd.concat(parts, ignore_index=True)
    cache.to_parquet(KLINE_CACHE, index=False)
    print(f"akshare 缓存构建完成：{cache['code'].nunique()} 只，写入 {KLINE_CACHE}")
    return True


def load_cache_frames() -> dict:
    """读取 kline_cache.parquet -> {code: DataFrame(按日期排序)}。"""
    if not KLINE_CACHE.exists():
        log_err(f"缓存缺失: {KLINE_CACHE}（intraday 前需先 bootstrap）")
        return {}
    cache = pd.read_parquet(KLINE_CACHE)
    cache["date"] = pd.to_datetime(cache["date"])
    frames = {}
    for code, g in cache.groupby("code"):
        frames[code] = g.sort_values("date").reset_index(drop=True)
    return frames


def trade_date_for(now_bj: datetime):
    """返回"计算日"日期：交易日 9:30 之后（含盘中与收盘后）= 当天（当日为已完成交易日，可追加）；
    交易日 9:30 前（隔夜）/ 周末 / 节假日 = None（不追加当日，仅沿用历史重算）。"""
    if now_bj.weekday() < 5 and now_bj.time() >= dtime(9, 30):
        return now_bj.date()
    return None


def apply_spot_to_frames(frames: dict, spot: pd.DataFrame, names: dict, now_bj: datetime):
    """用实时行情更新每只"当天那根"。返回 (更新后的 frames, 计算日日期)。"""
    trade_date = trade_date_for(now_bj)
    spot_map = {}
    if spot is not None and not spot.empty:
        for _, r in spot.iterrows():
            c = str(r["code"]).zfill(6)
            try:
                spot_map[c] = {
                    "open": float(r.get("今开")) if pd.notna(r.get("今开")) else np.nan,
                    "high": float(r.get("最高")) if pd.notna(r.get("最高")) else np.nan,
                    "low": float(r.get("最低")) if pd.notna(r.get("最低")) else np.nan,
                    "close": float(r.get("最新价")) if pd.notna(r.get("最新价")) else np.nan,
                    "volume": float(r.get("成交量")) if pd.notna(r.get("成交量")) else np.nan,
                    "name": str(r.get("名称", "")),
                }
            except Exception:
                continue
    # 计算日：盘中/收盘后=今天；非交易时段=最近历史日
    if trade_date is not None:
        calc_date = trade_date
    else:
        # 非交易时段（隔夜/周末/节假日）：不注入实时价，仅用历史缓存重算，
        # 避免把"当日实时价"错写进历史 bar（如周五 bar 被周一价覆盖）
        spot_map = {}
        calc_date = max((pd.Timestamp(fr["date"].iloc[-1]).date() for fr in frames.values()), default=date.today())

    updated = {}
    for code, df in frames.items():
        d = df.copy()
        last_date = pd.Timestamp(d["date"].iloc[-1]).date() if len(d) else None
        sp = spot_map.get(code)
        today_bar = None
        if sp and np.isfinite(sp["close"]):
            today_bar = {
                "date": pd.Timestamp(calc_date),
                "open": sp["open"], "high": sp["high"], "low": sp["low"],
                "close": sp["close"], "volume": sp["volume"], "code": code,
            }
            names[code] = sp.get("name") or names.get(code, "")
        if today_bar is None:
            updated[code] = d  # 无实时数据，保持历史原样
            continue
        if last_date == calc_date:
            d = d.iloc[:-1]  # 覆盖当天那根
        elif calc_date > last_date:
            pass  # 追加
        else:
            updated[code] = d  # 计算日早于历史（异常），不改
            continue
        d = pd.concat([d, pd.DataFrame([today_bar])], ignore_index=True).sort_values("date").reset_index(drop=True)
        updated[code] = d
    return updated, calc_date


def write_outputs(records, asof, source_desc, universe_count, spot_enhanced):
    if not records:
        print("[FATAL] 无数据产出", file=sys.stderr)
        sys.exit(1)
    print(f"产出 {len(records)} 只，行情截至 {asof}")
    fip_records = [{
        "code": r["code"], "name": r["name"],
        "fip50": r.get("fip50"), "fip120": r.get("fip120"), "fip250": r.get("fip250"),
        "fip_negative_count": r.get("fip_negative_count"),
    } for r in records]
    meta = {
        "asof": asof,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(records),
        "universe_count": int(universe_count),
        "source": source_desc,
        "stock_pool_note": f"全市场横截面基准为成功计算的 {len(records)} 只；in_watchlist 标记选股盘。",
        "market_cap_note": "总市值主用 data/mcap.csv 快照" + ("（盘中实时市值未增强）" if not spot_enhanced else "（已实时增强）") + "。",
        "formula": {
            "rps": "N日收益率在当前有效股票池中的横截面百分位×100",
            "composite_rps": "0.3×RPS50 + 0.3×RPS120 + 0.4×RPS250",
            "fip": "sign(N日收益) × (下跌日数−上涨日数) / N",
        },
    }
    RPS_JSON.write_text(json.dumps({"meta": meta, "data": records}, ensure_ascii=False, separators=(",", ":")),
                        encoding="utf-8")
    FIP_JSON.write_text(json.dumps(fip_records, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    META_JSON.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    # 板块（行业）RPS 聚合
    try:
        sector_records, _ = compute_sector(records, asof)
        write_sector(sector_records, asof, source_desc)
    except Exception as e:
        log_err(f"板块 RPS 计算失败（不影响个股数据）: {repr(e)[:200]}")
    if TEMPLATE.exists():
        html = TEMPLATE.read_text(encoding="utf-8")
        html = html.replace("__ASOF__", asof).replace("__GENERATED_AT__", meta["generated_at"])
        INDEX_HTML.write_text(html, encoding="utf-8")
        print(f"已生成 {INDEX_HTML}")
    else:
        log_err("index.template.html 缺失，未生成 index.html")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["bootstrap", "intraday", "close", "full", "sector-only"], default="intraday")
    ap.add_argument("--seed-local", action="store_true", help="bootstrap 时从本机 rps_fip kline 整合（秒级）")
    ap.add_argument("--limit", type=int, default=None, help="仅处理前 N 只（冒烟测试用）")
    args = ap.parse_args()

    # close 模式会写回缓存，必须基于全量 frame，禁止 --limit 截断
    if args.mode == "close":
        args.limit = None

    # sector-only：直接复用已有 data/rps.json，仅重算板块 RPS（无需联网/重算个股）
    if args.mode == "sector-only":
        if not RPS_JSON.exists():
            log_err(f"未找到 {RPS_JSON}，请先跑一次完整刷新（intraday/close/full）")
            sys.exit(1)
        try:
            payload = json.loads(RPS_JSON.read_text(encoding="utf-8"))
            records = payload["data"]
            asof = payload["meta"]["asof"]
            sector_records, _ = compute_sector(records, asof)
            write_sector(sector_records, asof, payload["meta"].get("source", "复用 rps.json"))
        except Exception as e:
            log_err(f"sector-only 失败: {repr(e)[:200]}")
            sys.exit(1)
        sys.exit(0)

    print(f"== 模式: {args.mode} ==")
    industry_map, mcap_csv, watch_codes, watch_names = load_meta()
    mcap_map = dict(mcap_csv)

    if args.mode == "bootstrap":
        ok = build_cache_from_local() if args.seed_local else build_cache_from_akshare(limit=args.limit)
        if ok and args.seed_local:
            shutil.copy2(KLINE_CACHE, KLINE_SEED)
            print(f"已复制冷启动种子 -> {KLINE_SEED}")
        sys.exit(0 if ok else 1)

    if args.mode == "full":
        uni = fetch_universe()
        if uni is None:
            sys.exit(1)
        names = dict(zip(uni["code"], uni["name"].fillna("")))
        frames = {}
        codes = uni["code"].tolist()
        if args.limit:
            codes = codes[:args.limit]
        for i, code in enumerate(codes, 1):
            df = fetch_daily(prefix(code))
            if df is None or df.empty:
                continue
            frames[code] = df.sort_values("date").reset_index(drop=True)
            if i % 500 == 0:
                print(f"已拉取 {i}/{len(codes)}")
        names.update(watch_names)
        records, asof = compute_all(frames, names, industry_map, mcap_map, watch_codes)
        write_outputs(records, asof, "akshare 新浪前复权日线（全市场逐只）", len(frames), False)
        return

    # intraday / close：加载缓存 + 实时行情
    frames = load_cache_frames()
    if not frames:
        log_err("缓存为空，请先运行 --mode bootstrap")
        sys.exit(1)
    if args.limit:
        frames = dict(list(frames.items())[:args.limit])
    names = dict(watch_names)
    # 优先用 codes.csv 名称，缺失时后面用 spot 名称补全
    for c in frames:
        names.setdefault(c, "")

    print("== 拉取全市场实时行情 ==")
    spot = fetch_spot()
    if spot is None:
        log_err("实时行情拉取失败，沿用缓存历史（不更新当天那根）")
        spot = None
    else:
        print(f"实时行情：{len(spot)} 只")

    now_bj = datetime.now(BJ)
    updated, calc_date = apply_spot_to_frames(frames, spot, names, now_bj)
    records, asof = compute_all(updated, names, industry_map, mcap_map, watch_codes)
    write_outputs(records, str(calc_date), "新浪实时行情 + 本地 300 日缓存（无未来函数）", len(updated), False)

    if args.mode == "close":
        # 收盘定稿：把当日完成的日线写回缓存
        try:
            out = []
            for code, d in updated.items():
                out.append(d[["date", "open", "high", "low", "close", "volume", "code"]])
            pd.concat(out, ignore_index=True).to_parquet(KLINE_CACHE, index=False)
            print(f"收盘定稿：已写回缓存 {KLINE_CACHE}（{len(out)} 只）")
        except Exception as e:
            log_err(f"写回缓存失败: {repr(e)[:200]}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log_err("未捕获异常:\n" + traceback.format_exc())
        raise
