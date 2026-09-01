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
import os
import re
import shutil
import sys
import time
import traceback
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
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
HISTORY_JSON = DATA_DIR / "history.json"                     # 个股 K 线（OHLC+MA），按需懒加载，不进 rps.json（rps.json 因此从 ~33MB 降到 ~4MB）
FIP_JSON = DATA_DIR / "fip.json"
META_JSON = DATA_DIR / "meta.json"
SECTOR_JSON = DATA_DIR / "sector_rps.json"
SECTOR_HISTORY_JSON = DATA_DIR / "sector_rps_history.json"   # 板块 RPS 逐日快照（供历史排名走势图）
RUN_LOG_JSON = DATA_DIR / "cloud_run_log.json"               # 运行诊断（云端可观测：行情源/行数/asof/守卫）
MAX_HISTORY_WEEKS = 260                                     # 最多保留约 5 年周快照（周频）
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
    ts = datetime.now(BJ).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}\n"
    try:
        with ERROR_LOG.open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass
    print("ERROR:", msg, file=sys.stderr)


# 实时行情诊断：记录每个数据源各取到多少只、最终用了哪个源
SPOT_DIAG = {"attempts": [], "source": None, "rows": 0}
# 实际行情时间（新浪/akshare 返回的行情时刻，如 "2026-09-01 10:24:56"），非流水线跑批时间
LAST_QUOTE_TIME = {"ts": None}


def write_run_log(**kw):
    """把本次运行的关键诊断信息写入 data/cloud_run_log.json。

    无条件覆盖写入（即使实时行情失败/守卫跳过），便于在无法查看
    GitHub Actions 日志时，直接公网读取判断云端到底发生了什么。
    """
    try:
        rec = {
            "ts": datetime.now(BJ).strftime("%Y-%m-%d %H:%M:%S"),
            "host": "cloud" if os.environ.get("GITHUB_ACTIONS") else "local",
            "spot_attempts": list(SPOT_DIAG["attempts"]),
            "spot_source": SPOT_DIAG["source"],
            "spot_rows": SPOT_DIAG["rows"],
        }
        rec.update(kw)
        RUN_LOG_JSON.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[runlog] 已写 {RUN_LOG_JSON.name}: {rec}")
    except Exception as e:
        log_err(f"写 {RUN_LOG_JSON.name} 失败: {repr(e)[:160]}")


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


def is_risk_warning_name(name: str) -> bool:
    """识别当前证券简称中的风险警示/退市整理标记。

    口径：ST、*ST、S*ST，以及简称末尾「退」（退市整理期）。
    名称来自当日实时行情，避免静态 codes.csv 名称滞后导致戴帽股票混入。
    """
    s = re.sub(r"\s+", "", str(name or "")).upper()
    return bool(re.match(r"^(?:\*ST|ST|S\*ST|SST)", s)) or s.endswith("退")


def filter_risk_warning_frames(frames: dict, names: dict):
    """在横截面计算前剔除风险警示股票，返回 (过滤后 frames, 被剔除清单)。"""
    excluded = [
        (code, str(names.get(code, "")))
        for code in frames
        if is_risk_warning_name(names.get(code, ""))
    ]
    if not excluded:
        return frames, []
    excluded_codes = {code for code, _ in excluded}
    clean = {code: df for code, df in frames.items() if code not in excluded_codes}
    sample = "、".join(f"{c} {n}" for c, n in excluded[:8])
    more = f" 等（另 {len(excluded)-8} 只）" if len(excluded) > 8 else ""
    print(f"股票池风险过滤：剔除 ST/*ST/退市整理 {len(excluded)} 只：{sample}{more}")
    return clean, excluded


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
    chg = df.attrs.get("spot_pct_change")
    if chg is None or not np.isfinite(chg):
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
            "open": [finite(x, 3) for x in history["open"]],
            "high": [finite(x, 3) for x in history["high"]],
            "low": [finite(x, 3) for x in history["low"]],
            "close": [finite(x, 3) for x in history["close"]],
            "ma20": [finite(x, 3) for x in history["close"].rolling(20).mean()],
            "ma50": [finite(x, 3) for x in history["close"].rolling(50).mean()],
        },
    }
    for w in WINDOWS:
        row[f"ret{w}"] = finite(pct_return(close, w), 4)
    return row


def compute_all(frames: dict, names: dict, industry_map, mcap_map, watch_codes):
    """对有效股票池计算指标 + 横截面 RPS（无未来函数）。返回 (records, asof)。

    股票池严格以 data/codes.csv（watch_codes）为准；同时按当前证券简称动态剔除
    ST、*ST、S*ST 与退市整理期，避免静态名单更新滞后。
    """
    if watch_codes is not None:
        frames = {code: df for code, df in frames.items() if code in watch_codes}
    rows = []
    asof_counts = {}
    total = len(frames)
    done = 0
    for code, df in frames.items():
        name = names.get(code, "")
        if is_risk_warning_name(name):
            continue
        try:
            row = metrics_from_df(df, code, name, industry_map, mcap_map, watch_codes)
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
SECTOR_RET_COLS = {                                # 板块区间收益窗口（与个股区间收益口径一致）
    "ret1w": "1W", "ret1m": "1M", "ret3m": "3M", "ret6m": "6M",
}


# ---------------- 板块周频历史 ----------------
_TRADING_DATES_CACHE = None

def get_trading_dates():
    """返回本地 K 线缓存中的全部交易日（去重、升序），用于确定周频记录点。"""
    global _TRADING_DATES_CACHE
    if _TRADING_DATES_CACHE is None:
        try:
            kp = pd.read_parquet(KLINE_CACHE, columns=["date"])
            _TRADING_DATES_CACHE = sorted(
                pd.to_datetime(kp["date"]).dt.normalize().unique().tolist()
            )
        except Exception as e:
            log_err(f"读取交易日历失败: {repr(e)[:160]}")
            _TRADING_DATES_CACHE = []
    return _TRADING_DATES_CACHE

def week_end_trading_date(d):
    """返回包含 d 的那一周（周一~周日）内的最后一个交易日（周频记录点）。
    周五为交易日则返回周五；周五休假则返回周四等更早的交易日。"""
    d = pd.Timestamp(d).normalize()
    monday = d - pd.Timedelta(days=d.weekday())
    sunday = monday + pd.Timedelta(days=6)
    tds = get_trading_dates()
    if not tds:
        return d
    week_tds = [t for t in tds if monday <= t <= sunday]
    return max(week_tds) if week_tds else d


def compute_sector(records, asof):
    """从个股 records 聚合出板块（行业）的区间涨幅排名。

    直接用「市值加权区间收益」在全部行业中的名次（涨幅最高 = 第 1 名，向下递增），
    不再使用 RPS 百分位。窗口：1周 / 1月 / 3月 / 6月；每个窗口同时给出排名与该窗口的
    市值加权区间涨跌幅。另含成分股数、强势股数、领涨股等。
    返回 (sector_records, asof)；无有效行业时返回 ([], asof)。
    """
    if not records:
        return [], asof
    df = pd.DataFrame(records)
    df = df[df["industry"].notna() & (df["industry"].astype(str).str.len() > 0)].copy()
    if df.empty:
        return [], asof
    for c in ["composite_rps", "market_cap_yi"] + list(SECTOR_RET_COLS):
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
        # 市值加权区间收益（已剔除等权口径）
        cw_ret = {}
        for k in SECTOR_RET_COLS:
            cw_ret[k] = float((g[k].fillna(0.0) * w).sum()) if (w is not None and tot_mc > 0) else (float(g[k].mean(skipna=True)) if g[k].notna().any() else np.nan)
        n_strong = int((g["composite_rps"] >= 90).sum())
        top = g.sort_values("composite_rps", ascending=False, na_position="last").iloc[0]
        rows.append({
            "industry": ind,
            "n_stocks": int(n),
            "ret1w_cw": cw_ret["ret1w"], "ret1m_cw": cw_ret["ret1m"],
            "ret3m_cw": cw_ret["ret3m"], "ret6m_cw": cw_ret["ret6m"],
            "n_strong": n_strong,
            "top_code": str(top["code"]), "top_name": str(top["name"]),
            "top_composite_rps": None if pd.isna(top["composite_rps"]) else float(top["composite_rps"]),
        })

    sdf = pd.DataFrame(rows)
    # 排名：各行业「市值加权区间收益」降序名次（涨幅最高 = 第 1 名）
    # 强度：RPS 式百分位（涨幅最高 ≈ 100，与个股 RPS 口径一致）
    for k in SECTOR_RET_COLS:
        c = k + "_cw"                       # k 已是 ret1w/ret1m/ret3m/ret6m
        rank_col = "rank" + k[3:]
        str_col = "str" + k[3:]
        sdf[rank_col] = sdf[c].rank(ascending=False, method="first").astype(int)
        sdf[str_col] = (sdf[c].rank(pct=True, method="average") * 100.0).round(1)
    for c in ["ret1w_cw", "ret1m_cw", "ret3m_cw", "ret6m_cw"]:
        if c in sdf.columns:
            sdf[c] = sdf[c].round(2)
    sdf = sdf.sort_values("rank6m", ascending=True, na_position="last")
    return sdf.replace({np.nan: None}).to_dict(orient="records"), asof


def _lookup_hist_rank1m(hist, industry, asof, weeks_ago):
    """从历史周频快照找 industry 的 1M 排名，取 asof 所在周之前第 weeks_ago 个周的快照。"""
    if not hist or not asof or not industry:
        return None
    try:
        asof_ts = pd.Timestamp(asof).normalize()
    except Exception:
        return None
    valid = sorted(d for d in hist.keys() if pd.Timestamp(d).normalize() <= asof_ts)
    if not valid:
        return None
    idx = len(valid) - 1 - weeks_ago
    if idx < 0:
        return None
    rec = hist[valid[idx]].get(industry)
    if not rec:
        return None
    return rec.get("rank1m")


def write_sector(records, asof, source_desc):
    if not records:
        log_err("无板块数据可写（sector_rps.json 未生成）")
        return
    # 读取历史快照，附加 1M 排名的历史值（1周前 / 3月前 / 6月前）
    hist = {}
    if SECTOR_HISTORY_JSON.exists():
        try:
            hist = json.loads(SECTOR_HISTORY_JSON.read_text(encoding="utf-8"))
        except Exception as e:
            log_err(f"读取 {SECTOR_HISTORY_JSON.name} 失败，历史排名置空: {repr(e)[:160]}")
            hist = {}
    for r in records:
        ind = r.get("industry")
        r["rank1m_1w_ago"] = _lookup_hist_rank1m(hist, ind, asof, 1)
        r["rank1m_3m_ago"] = _lookup_hist_rank1m(hist, ind, asof, 13)
        r["rank1m_6m_ago"] = _lookup_hist_rank1m(hist, ind, asof, 26)

    meta = {
        "asof": asof,
        "generated_at": datetime.now(BJ).strftime("%Y-%m-%d %H:%M:%S"),
        "quote_time": LAST_QUOTE_TIME["ts"],
        "count": len(records),
        "min_stocks_per_sector": SECTOR_MIN_STOCKS,
        "source": source_desc,
        "formula": {
            "排名口径": "各行业「市值加权区间收益」在全部行业中的降序名次：涨幅最高=第1名，依次递增",
            "强度口径": "各行业「市值加权区间收益」的横截面百分位×100，与个股RPS一致：涨幅最高≈100",
            "区间窗口": "1周 / 1月 / 3月 / 6月（与个股区间收益口径一致）",
            "市值加权收益": "行业内成分股按总市值加权平均的区间收益率（已剔除等权口径）",
            "历史1M排名": "取历史周频快照（每周最后一个交易日）中，asof 所在周之前第 1/13/26 个周的 1M 排名（约 上周 / 3月前 / 6月前）",
        },
    }
    SECTOR_JSON.write_text(
        json.dumps({"meta": meta, "data": records}, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    # 历史快照（按日累积，供板块历史排名走势图使用；同 asof 覆盖，每天 1 个点）
    try:
        write_sector_history(records, asof, existing_hist=hist)
    except Exception as e:
        log_err(f"板块历史快照写入失败（不影响板块RPS）: {repr(e)[:200]}")
    print(f"已生成 {SECTOR_JSON}（{len(records)} 个行业）")


def write_sector_history(records, asof, existing_hist=None):
    """把板块 1M 排名的「周频」快照追加进历史文件。

    - 仅在 asof 为该周最后一个交易日（周频记录点）时才写入，避免重复；
      因此每个自然周最终只保留 1 个最新点（收盘定稿覆盖）。
    - 每个周点记录每个行业的：1M 排名 / 1M 强度 / 1M 市值加权涨幅 / 成分股数。
    - 仅保留最近 MAX_HISTORY_WEEKS 个周，避免无限膨胀。
    """
    if not records:
        return
    asof_ts = pd.Timestamp(asof).normalize()
    wk = week_end_trading_date(asof)
    if asof_ts != wk:
        return  # 非周频记录点（周内普通交易日），跳过
    wk_str = wk.strftime("%Y-%m-%d")
    today = {}
    for r in records:
        ind = r.get("industry")
        if not ind:
            continue
        today[ind] = {
            "rank1m": r.get("rank1m"),
            "str1m": r.get("str1m"),
            "ret1m_cw": r.get("ret1m_cw"),
            "n_stocks": r.get("n_stocks"),
        }
    if existing_hist is not None:
        hist = existing_hist
    else:
        hist = {}
        if SECTOR_HISTORY_JSON.exists():
            try:
                hist = json.loads(SECTOR_HISTORY_JSON.read_text(encoding="utf-8"))
            except Exception as e:
                log_err(f"读取 {SECTOR_HISTORY_JSON.name} 失败，重建: {repr(e)[:160]}")
                hist = {}
    hist[wk_str] = today
    keys = sorted(hist.keys())
    if len(keys) > MAX_HISTORY_WEEKS:
        for k in keys[:-MAX_HISTORY_WEEKS]:
            hist.pop(k, None)
    SECTOR_HISTORY_JSON.write_text(
        json.dumps(hist, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    print(f"已写入板块周频历史快照 {SECTOR_HISTORY_JSON.name}（周 {wk_str}，共 {len(hist)} 周）")


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


def _fetch_spot_sina_raw(codes):
    """直连新浪 hq.sinajs.cn 批量行情（按交易所前缀分块）。

    返回与 akshare.stock_zh_a_spot 同 schema 的 DataFrame
    （列：代码/名称/最新价/今开/最高/最低/成交量）或 None。
    不依赖 akshare，单请求批量、海外通常可达，是云端主数据源。
    """
    if not codes:
        return None
    chunks = [codes[i:i + 400] for i in range(0, len(codes), 400)]
    rows = []
    qts_seen = []
    for ch in chunks:
        syms = ",".join(prefix(c) for c in ch)
        url = "https://hq.sinajs.cn/list=" + syms
        try:
            req = urllib.request.Request(url, headers={
                "Referer": "https://finance.sina.com.cn",
                "User-Agent": "Mozilla/5.0",
            })
            raw = urllib.request.urlopen(req, timeout=15).read().decode("gbk", "ignore")
        except Exception as e:
            log_err(f"Sina 原始接口分块失败: {repr(e)[:160]}")
            continue
        for line in raw.splitlines():
            line = line.strip()
            if not line.startswith("var hq_str_"):
                continue
            m = re.match(r'var hq_str_(\w+?)="(.*)";?\s*$', line)
            if not m:
                continue
            sym = m.group(1)                      # e.g. sh600000
            cm = re.search(r"(\d{6})$", sym)
            if not cm:
                continue
            parts = m.group(2).split(",")
            if len(parts) < 10:
                continue
            try:
                name = parts[0]
                openp = float(parts[1])
                prev_close = float(parts[2])       # 昨收
                closep = float(parts[3])          # 现价
                high = float(parts[4])
                low = float(parts[5])
                vol = float(parts[8])             # 成交量（手）
            except Exception:
                continue
            if not np.isfinite(closep) or closep <= 0:
                continue
            pct_change = ((closep / prev_close - 1.0) * 100.0) if (np.isfinite(prev_close) and prev_close > 0) else np.nan
            rows.append({
                "code": cm.group(1), "代码": sym, "名称": name, "最新价": closep,
                "今开": openp, "最高": high, "最低": low, "成交量": vol,
                "涨跌幅": pct_change,
            })
            if len(parts) > 31:                       # [30]=日期, [31]=时间 -> 实际行情时刻
                d, t = parts[30].strip(), parts[31].strip()
                if re.match(r"\d{4}-\d{2}-\d{2}$", d) and re.match(r"\d{2}:\d{2}:\d{2}$", t):
                    qts_seen.append(f"{d} {t}")
    if not rows:
        return None
    if qts_seen:                                      # 取全市场最新的一条行情时间
        LAST_QUOTE_TIME["ts"] = max(qts_seen)
    return pd.DataFrame(rows)


def fetch_hist_range(code: str, start_date: str, end_date: str):
    """拉取指定日期区间的前复权日线（stock_zh_a_hist，只返回区间内数据，轻量）。

    与缓存口径对齐要点：
      - 该接口「成交量」单位为「手」，缓存为「股」，故 volume × 100；
      - OHLC 为前复权(qfq)，与缓存(新浪前复权)在重叠日期上一致。
    返回 DataFrame(date,open,high,low,close,volume,code) 或 None。
    """
    df = None
    for i in range(3):                       # 接口在高并发下会 RemoteDisconnected，退避重试
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily",
                                    start_date=start_date, end_date=end_date, adjust="qfq")
            break
        except Exception as e:
            if i == 2:
                log_err(f"stock_zh_a_hist 失败 [{code}]: {repr(e)[:120]}")
            time.sleep(0.5 * (i + 1))
    if df is None or df.empty:
        return None
    df = df.rename(columns={"日期": "date", "开盘": "open", "最高": "high",
                            "最低": "low", "收盘": "close", "成交量": "volume"}).copy()
    df["date"] = pd.to_datetime(df["date"])
    for c in ("open", "high", "low", "close", "volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["volume"] = df["volume"] * 100.0          # 手 -> 股，与缓存一致
    df["code"] = str(code).zfill(6)
    df = df.dropna(subset=["date", "close"])
    return df[["date", "open", "high", "low", "close", "volume", "code"]]


def backfill_kline(days: int = 5, workers: int = 16):
    """回补近 N 个交易日的日线到 kline_cache.parquet，并同步 kline_cache_seed.parquet。

    只补到「昨天」：今天那根仍由实时行情(spot)提供，避免盘中的半根日线污染历史。
    新数据按 (code, date) 覆盖旧行，去重后每只保留最近 N_TRADING_DAYS 根。
    """
    frames = load_cache_frames()
    if not frames:
        log_err("backfill: 缓存为空，请先 bootstrap")
        return False
    codes = sorted(frames.keys())
    today = datetime.now(BJ).date()
    end_d = today - pd.Timedelta(days=1)                       # 只补到昨天
    start_d = today - pd.Timedelta(days=int(days) * 2 + 2)     # 日历日回溯，确保覆盖 N 个交易日
    s, e = start_d.strftime("%Y%m%d"), end_d.strftime("%Y%m%d")
    print(f"== 回补日线 {s} ~ {e}（{len(codes)} 只，并发 {workers}）==")

    def _fetch_many(code_list, w):
        got, bad = [], []
        with ThreadPoolExecutor(max_workers=w) as ex:
            futs = {ex.submit(fetch_hist_range, c, s, e): c for c in code_list}
            n = 0
            for fu in as_completed(futs):
                c = futs[fu]
                try:
                    d = fu.result()
                except Exception:
                    d = None
                n += 1
                if d is None or d.empty:
                    bad.append(c)
                else:
                    got.append(d)
                if n % 1000 == 0:
                    print(f"  进度 {n}/{len(code_list)}（失败 {len(bad)}）")
        return got, bad

    parts, fails = _fetch_many(codes, workers)
    if fails:                                 # 首轮失败的降并发再试一轮，尽量收敛
        print(f"== 首轮失败 {len(fails)} 只，降并发重试 ==")
        time.sleep(2)
        got2, fails = _fetch_many(fails, max(2, workers // 3))
        parts += got2
    if not parts:
        log_err("backfill: 未取到任何日线")
        return False

    newdf = pd.concat(parts, ignore_index=True)
    print(f"取回日线：{len(newdf)} 行 / {newdf['code'].nunique()} 只，"
          f"日期 {newdf['date'].min().date()} ~ {newdf['date'].max().date()}，仍失败 {len(fails)} 只")

    cache = pd.read_parquet(KLINE_CACHE)
    cache["date"] = pd.to_datetime(cache["date"])
    new_keys = set(newdf["code"].astype(str) + "|" + newdf["date"].dt.strftime("%Y-%m-%d"))
    old_keys = cache["code"].astype(str) + "|" + cache["date"].dt.strftime("%Y-%m-%d")
    cache = cache[~old_keys.isin(new_keys)]                    # 新数据覆盖同 (code,date) 旧行
    merged = pd.concat([cache, newdf], ignore_index=True)
    merged = merged.drop_duplicates(subset=["code", "date"], keep="last")
    merged = merged.sort_values(["code", "date"]).reset_index(drop=True)
    merged = merged.groupby("code", group_keys=False).tail(N_TRADING_DAYS)
    merged.to_parquet(KLINE_CACHE, index=False)
    print(f"合并后缓存：{len(merged)} 行 / {merged['code'].nunique()} 只，最后日期 {merged['date'].max().date()}")
    shutil.copy2(KLINE_CACHE, KLINE_SEED)
    print(f"已同步种子 -> {KLINE_SEED}")
    return True


def fetch_spot(codes=None):
    """全市场实时行情，多源兜底：新浪原始接口(主) -> akshare(末)。

    返回 DataFrame（列：代码/名称/最新价/今开/最高/最低/成交量）或 None。
    显式打印每个数据源的行数，便于在 GitHub Actions 日志里确认是否取到实时行情。
    """
    # 1) 新浪原始接口（主源：单请求批量、不依赖 akshare、海外通常可达）
    if codes:
        df = _fetch_spot_sina_raw(codes)
        n = len(df) if df is not None else 0
        SPOT_DIAG["attempts"].append({"src": "sina_raw", "rows": n})
        print(f"[spot] 新浪原始接口：{n} 只")
        if df is not None and not df.empty:
            SPOT_DIAG["source"], SPOT_DIAG["rows"] = "sina_raw", n
            return df
    else:
        SPOT_DIAG["attempts"].append({"src": "sina_raw", "rows": 0, "note": "无代码列表，跳过"})
        print("[spot] 未提供代码列表，跳过新浪原始接口")

    # 2) akshare stock_zh_a_spot（兜底）
    def _call():
        d = ak.stock_zh_a_spot()
        if d is None or d.empty:
            return None
        d = d.copy()
        # 新浪 spot 的"代码"带交易所前缀（sh600519/sz000001/bj920000），
        # 须提取末 6 位纯数字，才能与缓存的纯数字 code 对齐
        d["code"] = d["代码"].astype(str).str.extract(r"(\d{6})$")[0]
        d = d.dropna(subset=["code"])
        return d
    df = with_retry(_call, "stock_zh_a_spot")
    n = len(df) if df is not None else 0
    SPOT_DIAG["attempts"].append({"src": "akshare", "rows": n})
    print(f"[spot] akshare：{n} 只")
    if df is not None and not df.empty:
        SPOT_DIAG["source"], SPOT_DIAG["rows"] = "akshare", n
        # 补齐/统一 日内实时涨跌幅
        if "涨跌幅" not in df.columns or df["涨跌幅"].notna().sum() == 0:
            price_col = "最新价" if "最新价" in df.columns else None
            prev_cols = [c for c in ("昨收", "昨日收盘", "昨收价") if c in df.columns]
            if price_col and prev_cols:
                df["涨跌幅"] = (df[price_col] / df[prev_cols[0]] - 1.0) * 100.0
        if "时间戳" in df.columns:                    # 形如 "09:35:43"，补上当天日期
            try:
                t = str(df["时间戳"].dropna().max()).strip()
                if re.match(r"\d{2}:\d{2}:\d{2}$", t):
                    LAST_QUOTE_TIME["ts"] = f"{datetime.now(BJ).date()} {t}"
            except Exception:
                pass
        return df
    log_err("实时行情全部数据源失败，沿用缓存历史（不更新当天那根）")
    return None


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
                    "pct_change": float(r.get("涨跌幅")) if pd.notna(r.get("涨跌幅")) else np.nan,
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
        # 日内实时涨跌幅（最新价/昨收）优先来自行情接口，比 close[-1]/close[-2] 更实时
        pc = sp.get("pct_change")
        if pc is not None and np.isfinite(pc):
            d.attrs["spot_pct_change"] = float(pc)
        updated[code] = d
    return updated, calc_date


def write_outputs(records, asof, source_desc, universe_count, spot_enhanced):
    if not records:
        print("[FATAL] 无数据产出", file=sys.stderr)
        sys.exit(1)
    print(f"产出 {len(records)} 只，行情截至 {asof}")
    # 把 K 线历史从主记录中拆出，单独写 history.json（懒加载），大幅缩减 rps.json 首屏体积
    hist_map = {}
    for r in records:
        if "history" in r:
            hist_map[r["code"]] = r.pop("history")
    if hist_map:
        HISTORY_JSON.write_text(json.dumps(hist_map, ensure_ascii=False, separators=(",", ":")),
                                encoding="utf-8")
        print(f"已生成 {HISTORY_JSON}（{len(hist_map)} 只 K 线历史，懒加载）")
    fip_records = [{
        "code": r["code"], "name": r["name"],
        "fip50": r.get("fip50"), "fip120": r.get("fip120"), "fip250": r.get("fip250"),
        "fip_negative_count": r.get("fip_negative_count"),
    } for r in records]
    meta = {
        "asof": asof,
        "generated_at": datetime.now(BJ).strftime("%Y-%m-%d %H:%M:%S"),
        "quote_time": LAST_QUOTE_TIME["ts"],
        "count": len(records),
        "universe_count": int(universe_count),
        "source": source_desc,
        "stock_pool_note": f"股票池严格以 data/codes.csv 为准，有效横截面为成功计算的 {len(records)} 只；同时按当日实时证券简称动态剔除 ST、*ST、S*ST 与退市整理期股票。",
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
        html = (html.replace("__ASOF__", asof)
                    .replace("__GENERATED_AT__", meta["generated_at"])
                    .replace("__QUOTE_TIME__", meta.get("quote_time") or "—"))
        INDEX_HTML.write_text(html, encoding="utf-8")
        print(f"已生成 {INDEX_HTML}")
    else:
        log_err("index.template.html 缺失，未生成 index.html")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["bootstrap", "intraday", "close", "full", "sector-only", "backfill"],
                    default="intraday")
    ap.add_argument("--seed-local", action="store_true", help="bootstrap 时从本机 rps_fip kline 整合（秒级）")
    ap.add_argument("--limit", type=int, default=None, help="仅处理前 N 只（冒烟测试用）")
    ap.add_argument("--days", type=int, default=5, help="backfill：回补最近 N 个交易日的日线")
    ap.add_argument("--workers", type=int, default=8, help="backfill：并发线程数（过高会被接口掐连接）")
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
            if TEMPLATE.exists():
                html = TEMPLATE.read_text(encoding="utf-8")
                html = (html.replace("__ASOF__", asof)
                            .replace("__GENERATED_AT__", datetime.now(BJ).strftime("%Y-%m-%d %H:%M:%S"))
                            .replace("__QUOTE_TIME__", payload["meta"].get("quote_time") or "—"))
                INDEX_HTML.write_text(html, encoding="utf-8")
                print(f"已生成 {INDEX_HTML}")
        except Exception as e:
            log_err(f"sector-only 失败: {repr(e)[:200]}")
            sys.exit(1)
        sys.exit(0)

    print(f"== 模式: {args.mode} ==")
    industry_map, mcap_csv, watch_codes, watch_names = load_meta()
    mcap_map = dict(mcap_csv)

    # backfill：回补近 N 日日线（用东财/akshare 区间接口，本地网络跑；只补到昨天，今天那根由 spot 提供）
    if args.mode == "backfill":
        sys.exit(0 if backfill_kline(days=args.days, workers=args.workers) else 1)

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
    spot = fetch_spot(list(frames.keys()))
    if spot is None:
        log_err("实时行情拉取失败，沿用缓存历史（不更新当天那根）")
        spot = None
    else:
        print(f"实时行情：{len(spot)} 只")
    write_run_log(mode=args.mode, stage="spot_done", n_codes=len(frames))

    now_bj = datetime.now(BJ)
    updated, calc_date = apply_spot_to_frames(frames, spot, names, now_bj)
    # 必须在 RPS 横截面与板块聚合之前剔除风险警示股；名称以当日实时行情为准。
    updated, risk_excluded = filter_risk_warning_frames(updated, names)
    if watch_codes is not None and risk_excluded:
        watch_codes = watch_codes - {code for code, _ in risk_excluded}
    records, asof = compute_all(updated, names, industry_map, mcap_map, watch_codes)

    # 防回退守卫：若实时行情拉取失败导致 asof 比已发布数据更旧，则跳过写盘，
    # 避免云端把已正确的当日数据回退到缓存里的旧交易日（如 08-28）。
    if RPS_JSON.exists():
        try:
            prev = json.loads(RPS_JSON.read_text(encoding="utf-8")).get("meta", {}).get("asof")
            if prev and asof < prev:
                log_err(f"计算 asof={asof} 早于已发布 asof={prev}，疑似实时行情缺失，跳过写盘以免回退")
                print(f"[guard] 跳过写盘：asof {asof} < 已发布 {prev}")
                write_run_log(mode=args.mode, stage="guard_skip",
                              asof=asof, published_asof=prev, guard_skipped=True,
                              note="实时行情缺失，跳过写盘以防回退")
                return
        except Exception:
            pass

    write_outputs(records, str(calc_date), "新浪实时行情 + 本地 300 日缓存（无未来函数）", len(updated), False)
    write_run_log(mode=args.mode, stage="written", asof=str(calc_date),
                  guard_skipped=False, n_records=len(records))

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
