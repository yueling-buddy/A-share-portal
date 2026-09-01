#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""基于本地 K 线缓存回溯补全板块历史排名数据（周频）。

用法:
    python scripts/backfill_sector_history.py [--start 2025-04-02] [--end 2026-08-31]

说明:
    - 直接读 data/kline_cache.parquet + data/industry.csv + data/mcap.csv + data/codes.csv
    - 按周聚合：每个自然周仅取其最后一个交易日截面，计算各行业「市值加权 1月收益」的
      名次（涨幅最高=第1名）与 RPS 式强度（横截面百分位×100）
    - 每条周快照只记录各板块的 1M 排名 / 1M 强度 / 1M 涨幅 / 成分股数
    - 结果合并进 data/sector_rps_history.json（按周频键覆盖；保留 end 之后的既有周）
    - 仅补齐历史周；本周及之后仍由 refresh.py 在每周收盘定稿时实时产出
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
KLINE_CACHE = DATA_DIR / "kline_cache.parquet"
INDUSTRY_CSV = DATA_DIR / "industry.csv"
MCAP_CSV = DATA_DIR / "mcap.csv"
CODES_CSV = DATA_DIR / "codes.csv"
SECTOR_HISTORY_JSON = DATA_DIR / "sector_rps_history.json"
SECTOR_MIN_STOCKS = 3


def load_data():
    print(f"读取 K 线缓存 {KLINE_CACHE.name} ...")
    kline = pd.read_parquet(KLINE_CACHE)
    print(f"K 线缓存: {kline.shape[0]} 行, 日期 {kline['date'].min().date()} ~ {kline['date'].max().date()}")

    industry = pd.read_csv(INDUSTRY_CSV)
    mcap = pd.read_csv(MCAP_CSV)
    codes = pd.read_csv(CODES_CSV)

    # code 标准化为 6 位字符串
    for df in [kline, industry, mcap, codes]:
        df["code"] = df["code"].astype(str).str.zfill(6)

    universe = set(codes["code"])
    kline = kline[kline["code"].isin(universe)].copy()
    print(f"股票池: {len(universe)} 只, K 线缓存覆盖 {kline['code'].nunique()} 只")

    # 合并行业和市值
    kline = kline.merge(industry[["code", "industry"]], on="code", how="left")
    kline = kline.merge(mcap[["code", "mcap"]], on="code", how="left")
    kline["mcap"] = pd.to_numeric(kline["mcap"], errors="coerce").fillna(0.0)
    kline["close"] = pd.to_numeric(kline["close"], errors="coerce")
    kline = kline[kline["close"].notna() & kline["industry"].notna()].copy()
    return kline


def compute_sector_for_date(group: pd.DataFrame) -> pd.DataFrame:
    """对单个交易日的股票截面，按行业聚合 1月收益并排名（含 RPS 式强度）。"""
    def _industry_agg(g):
        tot = g["mcap"].sum()
        w = g["mcap"] / tot if tot > 0 else pd.Series([1.0 / len(g)] * len(g), index=g.index)
        return pd.Series({
            "ret1m_cw": (g["ret1m"] * w).sum() if g["ret1m"].notna().any() else np.nan,
            "n_stocks": len(g),
        })

    counts = group.groupby("industry")["code"].count()
    valid = counts[counts >= SECTOR_MIN_STOCKS].index
    group = group[group["industry"].isin(valid)].copy()
    if group.empty:
        return pd.DataFrame()

    ind = group.groupby("industry").apply(_industry_agg, include_groups=False).reset_index()
    ind["ret1m_cw"] = pd.to_numeric(ind["ret1m_cw"], errors="coerce")
    # 涨幅最高 = 第 1 名
    ind["rank1m"] = ind["ret1m_cw"].rank(ascending=False, method="first").astype("Int64")
    # RPS 式强度：横截面百分位 × 100
    ind["str1m"] = (ind["ret1m_cw"].rank(pct=True, method="average") * 100.0).round(1)
    ind["ret1m_cw"] = ind["ret1m_cw"].round(2)
    return ind


def week_end_trading_date(tds, d):
    """返回包含 d 的那一周（周一~周日）内的最后一个交易日。"""
    d = pd.Timestamp(d).normalize()
    monday = d - pd.Timedelta(days=d.weekday())
    sunday = monday + pd.Timedelta(days=6)
    week_tds = [t for t in tds if monday <= t <= sunday]
    return max(week_tds) if week_tds else d


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2025-04-02", help="起始日期 (YYYY-MM-DD)")
    parser.add_argument("--end", default="2026-08-31", help="结束日期 (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="只计算不写入")
    args = parser.parse_args()

    kline = load_data()

    # 计算每只股票在每个交易日的 1月 区间收益（当前 close vs 20 个交易日前 close）
    print("计算个股 1月 区间收益 ...")
    kline = kline.sort_values(["code", "date"])
    kline["ret1m"] = (
        kline.groupby("code")["close"]
        .pct_change(periods=20, fill_method=None) * 100.0
    )

    start = pd.Timestamp(args.start)
    end = pd.Timestamp(args.end)
    kline = kline[(kline["date"] >= start) & (kline["date"] <= end)].copy()
    dates = sorted(pd.to_datetime(kline["date"]).dt.normalize().unique().tolist())
    print(f"待回溯交易日: {len(dates)} 个 ({dates[0].date()} ~ {dates[-1].date()})")

    # 每个交易日 -> 其所在周的最后一个交易日（周频记录点）
    date2wk = {d: week_end_trading_date(dates, d) for d in dates}
    # 每个周频记录点 -> 该周代表性交易日（= 该周最大交易日）
    wk_rep = {}
    for d in dates:
        wk = date2wk[d]
        if wk not in wk_rep or d > wk_rep[wk]:
            wk_rep[wk] = d
    weeks = sorted(wk_rep.keys())
    print(f"待回溯周数: {len(weeks)} 周 ({weeks[0].date()} ~ {weeks[-1].date()})")

    # 读取已有历史：仅保留 end 之后、且已是新周频结构（含 str1m）的未来周，
    # 避免覆盖云端已写入的更新周；区间内的旧「逐日」条目一律丢弃后由本次重写。
    hist = {}
    if SECTOR_HISTORY_JSON.exists():
        try:
            old = json.loads(SECTOR_HISTORY_JSON.read_text(encoding="utf-8"))
            kept = []
            for k, v in old.items():
                if (pd.Timestamp(k).normalize() > end and isinstance(v, dict) and v
                        and "str1m" in next(iter(v.values()))):
                    kept.append(k)
            for k in kept:
                hist[k] = old[k]
            print(f"已有历史: {len(old)} 条；保留新结构未来周 {len(kept)} 条，区间内 {len(old)-len(kept)} 条将重写")
        except Exception as e:
            print(f"读取已有历史失败，重建: {e}")
            hist = {}

    new_count = 0
    for wk in weeks:
        rep = wk_rep[wk]
        day_df = kline[kline["date"] == rep]
        sector_df = compute_sector_for_date(day_df)
        if sector_df.empty:
            continue
        today = {}
        for _, r in sector_df.iterrows():
            today[r["industry"]] = {
                "rank1m": None if pd.isna(r["rank1m"]) else int(r["rank1m"]),
                "str1m": None if pd.isna(r["str1m"]) else float(r["str1m"]),
                "ret1m_cw": None if pd.isna(r["ret1m_cw"]) else float(r["ret1m_cw"]),
                "n_stocks": int(r["n_stocks"]),
            }
        wk_str = wk.strftime("%Y-%m-%d")
        if wk_str not in hist:
            new_count += 1
        hist[wk_str] = today

    print(f"新增/覆盖周: {new_count} 个；历史总周数: {len(hist)} 个")

    if args.dry_run:
        print("dry-run 模式，不写入文件")
        return

    SECTOR_HISTORY_JSON.write_text(
        json.dumps(hist, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    print(f"已写入 {SECTOR_HISTORY_JSON.name}")


if __name__ == "__main__":
    main()
