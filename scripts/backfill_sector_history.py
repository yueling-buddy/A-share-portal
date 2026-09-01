#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""基于本地 K 线缓存回溯补全板块历史排名数据。

用法:
    python scripts/backfill_sector_history.py [--start 2025-04-02] [--end 2026-08-31]

说明:
    - 直接读 data/kline_cache.parquet + data/industry.csv + data/mcap.csv + data/codes.csv
    - 按交易日、行业聚合市值加权区间收益（1周/1月/3月/6月）
    - 在每个交易日截面内计算各行业名次（涨幅最高=第1名）
    - 结果合并进 data/sector_rps_history.json（同日期覆盖）
    - 仅补齐收盘日截面；盘中 09-01 及之后仍由 refresh.py 实时产出
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
    """对单个交易日的股票截面，按行业聚合并计算排名。"""
    # 行业内市值加权收益
    def _industry_agg(g):
        tot = g["mcap"].sum()
        w = g["mcap"] / tot if tot > 0 else pd.Series([1.0 / len(g)] * len(g), index=g.index)
        return pd.Series({
            "ret1w_cw": (g["ret1w"] * w).sum() if g["ret1w"].notna().any() else np.nan,
            "ret1m_cw": (g["ret1m"] * w).sum() if g["ret1m"].notna().any() else np.nan,
            "ret3m_cw": (g["ret3m"] * w).sum() if g["ret3m"].notna().any() else np.nan,
            "ret6m_cw": (g["ret6m"] * w).sum() if g["ret6m"].notna().any() else np.nan,
            "n_stocks": len(g),
        })

    counts = group.groupby("industry")["code"].count()
    valid = counts[counts >= SECTOR_MIN_STOCKS].index
    group = group[group["industry"].isin(valid)].copy()
    if group.empty:
        return pd.DataFrame()

    ind = group.groupby("industry").apply(_industry_agg, include_groups=False).reset_index()
    for win in ["1w", "1m", "3m", "6m"]:
        col = f"ret{win}_cw"
        rank_col = f"rank{win}"
        ind[col] = pd.to_numeric(ind[col], errors="coerce")
        # 涨幅最高 = 第 1 名
        ind[rank_col] = ind[col].rank(ascending=False, method="first").astype("Int64")
        ind[col] = ind[col].round(2)
    return ind


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2025-04-02", help="起始日期 (YYYY-MM-DD)")
    parser.add_argument("--end", default="2026-08-31", help="结束日期 (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="只计算不写入")
    args = parser.parse_args()

    kline = load_data()

    # 计算每只股票在每个交易日的区间收益（用未来值对齐：当前 close vs N 个交易日前 close）
    print("计算个股区间收益 ...")
    kline = kline.sort_values(["code", "date"])
    for periods, label in [(5, "1w"), (20, "1m"), (60, "3m"), (120, "6m")]:
        kline[f"ret{label}"] = (
            kline.groupby("code")["close"]
            .pct_change(periods=periods, fill_method=None) * 100.0
        )

    # 过滤日期范围
    start = pd.Timestamp(args.start)
    end = pd.Timestamp(args.end)
    kline = kline[(kline["date"] >= start) & (kline["date"] <= end)].copy()
    dates = sorted(kline["date"].unique())
    print(f"待回溯交易日: {len(dates)} 个 ({str(dates[0].date())} ~ {str(dates[-1].date())})")

    # 读取已有历史
    hist = {}
    if SECTOR_HISTORY_JSON.exists():
        try:
            hist = json.loads(SECTOR_HISTORY_JSON.read_text(encoding="utf-8"))
            print(f"已有历史: {len(hist)} 个交易日")
        except Exception as e:
            print(f"读取已有历史失败，重建: {e}")
            hist = {}

    # 逐日计算
    new_count = 0
    for d in dates:
        dstr = pd.Timestamp(d).strftime("%Y-%m-%d")
        day_df = kline[kline["date"] == d]
        sector_df = compute_sector_for_date(day_df)
        if sector_df.empty:
            continue
        today = {}
        for _, r in sector_df.iterrows():
            today[r["industry"]] = {
                "ret1w_cw": None if pd.isna(r["ret1w_cw"]) else float(r["ret1w_cw"]),
                "rank1w": None if pd.isna(r["rank1w"]) else int(r["rank1w"]),
                "ret1m_cw": None if pd.isna(r["ret1m_cw"]) else float(r["ret1m_cw"]),
                "rank1m": None if pd.isna(r["rank1m"]) else int(r["rank1m"]),
                "ret3m_cw": None if pd.isna(r["ret3m_cw"]) else float(r["ret3m_cw"]),
                "rank3m": None if pd.isna(r["rank3m"]) else int(r["rank3m"]),
                "ret6m_cw": None if pd.isna(r["ret6m_cw"]) else float(r["ret6m_cw"]),
                "rank6m": None if pd.isna(r["rank6m"]) else int(r["rank6m"]),
                "n_stocks": int(r["n_stocks"]),
                "n_strong": None,   # 回溯不计算 composite_rps 强势股数
            }
        if dstr not in hist:
            new_count += 1
        hist[dstr] = today

    print(f"新增/覆盖交易日: {new_count} 个；历史总交易日: {len(hist)} 个")

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
