#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""用新浪日K接口回补 kline_cache.parquet 缺失的交易日。

背景：scripts/refresh.py --mode backfill 走的是东财 stock_zh_a_hist
（push2his.eastmoney.com），该域名在本机被代理 RST，100% 失败。
新浪 money.finance.sina.com.cn 的 getKLineData 在本机可用，且与现有缓存口径一致：
  - 前复权（重叠日期 close 与缓存逐笔相同）
  - volume 单位为「股」（实测与缓存 ratio = 1.000，无需 x100）

用法：
  python scripts/backfill_sina_kline.py              # 自动探测缺失交易日并回补
  python scripts/backfill_sina_kline.py --workers 8  # 指定并发
  python scripts/backfill_sina_kline.py --dry-run    # 只看缺口，不写盘
"""
import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent.parent
CACHE = ROOT / "data" / "kline_cache.parquet"
SEED = ROOT / "data" / "kline_cache_seed.parquet"
UA = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn/"}
URL = ("https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
       "CN_MarketData.getKLineData")
REF_CODE = "600519"          # 用贵州茅台做交易日历参照（流动性好、几乎不停牌）
COLS = ["date", "open", "high", "low", "close", "volume", "code"]


def prefix(code: str) -> str:
    """与 scripts/refresh.py 的 prefix 保持一致。"""
    code = str(code)
    if code.startswith(("8", "4", "9")):
        return "bj" + code
    if code.startswith("6"):
        return "sh" + code
    return "sz" + code


def fetch_bars(code: str, datalen: int = 20, retries: int = 3, timeout: int = 15):
    """拉取单只股票最近 datalen 根日线，返回 DataFrame 或 None。"""
    params = {"symbol": prefix(code), "scale": "240", "ma": "no", "datalen": str(datalen)}
    for i in range(retries):
        try:
            r = requests.get(URL, params=params, headers=UA, timeout=timeout)
            txt = r.text.strip()
            if not txt or txt in ("null", "[]"):
                return None
            rows = json.loads(txt)
            if not isinstance(rows, list) or not rows:
                return None
            df = pd.DataFrame(rows)
            df = df.rename(columns={"day": "date"})
            df["date"] = pd.to_datetime(df["date"])
            for c in ("open", "high", "low", "close", "volume"):
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df["code"] = str(code).zfill(6)
            df = df.dropna(subset=["date", "close"])
            return df[COLS]
        except Exception:
            if i == retries - 1:
                return None
            time.sleep(0.4 * (i + 1))
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--datalen", type=int, default=20, help="每只拉取最近 N 根日线")
    ap.add_argument("--dry-run", action="store_true", help="只探测缺口，不回补")
    args = ap.parse_args()

    if not CACHE.exists():
        print(f"未找到缓存 {CACHE}")
        sys.exit(1)

    cache = pd.read_parquet(CACHE)
    cache["date"] = pd.to_datetime(cache["date"])
    cache["code"] = cache["code"].astype(str).str.zfill(6)
    cache_dates = set(cache["date"].dt.strftime("%Y-%m-%d"))
    print(f"缓存现状: {len(cache):,} 行 / {cache['code'].nunique():,} 只 / {len(cache_dates)} 个交易日")

    # 1) 用参照股确定「应该存在」的交易日
    ref = fetch_bars(REF_CODE, datalen=max(args.datalen, 40))
    if ref is None or ref.empty:
        print("无法获取参照股交易日历，放弃")
        sys.exit(1)
    lo, hi = cache["date"].min(), cache["date"].max()
    ref_dates = {d.strftime("%Y-%m-%d")
                 for d in ref["date"] if lo <= d <= hi}
    missing = sorted(ref_dates - cache_dates)
    print(f"缓存日期范围: {lo.date()} ~ {hi.date()}")
    print(f"缺失交易日: {missing if missing else '无（缓存连续）'}")

    if not missing:
        print("无需回补")
        return
    if args.dry_run:
        print("[dry-run] 未写盘")
        return

    missing_set = set(missing)
    codes = sorted(cache["code"].unique())
    print(f"== 回补 {len(missing)} 个交易日 / {len(codes)} 只（并发 {args.workers}）==")

    new_rows, ok, fail = [], 0, 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(fetch_bars, c, args.datalen): c for c in codes}
        for n, fut in enumerate(as_completed(futs), 1):
            df = fut.result()
            if df is not None and not df.empty:
                ok += 1
                sel = df[df["date"].dt.strftime("%Y-%m-%d").isin(missing_set)]
                if not sel.empty:
                    new_rows.append(sel)
            else:
                fail += 1
            if n % 500 == 0:
                print(f"  已处理 {n}/{len(codes)}（成功 {ok} 失败 {fail}，"
                      f"{time.time() - t0:.0f}s）")

    print(f"抓取完成: 成功 {ok} / 失败 {fail}，耗时 {time.time() - t0:.0f}s")
    if not new_rows:
        print("未抓到任何可回补数据，未改动缓存")
        sys.exit(1)

    add = pd.concat(new_rows, ignore_index=True)
    add["code"] = add["code"].astype(str).str.zfill(6)
    print(f"待合并新行: {len(add):,} 行，覆盖 {add['code'].nunique():,} 只")

    merged = pd.concat([cache, add], ignore_index=True)
    # 新数据在后，keep='last' => 新值覆盖旧值
    merged = merged.drop_duplicates(subset=["code", "date"], keep="last")
    merged = merged.sort_values(["code", "date"]).reset_index(drop=True)

    # 回补前先留一份快照
    bak = CACHE.with_suffix(".parquet.prebackfill")
    import shutil
    shutil.copy2(CACHE, bak)
    print(f"已备份旧缓存 -> {bak.name}")

    merged.to_parquet(CACHE, index=False)
    print(f"已写回 {CACHE.name}: {len(merged):,} 行 / {merged['code'].nunique():,} 只")

    # 与 refresh.py 的 backfill 行为一致：同步 seed
    if SEED.exists():
        try:
            seed = pd.read_parquet(SEED)
            seed["date"] = pd.to_datetime(seed["date"])
            seed["code"] = seed["code"].astype(str).str.zfill(6)
            seed_m = pd.concat([seed, add], ignore_index=True)
            seed_m = seed_m.drop_duplicates(subset=["code", "date"], keep="last")
            seed_m = seed_m.sort_values(["code", "date"]).reset_index(drop=True)
            seed_m.to_parquet(SEED, index=False)
            print(f"已同步 {SEED.name}: {len(seed_m):,} 行")
        except Exception as e:
            print(f"同步 seed 失败（不影响主缓存）: {repr(e)[:120]}")

    # 校验
    chk = pd.read_parquet(CACHE)
    chk["date"] = pd.to_datetime(chk["date"])
    got = set(chk["date"].dt.strftime("%Y-%m-%d"))
    still = sorted(set(missing) - got)
    print(f"校验: 缺失日已补齐 {sorted(set(missing) & got)}")
    print(f"      仍缺失: {still if still else '无'}")


if __name__ == "__main__":
    main()
