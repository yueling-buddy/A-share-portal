#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""9:25 集合竞价快照：跑 refresh.py --mode auction，并把结果发布到 GitHub Pages。

背景
----
GitHub Actions 的 cron 在早间档经常漂移或被丢弃（9:20-9:45 窗口内常常一次 run 都没有），
导致 data/auction.json 长期不更新、看板「涨跌幅%」列看不到竞价 gap。改由本机自动化在
9:25-9:30 触发本脚本兜底（与云端 cron 并行，谁先跑谁生效，幂等）。

只发布竞价相关的 4 个文件，不碰 pick.html / index.html / history.json：
    data/auction.json, data/rps.json, data/meta.json, data/cloud_run_log.json

推送链路复用 publish_pick.py 的已验证姿势（见 .workbuddy/memory/MEMORY.md）：
    1) 删除残留 .git/index.lock
    2) HTTPS fetch origin（匿名可读，443 放行）
    3) reset --hard FETCH_HEAD 线性对齐（**禁用 rebase**）
    4) 在「云端最新 rps.json」基础上跑 auction 模式，只打竞价补丁
    5) commit + push（SSH-over-443 优先；失败自动回退 HTTPS+PAT）
    6) 若 push 因远端又有新提交被拒 → 重新对齐并重跑一次（最多 2 轮）

用法
----
    python scripts/auction_run.py [--no-push] [--force]

  --no-push  只本地生成，不推送（调试用）
  --force    忽略时间窗：调试时强制抓一次（会写 auction.json）
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
KEY = ROOT.parent / ".workbuddy" / "github_a_share_deploy_key"
TOKEN_FILE = Path.home() / ".workbuddy" / ".gh_token"
HTTPS_URL = "https://github.com/yueling-buddy/A-share-portal.git"
SSH_URL = "ssh://git@ssh.github.com:443/yueling-buddy/A-share-portal.git"
BRANCH = "main"
SSH_BIN = r"C:\Users\yueling.liu\.workbuddy\binaries\PortableGit\versions\1.2.0\usr\bin\ssh.exe"
VENV_PY = r"C:\Users\yueling.liu\.workbuddy\binaries\python\envs\refresh_ashare\Scripts\python.exe"
PUBLISH = [
    "data/auction.json",
    "data/rps.json",
    "data/meta.json",
    "data/cloud_run_log.json",
]


def _pick_python() -> str:
    """选一个能 import pandas 的解释器：优先当前解释器，否则用 refresh_ashare venv。"""
    probe = "import pandas, akshare"
    for cand in (sys.executable, VENV_PY):
        if not cand:
            continue
        try:
            r = subprocess.run([cand, "-c", probe], capture_output=True, timeout=120)
            if r.returncode == 0:
                return cand
        except Exception:
            continue
    raise SystemExit("找不到可用解释器（需 pandas+akshare）：请用 refresh_ashare venv 运行本脚本")


def sh(args, cwd=ROOT, env=None, timeout=600_000, check=False):
    print("$", " ".join(args), flush=True)
    r = subprocess.run(args, cwd=cwd, env=env, text=True, timeout=timeout,
                       capture_output=True)
    if r.stdout:
        print(r.stdout, end="", flush=True)
    if r.stderr:
        print(r.stderr, end="", file=sys.stderr, flush=True)
    if check and r.returncode != 0:
        raise SystemExit(f"命令失败({r.returncode}): {' '.join(args)}")
    return r.returncode


def resolve_ssh_ip():
    """DoH 查 ssh.github.com 真实 IP（本机 DNS 被污染）。"""
    import json
    import urllib.request

    for url in ("https://dns.google/resolve?name=ssh.github.com&type=A",
                "https://cloudflare-dns.com/dns-query?name=ssh.github.com&type=A&ct=application/dns-json"):
        try:
            with urllib.request.urlopen(url, timeout=8) as f:
                data = json.load(f)
            for ans in data.get("Answer", []):
                if ans.get("type") == 1:
                    return ans["data"]
        except Exception:
            continue
    return None


def ssh_env() -> dict:
    ip = resolve_ssh_ip()
    host_opt = f"-o HostName={ip} " if ip else ""
    ssh = SSH_BIN if Path(SSH_BIN).exists() else "ssh"
    env = os.environ.copy()
    env["GIT_SSH_COMMAND"] = (
        f'"{ssh}" -i "{KEY}" -p 443 {host_opt}'
        "-o BatchMode=yes -o ConnectTimeout=15"
        "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
    )
    return env


def align_to_remote() -> None:
    """HTTPS fetch + reset --hard FETCH_HEAD（线性对齐，禁用 rebase）。"""
    (ROOT / ".git" / "index.lock").unlink(missing_ok=True)
    rc = sh(["git", "fetch", HTTPS_URL, BRANCH])
    if rc != 0:
        raise SystemExit("git fetch 失败")
    sh(["git", "remote", "set-url", "origin", HTTPS_URL])
    rc = sh(["git", "reset", "--hard", "FETCH_HEAD"])
    if rc != 0:
        raise SystemExit("git reset --hard FETCH_HEAD 失败")


def run_auction_mode(force: bool) -> int:
    py = _pick_python()
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"   # 防 GBK 编码下 emoji/中文 打印崩溃
    args = [py, "scripts/refresh.py", "--mode", "auction"]
    if force:
        args.append("--force")
    return sh(args, env=env, timeout=600_000)


def stage_and_commit() -> bool:
    """add 4 个竞价文件；无变更返回 False。"""
    files = [f for f in PUBLISH if (ROOT / f).exists()]
    if not files:
        print("待发布文件都不存在，跳过")
        return False
    sh(["git", "add", "--"] + files)
    r = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=ROOT)
    if r.returncode == 0:
        print("无变更可提交（竞价窗口未命中或数据未变）")
        return False
    msg = "auction: 9:25 集合竞价快照"
    try:
        import json
        meta = json.loads((ROOT / "data" / "auction.json").read_text(encoding="utf-8")).get("meta", {})
        msg += f" {meta.get('asof', '')} {meta.get('captured_at', '')}".rstrip()
    except Exception:
        pass
    sh(["git", "config", "user.name", "WorkBuddy"])
    sh(["git", "config", "user.email", "workbuddy@local"])
    return sh(["git", "commit", "-q", "-m", msg]) == 0


def push() -> bool:
    # 1) SSH-over-443
    ssh_rc = sh(["git", "push", SSH_URL, f"HEAD:refs/heads/{BRANCH}"],
                env=ssh_env(), timeout=300_000)
    if ssh_rc == 0:
        return True
    print("SSH 推送失败，回退 HTTPS+PAT ...")
    # 2) HTTPS + PAT 回退
    if not TOKEN_FILE.is_file():
        print(f"未找到 {TOKEN_FILE}，无法回退")
        return False
    token = TOKEN_FILE.read_text(encoding="utf-8").strip()
    url = f"https://x-access-token:{token}@github.com/yueling-buddy/A-share-portal.git"
    return sh(["git", "-c", "http.postBuffer=524288000", "-c", "http.version=HTTP/1.1",
               "push", url, f"HEAD:refs/heads/{BRANCH}"], timeout=900_000) == 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-push", action="store_true", help="只本地生成，不推送")
    ap.add_argument("--force", action="store_true", help="忽略时间窗（调试用）")
    args = ap.parse_args()

    if not KEY.is_file():
        raise SystemExit(f"部署密钥不存在: {KEY}")

    for attempt in (1, 2):
        print(f"\n===== 第 {attempt} 轮 =====", flush=True)
        align_to_remote()
        rc = run_auction_mode(args.force)
        if rc != 0:
            raise SystemExit(f"auction 模式失败（rc={rc}）")
        if not stage_and_commit():
            print("结束：无变更")
            return
        if args.no_push:
            print("--no-push：已本地提交，未推送")
            return
        if push():
            print("\n发布完成：https://yueling-buddy.github.io/A-share-portal/")
            return
        print("push 失败（可能远端又有新提交），重新对齐后重试 ...")
    raise SystemExit("push 连续失败，请人工检查")


if __name__ == "__main__":
    main()
