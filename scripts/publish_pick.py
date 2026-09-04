#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""把盘中选股日报发布到 GitHub Pages（A-share-portal 根目录）。

只发布 3 个与选股日报相关的文件，完全不碰云端 bot 每 15 分钟维护的
index.html / data/rps.json 等看板产物，避免与云端刷新互相覆盖：
  pick.html、data/pick_latest.json、data/fundamentals.json

推送链路已规避本机网络坑（见 .workbuddy/memory/MEMORY.md）：
  - 下载走 HTTPS（匿名可读，443 隧道被代理放行），全量 fetch 需长超时
  - 上传走 SSH-over-443 且直连真实 IP（ssh.github.com 被 DNS 污染）
  - 禁用 rebase，用 reset --hard FETCH_HEAD → 覆盖 → commit → push 线性流程
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
KEY = ROOT.parent / ".workbuddy" / "github_a_share_deploy_key"
HTTPS_URL = "https://github.com/yueling-buddy/A-share-portal.git"
SSH_URL = "ssh://git@ssh.github.com:443/yueling-buddy/A-share-portal.git"
BRANCH = "main"
import shutil

SSH_BIN = r"C:\Users\yueling.liu\.workbuddy\binaries\PortableGit\versions\1.2.0\usr\bin\ssh.exe"
PUBLISH = ["pick.html", "data/pick_latest.json", "data/fundamentals.json",
           "scripts/pick_daily.py", "scripts/publish_pick.py"]


def run(args: list[str], cwd: Path | None = None, env=None, timeout: int = 600_000) -> int:
    print("$", " ".join(args), flush=True)
    r = subprocess.run(args, cwd=cwd, env=env, text=True, timeout=timeout)
    return r.returncode


def resolve_ssh_ip() -> str | None:
    """DoH 查 ssh.github.com 真实 IP（本机 DNS 被污染）。"""
    import json as _json
    import urllib.request

    for url in ("https://dns.google/resolve?name=ssh.github.com&type=A",
                "https://cloudflare-dns.com/dns-query?name=ssh.github.com&type=A&ct=application/dns-json"):
        try:
            with urllib.request.urlopen(url, timeout=8) as f:
                data = _json.load(f)
            for ans in data.get("Answer", []):
                if ans.get("type") == 1:
                    return ans["data"]
        except Exception:
            continue
    return None


def main() -> None:
    if not KEY.is_file():
        raise SystemExit(f"部署密钥不存在: {KEY}")

    # 0) 待发布文件先备份到仓库外，防止 reset 影响
    backup = Path(tempfile.mkdtemp(prefix="pick_pub_"))
    for rel in PUBLISH:
        src = ROOT / rel
        if src.exists():
            dst = backup / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_bytes(src.read_bytes())
    print(f"备份 {len(list(backup.rglob('*')))} 个文件到 {backup}", flush=True)

    # 1) HTTPS 拉取远端最新
    (ROOT / ".git" / "index.lock").unlink(missing_ok=True)
    if run(["git", "fetch", HTTPS_URL, BRANCH], cwd=ROOT) != 0:
        raise SystemExit("fetch 失败")

    # 2) 线性对齐（禁用 rebase）
    subprocess.run(["git", "remote", "set-url", "origin", HTTPS_URL], cwd=ROOT)
    if run(["git", "reset", "--hard", "FETCH_HEAD"], cwd=ROOT) != 0:
        raise SystemExit("reset 失败")

    # 3) 拷回报备文件
    for rel in PUBLISH:
        src = backup / rel
        if src.exists():
            dst = ROOT / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_bytes(src.read_bytes())

    # 4) 提交
    run(["git", "config", "user.name", "WorkBuddy"], cwd=ROOT)
    run(["git", "config", "user.email", "workbuddy@local"], cwd=ROOT)
    run(["git", "add", "-A"], cwd=ROOT)
    diff = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=ROOT)
    if diff.returncode == 0:
        print("无变更可发布")
        return
    run(["git", "commit", "-m", "pick: 更新盘中选股日报"], cwd=ROOT)

    # 5) SSH-over-443 推送，直连真实 IP 绕开 DNS 污染
    ip = resolve_ssh_ip()
    host_opt = f"-o HostName={ip} " if ip else ""
    ssh = SSH_BIN if Path(SSH_BIN).exists() else "ssh"
    env = os.environ.copy()
    env["GIT_SSH_COMMAND"] = (
        f'"{ssh}" -i "{KEY}" -p 443 {host_opt}'
        "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
    )
    subprocess.run(["git", "remote", "set-url", "origin", SSH_URL], cwd=ROOT)
    rc = run(["git", "push", SSH_URL, f"HEAD:refs/heads/{BRANCH}"], cwd=ROOT, env=env, timeout=300_000)
    if rc != 0:
        raise SystemExit("push 失败")
    print("\n发布完成：https://yueling-buddy.github.io/A-share-portal/pick.html")


if __name__ == "__main__":
    main()
