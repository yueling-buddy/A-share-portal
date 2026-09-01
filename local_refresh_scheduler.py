#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本地盘中定时刷新调度器（交易时段每 15 分钟运行，由 Windows 计划任务唤醒）。

逻辑：
  - 周末 -> 跳过
  - 上午 09:30-11:30 / 下午 13:00-15:00 -> mode=intraday（拉实时行情重算 RPS）
  - 收盘后 15:10-15:30 -> mode=close（定稿写回缓存）
  - 其余时段 -> 跳过

运行：python local_refresh_scheduler.py
依赖：scripts/refresh.py 已就绪；Git + 部署密钥已配置。
"""
from __future__ import annotations

import datetime
import os
import subprocess
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

BJ = ZoneInfo("Asia/Shanghai")
ROOT = Path(__file__).resolve().parent
PY = r"C:\Users\yueling.liu\.workbuddy\binaries\python\envs\refresh_ashare\Scripts\python.exe"
GIT = r"C:\Users\yueling.liu\.workbuddy\binaries\PortableGit\versions\1.2.0\cmd\git.exe"
SSH = r"C:\Users\yueling.liu\.workbuddy\binaries\PortableGit\versions\1.2.0\usr\bin\ssh.exe"
KEY = r"D:\Workbuddy Space\A股大作战\.workbuddy\github_a_share_deploy_key"
LOG = ROOT / "local_scheduler.log"


def log(msg: str) -> None:
    ts = datetime.datetime.now(BJ).strftime("%F %T")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def decide_mode() -> str | None:
    now = datetime.datetime.now(BJ)
    if now.weekday() >= 5:  # 周六日
        return None
    t = now.time()
    if datetime.time(9, 30) <= t <= datetime.time(11, 30):
        return "intraday"
    if datetime.time(13, 0) <= t <= datetime.time(15, 0):
        return "intraday"
    if datetime.time(15, 10) <= t <= datetime.time(15, 30):
        return "close"
    return None


def run(cmd, env=None, timeout=600):
    e = dict(os.environ)
    if env:
        e.update(env)
    return subprocess.run(cmd, cwd=str(ROOT), env=e, timeout=timeout)


def main() -> int:
    mode = decide_mode()
    if mode is None:
        log("非交易时段/周末，跳过")
        return 0
    log(f"触发模式: {mode}")

    # 1) 运行 refresh
    refresh = Path(ROOT / "scripts" / "refresh.py")
    rc = run([PY, str(refresh), "--mode", mode]).returncode
    if rc != 0:
        log(f"refresh 失败 rc={rc}")
        return rc

    # 2) 提交并推送（仅刷新产物）
    env = {
        "GIT_SSH_COMMAND": f'"{SSH}" -i "{KEY}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no'
    }
    run([GIT, "add", "index.html", "data/rps.json", "data/fip.json", "data/meta.json"], env=env)
    commit = run(
        [GIT, "commit", "-m", f"local: {mode} refresh {datetime.datetime.now(BJ):%F_%H-%M}"],
        env=env,
    )
    if commit.returncode != 0:
        log("无变更或可提交内容，跳过 push")
        return 0
    push = run([GIT, "push", "-u", "origin", "main"], env=env, timeout=120)
    if push.returncode != 0:
        log("push 失败")
        return push.returncode
    log(f"{mode} 刷新并推送成功")
    return 0


if __name__ == "__main__":
    sys.exit(main())
