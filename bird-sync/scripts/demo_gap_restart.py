#!/usr/bin/env python3
"""缺件同步 · 进程级演示（无需 Docker）。

完整演示：
  1. bravo 产生第 1 条（删 R003）与第 2 条（改 R004）更正；
  2. alpha 只收到第 2 条 -> 缺第 1 条：向量时钟不虚高、缺口持久化、分析仍用旧数据；
  3. 缺件窗口内 alpha 本地更正（R003 -> 33）；
  4. 重启 alpha 进程（同一 SQLite 数据卷），缺口与时钟保持；
  5. 重启后一键 /sync/run 自动补齐缺件 -> 删除与本地更正冲突，分析排除；
  6. 重复/乱序投递不重复入库；补齐后再同步不重复传输。

运行：python3 scripts/demo_gap_restart.py
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile

import httpx
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.main import create_app  # noqa: E402

CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "seed", "sample_counts.csv")


def banner(title: str) -> None:
    print("\n" + "=" * 16 + f" {title} " + "=" * 16)


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="bird-gap-demo-")
    alpha_db = os.path.join(tmp, "alpha.db")
    bravo_db = os.path.join(tmp, "bravo.db")
    try:
        bravo = create_app("bravo", bravo_db, CSV_PATH, peer_url="http://alpha")
        with TestClient(bravo) as bc:
            banner("1. bravo 产生两条更正（op1 删除、op2 改值）")
            print("delete R003 ->", bc.delete("/records/R003").json()["op_id"])
            print("correct R004 -> 44 ->",
                  bc.post("/records/R004/corrections", json={"count": 44}).json()["op_id"])
            full = bc.get("/sync/pull").json()
            op2 = [o for o in full["ops"] if o["id"] == "bravo-2"]

        alpha = create_app("alpha", alpha_db, CSV_PATH, peer_url="http://bravo")
        with TestClient(alpha) as ac, TestClient(bravo) as bc:
            alpha.state.peer_transport = httpx.ASGITransport(app=bravo)
            bravo.state.peer_transport = httpx.ASGITransport(app=alpha)

            banner("2. alpha 只收第 2 条（bravo-1 删除缺件）")
            print(ac.post("/sync/batch", json={"ops": op2}).json())
            st = ac.get("/status").json()
            print("向量时钟（不得含 bravo:2）：", st["version_vv"])
            print("收件缺口：", st["inbound_gaps"])
            assert st["version_vv"] == {}
            assert st["inbound_gaps"] == {"bravo": {"op": [2]}}

            banner("3. 缺件期间本地更正 R003 -> 33（允许）")
            print(ac.post("/records/R003/corrections", json={"count": 33}).json())
            print("R003 本地视图：", ac.get("/records/R003").json())

            banner("4. 重复投递 bravo-2 不重复入库")
            print(ac.post("/sync/batch", json={"ops": op2}).json())

        # ---- 重启 alpha：关闭旧进程，用同一数据卷重新创建应用 ----
        banner("5. 重启 alpha（同一数据卷），缺口与时钟保持")
        alpha2 = create_app("alpha", alpha_db, CSV_PATH, peer_url="http://bravo")
        with TestClient(alpha2) as ac2, TestClient(bravo) as bc2:
            alpha2.state.peer_transport = httpx.ASGITransport(app=bravo)
            bravo.state.peer_transport = httpx.ASGITransport(app=alpha2)
            st = ac2.get("/status").json()
            print("重启后缺口：", st["inbound_gaps"], " 时钟：", st["version_vv"])
            assert st["inbound_gaps"] == {"bravo": {"op": [2]}}

            banner("6. 重启后一键同步补齐缺件")
            print(ac2.post("/sync/run").json())
            st = ac2.get("/status").json()
            print("补齐后缺口（应空）：", st["inbound_gaps"])
            print("补齐后时钟：", st["version_vv"])
            r3 = ac2.get("/records/R003").json()
            print("R003：conflicted =", r3["conflicted"],
                  "（删除 vs 本地改值冲突，不被任一方覆盖）")
            conflict = next(c for c in ac2.get("/conflicts").json()
                            if c["record_id"] == "R003")
            print("R003 分支：", [(b["type"], b["count"]) for b in conflict["branches"]])
            ids = {r["record_id"] for r in ac2.get("/analysis/counts").json()}
            assert "R003" not in ids, "冲突记录必须从分析取数排除"
            print("R003 已从分析取数排除；R004 =",
                  ac2.get("/records/R004").json()["count"])

            banner("7. 补齐后再次同步：零传输、幂等")
            again = ac2.post("/sync/run").json()
            print(again)
            assert again["pulled_ops"] == 0 and again["pushed_ops"] == 0

        print("\n演示完成：缺件不被消息时钟跳过，重启后补齐，冲突与分析排除均符合原规则。")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
