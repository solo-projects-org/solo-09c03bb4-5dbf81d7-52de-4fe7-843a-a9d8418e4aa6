"""端到端测试：双站离线更正、同步、冲突、裁定、幂等乱序重试与重启持久化。"""

from __future__ import annotations

import json
import os
import shutil
import tempfile

import httpx
import pytest
from fastapi.testclient import TestClient

from app.main import create_app

CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "seed", "sample_counts.csv")


@pytest.fixture()
def stations():
    tmp = tempfile.mkdtemp(prefix="bird-sync-test-")
    try:
        alpha_db = os.path.join(tmp, "alpha.db")
        bravo_db = os.path.join(tmp, "bravo.db")

        alpha = create_app("alpha", alpha_db, CSV_PATH, peer_url="http://bravo.local")
        bravo = create_app("bravo", bravo_db, CSV_PATH, peer_url="http://alpha.local")

        with TestClient(alpha) as ac, TestClient(bravo) as bc:
            # 进程内直连：/sync/run 通过 ASGI transport 访问对站，无需真实网络
            alpha.state.peer_transport = httpx.ASGITransport(app=bravo)
            bravo.state.peer_transport = httpx.ASGITransport(app=alpha)
            yield {"alpha": ac, "bravo": bc, "tmp": tmp,
                   "alpha_app": alpha, "bravo_app": bravo,
                   "alpha_db": alpha_db, "bravo_db": bravo_db}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# --------------------------------------------------------------------------- 基础


def test_seed_data_identical(stations):
    a = stations["alpha"].get("/records").json()
    b = stations["bravo"].get("/records").json()
    assert a == b
    assert len(a) == 8
    assert all(r["conflicted"] is False and r["deleted"] is False for r in a)
    expected = [
        {"record_id": r["record_id"], "site": r["site"],
         "species": r["species"], "count": r["count"]}
        for r in a
    ]
    assert stations["alpha"].get("/analysis/counts").json() == expected


def test_negative_count_rejected(stations):
    r = stations["alpha"].post(
        "/records/R001/corrections", json={"count": -1}
    )
    assert r.status_code == 422  # pydantic 校验


def test_unknown_record_404(stations):
    assert stations["alpha"].post(
        "/records/NOPE/corrections", json={"count": 1}
    ).status_code == 404


def test_delete_then_analysis_excludes(stations):
    c = stations["alpha"]
    r = c.delete("/records/R007")
    assert r.status_code == 200
    rec = c.get("/records/R007").json()
    assert rec["deleted"] is True
    ids = {row["record_id"] for row in c.get("/analysis/counts").json()}
    assert "R007" not in ids


# --------------------------------------------------------------------------- 冲突


def test_concurrent_different_counts_become_conflict(stations):
    a, b = stations["alpha"], stations["bravo"]
    a.post("/records/R001/corrections", json={"count": 50})
    b.post("/records/R001/corrections", json={"count": 70})
    assert a.get("/records/R001").json()["conflicted"] is False  # 未同步前互不知晓

    r = a.post("/sync/run")
    assert r.status_code == 200, r.text

    for c in (a, b):
        rec = c.get("/records/R001").json()
        assert rec["conflicted"] is True
        conflicts = c.get("/conflicts").json()
        assert len(conflicts) == 1
        assert conflicts[0]["record_id"] == "R001"
        assert {br["count"] for br in conflicts[0]["branches"]} == {50, 70}
        # 分析接口排除冲突记录
        assert "R001" not in {x["record_id"] for x in c.get("/analysis/counts").json()}


def test_concurrent_delete_vs_count_conflict(stations):
    a, b = stations["alpha"], stations["bravo"]
    a.post("/records/R002/corrections", json={"count": 99})
    b.delete("/records/R002")
    a.post("/sync/run")
    conflicts = a.get("/conflicts").json()
    assert len(conflicts) == 1
    types = {br["type"] for br in conflicts[0]["branches"]}
    assert types == {"count", "delete"}


def test_same_value_concurrent_updates_auto_merge(stations):
    a, b = stations["alpha"], stations["bravo"]
    a.post("/records/R003/corrections", json={"count": 11})
    b.post("/records/R003/corrections", json={"count": 11})
    a.post("/sync/run")
    rec = a.get("/records/R003").json()
    assert rec["conflicted"] is False
    assert rec["count"] == 11
    assert a.get("/conflicts").json() == []


def test_concurrent_deletes_auto_merge(stations):
    a, b = stations["alpha"], stations["bravo"]
    a.delete("/records/R004")
    b.delete("/records/R004")
    a.post("/sync/run")
    rec = a.get("/records/R004").json()
    assert rec["deleted"] is True
    assert rec["conflicted"] is False


def test_causal_chained_updates_no_conflict(stations):
    a, b = stations["alpha"], stations["bravo"]
    a.post("/records/R005/corrections", json={"count": 1})
    a.post("/sync/run")
    # bravo 在已读到 alpha 修改的基础上再改
    b.post("/records/R005/corrections", json={"count": 2})
    b.post("/sync/run")
    rec = a.get("/records/R005").json()
    assert rec["conflicted"] is False
    assert rec["count"] == 2


# --------------------------------------------------------------------------- 裁定


def test_resolve_conflict_with_count(stations):
    a, b = stations["alpha"], stations["bravo"]
    a.post("/records/R001/corrections", json={"count": 50})
    b.post("/records/R001/corrections", json={"count": 70})
    a.post("/sync/run")

    r = a.post("/conflicts/R001/resolve", json={"choice": "count", "count": 70})
    assert r.status_code == 201, r.text
    assert a.get("/records/R001").json()["conflicted"] is False
    assert a.get("/records/R001").json()["count"] == 70
    assert "R001" in {x["record_id"] for x in a.get("/analysis/counts").json()}

    # 裁定同步到对站
    a.post("/sync/run")
    assert b.get("/records/R001").json()["count"] == 70
    assert b.get("/records/R001").json()["conflicted"] is False
    # 冲突列表不再包含已生效裁定的记录
    assert b.get("/conflicts").json() == []


def test_resolve_conflict_with_delete(stations):
    a, b = stations["alpha"], stations["bravo"]
    # 改值 vs 删除 的冲突，裁定选择删除分支
    a.post("/records/R002/corrections", json={"count": 5})
    b.delete("/records/R002")
    a.post("/sync/run")
    r = a.post("/conflicts/R002/resolve", json={"choice": "delete"})
    assert r.status_code == 201
    assert a.get("/records/R002").json()["deleted"] is True
    a.post("/sync/run")
    assert b.get("/records/R002").json()["deleted"] is True
    assert "R002" not in {x["record_id"] for x in b.get("/analysis/counts").json()}


def test_resolve_delete_requires_delete_branch(stations):
    """冲突双方都是计数时，删除不是可选分支，裁定被拒绝（400）。"""
    a, b = stations["alpha"], stations["bravo"]
    a.post("/records/R002/corrections", json={"count": 5})
    b.post("/records/R002/corrections", json={"count": 8})
    a.post("/sync/run")
    r = a.post("/conflicts/R002/resolve", json={"choice": "delete"})
    assert r.status_code == 400


def test_resolve_must_pick_existing_branch(stations):
    a, b = stations["alpha"], stations["bravo"]
    a.post("/records/R001/corrections", json={"count": 50})
    b.post("/records/R001/corrections", json={"count": 70})
    a.post("/sync/run")
    r = a.post("/conflicts/R001/resolve", json={"choice": "count", "count": 999})
    assert r.status_code == 400


def test_resolve_without_conflict_rejected(stations):
    r = stations["alpha"].post(
        "/conflicts/R003/resolve", json={"choice": "delete"}
    )
    assert r.status_code == 409


def test_stale_resolution_rejected_after_branch_change(stations):
    """读取后分支有变：基于旧分支的裁定必须被拒绝。"""
    a, b = stations["alpha"], stations["bravo"]
    a.post("/records/R001/corrections", json={"count": 50})
    b.post("/records/R001/corrections", json={"count": 70})
    a.post("/sync/run")

    conflict = a.get("/conflicts").json()[0]
    # 记下读取时刻 alpha 分支（count=50）的向量时钟
    old_branch_vv = next(
        br["vv"] for br in conflict["branches"] if br["count"] == 50
    )

    # 任一站点在该记录上追加一个因果后继更正，分支集合改变
    a.post("/records/R001/corrections", json={"count": 55})
    r = a.post(
        "/conflicts/R001/resolve",
        json={"choice": "count", "count": 70, "expected_branch_vv": old_branch_vv},
    )
    assert r.status_code == 409


def test_unseen_concurrent_correction_reopens_conflict(stations):
    """裁定后才收到未见的并发更正：重新列为冲突，裁定标记为陈旧。

    用第三个离线营地 charlie 产生一条从未参与裁定的并发更正。
    """
    a, b = stations["alpha"], stations["bravo"]
    c_app = create_app(
        "charlie",
        os.path.join(stations["tmp"], "charlie.db"),
        CSV_PATH,
        peer_url="http://alpha.local",
    )
    with TestClient(c_app) as c:
        c_app.state.peer_transport = httpx.ASGITransport(app=stations["alpha_app"])
        a.post("/records/R001/corrections", json={"count": 50})
        b.post("/records/R001/corrections", json={"count": 70})
        # charlie 断网独立修改，与 alpha/bravo 的分支并发
        c.post("/records/R001/corrections", json={"count": 77})
        a.post("/sync/run")
        a.post("/conflicts/R001/resolve", json={"choice": "count", "count": 50})
        assert a.get("/records/R001").json()["conflicted"] is False

        # 联网后 alpha 才收到 charlie 未见的并发更正
        c.post("/sync/run")
        a.post("/sync/run")

        rec = a.get("/records/R001").json()
        assert rec["conflicted"] is True
        conflicts = a.get("/conflicts").json()
        assert len(conflicts) == 1
        assert conflicts[0]["resolution_stale"] is True
        assert conflicts[0]["resolution_effective"] is False
        assert {br["count"] for br in conflicts[0]["branches"]} == {50, 70, 77}
        assert "R001" not in {
            x["record_id"] for x in a.get("/analysis/counts").json()
        }

        # 重新裁定后收敛
        conflict = a.get("/conflicts").json()[0]
        new_vv = next(br["vv"] for br in conflict["branches"] if br["count"] == 77)
        r = a.post(
            "/conflicts/R001/resolve",
            json={"choice": "count", "count": 77, "expected_branch_vv": new_vv},
        )
        assert r.status_code == 201
        c.post("/sync/run")
        assert c.get("/records/R001").json()["count"] == 77
        assert c.get("/records/R001").json()["conflicted"] is False


def test_successor_update_silently_overrides_resolution(stations):
    """裁定后收到的是合并所有旧分支的后继更正（非并发）：静默覆盖裁定。"""
    a, b = stations["alpha"], stations["bravo"]
    a.post("/records/R001/corrections", json={"count": 50})
    b.post("/records/R001/corrections", json={"count": 70})
    a.post("/sync/run")
    a.post("/conflicts/R001/resolve", json={"choice": "count", "count": 50})
    a.post("/sync/run")  # bravo 收到裁定，读到全部旧分支

    b.post("/records/R001/corrections", json={"count": 88})
    a.post("/sync/run")
    for c in (a, b):
        rec = c.get("/records/R001").json()
        assert rec["conflicted"] is False
        assert rec["count"] == 88
    assert a.get("/conflicts").json() == []


# --------------------------------------------------------------------------- 乐观锁


def test_optimistic_branch_check_on_correction(stations):
    a, b = stations["alpha"], stations["bravo"]
    a.post("/records/R001/corrections", json={"count": 50})
    vv = a.get("/records/R001").json()["version_vv"]
    # 先让 bravo 读到 alpha 的更正，再在其基础上产生因果后继
    a.post("/sync/run")
    b.post("/records/R001/corrections", json={"count": 70})
    a.post("/sync/run")
    r = a.post(
        "/records/R001/corrections",
        json={"count": 60, "expected_branch_vv": vv},
    )
    assert r.status_code == 409
    # 不带检查仍然允许提交
    r2 = a.post("/records/R001/corrections", json={"count": 60})
    assert r2.status_code == 201


# --------------------------------------------------------------------------- 同步语义


def test_sync_idempotent_and_convergent(stations):
    a, b = stations["alpha"], stations["bravo"]
    for i in range(3):
        a.post(f"/records/R00{(i % 3) + 1}/corrections", json={"count": 100 + i})
    for i in range(2):
        b.post(f"/records/R00{(i % 3) + 4}/corrections", json={"count": 200 + i})

    first = a.post("/sync/run").json()
    assert first["pulled_ops"] >= 2
    # 立即重复同步：无新内容
    second = a.post("/sync/run").json()
    assert second["pulled_ops"] == 0 and second["pushed_ops"] == 0

    # 再从另一侧同步，结果一致且向量时钟相等
    b.post("/sync/run")
    assert a.get("/status").json()["version_vv"] == b.get("/status").json()["version_vv"]
    assert a.get("/records").json() == b.get("/records").json()


def test_bidirectional_conflicts_and_full_convergence(stations):
    a, b = stations["alpha"], stations["bravo"]
    # 制造三类冲突 + 若干无冲突修改
    a.post("/records/R001/corrections", json={"count": 1})
    b.post("/records/R001/corrections", json={"count": 2})
    a.post("/records/R002/corrections", json={"count": 3})
    b.delete("/records/R002")
    a.delete("/records/R003")
    b.delete("/records/R003")  # 同动作自动合并
    a.post("/records/R004/corrections", json={"count": 4})
    b.post("/records/R005/corrections", json={"count": 5})

    a.post("/sync/run")
    b.post("/sync/run")

    assert a.get("/status").json()["version_vv"] == b.get("/status").json()["version_vv"]
    ca = sorted(c["record_id"] for c in a.get("/conflicts").json())
    cb = sorted(c["record_id"] for c in b.get("/conflicts").json())
    assert ca == cb == ["R001", "R002"]
    assert a.get("/records").json() == b.get("/records").json()
    assert a.get("/analysis/counts").json() == b.get("/analysis/counts").json()

    # 全部裁定后两站完全一致
    a.post("/conflicts/R001/resolve", json={"choice": "count", "count": 2})
    a.post("/conflicts/R002/resolve", json={"choice": "delete"})
    a.post("/sync/run")
    b.post("/sync/run")
    assert a.get("/conflicts").json() == []
    assert a.get("/records").json() == b.get("/records").json()


def test_batch_atomic_rollback(stations):
    a = stations["alpha"]
    good = a.post("/records/R001/corrections", json={"count": 41}).json()
    op_id = good["op_id"]
    vv = good["version_vv"]
    valid_op = {
        "id": op_id, "record_id": "R001", "station_id": "alpha",
        "seq": int(op_id.split("-")[1]), "type": "count", "count": 41,
        "vv": vv, "created_at": "2026-01-01T00:00:00+00:00",
    }
    bad_op = {
        "id": "bravo-99", "record_id": "R002", "station_id": "bravo",
        "seq": 99, "type": "count", "count": 5,
        "vv": {"bravo": 1}, "created_at": "2026-01-01T00:00:00+00:00",
    }  # 向量时钟本站分量 1 与 seq 99 不一致 -> 业务层拒绝并整批回滚
    before = a.get("/status").json()
    r = a.post("/sync/batch", json={"ops": [valid_op, bad_op]})
    assert r.status_code == 400
    after = a.get("/status").json()
    # 好操作因整批回滚未写入（但它本来已存在为重复项——这里换成全新好操作验证）
    assert before["op_count"] == after["op_count"]

    fresh_good = dict(valid_op, id="bravo-1", seq=1, station_id="bravo",
                      vv={"bravo": 1}, count=42, record_id="R002")
    r2 = a.post("/sync/batch", json={"ops": [fresh_good, bad_op]})
    assert r2.status_code == 400
    assert a.get("/status").json()["op_count"] == before["op_count"]
    # 单独提交好操作成功
    r3 = a.post("/sync/batch", json={"ops": [fresh_good]})
    assert r3.status_code == 200
    assert r3.json()["applied_ops"] == 1


def test_batch_duplicate_id_payload_mismatch_rejected(stations):
    a = stations["alpha"]
    good = a.post("/records/R001/corrections", json={"count": 41}).json()
    forged = {
        "id": good["op_id"], "record_id": "R001", "station_id": "alpha",
        "seq": int(good["op_id"].split("-")[1]), "type": "count", "count": 999,
        "vv": good["version_vv"], "created_at": "2026-01-01T00:00:00+00:00",
    }
    r = a.post("/sync/batch", json={"ops": [forged]})
    assert r.status_code == 400
    assert a.get("/records/R001").json()["count"] == 41


def test_batch_bad_resolution_id_rolls_back(stations):
    a = stations["alpha"]
    # 载荷能通过模型校验，但 id 与 station_id 不符 -> 业务层 400 并整批回滚
    bad_res = {
        "id": "forged-id", "record_id": "R001", "station_id": "bravo",
        "choice": "delete", "count": None, "vv": {"bravo:r": 1},
        "read_max_vv": [{"bravo": 1}], "created_at": "2026-01-01T00:00:00+00:00",
    }
    before = a.get("/status").json()
    r = a.post("/sync/batch", json={"resolutions": [bad_res]})
    assert r.status_code == 400
    assert a.get("/status").json()["resolution_count"] == before["resolution_count"]


def test_out_of_order_delivery_converges(stations):
    """乱序投递：先到的操作不会破坏最终对账结果。"""
    a, b = stations["alpha"], stations["bravo"]
    a.post("/records/R001/corrections", json={"count": 10})
    a.post("/records/R001/corrections", json={"count": 20})
    a.post("/records/R002/corrections", json={"count": 30})
    b.post("/records/R001/corrections", json={"count": 40})

    batch = a.get("/sync/pull").json()
    ops = batch["ops"]
    assert len(ops) == 3
    # 逆序 + 打乱记录顺序投递
    shuffled = list(reversed(ops))
    r = b.post("/sync/batch", json={"ops": shuffled})
    assert r.status_code == 200
    assert r.json()["applied_ops"] == 3
    rec = b.get("/records/R001").json()
    assert rec["conflicted"] is True  # 20 与 40 并发冲突
    # 重复投递全部幂等
    r2 = b.post("/sync/batch", json={"ops": shuffled})
    assert r2.json()["applied_ops"] == 0
    assert r2.json()["duplicate_ops"] == 3


def test_interrupted_sync_retry_converges(stations):
    """模拟中断：每次只投递一个操作，多次重试后两站状态与一次全量同步相同。"""
    a, b = stations["alpha"], stations["bravo"]
    for rid, cnt in [("R001", 1), ("R002", 2), ("R003", 3), ("R004", 4)]:
        a.post(f"/records/{rid}/corrections", json={"count": cnt})
    b.post("/records/R001/corrections", json={"count": 9})

    full = a.get("/sync/pull").json()
    # 分批、交错、重复地投递
    for op in full["ops"][:2]:
        assert b.post("/sync/batch", json={"ops": [op]}).status_code == 200
    for op in full["ops"]:  # 从头重试，含重复
        assert b.post("/sync/batch", json={"ops": [op]}).status_code == 200
    for op in reversed(full["ops"][2:]):
        assert b.post("/sync/batch", json={"ops": [op]}).status_code == 200

    a.post("/sync/run")
    b.post("/sync/run")
    assert a.get("/status").json()["version_vv"] == b.get("/status").json()["version_vv"]
    assert a.get("/records").json() == b.get("/records").json()


def test_persistence_across_restart(stations):
    a = stations["alpha"]
    a.post("/records/R001/corrections", json={"count": 77})
    a.delete("/records/R006")
    before = a.get("/records").json()
    status_before = a.get("/status").json()

    # 用同一数据卷重新创建应用，模拟容器重启
    restarted = create_app(
        "alpha", stations["alpha_db"], CSV_PATH, peer_url="http://bravo.local"
    )
    with TestClient(restarted) as rc:
        assert rc.get("/records").json() == before
        assert rc.get("/status").json() == status_before
        assert rc.get("/records/R006").json()["deleted"] is True
        assert rc.get("/records/R001").json()["count"] == 77
        # 重启后序号继续递增，不与历史操作冲突
        r = rc.post("/records/R002/corrections", json={"count": 5})
        assert r.status_code == 201
        assert r.json()["op_id"] == "alpha-3"


def test_station_id_mismatch_on_volume_refuses_boot(stations):
    import sqlite3

    # bravo 试图挂载 alpha 的数据卷启动
    with pytest.raises(RuntimeError):
        from app import db as dbmod
        conn = dbmod.connect(stations["alpha_db"])
        try:
            dbmod.init_db(conn, "bravo", CSV_PATH)
        finally:
            conn.close()


# --------------------------------------------------------------------------- 裁定同步收敛


def test_concurrent_resolutions_deterministic_winner(stations):
    """两站分别裁定同一冲突，同步后保留确定性的同一条裁定。"""
    a, b = stations["alpha"], stations["bravo"]
    a.post("/records/R001/corrections", json={"count": 50})
    b.post("/records/R001/corrections", json={"count": 70})
    a.post("/sync/run")
    b.post("/sync/run")

    ra = a.post("/conflicts/R001/resolve", json={"choice": "count", "count": 50})
    rb = b.post("/conflicts/R001/resolve", json={"choice": "count", "count": 70})
    assert ra.status_code == rb.status_code == 201

    a.post("/sync/run")
    b.post("/sync/run")
    a.post("/sync/run")

    # 两站当前生效的裁定 id 与记录状态完全一致
    ca = a.get("/conflicts").json()
    cb = b.get("/conflicts").json()
    assert ca == cb
    assert a.get("/records").json() == b.get("/records").json()
    # 裁定日志在两站都完整保留（只追加），但每记录只有一条生效
    assert a.get("/status").json()["resolution_count"] == b.get(
        "/status"
    ).json()["resolution_count"]


def test_resolution_sync_before_conflict_ops(stations):
    """裁定相关日志先于部分操作到达（乱序），操作补齐后对账仍正确。"""
    a, b = stations["alpha"], stations["bravo"]
    a.post("/records/R001/corrections", json={"count": 50})
    b.post("/records/R001/corrections", json={"count": 70})
    a.post("/sync/run")
    res = a.post(
        "/conflicts/R001/resolve", json={"choice": "count", "count": 50}
    ).json()

    # bravo 清库重来，只接收裁定与 alpha 操作、暂不接收自己的历史操作
    fresh_dir = tempfile.mkdtemp(prefix="bird-bravo-fresh-")
    try:
        fresh_db = os.path.join(fresh_dir, "bravo.db")
        fresh = create_app("bravo", fresh_db, CSV_PATH)
        with TestClient(fresh) as fc:
            alpha_batch = a.get("/sync/pull").json()
            # 只投裁定 + alpha 的操作
            r = fc.post(
                "/sync/batch",
                json={
                    "ops": [o for o in alpha_batch["ops"] if o["station_id"] == "alpha"],
                    "resolutions": alpha_batch["resolutions"],
                },
            )
            assert r.status_code == 200
            # 此时无冲突（只有 alpha 一条分支），记录正常显示 50
            assert fc.get("/records/R001").json()["conflicted"] is False
            assert fc.get("/records/R001").json()["count"] == 50
            # 补齐 bravo 自己的并发操作后，分支集合恰为裁定读取的集合：裁定正常生效
            r2 = fc.post(
                "/sync/batch",
                json={"ops": [o for o in alpha_batch["ops"] if o["station_id"] == "bravo"]},
            )
            assert r2.status_code == 200
            assert fc.get("/conflicts").json() == []
            rec = fc.get("/records/R001").json()
            assert rec["conflicted"] is False and rec["count"] == 50
    finally:
        shutil.rmtree(fresh_dir, ignore_errors=True)


# --------------------------------------------------------------------------- 缺件同步


def _ops_by_id(client, **params):
    return {o["id"]: o for o in client.get("/sync/pull", params=params).json()["ops"]}


def test_missing_first_op_holds_clock_and_reports_gap(stations):
    """先到第 2 条、第 1 条缺件：时钟不得虚高，缺口持久化，旧数据仍可分析。"""
    a, b = stations["alpha"], stations["bravo"]
    b.post("/records/R001/corrections", json={"count": 70})   # bravo-1
    b.post("/records/R002/corrections", json={"count": 80})   # bravo-2
    ops = _ops_by_id(b)

    r = a.post("/sync/batch", json={"ops": [ops["bravo-2"]]})
    assert r.status_code == 200
    st = a.get("/status").json()
    # 缺件未补齐：向量时钟不能被消息携带的时钟带到 bravo:2
    assert st["version_vv"] == {}
    assert st["inbound_watermarks"].get("bravo", {}).get("op", 0) == 0
    assert st["inbound_gaps"] == {"bravo": {"op": [2]}}
    # 已到的第 2 条仍参与本地对账（R002 显示新值且无冲突），但 R001 保持种子旧值
    assert a.get("/records/R002").json()["count"] == 80
    assert a.get("/records/R001").json()["count"] == 32
    # 旧计数 R001 未被删除/冲突，分析仍可取；R002 也仍可取（尚未出现并发分支）
    ids = {x["record_id"] for x in a.get("/analysis/counts").json()}
    assert {"R001", "R002"} <= ids


def test_gap_fill_via_sync_run_makes_conflict_and_excludes_analysis(stations):
    """缺件期间允许本地更正；补齐后不同计数按原规则列冲突并从分析排除。"""
    a, b = stations["alpha"], stations["bravo"]
    b.post("/records/R001/corrections", json={"count": 70})   # bravo-1（缺件）
    b.post("/records/R002/corrections", json={"count": 80})   # bravo-2（先到）
    ops = _ops_by_id(b)
    a.post("/sync/batch", json={"ops": [ops["bravo-2"]]})

    # 缺件窗口内 alpha 对同一记录 R002 做本地更正
    r = a.post("/records/R002/corrections", json={"count": 11})
    assert r.status_code == 201

    # 一键同步自动补齐缺的 bravo-1，并交换 alpha 的本地更正
    r = a.post("/sync/run")
    assert r.status_code == 200, r.text
    st = a.get("/status").json()
    assert st["inbound_gaps"] == {}
    assert st["version_vv"] == {"alpha": 1, "bravo": 2}

    # R002：alpha 的 11 与 bravo 的 80 并发 -> 冲突，分析排除，且不被任一方覆盖
    rec = a.get("/records/R002").json()
    assert rec["conflicted"] is True
    assert {br["count"] for br in a.get("/conflicts").json()[0]["branches"]} == {11, 80}
    assert "R002" not in {x["record_id"] for x in a.get("/analysis/counts").json()}
    # R001：缺件补齐后正常收敛为 70，无冲突
    r1 = a.get("/records/R001").json()
    assert r1["count"] == 70 and r1["conflicted"] is False

    # 两站收齐相同日志后记录与冲突完全一致
    b.post("/sync/run")
    assert a.get("/records").json() == b.get("/records").json()
    assert a.get("/conflicts").json() == b.get("/conflicts").json()


def test_missing_delete_delivered_later_is_conflict_not_overwritten(stations):
    """缺件是一条删除：补齐后与缺件期间的本地更正冲突，不会被静默覆盖。"""
    a, b = stations["alpha"], stations["bravo"]
    b.delete("/records/R003")                                   # bravo-1 删除（缺件）
    b.post("/records/R004/corrections", json={"count": 44})    # bravo-2（先到）
    ops = _ops_by_id(b)
    a.post("/sync/batch", json={"ops": [ops["bravo-2"]]})

    # 缺件期间 alpha 在将被删除的 R003 上本地更正
    a.post("/records/R003/corrections", json={"count": 33})
    a.post("/sync/run")

    rec = a.get("/records/R003").json()
    assert rec["conflicted"] is True
    conflict = next(c for c in a.get("/conflicts").json() if c["record_id"] == "R003")
    assert {br["type"] for br in conflict["branches"]} == {"count", "delete"}
    assert "R003" not in {x["record_id"] for x in a.get("/analysis/counts").json()}


def test_gap_survives_restart_and_fills_after_boot(stations):
    """重启后缺口与水位仍在，继续同步可补齐缺件（含删除），两站收敛。"""
    a = stations["alpha"]
    b_app = stations["bravo_app"]
    b = stations["bravo"]
    b.delete("/records/R003")                                   # bravo-1
    b.post("/records/R004/corrections", json={"count": 44})    # bravo-2
    ops = _ops_by_id(b)
    a.post("/sync/batch", json={"ops": [ops["bravo-2"]]})
    assert a.get("/status").json()["inbound_gaps"] == {"bravo": {"op": [2]}}

    # 用同一数据卷重启 alpha
    restarted = create_app(
        "alpha", stations["alpha_db"], CSV_PATH, peer_url="http://bravo.local"
    )
    with TestClient(restarted) as rc:
        restarted.state.peer_transport = httpx.ASGITransport(app=b_app)
        st = rc.get("/status").json()
        # 缺口持久化、时钟仍未虚高
        assert st["inbound_gaps"] == {"bravo": {"op": [2]}}
        assert st["version_vv"] == {}

        r = rc.post("/sync/run")
        assert r.status_code == 200, r.text
        assert rc.get("/status").json()["inbound_gaps"] == {}
        assert rc.get("/records/R003").json()["deleted"] is True
        assert rc.get("/records/R004").json()["count"] == 44

        # 补齐后再次同步：已收的更正/删除不重复传输
        again = rc.post("/sync/run").json()
        assert again["pulled_ops"] == 0 and again["pushed_ops"] == 0


def test_failed_batch_leaves_no_progress_and_no_data(stations):
    """批次失败整批回滚：不写入任何操作，也不推进收件水位/缺口。"""
    a = stations["alpha"]
    before = a.get("/status").json()
    good = {
        "id": "bravo-7", "record_id": "R001", "station_id": "bravo",
        "seq": 7, "type": "count", "count": 5,
        "vv": {"bravo": 7}, "created_at": "2026-01-01T00:00:00+00:00",
    }
    bad = dict(good, id="bravo-9", seq=9, vv={"bravo": 1})  # 分量与 seq 不符
    r = a.post("/sync/batch", json={"ops": [good, bad]})
    assert r.status_code == 400
    after = a.get("/status").json()
    assert after["op_count"] == before["op_count"]
    assert after["inbound_watermarks"] == before["inbound_watermarks"]
    assert after["inbound_gaps"] == before["inbound_gaps"]
    assert after["version_vv"] == before["version_vv"]


def test_duplicate_and_out_of_order_delivery_never_duplicates(stations):
    """乱序 + 重复投递：缺口表与全局 ID 双重幂等，不重复入库，补齐后只算一次。"""
    a, b = stations["alpha"], stations["bravo"]
    b.post("/records/R001/corrections", json={"count": 70})  # bravo-1
    b.post("/records/R002/corrections", json={"count": 80})  # bravo-2
    b.post("/records/R003/corrections", json={"count": 90})  # bravo-3
    ops = _ops_by_id(b)

    # 先乱序投 3、再投 3（重复）、再投 2，最后投 1 补齐
    assert a.post("/sync/batch", json={"ops": [ops["bravo-3"]]}).status_code == 200
    dup = a.post("/sync/batch", json={"ops": [ops["bravo-3"]]}).json()
    assert dup["applied_ops"] == 0 and dup["duplicate_ops"] == 1
    a.post("/sync/batch", json={"ops": [ops["bravo-2"]]})
    st = a.get("/status").json()
    assert st["inbound_gaps"] == {"bravo": {"op": [2, 3]}}
    assert st["version_vv"] == {}  # 两件都在缺件窗口，时钟不动

    fill = a.post("/sync/batch", json={"ops": [ops["bravo-1"]]}).json()
    assert fill["applied_ops"] == 1  # 2、3 已在库，只新入 1
    st = a.get("/status").json()
    assert st["inbound_gaps"] == {}
    assert st["version_vv"] == {"bravo": 3}
    assert a.get("/status").json()["op_count"] == 3
    # 整批重放全部幂等
    replay = a.post("/sync/batch", json={"ops": list(ops.values())}).json()
    assert replay["applied_ops"] == 0 and replay["duplicate_ops"] == 3


def test_watermark_pull_does_not_resend_received(stations):
    """按收件水位拉取：已连续收齐的不再发送，缺口件在补齐前持续可补拉。"""
    a, b = stations["alpha"], stations["bravo"]
    b.post("/records/R001/corrections", json={"count": 70})  # bravo-1
    b.post("/records/R002/corrections", json={"count": 80})  # bravo-2

    # alpha 水位为 0：两件都应返回
    first = b.get(
        "/sync/pull", params={"watermarks": '{"bravo": {"op": 0}}'}
    ).json()
    assert {o["id"] for o in first["ops"]} == {"bravo-1", "bravo-2"}

    # 仅收到 bravo-2（缺口），水位仍为 0：补齐前 bravo-1 必须仍可拉取
    a.post("/sync/batch", json={"ops": [o for o in first["ops"] if o["id"] == "bravo-2"]})
    wm = a.get("/status").json()["inbound_watermarks"]
    second = b.get("/sync/pull", params={"watermarks": json.dumps(wm)}).json()
    assert {o["id"] for o in second["ops"]} == {"bravo-1", "bravo-2"}

    # 补齐后水位到 3：再拉为空，不重复传已收更正
    a.post("/sync/batch", json={"ops": [o for o in first["ops"] if o["id"] == "bravo-1"]})
    wm = a.get("/status").json()["inbound_watermarks"]
    third = b.get("/sync/pull", params={"watermarks": json.dumps(wm)}).json()
    assert third["ops"] == [] and third["resolutions"] == []


def test_gap_with_resolution_watermark_tracks_independently(stations):
    """裁定序号与操作序号分别记水位；缺件的裁定补齐前不推进 :r 分量。"""
    a, b = stations["alpha"], stations["bravo"]
    a.post("/records/R001/corrections", json={"count": 50})
    b.post("/records/R001/corrections", json={"count": 70})
    a.post("/sync/run")
    res = a.post(
        "/conflicts/R001/resolve", json={"choice": "count", "count": 70}
    ).json()
    assert res["resolution_id"] == "alpha-r1"
    st = a.get("/status").json()
    # 本站裁定水位独立登记：水位是“下一应收序号”，收到 r1 后为 2；时钟分量为 1
    assert st["inbound_watermarks"]["alpha"]["resolve"] == 2
    assert st["version_vv"]["alpha:r"] == 1

    # bravo 全量拉取包含该裁定，应用后裁定水位与操作水位分别就位
    batch = a.get("/sync/pull").json()
    r = b.post("/sync/batch", json=batch)
    assert r.status_code == 200
    bst = b.get("/status").json()
    assert bst["inbound_watermarks"]["alpha"]["resolve"] == 2
    assert bst["inbound_watermarks"]["alpha"]["op"] >= 1
    assert bst["version_vv"]["alpha:r"] == 1


def test_legacy_volume_backfills_watermarks_and_holds_clock_on_gap(stations):
    """旧版本数据卷（无收件表）升级：回填水位/缺口，缺件不被旧时钟跳过。"""
    import sqlite3

    legacy = os.path.join(stations["tmp"], "legacy-alpha.db")
    conn = sqlite3.connect(legacy)
    conn.executescript(
        """
        CREATE TABLE meta (id INTEGER PRIMARY KEY CHECK(id=1), station_id TEXT,
            vv TEXT, seq INTEGER, res_seq INTEGER);
        CREATE TABLE records (record_id TEXT PRIMARY KEY, site TEXT, species TEXT,
            seed_count INTEGER, count INTEGER, deleted INTEGER DEFAULT 0,
            version_vv TEXT, conflicted INTEGER DEFAULT 0, has_resolution INTEGER DEFAULT 0);
        CREATE TABLE ops (id TEXT PRIMARY KEY, record_id TEXT, station_id TEXT,
            seq INTEGER, type TEXT, count INTEGER, vv TEXT, created_at TEXT);
        CREATE TABLE resolutions (id TEXT PRIMARY KEY, record_id TEXT, station_id TEXT,
            choice TEXT, count INTEGER, vv TEXT, read_max_vv TEXT, created_at TEXT);
        """
    )
    # 旧库时钟被错误地并到 bravo:2，但 bravo-1 实际缺件
    conn.execute("INSERT INTO meta VALUES (1,'alpha','{\"bravo\":2}',1,0)")
    conn.execute(
        "INSERT INTO records VALUES ('R001','北岸滩涂','黑腹滨鹬',32,32,0,NULL,0,0)"
    )
    conn.execute(
        "INSERT INTO ops VALUES ('bravo-2','R001','bravo',2,'count',80,"
        "'{\"bravo\":2}','2026-01-01T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO ops VALUES ('alpha-1','R001','alpha',1,'count',11,"
        "'{\"alpha\":1}','2026-01-01T00:00:01+00:00')"
    )
    conn.commit()
    conn.close()

    upgraded = create_app("alpha", legacy, CSV_PATH, peer_url="http://bravo.local")
    with TestClient(upgraded) as uc:
        st = uc.get("/status").json()
        # bravo 连续水位停在 1（缺 bravo-1），已到的 2 进缺口；alpha 连续到 2
        assert st["inbound_watermarks"] == {
            "bravo": {"op": 1},
            "alpha": {"op": 2},
        }
        assert st["inbound_gaps"] == {"bravo": {"op": [2]}}
        # 重建后的时钟不再虚高：没有 bravo 分量
        assert st["version_vv"] == {"alpha": 1}
    # 重新打开同一卷，水位保持（重启幂等）
    again = create_app("alpha", legacy, CSV_PATH, peer_url="http://bravo.local")
    with TestClient(again) as ac2:
        st2 = ac2.get("/status").json()
        assert st2["inbound_gaps"] == {"bravo": {"op": [2]}}
        assert st2["version_vv"] == {"alpha": 1}
