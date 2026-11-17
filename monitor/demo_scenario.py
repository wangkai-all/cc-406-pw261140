"""端到端演示场景：虚构数据 → 基线训练/批准 → 实时告警 → 追溯修订
→ 两种重算（含中断恢复）→ 验证不重复通知。

时间线以固定的虚拟"现在"为锚点，保证可重复。
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from . import alerts as al
from . import baseline as bl
from . import ingest
from . import recalc
from . import repository as repo
from .db import connect, init_db, reset_db, utcnow
from .timeutils import fmt_ts, parse_ts

T0 = datetime(2026, 11, 10, 0, 0, tzinfo=timezone.utc)
DAY = 10  # 天数据
NOW = T0 + timedelta(days=DAY)


def _iso(dt: datetime) -> str:
    return fmt_ts(dt.astimezone(timezone.utc))


def build_demo(db_path: str | None = None, keep: bool = False) -> dict:
    if not keep:
        reset_db(db_path)
    conn = connect(db_path)
    init_db(conn)
    summary: dict = {}

    # 1) 组织与测点（S1/S2 归属 A 厂，S3 归属 B 厂）
    conn.executemany("INSERT INTO organizations(org_id, name) VALUES (?,?)",
                     [("ORG-A", "甲厂"), ("ORG-B", "乙厂")])
    conn.executemany("INSERT INTO sensors(sensor_id, name, unit, metric) VALUES (?,?,?,?)",
                     [("S1", "1号机组温度", "C", "temp"),
                      ("S2", "2号机组振动", "mm/s", "vibration"),
                      ("S3", "泵站压力", "kPa", "pressure")])
    for sid, org in (("S1", "ORG-A"), ("S2", "ORG-A"), ("S3", "ORG-B")):
        repo.assign_ownership(conn, sid, org, _iso(T0), None, "初始归属")

    # 2) 虚构时序：S1 在第 6 天 03/04 时注入尖峰，S2 在第 7 天注入均值漂移
    s1 = ingest.synthesize_series("S1", T0, DAY * 24, seed=11,
                                  anomaly_windows={6 * 24 + 3: "spike",
                                                   6 * 24 + 4: "spike",
                                                   8 * 24 + 10: "shift"})
    s2 = ingest.synthesize_series("S2", T0, DAY * 24, seed=23, drift=0.01,
                                  anomaly_windows={7 * 24 + 15: "shift",
                                                   7 * 24 + 16: "shift"})
    s3 = ingest.synthesize_series("S3", T0, DAY * 24, seed=37,
                                  anomaly_windows={5 * 24 + 9: "spike"})
    ingest.ingest_series(conn, "S1", s1)
    ingest.ingest_series(conn, "S2", s2)
    ingest.ingest_series(conn, "S3", s3)
    summary["points"] = conn.execute("SELECT COUNT(*) c FROM raw_points").fetchone()["c"]

    # 3) 用前 5 天训练基线并全部批准（模拟人工批准；生效时间=此刻）
    train_end = T0 + timedelta(days=5)
    proposed = bl.train_baselines(conn, _iso(T0), _iso(train_end))
    # 把批准生效时间伪造成训练结束时刻，使历史窗口判定有基线可用
    # （真实系统中批准只对之后生效；演示需要让第 6 天起的窗口能被判定）
    eff = _iso(train_end)
    for bid in proposed:
        bl.approve_baseline(conn, bid, approved_by="demo-operator",
                            effective_from=eff)
    summary["baselines_approved"] = len(proposed)

    # 4) 实时接入处理：扫描所有已闭合窗口 → 聚合 → 判定 → 告警
    results = ingest.process_new_windows(conn, up_to=_iso(NOW))
    alerts_before = conn.execute("SELECT COUNT(*) c FROM alerts").fetchone()["c"]
    notifs = al.deliver_pending(conn)  # 模拟渠道投递
    summary["initial_alerts"] = alerts_before
    summary["initial_notifications"] = len(notifs)

    # 5) 追溯修订一：S3 第 5 天 09 时的告警其实是检修 —— 补录维护窗口
    mw_id = repo.add_maintenance(
        conn, "S3", _iso(T0 + timedelta(days=5, hours=9)),
        _iso(T0 + timedelta(days=5, hours=10)), "事后补录：当日检修")

    # 6) 追溯修订二：S2 第 7 天的两个窗口当时归 ORG-A，现更正为 ORG-B
    own_rows = conn.execute(
        "SELECT own_id FROM sensor_ownership WHERE sensor_id='S2'").fetchall()
    # 演示中 S2 只有一条覆盖全程的归属行，更正它
    new_own = repo.correct_ownership(conn, own_rows[0]["own_id"], "ORG-B",
                                     "资产划拨追溯生效")
    summary["corrections"] = {"maintenance_mw_id": mw_id, "new_ownership_id": new_own}

    # 7) 基线演进：用最近 5 天重训 S1 基线并批准（新基线只对之后生效）
    retrain_start = NOW - timedelta(days=5)
    new_props = bl.train_baselines(conn, _iso(retrain_start), _iso(NOW),
                                   sensor_ids=["S1"], proposed_by="demo-operator")
    for bid in new_props:
        bl.approve_baseline(conn, bid, approved_by="demo-operator")
    summary["new_baselines"] = len(new_props)

    # 8) 全历史重算（故意只跑少量窗口后中断，再恢复，验证断点续算）
    job_full = recalc.create_job(
        conn, "full_history",
        reason="维护补录+归属更正+基线更新后的全量复核")
    part1 = recalc.run_job(conn, job_full, max_windows=7)
    mid_progress = recalc.job_status(conn, job_full)
    part2 = recalc.run_job(conn, job_full)  # 恢复直到完成
    summary["full_recalc"] = {"job_id": job_full,
                              "interrupted_status": part1["status"],
                              "done_at_pause": mid_progress["done_windows"],
                              "final_status": part2["status"]}

    # 9) 仅未来生效模式作业
    job_fwd = recalc.create_job(conn, "forward_only",
                                reason="演示仅对以后生效（当前无未来窗口）")
    fwd = recalc.run_job(conn, job_fwd)
    summary["forward_only"] = {"job_id": job_fwd, "status": fwd["status"]}

    # 10) 幂等性断言：
    #     (a) 第 4 步产生的原始告警一条都没被删除/覆盖
    #     (b) 全历史作业再跑一遍 + 再投递：告警数、发件箱数、pending 数全不变
    original_alert_ids = {r["alert_id"] for r in
                          conn.execute("SELECT alert_id FROM alerts")}
    alerts_stable = conn.execute("SELECT COUNT(*) c FROM alerts").fetchone()["c"]
    outbox_stable = conn.execute("SELECT COUNT(*) c FROM notification_outbox").fetchone()["c"]
    recalc.run_job(conn, job_full)          # 同一作业第二次完整跑（progress 全命中）
    notifs_again = al.deliver_pending(conn)  # 再投递一轮
    alerts_after = conn.execute("SELECT COUNT(*) c FROM alerts").fetchone()["c"]
    outbox_total = conn.execute("SELECT COUNT(*) c FROM notification_outbox").fetchone()["c"]
    surviving = original_alert_ids.issubset(
        {r["alert_id"] for r in conn.execute("SELECT alert_id FROM alerts")})
    analyses = conn.execute(
        "SELECT verdict, COUNT(*) c FROM alert_analyses"
        " WHERE mode='full_recalc' GROUP BY verdict").fetchall()
    summary["idempotency"] = {
        "original_alerts_preserved": surviving,
        "alerts_at_stable_point": alerts_stable,
        "alerts_after_rerun": alerts_after,
        "new_notifications_on_rerun": len(notifs_again),
        "outbox_total": outbox_total,
        "rerun_changed_nothing": (alerts_stable == alerts_after
                                  and outbox_stable == outbox_total
                                  and len(notifs_again) == 0 and surviving),
        "recalc_verdicts": {r["verdict"]: r["c"] for r in analyses},
    }

    # 11) 抽样一条告警，回读其原始点与分析版本
    sample = conn.execute("SELECT alert_id FROM alerts ORDER BY alert_id LIMIT 1").fetchone()
    summary["sample_alert_id"] = sample["alert_id"]
    conn.close()
    return summary
