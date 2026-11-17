"""重算引擎。

两种模式（创建作业时选定并永久留存在 recalc_jobs.mode）：
- full_history : 对历史所有窗口按当前知识重算。原始告警事实行不动，
  重算结论写入 alert_analyses 的新版本（stands/withdrawn/maintenance/reassigned），
  页面可同时查看 v1 当时依据与最新更正分析版。
- forward_only : 只处理 effective 边界之后的窗口（基线变更的标准用法），
  历史窗口完全不碰。

可靠性：
- 每个窗口完成即写 recalc_progress；进程被杀后 resume 跳过已完成窗口。
- 告警由 alerts.fingerprint 唯一约束兜底，通知由 outbox 唯一约束兜底，
  因此恢复后绝不重复建告警、绝不重复发通知。
"""
from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import timedelta
from typing import Optional

from . import alerts as al
from . import baseline as bl
from . import repository as repo
from .db import utcnow
from .stats import aggregate_window, upsert_window_stat
from .timeutils import fmt_ts, parse_ts


def create_job(conn: sqlite3.Connection, mode: str,
               scope_sensors: Optional[list[str]] = None,
               range_start: Optional[str] = None,
               range_end: Optional[str] = None,
               reason: str = "") -> str:
    assert mode in ("full_history", "forward_only")
    job_id = "job_" + uuid.uuid4().hex[:12]
    conn.execute(
        "INSERT INTO recalc_jobs(job_id, mode, scope_sensors, range_start, range_end,"
        " status, reason, created_at) VALUES (?,?,?,?,?, 'pending', ?, ?)",
        (job_id, mode, json.dumps(scope_sensors or []), range_start, range_end,
         reason, utcnow()),
    )
    return job_id


def _list_sensors(conn: sqlite3.Connection, scope: list[str]) -> list[str]:
    if scope:
        return scope
    return [r["sensor_id"] for r in
            conn.execute("SELECT sensor_id FROM sensors ORDER BY sensor_id")]


def _window_plan(conn: sqlite3.Connection, job: sqlite3.Row) -> list[tuple[str, str, str]]:
    """返回 (sensor_id, window_start, window_end) 计划清单。

    只枚举实际存在测点的整点窗口，避免对大量空窗口做无效聚合。
    """
    scope = json.loads(job["scope_sensors"])
    sensors = _list_sensors(conn, scope)
    r_start = job["range_start"]
    r_end = job["range_end"]
    if r_start is None:
        row = conn.execute("SELECT MIN(ts) AS m FROM raw_points").fetchone()
        r_start = row["m"]
    if r_end is None:
        # 默认覆盖到数据末端，而非真实时钟：合成/补录数据的时间轴可能
        # 与墙钟不一致（演示锚定虚拟"现在"，真实系统也可能批量导入历史数据）
        row = conn.execute("SELECT MAX(ts) AS m FROM raw_points").fetchone()
        r_end = row["m"] or utcnow()

    plan: list[tuple[str, str, str]] = []
    for sid in sensors:
        rows = conn.execute(
            "SELECT DISTINCT substr(ts,1,13) AS hour_key FROM raw_points"
            " WHERE sensor_id=? AND ts>=? AND ts<? ORDER BY hour_key",
            (sid, r_start, r_end)).fetchall()
        for r in rows:
            # substr 截到 'YYYY-MM-DDTHH'，补成完整整点 ISO 时间
            ws = parse_ts(r["hour_key"] + ":00:00.000000Z")
            if job["mode"] == "forward_only":
                # 仅处理作业创建时刻仍在未来的整点窗口
                boundary = parse_ts(utcnow()).replace(minute=0, second=0, microsecond=0)
                if ws < boundary:
                    continue
            plan.append((sid, fmt_ts(ws), fmt_ts(ws + timedelta(hours=1))))
    return plan


def run_job(conn: sqlite3.Connection, job_id: str,
            max_windows: Optional[int] = None) -> dict:
    """执行（或恢复执行）一个作业。max_windows 便于演示中断/分批。"""
    job = conn.execute("SELECT * FROM recalc_jobs WHERE job_id=?", (job_id,)).fetchone()
    if job is None:
        raise KeyError(job_id)
    if job["status"] == "done":
        return {"job_id": job_id, "status": "done", "skipped": True}

    conn.execute(
        "UPDATE recalc_jobs SET status='running', started_at=COALESCE(started_at, ?),"
        " heartbeat_at=?, worker_pid=? WHERE job_id=?",
        (utcnow(), utcnow(), os.getpid(), job_id))
    conn.commit()

    plan = _window_plan(conn, job)
    conn.execute("UPDATE recalc_jobs SET total_windows=? WHERE job_id=?",
                 (len(plan), job_id))

    done_keys = {r["window_key"] for r in conn.execute(
        "SELECT window_key FROM recalc_progress WHERE job_id=?", (job_id,))}

    processed = 0
    for sid, ws, we in plan:
        key = f"{sid}|{ws}"
        if key in done_keys:
            continue
        _process_one(conn, job, sid, ws, we)
        conn.execute(
            "INSERT OR REPLACE INTO recalc_progress(job_id, window_key, state, at)"
            " VALUES (?,?,?,?)",
            (job_id, key, "done", utcnow()))
        conn.execute(
            "UPDATE recalc_jobs SET done_windows=done_windows+1, heartbeat_at=?"
            " WHERE job_id=?", (utcnow(), job_id))
        conn.commit()
        processed += 1
        if max_windows is not None and processed >= max_windows:
            conn.execute("UPDATE recalc_jobs SET status='paused' WHERE job_id=?",
                         (job_id,))
            conn.commit()
            return {"job_id": job_id, "status": "paused", "processed": processed}

    conn.execute("UPDATE recalc_jobs SET status='done', finished_at=? WHERE job_id=?",
                 (utcnow(), job_id))
    conn.commit()
    return {"job_id": job_id, "status": "done", "processed": processed}


def _process_one(conn: sqlite3.Connection, job: sqlite3.Row,
                 sid: str, ws: str, we: str) -> None:
    agg = aggregate_window(conn, sid, parse_ts(ws), parse_ts(we))
    if agg is None:
        return
    hour = parse_ts(ws).hour

    if job["mode"] == "full_history":
        _full_history_window(conn, job, agg, sid, hour, ws)
    else:  # forward_only：走与实时完全相同的处理路径
        al.process_window(conn, agg, job_id=job["job_id"])


def _full_history_window(conn: sqlite3.Connection, job: sqlite3.Row,
                         agg: dict, sid: str, hour: int, ws: str) -> None:
    """全历史重算：刷新物化统计；对该窗口的既有告警产出更正分析版，
    绝不删除/覆盖原始 alerts 行，也不重发通知。"""
    upsert_window_stat(conn, agg, job["job_id"])
    current_base = bl.active_baseline(conn, sid, hour, ws)
    verdict_now = bl.evaluate(agg, current_base)

    old_alerts = conn.execute(
        "SELECT * FROM alerts WHERE sensor_id=? AND window_start=?", (sid, ws)
    ).fetchall()

    for a in old_alerts:
        # 用当前基线/当前归属/当前维护视图给出更正结论
        if agg["in_maintenance"]:
            new_verdict, rationale = "maintenance", "按当前维护记录，该窗口处于维护期"
        elif agg["org_id"] != a["org_id"]:
            new_verdict, rationale = "reassigned", \
                f"归属已由 {a['org_id']} 更正为 {agg['org_id']}"
        elif not verdict_now["alert"]:
            new_verdict, rationale = "withdrawn", \
                f"按当前基线重算不再越限（{verdict_now['reason']}）"
        else:
            new_verdict, rationale = "stands", "按当前基线重算仍成立"

        _next_analysis(conn, a["alert_id"], job["job_id"], current_base,
                       verdict_now, new_verdict, rationale)

    # 重算中新发现的告警（历史上没发过）：作为告警落库，但标记来源为重算作业，
    # 且不进通知发件箱 —— 历史告警补发通知会造成重复/误导。
    if verdict_now["alert"] and not old_alerts:
        al.emit_alert(conn, agg, verdict_now, job_id=job["job_id"],
                      send_notification=False)


def _next_analysis(conn: sqlite3.Connection, alert_id: int, job_id: str,
                   base: Optional[sqlite3.Row], verdict: dict,
                   new_verdict: str, rationale: str) -> None:
    last = conn.execute(
        "SELECT MAX(version) AS v FROM alert_analyses WHERE alert_id=?",
        (alert_id,)).fetchone()
    version = (last["v"] or 1) + 1
    threshold = verdict.get("threshold") or {}
    conn.execute(
        "INSERT INTO alert_analyses(alert_id, version, mode, recalc_job_id,"
        " baseline_id_used, threshold_snapshot, verdict, rationale, computed_at)"
        " VALUES (?,?, 'full_recalc', ?, ?, ?, ?, ?, ?)",
        (alert_id, version, job_id,
         base["baseline_id"] if base else None,
         json.dumps(threshold, ensure_ascii=False, sort_keys=True),
         new_verdict, rationale, utcnow()),
    )


def job_status(conn: sqlite3.Connection, job_id: str) -> dict:
    row = conn.execute("SELECT * FROM recalc_jobs WHERE job_id=?", (job_id,)).fetchone()
    if row is None:
        raise KeyError(job_id)
    return dict(row)
