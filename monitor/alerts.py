"""告警引擎。

关键不变量：
1. alerts 行一经写入不可变（阈值/统计全部 JSON 快照，后续基线变化不影响它）。
2. fingerprint 唯一约束保证同一 (传感器, 窗口, 判定依据) 重算/重启不重复建告警。
3. 通知走 outbox，(alert_id, channel) 唯一，任何重跑都不会重复发送。
4. 更正后的看法写入 alert_analyses 新版本，不动原始告警。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Optional

from . import baseline as bl
from .db import utcnow
from .stats import upsert_window_stat


def make_fingerprint(sensor_id: str, window_start: str, rule_kind: str,
                     baseline_id: Optional[int] = None) -> str:
    """幂等键只用稳定业务维度（传感器+窗口+规则），不含基线 id：

    基线演进后重算同一窗口，规则语义未变，应识别为同一条告警而不是新建。
    当时依据的基线 id 仍完整保存在告警行与阈值快照里。
    baseline_id 参数保留仅为调用兼容，不参与指纹。
    """
    raw = f"{sensor_id}|{window_start}|{rule_kind}"
    return hashlib.sha256(raw.encode()).hexdigest()


def stat_snapshot(agg: dict) -> str:
    keys = ("n", "mean", "stddev", "median", "mad", "min_value", "max_value",
            "p95", "in_maintenance", "points_digest", "point_ids")
    return json.dumps({k: agg.get(k) for k in keys if k in agg},
                      ensure_ascii=False, sort_keys=True)


def emit_alert(conn: sqlite3.Connection, agg: dict, verdict: dict,
               job_id: Optional[str] = None,
               send_notification: bool = True) -> Optional[int]:
    """对一个已判定为告警的窗口落库。已存在同指纹告警则返回既有 alert_id。"""
    base_id = verdict["threshold"]["baseline_id"]
    fp = make_fingerprint(agg["sensor_id"], agg["window_start"],
                          verdict["threshold"]["rule_kind"], base_id)
    existing = conn.execute("SELECT alert_id FROM alerts WHERE fingerprint=?",
                            (fp,)).fetchone()
    if existing:
        return existing["alert_id"]

    observed = agg["median"]
    cur = conn.execute(
        "INSERT INTO alerts(sensor_id, org_id, window_start, window_end, rule_kind,"
        " severity, observed_value, baseline_id, threshold_snapshot, stat_snapshot,"
        " fingerprint, source_job_id, triggered_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (agg["sensor_id"], agg["org_id"], agg["window_start"], agg["window_end"],
         verdict["threshold"]["rule_kind"], verdict["severity"], observed, base_id,
         json.dumps(verdict["threshold"], ensure_ascii=False, sort_keys=True),
         stat_snapshot(agg), fp, job_id, utcnow()),
    )
    alert_id = cur.lastrowid
    conn.execute(
        "INSERT INTO alert_status_events(alert_id, from_status, to_status, note, at)"
        " VALUES (?, NULL, 'open', ?, ?)",
        (alert_id, "initial trigger", utcnow()),
    )
    # v1 分析版：即"事件当时依据"
    conn.execute(
        "INSERT INTO alert_analyses(alert_id, version, mode, recalc_job_id,"
        " baseline_id_used, threshold_snapshot, verdict, rationale, computed_at)"
        " VALUES (?,1,'as_occurred',NULL,?,?, 'stands', ?, ?)",
        (alert_id, base_id,
         json.dumps(verdict["threshold"], ensure_ascii=False, sort_keys=True),
         "首次触发时的基线与窗口统计，永久保留", utcnow()),
    )
    if send_notification:
        conn.execute(
            "INSERT OR IGNORE INTO notification_outbox(alert_id, channel, payload,"
            " status, created_at) VALUES (?, 'web', ?, 'pending', ?)",
            (alert_id, json.dumps({
                "alert_id": alert_id, "sensor_id": agg["sensor_id"],
                "org_id": agg["org_id"], "window_start": agg["window_start"],
                "severity": verdict["severity"],
                "z_extreme": verdict.get("z_extreme"),
            }, ensure_ascii=False), utcnow()),
        )
    else:
        # 全历史重算补建的历史告警：同样占住 (alert,channel) 唯一槽位，
        # 但标记 suppressed，任何投递流程都不会发出，也杜绝后续误补发
        conn.execute(
            "INSERT OR IGNORE INTO notification_outbox(alert_id, channel, payload,"
            " status, created_at) VALUES (?, 'web', ?, 'suppressed', ?)",
            (alert_id, json.dumps({
                "alert_id": alert_id, "sensor_id": agg["sensor_id"],
                "org_id": agg["org_id"], "window_start": agg["window_start"],
                "severity": verdict["severity"], "historical": True,
            }, ensure_ascii=False), utcnow()),
        )
    return alert_id


def process_window(conn: sqlite3.Connection, agg: dict,
                   job_id: Optional[str] = None,
                   as_of: Optional[str] = None) -> dict:
    """单窗口完整处理：物化统计 → 取当时有效基线 → 判定 → 必要时落告警。"""
    upsert_window_stat(conn, agg, job_id)
    hour = int(agg["window_start"][11:13])
    if as_of is not None:
        base = bl.baseline_as_known(conn, agg["sensor_id"], hour, as_of)
    else:
        base = bl.active_baseline(conn, agg["sensor_id"], hour, agg["window_start"])
    verdict = bl.evaluate(agg, base)
    result = {"window": agg["window_start"], "sensor": agg["sensor_id"],
              "verdict": verdict["reason"], "severity": verdict.get("severity"),
              "alert_id": None}
    if verdict["alert"]:
        result["alert_id"] = emit_alert(conn, agg, verdict, job_id=job_id)
        result["state"] = "alerted"
    else:
        result["state"] = "done"
    return result


# ---------------- 状态流转（不改事实行） ----------------

def transition_status(conn: sqlite3.Connection, alert_id: int, to_status: str,
                      note: str = "") -> None:
    row = conn.execute(
        "SELECT to_status FROM alert_status_events WHERE alert_id=?"
        " ORDER BY event_id DESC LIMIT 1", (alert_id,)).fetchone()
    from_status = row["to_status"] if row else None
    conn.execute(
        "INSERT INTO alert_status_events(alert_id, from_status, to_status, note, at)"
        " VALUES (?,?,?,?,?)",
        (alert_id, from_status, to_status, note, utcnow()),
    )


def current_status(conn: sqlite3.Connection, alert_id: int) -> str:
    row = conn.execute(
        "SELECT to_status FROM alert_status_events WHERE alert_id=?"
        " ORDER BY event_id DESC LIMIT 1", (alert_id,)).fetchone()
    return row["to_status"] if row else "open"


def deliver_pending(conn: sqlite3.Connection, mark: bool = True) -> list[dict]:
    """模拟通知投递：取出 pending。mark=True 时置 sent（真实系统此处对接渠道）。
    重复调用不会拿到已发送项 —— 重算/重启不重复通知的最后一道闸。"""
    rows = conn.execute(
        "SELECT * FROM notification_outbox WHERE status='pending' ORDER BY notif_id"
    ).fetchall()
    out = []
    for r in rows:
        out.append({"notif_id": r["notif_id"], "alert_id": r["alert_id"],
                    "payload": json.loads(r["payload"])})
        if mark:
            conn.execute(
                "UPDATE notification_outbox SET status='sent', sent_at=?"
                " WHERE notif_id=?", (utcnow(), r["notif_id"]))
            conn.execute(
                "UPDATE alerts SET first_notified_at=? WHERE alert_id=?"
                " AND first_notified_at IS NULL", (utcnow(), r["alert_id"]))
    return out
