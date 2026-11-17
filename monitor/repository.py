"""双时态仓储：归属、维护窗口、原始点的 as-of 查询与追溯修订。

约定：
- as_of=None 表示"现在"（当前断言视图）；给定时点则重现该时点系统所知的世界。
- 任何更正都不 UPDATE/DELETE 旧行：旧行置 superseded_at，新行带 corrected/prev 链。
"""
from __future__ import annotations

import sqlite3
from typing import Optional

from .db import utcnow
from .timeutils import fmt_ts, parse_ts


# ---------------- 测点归属 ----------------

def assign_ownership(conn: sqlite3.Connection, sensor_id: str, org_id: str,
                     valid_from: str, valid_to: Optional[str],
                     reason: str = "") -> int:
    """首次/新一段归属断言。与同一传感器时间区间重叠的旧断言不被物理删除，
    追溯更正请用 correct_ownership。"""
    cur = conn.execute(
        "INSERT INTO sensor_ownership(sensor_id, org_id, valid_from, valid_to,"
        " recorded_at, reason) VALUES (?,?,?,?,?,?)",
        (sensor_id, org_id, valid_from, valid_to, utcnow(), reason),
    )
    return cur.lastrowid


def correct_ownership(conn: sqlite3.Connection, own_id: int, new_org_id: str,
                      reason: str) -> int:
    """追溯修订：复制旧断言为新行（指向新组织），旧断言置 superseded。"""
    old = conn.execute("SELECT * FROM sensor_ownership WHERE own_id=?", (own_id,)).fetchone()
    if old is None:
        raise KeyError(f"ownership {own_id} not found")
    now = utcnow()
    conn.execute("UPDATE sensor_ownership SET superseded_at=? WHERE own_id=?",
                 (now, own_id))
    cur = conn.execute(
        "INSERT INTO sensor_ownership(sensor_id, org_id, valid_from, valid_to,"
        " recorded_at, superseded_at, reason, prev_own_id)"
        " VALUES (?,?,?,?,?,NULL,?,?)",
        (old["sensor_id"], new_org_id, old["valid_from"], old["valid_to"],
         now, f"CORRECTION: {reason}", own_id),
    )
    return cur.lastrowid


def org_at(conn: sqlite3.Connection, sensor_id: str, t: str,
           as_of: Optional[str] = None) -> Optional[str]:
    """某测点在业务时间 t 的归属，按 as_of 时点的知识解析。"""
    as_of = as_of or utcnow()
    row = conn.execute(
        "SELECT org_id FROM sensor_ownership"
        " WHERE sensor_id=? AND valid_from<=?"
        "   AND (valid_to IS NULL OR valid_to>?)"
        "   AND recorded_at<=?"
        "   AND (superseded_at IS NULL OR superseded_at>?)"
        " ORDER BY recorded_at DESC, own_id DESC LIMIT 1",
        (sensor_id, t, t, as_of, as_of),
    ).fetchone()
    return row["org_id"] if row else None


def ownership_history(conn: sqlite3.Connection, sensor_id: str):
    """完整断言链（含已被更正的旧行），供审计页展示。"""
    return conn.execute(
        "SELECT * FROM sensor_ownership WHERE sensor_id=?"
        " ORDER BY valid_from, recorded_at", (sensor_id,)).fetchall()


# ---------------- 维护窗口 ----------------

def add_maintenance(conn: sqlite3.Connection, sensor_id: str, start_ts: str,
                    end_ts: str, reason: str = "", prev_mw_id: Optional[int] = None,
                    corrected_mw: Optional[int] = None) -> int:
    cur = conn.execute(
        "INSERT INTO maintenance_windows(sensor_id, start_ts, end_ts, reason,"
        " recorded_at, corrected_mw, prev_mw_id) VALUES (?,?,?,?,?,?,?)",
        (sensor_id, start_ts, end_ts, reason, utcnow(), corrected_mw, prev_mw_id),
    )
    return cur.lastrowid


def correct_maintenance(conn: sqlite3.Connection, mw_id: int,
                        start_ts: str, end_ts: str, reason: str) -> int:
    old = conn.execute("SELECT * FROM maintenance_windows WHERE mw_id=?", (mw_id,)).fetchone()
    if old is None:
        raise KeyError(f"maintenance {mw_id} not found")
    now = utcnow()
    conn.execute("UPDATE maintenance_windows SET superseded_at=? WHERE mw_id=?",
                 (now, mw_id))
    return add_maintenance(conn, old["sensor_id"], start_ts, end_ts,
                           f"CORRECTION: {reason}", prev_mw_id=mw_id,
                           corrected_mw=mw_id)


def in_maintenance(conn: sqlite3.Connection, sensor_id: str, win_start: str,
                   win_end: str, as_of: Optional[str] = None) -> Optional[sqlite3.Row]:
    """窗口与任一有效维护窗口相交即视为维护中（返回该维护行）。"""
    as_of = as_of or utcnow()
    return conn.execute(
        "SELECT * FROM maintenance_windows"
        " WHERE sensor_id=? AND start_ts<? AND end_ts>?"
        "   AND recorded_at<=?"
        "   AND (superseded_at IS NULL OR superseded_at>?)"
        " ORDER BY recorded_at DESC LIMIT 1",
        (sensor_id, win_end, win_start, as_of, as_of),
    ).fetchone()


def maintenance_history(conn: sqlite3.Connection, sensor_id: str):
    return conn.execute(
        "SELECT * FROM maintenance_windows WHERE sensor_id=?"
        " ORDER BY start_ts, recorded_at", (sensor_id,)).fetchall()


# ---------------- 原始测点 ----------------

def insert_point(conn: sqlite3.Connection, sensor_id: str, ts: str,
                 value: float, note: str = "") -> int:
    cur = conn.execute(
        "INSERT INTO raw_points(sensor_id, ts, value, recorded_at, note)"
        " VALUES (?,?,?,?,?)",
        (sensor_id, ts, float(value), utcnow(), note),
    )
    return cur.lastrowid


def correct_point(conn: sqlite3.Connection, point_id: int, new_value: float,
                  note: str) -> int:
    """数据更正：旧点置 superseded，插入修正点（同测量时间，新记录时间）。"""
    old = conn.execute("SELECT * FROM raw_points WHERE point_id=?", (point_id,)).fetchone()
    if old is None:
        raise KeyError(f"point {point_id} not found")
    now = utcnow()
    conn.execute("UPDATE raw_points SET superseded_at=? WHERE point_id=?",
                 (now, point_id))
    cur = conn.execute(
        "INSERT INTO raw_points(sensor_id, ts, value, recorded_at, corrected_point, note)"
        " VALUES (?,?,?,?,?,?)",
        (old["sensor_id"], old["ts"], float(new_value), now, point_id,
         f"CORRECTION: {note}"),
    )
    return cur.lastrowid


def points_between(conn: sqlite3.Connection, sensor_id: str, start: str, end: str,
                   as_of: Optional[str] = None) -> list[sqlite3.Row]:
    """取窗口内的点。as_of 给定时重现当时知识（排除之后才入库/更正的版本）。"""
    if as_of is None:
        return conn.execute(
            "SELECT * FROM raw_points WHERE sensor_id=? AND ts>=? AND ts<?"
            " AND superseded_at IS NULL ORDER BY ts",
            (sensor_id, start, end)).fetchall()
    return conn.execute(
        "SELECT * FROM raw_points WHERE sensor_id=? AND ts>=? AND ts<?"
        " AND recorded_at<=?"
        " AND (superseded_at IS NULL OR superseded_at>?)"
        " ORDER BY ts",
        (sensor_id, start, end, as_of, as_of)).fetchall()
