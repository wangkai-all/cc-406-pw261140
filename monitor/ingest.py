"""接入服务：生成虚构时序数据（确定性随机，不接任何真实设备），
批量入库，并对新落入的整点窗口做实时聚合判定。"""
from __future__ import annotations

import math
import random
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

from . import alerts as al
from . import repository as repo
from .timeutils import fmt_ts, hour_floor, parse_ts


# ---------------- 虚构数据生成 ----------------

def synthesize_series(sensor_id: str, start: datetime, hours: float,
                      seed: int, points_per_hour: int = 12,
                      anomaly_windows: Optional[dict[int, str]] = None,
                      drift: float = 0.0) -> list[tuple[str, float, str]]:
    """生成 (ts, value, note) 序列。

    信号 = 日周期正弦 + 线性漂移 + 高斯噪声；anomaly_windows 指定异常小时槽
    （'spike' 尖峰 / 'shift' 均值漂移 / 'flatline' 卡死）。
    """
    rng = random.Random(seed)
    anomaly_windows = anomaly_windows or {}
    out: list[tuple[str, float, str]] = []
    total = int(hours * points_per_hour)
    base = 20 + (seed % 7) * 3
    for i in range(total):
        dt = start.replace(minute=0, second=0, microsecond=0)
        # 用整点偏移 + 槽内偏移推进
        slot = i // points_per_hour
        frac = (i % points_per_hour) / points_per_hour
        from datetime import timedelta
        t = dt + timedelta(hours=slot, minutes=int(frac * 60))
        hour_index = slot
        diurnal = 4 * math.sin(2 * math.pi * t.hour / 24.0)
        noise = rng.gauss(0, 0.6 + 0.1 * (seed % 3))
        value = base + diurnal + drift * hour_index + noise
        note = ""
        kind = anomaly_windows.get(hour_index)
        if kind == "spike":
            if i % points_per_hour in (3, 4, 9):
                value += rng.choice((-1, 1)) * (9 + rng.random() * 4)
                note = "synthetic spike"
        elif kind == "shift":
            value += 8.5
            note = "synthetic level shift"
        elif kind == "flatline":
            value = base
            note = "synthetic flatline"
        out.append((fmt_ts(t), round(value, 3), note))
    return out


def ingest_series(conn: sqlite3.Connection, sensor_id: str,
                  series: list[tuple[str, float, str]]) -> int:
    n = 0
    conn.execute("BEGIN")
    try:
        for ts, value, note in series:
            repo.insert_point(conn, sensor_id, ts, value, note)
            n += 1
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return n


# ---------------- 实时处理 ----------------

def process_new_windows(conn: sqlite3.Connection, sensor_ids: Optional[list[str]] = None,
                        up_to: Optional[str] = None,
                        max_windows: Optional[int] = None) -> list[dict]:
    """扫描所有已闭合整点窗口（窗口内有点且未物化或 digest 过期），逐个处理。

    每测点只做一次范围查询，在内存按整点窗口分组，避免 N 次全表扫描。
    """
    if sensor_ids is None:
        sensor_ids = [r["sensor_id"] for r in
                      conn.execute("SELECT sensor_id FROM sensors ORDER BY sensor_id")]
    boundary = parse_ts(up_to) if up_to else parse_ts(repo.utcnow())
    last_closed = hour_floor(boundary)
    results: list[dict] = []
    for sid in sensor_ids:
        rows = conn.execute(
            "SELECT * FROM raw_points WHERE sensor_id=? AND superseded_at IS NULL"
            " ORDER BY ts", (sid,)).fetchall()
        if not rows:
            continue
        groups: dict[datetime, list] = {}
        for r in rows:
            ws = hour_floor(parse_ts(r["ts"]))
            if ws < last_closed:
                groups.setdefault(ws, []).append(r)
        for ws, points in sorted(groups.items()):
            we = ws + timedelta(hours=1)
            agg = _aggregate_from_rows(conn, sid, ws, we, points)
            if agg is None:
                continue
            existing = conn.execute(
                "SELECT points_digest FROM window_stats WHERE sensor_id=?"
                " AND window_start=?", (sid, fmt_ts(ws))).fetchone()
            if existing is None or existing["points_digest"] != agg["points_digest"]:
                results.append(al.process_window(conn, agg))
                if max_windows and len(results) >= max_windows:
                    return results
    return results


def _aggregate_from_rows(conn, sensor_id, win_start, win_end, points) -> Optional[dict]:
    """与 stats.aggregate_window 相同的产出，但点集由调用方给定（省一次查询）。"""
    from .stats import stats_bundle, points_digest
    if not points:
        return None
    values = [p["value"] for p in points]
    b = stats_bundle(values)
    ws, we = fmt_ts(win_start), fmt_ts(win_end)
    mw = repo.in_maintenance(conn, sensor_id, ws, we)
    org = repo.org_at(conn, sensor_id, ws)
    return {
        "sensor_id": sensor_id,
        "window_start": ws,
        "window_end": we,
        "org_id": org or "UNASSIGNED",
        "n": b["n"],
        "mean": b["mean"],
        "stddev": b["stddev"],
        "median": b["median"],
        "mad": b["mad"],
        "min_value": b["min"],
        "max_value": b["max"],
        "p95": b["p95"],
        "in_maintenance": 1 if mw else 0,
        "maintenance_id": mw["mw_id"] if mw else None,
        "points_digest": points_digest(points),
        "point_ids": [p["point_id"] for p in points],
        "points": [{"point_id": p["point_id"], "ts": p["ts"], "value": p["value"]}
                   for p in points],
    }
