"""稳健统计与窗口聚合。阈值不是固定列表，而是每个测点每小时由历史分布
经中位数/MAD 估计、人工批准后形成的基线动态决定。"""
from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from typing import Optional, Sequence

from . import repository as repo
from .db import utcnow
from .timeutils import fmt_ts, parse_ts


# ---------------- 基础统计量 ----------------

def median(xs: Sequence[float]) -> float:
    s = sorted(xs)
    n = len(s)
    if n == 0:
        raise ValueError("empty sample")
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def mad(xs: Sequence[float], med: Optional[float] = None) -> float:
    """中位数绝对偏差；常数 1.4826 使其对正态分布与标准差同尺度。"""
    m = med if med is not None else median(xs)
    return 1.4826 * median([abs(x - m) for x in xs])


def mean_stddev(xs: Sequence[float]) -> tuple[float, float]:
    n = len(xs)
    if n == 0:
        raise ValueError("empty sample")
    mu = sum(xs) / n
    var = sum((x - mu) ** 2 for x in xs) / n  # 总体标准差
    return mu, math.sqrt(var)


def quantile(xs: Sequence[float], q: float) -> float:
    """线性插值分位数（与 numpy 默认 linear 方法一致）。"""
    s = sorted(xs)
    if not s:
        raise ValueError("empty sample")
    if len(s) == 1:
        return s[0]
    pos = (len(s) - 1) * q
    lo = math.floor(pos)
    frac = pos - lo
    if lo + 1 >= len(s):
        return s[-1]
    return s[lo] * (1 - frac) + s[lo + 1] * frac


def stats_bundle(xs: Sequence[float]) -> dict:
    if not xs:
        return {}
    med = median(xs)
    scale = mad(xs, med)
    mu, sd = mean_stddev(xs)
    return {
        "n": len(xs),
        "mean": mu,
        "stddev": sd,
        "median": med,
        "mad": scale,
        "min": min(xs),
        "max": max(xs),
        "p95": quantile(xs, 0.95),
        "iqr_lower": quantile(xs, 0.25),
        "iqr_upper": quantile(xs, 0.75),
    }


# ---------------- 窗口聚合 ----------------

def points_digest(points: Sequence[sqlite3.Row]) -> str:
    """所用原始点的指纹：数据更正后指纹改变，重算可识别需刷新的窗口。"""
    h = hashlib.sha256()
    for p in points:
        h.update(f"{p['point_id']}:{p['ts']}:{p['value']};".encode())
    return h.hexdigest()[:16]


def aggregate_window(conn: sqlite3.Connection, sensor_id: str,
                     win_start, win_end, as_of: Optional[str] = None) -> Optional[dict]:
    """聚合一个小时窗口。返回 None 表示窗口内无点。"""
    ws, we = fmt_ts(win_start), fmt_ts(win_end)
    points = repo.points_between(conn, sensor_id, ws, we, as_of=as_of)
    if not points:
        return None
    values = [p["value"] for p in points]
    b = stats_bundle(values)
    mw = repo.in_maintenance(conn, sensor_id, ws, we, as_of=as_of)
    org = repo.org_at(conn, sensor_id, ws, as_of=as_of)
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


def upsert_window_stat(conn: sqlite3.Connection, agg: dict, job_id: Optional[str]) -> None:
    conn.execute(
        "INSERT INTO window_stats(sensor_id, window_start, window_end, org_id, n,"
        " mean, stddev, median, mad, min_value, max_value, p95, in_maintenance,"
        " points_digest, job_id, computed_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        " ON CONFLICT(sensor_id, window_start) DO UPDATE SET"
        " window_end=excluded.window_end, org_id=excluded.org_id, n=excluded.n,"
        " mean=excluded.mean, stddev=excluded.stddev, median=excluded.median,"
        " mad=excluded.mad, min_value=excluded.min_value, max_value=excluded.max_value,"
        " p95=excluded.p95, in_maintenance=excluded.in_maintenance,"
        " points_digest=excluded.points_digest, job_id=excluded.job_id,"
        " computed_at=excluded.computed_at",
        (agg["sensor_id"], agg["window_start"], agg["window_end"], agg["org_id"],
         agg["n"], agg["mean"], agg["stddev"], agg["median"], agg["mad"],
         agg["min_value"], agg["max_value"], agg["p95"], agg["in_maintenance"],
         agg["points_digest"], job_id, utcnow()),
    )
