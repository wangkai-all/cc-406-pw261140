"""基线计算器：按测点×小时槽，用训练窗内稳健统计生成基线提议；
人工批准后才生效，且只对生效时刻之后的判定起作用（不追溯改写旧告警）。"""
from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from typing import Optional

from . import repository as repo
from .db import utcnow
from .stats import mad, median, mean_stddev, quantile
from .timeutils import fmt_ts, parse_ts

# MAD 退化（近似常量信号，尺度≈0）时的兜底尺度：取均值的 1%，最小 0.1
FLOOR_SCALE_FRACTION = 0.01
FLOOR_SCALE_ABS = 0.1
# 判定倍数（基线批准后固化进 snapshot，可在批准时覆盖）
DEFAULT_Z_WARNING = 3.5
DEFAULT_Z_CRITICAL = 6.0


def _floor_scale(med: float, scale: float) -> float:
    if scale > 1e-12:
        return scale
    return max(abs(med) * FLOOR_SCALE_FRACTION, FLOOR_SCALE_ABS)


def train_baselines(conn: sqlite3.Connection, train_start: str, train_end: str,
                    sensor_ids: Optional[list[str]] = None,
                    job_id: Optional[str] = None,
                    proposed_by: str = "system") -> list[int]:
    """用 [train_start, train_end) 内每小时槽的点训练 24 条基线/测点（有数据的槽）。

    只生成 status='proposed' 行，绝不自动生效。
    """
    if sensor_ids is None:
        sensor_ids = [r["sensor_id"] for r in conn.execute(
            "SELECT sensor_id FROM sensors ORDER BY sensor_id")]

    # 收集 sensor -> hour -> [values]
    buckets: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for sid in sensor_ids:
        rows = repo.points_between(conn, sid, train_start, train_end)
        for r in rows:
            h = parse_ts(r["ts"]).hour
            buckets[sid][h].append(r["value"])

    proposed_ids: list[int] = []
    now = utcnow()
    for sid, hours in buckets.items():
        for hour, values in sorted(hours.items()):
            if len(values) < 3:  # 样本太少不给基线
                continue
            med = median(values)
            scale = _floor_scale(med, mad(values, med))
            mu, sd = mean_stddev(values)
            cur = conn.execute(
                "INSERT INTO baseline_versions(sensor_id, hour_of_day, median, mad,"
                " mean, stddev, iqr_lower, iqr_upper, z_warning, z_critical,"
                " sample_count, train_start, train_end, status, proposed_at,"
                " proposed_by, source_job_id)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, 'proposed', ?, ?, ?)",
                (sid, hour, med, scale, mu, sd, quantile(values, 0.25),
                 quantile(values, 0.75), DEFAULT_Z_WARNING, DEFAULT_Z_CRITICAL,
                 len(values), train_start, train_end, now, proposed_by, job_id),
            )
            proposed_ids.append(cur.lastrowid)
    return proposed_ids


def approve_baseline(conn: sqlite3.Connection, baseline_id: int,
                     approved_by: str = "operator",
                     z_warning: float = DEFAULT_Z_WARNING,
                     z_critical: float = DEFAULT_Z_CRITICAL,
                     effective_from: Optional[str] = None) -> str:
    """批准基线：旧的同槽已批准基线退休，新基线 effective_from=生效时刻。

    z 倍数不入库（schema 保持精简），随每个告警的 threshold_snapshot 固化，
    因此事后调倍数也不会改写历史告警的依据。返回生效时间。
    """
    row = conn.execute("SELECT * FROM baseline_versions WHERE baseline_id=?",
                       (baseline_id,)).fetchone()
    if row is None:
        raise KeyError(f"baseline {baseline_id} not found")
    if row["status"] != "proposed":
        raise ValueError(f"baseline {baseline_id} is {row['status']}, not proposed")
    eff = effective_from or utcnow()
    conn.execute(
        "UPDATE baseline_versions SET status='approved', approved_at=?,"
        " approved_by=?, effective_from=?, z_warning=?, z_critical=?"
        " WHERE baseline_id=?",
        (eff, approved_by, eff, z_warning, z_critical, baseline_id),
    )
    # 同测点同小时槽的旧批准基线退休
    conn.execute(
        "UPDATE baseline_versions SET status='retired', superseded_by=?"
        " WHERE sensor_id=? AND hour_of_day=? AND status='approved'"
        " AND baseline_id<>?",
        (baseline_id, row["sensor_id"], row["hour_of_day"], baseline_id),
    )
    return eff


def active_baseline(conn: sqlite3.Connection, sensor_id: str, hour_of_day: int,
                    at: str) -> Optional[sqlite3.Row]:
    """业务时间 at 做判定时应使用的已批准基线：
    要求 effective_from <= at —— 后批准的基线绝不追溯作用于旧窗口。"""
    return conn.execute(
        "SELECT * FROM baseline_versions"
        " WHERE sensor_id=? AND hour_of_day=? AND status='approved'"
        " AND effective_from<=?"
        " ORDER BY effective_from DESC, baseline_id DESC LIMIT 1",
        (sensor_id, hour_of_day, at),
    ).fetchone()


def baseline_as_known(conn: sqlite3.Connection, sensor_id: str, hour_of_day: int,
                      as_of: str) -> Optional[sqlite3.Row]:
    """重放 as_of 时点系统所知且当时已生效的基线（用于 as_occurred 分析）。"""
    return conn.execute(
        "SELECT * FROM baseline_versions"
        " WHERE sensor_id=? AND hour_of_day=? AND status='approved'"
        " AND effective_from<=? AND approved_at<=?"
        " ORDER BY effective_from DESC, baseline_id DESC LIMIT 1",
        (sensor_id, hour_of_day, as_of, as_of),
    ).fetchone()


def evaluate(agg: dict, baseline: Optional[sqlite3.Row],
             z_warning: float = DEFAULT_Z_WARNING,
             z_critical: float = DEFAULT_Z_CRITICAL) -> dict:
    """对一个窗口聚合结果做判定。维护窗口内不产生告警。

    判定量：稳健 z 分数 = (窗口中位数 - 基线中位数)/基线MAD。
    无基线时无法做统计判定（明确标记 unbaselined），不用固定阈值顶替。
    """
    if agg["in_maintenance"]:
        return {"alert": False, "reason": "maintenance", "severity": None}
    if baseline is None:
        return {"alert": False, "reason": "unbaselined", "severity": None}
    # 以批准时固化在基线行里的倍数为准；参数仅作向后兼容兜底
    z_warning = baseline["z_warning"] if "z_warning" in baseline.keys() else z_warning
    z_critical = baseline["z_critical"] if "z_critical" in baseline.keys() else z_critical
    scale = baseline["mad"]
    if scale <= 1e-12:
        scale = max(abs(baseline["median"]) * FLOOR_SCALE_FRACTION, FLOOR_SCALE_ABS)
    z_median = (agg["median"] - baseline["median"]) / scale
    z_max = (agg["max_value"] - baseline["median"]) / scale
    z_min = (agg["min_value"] - baseline["median"]) / scale
    z_extreme = max(z_max, -z_min)

    threshold = {
        "rule_kind": "mad_z",
        "baseline_id": baseline["baseline_id"],
        "baseline_median": baseline["median"],
        "baseline_mad": baseline["mad"],
        "hour_of_day": baseline["hour_of_day"],
        "z_warning": z_warning,
        "z_critical": z_critical,
        "iqr_lower": baseline["iqr_lower"],
        "iqr_upper": baseline["iqr_upper"],
        "effective_from": baseline["effective_from"],
        "approved_at": baseline["approved_at"],
    }
    result = {
        "alert": False, "reason": "normal", "severity": None,
        "z_median": z_median, "z_extreme": z_extreme, "threshold": threshold,
    }
    if abs(z_median) >= z_critical or z_extreme >= z_critical:
        result.update(alert=True, reason="mad_z_exceeded", severity="critical")
    elif abs(z_median) >= z_warning or z_extreme >= z_warning:
        result.update(alert=True, reason="mad_z_exceeded", severity="warning")
    return result
