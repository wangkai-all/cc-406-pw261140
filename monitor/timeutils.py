"""时间轴工具：ISO-8601 UTC 解析、小时窗口对齐、双时态 as-of 查询。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

HOUR = timedelta(hours=1)


def parse_ts(s: str) -> datetime:
    """解析 '...Z' 或 '+00:00' 文本为 aware datetime（内部统一 UTC）。"""
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def hour_floor(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def iter_hour_windows(start: datetime, end: datetime):
    """生成 [start, end) 内所有整点小时窗口的 (window_start, window_end)。"""
    cur = hour_floor(start)
    end_floor = hour_floor(end)
    while cur < end_floor:
        nxt = cur + HOUR
        yield cur, nxt
        cur = nxt


def hour_slots(start: datetime, end: datetime) -> list[datetime]:
    return [w[0] for w in iter_hour_windows(start, end)]
