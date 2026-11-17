"""SQLite 连接管理与 schema 初始化。全部使用 UTC ISO-8601 文本时间戳。"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_PATH = Path(__file__).with_name("schema.sql")
DEFAULT_DB = Path(__file__).resolve().parent.parent / "data" / "monitor.db"


def utcnow() -> str:
    """统一的 UTC 时间戳（微秒精度，Z 结尾）。

    微秒精度很关键：双时态 as-of 查询按 recorded_at 比较，同毫秒内的
    连续更正需要可区分的时间戳才能重现"更正之前"的视图。
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def connect(db_path: str | Path | None = None) -> sqlite3.Connection:
    path = Path(db_path) if db_path else DEFAULT_DB
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30, isolation_level=None,
                           check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    # WAL + NORMAL：只在 checkpoint 时 fsync，崩溃安全语义对本系统足够，
    # 避免 autocommit 下逐语句 fsync（网络盘/WSL2 上会慢两个数量级）
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))


def reset_db(db_path: str | Path | None = None) -> None:
    """仅用于 demo/测试：删掉物理库后重建。"""
    path = Path(db_path) if db_path else DEFAULT_DB
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(path) + suffix)
        if p.exists():
            p.unlink()
    conn = connect(path)
    init_db(conn)
    conn.close()
