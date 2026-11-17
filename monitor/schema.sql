-- 监测告警系统：双时态（bitemporal）schema
-- valid_from/valid_to : 业务有效时间（事件实际发生的时间轴）
-- recorded_at/superseded_at : 系统记录时间（我们何时知道这件事）
-- 追溯修订不更新旧行，而是把旧行 superseded_at 置位并插入新行

PRAGMA foreign_keys = ON;

-- ---------- 组织与测点 ----------
CREATE TABLE IF NOT EXISTS organizations (
    org_id      TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS sensors (
    sensor_id   TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    unit        TEXT NOT NULL DEFAULT '',
    metric      TEXT NOT NULL DEFAULT 'generic',
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- 测点归属：双时态。同一 (sensor, valid interval) 可有多代断言记录
CREATE TABLE IF NOT EXISTS sensor_ownership (
    own_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id       TEXT NOT NULL REFERENCES sensors(sensor_id),
    org_id          TEXT NOT NULL REFERENCES organizations(org_id),
    valid_from      TEXT NOT NULL,          -- 业务时间起（含）
    valid_to        TEXT,                   -- 业务时间止（不含），NULL=至今
    recorded_at     TEXT NOT NULL,          -- 本条断言写入系统的时间
    superseded_at   TEXT,                   -- 被更正断言取代的时间，NULL=当前断言
    reason          TEXT NOT NULL DEFAULT '',
    prev_own_id     INTEGER                 -- 追溯链
);
CREATE INDEX IF NOT EXISTS idx_own_sensor ON sensor_ownership(sensor_id, valid_from);

-- ---------- 原始测点数据（追加写，更正走新版本行） ----------
CREATE TABLE IF NOT EXISTS raw_points (
    point_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id       TEXT NOT NULL REFERENCES sensors(sensor_id),
    ts              TEXT NOT NULL,          -- 测量时间（业务时间）
    value           REAL NOT NULL,
    recorded_at     TEXT NOT NULL,          -- 入库时间
    superseded_at   TEXT,                   -- 被更正行取代的时间
    corrected_point INTEGER,                -- 更正自哪个 point_id
    note            TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_points_sensor_ts ON raw_points(sensor_id, ts);

-- ---------- 维护窗口（双时态，可补录/更正） ----------
CREATE TABLE IF NOT EXISTS maintenance_windows (
    mw_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id       TEXT NOT NULL REFERENCES sensors(sensor_id),
    start_ts        TEXT NOT NULL,
    end_ts          TEXT NOT NULL,
    reason          TEXT NOT NULL DEFAULT '',
    recorded_at     TEXT NOT NULL,
    superseded_at   TEXT,
    corrected_mw    INTEGER,
    prev_mw_id      INTEGER
);
CREATE INDEX IF NOT EXISTS idx_mw_sensor ON maintenance_windows(sensor_id, start_ts);

-- ---------- 基线版本（人工批准后才生效，历史分析永远引用当时 baseline_id） ----------
CREATE TABLE IF NOT EXISTS baseline_versions (
    baseline_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id       TEXT NOT NULL REFERENCES sensors(sensor_id),
    hour_of_day     INTEGER NOT NULL,       -- 0..23 分小时基线槽
    median          REAL NOT NULL,
    mad             REAL NOT NULL,          -- 中位数绝对偏差（稳健尺度）
    mean            REAL NOT NULL,
    stddev          REAL NOT NULL,
    iqr_lower       REAL NOT NULL,
    iqr_upper       REAL NOT NULL,
    z_warning       REAL NOT NULL DEFAULT 3.5,  -- 人工批准时可调，随告警快照固化
    z_critical      REAL NOT NULL DEFAULT 6.0,
    sample_count    INTEGER NOT NULL,
    train_start     TEXT NOT NULL,
    train_end       TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'proposed',  -- proposed/approved/retired
    proposed_at     TEXT NOT NULL,
    proposed_by     TEXT NOT NULL DEFAULT 'system',
    approved_at     TEXT,
    approved_by     TEXT,
    effective_from  TEXT,                   -- 批准时刻：仅对之后的判定生效
    superseded_by   INTEGER,                -- 被哪个新基线替代
    source_job_id   TEXT
);
CREATE INDEX IF NOT EXISTS idx_baseline_lookup
    ON baseline_versions(sensor_id, hour_of_day, status, effective_from);

-- ---------- 窗口统计（派生物化，可随时重建；告警另存快照不依赖它） ----------
CREATE TABLE IF NOT EXISTS window_stats (
    sensor_id       TEXT NOT NULL,
    window_start    TEXT NOT NULL,
    window_end      TEXT NOT NULL,
    org_id          TEXT NOT NULL,
    n               INTEGER NOT NULL,
    mean            REAL,
    stddev          REAL,
    median          REAL,
    mad             REAL,
    min_value       REAL,
    max_value       REAL,
    p95             REAL,
    in_maintenance  INTEGER NOT NULL DEFAULT 0,
    points_digest   TEXT NOT NULL,          -- 所用点的指纹，数据更正后 digest 变
    job_id          TEXT,
    computed_at     TEXT NOT NULL,
    PRIMARY KEY (sensor_id, window_start)
);

-- ---------- 告警（不可变事实行；阈值与统计以 JSON 快照固化） ----------
CREATE TABLE IF NOT EXISTS alerts (
    alert_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id           TEXT NOT NULL,
    org_id              TEXT NOT NULL,      -- 触发当时解析出的归属
    window_start        TEXT NOT NULL,
    window_end          TEXT NOT NULL,
    rule_kind           TEXT NOT NULL,      -- mad_z / iqr_range
    severity            TEXT NOT NULL,      -- warning/critical
    observed_value      REAL NOT NULL,
    baseline_id         INTEGER,            -- 当时依据的基线（可能为 NULL）
    threshold_snapshot  TEXT NOT NULL,      -- JSON：当时完整阈值依据
    stat_snapshot       TEXT NOT NULL,      -- JSON：当时窗口统计
    fingerprint         TEXT NOT NULL UNIQUE, -- 幂等键：重算/重启不重复建告警
    source_job_id       TEXT,
    triggered_at        TEXT NOT NULL,
    first_notified_at   TEXT
);
CREATE INDEX IF NOT EXISTS idx_alerts_org_win ON alerts(org_id, window_start);

-- 告警状态流转（事实行内容永不变，状态变化单独留痕）
CREATE TABLE IF NOT EXISTS alert_status_events (
    event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id    INTEGER NOT NULL REFERENCES alerts(alert_id),
    from_status TEXT,
    to_status   TEXT NOT NULL,
    note        TEXT NOT NULL DEFAULT '',
    at          TEXT NOT NULL
);

-- ---------- 告警分析版本（v1=当时依据；重算产生更正分析版，二者并存） ----------
CREATE TABLE IF NOT EXISTS alert_analyses (
    analysis_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id            INTEGER NOT NULL REFERENCES alerts(alert_id),
    version             INTEGER NOT NULL,
    mode                TEXT NOT NULL,      -- as_occurred / full_recalc / forward_only
    recalc_job_id       TEXT,
    baseline_id_used    INTEGER,
    threshold_snapshot  TEXT NOT NULL,
    verdict             TEXT NOT NULL,      -- stands / withdrawn / maintenance / reassigned
    rationale           TEXT NOT NULL,
    computed_at         TEXT NOT NULL,
    UNIQUE (alert_id, version)
);

-- ---------- 重算作业（断点续算，进程退出后可恢复） ----------
CREATE TABLE IF NOT EXISTS recalc_jobs (
    job_id          TEXT PRIMARY KEY,
    mode            TEXT NOT NULL,          -- full_history / forward_only
    scope_sensors   TEXT NOT NULL,          -- JSON 数组，空=全部
    range_start     TEXT,
    range_end       TEXT,
    status          TEXT NOT NULL DEFAULT 'pending', -- pending/running/paused/done/failed
    reason          TEXT NOT NULL DEFAULT '',
    total_windows   INTEGER NOT NULL DEFAULT 0,
    done_windows    INTEGER NOT NULL DEFAULT 0,
    heartbeat_at    TEXT,
    worker_pid      INTEGER,
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT,
    error           TEXT
);

CREATE TABLE IF NOT EXISTS recalc_progress (
    job_id          TEXT NOT NULL REFERENCES recalc_jobs(job_id),
    window_key      TEXT NOT NULL,          -- sensor_id|window_start
    state           TEXT NOT NULL,          -- done/skipped/alerted
    at              TEXT NOT NULL,
    PRIMARY KEY (job_id, window_key)
);

-- ---------- 通知发件箱（按 alert 幂等，重算/重启绝不重复发） ----------
-- status: pending 待发 / sent 已发 / suppressed 永不发（全历史重算补建的历史告警）
CREATE TABLE IF NOT EXISTS notification_outbox (
    notif_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id        INTEGER NOT NULL REFERENCES alerts(alert_id),
    channel         TEXT NOT NULL DEFAULT 'web',
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    sent_at         TEXT,
    created_at      TEXT NOT NULL,
    UNIQUE (alert_id, channel)
);
