# 监测数据统计告警系统（从零实现，纯标准库）

按组织与时间窗口对虚构时序数据做实际聚合与统计判定；基线经人工批准后生效；
维护窗口与测点归属支持追溯修订；全历史重算与仅未来生效两种模式并存，
**原始告警永不被覆盖**，更正后的结论以分析版本形式可查。

不接入任何真实生产设备，数据全部由确定性生成器合成；阈值不是固定列表，
而是每个测点 × 小时槽由历史分布的中位数/MAD 稳健统计训练得到。

## 技术栈

Python 3.11 标准库：`sqlite3`（WAL 模式）、`http.server`（零依赖 Web/API）、
原生 HTML/JS/SVG 前端。无第三方依赖。

## 快速开始

```bash
# 端到端演示（合成数据 → 基线批准 → 告警 → 追溯修订 → 两种重算 → 幂等校验）
python3 -I run_demo.py

# 演示 + 启动网页（http://127.0.0.1:8080）
python3 -I run_demo.py --serve

# 仅启动网页（使用 data/monitor.db）
python3 -I -m monitor.cli serve --port 8080

# 测试
python3 -I run_tests.py
```

## 模块

| 文件 | 职责 |
|---|---|
| `monitor/schema.sql` | 双时态（bitemporal）表结构 |
| `monitor/repository.py` | 归属/维护/原始点的 as-of 查询与追溯修订（只插入，不改旧行） |
| `monitor/stats.py` | 中位数/MAD/分位数等稳健统计与小时窗口聚合 |
| `monitor/baseline.py` | 基线训练（proposed）、人工批准（approved 才生效）、MAD-z 判定 |
| `monitor/alerts.py` | 不可变告警行、阈值/统计 JSON 快照、指纹幂等、通知发件箱 |
| `monitor/recalc.py` | 重算作业：full_history / forward_only，断点续算 |
| `monitor/ingest.py` | 虚构时序生成器与实时窗口接入处理 |
| `monitor/webapp.py` | Web/API 服务 |
| `monitor/demo_scenario.py` | 端到端演示编排 |

## 关键设计如何对应需求

### 1. 基线变了不能覆盖旧告警

- `alerts` 是事实行：触发时把完整阈值依据（基线 id、中位数、MAD、z 倍数、
  IQR、批准/生效时间）和窗口统计固化为 JSON 快照，之后任何基线变化都不触碰它。
- 新基线批准后旧基线置 `retired`，但历史告警仍通过 `baseline_id` 关联到当时那一条。
- 重算的新结论写入 `alert_analyses`（version 递增），v1 永远是 `as_occurred`
  当时依据，后续版本是 `full_recalc` 更正分析版，页面两者并排可查。

### 2. 人工批准 + 仅对以后生效

- 训练只产出 `status='proposed'`；批准时写 `effective_from`，并可调整
  z_warning/z_critical（持久化到基线行）。
- 判定只选 `effective_from <= 窗口时间` 的已批准基线，后批准的基线绝不追溯旧窗口。
- 网页批准时调整的 z 倍数随告警快照固化，事后再改也不改写历史告警的依据。

### 3. 追溯修订（归属、维护、数据）

全部双时态：旧行置 `superseded_at` 并插入新行（带 prev/corrected 链）。
所有查询支持 `as_of` 参数，可重现任意时点系统所知的世界：

```
org_at(conn, sensor, t, as_of=...)          # 当时点归属
in_maintenance(conn, sensor, ws, we, as_of) # 当时点维护视图
points_between(..., as_of=...)              # 当时点数据版本
```

### 4. 两种重算模式（创建时选定，留存在 recalc_jobs.mode）

- **full_history**：历史窗口按当前知识重算。旧告警不删不改不重发；
  逐条产出更正分析版（stands / withdrawn / maintenance / reassigned）；
  历史上漏发的告警可以补建，但**不进通知发件箱**（避免补发误导）。
- **forward_only**：只处理当前时刻之后的窗口，历史完全不碰，
  与实时处理走同一条代码路径。

### 5. 进程中途退出不重复发告警（三道闸）

1. `recalc_progress` 记录每个窗口完成状态，恢复时跳过已完成窗口（断点续算）。
2. `alerts.fingerprint` 唯一约束（传感器|窗口|规则|基线 id），重复触发返回旧 id。
3. `notification_outbox` 的 `(alert_id, channel)` 唯一约束 + `INSERT OR IGNORE`，
   通知只投一次；全历史重算补建的历史告警写入 `suppressed` 占位行，永不投递。
   测试显式断言重跑/跨进程恢复后 pending 通知数为 0。

## API 摘要

```
GET  /api/sensors | /api/baselines?status=proposed | /api/alerts
GET  /api/alerts/{id}            # 原始点、阈值快照、统计快照、全部分析版本
POST /api/baselines/train        # 生成提议
POST /api/baselines/{id}/approve # 人工批准（可带 z 倍数/生效时间）
POST /api/maintenance            # 补录维护
POST /api/maintenance/{id}/correct  # 追溯更正维护窗口
POST /api/jobs                   # 创建重算作业（mode=full_history|forward_only）
POST /api/jobs/{id}/run          # 执行/恢复（body 可带 max_windows 模拟中断）
GET  /api/jobs | /api/jobs/{id} | /api/outbox
```

## 数据模型要点

```
alerts (不可变事实行, fingerprint UNIQUE)
  └─ alert_analyses (v1 as_occurred → v2.. full_recalc 更正版)
  └─ alert_status_events (open/ack/resolved… 状态留痕，事实行不动)
  └─ notification_outbox (alert×channel 唯一，通知幂等)
baseline_versions (proposed → approved → retired)
sensor_ownership / maintenance_windows / raw_points (双时态，superseded 链)
recalc_jobs + recalc_progress (断点续算)
```
