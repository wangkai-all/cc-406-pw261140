"""零依赖 Web/API 服务（http.server）。

页面能力对应需求：
- 调整基线：查看提议、批准（可设 z 倍数、生效时间）、触发重新训练
- 补维护记录：新增/追溯更正维护窗口
- 告警详情：原始点、当时阈值快照、各分析版本（当时依据 vs 更正分析版）
- 重算作业：创建（两种模式）、断点恢复、进度查看
- 通知发件箱查看
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from typing import Any

from . import alerts as al
from . import baseline as bl
from . import ingest
from . import recalc
from . import repository as repo
from .db import connect, init_db, utcnow
from .timeutils import fmt_ts, parse_ts

WEB_DIR = Path(__file__).resolve().parent.parent / "web"


class _Handler(BaseHTTPRequestHandler):
    server_version = "Monitor/1.0"

    # ---------- 基础设施 ----------
    def log_message(self, fmt, *args):  # 安静日志
        pass

    def _json(self, obj: Any, status: int = 200):
        body = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def _static(self, rel: str):
        # 只允许 web/ 目录内文件
        target = (WEB_DIR / rel).resolve()
        if not str(target).startswith(str(WEB_DIR.resolve())) or not target.is_file():
            self.send_error(404)
            return
        ctype = "text/html; charset=utf-8" if target.suffix == ".html" else \
            "text/javascript; charset=utf-8" if target.suffix == ".js" else \
            "text/css; charset=utf-8" if target.suffix == ".css" else \
            "application/octet-stream"
        data = target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    # ---------- 路由 ----------
    def do_GET(self):
        parsed = urlparse(self.path)
        path, qs = parsed.path, parse_qs(parsed.query)
        try:
            if path == "/" or path == "/index.html":
                return self._static("index.html")
            if path.startswith("/static/"):
                return self._static("static/" + path[len("/static/"):])
            if path == "/api/health":
                return self._json({"ok": True, "time": utcnow()})
            if path == "/api/sensors":
                return self._sensors()
            if path == "/api/baselines":
                return self._baselines(qs)
            if path == "/api/alerts":
                return self._list_alerts(qs)
            if path.startswith("/api/alerts/"):
                return self._alert_detail(int(path.rsplit("/", 1)[1]))
            if path == "/api/maintenance":
                return self._list_maintenance(qs)
            if path == "/api/jobs":
                return self._list_jobs()
            if path.startswith("/api/jobs/"):
                return self._job(path.rsplit("/", 1)[1])
            if path == "/api/outbox":
                return self._outbox()
            if path == "/api/points":
                return self._points(qs)
            self.send_error(404)
        except Exception as e:  # noqa: BLE001 - API 层统一兜底
            self._json({"error": f"{type(e).__name__}: {e}"}, 400)

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            if path == "/api/baselines/train":
                return self._train()
            if path.startswith("/api/baselines/") and path.endswith("/approve"):
                return self._approve(int(path.split("/")[3]))
            if path == "/api/maintenance":
                return self._add_maintenance()
            if path.startswith("/api/maintenance/") and path.endswith("/correct"):
                return self._correct_maintenance(int(path.split("/")[3]))
            if path == "/api/jobs":
                return self._create_job()
            if path.startswith("/api/jobs/") and path.endswith("/run"):
                return self._run_job(path.split("/")[3])
            if path.startswith("/api/alerts/") and path.endswith("/status"):
                return self._alert_status(int(path.split("/")[3]))
            if path == "/api/ingest/run":
                return self._ingest_run()
            self.send_error(404)
        except Exception as e:  # noqa: BLE001
            self._json({"error": f"{type(e).__name__}: {e}"}, 400)

    @property
    def conn(self):
        return self.server.conn  # type: ignore[attr-defined]

    # ---------- 资源处理 ----------
    def _sensors(self):
        rows = self.conn.execute(
            "SELECT s.*, (SELECT org_id FROM sensor_ownership o WHERE o.sensor_id=s.sensor_id"
            " AND o.superseded_at IS NULL ORDER BY o.valid_from DESC LIMIT 1) AS org_id"
            " FROM sensors s ORDER BY s.sensor_id").fetchall()
        return self._json([dict(r) for r in rows])

    def _baselines(self, qs):
        sql = ("SELECT b.*, s.name AS sensor_name FROM baseline_versions b"
               " JOIN sensors s ON s.sensor_id=b.sensor_id WHERE 1=1")
        args: list[Any] = []
        if qs.get("sensor_id"):
            sql += " AND b.sensor_id=?"; args.append(qs["sensor_id"][0])
        if qs.get("status"):
            sql += " AND b.status=?"; args.append(qs["status"][0])
        sql += " ORDER BY b.sensor_id, b.hour_of_day, b.baseline_id DESC"
        return self._json([dict(r) for r in self.conn.execute(sql, args)])

    def _train(self):
        body = self._body()
        ids = bl.train_baselines(self.conn, body["train_start"], body["train_end"],
                                 body.get("sensor_ids"), proposed_by=body.get("by", "web"))
        return self._json({"proposed": ids, "count": len(ids)})

    def _approve(self, baseline_id: int):
        body = self._body()
        eff = bl.approve_baseline(self.conn, baseline_id,
                                  approved_by=body.get("by", "operator"),
                                  z_warning=float(body.get("z_warning", bl.DEFAULT_Z_WARNING)),
                                  z_critical=float(body.get("z_critical", bl.DEFAULT_Z_CRITICAL)),
                                  effective_from=body.get("effective_from"))
        return self._json({"baseline_id": baseline_id, "effective_from": eff})

    def _list_alerts(self, qs):
        sql = ("SELECT a.*, (SELECT to_status FROM alert_status_events e WHERE e.alert_id=a.alert_id"
               " ORDER BY e.event_id DESC LIMIT 1) AS status,"
               " (SELECT COUNT(*) FROM alert_analyses an WHERE an.alert_id=a.alert_id) AS analysis_versions"
               " FROM alerts a WHERE 1=1")
        args: list[Any] = []
        if qs.get("org_id"):
            sql += " AND a.org_id=?"; args.append(qs["org_id"][0])
        if qs.get("sensor_id"):
            sql += " AND a.sensor_id=?"; args.append(qs["sensor_id"][0])
        sql += " ORDER BY a.window_start DESC, a.alert_id DESC LIMIT 500"
        return self._json([dict(r) for r in self.conn.execute(sql, args)])

    def _alert_detail(self, alert_id: int):
        a = self.conn.execute("SELECT * FROM alerts WHERE alert_id=?", (alert_id,)).fetchone()
        if not a:
            return self._json({"error": "not found"}, 404)
        snap = json.loads(a["stat_snapshot"])
        point_ids = snap.get("point_ids", [])
        placeholders = ",".join("?" * len(point_ids))
        points = self.conn.execute(
            f"SELECT point_id, sensor_id, ts, value, recorded_at, corrected_point, note"
            f" FROM raw_points WHERE point_id IN ({placeholders}) ORDER BY ts",
            point_ids).fetchall() if point_ids else []
        analyses = self.conn.execute(
            "SELECT * FROM alert_analyses WHERE alert_id=? ORDER BY version",
            (alert_id,)).fetchall()
        statuses = self.conn.execute(
            "SELECT * FROM alert_status_events WHERE alert_id=? ORDER BY event_id",
            (alert_id,)).fetchall()
        base_row = None
        if a["baseline_id"]:
            base_row = self.conn.execute(
                "SELECT * FROM baseline_versions WHERE baseline_id=?",
                (a["baseline_id"],)).fetchone()
        return self._json({
            "alert": dict(a),
            "threshold_snapshot": json.loads(a["threshold_snapshot"]),
            "stat_snapshot": snap,
            "baseline": dict(base_row) if base_row else None,
            "raw_points": [dict(p) for p in points],
            "analyses": [dict(x) for x in analyses],
            "status_events": [dict(x) for x in statuses],
            "current_status": al.current_status(self.conn, alert_id),
        })

    def _list_maintenance(self, qs):
        sql = "SELECT * FROM maintenance_windows WHERE 1=1"
        args: list[Any] = []
        if qs.get("sensor_id"):
            sql += " AND sensor_id=?"; args.append(qs["sensor_id"][0])
        sql += " ORDER BY start_ts DESC, mw_id DESC LIMIT 500"
        return self._json([dict(r) for r in self.conn.execute(sql, args)])

    def _add_maintenance(self):
        b = self._body()
        mw_id = repo.add_maintenance(self.conn, b["sensor_id"], b["start_ts"],
                                     b["end_ts"], b.get("reason", ""))
        return self._json({"mw_id": mw_id})

    def _correct_maintenance(self, mw_id: int):
        b = self._body()
        new_id = repo.correct_maintenance(self.conn, mw_id, b["start_ts"],
                                          b["end_ts"], b.get("reason", ""))
        return self._json({"old_mw_id": mw_id, "new_mw_id": new_id})

    def _list_jobs(self):
        rows = self.conn.execute(
            "SELECT * FROM recalc_jobs ORDER BY created_at DESC").fetchall()
        return self._json([dict(r) for r in rows])

    def _job(self, job_id: str):
        st = recalc.job_status(self.conn, job_id)
        progress = self.conn.execute(
            "SELECT state, COUNT(*) AS n FROM recalc_progress WHERE job_id=?"
            " GROUP BY state", (job_id,)).fetchall()
        st["progress"] = [dict(r) for r in progress]
        return self._json(st)

    def _create_job(self):
        b = self._body()
        job_id = recalc.create_job(self.conn, b["mode"], b.get("scope_sensors"),
                                   b.get("range_start"), b.get("range_end"),
                                   b.get("reason", ""))
        return self._json({"job_id": job_id})

    def _run_job(self, job_id: str):
        b = self._body()
        result = recalc.run_job(self.conn, job_id, max_windows=b.get("max_windows"))
        return self._json(result)

    def _alert_status(self, alert_id: int):
        b = self._body()
        al.transition_status(self.conn, alert_id, b["to_status"], b.get("note", ""))
        return self._json({"alert_id": alert_id,
                           "status": al.current_status(self.conn, alert_id)})

    def _ingest_run(self):
        results = ingest.process_new_windows(self.conn)
        return self._json({"processed": len(results), "results": results})

    def _outbox(self):
        rows = self.conn.execute(
            "SELECT * FROM notification_outbox ORDER BY notif_id DESC LIMIT 200").fetchall()
        return self._json([dict(r, payload=json.loads(r["payload"])) for r in rows])

    def _points(self, qs):
        rows = repo.points_between(self.conn, qs["sensor_id"][0],
                                   qs["start"][0], qs["end"][0],
                                   as_of=qs.get("as_of", [None])[0])
        return self._json([dict(r) for r in rows])


def create_server(host: str = "127.0.0.1", port: int = 8080, db_path=None) -> ThreadingHTTPServer:
    conn = connect(db_path)
    init_db(conn)
    server = ThreadingHTTPServer((host, port), _Handler)
    server.conn = conn  # type: ignore[attr-defined]
    return server


def serve(host: str = "127.0.0.1", port: int = 8080, db_path=None) -> None:
    server = create_server(host, port, db_path)
    print(f"monitor web on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
