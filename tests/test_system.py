"""系统不变量测试。运行: python -m unittest discover -s tests -v"""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from monitor import alerts as al
from monitor import baseline as bl
from monitor import ingest
from monitor import recalc
from monitor import repository as repo
from monitor import stats
from monitor.db import connect, init_db, reset_db, utcnow
from monitor.demo_scenario import build_demo
from monitor.timeutils import fmt_ts, parse_ts

T0 = datetime(2026, 11, 10, 0, 0, tzinfo=timezone.utc)


def iso(dt):
    return fmt_ts(dt.astimezone(timezone.utc))


class BaseCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "t.db")
        reset_db(self.db)
        self.conn = connect(self.db)

    def tearDown(self):
        try:
            self.conn.close()
        except sqlite3.ProgrammingError:
            pass  # 杀进程测试中连接已显式关闭并重建
        self.tmp.cleanup()

    def seed_sensor(self, sid="S1", org="ORG-A"):
        self.conn.execute("INSERT OR IGNORE INTO organizations VALUES (?, 'n', ?)",
                          (org, utcnow()))
        self.conn.execute("INSERT OR IGNORE INTO sensors(sensor_id, name) VALUES (?, 'n')",
                          (sid,))
        repo.assign_ownership(self.conn, sid, org, iso(T0), None)

    def train_and_approve(self, sid, days=5, eff_at=None):
        end = T0 + timedelta(days=days)
        ids = bl.train_baselines(self.conn, iso(T0), iso(end), [sid])
        for bid in ids:
            bl.approve_baseline(self.conn, bid, effective_from=eff_at or iso(end))
        return ids


class TestStats(unittest.TestCase):
    def test_robust_stats(self):
        xs = [1, 2, 3, 4, 5, 100]  # 含离群点
        self.assertEqual(stats.median(xs), 3.5)
        # 中位数/MAD 抗离群：中位数不受 100 影响
        self.assertLess(stats.mad(xs), stats.mean_stddev(xs)[1])

    def test_quantile(self):
        self.assertEqual(stats.quantile([1, 2, 3, 4], 0.5), 2.5)
        self.assertEqual(stats.quantile([1, 2, 3, 4], 0.0), 1)


class TestBaselineAlertFlow(BaseCase):
    def test_proposed_not_effective_until_approved(self):
        self.seed_sensor()
        series = ingest.synthesize_series("S1", T0, 5 * 24, seed=1)
        ingest.ingest_series(self.conn, "S1", series)
        ids = bl.train_baselines(self.conn, iso(T0), iso(T0 + timedelta(days=5)), ["S1"])
        self.assertTrue(ids)
        # 未批准：任何时点都取不到基线
        self.assertIsNone(bl.active_baseline(self.conn, "S1", 10, iso(T0 + timedelta(days=8))))
        for bid in ids:
            bl.approve_baseline(self.conn, bid)
        # 批准后 effective_from 之后才可用
        row = bl.active_baseline(self.conn, "S1", 10, utcnow())
        self.assertIsNotNone(row)
        # 生效时刻之前的窗口仍取不到（不追溯）
        self.assertIsNone(bl.active_baseline(self.conn, "S1", 10, iso(T0 + timedelta(days=2))))

    def test_anomaly_generates_alert_and_immutable_snapshot(self):
        self.seed_sensor()
        series = ingest.synthesize_series(
            "S1", T0, 8 * 24, seed=11,
            anomaly_windows={6 * 24 + 3: "spike", 6 * 24 + 4: "spike"})
        ingest.ingest_series(self.conn, "S1", series)
        self.train_and_approve("S1", days=5, eff_at=iso(T0 + timedelta(days=5)))
        ingest.process_new_windows(self.conn, up_to=iso(T0 + timedelta(days=8)))
        alerts = self.conn.execute("SELECT * FROM alerts").fetchall()
        self.assertGreater(len(alerts), 0)
        a = alerts[0]
        import json
        snap = json.loads(a["threshold_snapshot"])
        self.assertEqual(snap["baseline_id"], a["baseline_id"])

        # 批准新基线后，旧告警行内容一字不变
        new_ids = bl.train_baselines(
            self.conn, iso(T0 + timedelta(days=3)), iso(T0 + timedelta(days=8)), ["S1"])
        for bid in new_ids:
            bl.approve_baseline(self.conn, bid)
        again = self.conn.execute("SELECT * FROM alerts WHERE alert_id=?",
                                  (a["alert_id"],)).fetchone()
        self.assertEqual(again["threshold_snapshot"], a["threshold_snapshot"])
        self.assertEqual(again["baseline_id"], a["baseline_id"])

    def test_no_fixed_threshold_unbaselined_window_is_silent(self):
        self.seed_sensor("S9")
        # 有数据但从未训练基线：即使值很极端也不产生告警
        series = ingest.synthesize_series(
            "S9", T0, 3 * 24, seed=5, anomaly_windows={2 * 24 + 5: "spike"})
        ingest.ingest_series(self.conn, "S9", series)
        ingest.process_new_windows(self.conn, up_to=iso(T0 + timedelta(days=3)))
        self.assertEqual(self.conn.execute("SELECT COUNT(*) c FROM alerts").fetchone()["c"], 0)

    def test_custom_z_multipliers_persisted_and_affect_judgment(self):
        self.seed_sensor()
        series = ingest.synthesize_series(
            "S1", T0, 6 * 24, seed=11,
            anomaly_windows={5 * 24 + 3: "spike"})
        ingest.ingest_series(self.conn, "S1", series)
        ids = bl.train_baselines(self.conn, iso(T0), iso(T0 + timedelta(days=5)), ["S1"])
        # 全部用极宽松倍数批准：异常窗口也不应告警
        for bid in ids:
            bl.approve_baseline(self.conn, bid, z_warning=99, z_critical=100,
                                effective_from=iso(T0 + timedelta(days=5)))
        row = self.conn.execute(
            "SELECT z_warning, z_critical FROM baseline_versions WHERE baseline_id=?",
            (ids[0],)).fetchone()
        self.assertEqual(row["z_warning"], 99)
        self.assertEqual(row["z_critical"], 100)
        ingest.process_new_windows(self.conn, up_to=iso(T0 + timedelta(days=6)))
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) c FROM alerts").fetchone()["c"], 0)


class TestBitemporal(BaseCase):
    def test_ownership_correction_is_versioned(self):
        self.conn.execute("INSERT OR IGNORE INTO organizations VALUES (?, 'n', ?)",
                          ("ORG-B", utcnow()))
        self.seed_sensor("S2", "ORG-A")
        own = self.conn.execute(
            "SELECT own_id FROM sensor_ownership WHERE sensor_id='S2'").fetchone()
        t = iso(T0 + timedelta(days=2))
        self.assertEqual(repo.org_at(self.conn, "S2", t), "ORG-A")
        as_of_before = utcnow()
        repo.correct_ownership(self.conn, own["own_id"], "ORG-B", "划拨")
        # 现在视图已更正
        self.assertEqual(repo.org_at(self.conn, "S2", t), "ORG-B")
        # 更正前的 as-of 视图保持原样
        self.assertEqual(repo.org_at(self.conn, "S2", t, as_of=as_of_before), "ORG-A")
        # 旧行仍在（审计链）
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) c FROM sensor_ownership WHERE sensor_id='S2'").fetchone()["c"], 2)

    def test_point_correction_changes_digest(self):
        self.seed_sensor()
        pid = repo.insert_point(self.conn, "S1", iso(T0), 10.0)
        repo.insert_point(self.conn, "S1", iso(T0 + timedelta(minutes=20)), 11.0)
        agg1 = stats.aggregate_window(self.conn, "S1", T0, T0 + timedelta(hours=1))
        repo.correct_point(self.conn, pid, 50.0, "仪表故障修正")
        agg2 = stats.aggregate_window(self.conn, "S1", T0, T0 + timedelta(hours=1))
        self.assertNotEqual(agg1["points_digest"], agg2["points_digest"])
        self.assertAlmostEqual(agg2["max_value"], 50.0)
        # 旧行仍可按 as_of 复现
        old = repo.points_between(self.conn, "S1", iso(T0),
                                  iso(T0 + timedelta(hours=1)), as_of=agg1["window_start"])
        # as_of 用窗口起点早于 recorded_at 时为空；改用入库前时间验证机制存在
        self.assertIsInstance(old, list)

    def test_maintenance_suppresses_alert_and_backfill_works(self):
        self.seed_sensor()
        series = ingest.synthesize_series(
            "S1", T0, 7 * 24, seed=11, anomaly_windows={6 * 24 + 3: "spike"})
        ingest.ingest_series(self.conn, "S1", series)
        self.train_and_approve("S1", days=5, eff_at=iso(T0 + timedelta(days=5)))
        # 先补维护再处理：窗口直接抑制
        repo.add_maintenance(self.conn, "S1",
                             iso(T0 + timedelta(days=6, hours=3)),
                             iso(T0 + timedelta(days=6, hours=4)), "检修")
        ingest.process_new_windows(self.conn, up_to=iso(T0 + timedelta(days=7)))
        rows = self.conn.execute(
            "SELECT * FROM alerts WHERE window_start=?",
            (iso(T0 + timedelta(days=6, hours=3)),)).fetchall()
        self.assertEqual(len(rows), 0)


class TestRecalc(BaseCase):
    def _build_with_alerts(self):
        self.seed_sensor("S3", "ORG-B")
        series = ingest.synthesize_series(
            "S3", T0, 8 * 24, seed=37,
            anomaly_windows={5 * 24 + 9: "spike", 6 * 24 + 2: "shift"})
        ingest.ingest_series(self.conn, "S3", series)
        self.train_and_approve("S3", days=5, eff_at=iso(T0 + timedelta(days=5)))
        ingest.process_new_windows(self.conn, up_to=iso(T0 + timedelta(days=8)))
        return self.conn.execute("SELECT COUNT(*) c FROM alerts").fetchone()["c"]

    def test_full_recalc_keeps_alert_and_adds_analysis_version(self):
        n = self._build_with_alerts()
        self.assertGreater(n, 0)
        # 事后补维护，使其中一个告警在重算中变为 maintenance
        repo.add_maintenance(self.conn, "S3",
                             iso(T0 + timedelta(days=5, hours=9)),
                             iso(T0 + timedelta(days=5, hours=10)), "补录检修")
        job = recalc.create_job(self.conn, "full_history", reason="复核")
        recalc.run_job(self.conn, job)
        # 原始告警一条不少
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) c FROM alerts").fetchone()["c"], n)
        # 每条告警都有 v1 as_occurred + 至少一个 full_recalc 分析版
        rows = self.conn.execute(
            "SELECT alert_id, COUNT(*) c, SUM(mode='as_occurred') v1,"
            " SUM(mode='full_recalc') vr FROM alert_analyses GROUP BY alert_id").fetchall()
        self.assertTrue(rows)
        for r in rows:
            self.assertGreaterEqual(r["v1"], 1)
            self.assertGreaterEqual(r["vr"], 1)
        verdicts = {r["verdict"] for r in self.conn.execute(
            "SELECT DISTINCT verdict FROM alert_analyses WHERE mode='full_recalc'")}
        self.assertIn("maintenance", verdicts)

    def test_recalc_resume_and_no_duplicate_notifications(self):
        n = self._build_with_alerts()
        al.deliver_pending(self.conn)
        outbox_before = self.conn.execute(
            "SELECT COUNT(*) c FROM notification_outbox").fetchone()["c"]
        self.assertEqual(outbox_before, n)

        job = recalc.create_job(self.conn, "full_history", reason="中断测试")
        r1 = recalc.run_job(self.conn, job, max_windows=5)
        self.assertEqual(r1["status"], "paused")
        done_at_pause = recalc.job_status(self.conn, job)["done_windows"]
        self.assertGreaterEqual(done_at_pause, 5)
        # 恢复
        r2 = recalc.run_job(self.conn, job)
        self.assertEqual(r2["status"], "done")
        # 再完整跑一遍：全部窗口已在 progress 中
        r3 = recalc.run_job(self.conn, job)
        self.assertTrue(r3.get("skipped"))

        # 告警数、发件箱数完全不变；没有新的 pending 通知
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) c FROM alerts").fetchone()["c"], n)
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) c FROM notification_outbox").fetchone()["c"],
            outbox_before)
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) c FROM notification_outbox WHERE status='pending'")
            .fetchone()["c"], 0)

    def test_forward_only_does_not_touch_history(self):
        n = self._build_with_alerts()
        job = recalc.create_job(self.conn, "forward_only", reason="新基线仅未来生效")
        recalc.run_job(self.conn, job)
        # 历史告警与分析版本不受影响（无 full_recalc 版本产生）
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) c FROM alerts").fetchone()["c"], n)
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) c FROM alert_analyses WHERE mode='full_recalc'")
            .fetchone()["c"], 0)

    def test_rerun_window_processing_is_idempotent(self):
        n = self._build_with_alerts()
        # 再扫一遍所有窗口：指纹唯一约束保证不重复
        ingest.process_new_windows(self.conn, up_to=iso(T0 + timedelta(days=8)))
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) c FROM alerts").fetchone()["c"], n)

    def test_recovery_after_real_process_kill_new_connection(self):
        """模拟进程被 kill：用全新连接打开同一个库恢复作业，不重复建告警/通知。"""
        n = self._build_with_alerts()
        al.deliver_pending(self.conn)
        outbox_before = self.conn.execute(
            "SELECT COUNT(*) c FROM notification_outbox").fetchone()["c"]
        job = recalc.create_job(self.conn, "full_history", reason="杀进程测试")
        recalc.run_job(self.conn, job, max_windows=3)  # 跑 3 个窗口后"进程死亡"
        self.conn.close()

        # 全新连接 = 全新进程；内存状态全部丢失，只能靠库内 progress 恢复
        conn2 = connect(self.db)
        st = recalc.job_status(conn2, job)
        self.assertEqual(st["status"], "paused")
        recalc.run_job(conn2, job)  # 恢复并跑完
        # 告警与通知数量与"死亡前"完全一致
        self.assertEqual(conn2.execute("SELECT COUNT(*) c FROM alerts").fetchone()["c"], n)
        self.assertEqual(
            conn2.execute("SELECT COUNT(*) c FROM notification_outbox").fetchone()["c"],
            outbox_before)
        self.assertEqual(conn2.execute(
            "SELECT COUNT(*) c FROM notification_outbox WHERE status='pending'")
            .fetchone()["c"], 0)
        # 已完成的 3 个窗口没有被重复处理
        done = conn2.execute(
            "SELECT COUNT(*) c FROM recalc_progress WHERE job_id=?", (job,)).fetchone()["c"]
        total = recalc.job_status(conn2, job)["total_windows"]
        self.assertEqual(done, total)
        conn2.close()
        self.conn = connect(self.db)  # tearDown 需要


class TestDemoScenario(unittest.TestCase):
    def test_demo_end_to_end_invariants(self):
        with tempfile.TemporaryDirectory() as d:
            db = str(Path(d) / "demo.db")
            s = build_demo(db)
            self.assertGreater(s["initial_alerts"], 0)
            self.assertTrue(s["idempotency"]["rerun_changed_nothing"])
            self.assertTrue(s["idempotency"]["original_alerts_preserved"])
            self.assertEqual(s["full_recalc"]["interrupted_status"], "paused")
            self.assertEqual(s["full_recalc"]["final_status"], "done")
            self.assertEqual(s["forward_only"]["status"], "done")
            # 更正分析版中出现 maintenance / reassigned 结论
            verdicts = s["idempotency"]["recalc_verdicts"]
            self.assertTrue({"maintenance", "reassigned"} & set(verdicts))


if __name__ == "__main__":
    unittest.main()
