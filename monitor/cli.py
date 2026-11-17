"""命令行入口。

用法:
  python -m monitor.cli init-db
  python -m monitor.cli demo                 # 端到端演示场景（含异常/更正/重算/中断恢复）
  python -m monitor.cli serve [--port 8080]
  python -m monitor.cli ingest-run
  python -m monitor.cli recalc <full_history|forward_only> [--max-windows N] [--resume JOB_ID]
"""
from __future__ import annotations

import argparse
import json
import sys

from . import alerts as al
from . import baseline as bl
from . import ingest
from . import recalc
from .db import DEFAULT_DB, connect, init_db, reset_db
from .demo_scenario import build_demo


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="monitor")
    p.add_argument("--db", default=str(DEFAULT_DB))
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init-db")
    sub.add_parser("reset-db")
    sp = sub.add_parser("serve"); sp.add_argument("--host", default="127.0.0.1")
    sp.add_argument("--port", type=int, default=8080)
    sub.add_parser("ingest-run")
    sub.add_parser("demo")
    rc = sub.add_parser("recalc")
    rc.add_argument("mode", choices=["full_history", "forward_only"])
    rc.add_argument("--max-windows", type=int)
    rc.add_argument("--resume")
    rc.add_argument("--reason", default="")
    args = p.parse_args(argv)

    if args.cmd == "init-db":
        conn = connect(args.db); init_db(conn); conn.close()
        print("initialized", args.db); return 0
    if args.cmd == "reset-db":
        reset_db(args.db); print("reset", args.db); return 0
    if args.cmd == "serve":
        from .webapp import serve
        serve(args.host, args.port, args.db); return 0
    if args.cmd == "demo":
        summary = build_demo(args.db)
        print(json.dumps(summary, ensure_ascii=False, indent=2)); return 0

    conn = connect(args.db)
    if args.cmd == "ingest-run":
        r = ingest.process_new_windows(conn)
        conn.close()
        print(json.dumps({"windows": len(r)}, ensure_ascii=False)); return 0
    if args.cmd == "recalc":
        if args.resume:
            job_id = args.resume
        else:
            job_id = recalc.create_job(conn, args.mode, reason=args.reason)
        result = recalc.run_job(conn, job_id, max_windows=args.max_windows)
        conn.close()
        print(json.dumps(result, ensure_ascii=False)); return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
