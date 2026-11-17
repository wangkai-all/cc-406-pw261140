#!/usr/bin/env python3
"""演示入口：python -I run_demo.py [--serve] [--port 8080]

先重建库并跑完端到端场景，打印关键不变量；--serve 时随后启动 Web。
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from monitor.cli import main as cli_main  # noqa: E402
from monitor.demo_scenario import build_demo  # noqa: E402

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--serve", action="store_true")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--keep", action="store_true", help="不重建库，追加演示")
    args = p.parse_args()
    summary = build_demo(keep=args.keep)
    import json
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if args.serve:
        cli_main(["serve", "--port", str(args.port)])
