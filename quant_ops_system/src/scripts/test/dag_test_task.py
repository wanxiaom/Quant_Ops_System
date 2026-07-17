#!/usr/bin/env python3
"""Small deterministic task used to exercise DAG scheduling."""

import argparse
import sys
import time
from datetime import datetime


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a lightweight DAG test step")
    parser.add_argument("--queue", default="test")
    parser.add_argument("--step", default="step")
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    parser.add_argument("--message", default="")
    parser.add_argument("--emit-lines", type=int, default=3)
    parser.add_argument("--fail", action="store_true")
    args, unknown = parser.parse_known_args()
    if unknown:
        print(f"ignored arguments: {' '.join(unknown)}", flush=True)
    return args


def main() -> int:
    args = parse_args()
    started_at = datetime.now().astimezone().isoformat(timespec="seconds")
    print(
        f"DAG test task started: queue={args.queue} step={args.step} "
        f"started_at={started_at}",
        flush=True,
    )
    if args.message:
        print(f"message: {args.message}", flush=True)

    line_count = max(0, args.emit_lines)
    sleep_seconds = max(0.0, args.sleep_seconds)
    interval = sleep_seconds / max(1, line_count)
    for index in range(line_count):
        print(f"progress: {index + 1}/{line_count}", flush=True)
        time.sleep(interval)

    remaining = sleep_seconds - interval * line_count
    if remaining > 0:
        time.sleep(remaining)

    if args.fail:
        print("DAG test task failed as requested", file=sys.stderr, flush=True)
        return 1

    print("DAG test task completed successfully", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
