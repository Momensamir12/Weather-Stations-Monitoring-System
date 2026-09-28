#!/usr/bin/env python3
"""
Parse BitcaskConsumerService WINDOW log lines and compute throughput.

Log format (from metricsLog.info):
    WINDOW,<timestampMs>,<windowCount>,<avgPutLatencyMs>,<avgE2ELatencyMs>

Usage:
    # From a log file
    python throughput.py app.log

    # From stdin (piped from tee or another process)
    tail -f app.log | python throughput.py

    # Live, reading a file that is still being written to
    python throughput.py -f app.log
"""

import argparse
import re
import sys
import time
from dataclasses import dataclass
from typing import Iterator, Optional


# Matches: WINDOW,1710000000000,1200,0.012,45.6
WINDOW_RE = re.compile(
    r"WINDOW,(?P<ts>\d+),(?P<interval>\d+),(?P<count>\d+),"
    r"(?P<mps>[-\d.]+),(?P<put_avg>[-\d.]+),(?P<put_max>[-\d.]+),"
    r"(?P<e2e_avg>[-\d.]+),(?P<e2e_max>[-\d.]+),"
    r"(?P<bytes>\d+),(?P<bps>[-\d.]+)"
)


@dataclass
class Window:
    ts_ms: int
    count: int
    avg_put_ms: float
    avg_e2e_ms: float


def parse_line(line: str) -> Optional[Window]:
    m = WINDOW_RE.search(line)
    if not m:
        return None
    try:
        return Window(
            ts_ms=int(m.group("ts")),
            count=int(m.group("count")),
            avg_put_ms=float(m.group("put_lat")),
            avg_e2e_ms=float(m.group("e2e_lat")),
        )
    except ValueError:
        return None


def iter_windows(stream, follow: bool = False) -> Iterator[Window]:
    """Yield Window objects from a stream. If follow=True, keeps reading new lines."""
    while True:
        line = stream.readline()
        if not line:
            if follow:
                time.sleep(0.5)
                continue
            return
        w = parse_line(line)
        if w:
            yield w


def fmt_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024.0:
            return f"{n:7.2f} {unit}"
        n /= 1024.0
    return f"{n:7.2f} TB"


def report(windows: Iterator[Window], print_every: int = 1) -> None:
    prev: Optional[Window] = None
    total_count = 0
    total_time_ms = 0
    n = 0
    t_start = time.time()

    print(
        f"{'time':>13}  {'interval':>9}  {'count':>8}  "
        f"{'msg/s':>10}  {'put_ms':>8}  {'e2e_ms':>8}",
        flush=True,
    )
    print("-" * 70, flush=True)

    for w in windows:
        if prev is not None:
            dt_ms = w.ts_ms - prev.ts_ms
            dt_s = dt_ms / 1000.0
            if dt_s > 0:
                mps = w.count / dt_s
                total_count += w.count
                total_time_ms += dt_ms
                n += 1

                if n % print_every == 0:
                    ts = time.strftime("%H:%M:%S", time.localtime(w.ts_ms / 1000))
                    print(
                        f"{ts:>13}  {dt_s:>8.3f}s  {w.count:>8d}  "
                        f"{mps:>10.1f}  {w.avg_put_ms:>8.3f}  {w.avg_e2e_ms:>8.1f}",
                        flush=True,
                    )
        prev = w

    # ---- summary ----
    print("-" * 70, flush=True)
    if n > 0 and total_time_ms > 0:
        overall = total_count / (total_time_ms / 1000.0)
        wall = time.time() - t_start
        print(f"Windows analyzed : {n}")
        print(f"Total messages   : {total_count}")
        print(f"Log span         : {total_time_ms / 1000.0:.2f} s")
        print(f"Overall msg/s    : {overall:.2f}")
        print(f"Wall-clock sec   : {wall:.2f} s (includes stream idle time)")
    else:
        print("No window pairs found — nothing to compute.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "file",
        nargs="?",
        help="Log file to read. If omitted, reads from stdin.",
    )
    ap.add_argument(
        "-f",
        "--follow",
        action="store_true",
        help="Keep reading new lines as they are appended (like tail -f).",
    )
    ap.add_argument(
        "-n",
        "--every",
        type=int,
        default=1,
        help="Print every Nth window (default: 1).",
    )
    args = ap.parse_args()

    try:
        if args.file:
            with open(args.file, "r", buffering=1) as f:
                if args.follow:
                    f.seek(0, 2)  # jump to end; only show new lines
                report(iter_windows(f, follow=args.follow), print_every=args.every)
        else:
            report(iter_windows(sys.stdin, follow=False), print_every=args.every)
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)


if __name__ == "__main__":
    main()