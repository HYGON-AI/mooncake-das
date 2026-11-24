#!/usr/bin/env python3
"""Sample master metrics, master RSS and node memory into a CSV timeline.

Runs on the master node for the whole test so the report can show when the
segments filled, when each eviction round ran, and what memory looked like at
that moment. Low frequency on purpose: the master log is the source of truth
for eviction start/stop, this is only the timeline around it.
"""
from __future__ import annotations

import argparse
import csv
import os
import time
import urllib.request

WANTED = (
    "master_key_count",
    "master_active_clients",
    "master_successful_evictions_total",
    "master_evicted_key_count",
    "master_client_offboarding_queue_depth",
)


def scrape(url: str) -> dict[str, float]:
    out: dict[str, float] = {}
    try:
        text = urllib.request.urlopen(url, timeout=8).read().decode()
    except Exception:
        return out
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) >= 2 and parts[0] in WANTED:
            try:
                out[parts[0]] = float(parts[1])
            except ValueError:
                pass
    return out


def master_rss_kib(pidfile: str) -> int:
    try:
        with open(pidfile) as handle:
            pid = int(handle.read().strip())
        with open(f"/proc/{pid}/status") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except Exception:
        pass
    return -1


def meminfo_gib() -> tuple[float, float]:
    total = avail = -1.0
    try:
        with open("/proc/meminfo") as handle:
            for line in handle:
                if line.startswith("MemTotal:"):
                    total = int(line.split()[1]) / 1024 / 1024
                elif line.startswith("MemAvailable:"):
                    avail = int(line.split()[1]) / 1024 / 1024
    except Exception:
        pass
    return total, avail


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics-url", required=True)
    parser.add_argument("--master-pidfile", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--interval", type=float, default=15.0)
    args = parser.parse_args()

    fields = [
        "ts", "iso", "key_count", "active_clients", "evictions_total",
        "evicted_key_count", "offboarding_queue", "master_rss_gib",
        "mem_total_gib", "mem_avail_gib",
    ]
    new = not os.path.exists(args.out) or os.path.getsize(args.out) == 0
    with open(args.out, "a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        if new:
            writer.writeheader()
            handle.flush()
        while True:
            now = time.time()
            m = scrape(args.metrics_url)
            rss = master_rss_kib(args.master_pidfile)
            total, avail = meminfo_gib()
            writer.writerow({
                "ts": f"{now:.0f}",
                "iso": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(now)),
                "key_count": int(m.get("master_key_count", -1)),
                "active_clients": int(m.get("master_active_clients", -1)),
                "evictions_total": int(m.get("master_successful_evictions_total", -1)),
                "evicted_key_count": int(m.get("master_evicted_key_count", -1)),
                "offboarding_queue": int(m.get("master_client_offboarding_queue_depth", -1)),
                "master_rss_gib": f"{rss/1024/1024:.1f}" if rss > 0 else "-1",
                "mem_total_gib": f"{total:.0f}",
                "mem_avail_gib": f"{avail:.0f}",
            })
            handle.flush()
            time.sleep(max(1.0, args.interval))


if __name__ == "__main__":
    raise SystemExit(main())
