#!/usr/bin/env python3
"""Wait for one consistently idle Hygon GPU and print its device ID."""

import re
import subprocess
import sys
import time


def available_gpus(threshold: float) -> set[int]:
    result = subprocess.run(
        ["hy-smi"], check=True, capture_output=True, text=True
    )
    available = set()
    for line in result.stdout.splitlines():
        match = re.match(r"^\s*(\d+)\s+", line)
        if not match:
            continue
        columns = re.split(r"\s+", line.strip())
        if len(columns) < 7:
            continue
        try:
            vram = float(columns[5].rstrip("%"))
            compute = float(columns[6].rstrip("%"))
        except ValueError:
            continue
        if max(vram, compute) <= threshold:
            available.add(int(match.group(1)))
    return available


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: select_available_gpu.py <usage-threshold> <max-wait-seconds>", file=sys.stderr)
        return 2

    threshold = float(sys.argv[1])
    max_wait = int(sys.argv[2])
    deadline = time.monotonic() + max_wait
    poll_count = 10

    while time.monotonic() < deadline:
        samples = []
        for attempt in range(poll_count):
            current = available_gpus(threshold)
            samples.append(current)
            print(
                f"GPU availability sample {attempt + 1}/{poll_count}: {sorted(current)}",
                file=sys.stderr,
            )
            if attempt + 1 < poll_count:
                time.sleep(1)

        stable = sorted(set.intersection(*samples)) if samples else []
        if stable:
            print(stable[0])
            return 0
        print("No consistently idle GPU; retrying...", file=sys.stderr)

    print(f"ERROR: no idle GPU became available within {max_wait} seconds", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
