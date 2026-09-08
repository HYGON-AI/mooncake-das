#!/usr/bin/env python3
"""Wait for one consistently idle Hygon GPU and print its device ID."""

import os
import re
import shutil
import subprocess
import sys
import time


def available_gpus(threshold: float):
    hy_smi = "/opt/hyhal/bin/hy-smi"
    if not os.path.isfile(hy_smi):
        hy_smi = shutil.which("hy-smi")
    if not hy_smi:
        raise RuntimeError(
            "hy-smi not found at /opt/hyhal/bin/hy-smi or in PATH"
        )
    result = subprocess.run(
        [hy_smi],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
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

    try:
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
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"ERROR: failed to query Hygon GPUs: {error}", file=sys.stderr)
        return 1

    print(f"ERROR: no idle GPU became available within {max_wait} seconds", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
