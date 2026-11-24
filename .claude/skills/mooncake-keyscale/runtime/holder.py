#!/usr/bin/env python3
"""Mount one Mooncake segment and keep the client alive until SIGTERM.

Same role as ../hold_segment.py, but the wait is interruptible and SIGTERM
runs an explicit close(), so the client actually sends UnmountSegment instead
of being SIGKILLed after the docker stop grace period. Use `mc4.sh stop` or
`restart-holder` to exercise that path.
"""

from __future__ import annotations

import argparse
import os
import signal
import threading
import time


def parse_size(text: str) -> int:
    units = {
        "B": 1,
        "K": 1000, "KB": 1000, "KI": 1024, "KIB": 1024,
        "M": 1000 ** 2, "MB": 1000 ** 2, "MI": 1024 ** 2, "MIB": 1024 ** 2,
        "G": 1000 ** 3, "GB": 1000 ** 3, "GI": 1024 ** 3, "GIB": 1024 ** 3,
    }
    raw = text.strip()
    upper = raw.upper()
    for suffix, scale in sorted(units.items(), key=lambda item: -len(item[0])):
        if upper.endswith(suffix):
            number = raw[: -len(suffix)].strip()
            if number:
                return int(float(number) * scale)
    return int(raw, 0)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--master-server", required=True)
    parser.add_argument("--metadata-server", required=True)
    parser.add_argument("--local-hostname", required=True)
    parser.add_argument("--protocol", default="rdma")
    parser.add_argument("--device-names", default="")
    parser.add_argument("--global-segment-size", required=True)
    parser.add_argument("--close-timeout", type=float, default=10.0)
    args = parser.parse_args()
    size = parse_size(args.global_segment_size)

    from mooncake.store import MooncakeDistributedStore

    store = MooncakeDistributedStore()
    rc = store.setup(
        args.local_hostname,
        args.metadata_server,
        size,
        0,
        args.protocol,
        args.device_names,
        args.master_server,
    )
    if rc != 0:
        print(f"holder_setup_failed rc={rc}", flush=True)
        return 1
    print(
        f"holder_ready hostname={args.local_hostname} "
        f"device={args.device_names or 'auto'} bytes={size}",
        flush=True,
    )

    stop = threading.Event()

    def request_stop(_signum: int, _frame: object) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    while not stop.wait(1.0):
        pass

    print("holder_closing", flush=True)
    # close() unmounts the segment; it can wait on a master lock, so bound it.
    def hard_exit() -> None:
        os._exit(0)

    timer = threading.Timer(args.close_timeout, hard_exit)
    timer.daemon = True
    timer.start()
    try:
        store.close()
    except Exception as exc:  # noqa: BLE001
        print(f"holder_close_error {type(exc).__name__}: {exc}", flush=True)
    timer.cancel()
    print("holder_closed", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
