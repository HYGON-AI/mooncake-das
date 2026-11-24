#!/usr/bin/env python3
"""Small-key writer for the Mooncake remount/timeout reproducer.

Writes fixed-size values with batch_put_from from one read-only registered
buffer. All worker threads share that buffer, so the writer spends its time in
the master RPC path, not in memcpy. Progress is emitted as JSON lines.
"""

from __future__ import annotations

import argparse
import ctypes
import json
import mmap
import os
import signal
import threading
import time
import uuid


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


def emit(event: str, **values: object) -> None:
    print(
        json.dumps(
            {"time": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "event": event,
             **values},
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--master-server", required=True)
    parser.add_argument("--metadata-server", required=True)
    parser.add_argument("--local-hostname", required=True)
    parser.add_argument("--protocol", default="rdma")
    parser.add_argument("--device-names", default="")
    parser.add_argument("--global-segment-size", required=True)
    parser.add_argument("--value-size", type=int, default=64)
    parser.add_argument("--batch-size", type=int, default=12288)
    parser.add_argument("--workers", type=int, default=128)
    parser.add_argument("--max-keys", type=int, default=0,
                        help="0 means write until stopped")
    parser.add_argument("--duration", type=float, default=0.0,
                        help="seconds; 0 means no limit")
    parser.add_argument("--progress-interval", type=float, default=5.0)
    parser.add_argument("--max-errors", type=int, default=0,
                        help="stop after this many failed batches; 0 = unlimited")
    parser.add_argument("--close-timeout", type=float, default=15.0,
                        help="force _exit if unregister/close hangs")
    parser.add_argument("--key-prefix", default="mc4")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args()

    from mooncake.store import MooncakeDistributedStore

    segment_size = parse_size(args.global_segment_size)
    run_id = args.run_id or uuid.uuid4().hex[:12]
    prefix = f"{args.key_prefix}-{run_id}-"

    store = MooncakeDistributedStore()
    rc = store.setup(
        args.local_hostname,
        args.metadata_server,
        segment_size,
        0,
        args.protocol,
        args.device_names,
        args.master_server,
    )
    if rc != 0:
        emit("setup_failed", rc=rc)
        return 1
    emit("setup_complete", health=store.health_check(),
         local_hostname=args.local_hostname, segment_size=segment_size,
         workers=args.workers, batch_size=args.batch_size,
         value_size=args.value_size, run_id=run_id)

    page = mmap.mmap(-1, 4096)
    page.write(b"m" * 4096)
    base = ctypes.addressof(ctypes.c_char.from_buffer(page))
    rc = store.register_buffer(base, 4096)
    if rc != 0:
        emit("register_buffer_failed", rc=rc)
        return 1

    lock = threading.Lock()
    stop = threading.Event()
    next_index = 0
    counters = {
        "batches": 0,
        "attempted": 0,
        "succeeded": 0,
        "failed": 0,
        "exceptions": 0,
        "result_codes": {},
        "first_error": None,
        "first_timeout_at": None,
    }

    def request_stop(_signum: int, _frame: object) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    def allocate() -> tuple[int, int]:
        nonlocal next_index
        with lock:
            if args.max_keys > 0 and next_index >= args.max_keys:
                stop.set()
                return 0, 0
            start = next_index
            take = args.batch_size
            if args.max_keys > 0:
                take = min(take, args.max_keys - start)
            next_index += take
        return start, take

    def record(codes: list[int], keys: list[str]) -> None:
        with lock:
            counters["batches"] += 1
            counters["attempted"] += len(keys)
            if len(codes) != len(keys):
                counters["exceptions"] += 1
                if counters["first_error"] is None:
                    counters["first_error"] = (
                        f"batch returned {len(codes)} codes for {len(keys)} keys"
                    )
            for code in codes:
                key = str(code)
                counters["result_codes"][key] = (
                    counters["result_codes"].get(key, 0) + 1
                )
                if code == 0:
                    counters["succeeded"] += 1
                else:
                    counters["failed"] += 1
                    if code == -901 and counters["first_timeout_at"] is None:
                        counters["first_timeout_at"] = time.strftime(
                            "%Y-%m-%dT%H:%M:%S%z"
                        )

    def worker() -> None:
        sizes = [args.value_size] * args.batch_size
        ptrs = [base] * args.batch_size
        while not stop.is_set():
            start, take = allocate()
            if take == 0:
                return
            keys = [f"{prefix}{start + i:012d}" for i in range(take)]
            try:
                codes = store.batch_put_from(keys, ptrs[:take], sizes[:take])
            except Exception as exc:  # noqa: BLE001 - report and continue
                with lock:
                    counters["batches"] += 1
                    counters["attempted"] += take
                    counters["failed"] += take
                    counters["exceptions"] += 1
                    if counters["first_error"] is None:
                        counters["first_error"] = (
                            f"{type(exc).__name__}: {exc}"
                        )
                if args.max_errors and counters["exceptions"] >= args.max_errors:
                    stop.set()
                continue
            record(list(codes), keys)
            if args.max_errors and counters["exceptions"] >= args.max_errors:
                stop.set()

    def progress() -> None:
        started = time.monotonic()
        while not stop.wait(max(0.05, args.progress_interval)):
            with lock:
                snapshot = dict(counters)
                snapshot["result_codes"] = dict(counters["result_codes"])
            elapsed = time.monotonic() - started
            snapshot["elapsed_s"] = round(elapsed, 3)
            snapshot["key_qps"] = (
                round(snapshot["attempted"] / elapsed, 2) if elapsed > 0 else 0.0
            )
            emit("progress", **snapshot)

    threads = [threading.Thread(target=worker, daemon=True)
               for _ in range(max(1, args.workers))]
    progress_thread = threading.Thread(target=progress, daemon=True)
    started = time.monotonic()
    for thread in threads:
        thread.start()
    progress_thread.start()

    if args.duration > 0:
        stop.wait(args.duration)
        stop.set()

    for thread in threads:
        thread.join()
    stop.set()
    progress_thread.join(timeout=2)

    with lock:
        final = dict(counters)
        final["result_codes"] = dict(counters["result_codes"])
    final["elapsed_s"] = round(time.monotonic() - started, 3)
    emit("complete", **final)

    # A stuck master can make unregister/close wait for the RPC timeout. The
    # reproducer must still exit so the next step can start.
    def hard_exit() -> None:
        os._exit(0)

    timer = threading.Timer(args.close_timeout, hard_exit)
    timer.daemon = True
    timer.start()
    try:
        store.unregister_buffer(base)
    except Exception as exc:  # noqa: BLE001
        emit("unregister_buffer_error", error=f"{type(exc).__name__}: {exc}")
    try:
        store.close()
    except Exception as exc:  # noqa: BLE001
        emit("close_error", error=f"{type(exc).__name__}: {exc}")
    timer.cancel()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
