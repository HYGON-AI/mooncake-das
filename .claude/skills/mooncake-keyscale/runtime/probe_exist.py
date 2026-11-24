#!/usr/bin/env python3
"""Time a tiny batch_is_exist against a busy Mooncake master.

The key does not need to exist. The RPC still has to be served, so the latency
shows whether the master's RPC thread is stuck behind large writes.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import time


def emit(event: str, **values) -> None:
    print(
        json.dumps(
            {"time": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "event": event, **values},
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--master-server", required=True)
    parser.add_argument("--metadata-server", required=True)
    parser.add_argument("--local-hostname", default=os.getenv("NODE_IP", ""))
    parser.add_argument("--protocol", default="rdma")
    parser.add_argument("--device-names", default="shca_0")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--warn-ms", type=float, default=1000.0)
    args = parser.parse_args()
    if not args.local_hostname:
        args.local_hostname = socket.gethostbyname(socket.gethostname())

    from mooncake.store import MooncakeDistributedStore

    store = MooncakeDistributedStore()
    rc = store.setup(
        args.local_hostname,
        args.metadata_server,
        0,
        0,
        args.protocol,
        args.device_names,
        args.master_server,
    )
    if rc != 0:
        emit("setup_failed", rc=rc)
        return 1
    emit("setup_complete", health_check=store.health_check())

    key = ["probe-exist-not-written"]
    while True:
        started = time.perf_counter()
        error = ""
        try:
            result = store.batch_is_exist(key)
        except Exception as exc:
            result = None
            error = f"{type(exc).__name__}: {exc}"
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        payload = {"elapsed_ms": round(elapsed_ms, 3), "result": str(result)}
        if error:
            payload["error"] = error
        emit("stall" if elapsed_ms >= args.warn_ms else "probe", **payload)
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
