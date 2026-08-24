#!/usr/bin/env python3
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Exercise Mooncake Store put/get through etcd-based service discovery."""

from __future__ import annotations

import argparse
import os
import socket
import sys


def host_ip() -> str:
    configured = os.environ.get("MOONCAKE_LOCAL_IP", "").strip()
    if configured:
        return configured
    addresses = os.popen("hostname -I").read().split()
    return addresses[0] if addresses else socket.gethostname()


def make_value(size: int) -> bytes:
    value = b"world" * ((size + 4) // 5)
    return value[:size]


def main() -> int:
    parser = argparse.ArgumentParser(description="Mooncake Store cross-node put/get test")
    parser.add_argument("--key", required=True)
    parser.add_argument("--value-mb", type=float, default=1.0)
    parser.add_argument("--mode", choices=("put", "get", "both"), default="both")
    parser.add_argument(
        "--etcd",
        default=os.environ.get("MOONCAKE_ETCD_URL", ""),
        help="etcd://HOST:2379 used for HA master and transfer metadata",
    )
    parser.add_argument(
        "--preferred-segment",
        default=os.environ.get("MOONCAKE_PREFERRED_SEGMENT", ""),
        help="preferred segment name for the memory replica",
    )
    parser.add_argument(
        "--cross-put",
        action="store_true",
        help="use MOONCAKE_REMOTE_IP as the preferred segment",
    )
    args = parser.parse_args()

    etcd = args.etcd.strip()
    if not etcd:
        print("set --etcd or MOONCAKE_ETCD_URL", file=sys.stderr)
        return 2
    if not etcd.startswith("etcd://"):
        etcd = f"etcd://{etcd}"

    preferred = args.preferred_segment.strip()
    if args.cross_put:
        preferred = os.environ.get("MOONCAKE_REMOTE_IP", "").strip()
        if not preferred:
            print("--cross-put needs MOONCAKE_REMOTE_IP", file=sys.stderr)
            return 2

    local_ip = host_ip()
    size = max(1, int(args.value_mb * 1024 * 1024))
    value = make_value(size)

    from mooncake.store import MooncakeDistributedStore, ReplicateConfig

    print(
        f"local={local_ip} master={etcd} metadata={etcd} "
        f"mode={args.mode} preferred_segment={preferred or '(unset)'}"
    )
    store = MooncakeDistributedStore()
    setup_result = store.setup(
        local_ip,
        etcd,
        0,
        max(64 * 1024 * 1024, size * 2),
        "rdma",
        "",
        etcd,
    )
    if setup_result not in (0, None):
        raise SystemExit(f"setup failed: {setup_result}")

    config = ReplicateConfig()
    config.replica_num = 1
    if preferred:
        config.preferred_segment = preferred

    if args.mode in ("put", "both"):
        put_result = store.put(args.key, value, config)
        print(f"put: {put_result}")
        if put_result not in (0, None):
            store.close()
            return 1

    if args.mode in ("get", "both"):
        received = store.get(args.key)
        matches = received == value
        print(f"get: len={len(received) if received else 0} match={matches}")
        if not matches:
            store.close()
            return 1

    store.close()
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
