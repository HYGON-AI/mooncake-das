#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

# Run the community store_kv_bench.py for Hygon CI against an already-running etcd/HA
# Mooncake Store deployment. The benchmark is launched only on the primary
# node; both primary and secondary mooncake_client processes provide storage.

set -Eeuo pipefail

unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY all_proxy

: "${BENCH_PY:=/work/store_kv_bench.py}"
: "${LOCAL_IP:?LOCAL_IP is required}"
: "${MASTER_SERVER:?MASTER_SERVER is required}"
: "${METADATA_SERVER:=${MASTER_SERVER}}"
: "${LOCAL_PORT:=15071}"
: "${NUMJOBS:=8}" "${IODEPTH:=4}" "${BATCH_SIZE:=100}"
: "${NR_OBJECTS:=3200}" "${WRITE_OBJECTS:=0}" "${RUNTIME:=60}"
: "${VALUE_SIZE:=10485760}" "${KEY_SIZE:=32}" "${KEY_PREFIX:=kvbench-rdma}" "${PATTERN:=rdma}"
: "${GLOBAL_SEGMENT_SIZE:=0}" "${LOCAL_BUFFER_SIZE:=42949672960}"
: "${MEMORY_REPLICA_NUM:=1}" "${NOF_REPLICA_NUM:=0}"
: "${IO_API:=plain}" "${LOG_LEVEL:=INFO}" "${PROTOCOL:=rdma}" "${DEVICE_NAME:=}"

[[ -f "${BENCH_PY}" ]] || { echo "ERROR: missing benchmark: ${BENCH_PY}" >&2; exit 1; }

MODE="${1:-all}"
shift || true

bench() {
    local mode="$1"
    local scenario="$2"
    shift 2
    echo
    echo "=== ${mode}: local=${LOCAL_IP} metadata=${METADATA_SERVER} master=${MASTER_SERVER} protocol=${PROTOCOL} ==="
    python3 "${BENCH_PY}" \
        --scenario "${scenario}" \
        --local-hostname "${LOCAL_IP}:${LOCAL_PORT}" \
        --metadata-server "${METADATA_SERVER}" \
        --master-server "${MASTER_SERVER}" \
        --protocol "${PROTOCOL}" \
        --device-name "${DEVICE_NAME}" \
        --global-segment-size "${GLOBAL_SEGMENT_SIZE}" \
        --local-buffer-size "${LOCAL_BUFFER_SIZE}" \
        --io-api "${IO_API}" \
        --numjobs "${NUMJOBS}" \
        --iodepth "${IODEPTH}" \
        --batch-size "${BATCH_SIZE}" \
        --nr-objects "${NR_OBJECTS}" \
        --write-objects "${WRITE_OBJECTS}" \
        --key-prefix "${KEY_PREFIX}-${mode}" \
        --key-size "${KEY_SIZE}" \
        --value-size "${VALUE_SIZE}" \
        --memory-replica-num "${MEMORY_REPLICA_NUM}" \
        --nof-replica-num "${NOF_REPLICA_NUM}" \
        --log-level "${LOG_LEVEL}" \
        "$@"
}

run_case() {
    local mode="$1"
    shift
    case "${mode}" in
        write_verify) bench write_verify verify_write --verify --pattern "${PATTERN}" "$@" ;;
        write_perf) bench write_perf write_perf --runtime "${RUNTIME}" "$@" ;;
        read_perf) bench read_perf read_perf --runtime "${RUNTIME}" --prepare-mode write --verify --pattern "${PATTERN}" "$@" ;;
        *) echo "ERROR: unknown mode: ${mode}" >&2; usage >&2; exit 2 ;;
    esac
}

usage() {
    cat <<EOF
Usage: $(basename "$0") [all|write_verify|write_perf|read_perf] [extra store_kv_bench args]

all runs: write_verify -> write_perf -> read_perf
EOF
}

case "${MODE}" in
    all)
        run_case write_verify "$@"
        run_case write_perf "$@"
        run_case read_perf "$@"
        ;;
    write_verify|write_perf|read_perf)
        run_case "${MODE}" "$@"
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        echo "ERROR: unknown mode: ${MODE}" >&2
        usage >&2
        exit 2
        ;;
esac
