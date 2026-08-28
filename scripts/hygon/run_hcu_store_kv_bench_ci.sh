#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

set -Eeuo pipefail

usage() {
    cat <<'EOF'
Usage: bash scripts/hygon/run_hcu_store_kv_bench_ci.sh <standard-wheel-path>

Run the Mooncake Store etcd/HA RDMA KV benchmark in temporary containers.
The script runs on the primary node and controls the secondary node over SSH.
Node addresses are resolved at runtime and are not stored in this file.

Required environment variables:
  HCU_TEST_IMAGE            Container image used on both nodes
  HCU_TEST_DTK_PKG_URL      DTK tarball installed in both containers

Optional environment variables:
  HCU_TEST_TARGET_HOST      Primary host (default: nmz4)
  HCU_TEST_INITIATOR_HOST   Secondary host (default: nmz36)
  HCU_TEST_TARGET_IP        Primary service IPv4 (otherwise resolve host)
  HCU_TEST_INITIATOR_IP     Secondary service IPv4 (otherwise resolve host)
  HCU_TEST_INITIATOR_SSH_HOST  SSH address (default: secondary service IPv4)
  HCU_TEST_REMOTE_USER      Secondary SSH user (default: github)
  HCU_TEST_SSH_PORT         Secondary SSH port (default: 22)
  HCU_STORE_PRIMARY_FILTER  MC_TE_FILTERS on primary (default: mlx5_6)
  HCU_STORE_SECONDARY_FILTER MC_TE_FILTERS on secondary (default: auto-discovery)
  HCU_STORE_ETCD_URL        etcd archive URL (default: etcd v3.6.1)
  HCU_STORE_PORT_RANGE_MIN  Lowest auto-allocated TCP port (default: 20000)
  HCU_STORE_PORT_RANGE_MAX  Highest auto-allocated TCP port (default: 29999)
  HCU_CROSS_NODE_LOCK_FILE  Shared test lock file on both nodes
  HCU_CROSS_NODE_LOCK_TIMEOUT Seconds to wait for each node lock (default: 900)
  HCU_STORE_SETUP_TIMEOUT   Per-node setup timeout (default: 900)
  HCU_STORE_READY_TIMEOUT   Service readiness timeout (default: 180)
  HCU_STORE_RUN_TIMEOUT     Full Store benchmark timeout (default: 600)
  HCU_STORE_LOG_DIR         Test log directory
  HCU_TEST_REDACT_IPS       Replace service IPs in saved logs (default: true in CI)
  HCU_TEST_SSH_KEY          Optional SSH private key
  HCU_TEST_KNOWN_HOSTS_FILE Optional SSH known_hosts file
EOF
}

require_env() {
    local name="$1"
    if [ -z "${!name:-}" ]; then
        echo "ERROR: required environment variable is not set: ${name}" >&2
        exit 2
    fi
}

activate_runtime() {
    set +u
    # shellcheck disable=SC1091
    source /opt/dtk/env.sh
    set -u
}

check_ports_available() {
    local node_name="$1"
    shift
    python3 - "${node_name}" "$@" <<'PY'
import socket
import sys

node_name = sys.argv[1]
sockets = []
try:
    for raw_port in sys.argv[2:]:
        port = int(raw_port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        try:
            # This socket is only a conservative availability probe and is
            # closed before any service starts; it is not a listening service.
            sock.bind(("0.0.0.0", port))
        except OSError as exc:
            print(f"ERROR: {node_name} TCP port {port} is unavailable: {exc}", file=sys.stderr)
            sys.exit(1)
        sockets.append(sock)
    print(f"Store CI ports are available on {node_name}: {' '.join(sys.argv[2:])}")
finally:
    for sock in sockets:
        sock.close()
PY
}

allocate_free_ports() {
    local count="$1"
    local range_min="$2"
    local range_max="$3"
    python3 - "${count}" "${range_min}" "${range_max}" <<'PY'
import random
import socket
import sys

count, range_min, range_max = map(int, sys.argv[1:])
candidates = list(range(range_min, range_max + 1))
random.SystemRandom().shuffle(candidates)
sockets = []
ports = []
try:
    for port in candidates:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        try:
            # Hold every candidate socket until the full unique set is chosen.
            sock.bind(("0.0.0.0", port))
        except OSError:
            sock.close()
            continue
        sockets.append(sock)
        ports.append(port)
        if len(ports) == count:
            print(" ".join(map(str, ports)))
            break
    else:
        print(
            f"ERROR: could not allocate {count} ports from {range_min}-{range_max}",
            file=sys.stderr,
        )
        sys.exit(1)
finally:
    for sock in sockets:
        sock.close()
PY
}

prepare_container() {
    local wheel_path="$1"
    local install_etcd="$2"
    local dtk_tarball="/tmp/$(basename "${HCU_TEST_DTK_PKG_URL}")"
    local dtk_dir=""

    wget -q "${HCU_TEST_DTK_PKG_URL}" -O "${dtk_tarball}"
    tar -xzf "${dtk_tarball}" -C /opt
    dtk_dir="$(find /opt -mindepth 1 -maxdepth 1 -type d -name 'dtk-*' -print -quit)"
    if [ -z "${dtk_dir}" ]; then
        echo "ERROR: the DTK archive did not create /opt/dtk-*" >&2
        exit 1
    fi
    ln -s "${dtk_dir}" /opt/dtk
    activate_runtime

    python3 -m pip install "${wheel_path}"
    command -v mooncake_master >/dev/null
    command -v mooncake_client >/dev/null
    python3 -c 'from mooncake.store import MooncakeDistributedStore, ReplicateConfig'

    if [ "${install_etcd}" = "true" ]; then
        local archive="/tmp/etcd.tar.gz"
        local extracted=""
        wget -q "${HCU_STORE_ETCD_URL}" -O "${archive}"
        tar -xzf "${archive}" -C /tmp
        extracted="$(find /tmp -mindepth 1 -maxdepth 1 -type d -name 'etcd-v*-linux-amd64' -print -quit)"
        if [ -z "${extracted}" ]; then
            echo "ERROR: etcd archive did not contain the expected directory" >&2
            exit 1
        fi
        mkdir -p /tmp/etcd-bin
        cp "${extracted}/etcd" "${extracted}/etcdctl" /tmp/etcd-bin/
        /tmp/etcd-bin/etcd --version
    fi
}

run_etcd() {
    local primary_ip="$1"
    local client_port="$2"
    local peer_port="$3"
    exec /tmp/etcd-bin/etcd \
        --name mooncake-ci \
        --data-dir /tmp/mooncake-store-etcd \
        --listen-client-urls "http://${primary_ip}:${client_port}" \
        --advertise-client-urls "http://${primary_ip}:${client_port}" \
        --listen-peer-urls "http://${primary_ip}:${peer_port}" \
        --initial-advertise-peer-urls "http://${primary_ip}:${peer_port}" \
        --initial-cluster "mooncake-ci=http://${primary_ip}:${peer_port}"
}

run_master() {
    local local_ip="$1"
    local primary_ip="$2"
    local etcd_client_port="$3"
    local rpc_port="$4"
    local metrics_port="$5"
    activate_runtime
    unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY all_proxy
    exec mooncake_master \
        --enable_ha=true \
        --etcd_endpoints="${primary_ip}:${etcd_client_port}" \
        --rpc_address="${local_ip}" \
        --rpc_port="${rpc_port}" \
        --metrics_port="${metrics_port}" \
        --default_kv_lease_ttl=300000 \
        --logtostderr=true
}

run_client() {
    local local_ip="$1"
    local primary_ip="$2"
    local device_filter="$3"
    local etcd_client_port="$4"
    local client_port="$5"
    activate_runtime
    unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY all_proxy
    if [ "${device_filter}" = "__auto__" ]; then
        device_filter=""
    fi
    if [ -n "${device_filter}" ]; then
        export MC_TE_FILTERS="${device_filter}"
    else
        unset MC_TE_FILTERS || true
    fi
    exec mooncake_client \
        --host="${local_ip}" \
        --port="${client_port}" \
        --global_segment_size=40GB \
        --master_server_address="etcd://${primary_ip}:${etcd_client_port}" \
        --metadata_server="etcd://${primary_ip}:${etcd_client_port}" \
        --protocol=rdma \
        --logtostderr=true
}

run_store_benchmark() {
    local local_ip="$1"
    local primary_ip="$2"
    local device_filter="$3"
    local etcd_client_port="$4"
    local bench_port="$5"
    shift 5
    activate_runtime
    unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY all_proxy
    export LOCAL_IP="${local_ip}"
    export MASTER_SERVER="etcd://${primary_ip}:${etcd_client_port}"
    export METADATA_SERVER="etcd://${primary_ip}:${etcd_client_port}"
    export LOCAL_PORT="${bench_port}"
    export BENCH_PY=/work/store_kv_bench.py
    if [ "${device_filter}" = "__auto__" ]; then
        device_filter=""
    fi
    if [ -n "${device_filter}" ]; then
        export MC_TE_FILTERS="${device_filter}"
    else
        unset MC_TE_FILTERS || true
    fi
    exec bash /work/run_store_kv_bench.sh "$@"
}

case "${1:-}" in
    __container_prepare)
        require_env HCU_TEST_DTK_PKG_URL
        require_env HCU_STORE_ETCD_URL
        prepare_container "$2" "$3"
        exit 0
        ;;
    __container_check_ports)
        shift
        check_ports_available "$@"
        exit 0
        ;;
    __host_allocate_ports)
        allocate_free_ports "$2" "$3" "$4"
        exit 0
        ;;
    __container_etcd)
        run_etcd "$2" "$3" "$4"
        exit 0
        ;;
    __container_master)
        run_master "$2" "$3" "$4" "$5" "$6"
        exit 0
        ;;
    __container_client)
        run_client "$2" "$3" "$4" "$5" "$6"
        exit 0
        ;;
    __container_benchmark)
        shift
        run_store_benchmark "$@"
        exit 0
        ;;
esac

if [ "$#" -ne 1 ]; then
    usage >&2
    exit 2
fi

require_env HCU_TEST_IMAGE
require_env HCU_TEST_DTK_PKG_URL

for command_name in awk docker flock getent grep python3 realpath scp sed ssh timeout; do
    command -v "${command_name}" >/dev/null 2>&1 || {
        echo "ERROR: required command is missing: ${command_name}" >&2
        exit 1
    }
done

WHEEL_PATH="$(realpath "$1")"
SELF_PATH="$(realpath "${BASH_SOURCE[0]}")"
BENCH_SCRIPT_PATH="$(realpath "$(dirname "${BASH_SOURCE[0]}")/run_store_kv_bench.sh")"
BENCH_PY_PATH="$(realpath "$(dirname "${BASH_SOURCE[0]}")/../../mooncake-store/benchmarks/store_kv_bench.py")"
if [ ! -f "${WHEEL_PATH}" ]; then
    echo "ERROR: wheel not found: ${WHEEL_PATH}" >&2
    exit 1
fi

PRIMARY_HOST="${HCU_TEST_TARGET_HOST:-nmz4}"
SECONDARY_HOST="${HCU_TEST_INITIATOR_HOST:-nmz36}"
REMOTE_USER="${HCU_TEST_REMOTE_USER:-github}"
SSH_PORT="${HCU_TEST_SSH_PORT:-22}"
PRIMARY_FILTER="${HCU_STORE_PRIMARY_FILTER:-mlx5_6}"
SECONDARY_FILTER="${HCU_STORE_SECONDARY_FILTER:-__auto__}"
ETCD_URL="${HCU_STORE_ETCD_URL:-https://github.com/etcd-io/etcd/releases/download/v3.6.1/etcd-v3.6.1-linux-amd64.tar.gz}"
PORT_RANGE_MIN="${HCU_STORE_PORT_RANGE_MIN:-20000}"
PORT_RANGE_MAX="${HCU_STORE_PORT_RANGE_MAX:-29999}"
LOCK_FILE="${HCU_CROSS_NODE_LOCK_FILE:-/tmp/mooncake-hcu-cross-node.lock}"
LOCK_TIMEOUT="${HCU_CROSS_NODE_LOCK_TIMEOUT:-900}"
SETUP_TIMEOUT="${HCU_STORE_SETUP_TIMEOUT:-900}"
READY_TIMEOUT="${HCU_STORE_READY_TIMEOUT:-180}"
RUN_TIMEOUT="${HCU_STORE_RUN_TIMEOUT:-600}"
REDACT_IPS="${HCU_TEST_REDACT_IPS:-${GITHUB_ACTIONS:-false}}"
TMP_ROOT="${RUNNER_TEMP:-/tmp}"
LOG_DIR="${HCU_STORE_LOG_DIR:-${TMP_ROOT}/hcu-store-logs}"
RUN_TOKEN="${GITHUB_RUN_ID:-manual}-${GITHUB_RUN_ATTEMPT:-0}-$$"
PRIMARY_CONTAINER="mooncake-store-primary-${RUN_TOKEN}"
SECONDARY_CONTAINER="mooncake-store-secondary-${RUN_TOKEN}"
REMOTE_WORK_DIR="/tmp/mooncake-hcu-store-${RUN_TOKEN}"
WHEEL_BASENAME="$(basename "${WHEEL_PATH}")"

if ! [[ "${PORT_RANGE_MIN}" =~ ^[0-9]+$ && "${PORT_RANGE_MAX}" =~ ^[0-9]+$ ]] || \
    ((PORT_RANGE_MIN < 1024 || PORT_RANGE_MAX > 65535 || PORT_RANGE_MAX - PORT_RANGE_MIN < 32)); then
    echo "ERROR: invalid Store port range ${PORT_RANGE_MIN}-${PORT_RANGE_MAX}" >&2
    exit 2
fi
if ! [[ "${LOCK_TIMEOUT}" =~ ^[0-9]+$ ]] || ((LOCK_TIMEOUT < 1)); then
    echo "ERROR: HCU_CROSS_NODE_LOCK_TIMEOUT must be a positive integer" >&2
    exit 2
fi

resolve_ipv4() {
    local host="$1"
    local configured_ip="$2"
    if [ -n "${configured_ip}" ]; then
        printf '%s\n' "${configured_ip}"
        return
    fi
    getent ahostsv4 "${host}" 2>/dev/null | awk 'NR == 1 { print $1 }' || true
}

PRIMARY_IP="$(resolve_ipv4 "${PRIMARY_HOST}" "${HCU_TEST_TARGET_IP:-}")"
SECONDARY_IP="$(resolve_ipv4 "${SECONDARY_HOST}" "${HCU_TEST_INITIATOR_IP:-}")"
if [ -z "${PRIMARY_IP}" ] || [ -z "${SECONDARY_IP}" ]; then
    echo "ERROR: failed to determine the primary or secondary IPv4 address" >&2
    exit 1
fi
SECONDARY_SSH_HOST="${HCU_TEST_INITIATOR_SSH_HOST:-${SECONDARY_IP}}"
REMOTE="${REMOTE_USER}@${SECONDARY_SSH_HOST}"

if [ -n "${PIP_INDEX_URL:-}" ] && [ -z "${PIP_TRUSTED_HOST:-}" ]; then
    pip_host="${PIP_INDEX_URL#*://}"
    export PIP_TRUSTED_HOST="${pip_host%%[:/]*}"
fi

SSH_OPTIONS=(-p "${SSH_PORT}" -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=10 -o ServerAliveCountMax=3 -o StrictHostKeyChecking=yes)
SCP_OPTIONS=(-P "${SSH_PORT}" -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes)
if [ -n "${HCU_TEST_SSH_KEY:-}" ]; then
    SSH_OPTIONS+=(-i "${HCU_TEST_SSH_KEY}")
    SCP_OPTIONS+=(-i "${HCU_TEST_SSH_KEY}")
fi
if [ -n "${HCU_TEST_KNOWN_HOSTS_FILE:-}" ]; then
    SSH_OPTIONS+=(-o "UserKnownHostsFile=${HCU_TEST_KNOWN_HOSTS_FILE}")
    SCP_OPTIONS+=(-o "UserKnownHostsFile=${HCU_TEST_KNOWN_HOSTS_FILE}")
fi

mkdir -p "${LOG_DIR}"
LOG_NAMES=(primary-lock secondary-lock primary-port-check secondary-port-check primary-prepare secondary-prepare etcd primary-master secondary-master primary-client secondary-client store-kv-bench)
for log_name in "${LOG_NAMES[@]}"; do
    : >"${LOG_DIR}/${log_name}.log"
done

PRIMARY_LOCK_PID=""
SECONDARY_LOCK_PID=""

print_logs() {
    local log_file=""
    for log_file in "${LOG_DIR}"/*.log; do
        echo "===== $(basename "${log_file}") ====="
        cat "${log_file}"
    done
}

redact_logs() {
    if [ "${REDACT_IPS}" != "true" ]; then
        return
    fi
    local primary_pattern="${PRIMARY_IP//./\.}"
    local secondary_pattern="${SECONDARY_IP//./\.}"
    sed -i -e "s/${primary_pattern}/${PRIMARY_HOST}/g" -e "s/${secondary_pattern}/${SECONDARY_HOST}/g" "${LOG_DIR}"/*.log
}

cleanup() {
    local rc=$?
    trap - EXIT
    set +e
    docker rm -f "${PRIMARY_CONTAINER}" >/dev/null 2>&1 || true
    ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- "${SECONDARY_CONTAINER}" "${REMOTE_WORK_DIR}" <<'REMOTE_CLEANUP' >/dev/null 2>&1
container_name="$1"
remote_dir="$2"
docker rm -f "${container_name}" >/dev/null 2>&1 || true
case "${remote_dir}" in
    /tmp/mooncake-hcu-store-*) rm -rf -- "${remote_dir}" ;;
    *) echo "Refusing to remove unexpected path: ${remote_dir}" >&2 ;;
esac
REMOTE_CLEANUP
    if [ -n "${SECONDARY_LOCK_PID}" ]; then
        kill "${SECONDARY_LOCK_PID}" >/dev/null 2>&1 || true
        wait "${SECONDARY_LOCK_PID}" >/dev/null 2>&1 || true
    fi
    if [ -n "${PRIMARY_LOCK_PID}" ]; then
        kill "${PRIMARY_LOCK_PID}" >/dev/null 2>&1 || true
        wait "${PRIMARY_LOCK_PID}" >/dev/null 2>&1 || true
    fi
    redact_logs
    if [ "${rc}" -ne 0 ]; then
        print_logs
    fi
    exit "${rc}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_for_log() {
    local log_file="$1"
    local pattern="$2"
    local process_id="$3"
    local description="$4"
    local attempt=0
    for ((attempt = 1; attempt <= READY_TIMEOUT; attempt++)); do
        if grep -q "${pattern}" "${log_file}"; then
            return 0
        fi
        if ! kill -0 "${process_id}" 2>/dev/null; then
            echo "ERROR: ${description} exited before becoming ready" >&2
            return 1
        fi
        sleep 1
    done
    echo "ERROR: ${description} was not ready within ${READY_TIMEOUT}s" >&2
    return 1
}

wait_for_lock() {
    local log_file="$1"
    local process_id="$2"
    local node_name="$3"
    local attempt=0
    for ((attempt = 1; attempt <= LOCK_TIMEOUT + 10; attempt++)); do
        if grep -q '^LOCK_ACQUIRED$' "${log_file}"; then
            return 0
        fi
        if ! kill -0 "${process_id}" 2>/dev/null; then
            echo "ERROR: failed to acquire the cross-node test lock on ${node_name}" >&2
            return 1
        fi
        sleep 1
    done
    echo "ERROR: timed out waiting for the cross-node test lock on ${node_name}" >&2
    return 1
}

echo "Primary:   ${PRIMARY_HOST}"
echo "Secondary: ${SECONDARY_HOST}"
echo "Image:     ${HCU_TEST_IMAGE}"

echo "Acquiring the cross-node test lock on ${PRIMARY_HOST}..."
PARENT_PID="$$"
(
    exec 9>"${LOCK_FILE}"
    if ! flock -w "${LOCK_TIMEOUT}" 9; then
        echo "ERROR: lock wait timed out"
        exit 75
    fi
    echo "LOCK_ACQUIRED"
    while kill -0 "${PARENT_PID}" 2>/dev/null; do sleep 5; done
) >"${LOG_DIR}/primary-lock.log" 2>&1 &
PRIMARY_LOCK_PID=$!
wait_for_lock "${LOG_DIR}/primary-lock.log" "${PRIMARY_LOCK_PID}" "${PRIMARY_HOST}"

echo "Acquiring the cross-node test lock on ${SECONDARY_HOST}..."
ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- "${LOCK_FILE}" "${LOCK_TIMEOUT}" \
    >"${LOG_DIR}/secondary-lock.log" 2>&1 <<'REMOTE_LOCK' &
set -Eeuo pipefail
lock_file="$1"
lock_timeout="$2"
session_parent="$PPID"
command -v flock >/dev/null
exec 9>"${lock_file}"
if ! flock -w "${lock_timeout}" 9; then
    echo "ERROR: lock wait timed out"
    exit 75
fi
echo "LOCK_ACQUIRED"
while kill -0 "${session_parent}" 2>/dev/null; do sleep 5; done
REMOTE_LOCK
SECONDARY_LOCK_PID=$!
wait_for_lock "${LOG_DIR}/secondary-lock.log" "${SECONDARY_LOCK_PID}" "${SECONDARY_HOST}"
echo "Cross-node test locks acquired."

ssh "${SSH_OPTIONS[@]}" "${REMOTE}" mkdir -p "${REMOTE_WORK_DIR}"
scp "${SCP_OPTIONS[@]}" "${WHEEL_PATH}" "${SELF_PATH}" "${REMOTE}:${REMOTE_WORK_DIR}/"

DOCKER_ENV_ARGS=(-e "HCU_TEST_DTK_PKG_URL=${HCU_TEST_DTK_PKG_URL}" -e "HCU_STORE_ETCD_URL=${ETCD_URL}")
[ -n "${PIP_INDEX_URL:-}" ] && DOCKER_ENV_ARGS+=(-e "PIP_INDEX_URL=${PIP_INDEX_URL}")
[ -n "${PIP_TRUSTED_HOST:-}" ] && DOCKER_ENV_ARGS+=(-e "PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST}")

docker run -d --name "${PRIMARY_CONTAINER}" --network host --privileged \
    --device /dev/kfd:/dev/kfd --device /dev/dri:/dev/dri --cap-add SYS_PTRACE \
    --volume /opt/hyhal:/opt/hyhal:ro \
    --volume "${WHEEL_PATH}:/work/${WHEEL_BASENAME}:ro" \
    --volume "${SELF_PATH}:/work/run_hcu_store_kv_bench_ci.sh:ro" \
    --volume "${BENCH_SCRIPT_PATH}:/work/run_store_kv_bench.sh:ro" \
    --volume "${BENCH_PY_PATH}:/work/store_kv_bench.py:ro" \
    "${DOCKER_ENV_ARGS[@]}" --entrypoint /bin/bash "${HCU_TEST_IMAGE}" -lc 'exec sleep infinity' >/dev/null

ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- \
    "${REMOTE_WORK_DIR}" "${WHEEL_BASENAME}" "$(basename "${SELF_PATH}")" \
    "${SECONDARY_CONTAINER}" "${HCU_TEST_IMAGE}" "${HCU_TEST_DTK_PKG_URL}" "${ETCD_URL}" \
    "${PIP_INDEX_URL:-}" "${PIP_TRUSTED_HOST:-}" <<'REMOTE_CONTAINER' >/dev/null
set -Eeuo pipefail
remote_dir="$1"; wheel="$2"; script="$3"; container="$4"; image="$5"
dtk_url="$6"; etcd_url="$7"; pip_index="$8"; pip_trusted="$9"
env_args=(-e "HCU_TEST_DTK_PKG_URL=${dtk_url}" -e "HCU_STORE_ETCD_URL=${etcd_url}")
[ -n "${pip_index}" ] && env_args+=(-e "PIP_INDEX_URL=${pip_index}")
[ -n "${pip_trusted}" ] && env_args+=(-e "PIP_TRUSTED_HOST=${pip_trusted}")
docker run -d --name "${container}" --network host --privileged \
    --device /dev/kfd:/dev/kfd --device /dev/dri:/dev/dri --cap-add SYS_PTRACE \
    --volume /opt/hyhal:/opt/hyhal:ro \
    --volume "${remote_dir}/${wheel}:/work/${wheel}:ro" \
    --volume "${remote_dir}/${script}:/work/run_hcu_store_kv_bench_ci.sh:ro" \
    "${env_args[@]}" --entrypoint /bin/bash "${image}" -lc 'exec sleep infinity'
REMOTE_CONTAINER

echo "Preparing DTK and standard wheel on both nodes in parallel..."
timeout "${SETUP_TIMEOUT}" docker exec "${PRIMARY_CONTAINER}" /bin/bash /work/run_hcu_store_kv_bench_ci.sh \
    __container_prepare "/work/${WHEEL_BASENAME}" true >"${LOG_DIR}/primary-prepare.log" 2>&1 &
PRIMARY_PREP_PID=$!
ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- "${SECONDARY_CONTAINER}" "${WHEEL_BASENAME}" "${SETUP_TIMEOUT}" \
    >"${LOG_DIR}/secondary-prepare.log" 2>&1 <<'REMOTE_PREPARE' &
set -Eeuo pipefail
timeout "$3" docker exec "$1" /bin/bash /work/run_hcu_store_kv_bench_ci.sh __container_prepare "/work/$2" false
REMOTE_PREPARE
SECONDARY_PREP_PID=$!

set +e
wait "${PRIMARY_PREP_PID}"; PRIMARY_PREP_RC=$?
wait "${SECONDARY_PREP_PID}"; SECONDARY_PREP_RC=$?
set -e
if [ "${PRIMARY_PREP_RC}" -ne 0 ] || [ "${SECONDARY_PREP_RC}" -ne 0 ]; then
    echo "ERROR: Store environment preparation failed (primary=${PRIMARY_PREP_RC}, secondary=${SECONDARY_PREP_RC})" >&2
    exit 1
fi

read -r ETCD_CLIENT_PORT ETCD_PEER_PORT PRIMARY_MASTER_RPC_PORT \
    PRIMARY_MASTER_METRICS_PORT PRIMARY_CLIENT_PORT BENCH_PORT \
    <<<"$(allocate_free_ports 6 "${PORT_RANGE_MIN}" "${PORT_RANGE_MAX}")"
read -r SECONDARY_MASTER_RPC_PORT SECONDARY_MASTER_METRICS_PORT SECONDARY_CLIENT_PORT \
    <<<"$(ssh "${SSH_OPTIONS[@]}" "${REMOTE}" /bin/bash \
        "${REMOTE_WORK_DIR}/$(basename "${SELF_PATH}")" __host_allocate_ports \
        3 "${PORT_RANGE_MIN}" "${PORT_RANGE_MAX}")"

for allocated_port in \
    "${ETCD_CLIENT_PORT}" "${ETCD_PEER_PORT}" \
    "${PRIMARY_MASTER_RPC_PORT}" "${PRIMARY_MASTER_METRICS_PORT}" \
    "${PRIMARY_CLIENT_PORT}" "${BENCH_PORT}" \
    "${SECONDARY_MASTER_RPC_PORT}" "${SECONDARY_MASTER_METRICS_PORT}" \
    "${SECONDARY_CLIENT_PORT}"; do
    if ! [[ "${allocated_port}" =~ ^[0-9]+$ ]]; then
        echo "ERROR: automatic Store port allocation returned an invalid value" >&2
        exit 1
    fi
done
echo "Primary ports:   etcd=${ETCD_CLIENT_PORT}/${ETCD_PEER_PORT} master=${PRIMARY_MASTER_RPC_PORT}/${PRIMARY_MASTER_METRICS_PORT} client=${PRIMARY_CLIENT_PORT} bench=${BENCH_PORT}"
echo "Secondary ports: master=${SECONDARY_MASTER_RPC_PORT}/${SECONDARY_MASTER_METRICS_PORT} client=${SECONDARY_CLIENT_PORT}"

echo "Checking auto-allocated Store CI ports on both nodes..."
docker exec "${PRIMARY_CONTAINER}" /bin/bash /work/run_hcu_store_kv_bench_ci.sh \
    __container_check_ports "${PRIMARY_HOST}" \
    "${ETCD_CLIENT_PORT}" "${ETCD_PEER_PORT}" "${PRIMARY_MASTER_RPC_PORT}" \
    "${PRIMARY_MASTER_METRICS_PORT}" "${PRIMARY_CLIENT_PORT}" "${BENCH_PORT}" \
    >"${LOG_DIR}/primary-port-check.log" 2>&1
ssh "${SSH_OPTIONS[@]}" "${REMOTE}" docker exec "${SECONDARY_CONTAINER}" \
    /bin/bash /work/run_hcu_store_kv_bench_ci.sh \
    __container_check_ports "${SECONDARY_HOST}" \
    "${SECONDARY_MASTER_RPC_PORT}" "${SECONDARY_MASTER_METRICS_PORT}" "${SECONDARY_CLIENT_PORT}" \
    >"${LOG_DIR}/secondary-port-check.log" 2>&1

docker exec "${PRIMARY_CONTAINER}" /bin/bash /work/run_hcu_store_kv_bench_ci.sh __container_etcd \
    "${PRIMARY_IP}" "${ETCD_CLIENT_PORT}" "${ETCD_PEER_PORT}" \
    >"${LOG_DIR}/etcd.log" 2>&1 &
ETCD_PID=$!
for ((attempt = 1; attempt <= READY_TIMEOUT; attempt++)); do
    if docker exec "${PRIMARY_CONTAINER}" /tmp/etcd-bin/etcdctl --endpoints="http://${PRIMARY_IP}:${ETCD_CLIENT_PORT}" endpoint health >/dev/null 2>&1; then
        break
    fi
    kill -0 "${ETCD_PID}" 2>/dev/null || { echo "ERROR: etcd exited during startup" >&2; exit 1; }
    sleep 1
done
if ! docker exec "${PRIMARY_CONTAINER}" /tmp/etcd-bin/etcdctl --endpoints="http://${PRIMARY_IP}:${ETCD_CLIENT_PORT}" endpoint health >/dev/null 2>&1; then
    echo "ERROR: etcd did not become healthy" >&2
    exit 1
fi

docker exec "${PRIMARY_CONTAINER}" /bin/bash /work/run_hcu_store_kv_bench_ci.sh __container_master \
    "${PRIMARY_IP}" "${PRIMARY_IP}" "${ETCD_CLIENT_PORT}" "${PRIMARY_MASTER_RPC_PORT}" "${PRIMARY_MASTER_METRICS_PORT}" \
    >"${LOG_DIR}/primary-master.log" 2>&1 &
PRIMARY_MASTER_PID=$!
wait_for_log "${LOG_DIR}/primary-master.log" 'Master runtime state -> serving, role=leader' "${PRIMARY_MASTER_PID}" "primary master"

ssh "${SSH_OPTIONS[@]}" "${REMOTE}" docker exec "${SECONDARY_CONTAINER}" \
    /bin/bash /work/run_hcu_store_kv_bench_ci.sh __container_master \
    "${SECONDARY_IP}" "${PRIMARY_IP}" "${ETCD_CLIENT_PORT}" "${SECONDARY_MASTER_RPC_PORT}" "${SECONDARY_MASTER_METRICS_PORT}" \
    >"${LOG_DIR}/secondary-master.log" 2>&1 &
SECONDARY_MASTER_PID=$!
wait_for_log "${LOG_DIR}/secondary-master.log" 'Master runtime state -> standby, role=standby' "${SECONDARY_MASTER_PID}" "secondary master"

docker exec "${PRIMARY_CONTAINER}" /bin/bash /work/run_hcu_store_kv_bench_ci.sh __container_client \
    "${PRIMARY_IP}" "${PRIMARY_IP}" "${PRIMARY_FILTER}" "${ETCD_CLIENT_PORT}" "${PRIMARY_CLIENT_PORT}" \
    >"${LOG_DIR}/primary-client.log" 2>&1 &
PRIMARY_CLIENT_PID=$!
ssh "${SSH_OPTIONS[@]}" "${REMOTE}" docker exec "${SECONDARY_CONTAINER}" \
    /bin/bash /work/run_hcu_store_kv_bench_ci.sh __container_client \
    "${SECONDARY_IP}" "${PRIMARY_IP}" "${SECONDARY_FILTER}" "${ETCD_CLIENT_PORT}" "${SECONDARY_CLIENT_PORT}" \
    >"${LOG_DIR}/secondary-client.log" 2>&1 &
SECONDARY_CLIENT_PID=$!
wait_for_log "${LOG_DIR}/primary-client.log" 'Starting real client service' "${PRIMARY_CLIENT_PID}" "primary client"
wait_for_log "${LOG_DIR}/secondary-client.log" 'Starting real client service' "${SECONDARY_CLIENT_PID}" "secondary client"

timeout "${RUN_TIMEOUT}" docker exec "${PRIMARY_CONTAINER}" /bin/bash /work/run_hcu_store_kv_bench_ci.sh \
    __container_benchmark "${PRIMARY_IP}" "${PRIMARY_IP}" "${PRIMARY_FILTER}" \
    "${ETCD_CLIENT_PORT}" "${BENCH_PORT}" all \
    >"${LOG_DIR}/store-kv-bench.log" 2>&1

for phase in write_verify verify_read write_perf prepare_write read_perf; do
    grep -q "=== phase ${phase} ===" "${LOG_DIR}/store-kv-bench.log"
done
if grep -Eq 'failed_requests=[1-9]|failed_kvs=[1-9]|misses=[1-9]|verify_failures=[1-9]| ERROR |Traceback' \
    "${LOG_DIR}/store-kv-bench.log"; then
    echo "ERROR: Mooncake Store benchmark reported failures" >&2
    exit 1
fi

echo "Mooncake Store full RDMA KV benchmark passed."
grep -E '=== |failed_requests|failed_kvs|misses|verify_failures|overall summary' \
    "${LOG_DIR}/store-kv-bench.log"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
        echo "### Mooncake Store etcd/HA RDMA KV benchmark"
        echo
        echo "- Primary: \`${PRIMARY_HOST}\`"
        echo "- Secondary: \`${SECONDARY_HOST}\`"
        echo "- write_verify: passed"
        echo "- write_perf: passed"
        echo "- read_perf: passed"
    } >>"${GITHUB_STEP_SUMMARY}"
fi
