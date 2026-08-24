#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

set -Eeuo pipefail

usage() {
    cat <<'EOF'
Usage: bash scripts/test_hcu_store_wheel.sh <standard-wheel-path>

Run the Mooncake Store etcd/HA cross-node tests in temporary containers.
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
  HCU_STORE_SETUP_TIMEOUT   Per-node setup timeout (default: 900)
  HCU_STORE_READY_TIMEOUT   Service readiness timeout (default: 180)
  HCU_STORE_RUN_TIMEOUT     Each put/get timeout (default: 180)
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
    exec /tmp/etcd-bin/etcd \
        --name mooncake-ci \
        --data-dir /tmp/mooncake-store-etcd \
        --listen-client-urls http://0.0.0.0:2379 \
        --advertise-client-urls "http://${primary_ip}:2379" \
        --listen-peer-urls http://0.0.0.0:2380 \
        --initial-advertise-peer-urls "http://${primary_ip}:2380" \
        --initial-cluster "mooncake-ci=http://${primary_ip}:2380"
}

run_master() {
    local local_ip="$1"
    local primary_ip="$2"
    activate_runtime
    unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY all_proxy
    exec mooncake_master \
        --enable_ha=true \
        --etcd_endpoints="${primary_ip}:2379" \
        --rpc_address="${local_ip}" \
        --rpc_port=50051 \
        --default_kv_lease_ttl=300000 \
        --logtostderr=true
}

run_client() {
    local local_ip="$1"
    local primary_ip="$2"
    local device_filter="$3"
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
        --port=50052 \
        --global_segment_size=10GB \
        --master_server_address="etcd://${primary_ip}:2379" \
        --metadata_server="etcd://${primary_ip}:2379" \
        --protocol=rdma \
        --logtostderr=true
}

run_python_test() {
    local local_ip="$1"
    local remote_ip="$2"
    local primary_ip="$3"
    local device_filter="$4"
    shift 4
    activate_runtime
    unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY all_proxy
    export MOONCAKE_LOCAL_IP="${local_ip}"
    export MOONCAKE_REMOTE_IP="${remote_ip}"
    export MOONCAKE_ETCD_URL="etcd://${primary_ip}:2379"
    if [ "${device_filter}" = "__auto__" ]; then
        device_filter=""
    fi
    if [ -n "${device_filter}" ]; then
        export MC_TE_FILTERS="${device_filter}"
    else
        unset MC_TE_FILTERS || true
    fi
    exec python3 /work/test_put_get_ha.py "$@"
}

case "${1:-}" in
    __container_prepare)
        require_env HCU_TEST_DTK_PKG_URL
        require_env HCU_STORE_ETCD_URL
        prepare_container "$2" "$3"
        exit 0
        ;;
    __container_etcd)
        run_etcd "$2"
        exit 0
        ;;
    __container_master)
        run_master "$2" "$3"
        exit 0
        ;;
    __container_client)
        run_client "$2" "$3" "$4"
        exit 0
        ;;
    __container_python)
        shift
        run_python_test "$@"
        exit 0
        ;;
esac

if [ "$#" -ne 1 ]; then
    usage >&2
    exit 2
fi

require_env HCU_TEST_IMAGE
require_env HCU_TEST_DTK_PKG_URL

for command_name in awk docker getent grep realpath scp sed ssh timeout; do
    command -v "${command_name}" >/dev/null 2>&1 || {
        echo "ERROR: required command is missing: ${command_name}" >&2
        exit 1
    }
done

WHEEL_PATH="$(realpath "$1")"
SELF_PATH="$(realpath "${BASH_SOURCE[0]}")"
PYTHON_TEST_PATH="$(realpath "$(dirname "${BASH_SOURCE[0]}")/store_etcd_ha_test/test_put_get_ha.py")"
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
SETUP_TIMEOUT="${HCU_STORE_SETUP_TIMEOUT:-900}"
READY_TIMEOUT="${HCU_STORE_READY_TIMEOUT:-180}"
RUN_TIMEOUT="${HCU_STORE_RUN_TIMEOUT:-180}"
REDACT_IPS="${HCU_TEST_REDACT_IPS:-${GITHUB_ACTIONS:-false}}"
TMP_ROOT="${RUNNER_TEMP:-/tmp}"
LOG_DIR="${HCU_STORE_LOG_DIR:-${TMP_ROOT}/hcu-store-logs}"
RUN_TOKEN="${GITHUB_RUN_ID:-manual}-${GITHUB_RUN_ATTEMPT:-0}-$$"
PRIMARY_CONTAINER="mooncake-store-primary-${RUN_TOKEN}"
SECONDARY_CONTAINER="mooncake-store-secondary-${RUN_TOKEN}"
REMOTE_WORK_DIR="/tmp/mooncake-hcu-store-${RUN_TOKEN}"
WHEEL_BASENAME="$(basename "${WHEEL_PATH}")"

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
LOG_NAMES=(primary-prepare secondary-prepare etcd primary-master secondary-master primary-client secondary-client cross-read-put cross-read-get cross-write-put cross-write-get)
for log_name in "${LOG_NAMES[@]}"; do
    : >"${LOG_DIR}/${log_name}.log"
done

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

run_secondary_python() {
    local output_log="$1"
    shift
    ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- \
        "${SECONDARY_CONTAINER}" "${SECONDARY_IP}" "${PRIMARY_IP}" \
        "${SECONDARY_FILTER}" "${RUN_TIMEOUT}" "$@" >"${output_log}" 2>&1 <<'REMOTE_PYTHON'
set -Eeuo pipefail
container_name="$1"
local_ip="$2"
primary_ip="$3"
device_filter="$4"
run_timeout="$5"
shift 5
timeout "${run_timeout}" docker exec "${container_name}" \
    /bin/bash /work/test_hcu_store_wheel.sh \
    __container_python "${local_ip}" "${primary_ip}" "${primary_ip}" "${device_filter}" "$@"
REMOTE_PYTHON
}

echo "Primary:   ${PRIMARY_HOST}"
echo "Secondary: ${SECONDARY_HOST}"
echo "Image:     ${HCU_TEST_IMAGE}"

ssh "${SSH_OPTIONS[@]}" "${REMOTE}" mkdir -p "${REMOTE_WORK_DIR}"
scp "${SCP_OPTIONS[@]}" "${WHEEL_PATH}" "${SELF_PATH}" "${PYTHON_TEST_PATH}" "${REMOTE}:${REMOTE_WORK_DIR}/"

DOCKER_ENV_ARGS=(-e "HCU_TEST_DTK_PKG_URL=${HCU_TEST_DTK_PKG_URL}" -e "HCU_STORE_ETCD_URL=${ETCD_URL}")
[ -n "${PIP_INDEX_URL:-}" ] && DOCKER_ENV_ARGS+=(-e "PIP_INDEX_URL=${PIP_INDEX_URL}")
[ -n "${PIP_TRUSTED_HOST:-}" ] && DOCKER_ENV_ARGS+=(-e "PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST}")

docker run -d --name "${PRIMARY_CONTAINER}" --network host --privileged \
    --device /dev/kfd:/dev/kfd --device /dev/dri:/dev/dri --cap-add SYS_PTRACE \
    --volume /opt/hyhal:/opt/hyhal:ro \
    --volume "${WHEEL_PATH}:/work/${WHEEL_BASENAME}:ro" \
    --volume "${SELF_PATH}:/work/test_hcu_store_wheel.sh:ro" \
    --volume "${PYTHON_TEST_PATH}:/work/test_put_get_ha.py:ro" \
    "${DOCKER_ENV_ARGS[@]}" --entrypoint /bin/bash "${HCU_TEST_IMAGE}" -lc 'exec sleep infinity' >/dev/null

ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- \
    "${REMOTE_WORK_DIR}" "${WHEEL_BASENAME}" "$(basename "${SELF_PATH}")" "$(basename "${PYTHON_TEST_PATH}")" \
    "${SECONDARY_CONTAINER}" "${HCU_TEST_IMAGE}" "${HCU_TEST_DTK_PKG_URL}" "${ETCD_URL}" \
    "${PIP_INDEX_URL:-}" "${PIP_TRUSTED_HOST:-}" <<'REMOTE_CONTAINER' >/dev/null
set -Eeuo pipefail
remote_dir="$1"; wheel="$2"; script="$3"; python_test="$4"; container="$5"; image="$6"
dtk_url="$7"; etcd_url="$8"; pip_index="$9"; pip_trusted="${10}"
env_args=(-e "HCU_TEST_DTK_PKG_URL=${dtk_url}" -e "HCU_STORE_ETCD_URL=${etcd_url}")
[ -n "${pip_index}" ] && env_args+=(-e "PIP_INDEX_URL=${pip_index}")
[ -n "${pip_trusted}" ] && env_args+=(-e "PIP_TRUSTED_HOST=${pip_trusted}")
docker run -d --name "${container}" --network host --privileged \
    --device /dev/kfd:/dev/kfd --device /dev/dri:/dev/dri --cap-add SYS_PTRACE \
    --volume /opt/hyhal:/opt/hyhal:ro \
    --volume "${remote_dir}/${wheel}:/work/${wheel}:ro" \
    --volume "${remote_dir}/${script}:/work/test_hcu_store_wheel.sh:ro" \
    --volume "${remote_dir}/${python_test}:/work/test_put_get_ha.py:ro" \
    "${env_args[@]}" --entrypoint /bin/bash "${image}" -lc 'exec sleep infinity'
REMOTE_CONTAINER

echo "Preparing DTK and standard wheel on both nodes in parallel..."
timeout "${SETUP_TIMEOUT}" docker exec "${PRIMARY_CONTAINER}" /bin/bash /work/test_hcu_store_wheel.sh \
    __container_prepare "/work/${WHEEL_BASENAME}" true >"${LOG_DIR}/primary-prepare.log" 2>&1 &
PRIMARY_PREP_PID=$!
ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- "${SECONDARY_CONTAINER}" "${WHEEL_BASENAME}" "${SETUP_TIMEOUT}" \
    >"${LOG_DIR}/secondary-prepare.log" 2>&1 <<'REMOTE_PREPARE' &
set -Eeuo pipefail
timeout "$3" docker exec "$1" /bin/bash /work/test_hcu_store_wheel.sh __container_prepare "/work/$2" false
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

docker exec "${PRIMARY_CONTAINER}" /bin/bash /work/test_hcu_store_wheel.sh __container_etcd "${PRIMARY_IP}" \
    >"${LOG_DIR}/etcd.log" 2>&1 &
ETCD_PID=$!
for ((attempt = 1; attempt <= READY_TIMEOUT; attempt++)); do
    if docker exec "${PRIMARY_CONTAINER}" /tmp/etcd-bin/etcdctl --endpoints="http://${PRIMARY_IP}:2379" endpoint health >/dev/null 2>&1; then
        break
    fi
    kill -0 "${ETCD_PID}" 2>/dev/null || { echo "ERROR: etcd exited during startup" >&2; exit 1; }
    sleep 1
done
if ! docker exec "${PRIMARY_CONTAINER}" /tmp/etcd-bin/etcdctl --endpoints="http://${PRIMARY_IP}:2379" endpoint health >/dev/null 2>&1; then
    echo "ERROR: etcd did not become healthy" >&2
    exit 1
fi

docker exec "${PRIMARY_CONTAINER}" /bin/bash /work/test_hcu_store_wheel.sh __container_master "${PRIMARY_IP}" "${PRIMARY_IP}" \
    >"${LOG_DIR}/primary-master.log" 2>&1 &
PRIMARY_MASTER_PID=$!
wait_for_log "${LOG_DIR}/primary-master.log" 'Master runtime state -> serving, role=leader' "${PRIMARY_MASTER_PID}" "primary master"

ssh "${SSH_OPTIONS[@]}" "${REMOTE}" docker exec "${SECONDARY_CONTAINER}" \
    /bin/bash /work/test_hcu_store_wheel.sh __container_master "${SECONDARY_IP}" "${PRIMARY_IP}" \
    >"${LOG_DIR}/secondary-master.log" 2>&1 &
SECONDARY_MASTER_PID=$!
wait_for_log "${LOG_DIR}/secondary-master.log" 'Master runtime state -> standby, role=standby' "${SECONDARY_MASTER_PID}" "secondary master"

docker exec "${PRIMARY_CONTAINER}" /bin/bash /work/test_hcu_store_wheel.sh __container_client \
    "${PRIMARY_IP}" "${PRIMARY_IP}" "${PRIMARY_FILTER}" >"${LOG_DIR}/primary-client.log" 2>&1 &
PRIMARY_CLIENT_PID=$!
ssh "${SSH_OPTIONS[@]}" "${REMOTE}" docker exec "${SECONDARY_CONTAINER}" \
    /bin/bash /work/test_hcu_store_wheel.sh __container_client "${SECONDARY_IP}" "${PRIMARY_IP}" "${SECONDARY_FILTER}" \
    >"${LOG_DIR}/secondary-client.log" 2>&1 &
SECONDARY_CLIENT_PID=$!
wait_for_log "${LOG_DIR}/primary-client.log" 'Starting real client service' "${PRIMARY_CLIENT_PID}" "primary client"
wait_for_log "${LOG_DIR}/secondary-client.log" 'Starting real client service' "${SECONDARY_CLIENT_PID}" "secondary client"

READ_KEY="hcu-ci-cross-read-${RUN_TOKEN}"
WRITE_KEY="hcu-ci-cross-write-${RUN_TOKEN}"

timeout "${RUN_TIMEOUT}" docker exec "${PRIMARY_CONTAINER}" /bin/bash /work/test_hcu_store_wheel.sh \
    __container_python "${PRIMARY_IP}" "${SECONDARY_IP}" "${PRIMARY_IP}" "${PRIMARY_FILTER}" \
    --mode put --key "${READ_KEY}" --preferred-segment "${PRIMARY_IP}" --value-mb 1 \
    >"${LOG_DIR}/cross-read-put.log" 2>&1
run_secondary_python "${LOG_DIR}/cross-read-get.log" --mode get --key "${READ_KEY}" --value-mb 1

timeout "${RUN_TIMEOUT}" docker exec "${PRIMARY_CONTAINER}" /bin/bash /work/test_hcu_store_wheel.sh \
    __container_python "${PRIMARY_IP}" "${SECONDARY_IP}" "${PRIMARY_IP}" "${PRIMARY_FILTER}" \
    --mode put --key "${WRITE_KEY}" --cross-put --value-mb 1 \
    >"${LOG_DIR}/cross-write-put.log" 2>&1
run_secondary_python "${LOG_DIR}/cross-write-get.log" --mode get --key "${WRITE_KEY}" --value-mb 1

for put_log in "${LOG_DIR}/cross-read-put.log" "${LOG_DIR}/cross-write-put.log"; do
    grep -q 'put: 0' "${put_log}"
    grep -q '^OK$' "${put_log}"
done
for get_log in "${LOG_DIR}/cross-read-get.log" "${LOG_DIR}/cross-write-get.log"; do
    grep -q 'match=True' "${get_log}"
    grep -q '^OK$' "${get_log}"
done

echo "Mooncake Store cross-node read and cross-node write tests passed."
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
        echo "### Mooncake Store etcd/HA cross-node test"
        echo
        echo "- Primary: \`${PRIMARY_HOST}\`"
        echo "- Secondary: \`${SECONDARY_HOST}\`"
        echo "- Cross-node read: passed"
        echo "- Cross-node write: passed"
    } >>"${GITHUB_STEP_SUMMARY}"
fi
