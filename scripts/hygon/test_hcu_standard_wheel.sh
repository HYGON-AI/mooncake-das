#!/usr/bin/env bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

set -Eeuo pipefail

usage() {
    cat <<'EOF'
Usage: bash scripts/hygon/test_hcu_standard_wheel.sh <wheel-path>

Run the standard HCU wheel cross-node RDMA test in two temporary containers.
The GitHub Actions job runs on the target host and controls the initiator host
over SSH. Hostnames are resolved at runtime; no node IP is stored in this file.

Required environment variables:
  HCU_TEST_IMAGE            Container image used on both nodes
  HCU_TEST_DTK_PKG_URL      DTK tarball URL installed in both containers

Optional environment variables:
  HCU_TEST_TARGET_HOST      Target host (default: nmz4)
  HCU_TEST_INITIATOR_HOST   Initiator host (default: nmz36)
  HCU_TEST_TARGET_IP        Target service IPv4 (otherwise resolve target host)
  HCU_TEST_INITIATOR_IP     Initiator service IPv4 (otherwise resolve initiator host)
  HCU_TEST_INITIATOR_SSH_HOST  SSH address (default: initiator service IPv4)
  HCU_TEST_REMOTE_USER      Initiator SSH user (default: github)
  HCU_TEST_SSH_PORT         Initiator SSH port (default: 22)
  HCU_TEST_TARGET_FILTER    MC_TE_FILTERS on target (default: mlx5_6)
  HCU_TEST_SETUP_TIMEOUT    Per-node environment setup timeout (default: 600)
  HCU_TEST_READY_TIMEOUT    Target startup timeout in seconds (default: 300)
  HCU_TEST_SETTLE_SECONDS   Delay after target publishes its port (default: 5)
  HCU_TEST_RUN_TIMEOUT      Initiator container timeout in seconds (default: 300)
  HCU_TEST_LOG_DIR          Directory for target and initiator logs
  HCU_CROSS_NODE_LOCK_FILE  Shared test lock file on both nodes
  HCU_CROSS_NODE_LOCK_TIMEOUT Seconds to wait for each node lock (default: 900)
  HCU_TEST_REDACT_IPS       Replace IPs with hostnames in logs (default: true in CI)
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

# This function runs inside both test containers. A fresh container must have
# the same DTK runtime before the wheel or transfer_engine_bench can be used.
prepare_container_runtime() {
    local wheel_path="$1"
    local dtk_tarball="/tmp/$(basename "${HCU_TEST_DTK_PKG_URL}")"
    local dtk_dir=""

    echo "Installing DTK from ${HCU_TEST_DTK_PKG_URL}..."
    wget -q "${HCU_TEST_DTK_PKG_URL}" -O "${dtk_tarball}"
    tar -xzf "${dtk_tarball}" -C /opt
    dtk_dir="$(find /opt -mindepth 1 -maxdepth 1 -type d -name 'dtk-*' -print -quit)"
    if [ -z "${dtk_dir}" ]; then
        echo "ERROR: the DTK archive did not create /opt/dtk-*" >&2
        exit 1
    fi
    ln -s "${dtk_dir}" /opt/dtk
    # DTK's env.sh reads variables such as CMAKE_PREFIX_PATH before assigning
    # them, so nounset must be disabled while the vendor script is sourced.
    set +u
    # shellcheck disable=SC1091
    source /opt/dtk/env.sh
    set -u

    echo "Installing standard wheel ${wheel_path}..."
    python3 -m pip install "${wheel_path}"
    python3 -m pip show mooncake-transfer-engine
    if ! command -v transfer_engine_bench >/dev/null 2>&1; then
        echo "ERROR: transfer_engine_bench was not installed by the wheel" >&2
        exit 1
    fi
}

activate_container_runtime() {
    set +u
    # shellcheck disable=SC1091
    source /opt/dtk/env.sh
    set -u
    if ! command -v transfer_engine_bench >/dev/null 2>&1; then
        echo "ERROR: prepared container does not contain transfer_engine_bench" >&2
        exit 1
    fi
}

run_container_target() {
    local target_ip="$1"

    activate_container_runtime
    export MC_TE_FILTERS="${HCU_TEST_TARGET_FILTER}"
    echo "Starting target on ${target_ip} with MC_TE_FILTERS=${MC_TE_FILTERS}..."
    exec transfer_engine_bench \
        --mode=target \
        --auto_discovery \
        --protocol=rdma \
        --metadata_server=P2PHANDSHAKE \
        --gpu_id=-1 \
        --local_server_name="${target_ip}"
}

run_container_initiator() {
    local initiator_ip="$1"
    local target_ip="$2"
    local target_port="$3"

    activate_container_runtime
    echo "Starting initiator on ${initiator_ip}; target is ${target_ip}:${target_port}..."
    exec transfer_engine_bench \
        --mode=initiator \
        --auto_discovery \
        --protocol=rdma \
        --metadata_server=P2PHANDSHAKE \
        --gpu_id=-1 \
        --local_server_name="${initiator_ip}" \
        --segment_id="${target_ip}:${target_port}"
}

# The same file is mounted into each container and used as its entry point.
case "${1:-}" in
    __container_prepare)
        require_env HCU_TEST_DTK_PKG_URL
        prepare_container_runtime "$2"
        exit 0
        ;;
    __container_target)
        require_env HCU_TEST_TARGET_FILTER
        run_container_target "$2"
        exit 0
        ;;
    __container_initiator)
        run_container_initiator "$2" "$3" "$4"
        exit 0
        ;;
esac

if [ "$#" -ne 1 ]; then
    usage >&2
    exit 2
fi

require_env HCU_TEST_IMAGE
require_env HCU_TEST_DTK_PKG_URL

for command_name in awk docker flock getent grep realpath scp sed ssh timeout; do
    if ! command -v "${command_name}" >/dev/null 2>&1; then
        echo "ERROR: required command is missing: ${command_name}" >&2
        exit 1
    fi
done

WHEEL_PATH="$(realpath "$1")"
SELF_PATH="$(realpath "${BASH_SOURCE[0]}")"
if [ ! -f "${WHEEL_PATH}" ]; then
    echo "ERROR: wheel not found: ${WHEEL_PATH}" >&2
    exit 1
fi

TARGET_HOST="${HCU_TEST_TARGET_HOST:-nmz4}"
INITIATOR_HOST="${HCU_TEST_INITIATOR_HOST:-nmz36}"
REMOTE_USER="${HCU_TEST_REMOTE_USER:-github}"
SSH_PORT="${HCU_TEST_SSH_PORT:-22}"
TARGET_FILTER="${HCU_TEST_TARGET_FILTER:-mlx5_6}"
SETUP_TIMEOUT="${HCU_TEST_SETUP_TIMEOUT:-600}"
READY_TIMEOUT="${HCU_TEST_READY_TIMEOUT:-300}"
SETTLE_SECONDS="${HCU_TEST_SETTLE_SECONDS:-5}"
RUN_TIMEOUT="${HCU_TEST_RUN_TIMEOUT:-300}"
LOCK_FILE="${HCU_CROSS_NODE_LOCK_FILE:-/tmp/mooncake-hcu-cross-node.lock}"
LOCK_TIMEOUT="${HCU_CROSS_NODE_LOCK_TIMEOUT:-900}"
REDACT_IPS="${HCU_TEST_REDACT_IPS:-${GITHUB_ACTIONS:-false}}"
TMP_ROOT="${RUNNER_TEMP:-/tmp}"
LOG_DIR="${HCU_TEST_LOG_DIR:-${TMP_ROOT}/hcu-standard-logs}"
RUN_TOKEN="${GITHUB_RUN_ID:-manual}-${GITHUB_RUN_ATTEMPT:-0}-$$"
TARGET_CONTAINER="mooncake-hcu-target-${RUN_TOKEN}"
INITIATOR_CONTAINER="mooncake-hcu-initiator-${RUN_TOKEN}"
REMOTE_WORK_DIR="/tmp/mooncake-hcu-standard-${RUN_TOKEN}"
WHEEL_BASENAME="$(basename "${WHEEL_PATH}")"
TARGET_LOG="${LOG_DIR}/target.log"
INITIATOR_LOG="${LOG_DIR}/initiator.log"
TARGET_LOCK_LOG="${LOG_DIR}/target-lock.log"
INITIATOR_LOCK_LOG="${LOG_DIR}/initiator-lock.log"

if ! [[ "${LOCK_TIMEOUT}" =~ ^[0-9]+$ ]] || ((LOCK_TIMEOUT < 1)); then
    echo "ERROR: HCU_CROSS_NODE_LOCK_TIMEOUT must be a positive integer" >&2
    exit 2
fi

resolve_ipv4() {
    local host="$1"
    local configured_ip="$2"
    local addresses=""

    if [ -n "${configured_ip}" ]; then
        printf '%s\n' "${configured_ip}"
        return
    fi

    addresses="$(getent ahostsv4 "${host}" 2>/dev/null || true)"
    awk 'NR == 1 { print $1 }' <<<"${addresses}"
}

TARGET_IP="$(resolve_ipv4 "${TARGET_HOST}" "${HCU_TEST_TARGET_IP:-}")"
INITIATOR_IP="$(resolve_ipv4 "${INITIATOR_HOST}" "${HCU_TEST_INITIATOR_IP:-}")"
if [ -z "${TARGET_IP}" ] || [ -z "${INITIATOR_IP}" ]; then
    echo "ERROR: failed to determine the target or initiator IPv4 address" >&2
    echo "Set HCU_TEST_TARGET_IP and HCU_TEST_INITIATOR_IP, or configure DNS or /etc/hosts for ${TARGET_HOST} and ${INITIATOR_HOST}." >&2
    exit 1
fi
INITIATOR_SSH_HOST="${HCU_TEST_INITIATOR_SSH_HOST:-${INITIATOR_IP}}"
REMOTE="${REMOTE_USER}@${INITIATOR_SSH_HOST}"

if [ -n "${PIP_INDEX_URL:-}" ] && [ -z "${PIP_TRUSTED_HOST:-}" ]; then
    pip_host="${PIP_INDEX_URL#*://}"
    export PIP_TRUSTED_HOST="${pip_host%%[:/]*}"
fi

mkdir -p "${LOG_DIR}"
: >"${TARGET_LOG}"
: >"${INITIATOR_LOG}"
: >"${TARGET_LOCK_LOG}"
: >"${INITIATOR_LOCK_LOG}"
TARGET_LOCK_PID=""
INITIATOR_LOCK_PID=""

SSH_OPTIONS=(
    -p "${SSH_PORT}"
    -o BatchMode=yes
    -o ConnectTimeout=10
    -o ServerAliveInterval=10
    -o ServerAliveCountMax=3
    -o StrictHostKeyChecking=yes
)
SCP_OPTIONS=(
    -P "${SSH_PORT}"
    -o BatchMode=yes
    -o ConnectTimeout=10
    -o StrictHostKeyChecking=yes
)
if [ -n "${HCU_TEST_SSH_KEY:-}" ]; then
    SSH_OPTIONS+=(-i "${HCU_TEST_SSH_KEY}")
    SCP_OPTIONS+=(-i "${HCU_TEST_SSH_KEY}")
fi
if [ -n "${HCU_TEST_KNOWN_HOSTS_FILE:-}" ]; then
    SSH_OPTIONS+=(-o "UserKnownHostsFile=${HCU_TEST_KNOWN_HOSTS_FILE}")
    SCP_OPTIONS+=(-o "UserKnownHostsFile=${HCU_TEST_KNOWN_HOSTS_FILE}")
fi

DOCKER_ENV_ARGS=(
    -e "HCU_TEST_DTK_PKG_URL=${HCU_TEST_DTK_PKG_URL}"
)
if [ -n "${PIP_INDEX_URL:-}" ]; then
    DOCKER_ENV_ARGS+=(-e "PIP_INDEX_URL=${PIP_INDEX_URL}")
fi
if [ -n "${PIP_TRUSTED_HOST:-}" ]; then
    DOCKER_ENV_ARGS+=(-e "PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST}")
fi

print_logs() {
    echo "===== target lock (${TARGET_HOST}) ====="
    cat "${TARGET_LOCK_LOG}"
    echo "===== initiator lock (${INITIATOR_HOST}) ====="
    cat "${INITIATOR_LOCK_LOG}"
    echo "===== target (${TARGET_HOST}) full log ====="
    cat "${TARGET_LOG}"
    echo "===== initiator (${INITIATOR_HOST}) full log ====="
    cat "${INITIATOR_LOG}"
}

redact_logs() {
    local target_pattern="${TARGET_IP//./\.}"
    local initiator_pattern="${INITIATOR_IP//./\.}"

    if [ "${REDACT_IPS}" != "true" ]; then
        return
    fi
    sed -i \
        -e "s/${target_pattern}/${TARGET_HOST}/g" \
        -e "s/${initiator_pattern}/${INITIATOR_HOST}/g" \
        "${TARGET_LOG}" "${INITIATOR_LOG}"
}

cleanup() {
    local rc=$?
    trap - EXIT
    set +e

    if docker inspect "${TARGET_CONTAINER}" >/dev/null 2>&1; then
        docker rm -f "${TARGET_CONTAINER}" >/dev/null 2>&1
    fi

    ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- \
        "${INITIATOR_CONTAINER}" "${REMOTE_WORK_DIR}" <<'REMOTE_CLEANUP' >/dev/null 2>&1
container_name="$1"
remote_dir="$2"
docker rm -f "${container_name}" >/dev/null 2>&1 || true
case "${remote_dir}" in
    /tmp/mooncake-hcu-standard-*) rm -rf -- "${remote_dir}" ;;
    *) echo "Refusing to remove unexpected path: ${remote_dir}" >&2 ;;
esac
REMOTE_CLEANUP

    if [ -n "${INITIATOR_LOCK_PID}" ]; then
        kill "${INITIATOR_LOCK_PID}" >/dev/null 2>&1 || true
        wait "${INITIATOR_LOCK_PID}" >/dev/null 2>&1 || true
    fi
    if [ -n "${TARGET_LOCK_PID}" ]; then
        kill "${TARGET_LOCK_PID}" >/dev/null 2>&1 || true
        wait "${TARGET_LOCK_PID}" >/dev/null 2>&1 || true
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

echo "Target:    ${TARGET_HOST}"
echo "Initiator: ${INITIATOR_HOST}"
echo "Image:     ${HCU_TEST_IMAGE}"

echo "Acquiring the cross-node test lock on ${TARGET_HOST}..."
PARENT_PID="$$"
(
    exec 9>"${LOCK_FILE}"
    if ! flock -w "${LOCK_TIMEOUT}" 9; then
        echo "ERROR: lock wait timed out"
        exit 75
    fi
    echo "LOCK_ACQUIRED"
    while kill -0 "${PARENT_PID}" 2>/dev/null; do sleep 5; done
) >"${TARGET_LOCK_LOG}" 2>&1 &
TARGET_LOCK_PID=$!
wait_for_lock "${TARGET_LOCK_LOG}" "${TARGET_LOCK_PID}" "${TARGET_HOST}"

echo "Acquiring the cross-node test lock on ${INITIATOR_HOST}..."
ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- "${LOCK_FILE}" "${LOCK_TIMEOUT}" \
    >"${INITIATOR_LOCK_LOG}" 2>&1 <<'REMOTE_LOCK' &
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
INITIATOR_LOCK_PID=$!
wait_for_lock "${INITIATOR_LOCK_LOG}" "${INITIATOR_LOCK_PID}" "${INITIATOR_HOST}"
echo "Cross-node test locks acquired."

echo "Copying the wheel and test entry script to ${INITIATOR_HOST}..."
ssh "${SSH_OPTIONS[@]}" "${REMOTE}" mkdir -p "${REMOTE_WORK_DIR}"
scp "${SCP_OPTIONS[@]}" "${WHEEL_PATH}" "${SELF_PATH}" "${REMOTE}:${REMOTE_WORK_DIR}/"

echo "Creating target container ${TARGET_CONTAINER} on ${TARGET_HOST}..."
docker run -d \
    --name "${TARGET_CONTAINER}" \
    --network host \
    --privileged \
    --device /dev/kfd:/dev/kfd \
    --device /dev/dri:/dev/dri \
    --cap-add SYS_PTRACE \
    --volume /opt/hyhal:/opt/hyhal:ro \
    --volume "${WHEEL_PATH}:/work/${WHEEL_BASENAME}:ro" \
    --volume "${SELF_PATH}:/work/test_hcu_standard_wheel.sh:ro" \
    "${DOCKER_ENV_ARGS[@]}" \
    -e "HCU_TEST_TARGET_FILTER=${TARGET_FILTER}" \
    --entrypoint /bin/bash \
    "${HCU_TEST_IMAGE}" \
    -lc 'exec sleep infinity' >/dev/null

echo "Creating initiator container ${INITIATOR_CONTAINER} on ${INITIATOR_HOST}..."
ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- \
    "${REMOTE_WORK_DIR}" "${WHEEL_BASENAME}" "$(basename "${SELF_PATH}")" \
    "${INITIATOR_CONTAINER}" "${HCU_TEST_IMAGE}" "${HCU_TEST_DTK_PKG_URL}" \
    "${PIP_INDEX_URL:-}" "${PIP_TRUSTED_HOST:-}" <<'REMOTE_START' >/dev/null
set -Eeuo pipefail
remote_dir="$1"
wheel_basename="$2"
script_basename="$3"
container_name="$4"
image="$5"
dtk_pkg_url="$6"
pip_index_url="$7"
pip_trusted_host="$8"

docker_env_args=(-e "HCU_TEST_DTK_PKG_URL=${dtk_pkg_url}")
[ -n "${pip_index_url}" ] && docker_env_args+=(-e "PIP_INDEX_URL=${pip_index_url}")
[ -n "${pip_trusted_host}" ] && docker_env_args+=(-e "PIP_TRUSTED_HOST=${pip_trusted_host}")

docker run -d \
    --name "${container_name}" \
    --network host \
    --privileged \
    --device /dev/kfd:/dev/kfd \
    --device /dev/dri:/dev/dri \
    --cap-add SYS_PTRACE \
    --volume /opt/hyhal:/opt/hyhal:ro \
    --volume "${remote_dir}/${wheel_basename}:/work/${wheel_basename}:ro" \
    --volume "${remote_dir}/${script_basename}:/work/test_hcu_standard_wheel.sh:ro" \
    "${docker_env_args[@]}" \
    --entrypoint /bin/bash \
    "${image}" \
    -lc 'exec sleep infinity'
REMOTE_START

echo "Preparing DTK and wheel on ${TARGET_HOST} and ${INITIATOR_HOST} in parallel..."
timeout "${SETUP_TIMEOUT}" docker exec "${TARGET_CONTAINER}" \
    /bin/bash /work/test_hcu_standard_wheel.sh \
    __container_prepare "/work/${WHEEL_BASENAME}" \
    >"${TARGET_LOG}" 2>&1 &
TARGET_PREP_PID=$!

ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- \
    "${INITIATOR_CONTAINER}" "${WHEEL_BASENAME}" "${SETUP_TIMEOUT}" \
    >"${INITIATOR_LOG}" 2>&1 <<'REMOTE_PREPARE' &
set -Eeuo pipefail
container_name="$1"
wheel_basename="$2"
setup_timeout="$3"
timeout "${setup_timeout}" docker exec "${container_name}" \
    /bin/bash /work/test_hcu_standard_wheel.sh \
    __container_prepare "/work/${wheel_basename}"
REMOTE_PREPARE
INITIATOR_PREP_PID=$!

set +e
wait "${TARGET_PREP_PID}"
TARGET_PREP_RC=$?
wait "${INITIATOR_PREP_PID}"
INITIATOR_PREP_RC=$?
set -e

if [ "${TARGET_PREP_RC}" -ne 0 ] || [ "${INITIATOR_PREP_RC}" -ne 0 ]; then
    echo "ERROR: environment preparation failed (target=${TARGET_PREP_RC}, initiator=${INITIATOR_PREP_RC})" >&2
    exit 1
fi
echo "Both test containers are ready."

echo "Starting target service on ${TARGET_HOST}..."
docker exec -e "HCU_TEST_TARGET_FILTER=${TARGET_FILTER}" "${TARGET_CONTAINER}" \
    /bin/bash /work/test_hcu_standard_wheel.sh \
    __container_target "${TARGET_IP}" \
    >>"${TARGET_LOG}" 2>&1 &
TARGET_PROCESS_PID=$!

TARGET_PORT=""
for ((attempt = 1; attempt <= READY_TIMEOUT; attempt++)); do
    TARGET_PORT="$(
        sed -nE 's/.*Transfer Engine RPC using .*, listening on [^:[:space:]]+:([0-9]+).*/\1/p' \
            "${TARGET_LOG}" | tail -n 1
    )"
    if [ -n "${TARGET_PORT}" ]; then
        break
    fi
    if ! kill -0 "${TARGET_PROCESS_PID}" 2>/dev/null; then
        echo "ERROR: target service exited before publishing its RPC port" >&2
        exit 1
    fi
    sleep 1
done

if [ -z "${TARGET_PORT}" ]; then
    echo "ERROR: target did not publish an RPC port within ${READY_TIMEOUT}s" >&2
    exit 1
fi
echo "Target endpoint: ${TARGET_HOST}:${TARGET_PORT}"
sleep "${SETTLE_SECONDS}"

if ! kill -0 "${TARGET_PROCESS_PID}" 2>/dev/null; then
    echo "ERROR: target service exited during initialization" >&2
    exit 1
fi

echo "Starting initiator service on ${INITIATOR_HOST}..."
if ! ssh "${SSH_OPTIONS[@]}" "${REMOTE}" bash -s -- \
    "${INITIATOR_CONTAINER}" "${INITIATOR_IP}" "${TARGET_IP}" \
    "${TARGET_PORT}" "${RUN_TIMEOUT}" >>"${INITIATOR_LOG}" 2>&1 <<'REMOTE_TEST'; then
set -Eeuo pipefail
container_name="$1"
initiator_ip="$2"
target_ip="$3"
target_port="$4"
run_timeout="$5"

timeout "${run_timeout}" docker exec "${container_name}" \
    /bin/bash /work/test_hcu_standard_wheel.sh \
    __container_initiator "${initiator_ip}" "${target_ip}" "${target_port}"
REMOTE_TEST
    echo "ERROR: initiator service failed" >&2
    exit 1
fi

if ! kill -0 "${TARGET_PROCESS_PID}" 2>/dev/null; then
    echo "ERROR: target service exited unexpectedly during the test" >&2
    exit 1
fi

SUCCESS_LINE="$(grep 'Test completed:' "${INITIATOR_LOG}" | tail -n 1 || true)"
if [ -z "${SUCCESS_LINE}" ]; then
    echo "ERROR: initiator log does not contain 'Test completed:'" >&2
    exit 1
fi

echo "Standard wheel cross-node RDMA test passed:"
echo "${SUCCESS_LINE}"

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
        echo "### Standard HCU wheel cross-node test"
        echo
        echo "- Target: \`${TARGET_HOST}\` (\`MC_TE_FILTERS=${TARGET_FILTER}\`)"
        echo "- Initiator: \`${INITIATOR_HOST}\`"
        echo "- Result: passed"
        echo
        echo '```text'
        echo "${SUCCESS_LINE}"
        echo '```'
    } >>"${GITHUB_STEP_SUMMARY}"
fi
