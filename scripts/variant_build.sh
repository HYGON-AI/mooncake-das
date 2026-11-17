#!/bin/bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JOBS="${JOBS:-$(nproc)}"
BUILD_DIR="build"
FINAL_DIST_DIR="${REPO_ROOT}/mooncake-wheel/dist"
PYPROJECT_FILE="${REPO_ROOT}/mooncake-wheel/pyproject.toml"

RUN_DEPS=1
DRY_RUN=0
BUILD_VARIANT="standard"
USE_HYLINK=OFF
USE_HIP=OFF
USE_ETCD=OFF
PYPROJECT_BACKUP=""
BUILD_TYPE="Release"

usage() {
    cat <<'EOF'
Usage: ./scripts/variant_build.sh [options]

Without feature flags, build for a normal RDMA NIC with HIP and HyLink disabled.
HyLink depends on HIP, so --hylink also enables USE_HIP.
HIP also enables USE_HYGON to build and use the Store H2D scatter kernel.

USE_ETCD and STORE_USE_ETCD default to OFF; --etcd enables both.
HIP or HyLink additionally enables ENABLE_MULTI_PROTOCOL.

Options:
  --shca        Enable USE_SHCA for TianLong SHCA NICs
  --hylink      Enable USE_HYLINK (also enables USE_HIP)
  --hip         Enable USE_HIP and USE_HYGON without requiring HyLink
  --etcd        Enable USE_ETCD and STORE_USE_ETCD together
  --skip-deps   Skip `bash dependencies.sh`
  --dry-run     Print commands without executing them
  --jobs N      Override build parallelism
  -h, --help    Show this help

Build directory: build (shared by all configurations)
Wheel output: mooncake-wheel/dist
Package names depend only on the NIC: mooncake-transfer-engine for standard,
mooncake-transfer-engine-shca for SHCA, regardless of HIP/HyLink/etcd flags.

Examples:
  bash scripts/variant_build.sh
  bash scripts/variant_build.sh --shca
  bash scripts/variant_build.sh --hylink
  bash scripts/variant_build.sh --shca --hylink
  bash scripts/variant_build.sh --hip
  bash scripts/variant_build.sh --shca --hip
  bash scripts/variant_build.sh --shca --hip --etcd
  bash scripts/variant_build.sh --hylink --etcd
EOF
}

restore_pyproject() {
    if [ -n "${PYPROJECT_BACKUP}" ] && [ -f "${PYPROJECT_BACKUP}" ]; then
        mv "${PYPROJECT_BACKUP}" "${PYPROJECT_FILE}"
        PYPROJECT_BACKUP=""
    fi
}

patch_pyproject() {
    local package_name="$1"
    local package_description="$2"
    local package_keywords="$3"

    if [ -n "${PYPROJECT_BACKUP}" ]; then
        restore_pyproject
    fi

    PYPROJECT_BACKUP="$(mktemp)"
    cp -p "${PYPROJECT_FILE}" "${PYPROJECT_BACKUP}"

    python - "${PYPROJECT_FILE}" "${package_name}" "${package_description}" "${package_keywords}" <<'PY'
import re
import sys

path, package_name, package_description, package_keywords = sys.argv[1:]
with open(path, "r", encoding="utf-8") as f:
    content = f.read()

content = re.sub(
    r'^name = ".*"$',
    f'name = "{package_name}"',
    content,
    count=1,
    flags=re.MULTILINE,
)
content = re.sub(
    r'^description = ".*"$',
    f'description = "{package_description}"',
    content,
    count=1,
    flags=re.MULTILINE,
)
content = re.sub(
    r'^keywords = \[.*\]$',
    f'keywords = {package_keywords}',
    content,
    count=1,
    flags=re.MULTILINE,
)

with open(path, "w", encoding="utf-8") as f:
    f.write(content)
PY
}

run_cmd() {
    printf '+'
    printf ' %q' "$@"
    printf '\n'
    if [ "${DRY_RUN}" -eq 0 ]; then
        "$@"
    fi
}

refresh_go_env() {
    if [ -d "/usr/local/go/bin" ] && [[ ":${PATH}:" != *":/usr/local/go/bin:"* ]]; then
        export PATH="${PATH}:/usr/local/go/bin"
    fi
    hash -r
}

run_wheel_build() {
    (
        cd "${REPO_ROOT}"
        run_cmd env BUILD_DIR="${BUILD_DIR}" OUTPUT_DIR=dist \
            bash "${REPO_ROOT}/scripts/build_wheel.sh"
    )
}

setup_shca_env() {
    echo "==> Preparing TianLong SHCA environment..."

    # 检查驱动头文件是否已存在
    if [ -f /usr/include/infiniband/shca_17b_types.h ]; then
        echo "SHCA driver header found (/usr/include/infiniband/shca_17b_types.h), skipping setup."
        return 0
    fi

    if [ "${DRY_RUN}" -eq 1 ]; then
        echo "[dry-run] Would download and install SHCA driver."
        return 0
    fi

    local SETUP_DIR="/tmp/shca_setup"
    mkdir -p "${SETUP_DIR}"
    cd "${SETUP_DIR}"

    # 下载执行脚本
    wget ${RESOURCE_SERVER_URL}/Jenkins/CompileDep/mooncake/mlxtoshca.sh

    # 下载驱动安装包
    wget ${RESOURCE_SERVER_URL}/Jenkins/CompileDep/mooncake/shca-tools_2.500.4.B068-Ubuntu22.04_amd64.deb

    # 安装驱动
    bash "${SETUP_DIR}/mlxtoshca.sh"

    # 验证安装
    ibv_devinfo
    echo "SHCA environment setup complete."
}

build_variant() {
    local nic_type="$1"
    local package_name="mooncake-transfer-engine"
    local nic_description="standard RDMA"
    local gpu_description=""
    local use_shca=OFF
    # HIP/HyLink share the segment with RDMA and need multi-protocol routing.
    local -a cmake_args=(-DUSE_ETCD="${USE_ETCD}" -DSTORE_USE_ETCD="${USE_ETCD}"
                         -DBUILD_UNIT_TESTS=OFF
                         -DENABLE_MULTI_PROTOCOL="${USE_HIP}")

    case "${nic_type}" in
        standard)
            ;;
        shca)
            package_name+="-shca"
            nic_description="TianLong SHCA"
            use_shca=ON
            setup_shca_env
            ;;
        *)
            echo "Unknown build variant: ${nic_type}" >&2
            exit 1
            ;;
    esac

    case "${GPU_BACKEND}" in
        hylink)
            gpu_description="HyLink + HIP"
            ;;
        hip)
            gpu_description="HIP"
            ;;
        none)
            gpu_description="HIP/HyLink disabled"
            ;;
    esac
    cmake_args+=(-DUSE_SHCA="${use_shca}" -DUSE_HYLINK="${USE_HYLINK}"
                 -DUSE_HIP="${USE_HIP}" -DUSE_HYGON="${USE_HIP}" -DUSE_FAKE_HIP_RPC="${USE_HIP}" -DUSE_CUDA=OFF)

    if [ "${DRY_RUN}" -eq 0 ] && \
        { [ "${nic_type}" != standard ] || [ "${GPU_BACKEND}" != hylink ]; }; then
        patch_pyproject \
            "${package_name}" \
            "Python binding of a Mooncake library using pybind11 (${nic_description}, ${gpu_description})" \
            '["mooncake", "data transfer", "kv cache", "llm inference", "'"${nic_type}"'", "'"${GPU_BACKEND}"'"]'
    fi

    echo "==> Building ${package_name} (${nic_description}, ${gpu_description})"
    echo "==> Wheel output: ${FINAL_DIST_DIR}"
    run_cmd mkdir -p "${REPO_ROOT}/${BUILD_DIR}" "${FINAL_DIST_DIR}"
    unset VERBOSE || true
    run_cmd cmake -S "${REPO_ROOT}" -B "${REPO_ROOT}/${BUILD_DIR}" "${cmake_args[@]}" \
        -DSTORE_USE_JEMALLOC=ON \
        -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" -DBUILD_SHARED_LIBS=ON \
        -DCMAKE_VERBOSE_MAKEFILE=OFF -DCMAKE_CXX_FLAGS="" \
        -DCMAKE_IGNORE_PREFIX_PATH="${CONDA_PREFIX:-/opt/hyhal/hbm/Miniconda3}"
    run_cmd cmake --build "${REPO_ROOT}/${BUILD_DIR}" -j"${JOBS}"
    run_wheel_build
}

while [ $# -gt 0 ]; do
    case "$1" in
        --shca)
            BUILD_VARIANT="shca"
            ;;
        --hylink)
            USE_HYLINK=ON
            ;;
        --hip)
            USE_HIP=ON
            ;;
        --etcd)
            USE_ETCD=ON
            ;;
        --skip-deps)
            RUN_DEPS=0
            ;;
        --dry-run)
            DRY_RUN=1
            ;;
        --jobs)
            if [ $# -lt 2 ]; then
                echo "Error: --jobs requires a positive integer." >&2
                exit 1
            fi
            JOBS="$2"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
    shift
done

if ! [[ "${JOBS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: --jobs/JOBS must be a positive integer." >&2
    exit 1
fi

GPU_BACKEND="none"
if [ "${USE_HYLINK}" = ON ]; then
    USE_HIP=ON
    GPU_BACKEND="hylink"
elif [ "${USE_HIP}" = ON ]; then
    GPU_BACKEND="hip"
fi

trap restore_pyproject EXIT

run_cmd mkdir -p "${FINAL_DIST_DIR}"

if [ "${RUN_DEPS}" -eq 1 ]; then
    run_cmd bash "${REPO_ROOT}/dependencies.sh" -y
fi

refresh_go_env

build_variant "${BUILD_VARIANT}"

run_cmd ls -1 "${FINAL_DIST_DIR}"
