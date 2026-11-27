#!/usr/bin/env bash
# Build and stage the private extension providers.
#
# Usage:
#   package_private_ext.sh            # 默认：编译并打包全部 provider（DFE + AHE）
#   package_private_ext.sh all        # 同上
#   package_private_ext.sh dfe        # 只编译 DFE（纯 POSIX，无需 HIP）
#   package_private_ext.sh ahe        # 只编译 AHE（需要 HIP/DTK）
#
# The output directory is what an operator points the deployment at:
#   MOONCAKE_EXT_DIR=<out_dir>
# The loader looks every provider up by its fixed file name inside that single
# directory (falling back to /opt/mooncake/plugins).
#
# These artifacts are secret and must never be copied into the public wheel.
set -euo pipefail

PRIVATE_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BUILD_DIR=${BUILD_DIR:-${PRIVATE_ROOT}/build}
OUT_DIR=${OUT_DIR:-${PRIVATE_ROOT}/dist}
JOBS=${JOBS:-$(nproc)}
MODE=${1:-all}

case "$MODE" in
    all|both)
        MODE=all
        ENABLE_DFE=ON
        ENABLE_AHE=ON
        ;;
    dfe)
        ENABLE_DFE=ON
        ENABLE_AHE=OFF
        ;;
    ahe)
        ENABLE_DFE=OFF
        ENABLE_AHE=ON
        ;;
    *)
        echo "unknown target: $MODE" >&2
        echo "usage: $0 [all|dfe|ahe]" >&2
        exit 2
        ;;
esac

echo "==> Building private providers (mode=${MODE}, DFE=${ENABLE_DFE}, AHE=${ENABLE_AHE})"

# Drop previously staged providers so the directory always matches this run.
rm -f "${OUT_DIR}"/libmooncake_dfe.so* \
      "${OUT_DIR}"/libmooncake_ahe.so* \
      "${OUT_DIR}"/mc_copy_kernel.co

cmake -S "${PRIVATE_ROOT}" -B "${BUILD_DIR}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DMOONCAKE_ENABLE_DFE="${ENABLE_DFE}" \
    -DMOONCAKE_ENABLE_AHE="${ENABLE_AHE}" \
    -DMOONCAKE_PRIVATE_EXT_INSTALL_DIR="${OUT_DIR}"
cmake --build "${BUILD_DIR}" --config Release -j "${JOBS}"
cmake --install "${BUILD_DIR}"

echo "==> Verifying install layout in ${OUT_DIR}"
if [ "${ENABLE_DFE}" = "ON" ]; then
    test -f "${OUT_DIR}/libmooncake_dfe.so.1" \
        || { echo "missing libmooncake_dfe.so.1" >&2; exit 1; }
fi
if [ "${ENABLE_AHE}" = "ON" ]; then
    test -f "${OUT_DIR}/libmooncake_ahe.so.1" \
        || { echo "missing libmooncake_ahe.so.1" >&2; exit 1; }
    # The kernel must sit beside its provider: the AHE loader resolves it
    # relative to its own shared object.
    test -f "${OUT_DIR}/mc_copy_kernel.co" \
        || { echo "missing mc_copy_kernel.co" >&2; exit 1; }
fi

# Ship the release gate and the target-side check next to the providers so the
# package is self-describing.
for helper in verify_release.sh check_environment.sh; do
    if command -v install >/dev/null 2>&1; then
        install -m 0755 "${PRIVATE_ROOT}/${helper}" "${OUT_DIR}/${helper}"
    else
        cp -f "${PRIVATE_ROOT}/${helper}" "${OUT_DIR}/${helper}"
        chmod 0755 "${OUT_DIR}/${helper}"
    fi
done

echo "==> Verifying staged package in ${OUT_DIR}"
EXPECT_PROVIDERS="${MODE}" bash "${OUT_DIR}/verify_release.sh" "${OUT_DIR}" || {
    echo "release verification failed" >&2
    exit 1
}

cat <<EOF
Private providers staged in ${OUT_DIR} (mode=${MODE})

Two helper scripts are packaged alongside them:
  verify_release.sh     run on this build machine (already executed above)
  check_environment.sh  run on the target host after copying the directory

Deploy them as one directory and set:
  export MOONCAKE_EXT_DIR=${OUT_DIR}

Each provider is looked up by its fixed file name inside that directory. The
loader resolves in this order:
  1. \$MOONCAKE_EXT_DIR/<file name>
  2. /opt/mooncake/plugins/<file name>
Set MOONCAKE_ENABLE_EXT=0 to disable both providers without moving files.

Master needs libmooncake_dfe.so.1; clients need both providers. When a provider
is absent every caller falls back to its own baseline implementation.
EOF
