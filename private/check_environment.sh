#!/usr/bin/env bash
# 新环境部署检查（在目标机执行）。
#
# 脚本默认检查自身所在目录，通常与 .so 一起打包；
# 也可以指定目录：bash check_environment.sh /opt/mooncake/private-ext
#
# 检查项：系统架构、文件完整性、动态依赖可解析、运行时版本、
#         ABI 握手（python3 可用时）、环境变量解析结果。
set -uo pipefail

SELF_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
EXT_DIR=${1:-$SELF_DIR}
DFE=libmooncake_dfe.so.1
AHE=libmooncake_ahe.so.1
KERNEL=mc_copy_kernel.co

failures=0
warnings=0
say_ok()   { printf '  [ OK ] %s\n' "$*"; }
say_warn() { printf '  [WARN] %s\n' "$*"; warnings=$((warnings + 1)); }
say_fail() { printf '  [FAIL] %s\n' "$*"; failures=$((failures + 1)); }
need()     { command -v "$1" >/dev/null 2>&1; }

echo "============================================================"
echo " Private provider deployment check"
echo " directory: $EXT_DIR"
echo " host     : $(uname -srm 2>/dev/null || echo unknown)"
echo "============================================================"

echo "[1] 系统架构"
case "$(uname -m 2>/dev/null)" in
    x86_64|amd64) say_ok "x86_64" ;;
    *) say_fail "架构 $(uname -m) 与构建产物（x86_64）不一致" ;;
esac

echo "[2] 文件布局与权限"
if [ -f "$EXT_DIR/$DFE" ]; then
    [ -r "$EXT_DIR/$DFE" ] && say_ok "$DFE 可读" || say_fail "$DFE 不可读"
elif [ -e "$EXT_DIR/$AHE" ]; then
    say_warn "未发现 $DFE（当前为 AHE-only 包）"
else
    say_fail "缺少 $DFE"
fi
if [ -e "$EXT_DIR/$AHE" ]; then
    [ -r "$EXT_DIR/$AHE" ] && say_ok "$AHE 可读" || say_fail "$AHE 不可读"
    if [ -f "$EXT_DIR/$KERNEL" ] && [ -r "$EXT_DIR/$KERNEL" ]; then
        say_ok "$KERNEL 与 AHE 同目录"
    else
        say_fail "缺少可读的 $KERNEL（必须与 $AHE 同目录）"
    fi
else
    say_warn "未发现 $AHE（将使用逐 range 拷贝的公有兜底）"
fi

echo "[3] 动态依赖解析"
for lib in "$DFE" "$AHE"; do
    path="$EXT_DIR/$lib"
    [ -f "$path" ] || continue
    if need ldd; then
        if ldd "$path" 2>/dev/null | grep -q 'not found'; then
            say_fail "$lib 存在无法解析的库："
            ldd "$path" 2>/dev/null | grep 'not found' | sed 's/^/         /'
        else
            say_ok "$lib 依赖全部可解析"
        fi
    else
        say_warn "找不到 ldd，跳过 $lib 依赖检查"
    fi
done

echo "[4] 运行时版本下限"
if need objdump && need getconf; then
    have_glibc=$(getconf GNU_LIBC_VERSION 2>/dev/null | awk '{print $2}')
    printf '  目标机 glibc: %s\n' "${have_glibc:-unknown}"
    for lib in "$DFE" "$AHE"; do
        [ -f "$EXT_DIR/$lib" ] || continue
        req_glibc=$(objdump -T "$EXT_DIR/$lib" 2>/dev/null \
            | grep -oE 'GLIBC_[0-9.]+' | sort -Vu | tail -1)
        req_cxx=$(objdump -T "$EXT_DIR/$lib" 2>/dev/null \
            | grep -oE 'GLIBCXX_[0-9.]+' | sort -Vu | tail -1)
        printf '  %-26s 需要 %s %s\n' "$lib" "${req_glibc:-GLIBC_?}" \
            "${req_cxx:-GLIBCXX_?}"
        if [ -n "${have_glibc:-}" ] && [ -n "${req_glibc:-}" ] &&
            [ "$(printf '%s\n%s\n' "${req_glibc#GLIBC_}" "$have_glibc" \
                | sort -V | head -1)" != "${req_glibc#GLIBC_}" ]; then
            say_fail "$lib 需要的 glibc 高于目标机"
        fi
    done
else
    say_warn "找不到 objdump/getconf，跳过版本下限检查"
fi

echo "[5] ABI 握手（运行时加载）"
if need python3; then
    python3 - "$EXT_DIR" <<'PY'
import ctypes
import os
import sys

ext_dir = sys.argv[1]
checks = [("libmooncake_dfe.so.1", "mooncake_dfe_extension_get")]
if os.path.exists(os.path.join(ext_dir, "libmooncake_ahe.so.1")):
    checks.append(("libmooncake_ahe.so.1", "mooncake_ahe_extension_get"))

failures = 0
for name, symbol in checks:
    path = os.path.join(ext_dir, name)
    try:
        # Loading resolves every DTK/runtime dependency, which is what this
        # host-side check is about. The real ABI handshake is performed by the
        # loader inside the process; its result shows up as dfe_provider=.../
        # ahe_provider=... in the application log.
        lib = ctypes.CDLL(path)
    except OSError as exc:
        print(f"  [FAIL] 加载 {name} 失败: {exc}")
        failures += 1
        continue
    try:
        getattr(lib, symbol)
    except AttributeError:
        print(f"  [FAIL] {name} 缺少入口 {symbol}")
        failures += 1
        continue
    print(f"  [ OK ] {name} 加载成功且入口存在")

sys.exit(1 if failures else 0)
PY
    if [ $? -ne 0 ]; then
        failures=$((failures + 1))
    fi
else
    say_warn "找不到 python3，跳过运行时加载检查"
fi

echo "[6] 环境变量解析"
if [ "${MOONCAKE_ENABLE_EXT:-1}" = "0" ]; then
    say_warn "MOONCAKE_ENABLE_EXT=0，扩展加载被禁用"
fi
if [ -n "${MOONCAKE_EXT_DIR:-}" ]; then
    if [ -d "$MOONCAKE_EXT_DIR" ]; then
        say_ok "MOONCAKE_EXT_DIR=$MOONCAKE_EXT_DIR"
        for lib in "$DFE" "$AHE"; do
            [ -e "$EXT_DIR/$lib" ] || continue
            if [ -f "$MOONCAKE_EXT_DIR/$lib" ]; then
                say_ok "  $lib 位于 MOONCAKE_EXT_DIR 中"
            else
                say_warn "  $lib 不在 MOONCAKE_EXT_DIR 中，将回落到默认目录"
            fi
        done
    else
        say_fail "MOONCAKE_EXT_DIR 不是目录"
    fi
else
    say_warn "未设置 MOONCAKE_EXT_DIR，将回落到 /opt/mooncake/plugins"
fi

echo "------------------------------------------------------------"
echo " 提示：本机直接用 bash $(basename "${BASH_SOURCE[0]}") 重新检查"
if [ "$failures" -eq 0 ]; then
    echo "RESULT: PASS (${warnings} warning)"
    exit 0
fi
echo "RESULT: FAIL ($failures 项不通过, ${warnings} warning)"
exit 1
