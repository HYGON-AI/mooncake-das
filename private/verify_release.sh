#!/usr/bin/env bash
# 发布前验证（在编译机执行）。
#
# 脚本默认检查自身所在目录，可直接放在 dist 与 .so 一起打包；
# 也可以在 private/ 目录执行：bash verify_release.sh private/dist
#
# 检查项：文件布局、架构、导出符号收敛、依赖边界、RUNPATH、
#        工具链版本下限、ABI 握手冒烟测试。
set -uo pipefail

SELF_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
EXT_DIR=${1:-$SELF_DIR}
DFE=libmooncake_dfe.so.1
AHE=libmooncake_ahe.so.1
KERNEL=mc_copy_kernel.co

# Which providers this package is supposed to contain: all | dfe | ahe.
EXPECT_PROVIDERS=${EXPECT_PROVIDERS:-all}
case "$EXPECT_PROVIDERS" in
    all|both|"") EXPECT_PROVIDERS=all ;;
    dfe|ahe) ;;
    *)
        echo "invalid EXPECT_PROVIDERS=$EXPECT_PROVIDERS (all|dfe|ahe)" >&2
        exit 2
        ;;
esac

failures=0
say_ok()   { printf '  [ OK ] %s\n' "$*"; }
say_warn() { printf '  [WARN] %s\n' "$*"; }
say_fail() { printf '  [FAIL] %s\n' "$*"; failures=$((failures + 1)); }
need()     { command -v "$1" >/dev/null 2>&1; }

find_abi_header() {
    local dir=$SELF_DIR
    local i
    for i in 1 2 3 4 5; do
        local header="$dir/mooncake-store/include/ext/mooncake_private_ext_abi.h"
        if [ -f "$header" ]; then
            printf '%s\n' "$header"
            return 0
        fi
        dir=$(dirname "$dir")
    done
    return 1
}

echo "============================================================"
echo " Private provider release gate"
echo " directory: $EXT_DIR"
echo "============================================================"

echo "[1] 文件布局（期望: $EXPECT_PROVIDERS）"
if [ -f "$EXT_DIR/$DFE" ]; then
    say_ok "$DFE"
elif [ "$EXPECT_PROVIDERS" = "ahe" ]; then
    : # AHE-only package: DFE is intentionally absent.
else
    say_fail "缺少 $DFE"
fi
if [ -f "$EXT_DIR/$AHE" ]; then
    say_ok "$AHE"
    [ -f "$EXT_DIR/$KERNEL" ] && say_ok "$KERNEL 与 AHE 同目录" \
        || say_fail "缺少 $KERNEL（必须与 $AHE 同目录）"
elif [ "$EXPECT_PROVIDERS" = "ahe" ]; then
    say_fail "缺少 $AHE"
elif [ "$EXPECT_PROVIDERS" = "dfe" ]; then
    : # DFE-only package: AHE is intentionally absent.
else
    say_warn "未发现 $AHE（仅发布 DFE）"
fi

echo "[2] 架构"
if need readelf; then
    for lib in "$DFE" "$AHE"; do
        [ -f "$EXT_DIR/$lib" ] || continue
        machine=$(readelf -h "$EXT_DIR/$lib" | sed -n 's/.*Machine: *//p')
        case "$machine" in
            *X86-64*|*x86-64*) say_ok "$lib: $machine" ;;
            *) say_fail "$lib 架构异常: ${machine:-unknown}" ;;
        esac
    done
else
    say_warn "找不到 readelf，跳过架构检查"
fi

echo "[3] 导出符号（应只有入口函数）"
check_exports() {
    local lib=$1
    shift
    local path="$EXT_DIR/$lib"
    [ -f "$path" ] || return
    if ! need nm; then
        say_warn "找不到 nm，跳过 $lib 符号检查"
        return
    fi
    local actual expected
    actual=$(nm -D --defined-only "$path" 2>/dev/null | awk '$2 == "T" {print $3}' | sort)
    expected=$(printf '%s\n' "$@" | sort)
    if [ "$actual" = "$expected" ]; then
        say_ok "$lib 导出: $(printf '%s ' $expected)"
    else
        say_fail "$lib 导出符号不符合预期"
        printf '         expected: %s\n' "$(printf '%s ' $expected)"
        printf '         actual  : %s\n' "$(printf '%s ' $actual)"
    fi
}
check_exports "$DFE" mooncake_dfe_extension_get
[ -f "$EXT_DIR/$AHE" ] && check_exports "$AHE" mooncake_ahe_extension_get

echo "[4] 依赖边界"
if [ -f "$EXT_DIR/$DFE" ] && need readelf; then
    if readelf -d "$EXT_DIR/$DFE" | grep NEEDED | grep -Eqi 'hip|cuda|dtk|galaxy'; then
        say_fail "$DFE 不应依赖任何设备运行时"
    else
        say_ok "$DFE 无设备运行时依赖"
    fi
fi
if [ -f "$EXT_DIR/$AHE" ]; then
    if need readelf; then
        readelf -d "$EXT_DIR/$AHE" | grep NEEDED | grep -Eqi 'hip|galaxy' \
            && say_ok "$AHE 依赖设备运行时" \
            || say_warn "$AHE 未列出设备运行时依赖（请确认）"
        runpath=$(readelf -d "$EXT_DIR/$AHE" \
            | sed -n 's/.*(RUNPATH).*\[\(.*\)\].*/\1/p')
        if [ -n "$runpath" ]; then
            say_ok "$AHE RUNPATH=$runpath"
            case "$runpath" in
                *dtk*|*rocm*|*hip*) ;;
                *) say_warn "$AHE RUNPATH 未包含设备运行时目录" ;;
            esac
        else
            say_warn "$AHE 没有 RUNPATH，目标机需要 LD_LIBRARY_PATH"
        fi
    fi
    if need ldd; then
        if ldd "$EXT_DIR/$AHE" 2>/dev/null | grep -q 'not found'; then
            say_fail "$AHE 存在无法解析的库"
            ldd "$EXT_DIR/$AHE" 2>/dev/null | grep 'not found' | sed 's/^/         /'
        else
            say_ok "$AHE 依赖在当前机器全部可解析"
        fi
    fi
fi

echo "[5] 工具链版本下限（目标机不得低于此）"
if need objdump; then
    for lib in "$DFE" "$AHE"; do
        [ -f "$EXT_DIR/$lib" ] || continue
        max_glibc=$(objdump -T "$EXT_DIR/$lib" 2>/dev/null \
            | grep -oE 'GLIBC_[0-9.]+' | sort -Vu | tail -1)
        max_cxx=$(objdump -T "$EXT_DIR/$lib" 2>/dev/null \
            | grep -oE 'GLIBCXX_[0-9.]+' | sort -Vu | tail -1)
        printf '  %-26s 需要 %s %s\n' "$lib" "${max_glibc:-GLIBC_?}" \
            "${max_cxx:-GLIBCXX_?}"
    done
else
    say_warn "找不到 objdump，跳过版本下限检查"
fi

echo "[6] ABI 握手冒烟（dlopen + 入口调用）"
header=$(find_abi_header || true)
if [ -n "${header:-}" ] && need cc; then
    tmpdir=$(mktemp -d)
    trap 'rm -rf "$tmpdir"' EXIT
    cat > "$tmpdir/smoke.c" <<'EOF'
#include <dlfcn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mooncake_private_ext_abi.h"

static int check(const char* path, const char* sym, uint32_t abi) {
    void* handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (handle == NULL) {
        printf("  [FAIL] dlopen %s: %s\n", path, dlerror());
        return 1;
    }
    int (*get)(void*) = (int (*)(void*))dlsym(handle, sym);
    if (get == NULL) {
        printf("  [FAIL] %s 缺少入口 %s\n", path, sym);
        return 1;
    }
    /* The provider requires struct_size to match its own layout exactly, so
       allocate the real struct instead of an oversized scratch buffer. */
    union {
        mooncake_dfe_ext dfe;
        mooncake_ahe_ext ahe;
    } ext;
    memset(&ext, 0, sizeof(ext));
    uint32_t* fields = (uint32_t*)&ext;
    fields[0] = abi;
    fields[1] = (uint32_t)(strstr(sym, "dfe") != NULL
                               ? sizeof(mooncake_dfe_ext)
                               : sizeof(mooncake_ahe_ext));
    int rc = get(&ext);
    if (rc != MOONCAKE_EXT_STATUS_OK) {
        printf("  [FAIL] %s ABI 握手返回 %d\n", sym, rc);
        return 1;
    }
    printf("  [ OK ] %s 握手成功\n", sym);
    return 0;
}

int main(int argc, char** argv) {
    int fail = 0;
    int i;
    /* Arguments come in triples: <path> <symbol> <expected abi version>. */
    for (i = 1; i + 2 < argc; i += 3) {
        fail += check(argv[i], argv[i + 1],
                      (uint32_t)strtoul(argv[i + 2], NULL, 10));
    }
    if (i == 1) {
        printf("  [FAIL] 未指定要检查的 provider\n");
        return 1;
    }
    return fail;
}
EOF
    if cc -O0 -o "$tmpdir/smoke" "$tmpdir/smoke.c" -ldl \
        -I "$(dirname "$header")" 2>"$tmpdir/build.log"; then
        dfe_abi=$(sed -n \
            's/^#define[[:space:]]*MOONCAKE_DFE_ABI_VERSION[[:space:]]*\([0-9][0-9]*\).*/\1/p' \
            "$header" | head -1)
        ahe_abi=$(sed -n \
            's/^#define[[:space:]]*MOONCAKE_AHE_ABI_VERSION[[:space:]]*\([0-9][0-9]*\).*/\1/p' \
            "$header" | head -1)
        args=()
        [ -f "$EXT_DIR/$DFE" ] && args+=("$EXT_DIR/$DFE" \
            mooncake_dfe_extension_get "${dfe_abi:-2}")
        [ -f "$EXT_DIR/$AHE" ] && args+=("$EXT_DIR/$AHE" \
            mooncake_ahe_extension_get "${ahe_abi:-1}")
        if [ "${#args[@]}" -eq 0 ]; then
            say_warn "目录中没有可做冒烟测试的 provider"
        elif "$tmpdir/smoke" "${args[@]}"; then
            say_ok "provider ABI 版本与公有头一致"
        else
            say_fail "ABI 握手失败"
        fi
    else
        say_warn "冒烟程序编译失败，跳过"
        sed 's/^/         /' "$tmpdir/build.log" | tail -5
    fi
else
    say_warn "缺少 ABI 头或 cc，跳过冒烟测试"
fi

echo "[7] DFE 批放置策略冒烟（生产尺寸桶）"
if [ -f "$EXT_DIR/$DFE" ] && [ -n "${header:-}" ] && need cc; then
    tmpdir_align=$(mktemp -d)
    cat > "$tmpdir_align/align.c" <<'EOF'
#include <dlfcn.h>
#include <stdio.h>
#include <string.h>

#include "mooncake_private_ext_abi.h"

int main(int argc, char** argv) {
    if (argc < 2) return 1;
    const char* mode = argc > 2 ? argv[2] : "aligned";
    void* handle = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL);
    if (handle == NULL) {
        printf("  [FAIL] dlopen: %s\n", dlerror());
        return 1;
    }
    int (*get)(mooncake_dfe_ext*) = (int (*)(mooncake_dfe_ext*))dlsym(
        handle, "mooncake_dfe_extension_get");
    if (get == NULL) {
        printf("  [FAIL] 缺少入口 mooncake_dfe_extension_get\n");
        return 1;
    }
    mooncake_dfe_ext ext;
    memset(&ext, 0, sizeof(ext));
    ext.abi_version = MOONCAKE_DFE_ABI_VERSION;
    ext.struct_size = sizeof(ext);
    if (get(&ext) != MOONCAKE_EXT_STATUS_OK || ext.align_batch_offset == NULL) {
        printf("  [FAIL] ABI 握手失败\n");
        return 1;
    }
    /* Production-like geometry: a large bucket and a small object appended
       after a non-aligned offset. */
    const uint64_t capacity = 1ull << 32;
    uint64_t offset = 0;
    uint64_t alignment = 0;
    uint32_t fits = 0;
    int rc = ext.align_batch_offset(ext.user_data, 4096, capacity, 4096,
                                    &offset, &alignment, &fits);
    if (rc != MOONCAKE_EXT_STATUS_OK || !fits) {
        printf("  [FAIL] align_batch_offset rc=%d fits=%u\n", rc, fits);
        return 1;
    }
    if (strcmp(mode, "compact") == 0) {
        if (offset != 4096 || alignment != 0) {
            printf("  [FAIL] 关闭对齐未回到紧凑布局: offset=%llu alignment=%llu\n",
                   (unsigned long long)offset, (unsigned long long)alignment);
            return 1;
        }
        printf("  [ OK ] 关闭对齐: offset=4096 alignment=0\n");
        return 0;
    }
    if (alignment == 0 || offset != alignment) {
        printf("  [FAIL] 未按声明对齐: offset=%llu alignment=%llu\n",
               (unsigned long long)offset, (unsigned long long)alignment);
        return 1;
    }
    printf("  [ OK ] 对齐生效: offset=%llu alignment=%llu\n",
           (unsigned long long)offset, (unsigned long long)alignment);
    return 0;
}
EOF
    if cc -O0 -o "$tmpdir_align/align" "$tmpdir_align/align.c" -ldl \
        -I "$(dirname "$header")" 2>"$tmpdir_align/build.log"; then
        if MOONCAKE_DFS_BUCKET_BATCH_ALIGNMENT= "$tmpdir_align/align" \
            "$EXT_DIR/$DFE" aligned; then
            say_ok "对齐策略按 provider 默认值生效"
        else
            say_fail "对齐策略未生效"
        fi
        if MOONCAKE_DFS_BUCKET_BATCH_ALIGNMENT=0 "$tmpdir_align/align" \
            "$EXT_DIR/$DFE" compact; then
            say_ok "对齐可关闭并回到紧凑布局"
        else
            say_fail "对齐关闭开关无效"
        fi
    else
        say_warn "对齐冒烟程序编译失败，跳过"
        sed 's/^/         /' "$tmpdir_align/build.log" | tail -5
    fi
    rm -rf "$tmpdir_align"
else
    say_warn "无 DFE 或缺 ABI 头/cc，跳过对齐冒烟"
fi

echo "------------------------------------------------------------"
if [ "$failures" -eq 0 ]; then
    echo "RESULT: PASS"
    exit 0
fi
echo "RESULT: FAIL ($failures 项不通过)"
exit 1
