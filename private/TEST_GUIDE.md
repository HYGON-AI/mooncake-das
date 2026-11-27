# 私有扩展（DFE / AHE）测试指导

> 适用版本：ABI DFE v3、AHE v1。本文所有命令均以
> `/stortest/wangrui_space/HYGON-AI/mooncake-das` 为仓库根目录。

## 0. 前置准备

```bash
cd /stortest/wangrui_space/HYGON-AI/mooncake-das

# 1) 公有侧编译（含契约测试）
GOPROXY='https://goproxy.cn,https://goproxy.io,direct' \
  bash scripts/variant_build.sh --shca --hip --etcd --skip-deps

# 2) 打开单元测试开关（variant_build.sh 默认 BUILD_UNIT_TESTS=OFF）
cmake -S . -B build -DBUILD_UNIT_TESTS=ON
cmake --build build -j"$(nproc)"

# 3) 私有 provider 打包（默认 DFE + AHE，并自动跑发布验证）
bash private/package_private_ext.sh
# 产物：private/dist/{libmooncake_dfe.so.1, libmooncake_ahe.so.1, mc_copy_kernel.co}
```

> 每次重跑 `variant_build.sh` 都会把 `BUILD_UNIT_TESTS` 重置为 `OFF`，
> 需要再执行一次上面的 `cmake -S . -B build -DBUILD_UNIT_TESTS=ON`。
> 测试二进制位于 `build/mooncake-store/tests/`。

两个环境变量：

| 变量 | 含义 |
|---|---|
| `MOONCAKE_EXT_DIR` | provider 目录；按固定文件名查找，未设置时回落 `/opt/mooncake/plugins` |
| `MOONCAKE_ENABLE_EXT` | `0` 时两个 provider 都不加载（等价于"无 .so"） |

> 私有库**自身不打任何日志**（避免泄露）。效果通过"加载状态日志 + 行为差异 +
> 性能"三类证据判断。

---

## 1. 第一层：包自检（不启动服务，1 分钟）

```bash
cd private/dist
bash ./verify_release.sh
bash ./check_environment.sh
```

期望结尾：

```
[6] ABI 握手冒烟（dlopen + 入口调用）
  [ OK ] mooncake_dfe_extension_get 握手成功
  [ OK ] mooncake_ahe_extension_get 握手成功
RESULT: PASS
```

失败对照：

| 输出 | 原因 | 处理 |
|---|---|---|
| `缺少 libmooncake_dfe.so.1` | 当前目录不是 dist | `cd private/dist` 后再跑 |
| `ABI 握手返回 2` | 私有 .so 与公有 ABI 头/版本不一致（INVALID_ARGUMENT） | 重编私有库 |
| `加载失败 ... galaxyhip` | 目标机缺 DTK | 检查 `/opt/dtk/hip/lib`、`LD_LIBRARY_PATH` |
| `RESULT: FAIL` | 按上方逐项修复 | — |

---

## 2. 第二层：公有契约测试（不依赖真实 .so）

```bash
ctest --test-dir build --output-on-failure -R "extension_loader"
```

应看到 4 个用例全过：

| 用例 | 验证点 |
|---|---|
| `Loaded` | fake provider 全部回调（io_batch / align / fd / mtime / buffer_policy）契约 |
| `Absent` | 目录里没有 provider → `available()==false`，调用方走兜底 |
| `Disabled` | `MOONCAKE_ENABLE_EXT=0` → 不加载 |
| `AbiMismatch` | provider 返回 ABI 错误 → 安全降级、不崩溃 |

这一步与真实 `.so` 无关；即使没打私有包也应通过。

---

## 3. 第三层：无 .so 基线（降级正确性）

```bash
unset MOONCAKE_EXT_DIR
ctest --test-dir build --output-on-failure \
      -R "dfs_immutable_bucket|file_storage|distributed_storage_config"
```

通过标准：全部 PASS。这些用例覆盖读合并回落、写逐对象 `pwrite`、bucket
分配不推进对齐等基线行为。

---

## 4. 第四层：有 .so 私有路径（功能一致性）

默认对齐（512K）下现在可以直接跑：分配器不再盲信 provider 的 `fits`，
而是自己按桶几何计算起点和容量，`ConcurrentFreeAndUpdateAccessAreSafe`
与 `ConcurrentAllocateProducesDisjointRegions` 在 4 MiB 桶上都能通过，并新增
了 `DefaultAlignmentPacksPastHalfAndCrossesBuckets` 覆盖“写过半、跨桶”。

只有断言**精确紧凑偏移**（0/4096/8192…）的用例仍需关掉对齐：

```bash
export MOONCAKE_EXT_DIR=$PWD/private/dist
# 先按默认对齐跑一遍（含跨桶回归）：
ctest --test-dir build --output-on-failure \
      -R "dfs_immutable_bucket|file_storage|distributed_storage_config"
# 再关掉对齐跑紧凑布局一致性：
export MOONCAKE_DFS_BUCKET_BATCH_ALIGNMENT=0
ctest --test-dir build --output-on-failure \
      -R "dfs_immutable_bucket|file_storage|distributed_storage_config"
```

通过标准：与第三层结果一致（数量、通过率相同）。
一致性即证明"有/无私有实现，功能正确性不变"。

`BatchReadIssuesOneReadPerObjectWithoutProvider` 在 provider 已加载时会自动
`SKIP`（provider 是进程级单例，该用例只验证无 `.so` 基线，由第三层覆盖）。

> 若测试进程未触发 DFS/加速器初始化，可能看不到加载日志，属正常；
> 加载日志以服务级验证（第 5 层）为准。

### 4.1 已知失败基线（与私有化无关）

以下用例在**无 `.so` 时同样失败**，属于本机既有问题，不参与"有/无一致性"判定：

| 套件 | 用例 |
|---|---|
| `file_storage_test` | `RemoveAllOrdinarySsdReclaimedAtWatermark`、`RemoveAllReclaimsOrdinaryFileBesideManagedFiles`、`RemoveAllReclaimsOrdinaryFileAfterProtectedPrefix`（走 `file_per_key_dir`/`managed_ssd`，不经过 DFE；`StoredFileBytes` 统计为 0） |
| `dfs_immutable_bucket_client_test` | `UpsertKeepsSynchronousDfsSemantics`、`BatchUpsertKeepsSynchronousDfsSemantics`、`StaleDfsReadRetryAllowsExistenceFilteredDfs`、`DfsBucketEpochClientTest.HealthyRestartRefreshesOnlyUnchangedReadViews` |

此外，以下 9 个用例靠公有 adapter 注入"写阻塞/写失败"来构造场景，
数据面走 provider 后注入无法生效，因此在 provider 加载时自动 `SKIP`：

| 套件 | 用例 |
|---|---|
| `DfsImmutableBucketClientTest` | `UpsertReturnsAfterWriteAndFailureRevokes`、`DfsOnlyObjectsLiveAndDieWithTheirBucket` |
| `DfsBucketClientSlotTimeoutTest` | `SlotTimeoutKeepsMemoryReplicasForUnsubmittedChunks` |
| `DfsBucketClientTest` | `BatchPutDeadlineKeepsMemoryAndLateDfsWriteIsRevoked`、`DuplicateKeyTimeoutKeepsEveryBufferAlive`、`FailedBoundedAsyncWriteKeepsMemoryReplica`、`ServiceMetricsAsyncWriteMovesFromPendingToPhysicalOutcome`、`ServiceMetricsDeadlineSettlesBlockedWriteAndIgnoresLateResult`、`ServiceMetricsSynchronousDualWriteReportsPhysicalPartialOutcome` |

**判定标准**：有 `.so` 的失败集合 == 无 `.so` 的失败集合（skip 不计）。

### 4.2 验证 512K 对齐生效（已并入发布门禁）

对齐只在桶几何兼容时启用（`capacity % alignment == 0` 且 `alignment <= capacity`），
测试桶（4 KiB / 1 MiB）不具代表性，因此该项不在单测里验证。

`verify_release.sh` 的 `[7] DFE 批放置策略冒烟` 已自动覆盖：

- 以 4 GiB 桶 + 4096 起始偏移询问 provider，断言 `alignment > 0` 且
  `out_offset` 等于上报的对齐值（默认即 524288）；
- 设 `MOONCAKE_DFS_BUCKET_BATCH_ALIGNMENT=0` 再询问，断言回到紧凑布局
  （`out_offset == 4096`、`alignment == 0`）。

```bash
cd private/dist && ./verify_release.sh
# 期望:
# [7] DFE 批放置策略冒烟（生产尺寸桶）
#   [ OK ] 对齐生效: offset=524288 alignment=524288
#   [ OK ] 对齐可关闭并回到紧凑布局
```

若要在真实服务上复核，可用 4 GiB 桶启动 master，写两个小对象后对比
`MOONCAKE_DFS_BUCKET_BATCH_ALIGNMENT` 默认与 `=0` 时的对象起始偏移。

> 实测记录（2026-11-26，DTK 环境）：`offset=524288 alignment=524288`；
> `MOONCAKE_DFS_BUCKET_BATCH_ALIGNMENT=0` 时 `offset=4096 alignment=0`。

---

## 5. 第五层：服务级冒烟与日志确认

按你现有方式启动 master 和 client，仅在**两个进程**前加同一环境变量：

```bash
export MOONCAKE_EXT_DIR=/stortest/wangrui_space/HYGON-AI/mooncake-das/private/dist
# 启动 master（原命令不变）
# 启动 client（原命令不变）
```

### 升级顺序约束（写上下文 / mtime）

master 启动后在 DFS 根目录**原子发布**
`.mooncake_dfs_write_context_capability`（内容
`mooncake-dfs-write-context-v1\n0\n` 或 `...\n1\n`：`1` 表示 master 装了 DFE、
能在 seal 时刷新桶 mtime；`0` 表示不能）。client 冷开时读取该文件：

- **新 client**：只有解析到 `1` 才跳过“每次写更新 mtime”；文件缺失、损坏或为
  `0` 时一律保留每次写的 mtime 更新（安全默认，mtime 不会冻结）。
- **旧 client**（不读该文件，如基线 `fe0e9011`）：只要
  `MOONCAKE_DFS_POSIX_WRITE_CONTEXT_ENABLED` 开启且文件系统 ioctl 成功，就跳过
  每次写 mtime，完全依赖 master 在 seal 时刷新。

不安全组合：**不装 DFE 的 master（不在 seal 刷新 mtime）+ 旧 client** → 桶 mtime
冻结在最后一次真实写入。

安全升级顺序：

1. 先升 client：新 client 会读能力文件，面对不装库的 master 自动保留每次写
   mtime，无冻结风险；
2. client 全部升级完成后，再允许 master 不带 DFE 运行；
3. 过渡期若必须让旧 client 对接不装库的 master，在旧 client 上设
   `MOONCAKE_DFS_POSIX_WRITE_CONTEXT_ENABLED=0`，强制走每次写 mtime 的安全兜底。

### 关键日志（glog INFO 级别，默认可见）

| 日志 | 含义 | 期望 |
|---|---|---|
| `dfe_provider=loaded path=.../libmooncake_dfe.so.1` | DFE 加载成功 | master、client 都应有 |
| `ahe_provider=loaded path=.../libmooncake_ahe.so.1` | AHE 加载成功 | client（HIP 环境）应有 |
| `dfe_provider=absent file=libmooncake_dfe.so.1` | 目录里没有 DFE | 未部署时正常 |
| `ahe_provider=absent file=libmooncake_ahe.so.1` | 目录里没有 AHE | CPU-only 环境正常 |
| `dfe_provider=disabled reason=not_enabled` | `MOONCAKE_ENABLE_EXT=0` | 主动关闭时出现 |
| `dfe_provider=disabled reason=abi_mismatch path=... status=...` | ABI 不一致 | **必须重编私有库** |
| `dfe_provider=disabled reason=load_failed path=... detail=...` | 依赖缺失 | 看 `detail`，多为 `ldd` 问题 |
| `dfe_provider=disabled reason=symbol_not_found path=...` | 包不完整/文件被替换 | 重新打包 |
| `ahe_provider=disabled reason=abi_mismatch ...` | AHE 同上 | 重编 |
| `Failed to synchronize a DFS H2D stream during shutdown` | 退出时 stream 同步失败 | 偶发可接受，复现则排查 |

快速过滤：

```bash
grep -E "dfe_provider|ahe_provider" master.log client.log
```

### 一次功能冒烟

1. client `put` 一批对象（建议 ≥ 2 MiB，含多 key），再 `get` 回来比对内容；
2. 检查 master 日志出现 `dfe_provider=loaded`；
3. 客户端写入时观察 bucket 文件布局：`MOONCAKE_DFS_BUCKET_BATCH_ALIGNMENT=0`
   与默认值 512K 两次对比（文件内对象起始偏移是否 512K 对齐）。

---

## 6. 效果对照表（同一负载，改环境变量即可 A/B）

| 观测点 | 无 .so / 关闭 | 有 .so | 验证方式 |
|---|---|---|---|
| 读路径 | 多 task 并发读（RunParallel 上限 128 线程，不合并相邻 range） | provider 合并相邻 range + 并行 | 大 batch_get 吞吐、CPU 利用率 |
| 写路径 | 逐对象 `pwrite` | provider run 合并 + 并行 | 批量 put 吞吐、iowait |
| 写 batch 对齐 | 紧凑追加 | 512K 对齐 | bucket 文件内偏移 |
| buffer 准入 | 固定 2 读 / 1 写 chunk | provider 策略 | 高水位告警日志、OOM/复制次数 |
| H2D | 逐 range `hipMemcpyAsync` | scatter kernel（≥32 range） | 批量上卡耗时、`ahe_provider=loaded` |

常用私有侧开关（只对私有库生效，公有树不解析）：

```bash
export MOONCAKE_DFS_BUCKET_BATCH_ALIGNMENT=0        # 退回旧紧凑布局
export MOONCAKE_DFS_BATCH_READ_MERGE_ENABLED=0      # 关闭读合并
export MOONCAKE_DFS_BATCH_READ_THREADS=32           # 读并发上限
```

---

## 7. 性能对比步骤

1. 固定同一构建产物、同一数据、同一并发，清 page cache 或统一预热；
2. 先 `unset MOONCAKE_EXT_DIR` 跑 3 轮取中位数；
3. 再 `export MOONCAKE_EXT_DIR=...` 跑 3 轮取中位数；
4. 关注：batch_get QPS、put 吞吐、p99 延迟、CPU 使用率、iowait；
5. 预期：有 `.so` 不明显退化；私有路径在批量读/写上应更优。

---

## 8. 验收清单

- [ ] `verify_release.sh` RESULT: PASS
- [ ] `check_environment.sh` RESULT: PASS（目标机）
- [ ] `extension_loader_test` 4 个用例全过
- [ ] 无 `.so` 与有 `.so` 两轮 DFS 相关测试结果一致
- [ ] master/client 日志出现 `dfe_provider=loaded`（HIP 环境另有 `ahe_provider=loaded`）
- [ ] `MOONCAKE_ENABLE_EXT=0` 时两个 provider 都不加载，功能仍正确
- [ ] 冒烟 put/get 内容一致，无 `abi_mismatch` / `load_failed`
