# 私有 SO 方案说明

## 一、什么是私有 SO

公有 Mooncake Store 自带一套 DFS 数据面与 GPU 搬运实现（基础兜底版），代码开源。
HYGON-AI 的高性能优化已从公有树剥离为两个独立动态库，代码不开源，运行时按需加载。
当前私有化的能力如下。

### DFE（`libmooncake_dfe.so.1`，DFS 数据面扩展）

| # | 能力 | 当前实现 |
|---|---|---|
| 1 | 读聚合并发 | 把同 fd、连续偏移的读请求合并为 run，多 run 走线程池并行；无 provider 时逐 task 串行读 |
| 2 | 并发异步写 | 写请求同样合并为 run 后并行落盘；无 provider 时逐对象 `pwrite` |
| 3 | 写 batch 512K 对齐并禁用区间扩展锁和时间 | 批量写按 512K 对齐放置（桶几何兼容时）；写前下发 `SKIP_TIME` + `NO_EXTEND_LOCK` 写上下文 ioctl，写后刷新 mtime。文件系统不支持时单次告警后降级，不再重试 |
| 4 | 统一 buffer（低优），初始化 mapped 内存 | 由私有策略给出 buffer 准入水位；mapped 分配优先 AHE，失败回退 `hipHostMallocMapped`，再失败回退纯 pinned |

### AHE（`libmooncake_ahe.so.1`，HIP/GPU 侧扩展）

| # | 能力 | 当前实现 |
|---|---|---|
| 1 | H2D 的 kernel 函数和拷贝 | 自带 `mc_copy_kernel.co` scatter kernel 与 `submit/sync` 批量 H2D 拷贝；无 AHE 时逐 range `hipMemcpyAsync` |

加载时机：DFE 在初始化 DFS bucket 存储时立即加载（通常为 client）；
AHE 在 client 首次触发 GPU mapped 分配时懒加载（不一定每次推理都会走到）。

公有树中不含任何私有参数与算法。私有库缺失或 ABI 不匹配时，自动回退公有基线实现，
功能正确性不变，仅性能下降。

## 二、开启方法

默认配置下是开启的，把私有库so文件部署到固定目录即可，默认目录为 `/opt/mooncake/plugins`：

```bash
cp -r private/dist/* /opt/mooncake/plugins/
```

私有库按固定文件名查找，无需其他配置。若要放到自定义目录，需在需要加载
私有库的进程启动前设置：

```bash
export MOONCAKE_EXT_DIR=/你的/自定义/路径
```

启动命令与之前完全一致，无需改动。

依赖文件清单：

```
libmooncake_dfe.so.1         ← DFE 主库（实际加载的文件名）
libmooncake_ahe.so.1         ← AHE 主库（实际加载的文件名）
mc_copy_kernel.co            ← AHE 配套 GPU kernel，必须与 AHE 同目录
```

## 三、关闭方法

```bash
export MOONCAKE_ENABLE_EXT=0
```

默认值是开启的，关闭需设置为0；设为 `0` 时两个 provider 都不加载，等价于无 SO 环境。`unset` 该变量即恢复加载。

## 四、开启成功的日志

日志关键字过滤：

```bash
grep -E "dfe_provider|ahe_provider" client.log
```

DFE 加载日志出现在**初始化 DFS bucket 存储的进程**中。client 上必有：

```
dfe_provider=loaded path=/opt/mooncake/plugins/libmooncake_dfe.so.1
```

master 只有在自身也配置了 DFS bucket 存储（`fsdir`）时才会初始化这部分，
才可能出现同样的日志；纯元数据模式的 master 不触发，看不到该日志属正常。

client 在 HIP 环境首次触发 GPU mapped 分配后应有 AHE 加载日志：

```
ahe_provider=loaded path=/opt/mooncake/plugins/libmooncake_ahe.so.1
```

### 未加载成功的日志

若看到以下日志，说明对应 provider 没有成功加载，需按下表排查
（DFE 前缀为 `dfe_provider`，AHE 为 `ahe_provider`，其余结构相同）：

| 日志 | 说明的问题 | 处置 |
|---|---|---|
| `<prov>=absent file=...` | 默认目录与 `MOONCAKE_EXT_DIR` 都没有该文件 | 确认已拷到 `/opt/mooncake/plugins/`，或 `MOONCAKE_EXT_DIR` 指向正确 |
| `<prov>=disabled reason=not_enabled` | `MOONCAKE_ENABLE_EXT=0`，主动关闭 | 若要开启，`unset MOONCAKE_ENABLE_EXT` |
| `<prov>=disabled reason=load_failed path=... detail=...` | `dlopen` 失败，通常是依赖库缺失 | 看 `detail`，用 `ldd` 排查（如 AHE 缺 `libgalaxyhip.so.5`） |
| `<prov>=disabled reason=symbol_not_found path=...` | `.so` 存在但缺少入口符号 | 包不完整或被替换，重新部署 `dist/` |
| `<prov>=disabled reason=abi_mismatch path=... status=...` | 私有库与公有 ABI 头版本不一致 | 用匹配的公有头重编私有库 |

## 五、测试准则

**功能相同，性能不同。**

有 SO 与无 SO 两种情况下，功能正确性与输出结果应完全一致，差异只体现在
性能上（有 SO 的 DFS 批量读写吞吐更高、推理总耗时更短）。若功能结果出现
差异，即视为问题；若仅有性能差异，属正常。
