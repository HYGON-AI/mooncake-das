# 等待队列 DFS 预取监控

本页针对 SGLang 的 `--mooncake-enable-waiting-queue-dfs-prefetch`：forward
期间接收请求，经过后台预取队列，将 DFS 对象提前放入 session 的 host 缓冲，
请求加载时再消费该缓冲。请求实时读取同一条 session range 路径作为对照。
不统计其他预取机制；MEMORY 和 LOCAL_DISK 数据不进入以下 L4 消费量。

## 指标口径

指标从 Mooncake 客户端的 `/metrics` 导出，沿用现有 ClientMetric 开关
`MC_STORE_CLIENT_METRIC`（默认开启），不需要启用 trace。
抓取 SGLang 进程内 Mooncake 客户端的 HTTP 端口，不是仅抓 master。

| 指标 | 标签 | 含义 |
| --- | --- | --- |
| `mooncake_l4_read_operations_total` | `mode`, `result` | 真正发起的 DFS `BatchGet` 批次数 |
| `mooncake_l4_read_bytes_total` | `mode`, `result` | 这些批次中成功从 DFS 读取的物理字节 |
| `mooncake_l4_read_duration_seconds` | `mode`, `result` | 每个 DFS `BatchGet` 的耗时直方图 |
| `mooncake_l4_restore_ranges_total` | `mode` | 请求成功加载的 DFS range 数量 |
| `mooncake_l4_restore_bytes_total` | `mode` | 请求成功加载的 DFS range 有效字节 |

读取指标的 `mode=prefetch` 表示等待队列后台预取；`mode=on_demand`
表示请求加载阶段实时回源。`result=success/failure` 表示整个批次的结果，
部分失败批次的成功字节仍计在 `failure` 中。缓冲分配失败且未调用 `BatchGet`
不计读取次数，由 SGLang 的预取任务失败指标反映。

消费指标的 `mode=prefetched` 表示使用等待队列预取缓冲；`mode=on_demand`
表示使用实时回源的数据。只在 scatter 成功完成后累加，失败传输和仅完成
host 预取均不算消费。range 数量按调用中提供的 ranges 计数，不是请求数。

## 核心看板

预取覆盖率，分子与分母都是请求消费的有效字节：

```promql
100 * (sum(rate(mooncake_l4_restore_bytes_total{mode="prefetched"}[5m])) or vector(0))
/ sum(rate(mooncake_l4_restore_bytes_total[5m]))
```

两种方式的请求消费带宽：

```promql
sum by (mode) (rate(mooncake_l4_restore_bytes_total[5m]))
```

预取和实时回源各自的物理读取带宽、P95 耗时：

```promql
sum by (mode) (rate(mooncake_l4_read_bytes_total[5m]))
```

```promql
histogram_quantile(0.95,
  sum by (le, mode) (rate(mooncake_l4_read_duration_seconds_bucket[5m])))
```

物理读取可能含对齐、整对象读取和重复加载；不能把消费字节除以预取读取字节
称为命中率或精确利用率。覆盖率无 L4 消费时没有有效值；仅实时回源时为 0%。
多个实例/模型请在查询中选择同一组实例，不要混合不同业务的分母。
关闭等待队列预取后，正常 DFS range 加载只产生 `on_demand`，可用于对比实验。

SGLang 的任务完成、失败、取消、领取和提前就绪时间见其 Mooncake Store README。
领取并不代表 DMA 成功，实际收益以上述成功消费覆盖率为准。
