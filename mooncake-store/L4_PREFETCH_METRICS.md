# 等待队列 DFS 预取监控

本页针对 SGLang 的 `--mooncake-enable-waiting-queue-dfs-prefetch`：forward
期间接收请求，经过后台预取队列，将 DFS 对象提前放入 session 的 host 缓冲，
请求加载时再消费该缓冲。请求实时读取同一条 session range 路径作为对照。
不统计其他预取机制；MEMORY 和 LOCAL_DISK 数据不进入以下 L4 消费量。

## 指标口径

默认从 **master 的 `/metrics`** 抓取下表指标，按 `service_endpoint`（SGLang 业务 IP:端口）汇总各客户端。SGLang 已在初始化时自动注册服务地址，复用约 5 秒一次的后台累计快照上报，不增加前台 RPC，不依赖客户端 HTTP 端口或 `MC_STORE_CLIENT_METRIC` 开关。

原客户端 `mooncake_l4_*` 指标仍可用于排障，计数与服务上报相互独立。需要同时更新 master 和 SGLang 使用的 Mooncake 包，本次扩展了服务指标 RPC 快照；不要混用新旧版本报告端。

| 指标 | 标签 | 含义 |
| --- | --- | --- |
| `mooncake_master_service_l4_read_operations_total` | `service_endpoint`, `mode`, `result` | 真正发起的 DFS `BatchGet` 批次数 |
| `mooncake_master_service_l4_read_bytes_total` | `service_endpoint`, `mode`, `result` | 这些批次中成功从 DFS 读取的物理字节 |
| `mooncake_master_service_l4_read_duration_seconds` | `service_endpoint`, `mode`, `result` | 每个 DFS `BatchGet` 的耗时直方图 |
| `mooncake_master_service_l4_restore_ranges_total` | `service_endpoint`, `mode` | 请求成功加载的 DFS range 数量 |
| `mooncake_master_service_l4_restore_bytes_total` | `service_endpoint`, `mode` | 请求成功加载的 DFS range 有效字节 |

读取指标的 `mode=prefetch` 表示等待队列后台预取；`mode=on_demand`
表示请求加载阶段实时回源。`result=success/failure` 表示整个批次的结果，
部分失败批次的成功字节仍计在 `failure` 中。缓冲分配失败且未调用 `BatchGet`
不计读取次数，由 SGLang 的预取任务失败指标反映。

消费指标的 `mode=prefetched` 表示使用等待队列预取缓冲；`mode=on_demand`
表示使用实时回源的数据。只在 scatter 成功完成后累加，失败传输和仅完成
host 预取均不算消费。range 数量按调用中提供的 ranges 计数，不是请求数。

`mooncake_master_service_reporting_clients{service_endpoint}` 是最近 30 秒内有有效上报的客户端数量。首次上报前无系列；断报超过 30 秒后为 0，历史累计 Counter 保留；恢复上报不会重复累计旧数据。

master 按客户端 UUID 与 sequence 去重，只累计快照差值；多个 rank 同服务地址汇总，新 UUID 的重启客户端从自己的累计值开始追加。读取耗时传输固定桶计数与微秒总和，master 合并直方图后仍可查询 P95/P99，单位为秒。

## 核心看板

预取覆盖率，分子与分母都是请求消费的有效字节：

```promql
100 * (sum(rate(mooncake_master_service_l4_restore_bytes_total{mode="prefetched"}[5m])) or vector(0))
/ sum(rate(mooncake_master_service_l4_restore_bytes_total[5m]))
```

两种方式的请求消费带宽：

```promql
sum by (mode) (rate(mooncake_master_service_l4_restore_bytes_total[5m]))
```

预取和实时回源各自的物理读取带宽、P95 耗时：

```promql
sum by (mode) (rate(mooncake_master_service_l4_read_bytes_total[5m]))
```

```promql
histogram_quantile(0.95,
  sum by (le, mode) (rate(mooncake_master_service_l4_read_duration_seconds_bucket[5m])))
```

物理读取可能含对齐、整对象读取和重复加载；不能把消费字节除以预取读取字节
称为命中率或精确利用率。覆盖率无 L4 消费时没有有效值；仅实时回源时为 0%。
多个实例/模型请在所有查询中添加相同的 `service_endpoint` 标签过滤，并选择对应的 master。累计值与窗口速率会受约 5 秒后台上报及 Prometheus 抓取延迟影响，不能当作逐请求实时日志。
关闭等待队列预取后，正常 DFS range 加载只产生 `on_demand`，可用于对比实验。

SGLang 的任务完成、失败、取消、领取和提前就绪时间见其 Mooncake Store README。
领取并不代表 DMA 成功，实际收益以上述成功消费覆盖率为准。

## 抓取与职责

```yaml
scrape_configs:
  - job_name: mooncake-master
    static_configs:
      - targets: ["<master-host>:<master-metrics-port>"]
```

SGLang 的 `sglang:waiting_queue_prefetch_*` 队列事件、任务结果与提前量仍从 SGLang 服务端口抓取，需开启 `--enable-metrics`。业务端指标和 master 字节指标分开展示，不将各 rank 的任务计数与汇总字节相除。

## 底层 positional I/O 延迟（客户端本地）

上面所有指标的最小单位是 **一次 `BatchGet`**。一次 `BatchGet` 在 DFS 后端会展开成若干次
positional I/O（`preadv`/`pwritev`/`pread`），合并窗口上限 4 MiB。要直接观察 syscall
级延迟和每次 I/O 的实际字节数，用客户端本地指标：

| 指标 | 标签 | 含义 |
| --- | --- | --- |
| `mooncake_dfs_posix_read_latency_us` | `path`, `result` | 每次读调用耗时（us） |
| `mooncake_dfs_posix_read_ops_total` | `path`, `result` | 读调用次数 |
| `mooncake_dfs_posix_read_bytes_total` | `path`, `result` | 请求字节数 |
| `mooncake_dfs_posix_write_latency_us` | `path`, `result` | 每次写调用耗时（us） |
| `mooncake_dfs_posix_write_ops_total` | `path`, `result` | 写调用次数 |
| `mooncake_dfs_posix_write_bytes_total` | `path`, `result` | 请求字节数 |

`path` 区分适配器走的代码路径，三者延迟差一个数量级，**不要跨 path 求平均**：

- `direct`：请求满足对齐约束、直接 scatter 进调用方 buffer 的分支。通常走
  O_DIRECT，但文件系统不支持时 `OpenFileDirect` 会退回普通 fd，此时仍记为
  `direct`，所以**不能**据此断言绕过了 page cache。
- `staged`：未满足对齐约束，走对齐 bounce buffer + 拷贝；3FS USRBIO 也归此类
- `buffered`：page cache 读写（shard 模式的 `WriteAt` 恒为此类；bucket 写、以及
  关闭 direct read 时的 bucket 读也归此类）

这些指标**只走客户端本地出口**，不进入 `service_endpoint` 上报，因此 master 的
`/metrics` 里没有它们。用客户端 HTTP `/metrics` 或 Python `get_client_metrics()` 取，
后者不需要开启 HTTP 监听。客户端指标**默认开启**；设 `MC_STORE_CLIENT_METRIC=0`
（或 false）时关闭，此时也不会安装 observer。

单次 I/O 平均延迟（µs）与平均 I/O 大小（字节）：

```promql
rate(mooncake_dfs_posix_read_latency_us_sum{path="direct",result="success"}[5m])
  / rate(mooncake_dfs_posix_read_latency_us_count{path="direct",result="success"}[5m])
```

```promql
rate(mooncake_dfs_posix_read_bytes_total{path="direct",result="success"}[5m])
  / rate(mooncake_dfs_posix_read_ops_total{path="direct",result="success"}[5m])
```

第二个查询只能作为**辅助线索**，不能单独用来判断合并窗口是否生效。原因：

- `batch_read_merge_enabled` **默认关闭**，关闭时或非 bucket 模式下根本不合并，每个
  对象一次调用。而对象本身接近 4 MiB 时，平均值同样接近 4194304。
- 大于 4 MiB 的对象进 `oversized_tasks`，走单对象路径一次读完，会把平均值拉到
  4 MiB **以上**。
- 健康探针 `ProbeRead` 每轮对 `.health1` / `.health2` 各读一次 1 MiB，缓冲区 4 KiB
  对齐，走同一个 adapter 的对齐分支，因此计入 `direct/success`。例如一次合并的 4 MiB
  业务读加一轮探针，`bytes/ops` = 6 MiB / 3 = 2 MiB，会被误判为"合并没生效"。业务读
  少的时候探针几乎是唯一样本。

判断合并是否生效，应结合 `MOONCAKE_DFS_BATCH_READ_MERGE_ENABLED`、对象大小分布，并
按 `path` 和时段区分探针流量。`staged` 上存在非零计数说明有 slice 未对齐，属于可优化
的性能损失（对齐告警只在首次打印一次，容易漏看）。

## `result` 标签的真实含义

`result` 只是 **adapter 这次调用的返回状态**，与 `BatchGet` 是否超时无关：

- 超时只在 BatchGet 层实现：读在 detach 的线程里执行，到截止时间前台就返回
  `DFS_BATCH_GET_TIMEOUT`，后台读**不会被取消**，adapter 的 `preadv` 本身没有超时。
- 因此**卡了很久、最后成功返回**的读会以 `result="success"` 计入，延迟等于整个卡住
  的时长，进入成功均值和成功直方图。`result="failure"` 隔离不了它。
- **一直不返回**的读不产生任何样本：`_ops_total` 不涨，看起来像"既没有 I/O 也没有
  失败"。
- 超时闩锁置位后，新的 BatchGet 在进入 adapter 之前就被拒绝，这段时间的读样本几乎
  只剩健康探针。

结论：文件系统整体卡住、最需要看这组指标的时候，这组数字最不可信。它们统计的是
**已经返回的 adapter 调用**。要观察卡住的 I/O，需要另外的在途计数（gauge），当前
没有。

真正的错误码拆分看 `mooncake_dfs_read_errors_total` /
`mooncake_dfs_write_errors_total`。

## 口径细节

- **短读的判定在各分支不一致**：posix `ReadAt` 和对齐的 `DirectReadAt` 把短读记
  success，`staged` 分支和 3FS 的读写会把不足请求长度视为 `FILE_READ_FAIL` /
  `FILE_WRITE_FAIL`。跨 path 比较 `result` 时要考虑这一点。
- **一次观测不一定对应一次 syscall**：3FS 按 `iov_size` 分块循环提交，buffered 分支
  遇到 EINTR 会重试。
- **`_bytes_total` 记录请求长度，且短读后会重复计入**：backend 对剩余部分会再发一次
  请求，那部分长度会被计入两次。所以 `_bytes_total` 不是实际传输量。
- **提前返回不产生观测**：参数校验失败，以及 3FS `getThreadResource()` 初始化失败，
  都发生在 scope 建立之前。

因为这些指标建立在 adapter 层，它们与 `mooncake_l4_read_operations_total` 之间不
存在固定倍数关系。
