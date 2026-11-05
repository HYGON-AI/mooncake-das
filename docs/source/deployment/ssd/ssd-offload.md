# SSD Offload

## Overview

Mooncake Store supports offloading KV cache objects from distributed memory to a local filesystem path, typically backed by local SSDs. When memory pressure is high, the master instructs clients to persist selected objects to disk. On a cache miss, the client automatically falls back to reading from the local filesystem-backed offload path.

For measured TTFT and throughput impact in multi-turn workloads, see [Mooncake SSD Offload Benchmark](../../performance/mooncake/ssd-offload-benchmark-results.md).

SSD offload requires the **Real Client** and supports two deployment modes:

- **Mode A: Embedded Real Client** — the Python process embeds the Real Client, and SSD offload runs inside the Python process.
- **Mode B: Standalone Real Client + DummyClient** — a standalone `mooncake_client` process runs SSD offload, and the Python process connects via a DummyClient.

In both modes, all SSD reads and writes happen within the Real Client (embedded or standalone).

SSD offload does not use the master's `--root_fs_dir` option. Configure the local disk path on each Real Client with `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH`; the master tracks offloaded objects as `LOCAL_DISK` replicas. `--root_fs_dir` is a legacy parameter from an older persistence path and may cause issues when used with `--enable_offload=true`.

## Startup Steps

### Step 1: Create the SSD storage directory

```bash
mkdir -p /nvme/mooncake_offload
```

### Step 2: Start the master

```bash
mooncake_master \
    --rpc_port=50051
```

### Step 3A (Mode A): Start the application with embedded Real Client

Use the `--enable_ssd_offload` flag to enable SSD offload, and set environment variables to specify the storage path and backend:

```python
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup(
    local_hostname="<machine IP>",
    metadata_server="P2PHANDSHAKE",
    global_segment_size=4 * 1024 * 1024 * 1024,  # 4 GB
    local_buffer_size=512 * 1024 * 1024,  # 512 MB
    protocol="rdma",
    rdma_devices="eth0",
    master_server_addr="127.0.0.1:50051",
    enable_ssd_offload=True,
    ssd_offload_path="/nvme/mooncake_offload"
)
```

### Step 3B (Mode B): Start the standalone real client with SSD offload enabled and connect it with the dummy client

Set the same SSD offload environment variables as in Mode A.

```bash
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/nvme/mooncake_offload
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=bucket_storage_backend

mooncake_client \
    --master_server_address=127.0.0.1:50051 \
    --host=<machine IP> \
    --protocol="rdma"  \
    --device_names=<NIC name, e.g. eth0> \
    --port=50052 \
    --global_segment_size="4 GB" \
    --enable_offload=true  \
    --metadata_server="P2PHANDSHAKE"
```

The application (e.g., SGLang) connects to the standalone real client via the `MooncakeDistributedStore` Python SDK. The Python process acts as a DummyClient; SSD offload and fallback loading are handled by the standalone real client process.

```python
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup_dummy(
    mem_pool_size=4 * 1024 * 1024 * 1024,  # 4 GB
    local_buffer_size=512 * 1024 * 1024,  # 512 MB
    server_address="<machine IP>:50052"   # mooncake_client RPC address from Step 3
)
```

> **Note:** When using `bucket_storage_backend` or `file_per_key_storage_backend`, the real client scans existing SSD metadata on startup and reports it to the master automatically. `offset_allocator_storage_backend` is the exception: it truncates its data file during initialization and does not recover previously offloaded objects after a restart.

---

## Real Client Parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--metadata_server` | `http://127.0.0.1:8080/metadata` | Metadata server connection string |
| `--master_server_address` | `127.0.0.1:50051` | Master address |
| `--host` | `0.0.0.0` | This machine's externally reachable IP. Accepts `ip:port` to specify the data plane port for TransferEngine |
| `--port` | `50052` | Real client RPC listening port |
| `--device_names` | ` ` | NIC name(s), e.g. `eth0` or `mlx5_0` |
| `--protocol` | `tcp` | Transport protocol: `tcp` or `rdma` |
| `--global_segment_size` | `4 GB` | Memory pool size allocated for this node |
| `--enable_offload` | `false` | **Must be set to `true` to enable SSD offload** |
| `--start_offload_rpc_server` | `true` | Start the offload RPC server used by DummyClients for SSD reads. Effective only when `--enable_offload=true`; disable only for write-only owners |
| `--threads` | `1` | Number of RPC server threads |

---

## Master Offload Parameters

These flags control when the master asks clients to persist objects to SSD, whether SSD-only objects can be promoted back to DRAM, and how much offload work can be queued during eviction.

| Flag | Default | Description |
|------|---------|-------------|
| `--enable_offload` | `false` | Enables the master-side SSD offload control plane. Set this on the master and on every real client that owns SSD storage |
| `--offload_on_evict` | `false` | Defer SSD persistence until memory eviction selects an object. When `false`, successful `Put` completion queues eager SSD persistence |
| `--offload_force_evict` | `false` | If offload-on-evict exceeds the per-cycle offload cap, force-evict excess objects instead of leaving them in DRAM |
| `--offloading_queue_limit` | `50000` | Maximum pending offload objects per local disk segment. Must be greater than `0` and at most `100000000` |
| `--offload_cap_ratio` | `0.5` | Per-eviction-cycle cap as a fraction of `offloading_queue_limit`; range `[0.0, 1.0]`. Default cap is `25000` objects per cycle |
| `--promotion_on_hit` | `false` | Promote SSD-resident objects back to DRAM after read hits |
| `--promotion_admission_threshold` | `2` | Minimum CountMinSketch count before promotion is admitted. Set `1` to disable second-touch gating |
| `--promotion_max_per_heartbeat` | `1` | Maximum promotion tasks returned to one client per heartbeat. Keep conservative for large objects because each task does SSD read plus memory write |
| `--promotion_queue_limit` | `50000` | Maximum in-flight promotion tasks tracked by the master |

Start with `--enable_offload=true` for eager SSD persistence. Add `--offload_on_evict=true` when SSD writes should happen only under memory pressure. For SSD-heavy workloads where offload-on-evict is dropping too many objects, raise both `--offloading_queue_limit` and `--offload_cap_ratio`; for example, `--offloading_queue_limit=500000 --offload_cap_ratio=0.8` allows up to `400000` objects to be queued in one eviction cycle.

---

## SSD Offload Configuration

### Core settings

| Environment Variable | Default | Description |
|---|---|---|
| `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` | `/data/file_storage` | Absolute path to the SSD storage directory |
| `MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR` | `bucket_storage_backend` | Storage backend type (see below) |
| `MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES` | `1342177280` (1.25 GB) | Client-side staging buffer size |
| `MC_STORE_DFS_UNIFIED_BUFFER_SIZE_BYTES` | `8589934592` (8 GiB) | Shared pinned unified buffer for DFS read, prefetch, and write staging |
| `MC_STORE_DFS_READ_CHUNK_SIZE_BYTES` | `1073741824` (1 GiB) | DFS read chunk size; values `<= 0` use the default |
| `MC_STORE_DFS_WRITE_CHUNK_SIZE_BYTES` | `1073741824` (1 GiB) | DFS write chunk size; values `<= 0` use the default |
| `MC_STORE_DFS_PREFETCH_CAPACITY_RATIO` | `0.5` | Maximum fraction of the unified buffer retained by prefetch allocations; truncated to what remains after one read chunk and one write chunk |
| `MC_STORE_DFS_WRITE_ARENA_WAIT_MS` | `10000` | Maximum time DFS write staging waits for unified-buffer space |
| `MOONCAKE_OFFLOAD_SCANMETA_ITERATOR_KEYS_LIMIT` | `20000` | Max keys processed per iteration when scanning existing SSD metadata on startup |
| `MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES` | `2199023255552` (2 TB) | Maximum disk usage |
| `MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT` | `10000000` | Maximum number of objects on disk |
| `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` | `10` | Interval for offload heartbeat to master (seconds) |
| `MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_INTERVAL_SECONDS` | `1` | Interval for reclaiming expired offload buffers; defaults to the heartbeat interval in the current implementation |
| `MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_TTL_MS` | `5000` | Lease time for buffers returned by `batch_get_offload_object` before GC reclaims them |
| `MOONCAKE_OFFLOAD_USE_URING` | `false` | Enable io_uring for async file I/O |
| `MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION` | `true` | Enable proactive local-disk eviction from the FileStorage heartbeat |
| `MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO` | `0.90` | Trigger proactive disk eviction when backend usage exceeds this ratio of its quota |
| `MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO` | `0.80` | Target backend usage ratio for proactive disk eviction |

The `MOONCAKE_OFFLOAD_*` watermark names are preferred. Short aliases `MOONCAKE_DISK_EVICTION_HIGH_WATERMARK_RATIO` and `MOONCAKE_DISK_EVICTION_LOW_WATERMARK_RATIO` are also accepted. The high watermark must be greater than the low watermark.

The unified DFS buffer is created when DFS is enabled and its configured size is positive. It is shared by DFS reads, prefetch, and writes; io_uring and local memcpy settings do not disable its creation. On HYGON, a mapped pinned allocation is preferred and falls back to ordinary pinned host memory if the device-visible alias cannot be created. If the configured unified buffer cannot be allocated, client setup fails with `DFS_SERVICE_UNAVAILABLE` instead of degrading at request time.

On a machine without pinned memory the only way to start is `MC_STORE_DFS_UNIFIED_BUFFER_SIZE_BYTES=0`. That disables the unified buffer entirely, and DFS writes lose their staging space: the asynchronous bucket path fails every chunk with `allocator_unavailable` and keeps only the memory replicas, a synchronous write of an object containing GPU slices returns `NO_AVAILABLE_HANDLE` for the DFS replica, and only a synchronous write of a pure host object can still reach DFS. A warning is logged at setup when DFS is enabled with a zero-sized buffer, because that configuration silently degrades DFS writes to memory-only.

#### Unified buffer admission

The unified buffer is not physically partitioned; three byte budgets are enforced on top of the single pool:

| Budget | Value | Effect |
|---|---|---|
| Read protection | One read chunk after alignment, capped at half the buffer | Non-read usage must always leave this many bytes free, whether or not a read is in flight |
| Write protection | One write chunk after alignment, capped by the space left after the read protection | Carved out of the prefetch budget, so admission always reserves room for one write chunk when prefetch is admitted |
| Prefetch limit | `MC_STORE_DFS_PREFETCH_CAPACITY_RATIO × buffer size`, truncated to `buffer − read protection − write protection` | Prefetch never retains more than this |

Both protections are resolved once at startup. The prefetch limit is `min(ratio × buffer, buffer − read protection − write protection)`; when the ratio asks for more, the limit is silently truncated and a warning records both values. If the two protections consume the whole buffer, the prefetch limit becomes `0`, prefetch is disabled, and a warning says so — raise `MC_STORE_DFS_UNIFIED_BUFFER_SIZE_BYTES` or reduce the chunk sizes. Write protection is a configuration-time reservation only: writes wait for space, bounded by `MC_STORE_DFS_WRITE_ARENA_WAIT_MS` (default 10 s), rather than failing immediately, so a write proceeds as soon as reads release memory. Per-request admission therefore does not re-check the write protection.

Read protection is a byte-level floor, not a guarantee of a contiguous free region. The effective read protection is `max(read chunk, in-flight read bytes)`: with no reads in flight it is exactly one read chunk, and it grows when concurrent reads exceed one chunk so that they are not crowded out by prefetch or writes. The floor is always held, even when no read is in flight: prefetch and writes together may use at most `buffer − read protection`, and the remainder stays reserved for reads that arrive later. This is intentional — reads never wait, so the space must already be there when a read starts. A single read request is never rejected by these budgets; only non-read requests can be refused at admission time.

If `MC_STORE_DFS_READ_CHUNK_SIZE_BYTES` after alignment exceeds half the buffer, the read protection is clamped to half the buffer and a warning is logged. In that configuration writes and prefetch can fill the remaining half, which may leave too little contiguous space for a full read chunk. Set the read chunk to at most half the unified buffer, or size the buffer to at least twice the read chunk, to avoid this.

Note that the aligned allocation size of a chunk is larger than the chunk size itself: a request is padded by up to `MOONCAKE_DFS_ALIGNMENT - 1` bytes and then rounded up to the allocator bin size, so a 4 KiB read chunk with 4 KiB alignment reserves 8 KiB. Size the buffer against the aligned sizes, not the raw chunk sizes.

A single DFS write staging request is limited to `buffer − read protection`. The asynchronous bucket path splits payloads by `MC_STORE_DFS_WRITE_CHUNK_SIZE_BYTES`, so it stays within that bound as long as the write chunk fits.

The synchronous shard path behaves differently depending on where the object lives. A **host-only** object is written straight from the caller's slices and never consumes unified buffer space, so its size is not bounded by the buffer. An object that contains **device (GPU) slices** must be staged, because each GPU slice is copied to host anyway: the whole object is staged in one request so every slice shares one allocation for the lifetime of the write, and such an object must therefore fit within `buffer − read protection`. A device object larger than that bound is refused with `NO_AVAILABLE_HANDLE` for the DFS replica while its memory replicas still succeed. Objects written through `put_from` from ordinary host memory are not affected.

Because the protections only reserve bytes, fragmentation can still make a large aligned allocation fail even though the admission counters allow it. While the unified buffer is active, a failed DFS staging allocation returns an allocation error rather than bypassing the unified budget through the ordinary client buffer. When the unified buffer is explicitly disabled, DFS reads may use the ordinary client buffer if one is available.

The merged implementation uses this L4 unified buffer instead of the separate community `MC_STORE_PINNED_RESTORE_ARENA_SIZE_BYTES` quota; that older setting is not read. Same-process SSD restores may also prefer the pinned arena when it exists, with ordinary FileStorage-buffer fallback on exhaustion. Requested payload windows can be read directly into supplied buffers; file-per-key storage may still allocate its own temporary buffer internally. These local SSD paths do not change the DFS admission rules above.

### Bucket backend settings

Applies when `MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=bucket_storage_backend`.

| Environment Variable | Default | Description |
|---|---|---|
| `MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES` | `268435456` (256 MB) | Max size per bucket |
| `MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT` | `500` | Max keys per bucket |
| `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE` | `0` | Eviction threshold in bytes. When set to `0`, the backend uses **90% of the physical disk capacity** as the quota — it does not mean unlimited. Set an explicit value to control disk usage precisely. |
| `MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY` | `fifo` | Eviction policy: `none` / `fifo` / `lru` |
| `MOONCAKE_OFFLOAD_BUCKET_MAX_PHYSICAL_BYTES` | `0` (disabled) | Hard cap on **real on-disk** bytes (`du`-equivalent) under this backend's `ssd_offload_path`. `0` disables it. With per-rank directories (required), the cap is per-rank — see below. |
| `MOONCAKE_OFFLOAD_BUCKET_DISK_SCAN_CACHE_MS` | `500` | How long the directory-scan result is cached before re-scanning, to bound the cost of the physical-usage check. `<= 0` scans on every check. |

### File-per-key backend settings

Applies when `MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=file_per_key_storage_backend`.

| Environment Variable | Default | Description |
|---|---|---|
| `MOONCAKE_OFFLOAD_FSDIR` | `file_per_key_dir` | Subdirectory name created under `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` |
| `MOONCAKE_OFFLOAD_ENABLE_EVICTION` | `true` | Enables local-storage eviction logic for this backend. Short alias: `ENABLE_EVICTION` |

---

## Storage Backends

### `bucket_storage_backend` (recommended)

Groups multiple objects into bucket files. Reduces filesystem overhead, supports efficient batch I/O, and supports FIFO and LRU eviction.

**File layout:**
```
/nvme/mooncake_offload/
├── 1710000000000-0.bucket   # data file (multiple KV pairs)
├── 1710000000000-0.meta     # metadata file
├── 1710000000001-0.bucket
└── ...
```

Best for: general-purpose use, large-scale deployments.

### `file_per_key_storage_backend`

Stores each object in an individual file. Simple and easy to inspect, but generates many small files at scale.

| Environment Variable | Default | Description |
|---|---|---|
| `MOONCAKE_OFFLOAD_FSDIR` | `file_per_key_dir` | Subdirectory name under `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` where objects are stored |
| `MOONCAKE_OFFLOAD_ENABLE_EVICTION` | `true` | Enable disk eviction for this backend. Short alias: `ENABLE_EVICTION` |

Best for: debugging or small-scale deployments.

### `offset_allocator_storage_backend`

Pre-allocates a single large file and manages offset-based allocation within it. Highest concurrency via 1024-shard metadata.

> **Warning:** This backend does **not** support metadata recovery on restart. On initialization, the data file is truncated and all in-memory metadata is cleared. Any previously offloaded objects become inaccessible after a process restart.

**Capacity:** `MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES` is used directly as the pre-allocated file size (100%, no safety margin). Unlike `bucket_storage_backend`, there is no separate quota variable — this is the sole disk usage control. Set it below the physical disk capacity to avoid filling the disk; writes are rejected once usage reaches this limit.

Best for: high-concurrency scenarios with many small objects where restart durability is not required.

---

## Eviction

### Write-time eviction

When `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE` or `MOONCAKE_OFFLOAD_BUCKET_MAX_PHYSICAL_BYTES` is set, the backend automatically evicts buckets before writing new ones if total disk usage would exceed the limit.

| Policy | Behavior |
|--------|----------|
| `none` | No eviction (default); writes fail when disk is full |
| `fifo` | Evict the oldest bucket first |
| `lru` | Evict the least recently read bucket first |

Eviction is two-phase: the bucket is removed from metadata and master is notified first, then in-flight reads are drained before files are deleted.

**`MAX_TOTAL_SIZE` vs `MAX_PHYSICAL_BYTES`**

`MAX_TOTAL_SIZE` bounds a *logical* in-memory counter (`data_size + meta_size` summed per object). It undercounts real disk usage: it ignores filesystem block rounding, and a bucket file stays on disk until *every* object in it is evicted while the counter drops per object. `MAX_PHYSICAL_BYTES` instead measures the *real* on-disk bytes (`du`-equivalent), so prefer it when you must not exceed a hard physical limit — most importantly a Kubernetes `emptyDir` with a `sizeLimit`, which the kubelet enforces by the volume's actual `du` usage and evicts the pod when exceeded.

**Ownership and scope.** LOCAL_DISK allows **one live client per** `ssd_offload_path` / `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH`. A second live client fails `Init()` with `storage_path already held by another live client`. Do **not** point several TP ranks at the same path — use **per-rank / per-client directories** (or separate disks). See the ownership contract in [SSD Offload design](../../design/store/ssd-offload.md).

`MAX_PHYSICAL_BYTES` scans this backend's own path. With the required per-rank layout, the cap bounds **each rank individually**. Set it to the per-rank budget (e.g. `sizeLimit / N` when N ranks share one volume via separate subdirectories, or the disk capacity when each rank has its own disk). A shared offload directory across live clients is no longer a supported deployment shape.

> A cap on the *global* sum across ranks that holds under any layout (including ranks on separate disks) cannot be done by a per-directory scan and would require master-side aggregation; it is not part of this feature.

The two limits are independent and may be combined — eviction fires when either would be exceeded. `MAX_PHYSICAL_BYTES` only takes effect when an eviction policy is set (`fifo`/`lru`); with `none`, eviction never runs.

---

### Proactive watermark eviction

When `MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION=true`, the FileStorage heartbeat asks the backend to check local-disk usage every `MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS` seconds. If usage exceeds `MOONCAKE_OFFLOAD_DISK_EVICTION_HIGH_WATERMARK_RATIO`, the backend evicts toward `MOONCAKE_OFFLOAD_DISK_EVICTION_LOW_WATERMARK_RATIO`.

| Backend | Behavior |
|---------|----------|
| `bucket_storage_backend` | Reuses `MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY`; set it to `fifo` or `lru` because the default policy `none` makes watermark eviction a no-op |
| `file_per_key_storage_backend` | Requires `MOONCAKE_OFFLOAD_ENABLE_EVICTION=true`; reuses the FIFO file eviction queue and recovered keys from startup metadata scan |
| `offset_allocator_storage_backend` | No-op in this version |

The watermark ratios apply to each backend's quota. For `bucket_storage_backend`, the quota is `MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE`; when that value is `0`, the backend defaults it to 90% of the physical disk capacity. For `file_per_key_storage_backend`, the underlying file backend uses its storage quota, which defaults to 90% of the physical disk capacity in SSD offload mode.

For watermark eviction, the real client notifies the master before deleting local files. If the notification fails, the selected files remain tracked locally and are retried by a later heartbeat.

---

## Example

The following example starts a master and a real client on a single machine.

### Environment

- Machine IP: `192.168.1.10`
- NIC: `eth0`
- SSD mount point: `/nvme`
- Memory pool size: 4 GB (smaller than the total data written, to trigger offload)

### Start the master

```bash
mooncake_master \
    --rpc_port=50051 \
    --enable_offload=true \
    --offload_on_evict=true \
    --promotion_on_hit=true \
    --promotion_admission_threshold=2
```

### Start the real client (new terminal)

```bash
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/nvme/mooncake_offload
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=bucket_storage_backend
export MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE=$((200 * 1024 * 1024 * 1024))  # 200 GB
export MOONCAKE_OFFLOAD_BUCKET_MAX_PHYSICAL_BYTES=$((200 * 1024 * 1024 * 1024)) # optional; per-rank dir: per-rank cap
export MOONCAKE_OFFLOAD_BUCKET_DISK_SCAN_CACHE_MS=500 # optional
export MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY=lru

mooncake_client \
    --master_server_address="192.168.1.10:50051" \
    --host="192.168.1.10" \
    --device_names="eth0" \
    --port=50052 \
    --protocol="rdma" \
    --global_segment_size="4GB" \
    --enable_offload="true"
```

---

## Notes

- `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` must be an absolute path to an existing, writable directory. Symbolic links and paths containing `..` are rejected.
- Each live Real Client needs its **own** offload directory. Sharing one path across TP ranks fails at `Init()` under the LOCAL_DISK ownership lock.
- On real client restart, `bucket_storage_backend` and `file_per_key_storage_backend` scan existing SSD metadata and report it to the master, so previously offloaded objects remain accessible. `offset_allocator_storage_backend` does not support restart recovery.
- Eviction only notifies the master and deletes local files; objects replicated on other nodes are unaffected.
- Each machine requires its own real client process. In multi-node deployments, ensure `--host` and `--port` are correctly set so nodes can reach each other.

**2-node example:** suppose Node A (`192.168.1.10`) runs the master and Node B (`192.168.1.11`) is a second worker. Both real clients must point to the same master and advertise their own externally reachable IP:

```bash
# Node A — runs the master and its own real client
mooncake_master --rpc_port=50051 --enable_offload=true &

export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/nvme/mooncake_offload
mooncake_client \
    --master_server_address="192.168.1.10:50051" \
    --host="192.168.1.10" \        # externally reachable IP of Node A
    --device_names="eth0" \
    --protocol="rdma" \
    --metadata_server="P2PHANDSHAKE" \
    --port=50052 \
    --global_segment_size="4GB" \
    --enable_offload="true"
```

```bash
# Node B — real client only; points to the same master on Node A
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/nvme/mooncake_offload
mooncake_client \
    --master_server_address="192.168.1.10:50051" \
    --host="192.168.1.11" \        # externally reachable IP of Node B, NOT 127.0.0.1
    --device_names="eth0" \
    --protocol="rdma" \
    --metadata_server="P2PHANDSHAKE" \
    --port=50052 \
    --global_segment_size="4GB" \
    --enable_offload="true"
```

---

## Troubleshooting

### SSD offload is not triggering

- Confirm `--enable_offload=true` is passed to both `mooncake_client` and `mooncake_master`. Both binaries define the same gflags boolean; hyphenated aliases may work, but the underscored spelling matches the source.
- Check that `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` points to an existing, writable directory. If the path is invalid, real client setup fails during `FileStorageConfig::Validate()`.
- Verify memory pressure is actually high enough for the master to trigger offload. If the memory pool (`--global_segment_size`) is large relative to the data written, offload may never activate.

### "Permission denied" or "No such file or directory" on the storage path

- Ensure the directory exists before starting the client: `mkdir -p <path>`.
- Confirm the process user has read/write access to the directory.
- Symbolic links and paths containing `..` are rejected — use an absolute, canonical path.
- The path must already exist and must be a directory. Passing a regular file path also causes setup to fail.

### "Failed to register buffer with UringFile" warning in logs

This warning appears when `MOONCAKE_OFFLOAD_USE_URING=true` and the io_uring fixed-buffer registration fails. The most common cause is that `MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES` exceeds the process's locked-memory limit (`RLIMIT_MEMLOCK`). io_uring requires the registered buffer to be pinned in physical memory, which counts against this limit.

Check the current limit:

```bash
ulimit -l        # in KB; "unlimited" means no cap
```

To raise it for the current session:

```bash
ulimit -l unlimited
```

To raise it permanently, add the following to `/etc/security/limits.conf`:

```
* soft memlock unlimited
* hard memlock unlimited
```

Alternatively, reduce `MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES` to a value within the existing limit. Note that the warning does not abort startup — the client falls back to non-fixed-buffer I/O — but performance may be lower than expected.
