---
name: mooncake-keyscale
description: Test Mooncake key-capacity limits and eviction stability with any multi-node / multi-segment layout. One conf file describes the roles (master, a pure-client writer, and N holders); every holder contributes one segment and can carry its own segment size, so the reachable resident-key cap is whatever the user's nodes and memory allow. The driver pushes resident keys to the per-segment object cap, then watches N eviction rounds and renders a report. The bundled 6-segment conf is only a reference sized to the memory limit of one environment (64GiB x 6 = 402,653,184 keys); any node count and segment count/size can be configured. Use this skill when the user wants to measure the key-capacity ceiling, verify master_active_clients against the segment count, check master RSS per key, confirm that eviction does not make BatchPutStart exceed 30s, or check that every node has the same Mooncake package installed. Trigger on phrases like "key 规模测试", "key 容量上限", "驱逐稳定性测试", "段上限测试", "多节点多段", "run key scale test", "eviction stability test", "BatchPutStart 超时复现", and "各节点版本是否一致".
---

> **Configure your own conf first.**
> Copy `conf/nodes.6seg.conf` and adapt it to your environment (nodes, container name, client IPs,
> devices, segment sizes), then self-check with `python3 driver/ks_control.py check <your conf>`
> and only then `run`. Column meanings, the device-to-IP check and the capacity math are all
> documented at the top of the conf files.

# Mooncake key-capacity and eviction-stability test

`python3 driver/ks_control.py` is the only entry point. Every run leaves a `report.md` in its run
directory as the record of that run.

The skill measures two things: **how many resident keys a layout can hold** (the per-segment
object cap) and **whether repeated eviction rounds stay stable after fill-up** (does
`BatchPutStart` exceed 30 seconds, do clients drop out).

**Node count and segment count are entirely conf-driven**, nothing is hard-coded:

- one `master` row, one `writer` row, and one `holderN` row per segment;
- a single machine may carry several holder rows with different client IPs, up to its HCA count;
- each holder can set its own segment size; mixed sizes are supported.

So segments = holder rows: 1, 4, 6 or more, depending on how many machines, HCAs and memory you
have. `conf/nodes.6seg.conf` (6 x 64GiB = 402,653,184) is **only a reference configuration sized
to the master memory limit of this environment**, not a requirement; with less memory shrink the
segment size or drop segments, and `plan` will tell you whether it fits.

The writer is a pure client: it mounts no segment and provides no memory, usually on the master
node, so `master_active_clients` should equal the number of holder rows (the writer does not
count).

## User prompts

When the user says any of the following, run this skill:

- 跑一下 key 规模测试
- 跑一下 6 段的 key 规模测试
- 测一下这套配置能放多少 key
- 驱逐稳定性测试 / 看驱逐是否稳定
- 各节点 Mooncake 版本是否一致

English equivalents: "run key scale test", "key capacity limit", "eviction stability test",
"segment cap test", "multi-node multi-segment", "are all node versions identical".

## What this skill does and does not do

Does: capacity accounting, per-node self-checks, starting services, waiting for fill-up, watching
eviction rounds, collecting logs, rendering the report, stopping cleanly.

**Does not**: fetch repositories, build wheels, unpack packages, or distribute them. Those are
**external prerequisites** handled by the user or another pipeline. The skill only verifies that
the environment is ready; it does not make it ready.

## Prerequisites

All of these must hold, otherwise `check` / `preflight` fails immediately:

| Requirement | How to confirm |
| --- | --- |
| The package is installed in each node's container | `python3 -c "import mooncake"` works and the package ships `mooncake_master` (or set `MASTER_BIN`) |
| Python can import mooncake | `python3 -c "from mooncake.store import MooncakeDistributedStore"` succeeds |
| All nodes run the same version | `ks_control.py check <conf>`: **it compares the package installed in site-packages** (python version, mooncake package version and the master git hash must all match) |
| One distinct client IP per segment | the `hca` column of `ks_control.py check <conf>`; at least one usable HCA per node. With `devices` omitted the engine uses every usable HCA, with explicit devices the segment count per node must not exceed the card count |
| Containers can reach the master | host networking, or at least the master RPC port is reachable |
| Each segment process can bind its own IP | containers share the host network namespace; with bridge networking segments cannot be distinguished |

Do not run when `check` fails, otherwise problems surface later in hard-to-debug ways during
start-up.

**By default the package installed in the environment is used** (`RUNTIME_DIR=-`): whichever
package `check` inspects is the one `run` imports. Only an explicit `RUNTIME_DIR=<dir>` prefers a
staged tree, and then `check` additionally requires the staged version to match site-packages,
otherwise it reports `staged(...)!=installed(...)`.

## Where to run it

`ks_control.py` is the **driver**; it does not run the test processes itself. Every role executes
inside a container through `docker exec`.

The script can live anywhere as long as that shell can ssh to the conf's nodes. Use `--driver` to
state where it is running:

| `--driver` | Meaning | When |
| --- | --- | --- |
| `master` (default) | must run on the **host of the master node**, otherwise it errors out and prints the correct ssh command | standard use |
| `host` | must run on any node of the conf | flexible debugging |
| `auto` | anywhere; uses local `docker exec` when the current host is a conf node | legacy compatibility |
| `local` | anywhere; always goes through ssh | running from a laptop |

Not sure where to run it? Ask the script:

```bash
python3 driver/ks_control.py where 6seg
```

It prints the role each node plays plus ready-to-copy `ssh <master> "python3 ... run ..."`
commands.

```bash
# on the host (auto is fine)
python3 driver/ks_control.py run 6seg 8

# from a laptop
python3 driver/ks_control.py run 6seg 8 --driver local
```

Nothing is written on the host: the script only does `ssh` plus `docker exec`, all reads and
writes happen inside the containers.

The script pushes the conf, `run.env` and the runtime scripts into `/tmp/ks/<run_id>/` on every
container; each role runs locally there and writes its logs; at the end the logs are streamed back
into the master container and aggregated. **No shared filesystem between nodes is required.**

## How an agent uses it (standard flow)

The user only has to say "run mooncake-keyscale".

1. **Confirm the environment is ready**: containers up, package built and installed. The skill
   only checks this.
2. **Decide where to run**: `ks_control.py where <conf>` prints the master host and full commands.
3. **Run on the master host**:
   ```bash
   ssh <master_host> "python3 <skill>/driver/ks_control.py check <conf>"
   ssh <master_host> "python3 <skill>/driver/ks_control.py plan <conf>"
   ssh <master_host> "python3 <skill>/driver/ks_control.py run <conf> 8"
   ```
4. **Collect the result**: the command ends with `artifacts: .../report.md`. Report the path and
   the key observations from the report (fill-up time and key count, eviction rounds, `-901` /
   timeout counts and the phase they happened in, master RSS).
5. When the environment is not ready, `check` says exactly what is missing (package not installed,
   versions differ, no RDMA card) -- do **not** force a run.

The master host must be able to read the skill directory (shared filesystem, or copy the skill
there first).

## Configuration

### conf/nodes.Nseg.conf

Four to six whitespace-separated columns, `#` starts a comment; the last two columns are optional:

```text
role  ssh_host  container  local_hostname  [devices]  [segment_size]
```

- `role`: `master` (one row), `writer` (one row, a pure client with no segment), `holderN` (one
  segment each)
- `ssh_host`: the name the driver uses to ssh to that node
- `container`: docker container name for that role (**required**, there is no default)
- `local_hostname`: client IP of that segment, **must be unique per segment**
- `devices` (optional): NIC names exactly as `ibv_devices` lists them (e.g. `mlx5_0` on Mellanox,
  `shca_0` on Hygon); the `mlx5_0,mlx5_1,...` values in the templates are **placeholders**. One card
  per segment = single rail per segment, easier to debug; several cards (comma separated) = multi-rail,
  more bandwidth but MR/QP count grows with the number of cards; `-` or omitting the column lets the
  engine auto-discover every usable HCA
- `segment_size` (optional): size of that segment, e.g. `64GiB`; when omitted the global
  `SEGMENT_SIZE` applies. Precedence: `SEGMENT_SIZE_<ROLE>` env var > conf column 5/6 >
  `SEGMENT_SIZE`. `plan` and the report sum the real per-segment sizes, mixed sizes are supported.

With five columns the 5th is classified by content: something that looks like a size (`64GiB`,
`1GB`) is `segment_size`, anything else is `devices`.

Segment count is derived from the conf: **number of holder rows** (the writer is not a segment).
The first command-line argument is the conf file; you never pass a segment count.

The same `ssh_host` on several rows means that node runs several segments, each with its own
`local_hostname`. Without `devices` those segments share all usable HCAs on the node (multi-rail);
to pin a segment to one card, list that card explicitly (the name `ibv_devices` shows, e.g.
`mlx5_0`), in which case segments per node cannot exceed the card count.

### Parameters (environment variables)

```bash
SEGMENT_SIZE=40GiB     # segment size: drives the resident cap and the memory need
VALUE_SIZE=64          # small values hit the object cap before the byte watermark
BATCH_SIZE=12288
WORKERS=128
EVICT_ROUNDS=8         # default number of eviction rounds `watch`/`run` waits for;
                       # `run <conf> N` overrides it and records N in run.env, and the
                       # report clips its window to that same N
SAMPLE_INTERVAL=15     # sampler interval in seconds
EVICTION_RATIO=0.1          # fraction deleted per round
EVICTION_HIGH_WATERMARK=0.9 # byte watermark; with 64B values the object cap is hit first
ALLOCATION_STRATEGY=random  # random | free_ratio_first | local_first ...
FILL_TIMEOUT=7200      # upper bound for waiting until the cap is reached
ROUND_TIMEOUT=1800     # upper bound for one eviction round
MASTER_PORT=50551      # the three ports deliberately avoid production
METADATA_PORT=38080
METRICS_PORT=39003
RUNTIME_DIR=-                # default: use the package installed in site-packages
                             # an explicit dir prefers that staged tree (must equal the installed one)
MASTER_BIN=<path>            # optional override for the master binary

RUN_ID=<id>                  # reuse an existing run directory
CONF_OVERRIDE=<path in container>  # temporarily swap in another node list
```

## Quick start

```bash
KS=<skill dir>/driver/ks_control.py

python3 $KS check 6seg        # is the installed package ready and identical on all nodes (starts nothing)
python3 $KS plan 6seg         # capacity accounting, does the hardware fit
python3 $KS preflight 6seg    # per-node self-check
python3 $KS run 6seg 8        # start, fill up, watch 8 eviction rounds, report, stop
```

Run only one configuration at a time (they share the master port).

After editing a conf, run `smoke 6seg` first (1GiB segments, a few minutes) to verify the wiring,
then run the real size.

For long runs detach, and keep the driver log inside the container rather than on the host:

```bash
nohup setsid bash -c "python3 $KS run 6seg 8 2>&1 | docker exec -i <container> tee $BASE/keyscale-runs/run6-driver.log > /dev/null" > /dev/null 2>&1 &
```

## Subcommands

| Command | Purpose |
| --- | --- |
| `check <conf>` | is the package installed in site-packages on each node, do python/mooncake/master versions match; starts nothing |
| `plan <conf>` | per-segment key counts, resident cap, master RSS, reconciled against free memory on every node |
| `preflight <conf>` | per-node self-check: package and versions, RDMA devices, ports, memory, local roles |
| `start <conf>` | master -> sampler -> holders -> writer, fixed order |
| `watch <conf> [rounds]` | wait for fill-up, then wait for N `[EVICT-DONE]` events; N is recorded in run.env so a later `report` clips to it |
| `status <conf>` | current metrics, eviction lines, writer timeout counts, memory per node |
| `report <conf> [run_id]` | render `report.md` (English) and `report.zh.md` (Chinese); without `run_id` the latest run is used |
| `stop <conf>` | stop writer -> holders -> master, and the sampler |
| `run <conf> [rounds]` | `start` + `watch` + `report` + `stop`; `rounds` defaults to `EVICT_ROUNDS` (8) and is what the report clips to |
| `smoke <conf>` | 1GiB wiring check, a few minutes |

If you want to confirm every step manually use only the atomic subcommands
(`start` -> `status` -> `watch 1` -> ...) instead of `run`.

## How the test works

The resident keys per segment are `min(segment bytes / 1024, 64Mi)`:

| Segment size | Keys per segment | 6 segments (reference config) |
| --- | --- | --- |
| 64GiB | 67,108,864 | **402,653,184** |
| 40GiB | 41,943,040 | 251,658,240 |
| 32GiB | 33,554,432 | 201,326,592 |
| 1GiB (smoke) | 1,048,576 | 6,291,456 |

Values are 64B, so **the object cap is hit before the 0.9 byte watermark**. At fill-up the master
log shows `[EVICT-TRIGGER] memory_ratio=0.0625 high_watermark=0.9 need_mem_eviction=1`.

Measured master RSS is about **1.2KiB/key** (1.12KiB is used for estimates); that is the real
constraint when choosing segment sizes: e.g. the reference 6 x 64GiB needs roughly 420GiB of free
memory on the master node, which is why that conf is sized that way. `plan` reconciles against each
node's `MemAvailable` and marks `TOO TIGHT` when it does not fit; then reduce the segment count or
segment size.

The failure chain to reproduce:

```text
BatchEvict / offboarding holds master_state_mutex_ for a long time
  -> the first Ping of a freshly mounted client returns NEED_REMOUNT
  -> the heartbeat thread sends ReMountSegment holding client_mutex_ exclusively
  -> every client's Ping blocks, the coro_rpc I/O threads stall
  -> BatchPutStart exceeds 30s, -901 / Connection timed out appear
```

## What to watch

| Metric | Target | What a deviation means |
| --- | --- | --- |
| `check` consistency | same python/mooncake/master-git on every node | packages differ, master and client may speak different protocols |
| `master_active_clients` | equals the holder count (writer mounts nothing) and never drops | a segment failed to mount or dropped; a common cause is two segments sharing one IP |
| `master_key_count` peak | at least 90% of the cap | did not fill up, or segments are smaller than expected |
| master RSS per key | about 1.2KiB | larger means look into namespace / ObjectRoute overhead |
| `[EVICT-DONE]` rounds | at least the requested number | eviction did not finish, possibly stuck on a lock |
| `-200` in `result_codes` | expected after fill-up | segment object cap reached, the writer is supposed to see it |
| `-901` in `result_codes` | 0 | `BatchPutStart` exceeded 30s; the report must say whether it happened before fill-up or during eviction |
| `Connection timed out` | 0 | I/O threads blocked by eviction / offboarding cleanup |

**The report only describes observed behavior -- no scoring, no PASS/FAIL/INCOMPLETE.** It must
state: when the keys filled up and how many, per-round eviction duration and deletions, the
`-901` / `Connection timed out` counts and the phase (before fill-up / during eviction), whether
`master_active_clients` stayed up, peak master RSS and the node's minimum available memory.

## Artifacts

```text
$BASE/keyscale-runs/<run_id>/
  report.md              <- archived English report of every run
  report.zh.md           <- archived Chinese report of every run
  run.env                 all parameters of this run (the report trusts this, not CLI defaults)
  nodes.conf              node list used by this run
  nseg
  <master_node>/
    master.log            [EVICT-TRIGGER] / [EVICT-DONE]
    sampler.csv           key_count / clients / master RSS / memory timeline
    sampler.log
    master.pid
    holder-*.log
  <writer_node>/
    loader.log            progress and result_codes
    holder-*.log
```

`run_id` looks like `20261118-104110-6seg`.

## Troubleshooting

- **`check` reports `consistency: MISMATCH`**: compare column by column. It compares the package
  **installed in site-packages**; versions must match before running. Install the same wheel on
  that container with `pip install --force-reinstall --no-deps <same wheel>`.
- **`check` reports `PREREQ MISSING`**: package not installed, not importable, no `mooncake_master`,
  or no RDMA card. This skill does not build or distribute packages; install it in the container
  first, or point `MASTER_BIN` at a binary.
- **`check` reports `staged(...)!=installed(...)`**: `RUNTIME_DIR` is set explicitly and that staged
  tree is not the installed package. Either drop `RUNTIME_DIR` (back to the installed package) or
  make both sides the same version.
- **`plan` says `TOO TIGHT`**: reduce `SEGMENT_SIZE` or move the master to a node with more memory.
  Do not rely on swap; a swapping master makes the whole test meaningless.
- **`plan` / `preflight` says `UNREACHABLE`**: first test `ssh <host> docker exec <container> true`
  manually. A common cause is that no job is running on that node and ssh is refused by
  `pam_slurm_adopt`; use another node.
- **`active_clients` below the holder count**: two segments share one `local_hostname`; every
  segment needs its own IP.
- **`preflight` shows a tiny `ulimit -l`**: 1GiB segments are fine in practice; if RDMA registration
  of large segments fails, raise memlock inside the container.
- **The run stops before fill-up**: read the writer's `loader.log`. Lots of `-200` means the segment
  cap was reached (expected); `-901` is the real problem.
- **Segment size in the report looks wrong**: `report` trusts the `run.env` inside the run
  directory. Seeing command-line defaults means that run's `run.env` was never generated -- check
  whether `start` really ran.

## Known limitations

- **The driver needs python3** (3.8+, standard library only). On Windows install the official
  python.org build; the Microsoft Store `python.exe` placeholder cannot run scripts. Without Python
  run it on any host.
- Every run leaves `/tmp/ks/<run_id>/` (scripts + logs) in each container for post-mortem; delete
  it manually once it is no longer needed.

## Constraints

- All roles run inside containers; the host only drives, never executes tests and leaves no files.
- Artifacts are archived under `$RUNS_ROOT` in the master container (default
  `/workspace/mooncake-realkey/keyscale-runs`).
- Only one configuration may run at a time.
- Do not modify the code under test and do not commit anything from it.
