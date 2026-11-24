# Typical usage: the user says one sentence

This file walks through one complete interaction. Parameters, theory and the conf column
definitions live in `SKILL.md`; here we only cover "how to drive it".

## The user says

> Run mooncake-keyscale.

## What the agent does

**1. Pick a configuration**

Default is 6 segments: `conf/nodes.6seg.conf` (master + writer on the same node).
If the user asked for a specific segment size, pass it as an environment variable instead of
editing the conf.

**2. Confirm where to run it**

```bash
python3 <skill>/driver/ks_control.py where <conf>
```

The output shows the master host, the role of every node, and copy-pasteable ssh commands.
Default is `--driver master`: running anywhere else fails immediately and prints the correct
command.

**3. Check the prerequisites** (starts nothing)

```bash
ssh <master_host> "python3 <skill>/driver/ks_control.py check <conf>"
```

- `consistency: OK` -> continue
- `PREREQ MISSING` -> **stop**, tell the user what is missing (package not installed / versions
  differ / no RDMA card); do not force a run

**4. Capacity accounting**

```bash
ssh <master_host> "python3 <skill>/driver/ks_control.py plan <conf>"
```

- every node `OK` -> continue
- `TOO TIGHT` -> **stop**, quote the need/avail numbers, suggest moving the master or shrinking
  `SEGMENT_SIZE`

**5. Run it**

```bash
ssh <master_host> "SEGMENT_SIZE=64GiB python3 <skill>/driver/ks_control.py run <conf> 8"
```

Rough timing: ~40 minutes to fill up, then 1-1.5 hours for 8 eviction rounds (7-10 minutes each).

**6. Report back**

Give the user three things: the `run_id`, the observations from the report (fill-up time and key
count / eviction rounds / `-901` and timeout counts with their phase), and the `report.md` path.

## Expected output

End of the command line:

```
artifacts: container_name@node-a:/workspace/mooncake-realkey/keyscale-runs/<run_id>/report.md
```

Report header:

```
# Mooncake 0.3.14 6-segment key-capacity test

Test       Resident cap              Result
6 segments 402,653,184 (~403M)       8 eviction rounds completed; -901 = 0, no timeouts
...
**Conclusion**: fill-up time, per-round eviction duration, -901/timeout counts and phase, master RSS
```

## How to report abnormal observations

| Observation | What the agent should say |
| --- | --- |
| `check` reports `PREREQ MISSING` | what is missing, on which machine, fix the environment before running |
| `plan` reports `TOO TIGHT` | the concrete need / avail numbers, suggest moving the master or lowering `SEGMENT_SIZE` |
| The report shows `-901` | the failure was reproduced; give the count, batch count, phase (before fill-up / during eviction) and the report path; **do not rerun to hide it** |
| The report shows no fill-up or unfinished eviction | give the peak key count and the last round's timestamp |

## Interrupting a run

`run` is `start -> watch -> report -> stop`. After an interruption use `status <conf>` to see
whether processes are still around, then decide whether to continue with `watch` or `stop`.
