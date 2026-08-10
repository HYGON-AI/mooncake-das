# PG Tests

## Environment Variables

- `MOONCAKE_PGTEST_DEVICE_FILTERS`
  Comma-separated NIC / IB device names passed to
  `pg.set_device_filter(...)`. Leave unset to use the backend default device
  selection.

- `MOONCAKE_PGTEST_MASTER_ADDR`
  Rendezvous address used for the local test process group. Defaults to
  `127.0.0.1`.

- `MOONCAKE_PGTEST_MASTER_PORT`
  Rendezvous port used for the local test process group. If unset, each test
  allocates a free local port automatically.

## Usage

```bash
# Run all test cases
python -m unittest discover -s mooncake-pg/tests -v

# Run CUDA test cases
python -m unittest discover -s mooncake-pg/tests -k CUDA -v

# Run CPU-only test cases
python -m unittest discover -s mooncake-pg/tests -k CPU -v
```

## Multinode GPU Group Smoke

`test_pg_multinode_gpu_group.py` is intended to be launched once per rank across
machines. For a two-node, two-GPU-per-node run, start ranks 0-1 on node 0 and
ranks 2-3 on node 1 with the same rendezvous address and port.

```bash
# Common values on every rank
export MASTER_ADDR=10.16.1.45
export MASTER_PORT=29500
export WORLD_SIZE=4
export LOCAL_WORLD_SIZE=2
export HIP_VISIBLE_DEVICES=0,1

# Node 0
MOONCAKE_PG_HOST_IP=10.16.1.45 RANK=0 LOCAL_RANK=0 \
  python3 mooncake-pg/tests/test_pg_multinode_gpu_group.py
MOONCAKE_PG_HOST_IP=10.16.1.45 RANK=1 LOCAL_RANK=1 \
  python3 mooncake-pg/tests/test_pg_multinode_gpu_group.py

# Node 1
MOONCAKE_PG_HOST_IP=10.16.1.61 RANK=2 LOCAL_RANK=0 \
  python3 mooncake-pg/tests/test_pg_multinode_gpu_group.py
MOONCAKE_PG_HOST_IP=10.16.1.61 RANK=3 LOCAL_RANK=1 \
  python3 mooncake-pg/tests/test_pg_multinode_gpu_group.py
```

The smoke covers world collectives, node-local subgroups, cross-node same-GPU
lane subgroups, world-ring P2P, and cross-node lane P2P. Set
`MOONCAKE_PGTEST_DEVICE_FILTERS` to a comma-separated HCA filter list when the
backend must bind specific RDMA devices.
