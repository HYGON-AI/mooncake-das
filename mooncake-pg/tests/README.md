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
machines. Use a distributed launcher so that all ranks start concurrently. For
a two-node, two-GPU-per-node run:

```bash
# Node 0
MOONCAKE_PG_HOST_IP=10.16.1.45 HIP_VISIBLE_DEVICES=0,1 \
  torchrun --nnodes=2 --nproc-per-node=2 --node-rank=0 \
    --master-addr=10.16.1.45 --master-port=29500 \
    mooncake-pg/tests/test_pg_multinode_gpu_group.py

# Node 1
MOONCAKE_PG_HOST_IP=10.16.1.61 HIP_VISIBLE_DEVICES=0,1 \
  torchrun --nnodes=2 --nproc-per-node=2 --node-rank=1 \
    --master-addr=10.16.1.45 --master-port=29500 \
    mooncake-pg/tests/test_pg_multinode_gpu_group.py
```

The smoke covers synchronous and asynchronous world collectives, rooted
collectives, node-local subgroups, cross-node same-GPU lane subgroup
collectives and P2P, world-ring P2P, direct and ordered cross-node P2P,
multiple cross-node senders, synchronized teardown, and world-group
destroy/re-init.

Set `MOONCAKE_PGTEST_DEVICE_FILTERS` to a comma-separated HCA filter list when
the backend must bind specific RDMA devices. Set `MC_GID_INDEX` when the RDMA
environment requires an explicit GID index. Use an outer process timeout in
automation and require every rank to exit with status 0 and print
`[rank N] PASS`.
