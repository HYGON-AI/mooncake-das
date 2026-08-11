import argparse
import os
import socket
from collections.abc import Callable
from datetime import timedelta

import torch
import torch.distributed as dist
from mooncake import pg


DEFAULT_TIMEOUT_S = 120


def _env_int(name: str, default: int | None = None) -> int:
    value = os.getenv(name)
    if value is None:
        if default is None:
            raise RuntimeError(f"{name} is required")
        return default
    return int(value)


def _device_filter_from_env() -> list[str] | None:
    raw = os.getenv("MOONCAKE_PGTEST_DEVICE_FILTERS")
    if not raw:
        return None
    filters = [item.strip() for item in raw.split(",") if item.strip()]
    return filters or None


def _set_mooncake_host_ip() -> None:
    host_ip = os.getenv("MOONCAKE_PG_HOST_IP")
    if not host_ip:
        raise RuntimeError("MOONCAKE_PG_HOST_IP is required for multinode PG tests")
    pg.set_host_ip(host_ip)


def _validate_launch_env(
    rank: int,
    world_size: int,
    local_rank: int,
    local_world_size: int,
) -> None:
    for name in ("MASTER_ADDR", "MASTER_PORT"):
        if not os.getenv(name):
            raise RuntimeError(f"{name} is required for multinode PG tests")
    if world_size <= 0:
        raise RuntimeError(f"WORLD_SIZE must be positive, got {world_size}")
    if local_world_size <= 0:
        raise RuntimeError(
            f"LOCAL_WORLD_SIZE must be positive, got {local_world_size}"
        )
    if world_size % local_world_size != 0:
        raise RuntimeError(
            f"WORLD_SIZE={world_size} must be divisible by "
            f"LOCAL_WORLD_SIZE={local_world_size}"
        )
    if world_size == local_world_size:
        raise RuntimeError(
            "multinode PG tests require at least two nodes: "
            f"WORLD_SIZE={world_size}, LOCAL_WORLD_SIZE={local_world_size}"
        )
    if not 0 <= rank < world_size:
        raise RuntimeError(f"RANK={rank} is outside [0, {world_size})")
    if not 0 <= local_rank < local_world_size:
        raise RuntimeError(
            f"LOCAL_RANK={local_rank} is outside [0, {local_world_size})"
        )
    expected_local_rank = rank % local_world_size
    if local_rank != expected_local_rank:
        raise RuntimeError(
            f"LOCAL_RANK={local_rank} does not match packed-rank topology; "
            f"expected RANK % LOCAL_WORLD_SIZE = {expected_local_rank}"
        )
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA/HIP runtime is unavailable")
    device_count = torch.cuda.device_count()
    if local_rank >= device_count:
        raise RuntimeError(
            f"LOCAL_RANK={local_rank} requires a visible GPU, but only "
            f"{device_count} device(s) are visible"
        )


def _init_process_group(rank: int, world_size: int, timeout_s: int) -> None:
    dist.init_process_group(
        "mooncake",
        rank=rank,
        world_size=world_size,
        timeout=timedelta(seconds=timeout_s),
    )
    if dist.get_rank() != rank:
        raise AssertionError(
            f"rank mismatch after init: expected {rank}, got {dist.get_rank()}"
        )
    if dist.get_world_size() != world_size:
        raise AssertionError(
            "world-size mismatch after init: "
            f"expected {world_size}, got {dist.get_world_size()}"
        )


def _init_world(timeout_s: int) -> tuple[int, int, int, int, torch.device]:
    rank = _env_int("RANK")
    world_size = _env_int("WORLD_SIZE")
    local_rank = _env_int("LOCAL_RANK")
    local_world_size = _env_int("LOCAL_WORLD_SIZE", 1)

    _validate_launch_env(rank, world_size, local_rank, local_world_size)
    _set_mooncake_host_ip()
    device_filters = _device_filter_from_env()
    if device_filters is not None:
        pg.set_device_filter(device_filters)

    torch.cuda.set_device(local_rank)
    device = torch.device("cuda", local_rank)
    _init_process_group(rank, world_size, timeout_s)
    return rank, world_size, local_rank, local_world_size, device


def _sync(device: torch.device) -> None:
    torch.cuda.synchronize(device)


def _node_groups(world_size: int, local_world_size: int) -> list[list[int]]:
    if world_size % local_world_size != 0:
        raise RuntimeError(
            f"WORLD_SIZE={world_size} must be divisible by "
            f"LOCAL_WORLD_SIZE={local_world_size}"
        )
    return [
        list(range(start, start + local_world_size))
        for start in range(0, world_size, local_world_size)
    ]


def _lane_groups(world_size: int, local_world_size: int) -> list[list[int]]:
    node_count = world_size // local_world_size
    return [
        [node * local_world_size + local_rank for node in range(node_count)]
        for local_rank in range(local_world_size)
    ]


def _create_groups(
    rank: int,
    world_size: int,
    local_world_size: int,
) -> tuple[object | None, object | None, list[int], list[int]]:
    my_node_group = None
    my_lane_group = None
    my_node_ranks: list[int] = []
    my_lane_ranks: list[int] = []

    for ranks in _node_groups(world_size, local_world_size):
        group = dist.new_group(ranks=ranks, backend="mooncake")
        if rank in ranks:
            my_node_group = group
            my_node_ranks = ranks

    for ranks in _lane_groups(world_size, local_world_size):
        group = dist.new_group(ranks=ranks, backend="mooncake")
        if rank in ranks:
            my_lane_group = group
            my_lane_ranks = ranks

    return my_node_group, my_lane_group, my_node_ranks, my_lane_ranks


def _assert_equal(actual, expected, label: str, rank: int) -> None:
    if actual != expected:
        raise AssertionError(f"rank {rank} {label}: expected {expected}, got {actual}")


def _test_world_collectives(rank: int, world_size: int, device: torch.device) -> None:
    expected_sum = sum(peer + 1 for peer in range(world_size))

    all_reduce_cases = (
        ("sum", rank + 1, dist.ReduceOp.SUM, expected_sum),
        ("min", rank + 10, dist.ReduceOp.MIN, 10),
        ("max", rank + 10, dist.ReduceOp.MAX, world_size + 9),
        ("product", 2, dist.ReduceOp.PRODUCT, 2**world_size),
    )
    for label, value, op, expected in all_reduce_cases:
        reduced = torch.tensor([value], dtype=torch.int32, device=device)
        dist.all_reduce(reduced, op=op)
        _sync(device)
        _assert_equal(
            int(reduced.cpu().item()),
            expected,
            f"all_reduce {label}",
            rank,
        )

    async_reduced = torch.tensor([rank + 1], dtype=torch.int32, device=device)
    async_work = dist.all_reduce(
        async_reduced,
        op=dist.ReduceOp.SUM,
        async_op=True,
    )
    async_work.wait()
    _sync(device)
    _assert_equal(
        int(async_reduced.cpu().item()),
        expected_sum,
        "async all_reduce",
        rank,
    )

    local = torch.full((4,), rank + 1, dtype=torch.int32, device=device)

    gathered = [torch.empty_like(local) for _ in range(world_size)]
    dist.all_gather(gathered, local)
    _sync(device)
    expected_gather = [[peer + 1] * 4 for peer in range(world_size)]
    _assert_equal(
        [tensor.cpu().tolist() for tensor in gathered],
        expected_gather,
        "all_gather",
        rank,
    )

    gather_input = torch.tensor([rank], dtype=torch.int32, device=device)
    gathered_tensor = torch.empty(world_size, dtype=torch.int32, device=device)
    dist.all_gather_into_tensor(gathered_tensor, gather_input)
    _sync(device)
    _assert_equal(
        gathered_tensor.cpu().tolist(),
        list(range(world_size)),
        "all_gather_into_tensor",
        rank,
    )

    broadcasted = torch.tensor(
        [777 if rank == 0 else -1],
        dtype=torch.int32,
        device=device,
    )
    dist.broadcast(broadcasted, src=0)
    _sync(device)
    _assert_equal(int(broadcasted.cpu().item()), 777, "broadcast", rank)

    scatter_input = torch.arange(
        rank * world_size,
        (rank + 1) * world_size,
        dtype=torch.int32,
        device=device,
    )
    scatter_output = torch.empty(1, dtype=torch.int32, device=device)
    dist.reduce_scatter_tensor(scatter_output, scatter_input, op=dist.ReduceOp.SUM)
    _sync(device)
    expected_rs = world_size * (world_size * (world_size - 1) // 2 + rank)
    _assert_equal(scatter_output.cpu().tolist(), [expected_rs], "reduce_scatter", rank)

    gather_value = torch.tensor([rank], dtype=torch.int32, device=device)
    if rank == 0:
        gather_list = [
            torch.empty_like(gather_value) for _ in range(world_size)
        ]
        dist.gather(gather_value, gather_list=gather_list, dst=0)
        _sync(device)
        _assert_equal(
            [int(item.cpu().item()) for item in gather_list],
            list(range(world_size)),
            "gather",
            rank,
        )
    else:
        dist.gather(gather_value, dst=0)

    scatter_output = torch.empty(1, dtype=torch.int32, device=device)
    if rank == 0:
        scatter_list = [
            torch.tensor([peer * 11], dtype=torch.int32, device=device)
            for peer in range(world_size)
        ]
        dist.scatter(scatter_output, scatter_list=scatter_list, src=0)
    else:
        dist.scatter(scatter_output, src=0)
    _sync(device)
    _assert_equal(
        int(scatter_output.cpu().item()),
        rank * 11,
        "scatter",
        rank,
    )

    reduce_value = torch.tensor([rank + 1], dtype=torch.int32, device=device)
    dist.reduce(reduce_value, dst=0, op=dist.ReduceOp.SUM)
    _sync(device)
    if rank == 0:
        _assert_equal(
            int(reduce_value.cpu().item()),
            expected_sum,
            "reduce",
            rank,
        )

    dist.barrier()


def _test_subgroups(
    rank: int,
    local_rank: int,
    device: torch.device,
    node_group,
    lane_group,
    node_ranks: list[int],
    lane_ranks: list[int],
) -> None:
    node_tensor = torch.tensor([rank], dtype=torch.int32, device=device)
    dist.all_reduce(node_tensor, group=node_group, op=dist.ReduceOp.SUM)
    _sync(device)
    _assert_equal(
        int(node_tensor.cpu().item()),
        sum(node_ranks),
        "node subgroup all_reduce",
        rank,
    )

    lane_tensor = torch.tensor(
        [rank * 10 + local_rank],
        dtype=torch.int32,
        device=device,
    )
    dist.all_reduce(lane_tensor, group=lane_group, op=dist.ReduceOp.SUM)
    _sync(device)
    expected_lane = sum(peer * 10 + local_rank for peer in lane_ranks)
    _assert_equal(
        int(lane_tensor.cpu().item()),
        expected_lane,
        "cross-node lane subgroup all_reduce",
        rank,
    )

    lane_input = torch.tensor([rank], dtype=torch.int32, device=device)
    lane_gathered = torch.empty(
        len(lane_ranks),
        dtype=torch.int32,
        device=device,
    )
    dist.all_gather_into_tensor(
        lane_gathered,
        lane_input,
        group=lane_group,
    )
    _sync(device)
    _assert_equal(
        lane_gathered.cpu().tolist(),
        lane_ranks,
        "cross-node lane subgroup all_gather_into_tensor",
        rank,
    )

    lane_root = lane_ranks[0]
    lane_broadcast = torch.tensor(
        [lane_root * 100 + 9 if rank == lane_root else -1],
        dtype=torch.int32,
        device=device,
    )
    dist.broadcast(lane_broadcast, src=lane_root, group=lane_group)
    _sync(device)
    _assert_equal(
        int(lane_broadcast.cpu().item()),
        lane_root * 100 + 9,
        "cross-node lane subgroup broadcast",
        rank,
    )

    lane_index = lane_ranks.index(rank)
    dst_index = (lane_index + 1) % len(lane_ranks)
    src_index = (lane_index - 1 + len(lane_ranks)) % len(lane_ranks)
    dst_rank = lane_ranks[dst_index]
    src_rank = lane_ranks[src_index]
    lane_send = torch.tensor(
        [rank * 1000 + dst_rank],
        dtype=torch.int64,
        device=device,
    )
    lane_recv = torch.empty_like(lane_send)
    lane_works = dist.batch_isend_irecv(
        [
            dist.P2POp(
                dist.isend,
                lane_send,
                group=lane_group,
                group_peer=dst_index,
            ),
            dist.P2POp(
                dist.irecv,
                lane_recv,
                group=lane_group,
                group_peer=src_index,
            ),
        ]
    )
    for work in lane_works:
        work.wait()
    _sync(device)
    _assert_equal(
        int(lane_recv.cpu().item()),
        src_rank * 1000 + rank,
        "cross-node lane subgroup p2p",
        rank,
    )

    dist.barrier(group=node_group)
    dist.barrier(group=lane_group)


def _test_world_ring_p2p(rank: int, world_size: int, device: torch.device) -> None:
    send_tensor = torch.tensor([rank], dtype=torch.int64, device=device)
    recv_tensor = torch.empty_like(send_tensor)
    dst = (rank + 1) % world_size
    src = (rank - 1 + world_size) % world_size
    works = dist.batch_isend_irecv(
        [
            dist.P2POp(dist.isend, send_tensor, peer=dst),
            dist.P2POp(dist.irecv, recv_tensor, peer=src),
        ]
    )
    for work in works:
        work.wait()
    _sync(device)
    _assert_equal(int(recv_tensor.cpu().item()), src, "ring p2p", rank)


def _test_cross_node_lane_p2p(
    rank: int,
    local_rank: int,
    world_size: int,
    local_world_size: int,
    device: torch.device,
) -> None:
    if world_size == local_world_size:
        return
    node_count = world_size // local_world_size
    node_rank = rank // local_world_size
    peer_node = (node_rank + 1) % node_count
    src_node = (node_rank - 1 + node_count) % node_count
    dst = peer_node * local_world_size + local_rank
    src = src_node * local_world_size + local_rank
    send_tensor = torch.tensor([rank * 100 + dst], dtype=torch.int64, device=device)
    recv_tensor = torch.empty_like(send_tensor)
    works = dist.batch_isend_irecv(
        [
            dist.P2POp(dist.isend, send_tensor, peer=dst),
            dist.P2POp(dist.irecv, recv_tensor, peer=src),
        ]
    )
    for work in works:
        work.wait()
    _sync(device)
    expected = src * 100 + rank
    _assert_equal(int(recv_tensor.cpu().item()), expected, "cross-node lane p2p", rank)


def _test_cross_node_pair_direct_and_ordered_p2p(
    rank: int,
    local_rank: int,
    local_world_size: int,
    device: torch.device,
) -> None:
    """Exercise direct send/recv and ordered batches between the first two nodes."""
    src = local_rank
    dst = local_world_size + local_rank

    dist.barrier()
    if rank == src:
        direct = torch.tensor(
            [src * 100 + dst],
            dtype=torch.int64,
            device=device,
        )
        dist.send(direct, dst=dst)

        ordered = [
            torch.tensor(
                [src * 10000 + index],
                dtype=torch.int64,
                device=device,
            )
            for index in range(4)
        ]
        works = dist.batch_isend_irecv(
            [
                dist.P2POp(dist.isend, tensor, peer=dst)
                for tensor in ordered
            ]
        )
        for work in works:
            work.wait()
        _sync(device)
    elif rank == dst:
        direct = torch.empty(1, dtype=torch.int64, device=device)
        dist.recv(direct, src=src)
        _sync(device)
        _assert_equal(
            int(direct.cpu().item()),
            src * 100 + dst,
            "cross-node direct p2p",
            rank,
        )

        ordered = [
            torch.empty(1, dtype=torch.int64, device=device)
            for _ in range(4)
        ]
        works = dist.batch_isend_irecv(
            [
                dist.P2POp(dist.irecv, tensor, peer=src)
                for tensor in ordered
            ]
        )
        for work in works:
            work.wait()
        _sync(device)
        _assert_equal(
            [int(tensor.cpu().item()) for tensor in ordered],
            [src * 10000 + index for index in range(4)],
            "cross-node ordered p2p",
            rank,
        )
    dist.barrier()


def _test_cross_node_multiple_senders(
    rank: int,
    world_size: int,
    local_world_size: int,
    device: torch.device,
) -> None:
    """Exercise concurrent cross-node sends targeting one receiver."""
    receiver = 0
    senders = [
        peer
        for peer in range(world_size)
        if peer // local_world_size != receiver // local_world_size
    ][:2]
    if len(senders) < 2:
        return

    dist.barrier()
    if rank == receiver:
        received = [
            torch.empty(1, dtype=torch.int64, device=device)
            for _ in senders
        ]
        works = dist.batch_isend_irecv(
            [
                dist.P2POp(dist.irecv, tensor, peer=peer)
                for peer, tensor in zip(senders, received, strict=True)
            ]
        )
        for work in works:
            work.wait()
        _sync(device)
        _assert_equal(
            [int(tensor.cpu().item()) for tensor in received],
            [peer * 100 + 7 for peer in senders],
            "cross-node multiple senders",
            rank,
        )
    elif rank in senders:
        payload = torch.tensor(
            [rank * 100 + 7],
            dtype=torch.int64,
            device=device,
        )
        works = dist.batch_isend_irecv(
            [dist.P2POp(dist.isend, payload, peer=receiver)]
        )
        for work in works:
            work.wait()
        _sync(device)
    dist.barrier()


def _run_step(rank: int, label: str, test: Callable[[], None]) -> None:
    if rank == 0:
        print(f"[world] BEGIN {label}", flush=True)
    test()
    dist.barrier()
    if rank == 0:
        print(f"[world] PASS {label}", flush=True)


def run(timeout_s: int) -> None:
    rank, world_size, local_rank, local_world_size, device = _init_world(timeout_s)
    node_group = None
    lane_group = None
    try:
        node_group, lane_group, node_ranks, lane_ranks = _create_groups(
            rank,
            world_size,
            local_world_size,
        )
        host = socket.gethostname()
        print(
            f"[rank {rank}] host={host} local_rank={local_rank} "
            f"world_size={world_size} local_world_size={local_world_size} "
            f"node_ranks={node_ranks} lane_ranks={lane_ranks}",
            flush=True,
        )

        _run_step(
            rank,
            "world collectives",
            lambda: _test_world_collectives(rank, world_size, device),
        )
        _run_step(
            rank,
            "node/lane subgroups",
            lambda: _test_subgroups(
                rank,
                local_rank,
                device,
                node_group,
                lane_group,
                node_ranks,
                lane_ranks,
            ),
        )
        _run_step(
            rank,
            "world ring p2p",
            lambda: _test_world_ring_p2p(rank, world_size, device),
        )
        _run_step(
            rank,
            "cross-node lane p2p",
            lambda: _test_cross_node_lane_p2p(
                rank,
                local_rank,
                world_size,
                local_world_size,
                device,
            ),
        )
        _run_step(
            rank,
            "cross-node direct/ordered p2p",
            lambda: _test_cross_node_pair_direct_and_ordered_p2p(
                rank,
                local_rank,
                local_world_size,
                device,
            ),
        )
        _run_step(
            rank,
            "cross-node multiple senders",
            lambda: _test_cross_node_multiple_senders(
                rank,
                world_size,
                local_world_size,
                device,
            ),
        )
        dist.barrier()
        print(f"[rank {rank}] PASS healthy traffic", flush=True)
    finally:
        for group in (lane_group, node_group):
            if group is not None:
                try:
                    dist.destroy_process_group(group)
                except Exception:
                    pass
        if dist.is_initialized():
            dist.destroy_process_group()

    _init_process_group(rank, world_size, timeout_s)
    try:
        reinit_value = torch.tensor(
            [rank + 1],
            dtype=torch.int32,
            device=device,
        )
        dist.all_reduce(reinit_value, op=dist.ReduceOp.SUM)
        _sync(device)
        _assert_equal(
            int(reinit_value.cpu().item()),
            sum(peer + 1 for peer in range(world_size)),
            "destroy/reinit all_reduce",
            rank,
        )
        dist.barrier()
        print(f"[rank {rank}] PASS", flush=True)
    finally:
        if dist.is_initialized():
            dist.destroy_process_group()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mooncake PG multinode multi-GPU group smoke test"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=int(os.getenv("MOONCAKE_PGTEST_TIMEOUT", DEFAULT_TIMEOUT_S)),
        help="process-group operation timeout in seconds",
    )
    args = parser.parse_args()
    run(args.timeout)


if __name__ == "__main__":
    main()
