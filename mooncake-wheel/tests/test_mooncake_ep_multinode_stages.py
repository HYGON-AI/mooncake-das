import argparse
import os
import random
from datetime import timedelta

import torch
import torch.distributed as dist
import torch.multiprocessing as mp

from ep_test_utils import calc_diff, per_token_cast_back
from mooncake import pg
from mooncake.mooncake_ep_buffer import Buffer


DEFAULT_TIMEOUT_S = 420
DEFAULT_EP_TIMEOUT_US = 120_000_000
STAGES = (
    "pg_init",
    "pg_collective",
    "ep_buffer",
    "ep_dispatch_send_only_bf16",
    "ep_dispatch_bf16",
    "ep_dispatch_async_repeat_bf16",
    "ep_dispatch_fp8",
    "ep_combine",
    "ep_update_member",
    "ep_recovery_lifecycle",
    "all",
)


def _env_int(name: str, default: int | None = None) -> int:
    value = os.getenv(name)
    if value is None:
        if default is None:
            raise RuntimeError(f"{name} is required")
        return default
    return int(value)


def _env_str(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _env_float(
    name: str, default: float, *, allow_zero: bool = False
) -> float:
    value = float(os.getenv(name, str(default)))
    lower_bound_ok = value >= 0.0 if allow_zero else value > 0.0
    if not lower_bound_ok or value > 1.0:
        interval = "[0, 1]" if allow_zero else "(0, 1]"
        raise RuntimeError(f"{name} must be in {interval}, got {value}")
    return value


def _device_filter_from_env() -> list[str] | None:
    raw = os.getenv("MOONCAKE_PGTEST_DEVICE_FILTERS") or os.getenv(
        "MOONCAKE_PG_DEVICE_FILTERS"
    )
    if not raw:
        return None
    filters = [item.strip() for item in raw.split(",") if item.strip()]
    return filters or None


def _init_dist(local_rank: int, timeout_s: int):
    local_world_size = _env_int("LOCAL_WORLD_SIZE")
    node_rank = _env_int("NODE_RANK")
    nnodes = _env_int("NNODES", 2)
    world_size = _env_int("WORLD_SIZE", nnodes * local_world_size)
    rank = node_rank * local_world_size + local_rank

    pg.set_host_ip(_env_str("MOONCAKE_PG_HOST_IP"))
    device_filters = _device_filter_from_env()
    if device_filters is not None:
        pg.set_device_filter(device_filters)

    torch.cuda.set_device(local_rank)
    dist.init_process_group(
        backend="mooncake",
        init_method=f"tcp://{_env_str('MASTER_ADDR')}:{_env_str('MASTER_PORT')}",
        rank=rank,
        world_size=world_size,
        timeout=timedelta(seconds=timeout_s),
    )
    group = dist.new_group(ranks=list(range(world_size)), backend="mooncake")
    return rank, world_size, local_rank, local_world_size, group


def _sync() -> None:
    torch.cuda.synchronize()


def _assert_equal(actual, expected, label: str, rank: int) -> None:
    if actual != expected:
        raise AssertionError(f"rank {rank} {label}: expected {expected}, got {actual}")


def _ep_shape(world_size: int) -> tuple[int, int, int, int]:
    num_tokens = _env_int("MOONCAKE_EP_STAGE_NUM_TOKENS", 32)
    hidden = _env_int("MOONCAKE_EP_STAGE_HIDDEN", 7168)
    num_topk = _env_int("MOONCAKE_EP_STAGE_TOPK", 2)
    experts_per_rank = _env_int("MOONCAKE_EP_STAGE_EXPERTS_PER_RANK", 72)
    if hidden % 128 != 0:
        raise RuntimeError("MOONCAKE_EP_STAGE_HIDDEN must be divisible by 128")
    return num_tokens, hidden, num_topk, world_size * experts_per_rank


def _ep_timeout_us() -> int:
    return _env_int("MOONCAKE_EP_STAGE_TIMEOUT_US", DEFAULT_EP_TIMEOUT_US)


def _make_inputs(rank: int, world_size: int):
    torch.manual_seed(2024 + rank)
    random.seed(2024 + rank)
    num_tokens, hidden, num_topk, num_experts = _ep_shape(world_size)
    if num_experts % world_size != 0:
        raise RuntimeError("num_experts must be divisible by WORLD_SIZE")

    rank_offset = 128
    x = torch.ones((num_tokens, hidden), dtype=torch.bfloat16, device="cuda") * (
        rank - rank_offset
    )
    x[:, -128:] = torch.arange(num_tokens, device="cuda").to(torch.bfloat16).view(-1, 1)

    topk_idx = _make_topk_idx(
        rank, world_size, num_tokens, num_topk, num_experts
    )
    topk_weights = torch.randn((num_tokens, num_topk), dtype=torch.float32, device="cuda")
    topk_weights = topk_weights.abs()

    active_ranks = torch.ones((world_size,), dtype=torch.int32, device="cuda")
    return x, topk_idx, topk_weights, active_ranks, num_tokens, hidden, num_experts


def _make_official_inputs(rank: int, world_size: int):
    seed = _env_int("MOONCAKE_EP_STAGE_INPUT_SEED", 1)
    torch.manual_seed(seed + rank)
    random.seed(seed + rank)
    num_tokens, hidden, num_topk, num_experts = _ep_shape(world_size)

    rank_offset = 128
    x = torch.ones((num_tokens, hidden), dtype=torch.bfloat16, device="cuda") * (
        rank - rank_offset
    )
    x[:, -128:] = torch.arange(num_tokens, device="cuda").to(torch.bfloat16).view(
        -1, 1
    )
    scores = (
        torch.randn(
            (num_tokens, num_experts), dtype=torch.float32, device="cuda"
        ).abs()
        + 1
    )
    topk_idx = torch.topk(
        scores, num_topk, dim=-1, largest=True, sorted=True
    )[1]
    for _ in range(10):
        topk_idx[
            random.randint(0, num_tokens - 1),
            random.randint(0, num_topk - 1),
        ] = -1

    topk_weights = torch.randn(
        (num_tokens, num_topk), dtype=torch.float32, device="cuda"
    ).abs()
    active_ranks_size = _env_int(
        "MOONCAKE_EP_STAGE_ACTIVE_RANKS_SIZE", world_size
    )
    active_ranks = torch.ones(
        (active_ranks_size,), dtype=torch.int32, device="cuda"
    )
    return x, topk_idx, topk_weights, active_ranks, num_tokens, hidden, num_experts


def _make_topk_idx(
    rank: int,
    world_size: int,
    num_tokens: int,
    num_topk: int,
    num_experts: int,
):
    if num_experts % world_size != 0:
        raise RuntimeError("num_experts must be divisible by WORLD_SIZE")
    experts_per_rank = num_experts // world_size
    tokens = torch.arange(num_tokens, dtype=torch.int64, device="cuda").view(-1, 1)
    offsets = torch.arange(num_topk, dtype=torch.int64, device="cuda").view(1, -1)
    dst_rank = (tokens + offsets + rank) % world_size
    local_expert = (
        tokens * num_topk + offsets + rank * num_topk
    ) % experts_per_rank
    topk_idx = dst_rank * experts_per_rank + local_expert
    if num_tokens >= 2 and num_topk >= 2:
        topk_idx[rank % num_tokens, rank % num_topk] = -1
    return topk_idx.contiguous()


def _make_all_topk_idx(world_size: int, num_tokens: int, num_topk: int, num_experts: int):
    return torch.stack(
        [
            _make_topk_idx(
                peer, world_size, num_tokens, num_topk, num_experts
            )
            for peer in range(world_size)
        ],
        dim=0,
    )


def _new_buffer(group, world_size: int) -> Buffer:
    num_tokens, hidden, _, num_experts = _ep_shape(world_size)
    logical_device = torch.cuda.current_device()
    preferred_hca = pg.get_preferred_hca(group, f"cuda:{logical_device}")
    if not preferred_hca:
        raise RuntimeError(
            f"rank {group.rank()} device {logical_device} has no preferred HCA"
        )
    print(
        "EP_TOPOLOGY_SELECTION "
        f"rank={group.rank()} logical_device={logical_device} "
        f"preferred_hca={preferred_hca}",
        flush=True,
    )
    buffer_bytes = Buffer.get_ep_buffer_size_hint(
        num_tokens,
        hidden,
        world_size,
        num_experts,
    )
    free_bytes, total_bytes = torch.cuda.mem_get_info()
    free_ratio = free_bytes / total_bytes
    min_free_ratio = _env_float(
        "MOONCAKE_EP_MIN_FREE_MEMORY_RATIO", 0.9, allow_zero=True
    )
    memory_gate_enabled = min_free_ratio > 0.0
    print(
        "EP_MEMORY_SNAPSHOT "
        f"rank={group.rank()} device={torch.cuda.current_device()} "
        f"free_bytes={free_bytes} total_bytes={total_bytes} "
        f"free_ratio={free_ratio:.6f} min_free_ratio={min_free_ratio:.6f} "
        f"memory_gate_enabled={str(memory_gate_enabled).lower()} "
        f"ep_buffer_bytes={buffer_bytes}",
        flush=True,
    )
    if memory_gate_enabled and free_ratio < min_free_ratio:
        raise RuntimeError(
            f"rank {group.rank()} device {torch.cuda.current_device()} "
            f"free_ratio={free_ratio:.6f}, required>={min_free_ratio:.6f}; "
            "refusing contaminated EP test"
        )
    return Buffer(group, num_ep_buffer_bytes=buffer_bytes)


def _assert_all_ranks_active(
    active_ranks: torch.Tensor, world_size: int, stage: str, rank: int
) -> None:
    actual = active_ranks.detach().cpu().tolist()
    _assert_equal(len(actual), world_size, f"{stage} active_ranks size", rank)
    _assert_equal(actual, [1] * world_size, f"{stage} active_ranks", rank)


def _gate_all_ranks_active(
    active_ranks: torch.Tensor, world_size: int, stage: str, rank: int
) -> None:
    actual = active_ranks.detach().cpu().tolist()
    local_ok = len(actual) == world_size and actual == [1] * world_size
    passed = torch.tensor([int(local_ok)], dtype=torch.int32, device="cuda")
    dist.all_reduce(passed, op=dist.ReduceOp.SUM)
    if not local_ok:
        raise AssertionError(
            f"rank {rank} {stage} active_ranks: expected {[1] * world_size}, "
            f"got {actual}"
        )
    if int(passed.item()) != world_size:
        raise AssertionError(
            f"rank {rank} {stage} global gate: only "
            f"{int(passed.item())}/{world_size} ranks passed"
        )


def _validate_dispatch(
    rank: int,
    world_size: int,
    group,
    packed_recv_x,
    packed_recv_count,
    handle,
    topk_idx: torch.Tensor,
    num_experts: int,
    use_fp8: bool,
) -> torch.Tensor:
    num_local_experts = num_experts // world_size
    all_topk_idx = torch.empty(
        (world_size, topk_idx.size(0), topk_idx.size(1)),
        dtype=topk_idx.dtype,
        device=topk_idx.device,
    )
    dist.all_gather_into_tensor(all_topk_idx, topk_idx, group=group)
    _sync()
    int_mask = (2**32) - 1
    src_info, layout_range = handle[0], handle[1]

    if use_fp8:
        recv_payload = per_token_cast_back(
            packed_recv_x[0].reshape(-1, packed_recv_x[0].size(-1)),
            packed_recv_x[1].reshape(-1, packed_recv_x[1].size(-1)),
        ).view(packed_recv_x[0].shape)
    else:
        recv_payload = packed_recv_x

    for local_expert in range(num_local_experts):
        expert_id = rank * num_local_experts + local_expert
        valid_count = int(packed_recv_count[local_expert].item())
        expected_count = int((all_topk_idx == expert_id).sum().item())
        if valid_count != expected_count:
            print(
                "dispatch count mismatch "
                f"rank={rank} local_expert={local_expert} expert_id={expert_id} "
                f"valid_count={valid_count} expected_count={expected_count} "
                f"layout_range={(layout_range[local_expert]).detach().cpu().tolist()} "
                f"src_info={src_info[local_expert, :max(valid_count, 8)].detach().cpu().tolist()}",
                flush=True,
            )
        _assert_equal(valid_count, expected_count, "dispatch count", rank)
        _assert_equal(
            valid_count,
            int((layout_range[local_expert] & int_mask).sum().item()),
            "dispatch layout count",
            rank,
        )

        recv_x = recv_payload[local_expert, :valid_count]
        recv_src_info = src_info[local_expert, :valid_count]
        if valid_count == 0:
            continue
        recv_body = recv_x[:, :-128]
        recv_owner = recv_body.amin(dim=-1)
        recv_body_max = recv_body.amax(dim=-1)
        owner_consistent = torch.equal(recv_owner, recv_body_max)
        if not owner_consistent:
            print(
                "dispatch owner mismatch "
                f"rank={rank} local_expert={local_expert} expert_id={expert_id} "
                f"valid_count={valid_count} "
                f"layout_range={(layout_range[local_expert]).detach().cpu().tolist()} "
                f"owners={recv_owner[:16].detach().cpu().tolist()} "
                f"body_max={recv_body_max[:16].detach().cpu().tolist()} "
                f"src_info={recv_src_info[:16].detach().cpu().tolist()}",
                flush=True,
            )
        _assert_equal(
            owner_consistent,
            True,
            "dispatch owner consistency",
            rank,
        )
        _assert_equal(
            float((recv_x[:, -128:] - recv_src_info.view(-1, 1) % topk_idx.size(0)).abs().sum().item()),
            0.0,
            "dispatch token source",
            rank,
        )
        for peer in range(world_size):
            peer_expected = int((all_topk_idx[peer] == expert_id).sum().item())
            peer_actual = int((recv_owner == peer - 128).sum().item())
            _assert_equal(peer_actual, peer_expected, "dispatch peer ownership", rank)

    return recv_payload


def _stage_pg_init(rank: int, world_size: int, local_rank: int, local_world_size: int):
    _assert_equal(dist.get_rank(), rank, "pg rank", rank)
    _assert_equal(dist.get_world_size(), world_size, "pg world_size", rank)
    print(
        f"stage=pg_init rank={rank} local_rank={local_rank} "
        f"local_world_size={local_world_size} ok",
        flush=True,
    )


def _stage_pg_collective(rank: int, world_size: int):
    tensor = torch.full((4,), rank + 1, dtype=torch.int32, device="cuda")
    dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
    _sync()
    _assert_equal(
        tensor.cpu().tolist(),
        [sum(range(1, world_size + 1))] * 4,
        "pg all_reduce",
        rank,
    )
    gathered = [torch.empty_like(tensor) for _ in range(world_size)]
    dist.all_gather(gathered, tensor.fill_(rank))
    _sync()
    _assert_equal(
        [item.cpu().tolist() for item in gathered],
        [[peer] * 4 for peer in range(world_size)],
        "pg all_gather",
        rank,
    )
    print(f"stage=pg_collective rank={rank} ok", flush=True)


def _stage_ep_buffer(rank: int, world_size: int, group):
    buffer = _new_buffer(group, world_size)
    mode = "fallback" if buffer._use_fallback else "fast"
    print(f"stage=ep_buffer rank={rank} mode={mode} ok", flush=True)


def _stage_ep_dispatch(rank: int, world_size: int, group, use_fp8: bool):
    buffer = _new_buffer(group, world_size)
    x, topk_idx, _, active_ranks, num_tokens, _, num_experts = _make_inputs(
        rank, world_size
    )
    packed_recv_x, packed_recv_count, handle, event, hook = buffer.dispatch(
        x,
        topk_idx,
        active_ranks,
        num_tokens,
        num_experts,
        _ep_timeout_us(),
        use_fp8=use_fp8,
        async_finish=False,
        return_recv_hook=False,
    )
    event.current_stream_wait()
    _gate_all_ranks_active(active_ranks, world_size, "dispatch", rank)
    _validate_dispatch(
        rank,
        world_size,
        group,
        packed_recv_x,
        packed_recv_count,
        handle,
        topk_idx,
        num_experts,
        use_fp8,
    )
    stage = "ep_dispatch_fp8" if use_fp8 else "ep_dispatch_bf16"
    print(
        f"stage={stage} rank={rank} "
        f"active_ranks={active_ranks.detach().cpu().tolist()} ok",
        flush=True,
    )


def _stage_ep_dispatch_async_repeat(rank: int, world_size: int, group):
    buffer = _new_buffer(group, world_size)
    if rank == 1:
        buffer = _new_buffer(group, world_size)
    else:
        buffer.update_ep_member()
    input_pattern = os.getenv(
        "MOONCAKE_EP_STAGE_INPUT_PATTERN", "balanced"
    ).lower()
    if input_pattern == "balanced":
        inputs = _make_inputs(rank, world_size)
    elif input_pattern == "official":
        inputs = _make_official_inputs(rank, world_size)
    else:
        raise RuntimeError(
            "MOONCAKE_EP_STAGE_INPUT_PATTERN must be balanced or official, "
            f"got {input_pattern}"
        )
    x, topk_idx, _, active_ranks, num_tokens, _, num_experts = inputs
    print(
        f"stage=ep_dispatch_async_repeat_bf16 rank={rank} "
        f"input_pattern={input_pattern} "
        f"active_ranks_size={active_ranks.numel()}",
        flush=True,
    )

    for iteration in range(2):
        print(
            f"stage=ep_dispatch_async_repeat_bf16 rank={rank} "
            f"iteration={iteration} begin",
            flush=True,
        )
        packed_recv_x, packed_recv_count, handle, event, hook = buffer.dispatch(
            x,
            topk_idx,
            active_ranks,
            num_tokens,
            num_experts,
            _ep_timeout_us(),
            use_fp8=False,
            async_finish=True,
            return_recv_hook=False,
        )
        event.current_stream_wait()
        print(
            f"stage=ep_dispatch_async_repeat_bf16 rank={rank} "
            f"iteration={iteration} submitted",
            flush=True,
        )

    _sync()
    _gate_all_ranks_active(
        active_ranks, world_size, "dispatch_async_repeat", rank
    )
    _validate_dispatch(
        rank,
        world_size,
        group,
        packed_recv_x,
        packed_recv_count,
        handle,
        topk_idx,
        num_experts,
        False,
    )
    print(
        f"stage=ep_dispatch_async_repeat_bf16 rank={rank} "
        f"active_ranks={active_ranks.detach().cpu().tolist()} ok",
        flush=True,
    )


def _stage_ep_dispatch_send_only(rank: int, world_size: int, group):
    buffer = _new_buffer(group, world_size)
    x, topk_idx, _, active_ranks, num_tokens, _, num_experts = _make_inputs(
        rank, world_size
    )
    _, _, _, _, hook = buffer.dispatch(
        x,
        topk_idx,
        active_ranks,
        num_tokens,
        num_experts,
        _ep_timeout_us(),
        use_fp8=False,
        async_finish=False,
        return_recv_hook=True,
    )
    torch.cuda.synchronize()
    _assert_equal(callable(hook), True, "dispatch send-only recv hook", rank)
    print(f"stage=ep_dispatch_send_only_bf16 rank={rank} ok", flush=True)


def _stage_ep_combine(
    rank: int,
    world_size: int,
    group,
    update_member: bool,
    replace_rank_one: bool = False,
):
    buffer = _new_buffer(group, world_size)
    if replace_rank_one:
        if rank == 1:
            buffer = _new_buffer(group, world_size)
        else:
            buffer.update_ep_member()
    elif update_member:
        buffer.update_ep_member()
    x, topk_idx, topk_weights, active_ranks, num_tokens, _, num_experts = _make_inputs(
        rank, world_size
    )
    packed_recv_x, packed_recv_count, handle, event, hook = buffer.dispatch(
        x,
        topk_idx,
        active_ranks,
        num_tokens,
        num_experts,
        _ep_timeout_us(),
        use_fp8=False,
        async_finish=False,
        return_recv_hook=False,
    )
    event.current_stream_wait()
    _gate_all_ranks_active(active_ranks, world_size, "dispatch", rank)
    recv_payload = _validate_dispatch(
        rank,
        world_size,
        group,
        packed_recv_x,
        packed_recv_count,
        handle,
        topk_idx,
        num_experts,
        False,
    )

    combined_x, event, hook = buffer.combine(
        recv_payload,
        topk_idx,
        topk_weights,
        active_ranks,
        _ep_timeout_us(),
        handle,
        zero_copy=False,
        async_finish=False,
        return_recv_hook=False,
    )
    event.current_stream_wait()
    _gate_all_ranks_active(active_ranks, world_size, "combine", rank)
    expected = x * topk_weights.masked_fill(topk_idx == -1, 0).sum(dim=1).view(-1, 1)
    diff = calc_diff(expected, combined_x)
    if torch.isnan(combined_x).sum().item() != 0 or diff >= 1e-5:
        abs_diff = (expected.float() - combined_x.float()).abs()
        flat_idx = int(abs_diff.argmax().item())
        token_idx = flat_idx // abs_diff.size(1)
        hidden_idx = flat_idx % abs_diff.size(1)
        print(
            "combine mismatch "
            f"rank={rank} diff={diff} token={token_idx} hidden={hidden_idx} "
            f"expected={float(expected[token_idx, hidden_idx].item())} "
            f"actual={float(combined_x[token_idx, hidden_idx].item())} "
            f"topk_idx={topk_idx[token_idx].detach().cpu().tolist()} "
            f"topk_weights={topk_weights[token_idx].detach().cpu().tolist()} "
            f"active_ranks={active_ranks.detach().cpu().tolist()}",
            flush=True,
        )
        raise AssertionError(f"rank {rank} combine diff too high: {diff}")
    if replace_rank_one:
        stage = "ep_recovery_lifecycle"
    else:
        stage = "ep_update_member" if update_member else "ep_combine"
    print(
        f"stage={stage} rank={rank} diff={diff} "
        f"active_ranks={active_ranks.detach().cpu().tolist()} ok",
        flush=True,
    )


def _run_stage(stage: str, rank: int, world_size: int, local_rank: int, local_world_size: int, group):
    if stage == "pg_init":
        _stage_pg_init(rank, world_size, local_rank, local_world_size)
    elif stage == "pg_collective":
        _stage_pg_collective(rank, world_size)
    elif stage == "ep_buffer":
        _stage_ep_buffer(rank, world_size, group)
    elif stage == "ep_dispatch_send_only_bf16":
        _stage_ep_dispatch_send_only(rank, world_size, group)
    elif stage == "ep_dispatch_bf16":
        _stage_ep_dispatch(rank, world_size, group, use_fp8=False)
    elif stage == "ep_dispatch_async_repeat_bf16":
        _stage_ep_dispatch_async_repeat(rank, world_size, group)
    elif stage == "ep_dispatch_fp8":
        _stage_ep_dispatch(rank, world_size, group, use_fp8=True)
    elif stage == "ep_combine":
        _stage_ep_combine(rank, world_size, group, update_member=False)
    elif stage == "ep_update_member":
        _stage_ep_combine(rank, world_size, group, update_member=True)
    elif stage == "ep_recovery_lifecycle":
        _stage_ep_combine(
            rank,
            world_size,
            group,
            update_member=False,
            replace_rank_one=True,
        )
    else:
        raise ValueError(f"Unsupported stage: {stage}")
    dist.barrier()


def _worker(local_rank: int, args):
    rank, world_size, local_rank, local_world_size, group = _init_dist(
        local_rank, args.timeout
    )
    try:
        if args.stage == "all":
            for stage in STAGES:
                if stage != "all":
                    _run_stage(stage, rank, world_size, local_rank, local_world_size, group)
        else:
            _run_stage(args.stage, rank, world_size, local_rank, local_world_size, group)
    finally:
        if dist.is_initialized():
            dist.destroy_process_group()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mooncake EP multinode staged correctness test"
    )
    parser.add_argument("--stage", choices=STAGES, required=True)
    parser.add_argument(
        "--timeout",
        type=int,
        default=int(os.getenv("MOONCAKE_EP_STAGE_TIMEOUT", DEFAULT_TIMEOUT_S)),
    )
    args = parser.parse_args()

    local_world_size = _env_int("LOCAL_WORLD_SIZE")
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA/HIP device is required")
    if torch.cuda.device_count() < local_world_size:
        raise RuntimeError(
            f"Need LOCAL_WORLD_SIZE={local_world_size} visible GPUs, "
            f"got {torch.cuda.device_count()}"
        )
    mp.spawn(_worker, args=(args,), nprocs=local_world_size, join=True)


if __name__ == "__main__":
    main()
