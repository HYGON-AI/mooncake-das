#pragma once

#define NUM_MAX_NVL_PEERS 8
#define NUM_MAX_RDMA_PEERS 20
#define NUM_MAX_FIFO_SLOTS 32768
#define NUM_WORKSPACE_BYTES (32 * 1024 * 1024)
#define NUM_MAX_LOCAL_EXPERTS 1024
#define NUM_BUFFER_ALIGNMENT_BYTES 128

#define FINISHED_SUM_TAG 1024
#define NUM_CPU_TIMEOUT_SECS 100
#define NUM_TIMEOUT_CYCLES 200000000000ull  // 200G cycles ~= 100s
#define NUM_WAIT_NANOSECONDS 500

#define LOW_LATENCY_SEND_PHASE 1
#define LOW_LATENCY_RECV_PHASE 2
#define EP_SIGNAL_PAD_INTS 4
#define EP_SIGNAL_HEAD_PADDING_BYTES (2 * 1024 * 1024)

#include <hip/hip_bfloat16.h>
#include <hip/hip_fp8.h>
#include <hip/hip_runtime.h>

static constexpr int kWarpSize = 64;
static constexpr int kEmulatedWarpSize = kWarpSize / 2;
static constexpr uint64_t kFullWarpMask = 0xffffffffffffffffull;
#include <infiniband/mlx5dv.h>
