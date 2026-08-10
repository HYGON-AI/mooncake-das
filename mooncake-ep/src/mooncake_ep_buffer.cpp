#include <mooncake_ep_buffer.h>
#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>
#include <glog/logging.h>

namespace mooncake {

// Check if IPv6 address is an IPv4-mapped address (::ffff:x.x.x.x)
static inline bool ipv6_addr_v4mapped(const struct in6_addr* a) {
    return ((a->s6_addr32[0] | a->s6_addr32[1]) == 0 &&
            a->s6_addr32[2] == htonl(0x0000ffff));
}

// Dynamically find the best GID index (RoCE v2 + IPv4-mapped address, or IB)
// Returns GID index on success, -1 on failure
static int findBestGidIndex(ibv_context* ctx, uint8_t port,
                            ibv_port_attr& port_attr) {
    for (int i = 0; i < port_attr.gid_tbl_len; i++) {
        ibv_gid_entry gid_entry;
        int ret = ibv_query_gid_ex(ctx, port, i, &gid_entry, 0);
        if (ret) {
            continue;
        }

        bool is_v4mapped = ipv6_addr_v4mapped(
            reinterpret_cast<const struct in6_addr*>(gid_entry.gid.raw));

        // Look for IPv4-mapped address + RoCE v2, or IB type
        if ((is_v4mapped && gid_entry.gid_type == IBV_GID_TYPE_ROCE_V2) ||
            gid_entry.gid_type == IBV_GID_TYPE_IB) {
            return i;
        }
    }
    return -1;
}

// Returns -1 when unset, -2 when explicitly set to an invalid value.
static int getForcedGidIndex(const ibv_port_attr& port_attr) {
    const char* env = std::getenv("MC_GID_INDEX");
    if (env == nullptr || env[0] == '\0') {
        return -1;
    }

    char* end = nullptr;
    long value = std::strtol(env, &end, 10);
    if (*end != '\0' || value < 0 || value >= port_attr.gid_tbl_len) {
        LOG(ERROR) << "[EP] Invalid MC_GID_INDEX='" << env
                   << "', expected range [0, " << port_attr.gid_tbl_len
                   << ")";
        return -2;
    }
    return static_cast<int>(value);
}

static std::string gidToString(const ibv_gid& gid) {
    char buf[INET6_ADDRSTRLEN] = {};
    if (inet_ntop(AF_INET6, gid.raw, buf, sizeof(buf)) == nullptr) {
        return "<invalid-gid>";
    }
    return std::string(buf);
}

MooncakeEpBuffer::MooncakeEpBuffer(int rank, int num_ranks,
                                   int64_t num_ep_buffer_bytes,
                                   std::string device_name)
    : rank(rank),
      num_ranks(num_ranks),
      num_ep_buffer_bytes(num_ep_buffer_bytes),
      device_name(std::move(device_name)),
      comm_stream(c10::hip::getStreamFromPoolMasqueradingAsCUDA(true)) {
    USE_QP_COUNT = MAX_QP_COUNT / num_ranks * num_ranks;
    // Get ranks
    HIP_CHECK(hipGetDevice(&device_id));
    LOG(INFO) << "[EP] Rank " << rank << " starts buffer initialization on GPU "
              << device_id << " with HCA '" << this->device_name << "'";
    HIP_CHECK(hipDeviceGetAttribute(&wall_clock_rate_khz,
                                    hipDeviceAttributeWallClockRate, device_id));

    HIP_CHECK(hipExtMallocWithFlags(&gdr_buffer, num_ep_buffer_bytes,
                                    hipDeviceMallocFinegrained));

    hipDeviceProp_t device_props{};
    HIP_CHECK(hipGetDeviceProperties(&device_props, device_id));
    gpu_hdp_reg = device_props.hdpMemFlushCntl;
    LOG(INFO) << "[EP] Rank " << rank << " allocated " << num_ep_buffer_bytes
              << " bytes for the finegrained EP data buffer";
    HIP_CHECK(hipMalloc(&raddrs, num_ranks * sizeof(uint64_t)));
    HIP_CHECK(hipMalloc(&rkeys, num_ranks * sizeof(uint32_t)));
    HIP_CHECK(
        hipMalloc(&qp_devctxs, USE_QP_COUNT * sizeof(mlx5gda_qp_devctx)));

    // Allocate NVLink P2P arrays
    HIP_CHECK(hipMalloc(&nvlink_available, num_ranks * sizeof(int32_t)));
    HIP_CHECK(hipMemset(nvlink_available, 0, num_ranks * sizeof(int32_t)));
    HIP_CHECK(hipHostMalloc(&ipc_peer_ptrs_host, num_ranks * sizeof(void*)));
    HIP_CHECK(hipMalloc(&ipc_peer_ptrs, num_ranks * sizeof(void*)));
    for (int i = 0; i < num_ranks; ++i) {
        ipc_peer_ptrs_host[i] = nullptr;
    }
    HIP_CHECK(hipMemset(ipc_peer_ptrs, 0, num_ranks * sizeof(void*)));

    int ret = init_ibgda();
    LOG(INFO) << "[EP] Rank " << rank << " completed IBGDA initialization with "
              << "status " << ret;
    if (ret != 0) {
        ibgda_disabled_ = true;
    }

    // Create 32 MiB workspace
    HIP_CHECK(hipMalloc(&workspace, NUM_WORKSPACE_BYTES));
    HIP_CHECK(hipMemsetAsync(workspace, 0, NUM_WORKSPACE_BYTES, comm_stream));
    HIP_CHECK(hipStreamSynchronize(comm_stream.stream()));
}

MooncakeEpBuffer::~MooncakeEpBuffer() noexcept(false) {
    cleanup_ibgda();
    hipFree(gdr_buffer);
    hipFree(raddrs);
    hipFree(rkeys);
    hipFree(qp_devctxs);
    if (nvlink_available) hipFree(nvlink_available);
    if (ipc_peer_ptrs) hipFree(ipc_peer_ptrs);
    if (ipc_peer_ptrs_host) {
        // Close IPC handles
        for (int i = 0; i < num_ranks; ++i) {
            if (ipc_peer_ptrs_host[i] != nullptr &&
                ipc_peer_ptrs_host[i] != gdr_buffer) {
                hipIpcCloseMemHandle(ipc_peer_ptrs_host[i]);
            }
        }
        hipHostFree(ipc_peer_ptrs_host);
    }
}

void MooncakeEpBuffer::cleanup_ibgda() {
    for (auto* qp : qps) {
        if (qp != nullptr && ctrl_buf_heap != nullptr) {
            mlx5gda_destroy_qp(ctrl_buf_heap, qp);
        }
    }
    qps.clear();
    if (ctrl_buf_heap != nullptr) {
        memheap_destroy(ctrl_buf_heap);
        ctrl_buf_heap = nullptr;
    }
    if (ctrl_buf_umem != nullptr) {
        mlx5dv_devx_umem_dereg(ctrl_buf_umem);
        ctrl_buf_umem = nullptr;
    }
    if (mr != nullptr) {
        ibv_dereg_mr(mr);
        mr = nullptr;
    }
    if (pd != nullptr) {
        ibv_dealloc_pd(pd);
        pd = nullptr;
    }
    if (ib_ctx != nullptr) {
        ibv_close_device(ib_ctx);
        ib_ctx = nullptr;
    }
    if (ctrl_buf_registered) {
        hipHostUnregister(ctrl_buf_host);
        ctrl_buf_registered = false;
    }
    if (ctrl_buf_host != nullptr) {
        free(ctrl_buf_host);
        ctrl_buf_host = nullptr;
        ctrl_buf_device = nullptr;
    }
}

std::tuple<torch::Tensor, std::optional<torch::Tensor>, torch::Tensor,
           torch::Tensor, torch::Tensor, std::optional<EventHandle>,
           std::optional<std::function<void()>>>
MooncakeEpBuffer::dispatch(const torch::Tensor& x,
                           const torch::Tensor& topk_idx,
                           torch::Tensor& active_ranks,
                           int num_max_dispatch_tokens_per_rank,
                           int num_experts, int timeout_us, bool use_fp8,
                           bool async, bool return_recv_hook) {
    // Tensor checks
    // By default using `ptp128c` FP8 cast
    EP_HOST_ASSERT(x.dim() == 2 and x.is_contiguous() and
                   x.scalar_type() == torch::kBFloat16);
    EP_HOST_ASSERT(x.size(1) % sizeof(int4) == 0 and x.size(1) % 128 == 0);
    EP_HOST_ASSERT(topk_idx.dim() == 2 and topk_idx.is_contiguous());
    EP_HOST_ASSERT(x.size(0) == topk_idx.size(0) and
                   x.size(0) <= num_max_dispatch_tokens_per_rank);
    EP_HOST_ASSERT(topk_idx.scalar_type() == torch::kInt64);
    EP_HOST_ASSERT(num_experts % num_ranks == 0);
    EP_HOST_ASSERT(USE_QP_COUNT % num_ranks == 0);

    auto num_tokens = static_cast<int>(x.size(0)),
         hidden = static_cast<int>(x.size(1));
    auto num_scales = hidden / 128,
         num_topk = static_cast<int>(topk_idx.size(1));
    int num_local_experts = num_experts / num_ranks;

    // Buffer control
    BufferPair layout(gdr_buffer, num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks, num_experts);
    EP_HOST_ASSERT(layout.total_bytes <= num_ep_buffer_bytes);
    auto buffer = layout.buffers[buffer_idx];
    auto next_buffer = layout.buffers[buffer_idx ^= 1];

    // Wait previous tasks to be finished
    // NOTES: the hook mode will always use the default stream
    auto compute_stream = c10::hip::getCurrentHIPStreamMasqueradingAsCUDA();
    auto launch_stream = return_recv_hook ? compute_stream : comm_stream;
    EP_HOST_ASSERT(not(async and return_recv_hook));
    if (not return_recv_hook) stream_wait(launch_stream, compute_stream);

    // Allocate packed tensors
    auto packed_recv_x = torch::empty(
        {num_local_experts, num_ranks * num_max_dispatch_tokens_per_rank,
         hidden},
        x.options().dtype(use_fp8 ? torch::kFloat8_e4m3fn : torch::kBFloat16));
    auto packed_recv_src_info = torch::empty(
        {num_local_experts, num_ranks * num_max_dispatch_tokens_per_rank},
        torch::dtype(torch::kInt32).device(torch::kCUDA));
    auto packed_recv_layout_range =
        torch::empty({num_local_experts, num_ranks},
                     torch::dtype(torch::kInt64).device(torch::kCUDA));
    auto packed_recv_count = torch::zeros(
        {num_local_experts}, torch::dtype(torch::kInt32).device(torch::kCUDA));

    // Allocate column-majored scales
    auto packed_recv_x_scales = std::optional<torch::Tensor>();
    float* packed_recv_x_scales_ptr = nullptr;
    if (use_fp8) {
        EP_HOST_ASSERT((num_ranks * num_max_dispatch_tokens_per_rank) % 4 ==
                           0 and
                       "TMA requires the number of tokens to be multiple of 4");
        packed_recv_x_scales =
            torch::empty({num_local_experts, num_scales,
                          num_ranks * num_max_dispatch_tokens_per_rank},
                         torch::dtype(torch::kFloat32).device(torch::kCUDA));
        packed_recv_x_scales =
            torch::transpose(packed_recv_x_scales.value(), 1, 2);
        packed_recv_x_scales_ptr = packed_recv_x_scales->data_ptr<float>();
    }

    int64_t timeout_ticks =
        timeout_us == -1
            ? -1
            : (int64_t)wall_clock_rate_khz * (int64_t)timeout_us / 1000;

    auto launcher = [=](int phases) {
        mooncake::dispatch(
            packed_recv_x.data_ptr(), packed_recv_x_scales_ptr,
            packed_recv_src_info.data_ptr<int>(),
            packed_recv_layout_range.data_ptr<int64_t>(),
            packed_recv_count.data_ptr<int>(), active_ranks.data_ptr<int32_t>(),
            gdr_buffer, buffer.rdma_send_signal_buffer,
            buffer.rdma_recv_signal_buffer, buffer.rdma_send_data_buffer,
            buffer.rdma_recv_data_buffer, nullptr, nullptr, raddrs, rkeys,
            qp_devctxs, nvlink_available, ipc_peer_ptrs, x.data_ptr(),
            topk_idx.data_ptr<int64_t>(), next_buffer.rdma_recv_signal_buffer,
            num_tokens, hidden, num_max_dispatch_tokens_per_rank, num_topk,
            num_experts, rank, num_ranks, use_fp8, workspace, launch_stream,
            gpu_hdp_reg, timeout_ticks, phases);
    };
    launcher(return_recv_hook
                 ? LOW_LATENCY_SEND_PHASE
                 : (LOW_LATENCY_SEND_PHASE | LOW_LATENCY_RECV_PHASE));

    // Wait streams
    std::optional<EventHandle> event;
    if (async) {
        // NOTES: we must ensure the all tensors will not be deallocated
        // before the stream-wait happens, so in Python API, we must wrap
        // all tensors into the event handle.
        event = EventHandle(launch_stream);
    } else if (not return_recv_hook) {
        stream_wait(compute_stream, launch_stream);
    }

    // Receiver callback
    std::optional<std::function<void()>> recv_hook = std::nullopt;
    if (return_recv_hook)
        recv_hook = [=]() { launcher(LOW_LATENCY_RECV_PHASE); };

    // Return values
    return {packed_recv_x,
            packed_recv_x_scales,
            packed_recv_count,
            packed_recv_src_info,
            packed_recv_layout_range,
            event,
            recv_hook};
}

std::tuple<torch::Tensor, std::optional<EventHandle>,
           std::optional<std::function<void()>>>
MooncakeEpBuffer::combine(const torch::Tensor& x, const torch::Tensor& topk_idx,
                          const torch::Tensor& topk_weights,
                          const torch::Tensor& src_info,
                          const torch::Tensor& layout_range,
                          torch::Tensor& active_ranks,
                          int num_max_dispatch_tokens_per_rank, int num_experts,
                          int timeout_us, bool zero_copy, bool async,
                          bool return_recv_hook,
                          const std::optional<torch::Tensor>& out) {
    // Tensor checks
    EP_HOST_ASSERT(x.dim() == 3 and x.is_contiguous() and
                   x.scalar_type() == torch::kBFloat16);
    EP_HOST_ASSERT(x.size(0) == num_experts / num_ranks);
    EP_HOST_ASSERT(x.size(1) == num_ranks * num_max_dispatch_tokens_per_rank);
    EP_HOST_ASSERT(x.size(2) % sizeof(int4) == 0 and x.size(2) % 128 == 0);
    EP_HOST_ASSERT(topk_idx.dim() == 2 and topk_idx.is_contiguous());
    EP_HOST_ASSERT(topk_idx.size(0) == topk_weights.size(0) and
                   topk_idx.size(1) == topk_weights.size(1));
    EP_HOST_ASSERT(topk_idx.scalar_type() == torch::kInt64);
    EP_HOST_ASSERT(topk_weights.dim() == 2 and topk_weights.is_contiguous());
    EP_HOST_ASSERT(topk_weights.size(0) <= num_max_dispatch_tokens_per_rank);
    EP_HOST_ASSERT(topk_weights.scalar_type() == torch::kFloat32);
    EP_HOST_ASSERT(src_info.dim() == 2 and src_info.is_contiguous());
    EP_HOST_ASSERT(src_info.scalar_type() == torch::kInt32 and
                   x.size(0) == src_info.size(0));
    EP_HOST_ASSERT(layout_range.dim() == 2 and layout_range.is_contiguous());
    EP_HOST_ASSERT(layout_range.scalar_type() == torch::kInt64);
    EP_HOST_ASSERT(layout_range.size(0) == num_experts / num_ranks and
                   layout_range.size(1) == num_ranks);
    auto hidden = static_cast<int>(x.size(2));
    auto num_local_experts = num_experts / num_ranks,
         num_topk = static_cast<int>(topk_weights.size(1));
    auto num_combined_tokens = static_cast<int>(topk_weights.size(0));

    // Buffer control
    BufferPair layout(gdr_buffer, num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks, num_experts);
    EP_HOST_ASSERT(layout.total_bytes <= num_ep_buffer_bytes);
    auto buffer = layout.buffers[buffer_idx];
    auto next_buffer = layout.buffers[buffer_idx ^= 1];

    // Wait previous tasks to be finished
    // NOTES: the hook mode will always use the default stream
    auto compute_stream = c10::hip::getCurrentHIPStreamMasqueradingAsCUDA();
    auto launch_stream = return_recv_hook ? compute_stream : comm_stream;
    EP_HOST_ASSERT(not(async and return_recv_hook));
    if (not return_recv_hook) stream_wait(launch_stream, compute_stream);

    // Allocate output tensor
    torch::Tensor combined_x;
    if (out.has_value()) {
        EP_HOST_ASSERT(out->dim() == 2 and out->is_contiguous());
        EP_HOST_ASSERT(out->size(0) == num_combined_tokens and
                       out->size(1) == hidden);
        EP_HOST_ASSERT(out->scalar_type() == x.scalar_type());
        combined_x = out.value();
    } else {
        combined_x = torch::empty({num_combined_tokens, hidden}, x.options());
    }

    int64_t timeout_ticks =
        timeout_us == -1
            ? -1
            : (int64_t)wall_clock_rate_khz * (int64_t)timeout_us / 1000;

    // Kernel launch
    auto launcher = [=](int phases) {
        mooncake::combine(
            combined_x.data_ptr(), active_ranks.data_ptr<int32_t>(), gdr_buffer,
            buffer.rdma_send_signal_buffer, buffer.rdma_recv_signal_buffer,
            buffer.rdma_send_data_buffer, buffer.rdma_recv_data_buffer, nullptr,
            nullptr, raddrs, rkeys, qp_devctxs, nvlink_available, ipc_peer_ptrs,
            x.data_ptr(), topk_idx.data_ptr<int64_t>(),
            topk_weights.data_ptr<float>(), src_info.data_ptr<int>(),
            layout_range.data_ptr<int64_t>(),
            next_buffer.rdma_recv_signal_buffer, num_combined_tokens, hidden,
            num_max_dispatch_tokens_per_rank, num_topk, num_experts, rank,
            num_ranks, workspace, launch_stream, gpu_hdp_reg, timeout_ticks, phases,
            zero_copy);
    };
    launcher(return_recv_hook
                 ? LOW_LATENCY_SEND_PHASE
                 : (LOW_LATENCY_SEND_PHASE | LOW_LATENCY_RECV_PHASE));

    // Wait streams
    std::optional<EventHandle> event;
    if (async) {
        // NOTES: we must ensure the all tensors will not be deallocated
        // before the stream-wait happens, so in Python API, we must wrap
        // all tensors into the event handle.
        event = EventHandle(launch_stream);
    } else if (not return_recv_hook) {
        stream_wait(compute_stream, launch_stream);
    }

    // Receiver callback
    std::optional<std::function<void()>> recv_hook = std::nullopt;
    if (return_recv_hook)
        recv_hook = [=]() { launcher(LOW_LATENCY_RECV_PHASE); };

    // Return values
    return {combined_x, event, recv_hook};
}

torch::Tensor MooncakeEpBuffer::get_next_combine_buffer(
    int num_max_dispatch_tokens_per_rank, int hidden, int num_experts) {
    BufferPair layout(gdr_buffer, num_max_dispatch_tokens_per_rank, hidden,
                      num_ranks, num_experts);

    auto buffer = layout.buffers[buffer_idx];
    auto dtype = torch::kBFloat16;
    size_t num_bytes_per_combine_msg = hidden * sizeof(hip_bfloat16);
    auto num_msg_elems = static_cast<int>(num_bytes_per_combine_msg /
                                          elementSize(torch::kBFloat16));

    EP_HOST_ASSERT(num_bytes_per_combine_msg % elementSize(torch::kBFloat16) ==
                   0);
    return torch::from_blob(
        buffer.rdma_send_data_buffer,
        {num_experts / num_ranks, num_ranks * num_max_dispatch_tokens_per_rank,
         hidden},
        {num_ranks * num_max_dispatch_tokens_per_rank * num_msg_elems,
         num_msg_elems, 1},
        torch::TensorOptions().dtype(dtype).device(torch::kCUDA));
}

int MooncakeEpBuffer::init_ibgda() {
    int num_devices;
    ibv_device** dev_list = ibv_get_device_list(&num_devices);
    if (device_name.empty() || dev_list == nullptr || num_devices == 0) {
        LOG(WARNING) << "[EP] No RDMA device selected or available; disabling "
                        "IBGDA and continuing with local HIP IPC";
        if (dev_list != nullptr && num_devices > 0) {
            ibv_free_device_list(dev_list);
        }
        return -1;
    }
    int nic_id = -1;
    for (int i = 0; i < num_devices; ++i) {
        const char* name = ibv_get_device_name(dev_list[i]);
        if (name && device_name == name) {
            nic_id = i;
            break;
        }
    }
    if (nic_id == -1) {
        LOG(WARNING) << "[EP] RDMA device '" << device_name
                     << "' not found; disabling IBGDA and continuing with "
                        "local HIP IPC";
        ibv_free_device_list(dev_list);
        return -1;
    }
    LOG(INFO) << "[EP] GPU " << device_id << " uses NIC " << nic_id
              << " out of " << num_devices << " NIC(s)";
    ib_ctx = ibv_open_device(dev_list[nic_id]);
    if (!ib_ctx) {
        perror("Failed to open device");
        return -1;
    }

    // Query port attributes to get GID table length
    ibv_port_attr port_attr;
    const uint8_t port_num = 1;
    if (ibv_query_port(ib_ctx, port_num, &port_attr)) {
        perror("Failed to query port");
        cleanup_ibgda();
        return -1;
    }

    // Prefer an explicit runtime GID index when provided; otherwise keep the
    // dynamic RoCE/IB selection used by the EP path.
    gid_index_ = getForcedGidIndex(port_attr);
    if (gid_index_ == -1) {
        gid_index_ = findBestGidIndex(ib_ctx, port_num, port_attr);
    }
    if (gid_index_ < 0) {
        LOG(ERROR) << "[EP] Failed to find a suitable GID index on "
                   << device_name;
        cleanup_ibgda();
        return -1;
    }

    if (ibv_query_gid(ib_ctx, port_num, gid_index_, &gid)) {
        perror("Failed to query gid");
        cleanup_ibgda();
        return -1;
    }
    ibv_free_device_list(dev_list);

    pd = ibv_alloc_pd(ib_ctx);
    if (!pd) {
        perror("Failed to allocate protection domain");
        cleanup_ibgda();
        return -1;
    }
    mlx5dv_obj dv_obj = {};
    dv_obj.pd.in = pd;
    dv_obj.pd.out = &mpd;
    if (mlx5dv_init_obj(&dv_obj, MLX5DV_OBJ_PD)) {
        perror("Failed to initialize mlx5dv object");
        cleanup_ibgda();
        return -1;
    }
    mr = ibv_reg_mr(pd, gdr_buffer, num_ep_buffer_bytes,
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
    if (!mr) {
        perror("Failed to reg mr");
        cleanup_ibgda();
        return -1;
    }

    // DEVX consumes the CPU VA while HIP kernels consume the mapped GPU VA.
    // Both addresses refer to the same page-aligned control allocation.
    if (posix_memalign(&ctrl_buf_host, 4096, CTRL_BUF_SIZE) != 0) {
        perror("Failed to allocate control buffer");
        cleanup_ibgda();
        return -1;
    }
    auto hip_status = hipHostRegister(
        ctrl_buf_host, CTRL_BUF_SIZE,
        hipHostRegisterMapped | hipHostRegisterPortable);
    if (hip_status != hipSuccess) {
        LOG(ERROR) << "[EP] Failed to register control buffer: "
                   << hipGetErrorString(hip_status);
        cleanup_ibgda();
        return -1;
    }
    ctrl_buf_registered = true;
    hip_status =
        hipHostGetDevicePointer(&ctrl_buf_device, ctrl_buf_host, 0);
    if (hip_status != hipSuccess) {
        LOG(ERROR) << "[EP] Failed to map control buffer to device: "
                   << hipGetErrorString(hip_status);
        cleanup_ibgda();
        return -1;
    }
    ctrl_buf_umem = mlx5dv_devx_umem_reg(
        ib_ctx, ctrl_buf_host, CTRL_BUF_SIZE, IBV_ACCESS_LOCAL_WRITE);
    if (!ctrl_buf_umem) {
        perror("Failed to register control buffer as umem");
        cleanup_ibgda();
        return -1;
    }
    ctrl_buf_heap = memheap_create(CTRL_BUF_SIZE);
    if (!ctrl_buf_heap) {
        perror("Failed to create memory heap");
        cleanup_ibgda();
        return -1;
    }
    // Individual regions (CQ, DBR) will be initialized as needed via async
    // memset.
    for (int i = 0; i < USE_QP_COUNT; ++i) {
        mlx5gda_qp* qp =
            mlx5gda_create_rc_qp(mpd, ctrl_buf_device, ctrl_buf_umem,
                                 ctrl_buf_heap, pd, 16384, 1,
                                 comm_stream.stream());
        if (!qp) {
            LOG(ERROR) << "[EP] Failed to create QP " << i << "/"
                       << USE_QP_COUNT << ": " << strerror(errno);
            cleanup_ibgda();
            return -1;
        }
        is_roce_ = qp->port_attr.link_layer == IBV_LINK_LAYER_ETHERNET;
        if (mlx5gda_modify_rc_qp_rst2init(qp, 0)) {
            perror("Failed to mlx5gda_modify_rc_qp_rst2init");
            mlx5gda_destroy_qp(ctrl_buf_heap, qp);
            cleanup_ibgda();
            return -1;
        }
        // Ensure all async memset operations are complete before accessing QP
        // structures
        HIP_CHECK(hipStreamSynchronize(comm_stream.stream()));

        mlx5gda_qp_devctx qp_devctx = {
            .qpn = qp->qpn,
            .wqeid_mask = qp->num_wqebb - 1,
            .wq = reinterpret_cast<mlx5gda_wqebb*>(
                static_cast<char*>(ctrl_buf_device) + qp->wq_offset),
            .cq = reinterpret_cast<mlx5_cqe64*>(
                static_cast<char*>(ctrl_buf_device) +
                qp->send_cq->cq_offset),
            .dbr = reinterpret_cast<mlx5gda_wq_dbr*>(
                static_cast<char*>(ctrl_buf_device) + qp->dbr_offset),
            .bf = static_cast<char*>(qp->uar_device_ptr),
        };
        HIP_CHECK(hipMemcpy(
            static_cast<mlx5gda_qp_devctx*>(qp_devctxs) + i, &qp_devctx,
            sizeof(mlx5gda_qp_devctx), hipMemcpyHostToDevice));
        qps.push_back(qp);
    }
    LOG(INFO) << "[EP] IBGDA initialized with " << qps.size()
              << " QPs on " << device_name;
    return 0;
}

void MooncakeEpBuffer::update_local_qpns() {
    for (int i = 0; i < USE_QP_COUNT; ++i) {
        if (qps[i]) {
            mlx5gda_destroy_qp(ctrl_buf_heap, qps[i]);
            qps[i] = nullptr;
        }
    }

    for (int i = 0; i < USE_QP_COUNT; ++i) {
        mlx5gda_qp* qp =
            mlx5gda_create_rc_qp(mpd, ctrl_buf_device, ctrl_buf_umem,
                                 ctrl_buf_heap, pd, 16384, 1,
                                 comm_stream.stream());
        if (!qp) {
            perror("Failed to recreate QP");
            ibgda_disabled_ = true;
            return;
        }
        is_roce_ = qp->port_attr.link_layer == IBV_LINK_LAYER_ETHERNET;
        if (mlx5gda_modify_rc_qp_rst2init(qp, 0)) {
            perror("Failed to mlx5gda_modify_rc_qp_rst2init");
            ibgda_disabled_ = true;
            return;
        }
        // Ensure all async memset operations are complete before accessing QP
        // structures
        HIP_CHECK(hipStreamSynchronize(comm_stream.stream()));

        mlx5gda_qp_devctx qp_devctx = {
            .qpn = qp->qpn,
            .wqeid_mask = qp->num_wqebb - 1,
            .wq = reinterpret_cast<mlx5gda_wqebb*>(
                static_cast<char*>(ctrl_buf_device) + qp->wq_offset),
            .cq = reinterpret_cast<mlx5_cqe64*>(
                static_cast<char*>(ctrl_buf_device) +
                qp->send_cq->cq_offset),
            .dbr = reinterpret_cast<mlx5gda_wq_dbr*>(
                static_cast<char*>(ctrl_buf_device) + qp->dbr_offset),
            .bf = static_cast<char*>(qp->uar_device_ptr),
        };
        HIP_CHECK(hipMemcpy(
            static_cast<mlx5gda_qp_devctx*>(qp_devctxs) + i, &qp_devctx,
            sizeof(mlx5gda_qp_devctx), hipMemcpyHostToDevice));
        qps[i] = qp;
    }
}

void MooncakeEpBuffer::sync_ib(const std::vector<int64_t>& remote_addrs,
                               const std::vector<int32_t>& remote_keys,
                               const std::vector<int32_t>& remote_qpns,
                               const std::vector<int32_t>& remote_lids,
                               const std::vector<int>& active_ranks_mask) {
    for (int i = 0; i < USE_QP_COUNT; ++i) {
        int peer_rank = i * num_ranks / USE_QP_COUNT;
        if (active_ranks_mask[peer_rank] == 0) continue;
        ibv_ah_attr ah_attr = {
            .dlid = (uint16_t)remote_lids[i],
            .port_num = 0,
        };
        if (mlx5gda_modify_rc_qp_init2rtr(
                qps[i], ah_attr, (uint32_t)remote_qpns[i], IBV_MTU_4096)) {
            perror("Failed to mlx5gda_modify_rc_qp_init2rtr");
            exit(1);
        }
        if (mlx5gda_modify_rc_qp_rtr2rts(qps[i])) {
            perror("Failed to mlx5gda_modify_rc_qp_rtr2rts");
            exit(1);
        }
    }
    for (int i = 0; i < num_ranks; ++i) {
        if (active_ranks_mask[i] == 0) continue;
        uint64_t raddr =
            i == rank ? (uint64_t)mr->addr : (uint64_t)remote_addrs[i];
        hipMemcpy(raddrs + i * sizeof(uint64_t), &raddr, sizeof(uint64_t),
                   hipMemcpyHostToDevice);
        uint32_t rkey = i == rank ? mr->lkey : (uint32_t)remote_keys[i];
        hipMemcpy(rkeys + i * sizeof(uint32_t), &rkey, sizeof(uint32_t),
                   hipMemcpyHostToDevice);
    }
}

void MooncakeEpBuffer::sync_roce(const std::vector<int64_t>& remote_addrs,
                                 const std::vector<int32_t>& remote_keys,
                                 const std::vector<int32_t>& remote_qpns,
                                 const std::vector<int64_t>& subnet_prefixes,
                                 const std::vector<int64_t>& interface_ids,
                                 const std::vector<int>& active_ranks_mask) {
    for (int i = 0; i < USE_QP_COUNT; ++i) {
        int peer_rank = i * num_ranks / USE_QP_COUNT;
        if (active_ranks_mask[peer_rank] == 0) continue;
        ibv_gid remote_gid{};
        remote_gid.global.subnet_prefix = subnet_prefixes[peer_rank];
        remote_gid.global.interface_id = interface_ids[peer_rank];
        ibv_ah_attr ah_attr = {};
        ah_attr.is_global = 1;
        ah_attr.grh.dgid = remote_gid;
        ah_attr.grh.sgid_index =
            gid_index_;  // Use dynamically discovered GID index
        ah_attr.grh.hop_limit = 1;
        ah_attr.port_num = 1;
        ah_attr.dlid = qps[i]->port_attr.lid | 0xC000;
        if (mlx5gda_modify_rc_qp_init2rtr(
                qps[i], ah_attr, (uint32_t)remote_qpns[i], IBV_MTU_4096)) {
            LOG(ERROR) << "[EP][connect] init2rtr failed: rank=" << rank
                       << " local_hca=" << device_name
                       << " local_gid_index=" << gid_index_
                       << " qp_index=" << i
                       << " local_qpn=" << qps[i]->qpn
                       << " peer_rank=" << peer_rank
                       << " remote_qpn=" << remote_qpns[i]
                       << " remote_gid=" << gidToString(remote_gid)
                       << " active=" << active_ranks_mask[peer_rank]
                       << " errno=" << errno << " " << strerror(errno);
            exit(1);
        }
        if (mlx5gda_modify_rc_qp_rtr2rts(qps[i])) {
            perror("Failed to mlx5gda_modify_rc_qp_rtr2rts");
            exit(1);
        }
    }
    for (int i = 0; i < num_ranks; ++i) {
        if (active_ranks_mask[i] == 0) continue;
        uint64_t raddr =
            i == rank ? (uint64_t)mr->addr : (uint64_t)remote_addrs[i];
        hipMemcpy(raddrs + i * sizeof(uint64_t), &raddr, sizeof(uint64_t),
                   hipMemcpyHostToDevice);
        uint32_t rkey = i == rank ? mr->lkey : (uint32_t)remote_keys[i];
        hipMemcpy(rkeys + i * sizeof(uint32_t), &rkey, sizeof(uint32_t),
                   hipMemcpyHostToDevice);
    }
}

std::vector<int32_t> MooncakeEpBuffer::get_ipc_handle() {
    hipIpcMemHandle_t handle;
    HIP_CHECK(hipIpcGetMemHandle(&handle, gdr_buffer));
    // Convert handle bytes to int32_t array
    const size_t handle_size = sizeof(hipIpcMemHandle_t);
    const size_t num_int32s =
        (handle_size + sizeof(int32_t) - 1) / sizeof(int32_t);
    std::vector<int32_t> handle_ints(num_int32s);
    memcpy(handle_ints.data(), &handle, handle_size);
    return handle_ints;
}

void MooncakeEpBuffer::sync_nvlink_ipc_handles(
    const std::vector<std::vector<int32_t>>& remote_handles,
    const std::vector<int>& active_ranks_mask) {
    int device_count = 0;
    HIP_CHECK(hipGetDeviceCount(&device_count));

    std::vector<int32_t> nvlink_array(num_ranks, 0);
    nvlink_array[rank] = 1;

    {
        // Use HIP IPC for intra-node P2P.
        int node_id = rank / device_count;
        int group_start = node_id * device_count;
        int group_end = std::min(group_start + device_count, num_ranks);

        for (int dst_rank = group_start; dst_rank < group_end; ++dst_rank) {
            if (active_ranks_mask[dst_rank] == 0) continue;
            if (dst_rank == rank) {
                ipc_peer_ptrs_host[dst_rank] = gdr_buffer;
                continue;
            }

            int dst_device = dst_rank % device_count;
            int can_access_peer = 0;
            hipError_t err = hipDeviceCanAccessPeer(&can_access_peer,
                                                      device_id, dst_device);
            if (err == hipSuccess && can_access_peer) {
                hipError_t peer_err =
                    hipDeviceEnablePeerAccess(dst_device, 0);
                if (peer_err == hipSuccess ||
                    peer_err == hipErrorPeerAccessAlreadyEnabled) {
                    if (peer_err == hipErrorPeerAccessAlreadyEnabled) {
                        hipGetLastError();
                    }
                    nvlink_array[dst_rank] = 1;

                    if (dst_rank >= static_cast<int>(remote_handles.size())) {
                        LOG(WARNING)
                            << "[EP] Rank " << rank
                            << " missing IPC handle for rank " << dst_rank;
                        continue;
                    }

                    const size_t handle_size = sizeof(hipIpcMemHandle_t);
                    const size_t num_int32s =
                        (handle_size + sizeof(int32_t) - 1) / sizeof(int32_t);
                    const auto& handle_ints = remote_handles[dst_rank];
                    if (handle_ints.size() < num_int32s) {
                        LOG(WARNING)
                            << "[EP] Rank " << rank
                            << " invalid IPC handle size for rank " << dst_rank;
                        continue;
                    }

                    hipIpcMemHandle_t remote_handle;
                    memcpy(&remote_handle, handle_ints.data(), handle_size);

                    void* peer_ptr = nullptr;
                    hipError_t ipc_err =
                        hipIpcOpenMemHandle(&peer_ptr, remote_handle,
                                             hipIpcMemLazyEnablePeerAccess);
                    if (ipc_err != hipSuccess) {
                        LOG(WARNING)
                            << "[EP] Rank " << rank
                            << " failed to open IPC handle for rank "
                            << dst_rank << ": " << hipGetErrorString(ipc_err);
                        nvlink_array[dst_rank] = 0;
                    } else {
                        ipc_peer_ptrs_host[dst_rank] = peer_ptr;
                    }
                }
            }
        }

        p2p_ipc_all_enabled_ = true;
        for (int i = 0; i < num_ranks; ++i) {
            if (active_ranks_mask[i] == 0) continue;
            if (nvlink_array[i] == 0 || ipc_peer_ptrs_host[i] == nullptr) {
                p2p_ipc_all_enabled_ = false;
                break;
            }
        }
        if (p2p_ipc_all_enabled_ && num_ranks > 1) {
            int first_node_id = 0 / device_count;
            int last_node_id = (num_ranks - 1) / device_count;
            if (first_node_id != last_node_id) {
                p2p_ipc_all_enabled_ = false;
            }
        }
    }

    HIP_CHECK(hipMemcpy(nvlink_available, nvlink_array.data(),
                          num_ranks * sizeof(int32_t), hipMemcpyHostToDevice));
    HIP_CHECK(hipMemcpy(ipc_peer_ptrs, ipc_peer_ptrs_host,
                          num_ranks * sizeof(void*), hipMemcpyHostToDevice));
}

}  // namespace mooncake
