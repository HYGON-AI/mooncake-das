#include "client_service.h"
#include "storage/distributed/dfs_log_limiter.h"

#include <boost/algorithm/string.hpp>
#include <glog/logging.h>

#include "ascii_string.h"
#include "allocator.h"
#include "segment.h"

#include <csignal>
#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <map>
#include <numeric>
#ifdef USE_NOF
#include <numa.h>
#endif
#include <optional>
#include <ranges>
#include <span>
#include <string_view>
#include <sched.h>
#include <thread>
#include <set>
#include <utility>
#include <vector>
#include <ylt/struct_json/json_reader.h>
#include <boost/asio/ip/address.hpp>

#include "transfer_engine.h"
#include "topology.h"
#include "transfer_task.h"
#include "transport/transport.h"
#include "config.h"
#include "ha/leadership/leader_coordinator_factory.h"
#ifdef MOONCAKE_ENABLE_TEST_FAILPOINTS
#include "ha/oplog/oplog_test_failpoint.h"
#endif
#include "types.h"
#include "client_buffer.h"
#include "utils.h"
#include "rpc_types.h"
#include "local_hot_cache.h"
#include "device/accelerator_registry.h"
#ifdef USE_INTRA_NVLINK
#include "gpu_vendor/intra_nvlink.h"
#endif
#include "crc_checksum.h"
#include "environ.h"
#include "environment_variables.h"
#include "storage/distributed/dfs_io_timeout.h"
#include "storage/distributed/distributed_storage_backend.h"


namespace mooncake {

std::optional<size_t> GetTransportRegistrationLimit(
    const std::string& protocol) {
    if (protocol == "efa" || protocol == "cxi") {
        return globalConfig().max_mr_size;
    }
    if (protocol == "ub") {
        return globalConfig().max_seg_size;
    }
    return std::nullopt;
}

namespace {

UUID DfsWriteEpoch(const std::vector<Replica::Descriptor>& replicas) {
    for (const auto& replica : replicas) {
        if (replica.is_dfs_replica()) {
            return replica.get_dfs_descriptor().MasterEpoch();
        }
    }
    return {};
}

template <typename Item, typename Rpc>
std::vector<tl::expected<void, ErrorCode>> FinalizeSessionsByEpoch(
    const std::vector<Item>& items, const std::vector<UUID>& epochs,
    Rpc&& rpc) {
    if (epochs.empty()) return rpc(items, UUID{});
    std::vector<tl::expected<void, ErrorCode>> results(
        items.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    if (epochs.size() != items.size()) return results;
    std::map<UUID, std::vector<size_t>> groups;
    for (size_t i = 0; i < items.size(); ++i) groups[epochs[i]].push_back(i);
    for (const auto& [epoch, indices] : groups) {
        std::vector<Item> group;
        group.reserve(indices.size());
        for (auto i : indices) group.push_back(items[i]);
        auto responses = rpc(group, epoch);
        for (size_t i = 0; i < indices.size(); ++i) {
            results[indices[i]] =
                responses.size() == indices.size()
                    ? std::move(responses[i])
                    : tl::expected<void, ErrorCode>(
                          tl::unexpected(ErrorCode::RPC_FAIL));
        }
    }
    return results;
}

constexpr size_t kObjectChecksumD2HChunkSize = 8 * 1024 * 1024;
constexpr size_t kDefaultDfsWriteChunkSize = 256ULL * 1024 * 1024;
constexpr size_t kDefaultDfsWriteArenaCount = 2;

size_t ReadDfsWriteChunkSize() {
    using Variables = FileStorageEnvironmentVariables;
    const auto configured = Environ::Read(
        Variables::MC_STORE_DFS_WRITE_CHUNK_SIZE_BYTES);
    if (!configured.has_value()) return kDefaultDfsWriteChunkSize;
    return *configured > 0 ? static_cast<size_t>(*configured) : 0;
}

size_t ReadDfsWriteArenaCount() {
    const auto configured = Environ::Read(
        FileStorageEnvironmentVariables::MC_STORE_DFS_WRITE_ARENA_COUNT);
    if (!configured.has_value()) return kDefaultDfsWriteArenaCount;
    return *configured > 0 ? static_cast<size_t>(*configured) : 0;
}

std::chrono::milliseconds ReadDfsWriteArenaWait() {
    constexpr int64_t kDefaultWaitMs = 10000;
    const auto configured = Environ::Read(
        FileStorageEnvironmentVariables::MC_STORE_DFS_WRITE_ARENA_WAIT_MS);
    const int64_t wait_ms = configured.value_or(kDefaultWaitMs);
    return std::chrono::milliseconds(std::max<int64_t>(wait_ms, 0));
}

bool IsValidServiceEndpoint(const std::string& endpoint) {
    if (endpoint.empty()) return false;

    std::string host;
    std::string_view port_text;
    if (endpoint.front() == '[') {
        const auto closing_bracket = endpoint.find(']');
        if (closing_bracket == std::string::npos ||
            closing_bracket + 1 >= endpoint.size() ||
            endpoint[closing_bracket + 1] != ':') {
            return false;
        }
        host = endpoint.substr(1, closing_bracket - 1);
        port_text = std::string_view(endpoint).substr(closing_bracket + 2);
        boost::system::error_code error;
        const auto address = boost::asio::ip::make_address(host, error);
        if (error || !address.is_v6() || address.is_unspecified()) {
            return false;
        }
    } else {
        const auto separator = endpoint.rfind(':');
        if (separator == std::string::npos ||
            endpoint.find(':') != separator) {
            return false;
        }
        host = endpoint.substr(0, separator);
        port_text = std::string_view(endpoint).substr(separator + 1);
        if (host.empty() || host == "*") return false;
        boost::system::error_code error;
        const auto address = boost::asio::ip::make_address(host, error);
        if (error || !address.is_v4() || address.is_unspecified()) {
            return false;
        }
    }

    if (port_text.empty()) return false;
    uint32_t port = 0;
    for (unsigned char c : port_text) {
        if (!std::isdigit(c)) return false;
        port = port * 10 + static_cast<uint32_t>(c - '0');
        if (port > 65535) return false;
    }
    return port != 0;
}

class ScopedObjectChecksumBuffer {
   public:
    ScopedObjectChecksumBuffer(PinnedBufferPool& pool, size_t size)
        : pool_(pool), buffer_(pool_.Acquire(size)) {}

    ~ScopedObjectChecksumBuffer() { pool_.Release(std::move(buffer_)); }

    char* data() const { return buffer_.data; }

   private:
    PinnedBufferPool& pool_;
    PinnedBufferPool::Buffer buffer_;
};

#ifdef USE_NOF
std::optional<int> GetConfiguredNumaSocketId() {
    const char* raw_value = std::getenv("MC_STORE_NUMA_SOCKET_ID");
    if (!raw_value || raw_value[0] == '\0') {
        return std::nullopt;
    }

    char* end_ptr = nullptr;
    errno = 0;
    long parsed = std::strtol(raw_value, &end_ptr, 10);
    if (errno != 0 || end_ptr == raw_value ||
        (end_ptr != nullptr && *end_ptr != '\0') || parsed < 0 ||
        parsed > std::numeric_limits<int>::max()) {
        LOG(WARNING) << "Invalid MC_STORE_NUMA_SOCKET_ID=" << raw_value
                     << ", falling back to auto-detect";
        return std::nullopt;
    }

    return static_cast<int>(parsed);
}

int GetCurrentNumaSocketId() {
    if (numa_available() < 0) {
        return 0;
    }
    int cpu = sched_getcpu();
    if (cpu < 0) {
        return 0;
    }
    int node = numa_node_of_cpu(cpu);
    return node < 0 ? 0 : node;
}
#endif

struct ContiguousSliceRange {
    void* ptr = nullptr;
    size_t size = 0;
};

std::optional<ContiguousSliceRange> GetContiguousSliceRange(
    std::span<const Slice> slices) {
    if (slices.empty()) {
        return std::nullopt;
    }

    uintptr_t expected_ptr = 0;
    uintptr_t start_ptr = 0;
    size_t total_size = 0;
    for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices[i];
        if (slice.ptr == nullptr) {
            return std::nullopt;
        }

        const auto current_ptr = reinterpret_cast<uintptr_t>(slice.ptr);
        if (i == 0) {
            start_ptr = current_ptr;
            expected_ptr = current_ptr;
        } else if (current_ptr != expected_ptr) {
            return std::nullopt;
        }

        if (slice.size > std::numeric_limits<size_t>::max() - total_size) {
            return std::nullopt;
        }
        if (slice.size > std::numeric_limits<uintptr_t>::max() - current_ptr) {
            return std::nullopt;
        }

        total_size += slice.size;
        expected_ptr = current_ptr + slice.size;
    }

    if (total_size == 0) {
        return std::nullopt;
    }

    return ContiguousSliceRange{.ptr = reinterpret_cast<void*>(start_ptr),
                                .size = total_size};
}

ErrorCode ScatterFragmentError(const Status& status) {
    return status.IsInvalidArgument() ? ErrorCode::INVALID_PARAMS
                                      : ErrorCode::TRANSFER_FAIL;
}

// Collects the fragments of many ranged entries into a single scatter submit.
//
// One submit per entry would hand the transport one tiny transfer per key per
// layer; batching them lets the transport coalesce every fragment bound for the
// same segment into one transfer, which is what keeps layer-wise sessions at
// full bandwidth.
//
// ScatterTransferRange holds non-owning spans, so the offset/length storage
// lives here and must outlive the submit. Both vectors are reserved to the
// exact fragment count up front so pushes never reallocate under a live span.
class ScatterRangeBuilder {
   public:
    explicit ScatterRangeBuilder(size_t fragment_count)
        : zero_offsets_(fragment_count, 0) {
        remote_offsets_.reserve(fragment_count);
        lengths_.reserve(fragment_count);
        ranges_.reserve(fragment_count);
    }

    // `error_slot` is shared by every fragment of one entry and keeps the first
    // failure. Callbacks run on the waiting thread, so no locking is needed.
    void Add(TransferRequest::OpCode opcode,
             const AllocatedBuffer::Descriptor& handle, const Slice& slice,
             uint64_t remote_offset, std::optional<ErrorCode>* error_slot) {
        const size_t index = remote_offsets_.size();
        remote_offsets_.push_back(static_cast<size_t>(remote_offset));
        lengths_.push_back(slice.size);
        ranges_.push_back(TransferEngine::ScatterTransferRange{
            .opcode = opcode,
            .remote_segment = handle.transport_endpoint_,
            .remote_base_offset = handle.buffer_address_,
            .remote_size = static_cast<size_t>(handle.size_),
            .local_buffer = slice.ptr,
            .local_capacity = slice.size,
            .local_offsets = std::span<const size_t>(&zero_offsets_[index], 1),
            .remote_offsets =
                std::span<const size_t>(&remote_offsets_[index], 1),
            .lengths = std::span<const size_t>(&lengths_[index], 1),
            .on_fragment_complete =
                [error_slot](size_t, const Status& status) {
                    if (!status.ok() && !error_slot->has_value()) {
                        *error_slot = ScatterFragmentError(status);
                    }
                },
        });
    }

    bool empty() const { return ranges_.empty(); }

    const std::vector<TransferEngine::ScatterTransferRange>& ranges() const {
        return ranges_;
    }

   private:
    std::vector<size_t> zero_offsets_;
    std::vector<size_t> remote_offsets_;
    std::vector<size_t> lengths_;
    std::vector<TransferEngine::ScatterTransferRange> ranges_;
};

struct ReplicaTransferSummary {
    size_t allocated_memory_replicas = 0;
    size_t allocated_nof_replicas = 0;
    size_t allocated_dfs_replicas = 0;
    size_t successful_memory_transfers = 0;
    size_t successful_nof_transfers = 0;
    size_t successful_dfs_transfers = 0;
    size_t failed_memory_transfers = 0;
    size_t failed_nof_transfers = 0;
    size_t failed_dfs_transfers = 0;
    ErrorCode first_error = ErrorCode::OK;
    // Set when the DFS replica's PutEnd/PutRevoke has been handed to the
    // background write task (BUCKET mode). FinalizeBatchPut must then leave the
    // DFS replica alone: sending its own End would publish data that has not
    // been written yet, and a Revoke would fight the task's completion.
    bool dfs_completion_deferred = false;

    void RecordAllocatedReplica(const Replica::Descriptor& replica) {
        if (replica.is_memory_replica()) {
            ++allocated_memory_replicas;
        } else if (replica.is_nof_replica()) {
            ++allocated_nof_replicas;
        } else if (replica.is_dfs_replica()) {
            ++allocated_dfs_replicas;
        }
    }

    void RecordSuccess(ReplicaType replica_type) {
        if (replica_type == ReplicaType::MEMORY) {
            ++successful_memory_transfers;
        } else if (replica_type == ReplicaType::NOF_SSD) {
            ++successful_nof_transfers;
        } else if (replica_type == ReplicaType::DFS) {
            ++successful_dfs_transfers;
        }
    }

    void RecordFailure(ReplicaType replica_type, ErrorCode error) {
        if (replica_type == ReplicaType::MEMORY) {
            ++failed_memory_transfers;
        } else if (replica_type == ReplicaType::NOF_SSD) {
            ++failed_nof_transfers;
        } else if (replica_type == ReplicaType::DFS) {
            ++failed_dfs_transfers;
        }
        if (first_error == ErrorCode::OK) {
            first_error = error;
        }
    }
};

bool NonDfsTransfersSucceeded(const ReplicaTransferSummary& summary) {
    return summary.successful_memory_transfers ==
               summary.allocated_memory_replicas &&
           summary.successful_nof_transfers == summary.allocated_nof_replicas &&
           summary.failed_memory_transfers == 0 &&
           summary.failed_nof_transfers == 0;
}

bool AllAllocatedTransfersSucceeded(const ReplicaTransferSummary& summary) {
    return NonDfsTransfersSucceeded(summary) &&
           summary.successful_dfs_transfers == summary.allocated_dfs_replicas &&
           summary.failed_dfs_transfers == 0;
}

bool HasExpectedReplicaAllocation(const ReplicateConfig& config,
                                  const ReplicaTransferSummary& summary,
                                  bool allow_missing_dfs = false) {
    if (config.nof_replica_num == 0 && config.dfs_replica_num == 0) {
        return summary.allocated_memory_replicas > 0;
    }
    if (DetermineReplicaWriteMode(config) ==
        ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
        return summary.allocated_memory_replicas +
                   summary.allocated_nof_replicas >
               0;
    }
    if (allow_missing_dfs && config.dfs_replica_num == 1 &&
        summary.allocated_dfs_replicas == 0) {
        return summary.allocated_memory_replicas == config.replica_num &&
               summary.allocated_nof_replicas == config.nof_replica_num;
    }
    return summary.allocated_memory_replicas == config.replica_num &&
           summary.allocated_nof_replicas == config.nof_replica_num &&
           summary.allocated_dfs_replicas == config.dfs_replica_num;
}

std::optional<ErrorCode> ValidatePreferSameNodeWriteConfig(
    const ReplicateConfig& config) {
    if (config.nof_replica_num > 0) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported with "
                      "NoF replicas";
        return ErrorCode::INVALID_PARAMS;
    }
    if (config.replica_num != 1) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported with "
                      "replica_num != 1";
        return ErrorCode::INVALID_PARAMS;
    }

    return std::nullopt;
}

// success describes whether the overall put should succeed. Reliable modes
// require all allocated replicas to complete. Flexible dual-replica mode only
// requires one replica type to succeed, so success may be true while
// revoke_type is also set for the failed side.
struct FinalizeDecision {
    std::optional<ReplicaType> end_type;
    std::optional<ReplicaType> revoke_type;
    bool success = false;
    ErrorCode error = ErrorCode::OK;
};

FinalizeDecision DetermineFinalizeDecision(
    const ReplicateConfig& config, const ReplicaTransferSummary& summary,
    bool allow_missing_dfs = false, bool allow_memory_only_fallback = false) {
    const auto write_mode = DetermineReplicaWriteMode(config);
    const bool allocation_satisfied =
        HasExpectedReplicaAllocation(config, summary, allow_missing_dfs);

    const bool dfs_deferred = summary.dfs_completion_deferred;
    if (dfs_deferred && summary.allocated_memory_replicas > 0 &&
        summary.allocated_nof_replicas > 0) {
        LOG(ERROR) << "Deferred DFS completion combined with both MEMORY and "
                      "NoF replicas is not supported";
    }
    // Preserve a completed local tier when an allocated DFS write fails on any
    // backend. Missing DFS allocation is degradable only for callers that
    // explicitly enable the bucket-mode memory-only fallback.
    const bool best_effort_dfs_failure =
        allow_missing_dfs && config.dfs_replica_num == 1 && !dfs_deferred &&
        NonDfsTransfersSucceeded(summary) &&
        ((allow_memory_only_fallback &&
          summary.allocated_dfs_replicas == 0) ||
         (summary.allocated_dfs_replicas == 1 &&
          summary.successful_dfs_transfers == 0 &&
          summary.failed_dfs_transfers > 0));
    // Return the replica type(s) owned by this finalize call whose transfers
    // succeeded. Deferred DFS completion stays owned by the background task.
    const auto successful_type = [&]() -> std::optional<ReplicaType> {
        const bool memory_succeeded =
            summary.allocated_memory_replicas > 0 &&
            summary.successful_memory_transfers ==
                summary.allocated_memory_replicas &&
            summary.failed_memory_transfers == 0;
        const bool nof_succeeded =
            summary.allocated_nof_replicas > 0 &&
            summary.successful_nof_transfers == summary.allocated_nof_replicas &&
            summary.failed_nof_transfers == 0;
        const bool dfs_succeeded =
            !dfs_deferred && summary.allocated_dfs_replicas > 0 &&
            summary.successful_dfs_transfers == summary.allocated_dfs_replicas &&
            summary.failed_dfs_transfers == 0;
        const size_t successful_types = static_cast<size_t>(memory_succeeded) +
                                        static_cast<size_t>(nof_succeeded) +
                                        static_cast<size_t>(dfs_succeeded);
        if (successful_types == 0) return std::nullopt;
        if (successful_types > 1) return ReplicaType::ALL;
        if (memory_succeeded) return ReplicaType::MEMORY;
        if (nof_succeeded) return ReplicaType::NOF_SSD;
        return ReplicaType::DFS;
    };

    // Return the replica type(s) still owned by this finalize call whose
    // transfers failed. A deferred DFS replica is excluded because the
    // background task owns its eventual End/Revoke.
    const auto failed_type = [&]() -> std::optional<ReplicaType> {
        const bool memory_failed =
            summary.allocated_memory_replicas > 0 &&
            summary.successful_memory_transfers <
                summary.allocated_memory_replicas;
        const bool nof_failed =
            summary.allocated_nof_replicas > 0 &&
            summary.successful_nof_transfers < summary.allocated_nof_replicas;
        const bool dfs_failed =
            !dfs_deferred && summary.allocated_dfs_replicas > 0 &&
            summary.successful_dfs_transfers < summary.allocated_dfs_replicas;
        const size_t failed_types = static_cast<size_t>(memory_failed) +
                                    static_cast<size_t>(nof_failed) +
                                    static_cast<size_t>(dfs_failed);
        if (failed_types == 0) return std::nullopt;
        if (failed_types > 1) return ReplicaType::ALL;
        if (memory_failed) return ReplicaType::MEMORY;
        if (nof_failed) return ReplicaType::NOF_SSD;
        return ReplicaType::DFS;
    };

    if (write_mode != ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
        const bool all_transfers_succeeded =
            AllAllocatedTransfersSucceeded(summary);
        if (allocation_satisfied && all_transfers_succeeded) {
            return {.end_type = successful_type(),
                    .revoke_type = std::nullopt,
                    .success = true,
                    .error = ErrorCode::OK};
        }
        if (best_effort_dfs_failure) {
            return {.end_type = successful_type(),
                    .revoke_type = failed_type(),
                    .success = true,
                    .error = ErrorCode::OK};
        }
        if (!dfs_deferred) {
            return {.end_type = std::nullopt,
                    .revoke_type = ReplicaType::ALL,
                    .success = false,
                    .error = allocation_satisfied
                                 ? (summary.first_error == ErrorCode::OK
                                        ? ErrorCode::TRANSFER_FAIL
                                        : summary.first_error)
                                 : ErrorCode::NO_AVAILABLE_HANDLE};
        }
        return {.end_type = successful_type(),
                .revoke_type = failed_type(),
                .success = false,
                .error = allocation_satisfied
                             ? (summary.first_error == ErrorCode::OK
                                    ? ErrorCode::TRANSFER_FAIL
                                    : summary.first_error)
                             : ErrorCode::NO_AVAILABLE_HANDLE};
    }

    const bool memory_succeeded = summary.successful_memory_transfers > 0;
    const bool nof_succeeded = summary.successful_nof_transfers > 0;

    if (memory_succeeded && nof_succeeded) {
        return {.end_type = successful_type(),
                .revoke_type = std::nullopt,
                .success = true,
                .error = ErrorCode::OK};
    }
    if (memory_succeeded) {
        return {.end_type = ReplicaType::MEMORY,
                .revoke_type = ReplicaType::NOF_SSD,
                .success = true,
                .error = ErrorCode::OK};
    }
    if (nof_succeeded) {
        return {.end_type = ReplicaType::NOF_SSD,
                .revoke_type = ReplicaType::MEMORY,
                .success = true,
                .error = ErrorCode::OK};
    }

    return {.end_type = successful_type(),
            .revoke_type = failed_type(),
            .success = false,
            .error = summary.first_error == ErrorCode::OK
                         ? ErrorCode::NO_AVAILABLE_HANDLE
                         : summary.first_error};
}

}  // namespace

[[nodiscard]] size_t CalculateSliceSize(const std::vector<Slice>& slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

[[nodiscard]] size_t CalculateSliceSize(std::span<const Slice> slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

Client::Client(const std::string& local_hostname,
               const std::string& metadata_connstring,
               const std::string& protocol,
               const std::map<std::string, std::string>& labels,
               const std::string& tenant_id)
    : client_id_(generate_uuid()),
      metrics_(ClientMetric::Create(merge_labels(labels))),
      master_client_(client_id_,
                     metrics_ ? &metrics_->master_client_metric : nullptr,
                     tenant_id),
      local_hostname_(local_hostname),
      host_id_(ResolveMooncakeHostId(local_hostname)),
      metadata_connstring_(metadata_connstring),
      protocol_(protocol),
      object_checksum_enabled_(Environ::Get().GetStoreChecksumEnabled()),
      dfs_batch_put_timeout_ms_(Environ::GetUInt64(
          "MOONCAKE_DFS_BATCH_PUT_TIMEOUT_MS", 10000)),
      dfs_write_arena_count_(ReadDfsWriteArenaCount()),
      dfs_write_chunk_size_(ReadDfsWriteChunkSize()),
      dfs_write_arena_wait_(ReadDfsWriteArenaWait()),
      pinned_buffer_pool_(
          std::make_shared<PinnedBufferPool>(Environ::GetSizeT(
              "MC_STORE_DFS_PINNED_POOL_BYTES",
              PinnedBufferPool::kDefaultMaxCachedBytes))),
      write_thread_pool_(2),
      task_thread_pool_(4),
      dfs_backfill_thread_pool_(1) {
    LOG(INFO) << "client_id=" << client_id_;
    if (!host_id_.empty()) {
        LOG(INFO) << "client_id=" << client_id_ << ", host_id=" << host_id_;
    }
    if (object_checksum_enabled_) {
        LOG(INFO) << "Object checksum validation is enabled";
    }
    LOG(INFO) << "DFS BatchPut timeout baseline: "
              << dfs_batch_put_timeout_ms_ << " ms (0 disables)";

    if (metrics_) {
        if (metrics_->GetReportingInterval() > 0) {
            LOG(INFO) << "Client metrics enabled with reporting thread started "
                         "(interval: "
                      << metrics_->GetReportingInterval() << "s)";
        } else {
            LOG(INFO)
                << "Client metrics enabled but reporting disabled (interval=0)";
        }
    } else {
        LOG(INFO) << "Client metrics disabled (set MC_STORE_CLIENT_METRIC=1 to "
                     "enable)";
    }
}

int Client::SetServiceEndpoint(const std::string& endpoint) {
    if (!IsValidServiceEndpoint(endpoint)) {
        return toInt(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard<std::mutex> lock(service_metrics_mutex_);
    if (!service_metrics_snapshot_.service_endpoint.empty()) {
        return service_metrics_snapshot_.service_endpoint == endpoint
                   ? 0
                   : toInt(ErrorCode::INVALID_PARAMS);
    }
    service_metrics_snapshot_.service_endpoint = endpoint;
    service_metrics_report_running_.store(true, std::memory_order_release);
    try {
        service_metrics_report_thread_ =
            std::thread(&Client::ServiceMetricsReporterLoop, this);
    } catch (const std::exception& error) {
        service_metrics_report_running_.store(false, std::memory_order_release);
        service_metrics_snapshot_.service_endpoint.clear();
        LOG(ERROR) << "Failed to start service metrics reporter: "
                   << error.what();
        return toInt(ErrorCode::INTERNAL_ERROR);
    }
    return 0;
}

void Client::RecordServiceReadFailure(ReplicaType replica_type) {
    std::lock_guard<std::mutex> lock(service_metrics_mutex_);
    if (service_metrics_snapshot_.service_endpoint.empty()) return;
    if (replica_type == ReplicaType::MEMORY) {
        ++service_metrics_snapshot_.read_failures_memory;
    } else if (replica_type == ReplicaType::DFS) {
        ++service_metrics_snapshot_.read_failures_dfs;
    }
}

void Client::RecordServiceDualWrite(bool memory_succeeded,
                                    bool dfs_succeeded) {
    std::lock_guard<std::mutex> lock(service_metrics_mutex_);
    if (service_metrics_snapshot_.service_endpoint.empty()) return;
    if (memory_succeeded && dfs_succeeded) {
        ++service_metrics_snapshot_.dual_write_both_success;
    } else if (memory_succeeded) {
        ++service_metrics_snapshot_.dual_write_memory_only;
    } else if (dfs_succeeded) {
        ++service_metrics_snapshot_.dual_write_dfs_only;
    } else {
        ++service_metrics_snapshot_.dual_write_neither_success;
    }
}

void Client::BeginServiceDualWritePending() {
    std::lock_guard<std::mutex> lock(service_metrics_mutex_);
    if (!service_metrics_snapshot_.service_endpoint.empty()) {
        ++service_metrics_snapshot_.dual_write_pending;
    }
}

void Client::CancelServiceDualWritePending() {
    std::lock_guard<std::mutex> lock(service_metrics_mutex_);
    if (!service_metrics_snapshot_.service_endpoint.empty() &&
        service_metrics_snapshot_.dual_write_pending > 0) {
        --service_metrics_snapshot_.dual_write_pending;
    }
}

void Client::CompleteServiceDualWrite(bool memory_succeeded,
                                      bool dfs_succeeded) {
    std::lock_guard<std::mutex> lock(service_metrics_mutex_);
    if (service_metrics_snapshot_.service_endpoint.empty()) return;
    if (service_metrics_snapshot_.dual_write_pending > 0) {
        --service_metrics_snapshot_.dual_write_pending;
    }
    if (memory_succeeded && dfs_succeeded) {
        ++service_metrics_snapshot_.dual_write_both_success;
    } else if (memory_succeeded) {
        ++service_metrics_snapshot_.dual_write_memory_only;
    } else if (dfs_succeeded) {
        ++service_metrics_snapshot_.dual_write_dfs_only;
    } else {
        ++service_metrics_snapshot_.dual_write_neither_success;
    }
}

void Client::CompleteAsyncDfsWriteMetrics(
    const std::shared_ptr<AsyncDfsWriteContext>& context,
    const std::vector<ErrorCode>& outcomes) {
    if (context->service_metrics_settled.exchange(true)) return;
    for (size_t i = 0; i < context->service_dual_writes.size(); ++i) {
        const auto& metric = context->service_dual_writes[i];
        if (!metric.eligible) continue;
        const bool dfs_succeeded = i < outcomes.size() &&
                                   outcomes[i] == ErrorCode::OK &&
                                   metric.requested_dfs_replicas == 1;
        CompleteServiceDualWrite(metric.memory_succeeded, dfs_succeeded);
    }
}

void Client::ExpireAsyncDfsWriteMetrics() {
    std::vector<std::shared_ptr<AsyncDfsWriteContext>> expired;
    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(service_metrics_mutex_);
        std::erase_if(
            service_pending_writes_, [&expired, now](const auto& item) {
                auto context = item.lock();
                if (!context || context->service_metrics_settled.load() ||
                    context->io_completed.load()) {
                    return true;
                }
                if (context->deadline > now) return false;
                expired.push_back(std::move(context));
                return true;
            });
    }
    for (const auto& context : expired) {
        // Publication is already past its deadline. Settle statistics only;
        // the worker still owns its buffers until the filesystem call ends.
        CompleteAsyncDfsWriteMetrics(context, {});
    }
}

void Client::ServiceMetricsReporterLoop() {
    while (service_metrics_report_running_.load(std::memory_order_acquire)) {
        for (int i = 0; i < 50 &&
                        service_metrics_report_running_.load(
                            std::memory_order_acquire);
             ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (!service_metrics_report_running_.load(std::memory_order_acquire)) {
            break;
        }

        ExpireAsyncDfsWriteMetrics();
        ServiceMetricsSnapshot snapshot;
        {
            std::lock_guard<std::mutex> lock(service_metrics_mutex_);
            if (service_metrics_snapshot_.service_endpoint.empty()) return;
            snapshot = service_metrics_snapshot_;
            snapshot.sequence = ++service_metrics_snapshot_.sequence;
        }
        auto result = master_client_.ReportServiceMetrics(snapshot);
        if (!result) {
            VLOG(1) << "Service metrics report failed: "
                    << toString(result.error());
        }
    }
}

void Client::StopServiceMetricsReporter() {
    service_metrics_report_running_.store(false, std::memory_order_release);
    if (service_metrics_report_thread_.joinable()) {
        service_metrics_report_thread_.join();
    }

    ServiceMetricsSnapshot snapshot;
    {
        std::lock_guard<std::mutex> lock(service_metrics_mutex_);
        if (service_metrics_snapshot_.service_endpoint.empty()) return;
        snapshot = service_metrics_snapshot_;
        snapshot.sequence = ++service_metrics_snapshot_.sequence;
    }
    auto result = master_client_.ReportServiceMetrics(snapshot);
    if (!result) {
        VLOG(1) << "Final service metrics report failed: "
                << toString(result.error());
    }
}

Client::~Client() {
    // Stop accepting new asynchronous DFS writes and wake callers waiting for
    // a fixed staging slot before draining tasks already submitted.
    if (dfs_write_staging_pool_) dfs_write_staging_pool_->Shutdown();
    // They report their result through master_client_ and write through
    // dfs_storage_backend_, so they must not outlive this object.
    DrainAsyncDfsWrites();
    write_thread_pool_.stop();
    StopServiceMetricsReporter();

    task_poll_running_ = false;
    if (task_poll_thread_.joinable()) {
        task_poll_thread_.join();
    }

    storage_heartbeat_running_ = false;
    if (storage_heartbeat_thread_.joinable()) {
        storage_heartbeat_thread_.join();
    }

    leader_monitor_running_ = false;
    if (leader_monitor_thread_.joinable()) {
        leader_monitor_thread_.join();
    }

    {
        std::lock_guard<std::mutex> lock(graceful_unmount_timer_mutex_);
        graceful_unmount_timer_stopping_ = true;
    }
    graceful_unmount_timer_cv_.notify_all();

    // Stop queued timer/task callbacks before tearing down segment state.
    task_running_ = false;
    task_thread_pool_.stop();
    dfs_backfill_thread_pool_.stop();

    // Make copies to avoid modifying while iterating
    std::vector<Segment> mounted_segments_copy;
    std::vector<Segment> gracefully_unmounting_segments_copy;
    std::vector<std::pair<UUID, std::function<void(const UUID&)>>>
        graceful_cleanup_callbacks;
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        mounted_segments_copy.reserve(mounted_segments_.size());
        for (auto& entry : mounted_segments_) {
            mounted_segments_copy.emplace_back(entry.second);
        }
        for (auto& entry : gracefully_unmounting_segments_) {
            gracefully_unmounting_segments_copy.emplace_back(entry.second);
        }
        graceful_cleanup_callbacks.reserve(
            graceful_unmount_cleanup_callbacks_.size());
        for (auto& entry : graceful_unmount_cleanup_callbacks_) {
            graceful_cleanup_callbacks.emplace_back(entry.first,
                                                    std::move(entry.second));
        }
        graceful_unmount_cleanup_callbacks_.clear();
    }

    // Unmount mounted segments: notify master + local cleanup
    for (auto& segment : mounted_segments_copy) {
        auto result =
            UnmountSegment(reinterpret_cast<void*>(segment.base), segment.size);
        if (!result) {
            LOG(ERROR) << "Failed to unmount segment in destructor: "
                       << toString(result.error());
        }
    }

    // Unregister gracefully unmounting segments: master already has timer
    for (auto& segment : gracefully_unmounting_segments_copy) {
        int rc = transfer_engine_->unregisterLocalMemory(
            reinterpret_cast<void*>(segment.base));
        if (rc != 0 && rc != ERR_ADDRESS_NOT_REGISTERED) {
            LOG(ERROR) << "Failed to unregister transfer buffer in destructor: "
                       << rc;
        }
    }

    // Clear any remaining segments
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        mounted_segments_.clear();
        gracefully_unmounting_segments_.clear();
    }

    for (auto& entry : graceful_cleanup_callbacks) {
        if (entry.second) {
            entry.second(entry.first);
        }
    }

    // Stop hot cache handler before unregistering/freeing its backing memory.
    hot_cache_handler_.reset();
    UnregisterLocalHotCacheMemory();
    hot_cache_.reset();
}

ReplicateConfig Client::AttachConfig(const ReplicateConfig& config) const {
    ReplicateConfig client_cfg = config;
    if (!host_id_.empty()) {
        client_cfg.host_id = host_id_;
    }
    if (!DfsWriteAllowed()) {
        client_cfg.dfs_replica_num = 0;
    } else if (DistributedStorageConfig::IsReplicaEnabledFromEnvironment()) {
        client_cfg.dfs_replica_num = 1;
    }
    return client_cfg;
}

static std::string NormalizeDfsPath(const std::string& path) {
    if (path.empty()) return {};
    std::error_code ec;
    auto normalized = std::filesystem::absolute(path, ec);
    if (ec) return path;
    return normalized.lexically_normal().string();
}

static void WarnOnClientDfsPathMismatch(const std::string& master_fsdir) {
    const auto storage_config = FileStorageConfig::FromEnvironment();
    if (storage_config.storage_backend_type != StorageBackendType::kDistributed) {
        return;
    }
    const auto client_fsdir = DistributedStorageConfig::FromEnvironment().fsdir;
    if (client_fsdir.empty() || master_fsdir.empty()) return;

    const auto normalized_client = NormalizeDfsPath(client_fsdir);
    const auto normalized_master = NormalizeDfsPath(master_fsdir);
    if (normalized_client == normalized_master) return;

    LOG(WARNING) << "DFS fsdir differs from master: client='"
                 << normalized_client << "', master='" << normalized_master
                 << ". Verify that both paths refer to the same DFS storage.";
}

static std::optional<bool> get_auto_discover() {
    const char* ev_ad = std::getenv("MC_MS_AUTO_DISC");
    if (ev_ad) {
        try {
            int iv = std::stoi(ev_ad);
            if (iv == 1) {
                LOG(INFO) << "auto discovery set by env MC_MS_AUTO_DISC";
                return true;
            } else if (iv == 0) {
                LOG(INFO) << "auto discovery not set by env MC_MS_AUTO_DISC";
                return false;
            }
        } catch (const std::exception&) {
            // A non-numeric or out-of-range value makes std::stoi throw; fall
            // through to the warning below and use the default instead of
            // letting the exception abort client initialization.
        }
        LOG(WARNING)
            << "invalid MC_MS_AUTO_DISC value: " << ev_ad
            << ", should be 0 or 1, using default: auto discovery not set";
    }
    return std::nullopt;
}

static std::vector<std::string> get_auto_discover_filters() {
    const char* raw_filters = std::getenv("MC_MS_FILTERS");
    if (raw_filters == nullptr) {
        return {};
    }

    LOG(INFO) << "whitelist filters: " << raw_filters;
    std::vector<std::string> filters;
    boost::split(filters, std::string(raw_filters), boost::is_any_of(","),
                 boost::token_compress_off);
    for (auto& filter : filters) {
        filter = std::string(TrimAsciiWhitespace(filter));
    }
    return filters;
}

static std::vector<std::string> ParseDeviceNames(std::string_view value) {
    std::vector<std::string> devices;
    boost::split(devices, std::string(value), boost::is_any_of(","),
                 boost::token_compress_on);
    for (auto& device : devices) {
        device = std::string(TrimAsciiWhitespace(device));
    }
    return devices;
}

tl::expected<std::optional<ha::HABackendSpec>, ErrorCode> ParseHABackendSpec(
    const std::string& master_server_entry) {
    const auto delimiter_pos = master_server_entry.find("://");
    if (delimiter_pos == std::string::npos) {
        return std::optional<ha::HABackendSpec>{std::nullopt};
    }

    auto backend_type =
        ha::ParseHABackendType(master_server_entry.substr(0, delimiter_pos));
    if (!backend_type.has_value()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto availability = ha::ValidateHABackendAvailability(backend_type.value());
    if (availability != ErrorCode::OK) {
        return tl::make_unexpected(availability);
    }

    return std::optional<ha::HABackendSpec>{ha::HABackendSpec{
        .type = backend_type.value(),
        .connstring = master_server_entry.substr(delimiter_pos + 3),
        .cluster_namespace = "",
    }};
}

tl::expected<void, ErrorCode> CheckRegisterMemoryParams(
    const void* addr, size_t length, const std::string& protocol) {
    if (addr == nullptr) {
        LOG(ERROR) << "addr is nullptr";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (length == 0) {
        LOG(ERROR) << "length is 0";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (auto limit = GetTransportRegistrationLimit(protocol);
        limit.has_value() && length > *limit) {
        LOG(ERROR) << "length " << length << " exceeds registration limit "
                   << *limit << " for protocol " << protocol;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

ErrorCode Client::ConnectToMaster(const std::string& master_server_entry) {
    auto ha_backend_spec = ParseHABackendSpec(master_server_entry);
    if (!ha_backend_spec) {
        LOG(ERROR) << "Invalid HA backend entry: " << master_server_entry;
        return ha_backend_spec.error();
    }

    if (ha_backend_spec.value().has_value()) {
        auto coordinator =
            ha::CreateLeaderCoordinator(ha_backend_spec.value().value());
        if (!coordinator) {
            LOG(ERROR) << "Failed to create HA backend coordinator: "
                       << toString(coordinator.error());
            return coordinator.error();
        }

        auto master_view = ha::ReadCurrentViewOrWaitForReady(
            *coordinator.value(), std::chrono::seconds(30));
        if (!master_view) {
            LOG(ERROR) << "Failed to discover a ready master view: "
                       << toString(master_view.error());
            return master_view.error();
        }
        if (!master_view->has_value()) {
            LOG(ERROR) << "No master became ready within 30 seconds";
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }

        const auto& current_master_view = master_view->value();
        auto err = SwitchLeader(current_master_view);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to master";
            return err;
        }

        leader_coordinator_ = std::move(coordinator.value());
        direct_master_address_.clear();

        return ErrorCode::OK;
    } else {
        auto err = master_client_.Connect(master_server_entry);
        if (err != ErrorCode::OK) {
            return err;
        }
        direct_master_address_ = master_server_entry;
        {
            std::lock_guard<std::mutex> lock(leader_switch_mutex_);
            current_master_view_.reset();
        }
        last_ping_success_.store(true);
        return ErrorCode::OK;
    }
}

void Client::EnterHaRuntimeMode() {
    if (!leader_coordinator_) {
        return;
    }

    master_client_.EnableHaConnectionPolicy();
    if (!leader_monitor_running_.exchange(true)) {
        leader_monitor_thread_ =
            std::thread([this]() { this->LeaderMonitorThreadMain(); });
    }
}

ErrorCode Client::SwitchLeader(const ha::MasterView& target_view) {
    std::lock_guard<std::mutex> lock(leader_switch_mutex_);

    if (current_master_view_.has_value()) {
        const auto& current_view = current_master_view_.value();
        if (target_view.view_version < current_view.view_version) {
            return ErrorCode::OK;
        }

        if (target_view.view_version == current_view.view_version &&
            target_view.leader_address == current_view.leader_address &&
            last_ping_success_.load()) {
            return ErrorCode::OK;
        }
    }

    auto err = master_client_.Connect(target_view.leader_address);
    if (err != ErrorCode::OK) {
        last_ping_success_.store(false);
        return err;
    }

    current_master_view_ = target_view;
    last_ping_success_.store(true);
    return ErrorCode::OK;
}

void Client::LeaderMonitorThreadMain() {
    // WaitForViewChange now blocks on a backend watch and returns as soon as
    // leadership actually changes, so this timeout no longer drives polling
    // cadence -- it is only a periodic re-sync safety net that re-reads the
    // current view in case a watch event was missed (e.g. a connection blip).
    // Keeping it long (30s instead of 1s) collapses steady-state backend load
    // from ~1 read/sec/client to ~1 read/30s/client without affecting failover
    // responsiveness, which is driven by the watch.
    constexpr auto kViewChangeTimeout = std::chrono::milliseconds(30000);
    constexpr auto kErrorRetryInterval = std::chrono::milliseconds(1000);

    while (leader_monitor_running_.load()) {
        std::optional<ViewVersionId> known_version;
        {
            std::lock_guard<std::mutex> lock(leader_switch_mutex_);
            if (current_master_view_.has_value()) {
                known_version = current_master_view_->view_version;
            }
        }

        auto view_change = leader_coordinator_->WaitForViewChange(
            known_version, kViewChangeTimeout);
        if (!view_change) {
            LOG(WARNING) << "Failed to wait for leader view change: "
                         << toString(view_change.error());
            std::this_thread::sleep_for(kErrorRetryInterval);
            continue;
        }

        if (!view_change->changed || !view_change->current_view.has_value()) {
            // A warming leader owns master_view without exposing a routable
            // endpoint. Acknowledge the empty observation so the next wait
            // blocks on the ready-value update instead of spinning on the old
            // view version.
            if (view_change->changed &&
                !view_change->current_view.has_value()) {
                std::lock_guard<std::mutex> lock(leader_switch_mutex_);
                // Do not let a stale empty watch result erase a newer view
                // installed concurrently by the heartbeat recovery path.
                const bool still_on_observed_view =
                    known_version.has_value()
                        ? current_master_view_.has_value() &&
                              current_master_view_->view_version ==
                                  known_version.value()
                        : !current_master_view_.has_value();
                if (still_on_observed_view) {
                    current_master_view_.reset();
                }
            }
            continue;
        }

        auto err = SwitchLeader(view_change->current_view.value());
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "Failed to switch to leader "
                         << view_change->current_view->leader_address << ": "
                         << toString(err);
            std::this_thread::sleep_for(kErrorRetryInterval);
        }
    }
}

void Client::EnsureStorageControlPlaneStarted() {
    if (!storage_heartbeat_running_.exchange(true)) {
        storage_heartbeat_thread_ =
            std::thread([this]() { this->StorageHeartbeatThreadMain(); });
    }

    if (!task_poll_running_.exchange(true)) {
        task_poll_thread_ =
            std::thread([this]() { this->TaskPollThreadMain(); });
    }
}

ErrorCode Client::InitTransferEngine(
    const std::string& local_hostname, const std::string& metadata_connstring,
    const std::string& protocol,
    const std::optional<std::string>& device_names) {
    // TEs created through the Store entry (Client::Create ->
    // InitTransferEngine) are tagged so ascend_direct can resolve a per-role
    // link config (e.g. Store=RoCE/D2H, P2P=HCCS/D2D). The flag only needs to
    // be live while the ascend transport is installed below; reset it on every
    // exit path. Assumes TE inits are serialized within the process.
    struct StoreTeInitGuard {
        ~StoreTeInitGuard() { globalConfig().ascend_store_te_init = false; }
    } store_te_init_guard;
    if (protocol == "ascend") {
        globalConfig().ascend_store_te_init = true;
    }

    // Check if using TENT mode - TENT handles transport configuration
    // internally. Use the engine's own check rather than the raw env var:
    // if Mooncake was built without USE_TENT, the env var has no effect on
    // the TransferEngine, so the store must still install transports.
    bool use_tent = transfer_engine_->isUsingTent();

    bool auto_discover = false;
    if (!use_tent) {
        // Get auto_discover and filters from env (non-TENT only)
        std::optional<bool> env_auto_discover = get_auto_discover();
        if (env_auto_discover.has_value()) {
            // Use user-specified auto-discover setting
            auto_discover = env_auto_discover.value();
        } else {
            // Enable auto-discover for RDMA/EFA if no devices are specified
            if ((protocol == "rdma" || protocol == "efa") &&
                !device_names.has_value()) {
                LOG(INFO) << "Set auto discovery ON by default for " << protocol
                          << " protocol, since no device names provided";
                auto_discover = true;
            }
        }
        transfer_engine_->setAutoDiscover(
            {.enabled = auto_discover, .protocol = protocol});

        // Honor filters when auto-discovery is enabled; otherwise warn once
        if (auto_discover) {
            LOG(INFO)
                << "Transfer engine auto discovery is enabled for protocol: "
                << protocol;
            auto filters = get_auto_discover_filters();
            transfer_engine_->setWhitelistFilters(std::move(filters));
        } else {
            const char* env_filters = std::getenv("MC_MS_FILTERS");
            if (env_filters && *env_filters != '\0') {
                LOG(WARNING)
                    << "MC_MS_FILTERS is set but auto discovery is disabled; "
                    << "ignoring whitelist: " << env_filters;
            }
        }
    }

    if (protocol == "ascend" || protocol == "ubshmem") {
        const char* ascend_use_fabric_mem =
            std::getenv("ASCEND_ENABLE_USE_FABRIC_MEM");
        if (ascend_use_fabric_mem) {
            globalConfig().ascend_use_fabric_mem = true;
        }
    }
    if (protocol == "sunrise_link") {
        const char* sunrise_use_device_mem_env =
            std::getenv("SUNRISE_USE_DEVICE_MEM");
        if (sunrise_use_device_mem_env) {
            globalConfig().sunrise_use_device_mem = true;
            LOG(INFO) << "SUNRISE_USE_DEVICE_MEM enabled: segments will be "
                      << "allocated in device memory via tangMalloc";
        }
    }
    auto [hostname, port] = parseHostNameWithPort(local_hostname);
    int rc = transfer_engine_->init(metadata_connstring, local_hostname,
                                    hostname, port);
    if (rc != 0) {
        LOG(ERROR) << "Failed to initialize transfer engine, rc=" << rc;
        return ErrorCode::INTERNAL_ERROR;
    }

    // TENT mode: Skip manual transport installation - TENT handles this
    // internally
    if (use_tent) {
        LOG(INFO)
            << "Using TENT mode - transport configuration handled internally";
        if (device_names.has_value()) {
            LOG(INFO)
                << "Note: device_names parameter is ignored in TENT mode. "
                << "Configure devices via TENT config file or environment "
                   "variables.";
        }
        return ErrorCode::OK;
    }

    if (!auto_discover) {
        LOG(INFO) << "Transfer engine auto discovery is disabled for protocol: "
                  << protocol;

        Transport* transport = nullptr;

        if (protocol == "rdma" || protocol == "efa") {
            if (!device_names.has_value() || device_names->empty()) {
                LOG(ERROR) << "RDMA protocol requires device names when auto "
                              "discovery is disabled";
                return ErrorCode::INVALID_PARAMS;
            }

            LOG(INFO) << "Using specified RDMA devices: "
                      << device_names.value();

            std::vector<std::string> devices =
                ParseDeviceNames(device_names.value());

            // Manually discover topology with specified devices only
            auto topology = transfer_engine_->getLocalTopology();
            if (topology) {
                topology->discover(devices);
                LOG(INFO) << "Topology discovery complete with specified "
                             "devices. Found "
                          << topology->getHcaList().size() << " HCAs";
            }

            transport = transfer_engine_->installTransport(protocol, nullptr);
            if (!transport) {
                LOG(ERROR) << "Failed to install RDMA transport with specified "
                              "devices";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "tcp") {
            if (device_names.has_value()) {
                LOG(WARNING)
                    << "TCP protocol does not use device names, ignoring";
            }

            try {
                transport = transfer_engine_->installTransport("tcp", nullptr);
            } catch (std::exception& e) {
                LOG(ERROR) << "tcp_transport_install_failed error_message=\""
                           << e.what() << "\"";
                return ErrorCode::INTERNAL_ERROR;
            }

            if (!transport) {
                LOG(ERROR) << "Failed to install TCP transport";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "ascend" || protocol == "ubshmem" ||
                   protocol == "sunrise_link") {
            if (device_names.has_value()) {
                LOG(WARNING) << protocol
                             << " protocol does not use device names, ignoring";
            }
            try {
                transport =
                    transfer_engine_->installTransport(protocol, nullptr);
            } catch (std::exception& e) {
                LOG(ERROR) << protocol
                           << "_transport_install_failed error_message=\""
                           << e.what() << "\"";
                return ErrorCode::INTERNAL_ERROR;
            }

            if (!transport) {
                LOG(ERROR) << "Failed to install " << protocol << " transport";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "cxl") {
            if (device_names.has_value()) {
                LOG(WARNING) << "CXL protocol does not use device "
                                "names, ignoring";
            }
            try {
                transport = transfer_engine_->installTransport("cxl", nullptr);
            } catch (std::exception& e) {
                LOG(ERROR) << "cxl_transport_install_failed error_message=\""
                           << e.what() << "\"";
                return ErrorCode::INTERNAL_ERROR;
            }

            if (!transport) {
                LOG(ERROR) << "Failed to install CXL transport";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "cxi") {
            try {
                transport = transfer_engine_->installTransport("cxi", nullptr);
            } catch (std::exception& e) {
                LOG(ERROR) << "cxi_transport_install_failed error_message=\""
                           << e.what() << "\"";
            }
        } else if (protocol == "ub") {
            auto deviceName = device_names.value_or("bonding_dev_0");
            LOG(ERROR) << "ub protocol entable device names is " << deviceName;
            auto devices = ParseDeviceNames(deviceName);
            auto topology = transfer_engine_->getLocalTopology();
            if (topology) {
                topology->discover(devices);
                LOG(INFO) << "Topology discovery complete with specified "
                             "devices. Found "
                          << topology->getHcaList().size() << " HCAs";
            }
            transport = transfer_engine_->installTransport("ub", nullptr);
            if (!transport) {
                LOG(ERROR) << "Failed to install ub transport with specified "
                              "devices";
                return ErrorCode::INTERNAL_ERROR;
            }
        } else if (protocol == "nvlink_intra") {
#ifdef USE_INTRA_NVLINK
            LOG(INFO) << "Using intra-NVLink protocol.";
            transport =
                transfer_engine_->installTransport("nvlink_intra", nullptr);
            if (!transport) {
                LOG(ERROR) << "Failed to install nvlink_intra transport.";
                return ErrorCode::INTERNAL_ERROR;
            }
#else
            LOG(ERROR)
                << "--protocol=nvlink_intra requires USE_INTRA_NVLINK=ON, "
                   "please rebuild mooncake from source.";
            return ErrorCode::INVALID_PARAMS;
#endif
        } else {
            LOG(ERROR) << "unsupported_protocol protocol=" << protocol;
            return ErrorCode::INVALID_PARAMS;
        }
    }

    return ErrorCode::OK;
}

void Client::InitTransferSubmitter() {
    // Initialize TransferSubmitter after transfer engine is ready
    // Keep using logical local_hostname for name-based behaviors; endpoint is
    // used separately where needed.
#ifdef USE_NOF
    int numa_socket_id =
        GetConfiguredNumaSocketId().value_or(GetCurrentNumaSocketId());
    transfer_submitter_ = std::make_unique<TransferSubmitter>(
        *transfer_engine_, storage_backend_, local_hostname_,
        metrics_ ? &metrics_->transfer_metric : nullptr, numa_socket_id);
#else
    transfer_submitter_ = std::make_unique<TransferSubmitter>(
        *transfer_engine_, storage_backend_, local_hostname_,
        metrics_ ? &metrics_->transfer_metric : nullptr);
#endif
}

std::optional<std::shared_ptr<Client>> Client::Create(
    const std::string& local_hostname, const std::string& metadata_connstring,
    const std::string& protocol, const std::optional<std::string>& device_names,
    const std::string& master_server_entry,
    const std::shared_ptr<TransferEngine>& transfer_engine,
    std::map<std::string, std::string> labels, const std::string& tenant_id) {
    auto client = std::shared_ptr<Client>(new Client(
        local_hostname, metadata_connstring, protocol, labels, tenant_id));

    ErrorCode err = client->ConnectToMaster(master_server_entry);
    if (err != ErrorCode::OK) {
        return std::nullopt;
    }

    // Initialize storage backend if storage_root_dir is valid
    auto config_response = client->master_client_.GetStorageConfig();
    if (!config_response) {
        LOG(ERROR) << "Failed to get storage config from master";
        // Fallback to GetFsdir for backward compatibility
        auto response = client->master_client_.GetFsdir();
        if (!response) {
            LOG(ERROR) << "Failed to get fsdir from master";
        } else if (response.value().empty()) {
            LOG(INFO)
                << "Storage root directory is not set. persisting data is "
                   "disabled.";
        } else {
            auto dir_string = response.value();
            size_t pos = dir_string.find_last_of('/');
            if (pos != std::string::npos) {
                std::string storage_root_dir = dir_string.substr(0, pos);
                std::string fs_subdir = dir_string.substr(pos + 1);
                LOG(INFO) << "Storage root directory is: " << storage_root_dir;
                LOG(INFO) << "Fs subdir is: " << fs_subdir;
                // Initialize storage backend with default eviction settings
                auto prep_err = client->PrepareStorageBackend(
                    storage_root_dir, fs_subdir, true, 0);
                if (prep_err != ErrorCode::OK) {
                    LOG(ERROR)
                        << "Failed to initialize storage backend: " << prep_err
                        << ". Persistence was requested via fsdir but is "
                           "unavailable.";
                    return std::nullopt;
                }
            } else {
                LOG(ERROR) << "Invalid fsdir format: " << dir_string;
                return std::nullopt;
            }
        }
    } else {
        auto config = config_response.value();
        client->dfs_root_identity_ = config.dfs_root_identity;
        client->master_dfs_root_dir_ = config.dfs_fsdir;
        WarnOnClientDfsPathMismatch(config.dfs_fsdir);
        if (config.fsdir.empty()) {
            LOG(INFO)
                << "Storage root directory is not set. persisting data is "
                   "disabled.";
        } else {
            size_t pos = config.fsdir.find_last_of('/');
            if (pos != std::string::npos) {
                std::string storage_root_dir = config.fsdir.substr(0, pos);
                std::string fs_subdir = config.fsdir.substr(pos + 1);
                LOG(INFO) << "Storage root directory is: " << storage_root_dir;
                LOG(INFO) << "Fs subdir is: " << fs_subdir;
                LOG(INFO) << "Disk eviction enabled: "
                          << config.enable_disk_eviction;
                LOG(INFO) << "Quota bytes: " << config.quota_bytes;
                // Initialize storage backend with config from master
                auto prep_err = client->PrepareStorageBackend(
                    storage_root_dir, fs_subdir, config.enable_disk_eviction,
                    config.quota_bytes);
                if (prep_err != ErrorCode::OK) {
                    LOG(ERROR)
                        << "Failed to initialize storage backend: " << prep_err
                        << ". Persistence was requested via storage config "
                           "but is unavailable.";
                    return std::nullopt;
                }
            } else {
                LOG(ERROR) << "Invalid fsdir format: " << config.fsdir;
                return std::nullopt;
            }
        }
    }

    client->EnterHaRuntimeMode();

    // this only performs RPC calls
    if (protocol == "rpc_only") {
        LOG(INFO) << "Use rpc only. Skip initializing transfer engine.";
        return client;
    }

    // Initialize transfer engine
    if (transfer_engine == nullptr) {
        client->transfer_engine_ = std::make_shared<TransferEngine>();
        err = client->InitTransferEngine(local_hostname, metadata_connstring,
                                         protocol, device_names);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to initialize transfer engine";
            return std::nullopt;
        }
    } else {
        client->transfer_engine_ = transfer_engine;
        LOG(INFO) << "Use existing transfer engine instance. Skip its "
                     "initialization.";
    }

    client->InitTransferSubmitter();
    // Initialize local hot cache
    err = client->InitLocalHotCache();
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to initialize local hot cache";
    }

    return client;
}

tl::expected<QueryResult, ErrorCode> Client::PrepareDfsReadRetry(
    const QueryResult& original, const QueryResult& refreshed) {
    Replica::Descriptor selected;
    if (FindFirstCompleteReplica(original.replicas, selected) !=
            ErrorCode::OK ||
        !selected.is_dfs_replica()) {
        return tl::unexpected(ErrorCode::DFS_STALE_HANDLE);
    }
    // The caller still owns buffers and session state based on the old query.
    // Only retry the same replica type, size and checksum view. If the caller
    // omitted a checksum, do not silently introduce one from a new query.
    if (original.object_checksum != refreshed.object_checksum) {
        return tl::unexpected(ErrorCode::DFS_STALE_HANDLE);
    }
    for (const auto& replica : refreshed.replicas) {
        if (!replica.is_dfs_replica() ||
            replica.status != ReplicaStatus::COMPLETE) {
            continue;
        }
        if (calculate_total_size(replica) != calculate_total_size(selected)) {
            return tl::unexpected(ErrorCode::DFS_STALE_HANDLE);
        }
        return QueryResult({replica}, refreshed.lease_timeout,
                           original.object_checksum);
    }
    return tl::unexpected(ErrorCode::DFS_STALE_HANDLE);
}

tl::expected<void, ErrorCode> Client::Get(const std::string& object_key,
                                          std::vector<Slice>& slices) {
    auto query_result = Query(object_key);
    if (!query_result) {
        return tl::unexpected(query_result.error());
    }
    return Get(object_key, query_result.value(), slices);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchGet(
    const std::vector<std::string>& object_keys,
    std::unordered_map<std::string, std::vector<Slice>>& slices) {
    auto batched_query_results = BatchQuery(object_keys);

    // If any queries failed, return error results immediately for failed
    // queries
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(object_keys.size());

    std::vector<QueryResult> valid_query_results;
    std::vector<size_t> valid_indices;
    std::vector<std::string> valid_keys;

    for (size_t i = 0; i < batched_query_results.size(); ++i) {
        if (batched_query_results[i]) {
            valid_query_results.emplace_back(batched_query_results[i].value());
            valid_indices.emplace_back(i);
            valid_keys.emplace_back(object_keys[i]);
            results.emplace_back();  // placeholder for successful results
        } else {
            results.emplace_back(
                tl::unexpected(batched_query_results[i].error()));
        }
    }

    // If we have any valid queries, process them
    if (!valid_keys.empty()) {
        std::unordered_map<std::string, std::vector<Slice>> valid_slices;
        for (const auto& key : valid_keys) {
            auto it = slices.find(key);
            if (it != slices.end()) {
                valid_slices[key] = it->second;
            }
        }

        auto valid_results =
            BatchGet(valid_keys, valid_query_results, valid_slices);

        // Merge results back
        for (size_t i = 0; i < valid_indices.size(); ++i) {
            results[valid_indices[i]] = valid_results[i];
        }
    }

    return results;
}

tl::expected<
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
    ErrorCode>
Client::BatchQueryIp(const std::vector<UUID>& client_ids) {
    auto result = master_client_.BatchQueryIp(client_ids);
    return result;
}

tl::expected<std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
             ErrorCode>
Client::QueryByRegex(const std::string& str) {
    auto result = master_client_.GetReplicaListByRegex(str);
    return result;
}

tl::expected<QueryResult, ErrorCode> Client::Query(
    const std::string& object_key) {
    std::chrono::steady_clock::time_point start_time =
        std::chrono::steady_clock::now();
    auto result = master_client_.GetReplicaList(object_key);
    if (!result) {
        return tl::unexpected(result.error());
    }
    return QueryResult(
        std::move(result.value().replicas),
        start_time + std::chrono::milliseconds(result.value().lease_ttl_ms),
        result.value().object_checksum);
}

std::vector<tl::expected<QueryResult, ErrorCode>> Client::BatchQuery(
    const std::vector<std::string>& object_keys) {
    return BatchQuery(object_keys, master_client_.tenant_id());
}

std::vector<tl::expected<QueryResult, ErrorCode>> Client::BatchQuery(
    const std::vector<std::string>& object_keys, const std::string& tenant_id) {
    std::chrono::steady_clock::time_point start_time =
        std::chrono::steady_clock::now();
    auto response = master_client_.BatchGetReplicaList(object_keys, tenant_id);

    // Check if we got the expected number of responses
    if (response.size() != object_keys.size()) {
        LOG(ERROR) << "BatchQuery response size mismatch. Expected: "
                   << object_keys.size() << ", Got: " << response.size();
        // Return vector of RPC_FAIL errors
        std::vector<tl::expected<QueryResult, ErrorCode>> results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
        }
        return results;
    }
    std::vector<tl::expected<QueryResult, ErrorCode>> results;
    results.reserve(response.size());
    for (size_t i = 0; i < response.size(); ++i) {
        if (response[i]) {
            results.emplace_back(QueryResult(
                std::move(response[i].value().replicas),
                start_time +
                    std::chrono::milliseconds(response[i].value().lease_ttl_ms),
                response[i].value().object_checksum));
        } else {
            results.emplace_back(tl::unexpected(response[i].error()));
        }
    }
    return results;
}

tl::expected<uint64_t, ErrorCode> Client::ComputeObjectChecksumForSlices(
    const std::string& object_key, const std::vector<Slice>& slices,
    size_t object_size, bool staged_for_dfs) {
    CrcChecksum checksum;
    size_t remaining = object_size;
    auto runtime_accelerator =
        device::GetAcceleratorRegistry().RuntimeAccelerators();
    std::unique_ptr<ScopedObjectChecksumBuffer> staging;

    for (const auto& slice : slices) {
        const size_t bytes = std::min(slice.size, remaining);
        if (bytes == 0) {
            continue;
        }
        if (slice.ptr == nullptr) {
            LOG(ERROR) << "object_checksum_null_slice key=" << object_key;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }

        if (staged_for_dfs ||
            !runtime_accelerator.FindDeviceForPointer(slice.ptr)) {
            checksum.Update(slice.ptr, bytes);
            remaining -= bytes;
            continue;
        }

        if (!staging) {
            staging = std::make_unique<ScopedObjectChecksumBuffer>(
                *pinned_buffer_pool_,
                std::min(kObjectChecksumD2HChunkSize, object_size));
        }
        size_t offset = 0;
        while (offset < bytes) {
            const size_t chunk =
                std::min(kObjectChecksumD2HChunkSize, bytes - offset);
            const auto* source = static_cast<const char*>(slice.ptr) + offset;
            if (!runtime_accelerator.CopyToHost(staging->data(), source,
                                                chunk)) {
                LOG(ERROR) << "object_checksum_d2h_failed key=" << object_key
                           << " size=" << chunk;
                return tl::unexpected(ErrorCode::TRANSFER_FAIL);
            }
            checksum.Update(staging->data(), chunk);
            offset += chunk;
        }
        remaining -= bytes;
    }

    if (remaining != 0) {
        LOG(ERROR) << "object_checksum_slices_too_small key=" << object_key
                   << " missing_bytes=" << remaining;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return checksum.Finalize();
}

tl::expected<void, ErrorCode> Client::VerifyObjectChecksum(
    const std::string& object_key, const std::vector<Slice>& slices,
    size_t object_size, std::optional<uint64_t> expected_checksum) {
    if (!object_checksum_enabled_) {
        return {};
    }
    if (!expected_checksum.has_value()) {
        VLOG(1) << "object_checksum_absent key=" << object_key;
        return {};
    }

    auto actual_checksum =
        ComputeObjectChecksumForSlices(object_key, slices, object_size);
    if (!actual_checksum) {
        return tl::unexpected(actual_checksum.error());
    }
    if (*actual_checksum != *expected_checksum) {
        LOG(ERROR) << "object_checksum_mismatch key=" << object_key
                   << " expected=" << *expected_checksum
                   << " actual=" << *actual_checksum;
        return tl::unexpected(ErrorCode::CHECKSUM_MISMATCH);
    }
    return {};
}

tl::expected<std::vector<std::string>, ErrorCode> Client::BatchReplicaClear(
    const std::vector<std::string>& object_keys, const UUID& client_id,
    const std::string& segment_name) {
    auto result =
        master_client_.BatchReplicaClear(object_keys, client_id, segment_name);
    return result;
}

tl::expected<void, ErrorCode> Client::Get(const std::string& object_key,
                                          const QueryResult& query_result,
                                          std::vector<Slice>& slices) {
    auto result = GetImpl(object_key, query_result, slices);
    if (result || result.error() != ErrorCode::DFS_STALE_HANDLE) return result;
    auto refreshed = Query(object_key);
    if (!refreshed) return tl::make_unexpected(refreshed.error());
    auto retry = PrepareDfsReadRetry(query_result, *refreshed);
    if (!retry) return tl::unexpected(retry.error());
    // Exactly one retry with a fresh lease, preserving the caller's read view.
    return GetImpl(object_key, *retry, slices);
}

tl::expected<void, ErrorCode> Client::GetImpl(const std::string& object_key,
                                              const QueryResult& query_result,
                                              std::vector<Slice>& slices) {
    // Find the first complete replica
    Replica::Descriptor replica;
    ErrorCode err = FindFirstCompleteReplica(query_result.replicas, replica);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::INVALID_REPLICA) {
            LOG(ERROR) << "no_complete_replicas_found key=" << object_key;
        }
        return tl::unexpected(err);
    }

    // Check local hot cache and update replica descriptor if cache hit
    bool cache_used = false;
    if (hot_cache_ && replica.is_memory_replica()) {
        cache_used = RedirectToHotCache(object_key, replica);
    }

    auto t0_get = std::chrono::steady_clock::now();
    if (replica.is_dfs_replica()) {
        err = ReadDfsReplica(object_key, replica, slices);
    } else {
        err = TransferRead(replica, slices);
    }

    if (replica.is_dfs_replica() && err == ErrorCode::FILE_NOT_FOUND) {
        const std::vector<DfsMissingBucketReport> reports{{
            replica.get_dfs_descriptor().shard_idx}};
        const auto invalidation_results =
            master_client_.BatchInvalidateDfsBuckets(reports);
        if (invalidation_results.size() != 1 ||
            !invalidation_results.front()) {
            LOG(WARNING) << "Failed to report missing DFS bucket "
                         << reports.front().bucket_id;
        }
    }

    // Release the cache block after transfer completes (memcpy is done)
    if (hot_cache_ && cache_used) {
        hot_cache_->ReleaseHotKey(object_key);
    }

    auto us_get = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0_get)
                      .count();
    if (metrics_) {
        metrics_->transfer_metric.get_latency_us.observe(us_get);
    }

    if (err != ErrorCode::OK) {
        if (replica.is_memory_replica() || replica.is_dfs_replica()) {
            RecordServiceReadFailure(replica.is_memory_replica()
                                         ? ReplicaType::MEMORY
                                         : ReplicaType::DFS);
        }
        LOG(ERROR) << "transfer_read_failed key=" << object_key;
        return tl::unexpected(err);
    }

    auto checksum_result =
        VerifyObjectChecksum(object_key, slices, calculate_total_size(replica),
                             query_result.object_checksum);
    if (!checksum_result) {
        if (replica.is_memory_replica() || replica.is_dfs_replica()) {
            RecordServiceReadFailure(replica.is_memory_replica()
                                         ? ReplicaType::MEMORY
                                         : ReplicaType::DFS);
        }
        return tl::unexpected(checksum_result.error());
    }

    // Frequency admission: only promote frequently accessed keys to hot cache.
    // Skip when cache_used — data was already served from local cache, no need
    // to re-promote or increment the CMS counter.
    if (ShouldAdmitToHotCache(object_key, cache_used)) {
        ProcessSlicesAsync(object_key, slices, replica);
    }

    if (query_result.IsLeaseExpired()) {
        LOG(WARNING) << "lease_expired_before_data_transfer_completed key="
                     << object_key;
        return tl::unexpected(ErrorCode::LEASE_EXPIRED);
    }
    // Log cache hit statistics
    if (hot_cache_ && replica.is_memory_replica()) {
        VLOG(1) << "Get completed: key=" << object_key
                << " cache_hit=" << (cache_used ? 1 : 0);
    }

    return {};
}

tl::expected<void, ErrorCode> Client::Get(const std::string& object_key,
                                          const QueryResult& query_result,
                                          std::vector<Slice>& slices,
                                          uint64_t src_offset) {
    Replica::Descriptor replica;
    ErrorCode err = FindFirstCompleteReplica(query_result.replicas, replica);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::INVALID_REPLICA) {
            LOG(ERROR) << "no_complete_replicas_found key=" << object_key;
        }
        return tl::unexpected(err);
    }
    if (!replica.is_memory_replica()) {
        LOG(ERROR) << "Range read only supported for memory replicas, key="
                   << object_key;
        return tl::unexpected(ErrorCode::INVALID_REPLICA);
    }

    auto t0_get = std::chrono::steady_clock::now();
    err = TransferReadRange(replica, slices, src_offset);
    auto us_get = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0_get)
                      .count();
    if (metrics_) {
        metrics_->transfer_metric.get_latency_us.observe(us_get);
    }

    if (err != ErrorCode::OK) {
        RecordServiceReadFailure(ReplicaType::MEMORY);
        LOG(ERROR) << "transfer_read_range_failed key=" << object_key;
        return tl::unexpected(err);
    }
    if (query_result.IsLeaseExpired()) {
        LOG(WARNING) << "lease_expired_before_data_transfer_completed key="
                     << object_key;
        return tl::unexpected(ErrorCode::LEASE_EXPIRED);
    }
    return {};
}

std::optional<TransferEngine::ScatterTransferOperation> Client::SubmitScatter(
    const std::vector<TransferEngine::ScatterTransferRange>& transfers) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return std::nullopt;
    }
    return transfer_submitter_->submitScatter(transfers);
}

struct BatchGetOperation {
    std::vector<Replica::Descriptor> replicas;
    std::vector<std::vector<Slice>> batched_slices;
    std::vector<size_t> key_indexes;
    std::vector<bool> cache_used;  // Track which keys used cache
    std::vector<TransferFuture> futures;
};

std::vector<tl::expected<void, ErrorCode>> Client::BatchGetWhenPreferSameNode(
    const std::vector<std::string>& object_keys,
    const std::vector<QueryResult>& query_results,
    std::vector<std::vector<Slice>>& batched_slices) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.resize(object_keys.size());

    std::unordered_map<std::string, BatchGetOperation> seg_to_op_map{};
    for (size_t i = 0; i < object_keys.size(); ++i) {
        const auto& key = object_keys[i];
        const auto& replica_list = query_results[i].replicas;
        if (batched_slices[i].empty()) {
            LOG(ERROR) << "Slices are empty for key: " << key
                       << ", operation_index=" << i;
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        Replica::Descriptor replica;
        ErrorCode err = FindFirstCompleteReplica(replica_list, replica);
        if (err != ErrorCode::OK) {
            if (err == ErrorCode::INVALID_REPLICA) {
                LOG(ERROR) << "no_complete_replicas_found key=" << key;
            }
            results[i] = tl::unexpected(err);
            continue;
        }
        if (!replica.is_memory_replica()) {
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }

        // Check local hot cache and update replica descriptor if cache hit
        bool cache_used = false;
        if (hot_cache_ && replica.is_memory_replica()) {
            cache_used = RedirectToHotCache(key, replica);
        }

        auto& memory_descriptor = replica.get_memory_descriptor();
        if (memory_descriptor.buffer_descriptor.size_ == 0) {
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }
        auto& buffer_descriptor = memory_descriptor.buffer_descriptor;
        auto seg = buffer_descriptor.transport_endpoint_;
        auto& op = seg_to_op_map[seg];
        op.replicas.emplace_back(replica);
        op.batched_slices.emplace_back(batched_slices[i]);
        op.key_indexes.emplace_back(i);
        op.cache_used.emplace_back(cache_used);
    }
    for (auto& seg_to_op : seg_to_op_map) {
        auto& op = seg_to_op.second;
        auto future = transfer_submitter_->submit_batch(
            op.replicas, op.batched_slices, TransferRequest::READ);
        if (!future) {
            for (size_t idx = 0; idx < op.key_indexes.size(); ++idx) {
                auto index = op.key_indexes[idx];
                if (hot_cache_ && idx < op.cache_used.size() &&
                    op.cache_used[idx]) {
                    hot_cache_->ReleaseHotKey(object_keys[index]);
                }
                RecordServiceReadFailure(ReplicaType::MEMORY);
                results[index] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
                LOG(ERROR) << "Failed to submit transfer operation for key: "
                           << object_keys[index];
            }
            continue;
        }
        op.futures.emplace_back(std::move(*future));
    }
    for (auto& seg_to_op : seg_to_op_map) {
        auto& op = seg_to_op.second;
        if (op.futures.empty()) {
            continue;
        }
        ErrorCode result = op.futures[0].get();
        if (result != ErrorCode::OK) {
            for (size_t idx = 0; idx < op.key_indexes.size(); ++idx) {
                auto index = op.key_indexes[idx];
                // Release cache block even on failure (memcpy may have started)
                if (hot_cache_ && idx < op.cache_used.size() &&
                    op.cache_used[idx]) {
                    hot_cache_->ReleaseHotKey(object_keys[index]);
                }
                RecordServiceReadFailure(ReplicaType::MEMORY);
                results[index] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
                LOG(ERROR) << "Failed to submit transfer operation for key: "
                           << object_keys[index];
            }
        } else {
            for (size_t idx = 0; idx < op.key_indexes.size(); ++idx) {
                auto index = op.key_indexes[idx];
                VLOG(1) << "Transfer completed successfully for key: "
                        << object_keys[index];

                // Release the cache block after transfer completes (memcpy is
                // done)
                if (hot_cache_ && idx < op.cache_used.size() &&
                    op.cache_used[idx]) {
                    hot_cache_->ReleaseHotKey(object_keys[index]);
                }

                auto checksum_result = VerifyObjectChecksum(
                    object_keys[index], op.batched_slices[idx],
                    calculate_total_size(op.replicas[idx]),
                    query_results[index].object_checksum);
                if (!checksum_result) {
                    RecordServiceReadFailure(ReplicaType::MEMORY);
                    results[index] = tl::unexpected(checksum_result.error());
                    continue;
                }
                results[index] = {};

                // Frequency admission: only promote frequently accessed keys.
                // Skip when cache was used (data served from local cache).
                if (idx < op.replicas.size() &&
                    idx < op.batched_slices.size() &&
                    ShouldAdmitToHotCache(
                        object_keys[index],
                        idx < op.cache_used.size() && op.cache_used[idx])) {
                    ProcessSlicesAsync(object_keys[index],
                                       op.batched_slices[idx],
                                       op.replicas[idx]);
                }
            }
        }
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchGet(
    const std::vector<std::string>& object_keys,
    const std::vector<QueryResult>& query_results,
    std::unordered_map<std::string, std::vector<Slice>>& slices,
    bool prefer_alloc_in_same_node,
    const std::unordered_map<std::string, std::shared_ptr<void>>*
        dfs_buffer_owners) {
    std::vector<std::vector<Slice>> batched_slices;
    batched_slices.reserve(object_keys.size());
    std::vector<std::shared_ptr<void>> aligned_buffer_owners;
    if (dfs_buffer_owners != nullptr) {
        aligned_buffer_owners.reserve(object_keys.size());
    }
    for (const auto& key : object_keys) {
        auto slices_it = slices.find(key);
        batched_slices.push_back(slices_it == slices.end()
                                     ? std::vector<Slice>{}
                                     : slices_it->second);
        if (dfs_buffer_owners != nullptr) {
            auto owner_it = dfs_buffer_owners->find(key);
            aligned_buffer_owners.push_back(
                owner_it == dfs_buffer_owners->end() ? nullptr
                                                     : owner_it->second);
        }
    }
    return BatchGet(object_keys, query_results, batched_slices,
                    prefer_alloc_in_same_node,
                    dfs_buffer_owners == nullptr ? nullptr
                                                 : &aligned_buffer_owners);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchGet(
    const std::vector<std::string>& object_keys,
    const std::vector<QueryResult>& query_results,
    std::vector<std::vector<Slice>>& batched_slices,
    bool prefer_alloc_in_same_node,
    const std::vector<std::shared_ptr<void>>* dfs_buffer_owners) {
    auto results =
        BatchGetImpl(object_keys, query_results, batched_slices,
                     prefer_alloc_in_same_node, dfs_buffer_owners);
    std::vector<std::string> retry_keys;
    std::vector<size_t> retry_indices;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i] && results[i].error() == ErrorCode::DFS_STALE_HANDLE) {
            retry_keys.push_back(object_keys[i]);
            retry_indices.push_back(i);
        }
    }
    if (retry_keys.empty()) return results;
    auto refreshed = BatchQuery(retry_keys);
    if (refreshed.size() != retry_keys.size()) {
        for (auto i : retry_indices)
            results[i] = tl::unexpected(ErrorCode::RPC_FAIL);
        return results;
    }
    std::vector<std::string> valid_keys;
    std::vector<QueryResult> valid_queries;
    std::vector<std::vector<Slice>> valid_slices;
    std::vector<std::shared_ptr<void>> valid_buffer_owners;
    std::vector<size_t> valid_indices;
    for (size_t i = 0; i < refreshed.size(); ++i) {
        if (!refreshed[i]) {
            results[retry_indices[i]] = tl::unexpected(refreshed[i].error());
            continue;
        }
        auto retry =
            PrepareDfsReadRetry(query_results[retry_indices[i]], *refreshed[i]);
        if (!retry) {
            results[retry_indices[i]] = tl::unexpected(retry.error());
            continue;
        }
        valid_keys.push_back(retry_keys[i]);
        valid_queries.push_back(std::move(*retry));
        valid_slices.push_back(batched_slices[retry_indices[i]]);
        if (dfs_buffer_owners != nullptr) {
            valid_buffer_owners.push_back(
                (*dfs_buffer_owners)[retry_indices[i]]);
        }
        valid_indices.push_back(retry_indices[i]);
    }
    if (!valid_keys.empty()) {
        auto retried = BatchGetImpl(
            valid_keys, valid_queries, valid_slices, prefer_alloc_in_same_node,
            dfs_buffer_owners == nullptr ? nullptr : &valid_buffer_owners);
        for (size_t i = 0; i < retried.size(); ++i)
            results[valid_indices[i]] = std::move(retried[i]);
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchGetImpl(
    const std::vector<std::string>& object_keys,
    const std::vector<QueryResult>& query_results,
    std::vector<std::vector<Slice>>& batched_slices,
    bool prefer_alloc_in_same_node,
    const std::vector<std::shared_ptr<void>>* dfs_buffer_owners) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        std::vector<tl::expected<void, ErrorCode>> results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            if (i < query_results.size()) {
                Replica::Descriptor replica;
                if (FindFirstCompleteReplica(query_results[i].replicas,
                                             replica) == ErrorCode::OK &&
                    (replica.is_memory_replica() ||
                     replica.is_dfs_replica())) {
                    RecordServiceReadFailure(
                        replica.is_memory_replica() ? ReplicaType::MEMORY
                                                    : ReplicaType::DFS);
                }
            }
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        return results;
    }

    // Validate input size consistency
    if (query_results.size() != object_keys.size() ||
        batched_slices.size() != object_keys.size() ||
        (dfs_buffer_owners != nullptr &&
         dfs_buffer_owners->size() != object_keys.size())) {
        LOG(ERROR) << "BatchGet operation-aligned input size mismatch: keys="
                   << object_keys.size()
                   << ", query_results=" << query_results.size()
                   << ", batched_slices=" << batched_slices.size()
                   << ", buffer_owners="
                   << (dfs_buffer_owners == nullptr
                           ? 0
                           : dfs_buffer_owners->size());
        std::vector<tl::expected<void, ErrorCode>> results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        return results;
    }
    if (prefer_alloc_in_same_node) {
        auto results = BatchGetWhenPreferSameNode(
            object_keys, query_results, batched_slices);
        auto now = std::chrono::steady_clock::now();
        for (size_t i = 0; i < object_keys.size(); ++i) {
            if (results[i].has_value() &&
                query_results[i].IsLeaseExpired(now)) {
                results[i] = tl::unexpected(ErrorCode::LEASE_EXPIRED);
            }
        }
        return results;
    }

    // Collect all transfer operations for parallel execution
    // Tuple: (index, key, future, replica, cache_used)
    std::vector<std::tuple<size_t, std::string, TransferFuture,
                           Replica::Descriptor, bool>>
        pending_transfers;
    std::vector<DfsReadRequest> dfs_read_requests;
    std::vector<size_t> dfs_read_indices;
    std::vector<tl::expected<void, ErrorCode>> results(object_keys.size());
    // Record batch get transfer latency (Submit + Wait)
    auto t0_batch_get = std::chrono::steady_clock::now();

    // Collect cache hit statistics for the entire batch
    size_t total_cache_hits = 0;

    // Submit all transfers in parallel
    for (size_t i = 0; i < object_keys.size(); ++i) {
        const auto& key = object_keys[i];
        const auto& query_result = query_results[i];
        auto& object_slices = batched_slices[i];
        if (object_slices.empty()) {
            LOG(ERROR) << "Slices are empty for key: " << key
                       << ", operation_index=" << i;
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        // Find the first complete replica for this key
        Replica::Descriptor replica;
        ErrorCode err =
            FindFirstCompleteReplica(query_result.replicas, replica);
        if (err != ErrorCode::OK) {
            if (err == ErrorCode::INVALID_REPLICA) {
                LOG(ERROR) << "no_complete_replicas_found key=" << key;
            }
            results[i] = tl::unexpected(err);
            continue;
        }

        bool cache_used = false;
        if (hot_cache_ && replica.is_memory_replica()) {
            cache_used = RedirectToHotCache(key, replica);
            if (cache_used) {
                total_cache_hits++;
            }
        }

        // Submit transfer operation asynchronously
        std::optional<TransferFuture> future;
        if (replica.is_dfs_replica()) {
            if (!DfsReadAllowed()) {
                RecordServiceReadFailure(ReplicaType::DFS);
                results[i] =
                    tl::unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
                continue;
            }
            if (!dfs_storage_backend_) {
                LOG(ERROR) << "DFS backend is not initialized";
                RecordServiceReadFailure(ReplicaType::DFS);
                results[i] = tl::unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
                continue;
            }
            const auto& desc = replica.get_dfs_descriptor();
            std::shared_ptr<void> buffer_owner;
            if (dfs_buffer_owners != nullptr) {
                buffer_owner = (*dfs_buffer_owners)[i];
            }
            dfs_read_requests.push_back(
                DfsReadRequest{key, desc, object_slices,
                               std::move(buffer_owner)});
            dfs_read_indices.push_back(i);
            continue;
        } else if (replica.is_nof_replica()) {
            auto contiguous_range = GetContiguousSliceRange(object_slices);
            if (!contiguous_range.has_value()) {
                LOG(ERROR) << "NoF transfer requires contiguous slices";
                results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
                continue;
            }
            future = transfer_submitter_->submit(
                replica, object_slices, TransferRequest::READ,
                contiguous_range->ptr, contiguous_range->size);
        } else {
            future = transfer_submitter_->submit(replica, object_slices,
                                                 TransferRequest::READ);
        }
        if (!future) {
            // Release cache block if submit failed
            if (hot_cache_ && cache_used) {
                hot_cache_->ReleaseHotKey(key);
            }
            LOG(ERROR) << "Failed to submit transfer operation for key: "
                       << key;
            if (replica.is_memory_replica()) {
                RecordServiceReadFailure(ReplicaType::MEMORY);
            }
            results[i] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            continue;
        }

        VLOG(1) << "Submitted transfer for key " << key
                << " using strategy: " << static_cast<int>(future->strategy());

        pending_transfers.emplace_back(i, key, std::move(*future), replica,
                                       cache_used);
    }

    if (!dfs_read_requests.empty()) {
        auto dfs_results = dfs_storage_backend_->BatchRead(dfs_read_requests);
        std::vector<DfsMissingBucketReport> missing_bucket_reports;
        if (dfs_results.size() != dfs_read_requests.size()) {
            LOG(ERROR) << "DFS BatchRead response size mismatch: expected "
                       << dfs_read_requests.size() << ", got "
                       << dfs_results.size();
            for (size_t index : dfs_read_indices) {
                RecordServiceReadFailure(ReplicaType::DFS);
                results[index] = tl::unexpected(ErrorCode::INTERNAL_ERROR);
            }
        } else {
            for (size_t i = 0; i < dfs_results.size(); ++i) {
                const size_t index = dfs_read_indices[i];
                const auto& request = dfs_read_requests[i];
                if (!dfs_results[i]) {
                    RecordServiceReadFailure(ReplicaType::DFS);
                    if (dfs_results[i].error() == ErrorCode::FILE_NOT_FOUND) {
                        if (missing_bucket_reports.empty()) {
                            missing_bucket_reports.reserve(
                                dfs_read_requests.size());
                        }
                        missing_bucket_reports.push_back(
                            DfsMissingBucketReport{request.descriptor.shard_idx});
                    }
                    results[index] = tl::unexpected(dfs_results[i].error());
                    continue;
                }
                auto checksum_result = VerifyObjectChecksum(
                    request.key, request.slices, request.descriptor.object_size,
                    query_results[index].object_checksum);
                if (!checksum_result) {
                    RecordServiceReadFailure(ReplicaType::DFS);
                    results[index] = tl::unexpected(checksum_result.error());
                    continue;
                }
                results[index] = {};
            }
            if (!missing_bucket_reports.empty()) {
                std::sort(
                    missing_bucket_reports.begin(), missing_bucket_reports.end(),
                    [](const auto& lhs, const auto& rhs) {
                        return lhs.bucket_id < rhs.bucket_id;
                    });
                missing_bucket_reports.erase(
                    std::unique(missing_bucket_reports.begin(),
                                missing_bucket_reports.end(),
                                [](const auto& lhs, const auto& rhs) {
                                    return lhs.bucket_id == rhs.bucket_id;
                                }),
                    missing_bucket_reports.end());
                const auto invalidation_results =
                    master_client_.BatchInvalidateDfsBuckets(
                        missing_bucket_reports);
                if (invalidation_results.size() !=
                    missing_bucket_reports.size()) {
                    LOG(WARNING)
                        << "DFS bucket invalidation response size mismatch: "
                        << "expected " << missing_bucket_reports.size()
                        << ", got "
                        << invalidation_results.size();
                }
                const size_t result_count = std::min(
                    invalidation_results.size(), missing_bucket_reports.size());
                for (size_t i = 0; i < result_count; ++i) {
                    if (!invalidation_results[i]) {
                        LOG(WARNING)
                            << "Failed to invalidate missing DFS bucket: "
                            << missing_bucket_reports[i].bucket_id
                            << ", error: "
                            << toString(invalidation_results[i].error());
                    }
                }
            }
        }
    }

    // Wait for all transfers to complete
    for (auto& [index, key, future, stored_replica, cache_used] :
         pending_transfers) {
        ErrorCode result = future.get();

        // Release the cache block after transfer completes (memcpy is done)
        if (hot_cache_ && cache_used) {
            hot_cache_->ReleaseHotKey(key);
        }
        if (result != ErrorCode::OK) {
            if (stored_replica.is_memory_replica()) {
                RecordServiceReadFailure(ReplicaType::MEMORY);
            }
            LOG(ERROR) << "Transfer failed for key: " << key
                       << " with error: " << static_cast<int>(result);
            results[index] = tl::unexpected(result);
        } else {
            VLOG(1) << "Transfer completed successfully for key: " << key;
            auto checksum_result = VerifyObjectChecksum(
                key, batched_slices[index],
                calculate_total_size(stored_replica),
                query_results[index].object_checksum);
            if (!checksum_result) {
                if (stored_replica.is_memory_replica() ||
                    stored_replica.is_dfs_replica()) {
                    RecordServiceReadFailure(
                        stored_replica.is_memory_replica() ? ReplicaType::MEMORY
                                                           : ReplicaType::DFS);
                }
                results[index] = tl::unexpected(checksum_result.error());
                continue;
            }
            results[index] = {};

            // Frequency admission: only promote frequently accessed keys.
            // Skip when cache was used (data served from local cache).
            if (hot_cache_) {
                if (ShouldAdmitToHotCache(key, cache_used)) {
                    ProcessSlicesAsync(key, batched_slices[index],
                                       stored_replica);
                }
            }
        }
    }

    auto now = std::chrono::steady_clock::now();
    size_t non_dfs_lease_expired_keys = 0;
    for (const auto& [index, key, future, replica, cache_used] :
         pending_transfers) {
        if (results[index].has_value() &&
            query_results[index].IsLeaseExpired(now)) {
            ++non_dfs_lease_expired_keys;
            results[index] = tl::unexpected(ErrorCode::BATCH_GET_LEASE_EXPIRED);
        }
    }
    if (non_dfs_lease_expired_keys != 0) {
        LOG(WARNING) << "BatchGet: non-DFS replicas expired, num_keys="
                     << non_dfs_lease_expired_keys;
    }
    // Only validated reads from an initialized immutable bucket survive expiry.
    const bool immutable_bucket =
        dfs_storage_backend_ && dfs_storage_backend_->IsInitialized() &&
        dfs_storage_backend_->GetAllocatorType() == DfsAllocatorType::BUCKET;
    for (size_t index : dfs_read_indices) {
        if (results[index].has_value() && !immutable_bucket &&
            query_results[index].IsLeaseExpired(now)) {
            results[index] = tl::unexpected(ErrorCode::BATCH_GET_LEASE_EXPIRED);
        }
    }

    auto us_batch_get = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - t0_batch_get)
                            .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_get_latency_us.observe(us_batch_get);
    }

    // Log overall cache hit statistics for the entire batch
    if (hot_cache_) {
        VLOG(1) << "BatchGet completed: num_keys=" << object_keys.size()
                << " total_cache_hits=" << total_cache_hits;
    } else {
        VLOG(1) << "BatchGet completed for " << object_keys.size() << " keys";
    }
    return results;
}

bool Client::RedirectToHotCache(const std::string& key,
                                Replica::Descriptor& replica) {
    if (!replica.is_memory_replica() || !hot_cache_) {
        return false;
    }

    auto& mem_desc = replica.get_memory_descriptor();
    HotMemBlock* blk = hot_cache_->GetHotKey(key);
    if (blk == nullptr) {
        return false;
    }

    if (mem_desc.buffer_descriptor.size_ != blk->size) {
        LOG(ERROR) << "Cache hit but size mismatch for key: " << key;
        return false;
    }

    mem_desc.buffer_descriptor.transport_endpoint_ =
        (metadata_connstring_ == P2PHANDSHAKE) ? GetTransportEndpoint()
                                               : local_hostname_;
    mem_desc.buffer_descriptor.buffer_address_ =
        reinterpret_cast<uintptr_t>(blk->addr);
    return true;
}

tl::expected<void, ErrorCode> Client::Put(const ObjectKey& key,
                                          std::vector<Slice>& slices,
                                          const ReplicateConfig& config) {
    ReplicateConfig client_cfg = AttachConfig(config);
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }
    const bool service_dual_write_eligible =
        client_cfg.replica_num > 0 && client_cfg.dfs_replica_num > 0;

    std::optional<uint64_t> object_checksum;
    if (object_checksum_enabled_) {
        auto checksum_result = ComputeObjectChecksumForSlices(
            key, slices, CalculateSliceSize(slices));
        if (!checksum_result) {
            if (service_dual_write_eligible &&
                checksum_result.error() != ErrorCode::INVALID_PARAMS) {
                RecordServiceDualWrite(false, false);
            }
            return tl::unexpected(checksum_result.error());
        }
        object_checksum = *checksum_result;
    }

    // Prepare slice lengths
    std::vector<size_t> slice_lengths;
    for (size_t i = 0; i < slices.size(); ++i) {
        slice_lengths.emplace_back(slices[i].size);
    }

    if (hot_cache_) {
        hot_cache_->RemoveHotKey(key);
    }

    // Start put operation
    auto start_result = master_client_.PutStart(key, slice_lengths, client_cfg);
    if (!start_result) {
        ErrorCode err = start_result.error();
        if (err == ErrorCode::OBJECT_ALREADY_EXISTS) {
            VLOG(1) << "object_already_exists key=" << key;
            return {};
        }
        if (service_dual_write_eligible && err != ErrorCode::INVALID_PARAMS) {
            RecordServiceDualWrite(false, false);
        }
        if (err == ErrorCode::NO_AVAILABLE_HANDLE) {
            LOG(WARNING) << "Failed to start put operation for key=" << key
                         << PUT_NO_SPACE_HELPER_STR;
        } else {
            LOG(ERROR) << "Failed to start put operation for key=" << key
                       << ": " << toString(err);
        }
        return tl::unexpected(err);
    }

    ReplicaTransferSummary transfer_summary;
    for (const auto& replica : start_result.value()) {
        transfer_summary.RecordAllocatedReplica(replica);
    }

    // Record Put transfer latency (all replicas)
    auto t0_put = std::chrono::steady_clock::now();

    // We must deal with disk replica first, then the disk putrevoke/putend can
    // be called surely
    if (storage_backend_) {
        for (auto it = start_result.value().rbegin();
             it != start_result.value().rend(); ++it) {
            const auto& replica = *it;
            if (replica.is_disk_replica()) {
                // Store to local file if storage backend is available
                auto disk_descriptor = replica.get_disk_descriptor();
                PutToLocalFile(key, slices, disk_descriptor);
                break;  // Only one disk replica is needed
            }
        }
    }

    for (const auto& replica : start_result.value()) {
        if (replica.is_memory_replica() || replica.is_nof_replica()) {
            // Transfer data using allocated handles from all replicas
            const auto replica_type = replica.is_memory_replica()
                                          ? ReplicaType::MEMORY
                                          : ReplicaType::NOF_SSD;
            ErrorCode transfer_err = TransferWrite(replica, slices);
            if (transfer_err != ErrorCode::OK) {
                transfer_summary.RecordFailure(replica_type, transfer_err);
                continue;
            }
            transfer_summary.RecordSuccess(replica_type);
        }
    }

    if (transfer_summary.allocated_dfs_replicas > 0 &&
        NonDfsTransfersSucceeded(transfer_summary)) {
        std::vector<std::string> dfs_keys;
        std::vector<const std::vector<Slice>*> dfs_slices;
        std::vector<DistributedFSDescriptor> dfs_descriptors;
        for (const auto& replica : start_result.value()) {
            if (!replica.is_dfs_replica()) {
                continue;
            }
            dfs_keys.push_back(key);
            dfs_slices.push_back(&slices);
            dfs_descriptors.push_back(replica.get_dfs_descriptor());
        }
        for (auto dfs_result :
             WriteDfsReplicas(dfs_keys, dfs_slices, dfs_descriptors)) {
            if (dfs_result == ErrorCode::OK) {
                transfer_summary.RecordSuccess(ReplicaType::DFS);
            } else {
                transfer_summary.RecordFailure(ReplicaType::DFS, dfs_result);
            }
        }
    }

    if (service_dual_write_eligible) {
        RecordServiceDualWrite(
            transfer_summary.successful_memory_transfers ==
                client_cfg.replica_num,
            transfer_summary.successful_dfs_transfers ==
                client_cfg.dfs_replica_num);
    }

    auto us_put = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0_put)
                      .count();
    if (metrics_) {
        metrics_->transfer_metric.put_latency_us.observe(us_put);
    }

    const auto finalize_decision =
        DetermineFinalizeDecision(client_cfg, transfer_summary,
                                  /*allow_missing_dfs=*/true,
                                  /*allow_memory_only_fallback=*/
                                      dfs_storage_backend_ &&
                                      dfs_storage_backend_->GetAllocatorType() ==
                                          DfsAllocatorType::BUCKET);

    if (finalize_decision.success && finalize_decision.revoke_type ==
                                      ReplicaType::DFS) {
        static DfsLogLimiter log_limiter;
        if (auto suppressed = log_limiter.ShouldLog()) {
            LOG(WARNING) << "Put degraded to memory-only for key " << key
                         << ", suppressed=" << *suppressed;
        }
    }

    std::optional<ErrorCode> finalize_error;
    if (finalize_decision.end_type.has_value()) {
        auto end_result = master_client_.PutEnd(
            ObjectMeta{key, object_checksum}, *finalize_decision.end_type,
            DfsWriteEpoch(*start_result));
        if (!end_result) {
            finalize_error = end_result.error();
            LOG(ERROR) << "Failed to end put operation: " << *finalize_error;
        }
    }

    if (finalize_decision.revoke_type.has_value()) {
        auto revoke_result = master_client_.PutRevoke(
            key, *finalize_decision.revoke_type, DfsWriteEpoch(*start_result));
        if (!revoke_result) {
            if (!finalize_error.has_value()) {
                finalize_error = revoke_result.error();
            }
            LOG(ERROR) << "Failed to revoke put operation: "
                       << toString(revoke_result.error());
        }
    }
    if (finalize_error.has_value()) {
        return tl::unexpected(*finalize_error);
    }

    if (!finalize_decision.success) {
        return tl::unexpected(finalize_decision.error);
    }

    return {};
}

tl::expected<void, ErrorCode> Client::Upsert(const ObjectKey& key,
                                             std::vector<Slice>& slices,
                                             const ReplicateConfig& config) {
    ReplicateConfig client_cfg = AttachConfig(config);
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }
    const bool service_dual_write_eligible =
        client_cfg.replica_num > 0 && client_cfg.dfs_replica_num > 0;

    std::optional<uint64_t> object_checksum;
    if (object_checksum_enabled_) {
        auto checksum_result = ComputeObjectChecksumForSlices(
            key, slices, CalculateSliceSize(slices));
        if (!checksum_result) {
            if (service_dual_write_eligible &&
                checksum_result.error() != ErrorCode::INVALID_PARAMS) {
                RecordServiceDualWrite(false, false);
            }
            return tl::unexpected(checksum_result.error());
        }
        object_checksum = *checksum_result;
    }

    // Prepare slice lengths
    std::vector<size_t> slice_lengths;
    for (size_t i = 0; i < slices.size(); ++i) {
        slice_lengths.emplace_back(slices[i].size);
    }

    if (hot_cache_) {
        hot_cache_->RemoveHotKey(key);
    }

    // Start upsert operation
    auto start_result =
        master_client_.UpsertStart(key, slice_lengths, client_cfg);
    if (!start_result) {
        ErrorCode err = start_result.error();
        if (service_dual_write_eligible && err != ErrorCode::INVALID_PARAMS) {
            RecordServiceDualWrite(false, false);
        }
        if (err == ErrorCode::NO_AVAILABLE_HANDLE) {
            LOG(WARNING) << "Failed to start upsert operation for key=" << key
                         << PUT_NO_SPACE_HELPER_STR;
        } else {
            LOG(ERROR) << "Failed to start upsert operation for key=" << key
                       << ": " << toString(err);
        }
        return tl::unexpected(err);
    }

    ReplicaTransferSummary transfer_summary;
    for (const auto& replica : start_result.value()) {
        transfer_summary.RecordAllocatedReplica(replica);
    }

    // Record transfer latency
    auto t0 = std::chrono::steady_clock::now();

    // Handle disk replicas first
    if (storage_backend_) {
        for (auto it = start_result.value().rbegin();
             it != start_result.value().rend(); ++it) {
            const auto& replica = *it;
            if (replica.is_disk_replica()) {
                auto disk_descriptor = replica.get_disk_descriptor();
                PutToLocalFile(key, slices, disk_descriptor);
                break;
            }
        }
    }

    // Transfer to memory and NoF replicas first.
    for (const auto& replica : start_result.value()) {
        if (replica.is_memory_replica() || replica.is_nof_replica()) {
            const auto replica_type = replica.is_memory_replica()
                                          ? ReplicaType::MEMORY
                                          : ReplicaType::NOF_SSD;
            ErrorCode transfer_err = TransferWrite(replica, slices);
            if (transfer_err != ErrorCode::OK) {
                transfer_summary.RecordFailure(replica_type, transfer_err);
                continue;
            }
            transfer_summary.RecordSuccess(replica_type);
        }
    }

    if (transfer_summary.allocated_dfs_replicas > 0 &&
        NonDfsTransfersSucceeded(transfer_summary)) {
        std::vector<std::string> dfs_keys;
        std::vector<const std::vector<Slice>*> dfs_slices;
        std::vector<DistributedFSDescriptor> dfs_descriptors;
        for (const auto& replica : start_result.value()) {
            if (!replica.is_dfs_replica()) {
                continue;
            }
            dfs_keys.push_back(key);
            dfs_slices.push_back(&slices);
            dfs_descriptors.push_back(replica.get_dfs_descriptor());
        }
        for (auto dfs_result :
             WriteDfsReplicas(dfs_keys, dfs_slices, dfs_descriptors)) {
            if (dfs_result == ErrorCode::OK) {
                transfer_summary.RecordSuccess(ReplicaType::DFS);
            } else {
                transfer_summary.RecordFailure(ReplicaType::DFS, dfs_result);
            }
        }
    }

    if (service_dual_write_eligible) {
        RecordServiceDualWrite(
            transfer_summary.successful_memory_transfers ==
                client_cfg.replica_num,
            transfer_summary.successful_dfs_transfers ==
                client_cfg.dfs_replica_num);
    }

    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    if (metrics_) {
        metrics_->transfer_metric.put_latency_us.observe(us);
    }

    const auto finalize_decision =
        DetermineFinalizeDecision(config, transfer_summary);
    std::optional<ErrorCode> finalize_error;
    if (finalize_decision.end_type.has_value()) {
        auto end_result = master_client_.UpsertEnd(
            ObjectMeta{key, object_checksum}, *finalize_decision.end_type,
            DfsWriteEpoch(*start_result));
        if (!end_result) {
            finalize_error = end_result.error();
            LOG(ERROR) << "Failed to end upsert operation: " << *finalize_error;
        }
    }
    if (finalize_decision.revoke_type.has_value()) {
        auto revoke_result = master_client_.UpsertRevoke(
            key, *finalize_decision.revoke_type, DfsWriteEpoch(*start_result));
        if (!revoke_result) {
            if (!finalize_error.has_value()) {
                finalize_error = revoke_result.error();
            }
            LOG(ERROR) << "Failed to revoke upsert operation: "
                       << toString(revoke_result.error());
        }
    }
    if (finalize_error.has_value()) {
        return tl::unexpected(*finalize_error);
    }
    if (!finalize_decision.success) {
        return tl::unexpected(finalize_decision.error);
    }

    // Success-side invalidation: a concurrent read between the pre-upsert
    // RemoveHotKey() and UpsertEnd could have read the old value and submitted
    // an async hot-cache fill with a still-valid token. Invalidate again now
    // that the new value is committed so that stale fill cannot publish.
    if (hot_cache_) {
        hot_cache_->RemoveHotKey(key);
    }

    return {};
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchUpsert(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const ReplicateConfig& config) {
    return BatchUpsert(keys, batched_slices, config, {});
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchUpsert(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const ReplicateConfig& config, const WriteBufferStager& stager) {
    ReplicateConfig client_cfg = AttachConfig(config);
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }
    std::vector<PutOperation> ops =
        CreatePutOperations(keys, batched_slices, client_cfg);
    ComputeBatchObjectChecksums(ops);
    if (client_cfg.prefer_alloc_in_same_node) {
        if (auto err = ValidatePreferSameNodeWriteConfig(client_cfg)) {
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(*err));
        }
        StartBatchUpsert(ops, client_cfg);
        StageWriteBuffersForRemoteReplicas(ops, stager);
        return BatchWriteWhenPreferSameNode(ops, true);
    }

    StartBatchUpsert(ops, client_cfg);
    StageWriteBuffersForRemoteReplicas(ops, stager);
    auto t0 = std::chrono::steady_clock::now();
    SubmitTransfers(ops);
    WaitForTransfers(ops);
    // Upserts keep the synchronous DFS write path: FinalizeBatchUpsert settles
    // the object with BatchUpsertEnd/Revoke, which act on ReplicaType::ALL and
    // would therefore complete (or revoke) a DFS replica still owned by a
    // background task.
    SubmitDfsWrites(ops, /*is_upsert=*/true, /*allow_async=*/false);
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_put_latency_us.observe(us);
    }

    RecordBatchServiceDualWriteMetrics(ops);
    FinalizeBatchUpsert(ops);
    return CollectResults(ops);
}

// TODO: `client.cpp` is too long, consider split it into multiple files
enum class PutOperationState {
    PENDING,
    MASTER_FAILED,
    TRANSFER_FAILED,
    FINALIZE_FAILED,
    SUCCESS
};

class PutOperation {
   public:
    struct PendingTransferRecord {
        ReplicaType replica_type;
        TransferFuture future;

        PendingTransferRecord(ReplicaType type,
                              TransferFuture&& transfer_future)
            : replica_type(type), future(std::move(transfer_future)) {}
    };

    PutOperation(std::string_view k, const std::vector<Slice>& s)
        : key(k), slices(s) {
        // Initialize with a pending error state to ensure result is always set
        result = tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    std::string key;
    std::vector<Slice> slices;
    std::optional<uint64_t> object_checksum;
    std::vector<std::vector<Slice>> batched_slices;

    // Enhanced state tracking
    PutOperationState state = PutOperationState::PENDING;
    tl::expected<void, ErrorCode> result;
    std::vector<Replica::Descriptor> replicas;
    std::vector<PendingTransferRecord> pending_transfers;

    size_t requested_memory_replicas = 0;
    size_t requested_nof_replicas = 0;
    size_t requested_dfs_replicas = 0;
    bool service_dual_write_eligible = false;
    bool service_metric_recorded = false;
    bool service_metric_excluded = false;
    ReplicaTransferSummary transfer_summary;

    // Error context for debugging
    std::optional<std::string> failure_context;
    // Helper methods for robust state management
    void SetSuccess() {
        state = PutOperationState::SUCCESS;
        result = {};
        failure_context.reset();
    }

    void SetTerminalError(ErrorCode error, PutOperationState terminal_state,
                          const std::string& context = "") {
        state = terminal_state;
        result = tl::unexpected(error);
        if (!context.empty()) {
            failure_context = context;
        }
    }

    void SetError(ErrorCode error, const std::string& context = "") {
        result = tl::unexpected(error);
        if (!context.empty()) {
            failure_context = toString(error) + ": " + context + "; " +
                              failure_context.value_or("");
        }

        // Update state based on current processing stage
        if (replicas.empty()) {
            state = PutOperationState::MASTER_FAILED;
        } else if (pending_transfers.empty()) {
            state = PutOperationState::TRANSFER_FAILED;
        } else {
            state = PutOperationState::FINALIZE_FAILED;
        }
    }

    void AppendFailureContext(const std::string& context) {
        if (context.empty()) {
            return;
        }
        if (!failure_context.has_value()) {
            failure_context = context;
            return;
        }
        failure_context = *failure_context + "; " + context;
    }

    void InitializeRequestedReplicas(const ReplicateConfig& config) {
        requested_memory_replicas = config.replica_num;
        requested_nof_replicas = config.nof_replica_num;
        requested_dfs_replicas = config.dfs_replica_num;
        service_dual_write_eligible = requested_memory_replicas > 0 &&
                                      requested_dfs_replicas > 0;
    }

    ReplicateConfig ToReplicateConfig() const {
        ReplicateConfig config;
        config.replica_num = requested_memory_replicas;
        config.nof_replica_num = requested_nof_replicas;
        config.dfs_replica_num = requested_dfs_replicas;
        return config;
    }

    void RecordAllocatedReplicas() {
        transfer_summary = ReplicaTransferSummary{};
        for (const auto& replica : replicas) {
            transfer_summary.RecordAllocatedReplica(replica);
        }
    }

    bool IsResolved() const { return state != PutOperationState::PENDING; }

    bool IsSuccessful() const {
        return state == PutOperationState::SUCCESS && result.has_value();
    }
};

std::vector<PutOperation> Client::CreatePutOperations(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& batched_slices,
    const ReplicateConfig& config) {
    std::vector<PutOperation> ops;
    ops.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        ops.emplace_back(keys[i], batched_slices[i]);
        ops.back().InitializeRequestedReplicas(config);
    }
    return ops;
}

void Client::RecordBatchServiceDualWriteMetrics(
    std::vector<PutOperation>& ops) {
    for (auto& op : ops) {
        if (!op.service_dual_write_eligible || op.service_metric_recorded ||
            op.service_metric_excluded ||
            op.transfer_summary.dfs_completion_deferred) {
            continue;
        }
        if (!op.result.has_value() &&
            op.result.error() == ErrorCode::OBJECT_ALREADY_EXISTS) {
            continue;
        }

        const bool memory_succeeded =
            op.transfer_summary.successful_memory_transfers ==
            op.requested_memory_replicas;
        const bool dfs_succeeded =
            op.transfer_summary.successful_dfs_transfers ==
            op.requested_dfs_replicas;
        RecordServiceDualWrite(memory_succeeded, dfs_succeeded);
        op.service_metric_recorded = true;
    }
}

void Client::ComputeBatchObjectChecksums(std::vector<PutOperation>& ops) {
    for (auto& op : ops) {
        ComputePutObjectChecksum(op, op.slices);
    }
}

void Client::ComputePutObjectChecksum(PutOperation& op,
                                      const std::vector<Slice>& slices,
                                      bool staged_for_dfs) {
    if (!object_checksum_enabled_ || op.IsResolved() ||
        op.object_checksum.has_value()) {
        return;
    }
    try {
        auto checksum_result = ComputeObjectChecksumForSlices(
            op.key, slices, CalculateSliceSize(op.slices), staged_for_dfs);
        if (!checksum_result) {
            if (checksum_result.error() == ErrorCode::INVALID_PARAMS) {
                op.service_metric_excluded = true;
            }
            op.SetTerminalError(checksum_result.error(),
                                op.replicas.empty()
                                    ? PutOperationState::MASTER_FAILED
                                    : PutOperationState::TRANSFER_FAILED,
                                "Object checksum calculation failed");
            return;
        }
        op.object_checksum = *checksum_result;
    } catch (const std::exception& e) {
        op.SetTerminalError(
            ErrorCode::INTERNAL_ERROR,
            op.replicas.empty() ? PutOperationState::MASTER_FAILED
                                : PutOperationState::TRANSFER_FAILED,
            "Object checksum calculation threw: " + std::string(e.what()));
    }
}

void Client::StartBatchPut(std::vector<PutOperation>& ops,
                           const ReplicateConfig& config) {
    std::vector<std::string> keys;
    std::vector<std::vector<uint64_t>> slice_lengths;
    std::vector<size_t> active_indices;

    keys.reserve(ops.size());
    slice_lengths.reserve(ops.size());
    active_indices.reserve(ops.size());

    if (hot_cache_) {
        std::vector<std::string> hot_keys;
        hot_keys.reserve(ops.size());
        for (const auto& op : ops) hot_keys.emplace_back(op.key);
        hot_cache_->RemoveHotKeys(hot_keys);
    }

    for (size_t i = 0; i < ops.size(); ++i) {
        const auto& op = ops[i];
        if (op.IsResolved()) {
            continue;
        }
        active_indices.emplace_back(i);
        keys.emplace_back(op.key);

        std::vector<uint64_t> slice_sizes;
        slice_sizes.reserve(op.slices.size());
        for (const auto& slice : op.slices) {
            slice_sizes.emplace_back(slice.size);
        }
        slice_lengths.emplace_back(std::move(slice_sizes));
    }

    if (active_indices.empty()) {
        return;
    }

    auto start_responses =
        master_client_.BatchPutStart(keys, slice_lengths, config);

    // Ensure response size matches request size
    if (start_responses.size() != active_indices.size()) {
        LOG(ERROR) << "BatchPutStart response size mismatch: expected "
                   << active_indices.size() << ", got "
                   << start_responses.size();
        for (size_t index : active_indices) {
            auto& op = ops[index];
            op.SetError(ErrorCode::RPC_FAIL,
                        "BatchPutStart response size mismatch");
        }
        return;
    }

    // Process individual responses with robust error handling
    for (size_t i = 0; i < active_indices.size(); ++i) {
        auto& op = ops[active_indices[i]];
        op.InitializeRequestedReplicas(config);
        if (!start_responses[i]) {
            if (start_responses[i].error() == ErrorCode::INVALID_PARAMS) {
                op.service_metric_excluded = true;
            }
            op.SetTerminalError(start_responses[i].error(),
                                PutOperationState::MASTER_FAILED,
                                "Master failed to start put operation");
        } else {
            op.replicas = start_responses[i].value();
            op.RecordAllocatedReplicas();
            if (!HasExpectedReplicaAllocation(config, op.transfer_summary,
                                              /*allow_missing_dfs=*/true)) {
                op.SetTerminalError(ErrorCode::NO_AVAILABLE_HANDLE,
                                    PutOperationState::MASTER_FAILED,
                                    "Allocated replicas do not satisfy "
                                    "requested replica policy");
                continue;
            }
            // Operation continues to next stage - result remains INTERNAL_ERROR
            // until fully successful
            VLOG(1) << "Successfully started put for key " << op.key << " with "
                    << op.replicas.size() << " replicas";
        }
    }
}

void Client::StartBatchUpsert(std::vector<PutOperation>& ops,
                              const ReplicateConfig& config) {
    std::vector<std::string> keys;
    std::vector<std::vector<uint64_t>> slice_lengths;
    std::vector<size_t> active_indices;

    keys.reserve(ops.size());
    slice_lengths.reserve(ops.size());
    active_indices.reserve(ops.size());

    if (hot_cache_) {
        std::vector<std::string> hot_keys;
        hot_keys.reserve(ops.size());
        for (const auto& op : ops) hot_keys.emplace_back(op.key);
        hot_cache_->RemoveHotKeys(hot_keys);
    }

    for (size_t i = 0; i < ops.size(); ++i) {
        const auto& op = ops[i];
        if (op.IsResolved()) {
            continue;
        }
        active_indices.emplace_back(i);
        keys.emplace_back(op.key);

        std::vector<uint64_t> slice_sizes;
        slice_sizes.reserve(op.slices.size());
        for (const auto& slice : op.slices) {
            slice_sizes.emplace_back(slice.size);
        }
        slice_lengths.emplace_back(std::move(slice_sizes));
    }

    if (active_indices.empty()) {
        return;
    }

    auto start_responses =
        master_client_.BatchUpsertStart(keys, slice_lengths, config);

    // Ensure response size matches request size
    if (start_responses.size() != active_indices.size()) {
        LOG(ERROR) << "BatchUpsertStart response size mismatch: expected "
                   << active_indices.size() << ", got "
                   << start_responses.size();
        for (size_t index : active_indices) {
            auto& op = ops[index];
            op.SetError(ErrorCode::RPC_FAIL,
                        "BatchUpsertStart response size mismatch");
        }
        return;
    }

    // Process individual responses with robust error handling
    for (size_t i = 0; i < active_indices.size(); ++i) {
        auto& op = ops[active_indices[i]];
        op.InitializeRequestedReplicas(config);
        if (!start_responses[i]) {
            if (start_responses[i].error() == ErrorCode::INVALID_PARAMS) {
                op.service_metric_excluded = true;
            }
            op.SetTerminalError(start_responses[i].error(),
                                PutOperationState::MASTER_FAILED,
                                "Master failed to start upsert operation");
        } else {
            op.replicas = start_responses[i].value();
            op.RecordAllocatedReplicas();
            if (!HasExpectedReplicaAllocation(config, op.transfer_summary)) {
                op.SetTerminalError(ErrorCode::NO_AVAILABLE_HANDLE,
                                    PutOperationState::MASTER_FAILED,
                                    "Allocated replicas do not satisfy "
                                    "requested replica policy");
                continue;
            }
            VLOG(1) << "Successfully started upsert for key " << op.key
                    << " with " << op.replicas.size() << " replicas";
        }
    }
}

void Client::SubmitTransfers(std::vector<PutOperation>& ops) {
    if (std::all_of(ops.begin(), ops.end(),
                    [](const PutOperation& op) { return op.IsResolved(); })) {
        return;
    }
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        for (auto& op : ops) {
            if (op.IsResolved()) {
                continue;
            }
            op.SetTerminalError(ErrorCode::INVALID_PARAMS,
                                PutOperationState::TRANSFER_FAILED,
                                "TransferSubmitter not initialized");
        }
        return;
    }

    for (auto& op : ops) {
        // Skip operations that already failed in previous stages
        if (op.IsResolved()) {
            continue;
        }

        // Skip operations that don't have replicas (failed in StartBatchPut)
        if (op.replicas.empty()) {
            op.SetTerminalError(ErrorCode::INTERNAL_ERROR,
                                PutOperationState::MASTER_FAILED,
                                "No replicas available for transfer");
            continue;
        }

        // We must deal with disk replica first, then the disk putrevoke/putend
        // can be called surely
        if (storage_backend_) {
            for (auto it = op.replicas.rbegin(); it != op.replicas.rend();
                 ++it) {
                const auto& replica = *it;
                if (replica.is_disk_replica()) {
                    auto disk_descriptor = replica.get_disk_descriptor();
                    PutToLocalFile(op.key, op.slices, disk_descriptor);
                    break;  // Only one disk replica is needed
                }
            }
        }

        for (size_t replica_idx = 0; replica_idx < op.replicas.size();
             ++replica_idx) {
            const auto& replica = op.replicas[replica_idx];
            if (replica.is_memory_replica() || replica.is_nof_replica()) {
                const auto replica_type = replica.is_memory_replica()
                                              ? ReplicaType::MEMORY
                                              : ReplicaType::NOF_SSD;
                std::optional<TransferFuture> submit_result;
                if (replica.is_nof_replica()) {
                    auto contiguous_range = GetContiguousSliceRange(op.slices);
                    if (!contiguous_range.has_value()) {
                        std::string failure_context =
                            "NoF transfer requires contiguous slices for "
                            "replica " +
                            std::to_string(replica_idx);
                        op.transfer_summary.RecordFailure(
                            replica_type, ErrorCode::INVALID_PARAMS);
                        op.AppendFailureContext(failure_context);
                        continue;
                    }
                    submit_result = transfer_submitter_->submit(
                        replica, op.slices, TransferRequest::WRITE,
                        contiguous_range->ptr, contiguous_range->size);
                } else {
                    submit_result = transfer_submitter_->submit(
                        replica, op.slices, TransferRequest::WRITE);
                }

                if (!submit_result) {
                    std::string failure_context =
                        "Failed to submit transfer for replica " +
                        std::to_string(replica_idx);
                    op.transfer_summary.RecordFailure(replica_type,
                                                      ErrorCode::TRANSFER_FAIL);
                    op.AppendFailureContext(failure_context);
                    continue;
                }

                op.pending_transfers.emplace_back(
                    replica_type, std::move(submit_result.value()));
            }
        }

        VLOG(1) << "Submitted " << op.pending_transfers.size()
                << " transfers for key " << op.key;
    }
}

void Client::WaitForTransfers(std::vector<PutOperation>& ops) {
    for (auto& op : ops) {
        // Skip operations that already failed or completed
        if (op.IsResolved()) {
            continue;
        }

        for (size_t i = 0; i < op.pending_transfers.size(); ++i) {
            auto& pending_transfer = op.pending_transfers[i];
            ErrorCode transfer_result = pending_transfer.future.get();
            if (transfer_result != ErrorCode::OK) {
                op.transfer_summary.RecordFailure(pending_transfer.replica_type,
                                                  transfer_result);
                std::string error_context =
                    "Transfer " + std::to_string(i) + " failed";
                op.AppendFailureContext(error_context);
            } else {
                op.transfer_summary.RecordSuccess(
                    pending_transfer.replica_type);
            }
        }

        VLOG(1) << "Transfers finished for key " << op.key << ", success(mem="
                << op.transfer_summary.successful_memory_transfers
                << ", nof=" << op.transfer_summary.successful_nof_transfers
                << "), fail(mem=" << op.transfer_summary.failed_memory_transfers
                << ", nof=" << op.transfer_summary.failed_nof_transfers << ")";
    }
}

std::vector<ErrorCode> Client::WriteDfsReplicas(
    const std::vector<std::string>& keys,
    const std::vector<const std::vector<Slice>*>& slice_lists,
    const std::vector<DistributedFSDescriptor>& descriptors,
    std::shared_ptr<DistributedStorageBackend> backend) {
    if (keys.size() != slice_lists.size() ||
        keys.size() != descriptors.size()) {
        return std::vector<ErrorCode>(keys.size(), ErrorCode::INVALID_PARAMS);
    }
    if (keys.empty()) {
        return {};
    }
    if (!backend) backend = dfs_storage_backend_;
    if (!backend) {
        LOG(ERROR) << "DFS backend is unavailable for synchronous write";
        return std::vector<ErrorCode>(keys.size(),
                                      ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }

    std::vector<ErrorCode> results(keys.size(), ErrorCode::OK);
    std::vector<DfsWriteRequest> requests;
    std::vector<size_t> request_indices;
    std::vector<PinnedBufferPool::Buffer> staging_buffers;
    requests.reserve(keys.size());
    request_indices.reserve(keys.size());

    auto runtime_accelerator =
        device::GetAcceleratorRegistry().RuntimeAccelerators();
    for (size_t i = 0; i < keys.size(); ++i) {
        if (slice_lists[i] == nullptr) {
            results[i] = ErrorCode::INVALID_PARAMS;
            continue;
        }

        std::vector<Slice> host_slices;
        host_slices.reserve(slice_lists[i]->size());
        bool staging_succeeded = true;
        for (const auto& slice : *slice_lists[i]) {
            device::PointerInfo info{};
            auto* device = slice.ptr == nullptr
                               ? nullptr
                               : runtime_accelerator.FindDeviceForPointer(
                                     slice.ptr, &info);
            if (device == nullptr) {
                host_slices.push_back(slice);
                continue;
            }

            device->SetContext(info.device_id);
            auto buffer = pinned_buffer_pool_->Acquire(slice.size);
            if (!device->Copy(buffer.data, slice.ptr, slice.size,
                              device::CopyDirection::kDeviceToHost)) {
                LOG(ERROR) << "DFS D2H staging failed for key " << keys[i];
                pinned_buffer_pool_->Release(std::move(buffer));
                results[i] = ErrorCode::TRANSFER_FAIL;
                staging_succeeded = false;
                break;
            }
            host_slices.emplace_back(Slice{buffer.data, slice.size});
            staging_buffers.push_back(std::move(buffer));
        }
        if (!staging_succeeded) {
            continue;
        }

        requests.push_back(
            DfsWriteRequest{keys[i], descriptors[i], std::move(host_slices)});
        request_indices.push_back(i);
    }

    const auto write_started = std::chrono::steady_clock::now();
    auto write_results = backend->BatchWrite(requests);
    const double write_duration_seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                      write_started)
            .count();
    uint64_t successful_bytes = 0;
    bool all_succeeded = requests.size() == keys.size() &&
                         !requests.empty() &&
                         write_results.size() == requests.size();
    if (write_results.size() != requests.size()) {
        LOG(ERROR) << "DFS BatchWrite response size mismatch: expected "
                   << requests.size() << ", got " << write_results.size();
        for (size_t index : request_indices) {
            results[index] = ErrorCode::INTERNAL_ERROR;
        }
    } else {
        for (size_t i = 0; i < write_results.size(); ++i) {
            results[request_indices[i]] =
                write_results[i] ? ErrorCode::OK : write_results[i].error();
            if (write_results[i]) {
                for (const auto& slice : requests[i].slices) {
                    successful_bytes += slice.size;
                }
            } else {
                all_succeeded = false;
            }
        }
    }
    ObserveDirectIo("write", DirectStorageMetricSource(ReplicaType::DFS),
                    all_succeeded, successful_bytes,
                    write_duration_seconds);

    for (auto& buffer : staging_buffers) {
        pinned_buffer_pool_->Release(std::move(buffer));
    }
    return results;
}

void Client::AsyncDfsWriteContext::ReleaseStagingSlot() {
    if (!staging_pool || !staging_slot) return;
    staging_pool->Release(std::move(staging_slot));
}

Client::AsyncDfsWriteContext::~AsyncDfsWriteContext() {
    slices.clear();
    ReleaseStagingSlot();
}

bool Client::StageDfsWriteData(
    AsyncDfsWriteContext& context,
    const std::vector<const std::vector<Slice>*>& slice_lists) {
    if (!context.staging_slot || !context.staging_slot->backing.data) return false;

    auto runtime_accelerator =
        device::GetAcceleratorRegistry().RuntimeAccelerators();

    size_t staging_offset = 0;
    context.slices.resize(slice_lists.size());
    for (size_t i = 0; i < slice_lists.size(); ++i) {
        if (slice_lists[i] == nullptr) {
            LOG(ERROR) << "Missing slices for async DFS write of key "
                       << context.keys[i];
            return false;
        }
        for (const auto& slice : *slice_lists[i]) {
            if (slice.size == 0) continue;
            if (slice.ptr == nullptr) {
                LOG(ERROR) << "Null slice for async DFS write of key "
                           << context.keys[i];
                return false;
            }

            if (staging_offset > context.staging_slot->backing.capacity ||
                slice.size > context.staging_slot->backing.capacity -
                                 staging_offset) {
                LOG(ERROR) << "DFS write staging arena exhausted for key "
                           << context.keys[i];
                return false;
            }
            char* destination =
                context.staging_slot->backing.data + staging_offset;
            staging_offset += slice.size;

            device::PointerInfo info{};
            auto* device =
                runtime_accelerator.FindDeviceForPointer(slice.ptr, &info);
            if (device != nullptr) {
                device->SetContext(info.device_id);
                if (!device->Copy(destination, slice.ptr, slice.size,
                                  device::CopyDirection::kDeviceToHost)) {
                    LOG(ERROR) << "DFS D2H staging failed for key "
                               << context.keys[i];
                    return false;
                }
            } else {
                std::memcpy(destination, slice.ptr, slice.size);
            }
            context.slices[i].push_back(Slice{destination, slice.size});
        }
    }
    return true;
}

void Client::RunAsyncDfsWrite(std::shared_ptr<AsyncDfsWriteContext> context) {
    std::vector<DfsWriteRequest> requests;
    requests.reserve(context->keys.size());
    for (size_t i = 0; i < context->keys.size(); ++i) {
        requests.push_back(DfsWriteRequest{
            context->keys[i], context->descriptors[i], context->slices[i]});
    }

    // Every request gets a definite outcome: a missing or short result vector is
    // treated as failure for the affected keys rather than silently ignored.
    std::vector<ErrorCode> outcomes(requests.size(),
                                    ErrorCode::INTERNAL_ERROR);
    const auto write_started = std::chrono::steady_clock::now();
    std::vector<tl::expected<void, ErrorCode>> results;
    try {
        results = context->backend->BatchWrite(requests);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Async DFS BatchWrite failed: " << e.what();
        results.assign(requests.size(),
                       tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
    } catch (...) {
        LOG(ERROR) << "Async DFS BatchWrite failed";
        results.assign(requests.size(),
                       tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
    }
    // Once the worker observes a result, it owns outcome classification. The
    // reporter must not turn a timely result into a timeout while we process
    // it.
    context->io_completed.store(true);
    const double write_duration_seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                      write_started)
            .count();
    uint64_t successful_bytes = 0;
    bool all_succeeded = !requests.empty() && results.size() == requests.size();
    if (results.size() != requests.size()) {
        LOG(ERROR) << "Async DFS BatchWrite response size mismatch: expected "
                   << requests.size() << ", got " << results.size();
    } else {
        for (size_t i = 0; i < results.size(); ++i) {
            outcomes[i] = results[i] ? ErrorCode::OK : results[i].error();
            if (results[i]) {
                for (const auto& slice : requests[i].slices) {
                    successful_bytes += slice.size;
                }
            } else {
                all_succeeded = false;
            }
        }
    }

    const bool timed_out =
        context->deadline != std::chrono::steady_clock::time_point::max() &&
        std::chrono::steady_clock::now() > context->deadline;
    if (timed_out) {
        // The foreground has already published any successful memory replica.
        // Do not publish a DFS replica whose write missed its deadline; revoke
        // only that replica after the blocking I/O has actually returned.
        std::fill(outcomes.begin(), outcomes.end(),
                  ErrorCode::DFS_BATCH_PUT_TIMEOUT);
        all_succeeded = false;
        successful_bytes = 0;
    }

    CompleteAsyncDfsWriteMetrics(context, outcomes);

    ObserveDirectIo("write", DirectStorageMetricSource(ReplicaType::DFS),
                    all_succeeded, successful_bytes,
                    write_duration_seconds);

    // BatchWrite is synchronous: once it returns, neither the backend nor the
    // local request views use the staging bytes. Release the slot before the
    // completion RPCs so another chunk can stage while PutEnd/PutRevoke runs.
    requests.clear();
    context->slices.clear();
    context->ReleaseStagingSlot();

    // Report each key individually so one failure cannot revoke its neighbours.
    // The RPCs are idempotent on the master side, so a bounded retry is safe.
    constexpr int kMaxCompletionAttempts = 3;
    for (size_t i = 0; i < outcomes.size(); ++i) {
        const auto& key = context->keys[i];
        const bool succeeded = outcomes[i] == ErrorCode::OK;
        if (!succeeded) {
            static DfsLogLimiter log_limiter;
            if (auto suppressed = log_limiter.ShouldLog()) {
                LOG(ERROR) << "Async DFS write failed for key " << key
                           << ", error=" << toString(outcomes[i])
                           << "; revoking the DFS replica"
                           << ", suppressed=" << *suppressed;
            }
        }

        tl::expected<void, ErrorCode> completion =
            tl::make_unexpected(ErrorCode::RPC_FAIL);
        const auto object_checksum = context->object_checksums[i];
        for (int attempt = 0; attempt < kMaxCompletionAttempts; ++attempt) {
            if (succeeded) {
                completion = context->is_upsert
                                 ? master_client_.UpsertEnd(
                                       ObjectMeta{key, object_checksum},
                                       ReplicaType::DFS,
                                       context->descriptors[i].MasterEpoch())
                                 : master_client_.PutEnd(
                                       ObjectMeta{key, object_checksum},
                                       ReplicaType::DFS,
                                       context->descriptors[i].MasterEpoch());
            } else {
                completion = context->is_upsert
                                 ? master_client_.UpsertRevoke(
                                       key, ReplicaType::DFS,
                                       context->descriptors[i].MasterEpoch())
                                 : master_client_.PutRevoke(
                                       key, ReplicaType::DFS,
                                       context->descriptors[i].MasterEpoch());
            }
            if (completion) break;
            // OBJECT_NOT_FOUND / INVALID_WRITE mean the object already moved on
            // (removed, or replaced by a newer generation). Retrying cannot help
            // and must not disturb the new state.
            const auto error = completion.error();
            if (error == ErrorCode::OBJECT_NOT_FOUND ||
                error == ErrorCode::INVALID_WRITE ||
                error == ErrorCode::ILLEGAL_CLIENT) {
                break;
            }
        }
        if (!completion) {
            LOG(ERROR) << "Failed to finalize async DFS write for key " << key
                       << " (" << (succeeded ? "PutEnd" : "PutRevoke")
                       << "), error=" << toString(completion.error())
                       << "; the master will reclaim the PROCESSING replica via "
                          "its put-start timeout";
        }
    }
}

void Client::SubmitDfsWriteChunk(
    std::vector<PutOperation>& ops, const std::vector<size_t>& chunk_indices,
    bool is_upsert, bool allow_async, bool* slot_timed_out,
    std::chrono::steady_clock::time_point deadline) {
    std::vector<std::string> keys;
    std::vector<const std::vector<Slice>*> slice_lists;
    std::vector<DistributedFSDescriptor> descriptors;
    std::vector<size_t> op_indices;
    const bool bucket_mode =
        dfs_storage_backend_ != nullptr &&
        dfs_storage_backend_->GetAllocatorType() == DfsAllocatorType::BUCKET;

    for (size_t i : chunk_indices) {
        auto& op = ops[i];
        if (op.IsResolved() ||
            op.transfer_summary.allocated_dfs_replicas == 0 ||
            !NonDfsTransfersSucceeded(op.transfer_summary)) {
            continue;
        }
        // In BUCKET mode, an incomplete requested topology will be revoked.
        // Skipping the DFS write keeps that revoke unambiguous instead of
        // racing a background completion.
        if (bucket_mode &&
            !HasExpectedReplicaAllocation(op.ToReplicateConfig(),
                                          op.transfer_summary)) {
            continue;
        }

        auto dfs_it = std::find_if(op.replicas.begin(), op.replicas.end(),
                                   [](const Replica::Descriptor& replica) {
                                       return replica.is_dfs_replica();
                                   });
        if (dfs_it == op.replicas.end()) {
            op.transfer_summary.RecordFailure(ReplicaType::DFS,
                                              ErrorCode::INVALID_REPLICA);
            op.AppendFailureContext("Allocated DFS replica has no descriptor");
            continue;
        }
        keys.push_back(op.key);
        slice_lists.push_back(&op.slices);
        descriptors.push_back(dfs_it->get_dfs_descriptor());
        op_indices.push_back(i);
    }

    if (keys.empty()) return;

    auto backend = dfs_storage_backend_;
    if (!backend) {
        // No backend: fail every affected key here instead of queueing a task
        // that would dereference a null pointer.
        LOG(ERROR) << "DFS backend is unavailable; failing " << keys.size()
                   << " DFS writes";
        for (const size_t index : op_indices) {
            auto& op = ops[index];
            op.transfer_summary.RecordFailure(
                ReplicaType::DFS, ErrorCode::DFS_SERVICE_UNAVAILABLE);
            op.AppendFailureContext("DFS backend is not initialized");
        }
        return;
    }

    // Restore the allocator's original contract: BUCKET writes are staged and
    // asynchronous, while SHARD writes remain synchronous. The configured
    // deadline limits publication of an asynchronous DFS replica; it does not
    // turn every allocator into a detached write path.
    const bool async_write =
        allow_async && backend->GetAllocatorType() == DfsAllocatorType::BUCKET;
    if (!async_write) {
        auto results = WriteDfsReplicas(keys, slice_lists, descriptors);
        for (size_t i = 0; i < results.size(); ++i) {
            auto& op = ops[op_indices[i]];
            if (results[i] == ErrorCode::OK) {
                op.transfer_summary.RecordSuccess(ReplicaType::DFS);
            } else {
                op.transfer_summary.RecordFailure(ReplicaType::DFS, results[i]);
                op.AppendFailureContext("Synchronous DFS write failed: " +
                                        toString(results[i]));
            }
        }
        return;
    }

    std::shared_ptr<AsyncDfsWriteContext> context;
    bool staged = false;
    try {
        context = std::make_shared<AsyncDfsWriteContext>();
        context->deadline = deadline;
        context->keys = keys;
        context->descriptors = descriptors;
        context->backend = std::move(backend);
        context->staging_pool = dfs_write_staging_pool_;
        auto staging_wait = dfs_write_arena_wait_;
        const auto now = std::chrono::steady_clock::now();
        if (deadline != std::chrono::steady_clock::time_point::max()) {
            const auto remaining =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    deadline > now ? deadline - now
                                   : std::chrono::steady_clock::duration::zero());
            staging_wait = std::min(staging_wait, remaining);
        }
        context->staging_slot =
            context->staging_pool->Acquire(staging_wait, slot_timed_out);
        if (!context->staging_slot) {
            throw std::runtime_error("DFS write staging slot unavailable");
        }
        context->is_upsert = is_upsert;
        context->object_checksums.resize(keys.size());
        context->service_dual_writes.resize(keys.size());
        for (size_t i = 0; i < op_indices.size(); ++i) {
            const auto& op = ops[op_indices[i]];
            auto& metric = context->service_dual_writes[i];
            metric.eligible = op.service_dual_write_eligible;
            metric.memory_succeeded =
                op.transfer_summary.successful_memory_transfers ==
                op.requested_memory_replicas;
            metric.requested_dfs_replicas = op.requested_dfs_replicas;
        }
        staged = StageDfsWriteData(*context, slice_lists);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to stage async DFS write: " << e.what();
    }
    if (!staged) {
        const bool staging_timed_out = slot_timed_out && *slot_timed_out;
        for (const size_t index : op_indices) {
            auto& op = ops[index];
            op.transfer_summary.RecordFailure(
                ReplicaType::DFS,
                staging_timed_out ? ErrorCode::DFS_BATCH_PUT_TIMEOUT
                                  : ErrorCode::TRANSFER_FAIL);
            op.AppendFailureContext(staging_timed_out
                                        ? "DFS BatchPut staging timed out"
                                        : "Failed to stage DFS write data");
        }
        return;
    }

    // No End may run until the foreground CRC is ready, even if DFS I/O
    // finishes before the memory replicas are finalized.
    bool checksums_ready = true;
    for (size_t i = 0; i < op_indices.size(); ++i) {
        auto& op = ops[op_indices[i]];
        ComputePutObjectChecksum(op, context->slices[i], true);
        context->object_checksums[i] = op.object_checksum;
        if (op.IsResolved()) checksums_ready = false;
    }
    if (!checksums_ready) {
        for (const size_t index : op_indices) {
            ops[index].transfer_summary.RecordFailure(ReplicaType::DFS,
                                                      ErrorCode::TRANSFER_FAIL);
        }
        // FinalizeBatchPut revokes all allocations for terminal CRC errors.
        return;
    }

    {
        std::lock_guard<std::mutex> lock(dfs_inflight_mutex_);
        if (dfs_writes_shutting_down_.load(std::memory_order_acquire)) {
            LOG(ERROR) << "Client is shutting down; refusing " << keys.size()
                       << " async DFS writes";
            for (const size_t index : op_indices) {
                auto& op = ops[index];
                op.transfer_summary.RecordFailure(
                    ReplicaType::DFS, ErrorCode::DFS_SERVICE_UNAVAILABLE);
                op.AppendFailureContext("Client is shutting down");
            }
            return;
        }
        ++dfs_inflight_writes_;
    }

    auto release_inflight = [this]() {
        std::lock_guard<std::mutex> lock(dfs_inflight_mutex_);
        if (dfs_inflight_writes_ > 0) --dfs_inflight_writes_;
        dfs_inflight_cv_.notify_all();
    };

    for (const auto& metric : context->service_dual_writes) {
        if (metric.eligible) BeginServiceDualWritePending();
    }

    try {
        {
            std::lock_guard<std::mutex> lock(service_metrics_mutex_);
            if (!service_metrics_snapshot_.service_endpoint.empty() &&
                context->deadline !=
                    std::chrono::steady_clock::time_point::max()) {
                service_pending_writes_.push_back(context);
            }
        }
#ifdef MOONCAKE_ENABLE_TEST_FAILPOINTS
        TestFailPoint::Wait("dfs_write_before_enqueue");
#endif
        write_thread_pool_.enqueue([this, context, release_inflight]() {
            try {
                RunAsyncDfsWrite(context);
            } catch (const std::exception& e) {
                LOG(ERROR) << "Async DFS write task threw: " << e.what();
                CompleteAsyncDfsWriteMetrics(context, {});
            } catch (...) {
                LOG(ERROR) << "Async DFS write task threw an unknown exception";
                CompleteAsyncDfsWriteMetrics(context, {});
            }
            release_inflight();
        });
    } catch (const std::exception& e) {
        // Enqueue may fail after the reporter has already settled the deadline.
        LOG(ERROR) << "Failed to enqueue async DFS write: " << e.what();
        release_inflight();
        const bool metrics_already_settled =
            context->service_metrics_settled.exchange(true);
        if (!metrics_already_settled) {
            for (const auto& metric : context->service_dual_writes) {
                if (metric.eligible) CancelServiceDualWritePending();
            }
        }
        for (const size_t index : op_indices) {
            auto& op = ops[index];
            if (metrics_already_settled && op.service_dual_write_eligible) {
                // The reporter owns this completion; the foreground must not
                // count it again when it handles the rejected write.
                op.service_metric_recorded = true;
            }
            op.transfer_summary.RecordFailure(
                ReplicaType::DFS, ErrorCode::DFS_SERVICE_UNAVAILABLE);
            op.AppendFailureContext("Failed to enqueue async DFS write");
        }
        return;
    }

    // The foreground never waits for DFS I/O in BUCKET mode. Publish the
    // successful local tier now; the background task exclusively owns DFS
    // End/Revoke and applies the deadline only to DFS publication.
    for (size_t i = 0; i < op_indices.size(); ++i) {
        auto& op = ops[op_indices[i]];
        op.transfer_summary.RecordSuccess(ReplicaType::DFS);
        op.transfer_summary.dfs_completion_deferred = true;
    }
}

void Client::SubmitDfsWrites(std::vector<PutOperation>& ops, bool is_upsert,
                             bool allow_async) {
    auto backend = dfs_storage_backend_;
    uint64_t total_dfs_payload_bytes = 0;
    for (const auto& op : ops) {
        if (op.IsResolved() ||
            op.transfer_summary.allocated_dfs_replicas == 0 ||
            !NonDfsTransfersSucceeded(op.transfer_summary)) {
            continue;
        }
        for (const auto& slice : op.slices) {
            SaturatingAddBytes(slice.size, &total_dfs_payload_bytes);
        }
    }
    const auto timeout_ms = DfsBatchPutTimeoutMs(
        dfs_batch_put_timeout_ms_, total_dfs_payload_bytes);
    const auto deadline =
        dfs_batch_put_timeout_ms_ == 0
            ? std::chrono::steady_clock::time_point::max()
            : DfsIoDeadlineFromNow(timeout_ms);
    const bool async_write =
        allow_async && backend &&
        backend->GetAllocatorType() == DfsAllocatorType::BUCKET;
    if (!async_write) {
        std::vector<size_t> all_indices(ops.size());
        std::iota(all_indices.begin(), all_indices.end(), 0);
        SubmitDfsWriteChunk(ops, all_indices, is_upsert, allow_async, nullptr,
                            deadline);
        return;
    }

    if (dfs_write_chunk_size_ == 0 || dfs_write_arena_count_ == 0) {
        std::vector<size_t> all_indices(ops.size());
        std::iota(all_indices.begin(), all_indices.end(), 0);
        SubmitDfsWriteChunk(ops, all_indices, is_upsert, false);
        return;
    }

    const size_t chunk_limit = dfs_write_chunk_size_;

    if (!dfs_write_staging_pool_) {
        LOG(ERROR) << "DFS write staging arena is unavailable";
        for (auto& op : ops) {
            if (!op.IsResolved() &&
                op.transfer_summary.allocated_dfs_replicas > 0 &&
                NonDfsTransfersSucceeded(op.transfer_summary)) {
                op.transfer_summary.RecordFailure(
                    ReplicaType::DFS, ErrorCode::TRANSFER_FAIL);
                op.AppendFailureContext("DFS write staging arena unavailable");
            }
        }
        return;
    }

    std::vector<std::vector<size_t>> chunks;
    std::vector<size_t> chunk;
    size_t chunk_bytes = 0;
    uint64_t total_payload_bytes = 0;
    const auto flush_chunk = [&]() {
        if (!chunk.empty()) {
            chunks.push_back(std::move(chunk));
            chunk.clear();
            chunk_bytes = 0;
        }
    };

    for (size_t i = 0; i < ops.size(); ++i) {
        auto& op = ops[i];
        if (op.IsResolved() || op.transfer_summary.allocated_dfs_replicas == 0 ||
            !NonDfsTransfersSucceeded(op.transfer_summary)) {
            continue;
        }
        size_t object_bytes = 0;
        bool overflow = false;
        for (const auto& slice : op.slices) {
            if (slice.size > std::numeric_limits<size_t>::max() - object_bytes) {
                overflow = true;
                break;
            }
            object_bytes += slice.size;
        }
        if (overflow || object_bytes > chunk_limit) {
            op.transfer_summary.RecordFailure(ReplicaType::DFS,
                                              ErrorCode::TRANSFER_FAIL);
            op.AppendFailureContext("DFS object exceeds staging chunk limit");
            continue;
        }
        if (total_payload_bytes <=
            std::numeric_limits<uint64_t>::max() - object_bytes) {
            total_payload_bytes += object_bytes;
        } else {
            total_payload_bytes = std::numeric_limits<uint64_t>::max();
        }
        if (!chunk.empty() && object_bytes > chunk_limit - chunk_bytes) {
            flush_chunk();
        }
        chunk.push_back(i);
        chunk_bytes += object_bytes;
    }
    flush_chunk();

    if (dfs_read_trace_enabled()) {
        LOG(INFO) << "Async DFS write batch: total_payload_bytes="
                  << total_payload_bytes << ", chunks=" << chunks.size()
                  << ", chunk_limit_bytes=" << chunk_limit
                  << ", arena_count=" << dfs_write_arena_count_
                  << ", timeout_ms=" << timeout_ms;
    }

    for (size_t chunk_index = 0; chunk_index < chunks.size(); ++chunk_index) {
        bool slot_timed_out = false;
        SubmitDfsWriteChunk(ops, chunks[chunk_index], is_upsert, allow_async,
                            &slot_timed_out, deadline);
        if (slot_timed_out) {
            LOG(WARNING) << "DFS write staging slot wait timed out; refusing "
                         << "remaining " << chunks.size() - chunk_index - 1
                         << " chunks";
            for (size_t remaining = chunk_index + 1;
                 remaining < chunks.size(); ++remaining) {
                for (const size_t op_index : chunks[remaining]) {
                    auto& op = ops[op_index];
                    if (!op.IsResolved() &&
                        op.transfer_summary.allocated_dfs_replicas > 0) {
                        op.transfer_summary.RecordFailure(
                            ReplicaType::DFS,
                            ErrorCode::DFS_BATCH_PUT_TIMEOUT);
                        op.AppendFailureContext(
                            "DFS write staging slot wait timed out");
                    }
                }
            }
            break;
        }
    }
}

void Client::DrainAsyncDfsWrites() {
    dfs_writes_shutting_down_.store(true, std::memory_order_release);
    std::unique_lock<std::mutex> lock(dfs_inflight_mutex_);
    // Wait for tasks that still use master_client_ / dfs_storage_backend_ so
    // they cannot outlive the Client and access freed state.
    dfs_inflight_cv_.wait(lock, [this] { return dfs_inflight_writes_ == 0; });
}

void Client::FinalizeBatchPut(std::vector<PutOperation>& ops,
                              bool allow_memory_only_fallback) {
    // All allocations in this Start batch came from the same master. Fence
    // finalization even when a group contains memory-only objects as well.
    UUID dfs_epoch{};
    for (const auto& op : ops) {
        const auto epoch = DfsWriteEpoch(op.replicas);
        if (epoch != UUID{}) {
            dfs_epoch = epoch;
            break;
        }
    }

    struct BatchFinalizeGroup {
        std::vector<std::string> keys;
        std::vector<size_t> indices;
    };

    BatchFinalizeGroup end_all_group;
    BatchFinalizeGroup end_memory_group;
    BatchFinalizeGroup end_nof_group;
    BatchFinalizeGroup end_dfs_group;
    BatchFinalizeGroup revoke_all_group;
    BatchFinalizeGroup revoke_memory_group;
    BatchFinalizeGroup revoke_nof_group;
    BatchFinalizeGroup revoke_dfs_group;

    std::vector<size_t> pending_finalize_actions(ops.size(), 0);
    std::vector<bool> should_succeed(ops.size(), false);
    std::vector<ErrorCode> terminal_errors(ops.size(), ErrorCode::OK);
    std::vector<std::optional<ErrorCode>> finalize_rpc_errors(ops.size());

    auto add_group_entry = [](BatchFinalizeGroup& group, const std::string& key,
                              size_t index) {
        group.keys.emplace_back(key);
        group.indices.emplace_back(index);
    };

    auto add_finalize_action =
        [&](const std::optional<ReplicaType>& replica_type, bool is_end,
            const std::string& key, size_t index) {
            if (!replica_type.has_value()) {
                return;
            }
            switch (*replica_type) {
                case ReplicaType::ALL:
                    add_group_entry(is_end ? end_all_group : revoke_all_group,
                                    key, index);
                    ++pending_finalize_actions[index];
                    break;
                case ReplicaType::MEMORY:
                    add_group_entry(
                        is_end ? end_memory_group : revoke_memory_group, key,
                        index);
                    ++pending_finalize_actions[index];
                    break;
                case ReplicaType::NOF_SSD:
                    add_group_entry(is_end ? end_nof_group : revoke_nof_group,
                                    key, index);
                    ++pending_finalize_actions[index];
                    break;
                case ReplicaType::DFS:
                    add_group_entry(is_end ? end_dfs_group : revoke_dfs_group,
                                    key, index);
                    ++pending_finalize_actions[index];
                    break;
                default:
                    LOG(ERROR) << "Unexpected replica type in batch finalize: "
                               << *replica_type;
                    finalize_rpc_errors[index] = ErrorCode::INVALID_PARAMS;
                    break;
            }
        };

    auto complete_finalize_action = [&](size_t index) {
        if (pending_finalize_actions[index] > 0) {
            --pending_finalize_actions[index];
        }
    };

    for (size_t i = 0; i < ops.size(); ++i) {
        auto& op = ops[i];
        if (op.IsResolved()) {
            if (!op.IsSuccessful() && !op.replicas.empty()) {
                terminal_errors[i] = op.result.has_value()
                                         ? ErrorCode::INTERNAL_ERROR
                                         : op.result.error();
                add_finalize_action(ReplicaType::ALL, false, op.key, i);
            }
            continue;
        }
        if (op.replicas.empty()) {
            op.SetTerminalError(ErrorCode::INTERNAL_ERROR,
                                PutOperationState::MASTER_FAILED,
                                "Operation has no replicas to finalize");
            continue;
        }

        const auto finalize_decision = DetermineFinalizeDecision(
            op.ToReplicateConfig(), op.transfer_summary,
            /*allow_missing_dfs=*/true, allow_memory_only_fallback);
        should_succeed[i] = finalize_decision.success;
        terminal_errors[i] = finalize_decision.error;
        add_finalize_action(finalize_decision.end_type, true, op.key, i);
        add_finalize_action(finalize_decision.revoke_type, false, op.key, i);
    }

    auto process_end_group = [&](BatchFinalizeGroup& group,
                                 ReplicaType replica_type) {
        if (group.keys.empty()) {
            return;
        }
        std::vector<ObjectMeta> object_metas;
        object_metas.reserve(group.indices.size());
        for (size_t i = 0; i < group.indices.size(); ++i) {
            object_metas.emplace_back(ObjectMeta{
                group.keys[i], ops[group.indices[i]].object_checksum});
        }
        auto responses =
            master_client_.BatchPutEnd(object_metas, replica_type, dfs_epoch);
        if (responses.size() != group.keys.size()) {
            for (size_t idx : group.indices) {
                finalize_rpc_errors[idx] = ErrorCode::RPC_FAIL;
                complete_finalize_action(idx);
            }
            return;
        }
        for (size_t i = 0; i < responses.size(); ++i) {
            const size_t op_idx = group.indices[i];
            if (!responses[i]) {
                finalize_rpc_errors[op_idx] = responses[i].error();
                LOG(ERROR) << "Failed to BatchPutEnd key " << group.keys[i]
                           << ": " << toString(responses[i].error());
                complete_finalize_action(op_idx);
                continue;
            }
            complete_finalize_action(op_idx);
        }
    };

    auto process_revoke_group = [&](BatchFinalizeGroup& group,
                                    ReplicaType replica_type,
                                    bool best_effort = false) {
        if (group.keys.empty()) {
            return;
        }
        auto responses =
            master_client_.BatchPutRevoke(group.keys, replica_type, dfs_epoch);
        if (responses.size() != group.keys.size()) {
            for (size_t idx : group.indices) {
                if (!best_effort) {
                    finalize_rpc_errors[idx] = ErrorCode::RPC_FAIL;
                }
                complete_finalize_action(idx);
            }
            return;
        }
        for (size_t i = 0; i < responses.size(); ++i) {
            const size_t op_idx = group.indices[i];
            if (!responses[i]) {
                if (!best_effort) {
                    finalize_rpc_errors[op_idx] = responses[i].error();
                }
                LOG(ERROR) << "Failed to BatchPutRevoke key " << group.keys[i]
                           << ": " << toString(responses[i].error());
                complete_finalize_action(op_idx);
                continue;
            }
            complete_finalize_action(op_idx);
        }
    };

    process_end_group(end_all_group, ReplicaType::ALL);
    process_end_group(end_memory_group, ReplicaType::MEMORY);
    process_end_group(end_nof_group, ReplicaType::NOF_SSD);
    process_end_group(end_dfs_group, ReplicaType::DFS);
    process_revoke_group(revoke_all_group, ReplicaType::ALL);
    process_revoke_group(revoke_memory_group, ReplicaType::MEMORY);
    process_revoke_group(revoke_nof_group, ReplicaType::NOF_SSD);
    // A DFS cleanup failure must not turn an already published memory replica
    // into a failed BatchPut. The master reclaims a stranded PROCESSING DFS
    // replica through its put-start timeout.
    process_revoke_group(revoke_dfs_group, ReplicaType::DFS,
                         /*best_effort=*/true);

    auto append_finalize_error_context = [&](PutOperation& op, size_t index) {
        if (finalize_rpc_errors[index].has_value()) {
            op.AppendFailureContext("Batch finalization RPC failed: " +
                                    toString(*finalize_rpc_errors[index]));
        }
        if (pending_finalize_actions[index] != 0) {
            op.AppendFailureContext(
                "Operation has unfinished finalize actions");
        }
    };

    for (size_t i = 0; i < ops.size(); ++i) {
        auto& op = ops[i];
        if (op.IsResolved()) {
            if (!op.IsSuccessful()) {
                append_finalize_error_context(op, i);
            }
            continue;
        }
        if (finalize_rpc_errors[i].has_value() ||
            pending_finalize_actions[i] != 0) {
            if (!should_succeed[i] && terminal_errors[i] != ErrorCode::OK) {
                append_finalize_error_context(op, i);
                op.SetTerminalError(
                    terminal_errors[i], PutOperationState::TRANSFER_FAILED,
                    op.failure_context.value_or(
                        "Replica transfer failed before finalize"));
            } else if (finalize_rpc_errors[i].has_value()) {
                op.SetTerminalError(*finalize_rpc_errors[i],
                                    PutOperationState::FINALIZE_FAILED,
                                    "Batch finalization RPC failed");
            } else {
                op.SetTerminalError(
                    ErrorCode::INTERNAL_ERROR,
                    PutOperationState::FINALIZE_FAILED,
                    "Operation has unfinished finalize actions");
            }
            continue;
        }
        if (should_succeed[i]) {
            op.SetSuccess();
            continue;
        }
        op.SetTerminalError(terminal_errors[i],
                            PutOperationState::TRANSFER_FAILED,
                            op.failure_context.value_or(
                                "Replica transfer failed before finalize"));
    }
}

void Client::FinalizeBatchUpsert(std::vector<PutOperation>& ops) {
    // All allocations in this Start batch came from the same master. Fence
    // finalization even when a group contains memory-only objects as well.
    UUID dfs_epoch{};
    for (const auto& op : ops) {
        const auto epoch = DfsWriteEpoch(op.replicas);
        if (epoch != UUID{}) {
            dfs_epoch = epoch;
            break;
        }
    }

    std::vector<ObjectMeta> successful_object_metas;
    std::vector<size_t> successful_indices;
    std::vector<std::string> failed_keys;
    std::vector<size_t> failed_indices;

    successful_object_metas.reserve(ops.size());
    successful_indices.reserve(ops.size());
    failed_keys.reserve(ops.size());
    failed_indices.reserve(ops.size());

    for (size_t i = 0; i < ops.size(); ++i) {
        auto& op = ops[i];
        if (!op.IsResolved() && !op.replicas.empty() &&
            HasExpectedReplicaAllocation(op.ToReplicateConfig(),
                                         op.transfer_summary) &&
            AllAllocatedTransfersSucceeded(op.transfer_summary)) {
            successful_object_metas.emplace_back(
                ObjectMeta{op.key, op.object_checksum});
            successful_indices.emplace_back(i);
            continue;
        }

        if (!op.IsResolved() && !op.replicas.empty()) {
            const auto error = op.transfer_summary.first_error == ErrorCode::OK
                                   ? ErrorCode::TRANSFER_FAIL
                                   : op.transfer_summary.first_error;
            op.SetTerminalError(
                error, PutOperationState::TRANSFER_FAILED,
                op.failure_context.value_or(
                    "Replica transfer failed before upsert finalize"));
        }
        if (!op.IsSuccessful() && !op.replicas.empty()) {
            failed_keys.emplace_back(op.key);
            failed_indices.emplace_back(i);
        }
    }

    // Process successful operations
    std::vector<std::string> finalized_keys;
    if (!successful_object_metas.empty()) {
        finalized_keys.reserve(successful_object_metas.size());
        auto end_responses =
            master_client_.BatchUpsertEnd(successful_object_metas, dfs_epoch);
        if (end_responses.size() != successful_object_metas.size()) {
            LOG(ERROR) << "BatchUpsertEnd response size mismatch: expected "
                       << successful_object_metas.size() << ", got "
                       << end_responses.size();
            for (size_t idx : successful_indices) {
                ops[idx].SetError(ErrorCode::RPC_FAIL,
                                  "BatchUpsertEnd response size mismatch");
            }
        } else {
            for (size_t i = 0; i < end_responses.size(); ++i) {
                const size_t op_idx = successful_indices[i];
                if (!end_responses[i]) {
                    LOG(ERROR) << "Failed to finalize upsert for key "
                               << successful_object_metas[i].key << ": "
                               << toString(end_responses[i].error());
                    ops[op_idx].SetError(end_responses[i].error(),
                                         "BatchUpsertEnd failed");
                } else {
                    ops[op_idx].SetSuccess();
                    finalized_keys.emplace_back(successful_object_metas[i].key);
                    VLOG(1) << "Successfully completed upsert for key "
                            << successful_object_metas[i].key;
                }
            }
        }
    }

    // Success-side invalidation for finalized upserts only: a concurrent read
    // between StartBatchUpsert()'s pre-invalidation and BatchUpsertEnd could
    // have read the old value and submitted an async hot-cache fill with a
    // still-valid token. Invalidate again now that the new values are
    // committed so those stale fills cannot publish.
    if (hot_cache_ && !finalized_keys.empty()) {
        hot_cache_->RemoveHotKeys(finalized_keys);
    }

    // Process failed operations that need cleanup
    if (!failed_keys.empty()) {
        auto revoke_responses =
            master_client_.BatchUpsertRevoke(failed_keys, dfs_epoch);
        if (revoke_responses.size() != failed_keys.size()) {
            LOG(ERROR) << "BatchUpsertRevoke response size mismatch: expected "
                       << failed_keys.size() << ", got "
                       << revoke_responses.size();
            for (size_t idx : failed_indices) {
                ops[idx].SetError(ErrorCode::RPC_FAIL,
                                  "BatchUpsertRevoke response size mismatch");
            }
        } else {
            for (size_t i = 0; i < revoke_responses.size(); ++i) {
                const size_t op_idx = failed_indices[i];
                if (!revoke_responses[i]) {
                    LOG(ERROR)
                        << "Failed to revoke upsert for key " << failed_keys[i]
                        << ": " << toString(revoke_responses[i].error());
                    std::string original_context =
                        ops[op_idx].failure_context.value_or("unknown error");
                    ops[op_idx].failure_context =
                        original_context + "; revoke also failed";
                } else {
                    LOG(INFO) << "Successfully revoked failed upsert for key "
                              << failed_keys[i];
                }
            }
        }
    }

    // Ensure all operations have definitive results
    for (auto& op : ops) {
        if (!op.IsResolved()) {
            op.SetError(ErrorCode::INTERNAL_ERROR,
                        "Operation not resolved after finalization");
            LOG(ERROR) << "Operation for key " << op.key
                       << " was not properly resolved";
        }
    }
}

std::vector<tl::expected<void, ErrorCode>> Client::CollectResults(
    const std::vector<PutOperation>& ops) {
    constexpr size_t kMaxFailureSamplesPerError = 3;
    struct FailureSummary {
        size_t count = 0;
        std::vector<std::string> key_samples;
    };

    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(ops.size());

    int no_available_handle_count = 0;
    std::map<ErrorCode, FailureSummary> failures;
    for (const auto& op : ops) {
        // With the new structure, result is always set (never nullopt)
        results.emplace_back(op.result);
        auto& result = results.back();

        // Additional validation and logging for debugging
        if (!result.has_value()) {
            // If error == object already exist, consider as ok (Put semantics).
            // UpsertStart never returns this error, so this branch is
            // unreachable for BatchUpsert — kept for BatchPut compatibility.
            if (result.error() == ErrorCode::OBJECT_ALREADY_EXISTS) {
                result = {};
                continue;
            }
            if (result.error() == ErrorCode::NO_AVAILABLE_HANDLE) {
                no_available_handle_count++;
            } else {
                auto& summary = failures[result.error()];
                ++summary.count;
                if (summary.key_samples.size() < kMaxFailureSamplesPerError) {
                    summary.key_samples.push_back(
                        op.key + (op.failure_context
                                      ? " (" + *op.failure_context + ")"
                                      : ""));
                }
            }
        } else {
            VLOG(1) << "Operation for key " << op.key
                    << " completed successfully";
        }
    }
    if (no_available_handle_count > 0) {
        LOG(WARNING) << "BatchPut failed for " << no_available_handle_count
                     << " keys" << PUT_NO_SPACE_HELPER_STR;
    }
    for (const auto& [error, summary] : failures) {
        LOG(ERROR) << "BatchPut failed for " << summary.count
                   << " keys, error=" << toString(error) << ", sample_keys="
                   << boost::algorithm::join(summary.key_samples, ",");
    }

    return results;
}

void Client::StageWriteBuffersForRemoteReplicas(
    std::vector<PutOperation>& ops, const WriteBufferStager& stager) {
    if (!stager) return;

    for (auto& op : ops) {
        if (op.IsResolved() || op.replicas.empty() ||
            std::all_of(op.replicas.begin(), op.replicas.end(),
                        [this](const Replica::Descriptor& replica) {
                            return CanUseLocalMemcpy(replica);
                        })) {
            continue;
        }
        auto staged_slices = stager(op.slices);
        if (!staged_slices) {
            if (staged_slices.error() == ErrorCode::INVALID_PARAMS) {
                op.service_metric_excluded = true;
            }
            op.SetError(staged_slices.error(),
                        "Failed to stage non-local write buffers");
            continue;
        }
        op.slices = std::move(*staged_slices);
        VLOG(1) << "Staged write buffers for non-local replica, key=" << op.key;
    }
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchWriteWhenPreferSameNode(
    std::vector<PutOperation>& ops, bool is_upsert) {
    auto t0 = std::chrono::steady_clock::now();
    std::unordered_map<std::string, PutOperation> seg_to_ops{};
    for (auto& op : ops) {
        if (op.IsResolved()) {
            continue;
        }
        if (op.replicas.empty()) {
            op.SetError(ErrorCode::INTERNAL_ERROR,
                        "No replicas available for transfer");
            continue;
        }
        auto replica = op.replicas[0];
        if (!replica.is_memory_replica()) {
            op.SetError(ErrorCode::INVALID_PARAMS, "only memory is supported.");
            continue;
        }
        auto& memory_descriptor = replica.get_memory_descriptor();
        if (memory_descriptor.buffer_descriptor.size_ == 0) {
            op.SetError(ErrorCode::INVALID_PARAMS, "buffer size is 0.");
            continue;
        }
        auto& buffer_descriptor = memory_descriptor.buffer_descriptor;
        auto seg = buffer_descriptor.transport_endpoint_;
        if (seg_to_ops.find(seg) == seg_to_ops.end()) {
            seg_to_ops.emplace(seg, PutOperation(op.key, op.slices));
        }
        // multiple replica-slices
        seg_to_ops.at(seg).batched_slices.emplace_back(op.slices);
        seg_to_ops.at(seg).replicas.emplace_back(replica);
    }
    std::vector<PutOperation> merged_ops;
    merged_ops.reserve(seg_to_ops.size());
    for (auto& seg_to_op : seg_to_ops) {
        auto& op = seg_to_op.second;
        bool all_transfers_submitted = true;
        std::string failure_context;
        merged_ops.emplace_back(op.key, op.slices);
        auto& merged_op = merged_ops.back();
        merged_op.replicas = op.replicas;
        merged_op.transfer_summary.allocated_memory_replicas = 1;
        auto submit_result = transfer_submitter_->submit_batch(
            op.replicas, op.batched_slices, TransferRequest::WRITE);
        if (!submit_result) {
            failure_context = "Failed to submit batch transfer";
            all_transfers_submitted = false;
        } else {
            merged_op.pending_transfers.emplace_back(
                ReplicaType::MEMORY, std::move(submit_result.value()));
        }
        if (!all_transfers_submitted) {
            LOG(ERROR) << "Transfer submission failed for key " << op.key
                       << ": " << failure_context;
            merged_op.transfer_summary.RecordFailure(ReplicaType::MEMORY,
                                                     ErrorCode::TRANSFER_FAIL);
            merged_op.failure_context = failure_context;
            merged_op.pending_transfers.clear();
        } else {
            VLOG(1) << "Successfully submitted "
                    << merged_op.pending_transfers.size()
                    << " transfers for key " << merged_ops.back().key;
        }
    }
    WaitForTransfers(merged_ops);
    for (auto& op : merged_ops) {
        auto& memory_descriptor = op.replicas[0].get_memory_descriptor();
        auto& buffer_descriptor = memory_descriptor.buffer_descriptor;
        auto seg = buffer_descriptor.transport_endpoint_;
        seg_to_ops.at(seg).transfer_summary = op.transfer_summary;
        seg_to_ops.at(seg).failure_context = op.failure_context;
    }
    for (auto& op : ops) {
        if (op.IsResolved()) {
            continue;
        }
        auto& memory_descriptor = op.replicas[0].get_memory_descriptor();
        auto& buffer_descriptor = memory_descriptor.buffer_descriptor;
        auto seg = buffer_descriptor.transport_endpoint_;
        op.transfer_summary.successful_memory_transfers =
            seg_to_ops.at(seg).transfer_summary.successful_memory_transfers > 0
                ? 1
                : 0;
        op.transfer_summary.failed_memory_transfers =
            seg_to_ops.at(seg).transfer_summary.failed_memory_transfers > 0 ? 1
                                                                            : 0;
        op.transfer_summary.first_error =
            seg_to_ops.at(seg).transfer_summary.first_error;
        op.failure_context = seg_to_ops.at(seg).failure_context;
    }
    // Puts may write asynchronously in bucket mode.
    SubmitDfsWrites(ops, /*is_upsert=*/false, /*allow_async=*/true);
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_put_latency_us.observe(us);
    }
    RecordBatchServiceDualWriteMetrics(ops);
    if (is_upsert) {
        FinalizeBatchUpsert(ops);
    } else {
        const bool allow_memory_only_fallback =
            dfs_storage_backend_ &&
            dfs_storage_backend_->GetAllocatorType() ==
                DfsAllocatorType::BUCKET;
        FinalizeBatchPut(ops, allow_memory_only_fallback);
    }
    return CollectResults(ops);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPut(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const ReplicateConfig& config) {
    return BatchPut(keys, batched_slices, config, {});
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPut(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const ReplicateConfig& config, const WriteBufferStager& stager) {
    ReplicateConfig client_cfg = AttachConfig(config);
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }
    std::vector<PutOperation> ops =
        CreatePutOperations(keys, batched_slices, client_cfg);
    const bool reuse_dfs_staging =
        object_checksum_enabled_ && !client_cfg.prefer_alloc_in_same_node &&
        client_cfg.dfs_replica_num > 0 && dfs_storage_backend_ &&
        dfs_storage_backend_->GetAllocatorType() == DfsAllocatorType::BUCKET;
    if (!reuse_dfs_staging) {
        ComputeBatchObjectChecksums(ops);
    } else {
        // Keep checksum's input validation ahead of memory transfers.
        for (auto& op : ops) {
            for (const auto& slice : op.slices) {
                if (slice.size != 0 && slice.ptr == nullptr) {
                    op.service_metric_excluded = true;
                    op.SetTerminalError(ErrorCode::INVALID_PARAMS,
                                        PutOperationState::MASTER_FAILED,
                                        "Object checksum has a null slice");
                    break;
                }
            }
        }
    }
    if (client_cfg.prefer_alloc_in_same_node) {
        if (auto err = ValidatePreferSameNodeWriteConfig(client_cfg)) {
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(*err));
        }
        StartBatchPut(ops, client_cfg);
        StageWriteBuffersForRemoteReplicas(ops, stager);
        return BatchWriteWhenPreferSameNode(ops, false);
    }
    StartBatchPut(ops, client_cfg);
    StageWriteBuffersForRemoteReplicas(ops, stager);

    auto t0 = std::chrono::steady_clock::now();
    SubmitTransfers(ops);
    WaitForTransfers(ops);
    SubmitDfsWrites(ops);
    // Covers missing DFS allocations, staging/enqueue failures and skipped
    // DFS writes. Already computed checksums (including zero) are preserved.
    if (reuse_dfs_staging) ComputeBatchObjectChecksums(ops);
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_put_latency_us.observe(us);
    }

    RecordBatchServiceDualWriteMetrics(ops);
    const bool allow_memory_only_fallback =
        dfs_storage_backend_ &&
        dfs_storage_backend_->GetAllocatorType() == DfsAllocatorType::BUCKET;
    FinalizeBatchPut(ops, allow_memory_only_fallback);
    return CollectResults(ops);
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
Client::StartBatchPutForSizes(const std::vector<std::string>& keys,
                              const std::vector<uint64_t>& object_sizes,
                              const ReplicateConfig& config) {
    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
        results(keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    if (keys.size() != object_sizes.size()) {
        LOG(ERROR) << "StartBatchPutForSizes size mismatch: keys="
                   << keys.size() << ", sizes=" << object_sizes.size();
        return results;
    }

    ReplicateConfig client_cfg = AttachConfig(config);
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }

    std::vector<std::vector<Slice>> batched_slices(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        batched_slices[i] = {Slice{nullptr, object_sizes[i]}};
    }
    std::vector<PutOperation> ops =
        CreatePutOperations(keys, batched_slices, client_cfg);
    StartBatchPut(ops, client_cfg);

    for (size_t i = 0; i < ops.size(); ++i) {
        if (ops[i].IsResolved()) {
            results[i] = tl::unexpected(ops[i].result.error());
            continue;
        }
        results[i] = std::move(ops[i].replicas);
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPutEnd(
    const std::vector<ObjectMeta>& object_metas, ReplicaType replica_type,
    const std::vector<UUID>& dfs_epochs) {
    return FinalizeSessionsByEpoch(
        object_metas, dfs_epochs, [&](const auto& group, const UUID& epoch) {
            return master_client_.BatchPutEnd(group, replica_type, epoch);
        });
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPutRevoke(
    const std::vector<std::string>& keys, ReplicaType replica_type,
    const std::vector<UUID>& dfs_epochs) {
    return FinalizeSessionsByEpoch(
        keys, dfs_epochs, [&](const auto& group, const UUID& epoch) {
            return master_client_.BatchPutRevoke(group, replica_type, epoch);
        });
}

tl::expected<void, ErrorCode> Client::Remove(const ObjectKey& key, bool force) {
    if (hot_cache_) {
        hot_cache_->BumpKeyGeneration(key);
    }

    auto result = master_client_.Remove(key, force);
    // if (storage_backend_) {
    //     storage_backend_->RemoveFile(key);
    // }
    if (!result) {
        return tl::unexpected(result.error());
    }
    if (hot_cache_) {
        hot_cache_->RemoveHotKey(key);
    }

    return {};
}

tl::expected<long, ErrorCode> Client::RemoveByRegex(const ObjectKey& str,
                                                    bool force) {
    if (hot_cache_) {
        hot_cache_->BumpCacheEpoch();
    }

    auto result = master_client_.RemoveByRegex(str, force);
    // if (storage_backend_) {
    //     storage_backend_->RemoveByRegex(str);
    // }
    if (!result) {
        return tl::unexpected(result.error());
    }
    if (result.value() > 0 && hot_cache_) {
        hot_cache_->BumpCacheEpoch();
        hot_cache_->RemoveHotKeysByRegex(str);
    }
    return result.value();
}

tl::expected<long, ErrorCode> Client::RemoveAll(bool force) {
    if (hot_cache_) {
        hot_cache_->BumpCacheEpoch();
    }

    auto result = master_client_.RemoveAll(force);
    if (result) {
        if (storage_backend_) {
            storage_backend_->RemoveAll();
        }
    }
    if (result && result.value() > 0 && hot_cache_) {
        hot_cache_->RemoveAllHotKeys();
    }
    return result;
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchRemove(
    const std::vector<ObjectKey>& keys, bool force) {
    if (hot_cache_) {
        hot_cache_->BumpKeyGenerations(keys);
    }

    auto results = master_client_.BatchRemove(keys, force);

    if (hot_cache_) {
        std::vector<std::string> removed_keys;
        removed_keys.reserve(std::min(keys.size(), results.size()));
        for (size_t i = 0; i < keys.size(); ++i) {
            if (i < results.size() && results[i].has_value()) {
                removed_keys.emplace_back(keys[i]);
            }
        }
        if (!removed_keys.empty()) {
            hot_cache_->RemoveHotKeys(removed_keys);
        }
    }

    return results;
}

tl::expected<void, ErrorCode> Client::EvictDiskReplica(
    const std::string& key, ReplicaType replica_type) {
    return master_client_.EvictDiskReplica(key, replica_type);
}

tl::expected<void, ErrorCode> Client::EvictDiskReplica(
    const std::string& key, const std::string& tenant_id,
    ReplicaType replica_type) {
    return master_client_.EvictDiskReplica(key, tenant_id, replica_type);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchEvictDiskReplica(
    const std::vector<std::string>& keys, ReplicaType replica_type) {
    return master_client_.BatchEvictDiskReplica(keys, replica_type);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchEvictDiskReplica(
    const std::vector<std::string>& keys, const std::string& tenant_id,
    ReplicaType replica_type) {
    return master_client_.BatchEvictDiskReplica(keys, tenant_id, replica_type);
}

std::vector<int> Client::GetNicNumaNodes() const {
    std::set<int> nodes;
    if (!transfer_engine_) return {};
    auto topo = transfer_engine_->getLocalTopology();
    if (!topo) return {};
    for (auto& [name, entry] : topo->getMatrix()) {
        if (name.rfind("cpu:", 0) != 0 || entry.preferred_hca.empty()) continue;
        int node = std::stoi(name.substr(4));
        nodes.insert(node);
    }
    return {nodes.begin(), nodes.end()};
}

tl::expected<void, ErrorCode> Client::MountSegment(
    const void* buffer, size_t size, const std::string& protocol,
    const std::string& location) {
    auto result = MountSegmentAndGetId(buffer, size, protocol, location);
    if (!result) {
        return tl::unexpected(result.error());
    }
    return {};
}

tl::expected<void, ErrorCode> Client::UnmountSegmentImpl(
    std::unordered_map<UUID, Segment, boost::hash<UUID>>::iterator it) {
    auto unmount_result = master_client_.UnmountSegment(it->second.id);
    if (!unmount_result) {
        ErrorCode err = unmount_result.error();
        LOG(ERROR) << "Failed to unmount segment from master: "
                   << toString(err);
        return tl::unexpected(err);
    }

    int rc = transfer_engine_->unregisterLocalMemory(
        reinterpret_cast<void*>(it->second.base));
    if (rc != 0) {
        LOG(ERROR) << "Failed to unregister transfer buffer with transfer "
                      "engine ret is "
                   << rc;
        if (rc != ERR_ADDRESS_NOT_REGISTERED) {
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        // Otherwise, the segment is already unregistered from transfer
        // engine, we can continue
    }

    mounted_segments_.erase(it);
    return {};
}

tl::expected<void, ErrorCode> Client::UnmountSegment(const void* buffer,
                                                     size_t size) {
    std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
    auto segment = mounted_segments_.end();

    for (auto it = mounted_segments_.begin(); it != mounted_segments_.end();
         ++it) {
        if (it->second.base == reinterpret_cast<uintptr_t>(buffer) &&
            it->second.size == size) {
            segment = it;
            break;
        }
    }
    if (segment == mounted_segments_.end()) {
        LOG(ERROR) << "segment_not_found base=" << buffer << " size=" << size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    return UnmountSegmentImpl(segment);
}

tl::expected<UUID, ErrorCode> Client::MountSegmentAndGetId(
    const void* buffer, size_t size, const std::string& protocol,
    const std::string& location) {
    auto check_result = CheckRegisterMemoryParams(buffer, size, protocol);
    if (!check_result) {
        return tl::unexpected(check_result.error());
    }

    UUID segment_id;
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);

        // Check if the segment overlaps with any existing segment
        for (auto& it : mounted_segments_) {
            auto& mtseg = it.second;
            uintptr_t l1 = reinterpret_cast<uintptr_t>(mtseg.base);
            uintptr_t r1 = reinterpret_cast<uintptr_t>(mtseg.size) + l1;
            uintptr_t l2 = reinterpret_cast<uintptr_t>(buffer);
            uintptr_t r2 = reinterpret_cast<uintptr_t>(size) + l2;
            if (std::max(l1, l2) < std::min(r1, r2)) {
                LOG(ERROR) << "segment_overlaps base1=" << mtseg.base
                           << " size1=" << mtseg.size << " base2=" << buffer
                           << " size2=" << size;
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            }
        }

        int rc = transfer_engine_->registerLocalMemory((void*)buffer, size,
                                                       location, true, true);
        if (rc != 0) {
            LOG(ERROR) << "register_local_memory_failed base=" << buffer
                       << " size=" << size << ", error=" << rc;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }

        Segment segment;
        segment.id = generate_uuid();
        segment.name = local_hostname_;
        segment.base = reinterpret_cast<uintptr_t>(buffer);
        segment.size = size;
        segment.protocol = protocol;
        segment.host_id = host_id_;
        if (metadata_connstring_ == P2PHANDSHAKE) {
            segment.te_endpoint = transfer_engine_->getLocalIpAndPort();
        } else {
            segment.te_endpoint = local_hostname_;
        }

        auto mount_result = master_client_.MountSegment(segment);
        if (!mount_result) {
            ErrorCode err = mount_result.error();
            LOG(ERROR) << "mount_segment_to_master_failed base=" << buffer
                       << " size=" << size << ", error=" << err;
            return tl::unexpected(err);
        }

        segment_id = segment.id;
        mounted_segments_[segment_id] = segment;
    }

    EnsureStorageControlPlaneStarted();
    return segment_id;
}

tl::expected<void, ErrorCode> Client::UnmountSegmentById(
    const UUID& segment_id, uint64_t grace_period_ms,
    std::function<void(const UUID&)> cleanup_callback) {
    std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
    auto segment = mounted_segments_.find(segment_id);
    if (segment == mounted_segments_.end()) {
        LOG(ERROR) << "segment_not_found id=" << UuidToString(segment_id);
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (grace_period_ms == 0) {
        return UnmountSegmentImpl(segment);
    }

    auto result =
        master_client_.GracefulUnmountSegment(segment_id, grace_period_ms);
    if (!result) {
        ErrorCode err = result.error();
        LOG(ERROR) << "Failed to graceful unmount segment from master: "
                   << toString(err);
        return tl::unexpected(err);
    }

    gracefully_unmounting_segments_.emplace(segment->first, segment->second);
    if (cleanup_callback) {
        graceful_unmount_cleanup_callbacks_[segment->first] =
            std::move(cleanup_callback);
    }
    mounted_segments_.erase(segment);
    StartGracefulUnmountTimer(segment_id, grace_period_ms);
    return {};
}

bool Client::WaitForGracefulUnmountDelay(std::chrono::milliseconds delay) {
    std::unique_lock<std::mutex> lock(graceful_unmount_timer_mutex_);
    return graceful_unmount_timer_cv_.wait_for(
        lock, delay, [this]() { return graceful_unmount_timer_stopping_; });
}

void Client::StartGracefulUnmountTimer(const UUID& segment_id,
                                       uint64_t grace_period_ms) {
    auto delay =
        std::chrono::milliseconds(grace_period_ms) + std::chrono::seconds(10);
    task_thread_pool_.enqueue([this, segment_id, delay]() {
        if (this->WaitForGracefulUnmountDelay(delay)) {
            return;
        }
        this->OnGracefulUnmountTimer(segment_id, /*retry_left=*/3);
    });
}

void Client::OnGracefulUnmountTimer(const UUID& segment_id, int retry_left) {
    // Query master to confirm the segment has been removed
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        auto it = gracefully_unmounting_segments_.find(segment_id);
        if (it == gracefully_unmounting_segments_.end()) {
            // Already cleaned up (e.g. by destructor)
            return;
        }
    }

    auto status = master_client_.QuerySegmentStatusById(segment_id);
    bool removed = false;
    if (!status) {
        if (status.error() == ErrorCode::SEGMENT_NOT_FOUND) {
            removed = true;
        } else {
            LOG(WARNING) << "Failed to query graceful unmount segment status: "
                         << toString(status.error());
        }
    } else if (status.value() == SegmentStatus::UNDEFINED) {
        removed = true;
    }

    if (removed) {
        std::function<void(const UUID&)> cleanup_callback;
        {
            std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
            auto it = gracefully_unmounting_segments_.find(segment_id);
            if (it != gracefully_unmounting_segments_.end()) {
                int rc = transfer_engine_->unregisterLocalMemory(
                    reinterpret_cast<void*>(it->second.base));
                if (rc != 0 && rc != ERR_ADDRESS_NOT_REGISTERED) {
                    LOG(ERROR)
                        << "Failed to unregister TE MR for graceful unmount: "
                        << rc;
                }
                gracefully_unmounting_segments_.erase(it);
            }
            auto callback_it =
                graceful_unmount_cleanup_callbacks_.find(segment_id);
            if (callback_it != graceful_unmount_cleanup_callbacks_.end()) {
                cleanup_callback = std::move(callback_it->second);
                graceful_unmount_cleanup_callbacks_.erase(callback_it);
            }
        }
        if (cleanup_callback) {
            cleanup_callback(segment_id);
        }
        return;
    }

    if (retry_left > 0) {
        try {
            task_thread_pool_.enqueue([this, segment_id, retry_left]() {
                if (this->WaitForGracefulUnmountDelay(
                        std::chrono::seconds(10))) {
                    return;
                }
                this->OnGracefulUnmountTimer(segment_id, retry_left - 1);
            });
        } catch (const std::runtime_error& e) {
            VLOG(1) << "Skip graceful unmount retry enqueue: " << e.what();
        }
    } else {
        LOG(WARNING) << "Graceful unmount cleanup timeout for segment "
                     << UuidToString(segment_id);
    }
}

tl::expected<void, ErrorCode> Client::RegisterLocalMemory(
    void* addr, size_t length, const std::string& location,
    bool remote_accessible, bool update_metadata) {
    auto check_result = CheckRegisterMemoryParams(addr, length, protocol_);
    if (!check_result) {
        return tl::unexpected(check_result.error());
    }
    if (this->transfer_engine_->registerLocalMemory(
            addr, length, location, remote_accessible, update_metadata) != 0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<void, ErrorCode> Client::unregisterLocalMemory(
    void* addr, bool update_metadata) {
    if (this->transfer_engine_->unregisterLocalMemory(addr, update_metadata) !=
        0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<bool, ErrorCode> Client::IsExist(const std::string& key) {
    auto result = master_client_.ExistKey(key);
    return result;
}

std::vector<tl::expected<bool, ErrorCode>> Client::BatchIsExist(
    const std::vector<std::string>& keys) {
    auto response = master_client_.BatchExistKey(keys);

    // Check if we got the expected number of responses
    if (response.size() != keys.size()) {
        LOG(ERROR) << "BatchExistKey response size mismatch. Expected: "
                   << keys.size() << ", Got: " << response.size();
        // Return vector of RPC_FAIL errors
        std::vector<tl::expected<bool, ErrorCode>> results;
        results.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
        }
        return results;
    }

    // Return the response directly as it's already in the correct
    // format
    return response;
}

std::vector<tl::expected<bool, ErrorCode>> Client::BatchIsExistByReplicaType(
    const std::vector<std::string>& keys, ReplicaType replica_type) {
    auto response =
        master_client_.BatchExistKeyByReplicaType(keys, replica_type);

    if (response.size() != keys.size()) {
        LOG(ERROR) << "BatchExistKeyByReplicaType response size mismatch. "
                   << "Expected: " << keys.size() << ", Got: "
                   << response.size();
        std::vector<tl::expected<bool, ErrorCode>> results;
        results.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
        }
        return results;
    }

    return response;
}

void* Client::GetBaseAddr() { return transfer_engine_->getBaseAddr(); }

tl::expected<void, ErrorCode> Client::MountLocalDiskSegment(
    bool enable_offloading) {
    auto response =
        master_client_.MountLocalDiskSegment(client_id_, enable_offloading);

    if (!response) {
        LOG(ERROR) << "MountLocalDiskSegment failed, error code is "
                   << response.error();
        return response;
    }

    EnsureStorageControlPlaneStarted();
    return response;
}

tl::expected<void, ErrorCode> Client::UnmountLocalDiskSegment() {
    auto response = master_client_.UnmountLocalDiskSegment(client_id_);
    if (!response) {
        LOG(ERROR) << "UnmountLocalDiskSegment failed, error code is "
                   << response.error();
    }
    return response;
}

tl::expected<void, ErrorCode> Client::OffloadObjectHeartbeat(
    bool enable_offloading, std::vector<OffloadTaskItem>& offloading_objects) {
    auto response =
        master_client_.OffloadObjectHeartbeat(client_id_, enable_offloading);
    if (!response) {
        LOG(ERROR) << "OffloadObjectHeartbeat failed, error code is "
                   << response.error();
        return tl::make_unexpected(response.error());
    }
    offloading_objects = std::move(response.value());
    return {};
}

tl::expected<DfsBucketSnapshotResponse, ErrorCode>
Client::GetDfsBucketSnapshot(const UUID& last_epoch, uint64_t last_version) {
    return master_client_.GetDfsBucketSnapshot(client_id_, last_epoch,
                                                last_version);
}

tl::expected<bool, ErrorCode> Client::PollRemoveAll() {
    return master_client_.PollRemoveAll();
}

tl::expected<void, ErrorCode> Client::ReportSsdCapacity(
    int64_t ssd_total_capacity_bytes) {
    auto response =
        master_client_.ReportSsdCapacity(client_id_, ssd_total_capacity_bytes);
    if (!response) {
        LOG(ERROR) << "ReportSsdCapacity failed, error code is "
                   << response.error();
        return tl::make_unexpected(response.error());
    }
    return {};
}

tl::expected<void, ErrorCode> Client::BatchGetOffloadObject(
    const std::string& transfer_engine_addr,
    const std::vector<std::string>& keys,
    const std::vector<uintptr_t>& pointers,
    const std::unordered_map<std::string, std::vector<Slice>>& batch_slices) {
    return BatchGetOffloadObject(transfer_engine_addr, keys, pointers,
                                 batch_slices,
                                 OffloadBufferAccess::kTransferEngine);
}

tl::expected<void, ErrorCode> Client::BatchGetOffloadObject(
    const std::string& transfer_engine_addr,
    const std::vector<std::string>& keys,
    const std::vector<uintptr_t>& pointers,
    const std::unordered_map<std::string, std::vector<Slice>>& batch_slices,
    OffloadBufferAccess buffer_access) {
    auto future = transfer_submitter_->submit_batch_get_offload_object(
        transfer_engine_addr, keys, pointers, batch_slices, buffer_access);
    if (!future) {
        LOG(ERROR) << "Failed to submit transfer operation";
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }
    VLOG(1) << "Using transfer strategy: " << future->strategy();
    auto result = future->get();
    if (result != ErrorCode::OK) {
        LOG(ERROR) << "Transfer failed, error code is " << result;
        return tl::make_unexpected(result);
    }
    return {};
}

tl::expected<void, ErrorCode> Client::NotifyOffloadSuccess(
    const std::vector<std::string>& keys,
    const std::vector<StorageObjectMetadata>& metadatas) {
    auto response =
        master_client_.NotifyOffloadSuccess(client_id_, keys, metadatas);
    return response;
}

tl::expected<void, ErrorCode> Client::NotifyOffloadSuccess(
    const std::vector<OffloadTaskItem>& tasks,
    const std::vector<StorageObjectMetadata>& metadatas) {
    return master_client_.NotifyOffloadSuccess(client_id_, tasks, metadatas);
}

void Client::SetDfsStorageBackend(
    std::shared_ptr<DistributedStorageBackend> backend) {
    if (backend && !master_dfs_root_dir_.empty()) {
        backend->SetDescriptorRootDir(master_dfs_root_dir_);
    }
    const bool bucket_backend =
        backend && backend->GetAllocatorType() == DfsAllocatorType::BUCKET;
    if (bucket_backend && dfs_write_chunk_size_ > 0 &&
        dfs_write_arena_count_ > 0 && !dfs_write_staging_pool_) {
        dfs_write_staging_pool_ = std::make_shared<DfsWriteStagingPool>(
            dfs_write_chunk_size_, dfs_write_arena_count_);
        LOG(INFO) << "Initialized DFS write staging arenas: count="
                  << dfs_write_arena_count_ << ", size="
                  << dfs_write_chunk_size_;
    }
    if (backend) {
        backend->SetMasterEpochProvider(
            master_client_.MakeDfsMasterEpochProvider());
    }
    dfs_storage_backend_ = std::move(backend);
    EnsureStorageControlPlaneStarted();
}

tl::expected<std::string, ErrorCode> Client::FetchDfsRootIdentity() {
    auto config = master_client_.GetStorageConfig();
    if (!config) return tl::make_unexpected(config.error());
    if (config->dfs_root_identity.empty()) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    return config->dfs_root_identity;
}

tl::expected<void, ErrorCode> Client::PromotionObjectHeartbeat(
    std::vector<PromotionTaskItem>& promotion_objects) {
    auto response = master_client_.PromotionObjectHeartbeat(client_id_);
    if (!response) {
        return tl::make_unexpected(response.error());
    }
    promotion_objects = std::move(response.value());
    return {};
}

tl::expected<PromotionAllocStartResponse, ErrorCode>
Client::PromotionAllocStart(
    const std::string& key, uint64_t size,
    const std::vector<std::string>& preferred_segments) {
    return master_client_.PromotionAllocStart(client_id_, key, size,
                                              preferred_segments);
}

tl::expected<PromotionAllocStartResponse, ErrorCode>
Client::PromotionAllocStart(
    const std::string& key, const std::string& tenant_id, uint64_t size,
    const std::vector<std::string>& preferred_segments) {
    return master_client_.PromotionAllocStart(client_id_, key, tenant_id, size,
                                              preferred_segments);
}

tl::expected<void, ErrorCode> Client::NotifyPromotionSuccess(
    const std::string& key) {
    return master_client_.NotifyPromotionSuccess(client_id_, key);
}

tl::expected<void, ErrorCode> Client::NotifyPromotionSuccess(
    const std::string& key, const std::string& tenant_id) {
    return master_client_.NotifyPromotionSuccess(client_id_, key, tenant_id);
}

tl::expected<void, ErrorCode> Client::NotifyPromotionFailure(
    const std::string& key) {
    return master_client_.NotifyPromotionFailure(client_id_, key);
}

tl::expected<void, ErrorCode> Client::NotifyPromotionFailure(
    const std::string& key, const std::string& tenant_id) {
    return master_client_.NotifyPromotionFailure(client_id_, key, tenant_id);
}

ErrorCode Client::PromotionWrite(const Replica::Descriptor& memory_descriptor,
                                 std::vector<Slice>& slices) {
    return TransferWrite(memory_descriptor, slices);
}

tl::expected<UUID, ErrorCode> Client::CreateCopyTask(
    const std::string& key, const std::vector<std::string>& targets) {
    return master_client_.CreateCopyTask(key, targets);
}

tl::expected<UUID, ErrorCode> Client::CreateCopyTask(
    const std::string& key, const std::string& tenant_id,
    const std::vector<std::string>& targets) {
    return master_client_.CreateCopyTask(key, tenant_id, targets);
}

tl::expected<UUID, ErrorCode> Client::CreateMoveTask(
    const std::string& key, const std::string& source,
    const std::string& target) {
    return master_client_.CreateMoveTask(key, source, target);
}

tl::expected<UUID, ErrorCode> Client::CreateMoveTask(
    const std::string& key, const std::string& tenant_id,
    const std::string& source, const std::string& target) {
    return master_client_.CreateMoveTask(key, tenant_id, source, target);
}

tl::expected<void, ErrorCode> Client::ExecuteReplicaTransfer(
    const std::string& key, const std::string& action_name,
    std::function<tl::expected<void, ErrorCode>()> end_fn,
    std::function<tl::expected<void, ErrorCode>()> revoke_fn,
    const Replica::Descriptor& source,
    const std::vector<Replica::Descriptor>& targets,
    std::shared_ptr<DistributedStorageBackend> dfs_backend) {
    auto revoke_lambda = [&]() {
        auto revoke_result = revoke_fn();
        if (!revoke_result.has_value()) {
            LOG(WARNING) << "action=replica_" << action_name << "_revoke_failed"
                         << ", key=" << key
                         << ", error_code=" << revoke_result.error();
        }
    };

    // currently only memory source replica is supported
    if (!source.is_memory_replica()) {
        LOG(ERROR) << "action=replica_" << action_name << "_failed"
                   << ", key=" << key << ", error=invalid_replica_type";
        revoke_lambda();
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate that source replica is in local memory
    if (!IsReplicaOnLocalMemory(source)) {
        LOG(ERROR) << "action=replica_" << action_name << "_failed"
                   << ", key=" << key
                   << ", error=source_replica_not_in_local_memory";
        revoke_lambda();
        return tl::unexpected(ErrorCode::REPLICA_NOT_IN_LOCAL_MEMORY);
    }

    // Split the source replica into slices for transfer
    // This avoids data copy because the source replica is already in memory
    const auto& buffer_descriptor =
        source.get_memory_descriptor().buffer_descriptor;
    void* buffer = reinterpret_cast<void*>(buffer_descriptor.buffer_address_);
    auto slices = split_into_slices(buffer, buffer_descriptor.size_);

    // Transfer to each target
    for (const auto& target : targets) {
        const auto result =
            target.is_dfs_replica()
                ? WriteDfsReplicas({key}, {&slices},
                                   {target.get_dfs_descriptor()}, dfs_backend)
                      .front()
                : TransferWrite(target, slices);
        if (result != ErrorCode::OK) {
            revoke_lambda();
            return tl::unexpected(ErrorCode::TRANSFER_FAIL);
        }
    }

    // Call end function to finalize
    auto end_result = end_fn();
    if (!end_result.has_value()) {
        revoke_lambda();
        return tl::unexpected(end_result.error());
    }

    return {};
}

tl::expected<void, ErrorCode> Client::Copy(
    const std::string& key, const std::string& source,
    const std::vector<std::string>& targets) {
    return Copy(key, master_client_.tenant_id(), source, targets);
}

tl::expected<void, ErrorCode> Client::Copy(
    const std::string& key, const std::string& tenant_id,
    const std::string& source, const std::vector<std::string>& targets) {
    return Copy(key, tenant_id, source, targets, UUID{}, 0);
}

tl::expected<void, ErrorCode> Client::Copy(
    const std::string& key, const std::string& tenant_id,
    const std::string& source, const std::vector<std::string>& targets,
    const UUID& dynamic_replication_lease_id,
    uint64_t dynamic_replication_version_epoch) {
    LOG(INFO) << "action=replica_copy_start" << ", key=" << key
              << ", targets_count=" << targets.size();

    // Call CopyStart first - it validates existence and allocates replicas
    auto start_result =
        dynamic_replication_lease_id == UUID{}
            ? master_client_.CopyStart(key, tenant_id, source, targets)
            : master_client_.DynamicReplicaCopyStart(
                  key, tenant_id, source, targets, dynamic_replication_lease_id,
                  dynamic_replication_version_epoch);
    if (!start_result.has_value()) {
        ErrorCode error = start_result.error();
        if (dynamic_replication_lease_id != UUID{} &&
            error == ErrorCode::OBJECT_ALREADY_EXISTS) {
            LOG(INFO) << "action=replica_copy_skipped"
                      << ", key=" << key
                      << ", info=target_replicas_already_exist";
            return {};
        }
        LOG(ERROR) << "action=replica_copy_failed" << ", key=" << key
                   << ", source=" << source << ", error=copy_start_failed"
                   << ", error_code=" << error;
        return tl::unexpected(error);
    }

    const auto& response = start_result.value();
    if (response.targets.empty()) {
        LOG(INFO) << "action=replica_copy_skipped" << ", key=" << key
                  << ", info=target_replicas_already_exist";
        // Target replicas already exist, consider it success
        auto copy_end_result =
            dynamic_replication_lease_id == UUID{}
                ? master_client_.CopyEnd(key, tenant_id)
                : master_client_.DynamicReplicaCopyEnd(
                      key, tenant_id, dynamic_replication_lease_id,
                      dynamic_replication_version_epoch);
        if (!copy_end_result.has_value()) {
            ErrorCode error = copy_end_result.error();
            LOG(ERROR) << "action=replica_copy_failed" << ", key=" << key
                       << ", error=copy_end_failed" << ", error_code=" << error;
            return tl::unexpected(error);
        }
        return {};
    }

    auto copy_end = [&]() {
        return dynamic_replication_lease_id == UUID{}
                   ? master_client_.CopyEnd(key, tenant_id)
                   : master_client_.DynamicReplicaCopyEnd(
                         key, tenant_id, dynamic_replication_lease_id,
                         dynamic_replication_version_epoch);
    };
    auto copy_revoke = [&]() {
        return dynamic_replication_lease_id == UUID{}
                   ? master_client_.CopyRevoke(key, tenant_id)
                   : master_client_.DynamicReplicaCopyRevoke(
                         key, tenant_id, dynamic_replication_lease_id,
                         dynamic_replication_version_epoch);
    };
    auto result = ExecuteReplicaTransfer(key, "copy", copy_end, copy_revoke,
                                         response.source, response.targets);

    if (result.has_value()) {
        LOG(INFO) << "action=replica_copy_success" << ", key=" << key
                  << ", target_count=" << response.targets.size();
    }

    return result;
}

tl::expected<void, ErrorCode> Client::Move(const std::string& key,
                                           const std::string& source,
                                           const std::string& target) {
    return Move(key, master_client_.tenant_id(), source, target);
}

tl::expected<void, ErrorCode> Client::Move(const std::string& key,
                                           const std::string& tenant_id,
                                           const std::string& source,
                                           const std::string& target) {
    LOG(INFO) << "action=replica_move_start" << ", key=" << key
              << ", source_segment=" << source << ", target_segment=" << target;

    // Call MoveStart first - it validates existence and allocates replica if
    // needed
    auto move_start_result =
        master_client_.MoveStart(key, tenant_id, source, target);
    if (!move_start_result.has_value()) {
        ErrorCode error = move_start_result.error();
        LOG(ERROR) << "action=replica_move_failed" << ", key=" << key
                   << ", error=move_start_failed" << ", error_code=" << error;
        // MoveStart already validated existence, so we just return the error
        return tl::unexpected(error);
    }

    const auto& response = move_start_result.value();
    if (!response.target.has_value()) {
        LOG(INFO) << "action=replica_move_skipped" << ", key=" << key
                  << ", info=target_replica_already_exists";
        // Target already exists, consider it success
        auto move_end_result = master_client_.MoveEnd(key, tenant_id);
        if (!move_end_result.has_value()) {
            ErrorCode error = move_end_result.error();
            LOG(ERROR) << "action=replica_move_failed" << ", key=" << key
                       << ", error=move_end_failed" << ", error_code=" << error;
            return tl::unexpected(error);
        }
        return {};
    }

    std::vector<Replica::Descriptor> targets = {response.target.value()};

    auto result = ExecuteReplicaTransfer(
        key, "move", [&]() { return master_client_.MoveEnd(key, tenant_id); },
        [&]() { return master_client_.MoveRevoke(key, tenant_id); },
        response.source, targets);

    if (result.has_value()) {
        LOG(INFO) << "action=replica_move_success" << ", key=" << key
                  << ", source_segment=" << source
                  << ", target_segment=" << target;
    }

    return result;
}

tl::expected<QueryTaskResponse, ErrorCode> Client::QueryTask(
    const UUID& task_id) {
    return master_client_.QueryTask(task_id);
}

tl::expected<std::vector<TaskAssignment>, ErrorCode> Client::FetchTasks(
    size_t batch_size) {
    return master_client_.FetchTasks(batch_size);
}

tl::expected<void, ErrorCode> Client::MarkTaskToComplete(
    const TaskCompleteRequest& update_request) {
    return master_client_.MarkTaskToComplete(update_request);
}

ErrorCode Client::PrepareStorageBackend(const std::string& storage_root_dir,
                                        const std::string& fsdir,
                                        bool enable_eviction,
                                        uint64_t quota_bytes) {
    // Initialize storage backend
    auto backend_result =
        StorageBackend::Create(storage_root_dir, fsdir, enable_eviction);
    if (!backend_result) {
        LOG(ERROR) << "Failed to create storage backend: "
                   << backend_result.error();
        return backend_result.error();
    }
    // Initialize into a local first and only publish to storage_backend_
    // after a successful Init(): users of storage_backend_ only null-check
    // it, so it must never point to a backend whose Init() failed (using it
    // before successful Init() is undefined behavior). If Init() throws,
    // stack unwinding destroys the local and storage_backend_ stays clean.
    auto backend = std::move(backend_result.value());
    auto init_result = backend->Init(quota_bytes);
    if (!init_result) {
        LOG(ERROR) << "Failed to initialize StorageBackend. Error: "
                   << init_result.error() << ". The backend will be unusable.";
        return init_result.error();
    }
    storage_backend_ = std::move(backend);
    return ErrorCode::OK;
}

void Client::PutToLocalFile(const std::string& key,
                            const std::vector<Slice>& slices,
                            const DiskDescriptor& disk_descriptor) {
    if (!storage_backend_) return;

    size_t total_size = 0;
    for (const auto& slice : slices) {
        total_size += slice.size;
    }

    std::string path = disk_descriptor.file_path;

    // Synchronous D2H staging + copy into std::string.
    // Done on the calling thread to guarantee GPU buffers are still valid
    // (BatchPut has not yet returned to Python, so blocks are not reused).
    std::string value;
    value.reserve(total_size);
    auto runtime_accelerator =
        device::GetAcceleratorRegistry().RuntimeAccelerators();

    for (const auto& slice : slices) {
        device::PointerInfo info{};
        auto* device =
            runtime_accelerator.FindDeviceForPointer(slice.ptr, &info);
        if (device) {
            device->SetContext(info.device_id);
            auto buf = pinned_buffer_pool_->Acquire(slice.size);
            if (!device->Copy(buf.data, slice.ptr, slice.size,
                              device::CopyDirection::kDeviceToHost)) {
                LOG(ERROR) << "D2H copy failed for key: " << key
                           << ", triggering PutRevoke for disk replica";
                pinned_buffer_pool_->Release(std::move(buf));
                // Must revoke to avoid phantom replica in master
                auto revoke_result =
                    master_client_.PutRevoke(key, ReplicaType::DISK);
                if (!revoke_result) {
                    LOG(ERROR)
                        << "Failed to revoke put operation for key: " << key;
                }
                return;
            }
            value.append(buf.data, slice.size);
            pinned_buffer_pool_->Release(std::move(buf));
        } else {
            value.append(static_cast<char*>(slice.ptr), slice.size);
        }
    }

    // Async StoreObject + PutEnd (unchanged from original)
    write_thread_pool_.enqueue([this, backend = storage_backend_, key,
                                value = std::move(value), path] {
        ReplicaType replica_type = ReplicaType::DISK;
        // Store the object
        const auto io_started = std::chrono::steady_clock::now();
        auto store_result = backend->StoreObject(
            path, value, key,
            [this, replica_type](const std::vector<std::string>& evicted_keys)
                -> tl::expected<void, ErrorCode> {
                auto evict_results = master_client_.BatchEvictDiskReplica(
                    evicted_keys, replica_type);
                if (evict_results.size() != evicted_keys.size()) {
                    return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                }
                for (size_t i = 0; i < evict_results.size(); ++i) {
                    if (!evict_results[i]) {
                        if (evict_results[i].error() ==
                            ErrorCode::OBJECT_NOT_FOUND) {
                            VLOG(1) << "Master no longer tracks evicted key: "
                                    << evicted_keys[i];
                            continue;
                        }
                        LOG(WARNING)
                            << "Failed to notify master about evicted key: "
                            << evicted_keys[i]
                            << ", error: " << evict_results[i].error();
                        return tl::make_unexpected(evict_results[i].error());
                    }
                }
                return {};
            });
        const double io_duration_seconds =
            std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                          io_started)
                .count();
        // Legacy DISK and DFS replicas share the same direct-storage metric
        // source. LOCAL_DISK replicas are the separate local-disk source.
        ObserveDirectIo("write", DirectStorageMetricSource(replica_type),
                        store_result.has_value(),
                        store_result ? value.size() : 0, io_duration_seconds);

        if (!store_result) {
            // If storage failed, revoke the put operation
            LOG(ERROR) << "Failed to store object for key: " << key;
            auto revoke_result = master_client_.PutRevoke(key, replica_type);
            if (!revoke_result) {
                LOG(ERROR) << "Failed to revoke put operation for key: " << key;
            }
            return;
        }

        // If storage succeeded, end the put operation
        auto end_result =
            master_client_.PutEnd(ObjectMeta{key, std::nullopt}, replica_type);
        if (!end_result) {
            LOG(ERROR) << "Failed to end put operation for key: " << key;
        }
    });
}

ErrorCode Client::TransferData(const Replica::Descriptor& replica_descriptor,
                               std::vector<Slice>& slices,
                               TransferRequest::OpCode op_code) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return ErrorCode::INVALID_PARAMS;
    }

    std::optional<TransferFuture> future;
    if (replica_descriptor.is_nof_replica()) {
        auto contiguous_range = GetContiguousSliceRange(slices);
        if (!contiguous_range.has_value()) {
            LOG(ERROR) << "NoF transfer requires contiguous slices";
            return ErrorCode::INVALID_PARAMS;
        }
        future = transfer_submitter_->submit(replica_descriptor, slices,
                                             op_code, contiguous_range->ptr,
                                             contiguous_range->size);
    } else {
        future =
            transfer_submitter_->submit(replica_descriptor, slices, op_code);
    }
    if (!future) {
        LOG(ERROR) << "Failed to submit transfer operation";
        return ErrorCode::TRANSFER_FAIL;
    }

    VLOG(1) << "Using transfer strategy: " << future->strategy();

    return future->get();
}

std::optional<TransferFuture> Client::SubmitRangeRead(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t src_offset) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return std::nullopt;
    }
    return transfer_submitter_->submitRangeRead(replica_descriptor, slices,
                                                src_offset);
}

std::optional<TransferFuture> Client::SubmitRangeWrite(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t dst_offset) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return std::nullopt;
    }
    return transfer_submitter_->submitRangeWrite(replica_descriptor, slices,
                                                 dst_offset);
}

std::vector<tl::expected<int64_t, ErrorCode>> Client::BatchTransferReadRanges(
    const std::vector<Replica::Descriptor>& replicas,
    const std::vector<std::vector<Slice>>& slices,
    const std::vector<std::vector<uint64_t>>& src_offsets) {
    std::vector<tl::expected<int64_t, ErrorCode>> results(
        replicas.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    if (replicas.size() != slices.size() ||
        replicas.size() != src_offsets.size()) {
        LOG(ERROR) << "BatchTransferReadRanges size mismatch: replicas="
                   << replicas.size() << ", slices=" << slices.size()
                   << ", offsets=" << src_offsets.size();
        return results;
    }

    size_t fragment_count = 0;
    for (const auto& entry : slices) {
        fragment_count += entry.size();
    }

    // Every fragment of every entry goes into one scatter submit so the
    // transport sees the whole layer at once instead of one transfer per key.
    ScatterRangeBuilder builder(fragment_count);
    std::vector<std::optional<ErrorCode>> entry_errors(replicas.size());
    for (size_t i = 0; i < replicas.size(); ++i) {
        if (slices[i].size() != src_offsets[i].size()) {
            LOG(ERROR) << "BatchTransferReadRanges fragment count mismatch, "
                       << "entry=" << i << ", slices=" << slices[i].size()
                       << ", offsets=" << src_offsets[i].size();
            continue;  // results[i] stays INVALID_PARAMS
        }
        if (!replicas[i].is_memory_replica()) {
            LOG(ERROR) << "Range read requires a memory replica, entry=" << i;
            continue;
        }

        const auto& handle =
            replicas[i].get_memory_descriptor().buffer_descriptor;
        int64_t transferred = 0;
        for (size_t j = 0; j < slices[i].size(); ++j) {
            builder.Add(TransferRequest::READ, handle, slices[i][j],
                        src_offsets[i][j], &entry_errors[i]);
            transferred += static_cast<int64_t>(slices[i][j].size);
        }
        results[i] = transferred;  // optimistic; corrected on await
    }

    if (builder.empty()) {
        return results;
    }

    auto operation = SubmitScatter(builder.ranges());
    if (!operation) {
        LOG(ERROR) << "Failed to submit batch range read";
        for (size_t i = 0; i < results.size(); ++i) {
            if (results[i].has_value()) {
                RecordServiceReadFailure(ReplicaType::MEMORY);
                results[i] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            }
        }
        return results;
    }
    (void)operation->wait();

    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            continue;
        }
        if (entry_errors[i].has_value()) {
            RecordServiceReadFailure(ReplicaType::MEMORY);
            LOG(ERROR) << "Range read failed, entry=" << i
                       << ", error="
                       << static_cast<int>(entry_errors[i].value());
            results[i] = tl::unexpected(entry_errors[i].value());
        }
    }
    return results;
}

std::vector<tl::expected<int64_t, ErrorCode>> Client::BatchTransferWriteRanges(
    const std::vector<std::vector<Replica::Descriptor>>& replicas_per_entry,
    const std::vector<std::vector<Slice>>& slices,
    const std::vector<std::vector<uint64_t>>& dst_offsets) {
    std::vector<tl::expected<int64_t, ErrorCode>> results(
        replicas_per_entry.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    if (replicas_per_entry.size() != slices.size() ||
        replicas_per_entry.size() != dst_offsets.size()) {
        LOG(ERROR) << "BatchTransferWriteRanges size mismatch: entries="
                   << replicas_per_entry.size() << ", slices=" << slices.size()
                   << ", offsets=" << dst_offsets.size();
        return results;
    }

    size_t fragment_count = 0;
    for (size_t i = 0; i < replicas_per_entry.size(); ++i) {
        for (const auto& replica : replicas_per_entry[i]) {
            if (replica.is_memory_replica()) {
                fragment_count += slices[i].size();
            }
        }
    }

    // One scatter submit covers every replica of every entry, so replication
    // fans out inside a single transfer instead of one submit per fragment.
    ScatterRangeBuilder builder(fragment_count);
    std::vector<std::optional<ErrorCode>> entry_errors(
        replicas_per_entry.size());
    for (size_t i = 0; i < replicas_per_entry.size(); ++i) {
        if (slices[i].size() != dst_offsets[i].size()) {
            LOG(ERROR) << "BatchTransferWriteRanges fragment count mismatch, "
                       << "entry=" << i << ", slices=" << slices[i].size()
                       << ", offsets=" << dst_offsets[i].size();
            continue;  // results[i] stays INVALID_PARAMS
        }

        // Logical bytes: counted once per fragment, not per replica.
        int64_t transferred = 0;
        for (const auto& slice : slices[i]) {
            transferred += static_cast<int64_t>(slice.size);
        }
        bool submitted = false;
        for (const auto& replica : replicas_per_entry[i]) {
            if (!replica.is_memory_replica()) {
                continue;
            }
            const auto& handle =
                replica.get_memory_descriptor().buffer_descriptor;
            for (size_t j = 0; j < slices[i].size(); ++j) {
                builder.Add(TransferRequest::WRITE, handle, slices[i][j],
                            dst_offsets[i][j], &entry_errors[i]);
                submitted = true;
            }
        }
        if (!submitted) {
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }
        results[i] = transferred;  // optimistic; corrected on await
    }

    if (builder.empty()) {
        return results;
    }

    auto operation = SubmitScatter(builder.ranges());
    if (!operation) {
        LOG(ERROR) << "Failed to submit batch range write";
        for (auto& result : results) {
            if (result.has_value()) {
                result = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            }
        }
        return results;
    }
    (void)operation->wait();

    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value() || !entry_errors[i].has_value()) {
            continue;
        }
        LOG(ERROR) << "Range write failed, entry=" << i
                   << ", error=" << static_cast<int>(entry_errors[i].value());
        results[i] = tl::unexpected(entry_errors[i].value());
    }
    return results;
}

ErrorCode Client::TransferReadInternal(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t src_offset) {
    auto future = SubmitRangeRead(replica_descriptor, slices, src_offset);
    if (!future) {
        LOG(ERROR) << "Failed to submit range read operation";
        return ErrorCode::TRANSFER_FAIL;
    }

    VLOG(1) << "Using transfer strategy: " << future->strategy();

    return future->get();
}

ErrorCode Client::TransferWrite(const Replica::Descriptor& replica_descriptor,
                                std::vector<Slice>& slices) {
    return TransferData(replica_descriptor, slices, TransferRequest::WRITE);
}

ErrorCode Client::TransferWriteRange(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t dst_offset) {
    auto future = SubmitRangeWrite(replica_descriptor, slices, dst_offset);
    if (!future) {
        LOG(ERROR) << "Failed to submit range write operation";
        return ErrorCode::TRANSFER_FAIL;
    }

    VLOG(1) << "Using transfer strategy: " << future->strategy();
    return future->get();
}

ErrorCode Client::TransferRead(const Replica::Descriptor& replica_descriptor,
                               std::vector<Slice>& slices) {
    size_t total_size = 0;
    if (replica_descriptor.is_memory_replica()) {
        auto& mem_desc = replica_descriptor.get_memory_descriptor();
        total_size = mem_desc.buffer_descriptor.size_;
    } else if (replica_descriptor.is_nof_replica()) {
        auto& nof_desc = replica_descriptor.get_nof_descriptor();
        total_size = nof_desc.buffer_descriptor.size_;
    } else if (replica_descriptor.is_disk_replica()) {
        auto& disk_desc = replica_descriptor.get_disk_descriptor();
        total_size = disk_desc.object_size;
    } else if (replica_descriptor.is_local_disk_replica()) {
        auto& disk_desc = replica_descriptor.get_local_disk_descriptor();
        total_size = disk_desc.object_size;
    } else if (replica_descriptor.is_dfs_replica()) {
        total_size = replica_descriptor.get_dfs_descriptor().object_size;
    }

    size_t slices_size = CalculateSliceSize(slices);
    if (slices_size < total_size) {
        LOG(ERROR) << "Slice size " << slices_size << " is smaller than total "
                   << "size " << total_size;
        return ErrorCode::INVALID_PARAMS;
    }

    if (replica_descriptor.is_dfs_replica()) {
        LOG(ERROR) << "DFS reads require object key context";
        return ErrorCode::INVALID_REPLICA;
    }

    return TransferData(replica_descriptor, slices, TransferRequest::READ);
}

ErrorCode Client::ReadDfsReplica(const std::string& key,
                                 const Replica::Descriptor& replica_descriptor,
                                 std::vector<Slice>& slices) {
    if (!replica_descriptor.is_dfs_replica()) {
        return ErrorCode::INVALID_REPLICA;
    }
    if (!DfsReadAllowed()) {
        return ErrorCode::DFS_SERVICE_UNAVAILABLE;
    }
    if (!dfs_storage_backend_) {
        LOG(ERROR) << "DFS backend is not initialized";
        return ErrorCode::DFS_SERVICE_UNAVAILABLE;
    }

    const auto& desc = replica_descriptor.get_dfs_descriptor();
    std::vector<DfsReadRequest> requests{
        DfsReadRequest{key, desc, slices, nullptr}};
    auto results = dfs_storage_backend_->BatchRead(requests);
    if (results.size() != 1) {
        LOG(ERROR) << "DFS BatchRead response size mismatch for key " << key;
        return ErrorCode::INTERNAL_ERROR;
    }
    if (!results[0]) {
        LOG(ERROR) << "DFS read failed for key " << key << ": "
                   << results[0].error();
        return results[0].error();
    }
    return ErrorCode::OK;
}

ErrorCode Client::TransferReadRange(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
    uint64_t src_offset) {
    return TransferReadInternal(replica_descriptor, slices, src_offset);
}

void Client::PollAndDispatchTasks() {
    // Drain a few full backfill batches per tick without an unbounded fetch
    // loop or changing the normal copy/move polling cadence.
    constexpr size_t kMaxBackfillBatches = 4;
    for (size_t batch = 0; batch < kMaxBackfillBatches && task_running_.load();
         ++batch) {
        auto fetch_result = FetchTasks(kTaskBatchSize);
        if (fetch_result.has_value()) {
            const auto& tasks = fetch_result.value();
            bool has_backfill = false;
            if (!tasks.empty()) {
                LOG(INFO) << "action=task_poll_success"
                          << ", task_count=" << tasks.size();
                for (const auto& task_assignment : tasks) {
                    has_backfill |=
                        task_assignment.type == TaskType::DFS_BACKFILL;
                    SubmitTask(task_assignment);
                }
            }
            if (tasks.size() < kTaskBatchSize || !has_backfill) return;
        } else {
            ErrorCode error = fetch_result.error();
            // Only log if it's not an RPC failure (which is expected
            // during connection failures)
            if (error != ErrorCode::RPC_FAIL) {
                LOG(WARNING)
                    << "action=task_poll_failed" << ", error_code=" << error;
            }
            return;
        }
    }
}

void Client::TaskPollThreadMain() {
    const auto poll_interval = std::chrono::milliseconds(1000);

    while (task_poll_running_.load()) {
        PollAndDispatchTasks();
        std::this_thread::sleep_for(poll_interval);
    }
}

void Client::SubmitTask(const TaskAssignment& assignment) {
    if (!task_running_.load()) {
        LOG(WARNING) << "action=task_rejected" << ", task_id=" << assignment.id
                     << ", reason=executor_stopped";
        return;
    }

    // Construct ClientTask from TaskAssignment
    ClientTask client_task;
    client_task.assignment = assignment;
    client_task.retry_count = 0;

    auto& executor = assignment.type == TaskType::DFS_BACKFILL
                         ? dfs_backfill_thread_pool_
                         : task_thread_pool_;
    executor.enqueue([this, client_task]() { ExecuteTask(client_task); });
}

tl::expected<std::shared_ptr<DistributedStorageBackend>, ErrorCode>
Client::GetDfsBackfillBackend() {
    if (dfs_storage_backend_) return dfs_storage_backend_;
    std::lock_guard<std::mutex> lock(dfs_backfill_backend_mutex_);
    if (dfs_backfill_backend_) return dfs_backfill_backend_;

    auto storage = master_client_.GetStorageConfig();
    if (!storage) return tl::make_unexpected(storage.error());
    if (storage->dfs_fsdir.empty() || storage->dfs_root_identity.empty()) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    FileStorageConfig config;
    config.storage_backend_type = StorageBackendType::kDistributed;
    auto backend = CreateStorageBackend(config, storage->dfs_root_identity,
                                        storage->dfs_fsdir);
    if (!backend) return tl::make_unexpected(backend.error());
    dfs_backfill_backend_ =
        std::dynamic_pointer_cast<DistributedStorageBackend>(*backend);
    if (dfs_backfill_backend_) {
        dfs_backfill_backend_->SetDescriptorRootDir(storage->dfs_fsdir);
        dfs_backfill_backend_->SetMasterEpochProvider(
            master_client_.MakeDfsMasterEpochProvider());
    }
    // The executor checks initialization before acquiring a source pin.
    return dfs_backfill_backend_;
}

bool Client::BeginDfsBackfillAttempt(const UUID& task_id) {
    std::lock_guard<std::mutex> lock(dfs_backfill_backend_mutex_);
    if (dfs_backfill_failures_ != 0) {
        if (std::chrono::steady_clock::now() < dfs_backfill_retry_after_ ||
            dfs_backfill_probe_task_ != UUID{})
            return false;
        // After a cooldown only one real write probes this holder's mount.
        dfs_backfill_probe_task_ = task_id;
    }
    return true;
}

void Client::FinishDfsBackfillAttempt(const UUID& task_id, ErrorCode result,
                                      bool storage_failure) {
    std::lock_guard<std::mutex> lock(dfs_backfill_backend_mutex_);
    const bool probe = dfs_backfill_probe_task_ == task_id;
    if (probe) dfs_backfill_probe_task_ = UUID{};
    if (result == ErrorCode::OK) {
        dfs_backfill_failures_ = 0;
        dfs_backfill_retry_after_ = {};
    } else if (storage_failure && (dfs_backfill_failures_ == 0 || probe)) {
        // Concurrent failures from the initial healthy batch do not keep
        // extending the pause. Missing keys/source races do not open it.
        dfs_backfill_failures_ = std::min(dfs_backfill_failures_ + 1, 4u);
        dfs_backfill_retry_after_ =
            std::chrono::steady_clock::now() +
            std::chrono::seconds(
                std::min(5u << (dfs_backfill_failures_ - 1), 30u));
    }
}

void Client::ExecuteTask(const ClientTask& client_task) {
    const auto& assignment = client_task.assignment;
    ErrorCode result = ErrorCode::OK;
    bool backfill_attempted = false;
    bool backfill_storage_failure = false;

    try {
        switch (assignment.type) {
            case TaskType::DFS_BACKFILL: {
                if (!DfsWriteAllowed() ||
                    !BeginDfsBackfillAttempt(assignment.id)) {
                    result = ErrorCode::DFS_SERVICE_UNAVAILABLE;
                    break;
                }
                backfill_attempted = true;
                backfill_storage_failure = true;
                auto backend = GetDfsBackfillBackend();
                if (!backend) {
                    result = backend.error();
                    break;
                }
                auto initialized = (*backend)->EnsureInitialized();
                if (!initialized) {
                    result = initialized.error();
                    break;
                }
                DfsBackfillPayload payload;
                struct_json::from_json(payload, assignment.payload);
                backfill_storage_failure = false;
                auto start = master_client_.DfsBackfillStart(
                    payload.key, payload.tenant_id, payload.source,
                    assignment.id);
                if (!start) {
                    result = start.error();
                    backfill_storage_failure =
                        result <= ErrorCode::DFS_NETWORK_TIMEOUT &&
                        result >= ErrorCode::DFS_PARTIAL_WRITE;
                    break;
                }
                auto finish = [&](bool success) {
                    return master_client_.DfsBackfillFinish(
                        payload.key, payload.tenant_id, assignment.id, success);
                };
                backfill_storage_failure = true;
                auto copied = ExecuteReplicaTransfer(
                    payload.key, "dfs_backfill", [&] { return finish(true); },
                    [&] { return finish(false); }, start->source,
                    start->targets, *backend);
                result = copied ? ErrorCode::OK : copied.error();
                backfill_storage_failure =
                    result == ErrorCode::TRANSFER_FAIL ||
                    result == ErrorCode::FILE_OPEN_FAIL ||
                    result == ErrorCode::FILE_WRITE_FAIL ||
                    (result <= ErrorCode::DFS_NETWORK_TIMEOUT &&
                     result >= ErrorCode::DFS_PARTIAL_WRITE);
                break;
            }
            case TaskType::REPLICA_COPY: {
                ReplicaCopyPayload payload;
                struct_json::from_json(payload, assignment.payload);
                const UUID dynamic_lease_id{
                    payload.dynamic_replication_lease_id_high,
                    payload.dynamic_replication_lease_id_low};
                auto copy_result =
                    Copy(payload.key, payload.tenant_id, payload.source,
                         payload.targets, dynamic_lease_id,
                         payload.dynamic_replication_version_epoch);
                if (copy_result.has_value()) {
                    result = ErrorCode::OK;
                } else {
                    result = copy_result.error();
                }
                break;
            }
            case TaskType::REPLICA_MOVE: {
                ReplicaMovePayload payload;
                struct_json::from_json(payload, assignment.payload);
                auto move_result = Move(payload.key, payload.tenant_id,
                                        payload.source, payload.target);
                if (move_result.has_value()) {
                    result = ErrorCode::OK;
                } else {
                    result = move_result.error();
                }
                break;
            }
            default:
                LOG(ERROR) << "action=task_execution_failed"
                           << ", task_id=" << assignment.id
                           << ", error=unknown_task_type"
                           << ", task_type=" << assignment.type;
                result = ErrorCode::INVALID_PARAMS;
                break;
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "action=task_execution_failed"
                   << ", task_id=" << assignment.id << ", error=exception"
                   << ", exception=" << e.what();
        result = ErrorCode::INTERNAL_ERROR;
    }

    if (backfill_attempted) {
        FinishDfsBackfillAttempt(assignment.id, result,
                                 backfill_storage_failure);
    }

    if (result == ErrorCode::OK) {
        TaskCompleteRequest complete_request;
        complete_request.id = assignment.id;
        complete_request.status = TaskStatus::SUCCESS;
        complete_request.message = "Task completed successfully";
        auto complete_result =
            master_client_.MarkTaskToComplete(complete_request);
        if (!complete_result.has_value()) {
            LOG(WARNING) << "action=task_complete_failed"
                         << ", task_id=" << assignment.id
                         << ", error_code=" << complete_result.error();
        }
    } else {
        uint32_t current_retry_count = client_task.retry_count;
        // Only retry on allocation failures (NO_AVAILABLE_HANDLE)
        // Other errors (e.g., OBJECT_NOT_FOUND, REPLICA_NOT_FOUND) should
        // not be retried
        bool should_retry =
            assignment.type != TaskType::DFS_BACKFILL &&
            (result == ErrorCode::NO_AVAILABLE_HANDLE) &&
            (current_retry_count < assignment.max_retry_attempts);

        if (should_retry) {
            ClientTask retry_task = client_task;
            retry_task.increment_retry();

            const auto retry_delay =
                std::chrono::milliseconds(50 * (current_retry_count + 1));
            std::this_thread::sleep_for(retry_delay);

            LOG(WARNING) << "action=task_execution_failed_retry"
                         << ", task_id=" << assignment.id
                         << ", error_code=" << result
                         << ", retry_count=" << current_retry_count
                         << ", max_retry_count="
                         << assignment.max_retry_attempts << ", will_retry=true"
                         << ", retry_delay=" << retry_delay.count() << "ms";

            task_thread_pool_.enqueue(
                [this, retry_task]() { ExecuteTask(retry_task); });
        } else {
            // DFS recovery is retried by the master after a cooldown. A
            // persistent outage must not produce one error per degraded key.
            static DfsLogLimiter dfs_backfill_limiter;
            auto suppressed = assignment.type == TaskType::DFS_BACKFILL
                                  ? dfs_backfill_limiter.ShouldLog()
                                  : std::optional<uint64_t>(0);
            if (suppressed)
                LOG(ERROR) << "action=task_execution_failed"
                           << ", task_id=" << assignment.id
                           << ", error_code=" << result
                           << ", retry_count=" << current_retry_count
                           << ", max_retry_count="
                           << assignment.max_retry_attempts
                           << ", suppressed=" << *suppressed;
            TaskCompleteRequest complete_request;
            complete_request.id = assignment.id;
            complete_request.status = TaskStatus::FAILED;
            complete_request.message =
                toString(result) + " (max retries reached: " +
                std::to_string(assignment.max_retry_attempts) + ")";
            auto complete_result =
                master_client_.MarkTaskToComplete(complete_request);
            if (!complete_result.has_value()) {
                LOG(WARNING) << "action=task_complete_failed"
                             << ", task_id=" << assignment.id
                             << ", error_code=" << complete_result.error();
            }
        }
    }
}

void Client::StorageHeartbeatThreadMain() {
    // How many failed pings before reconnecting via the HA coordinator
    const int max_ping_fail_count = 3;
    // How long to wait for next ping after success
    const int success_ping_interval_ms = 1000;
    // How long to wait for next ping after failure
    const int fail_ping_interval_ms = 1000;
    // Increment after a ping failure, reset after a ping success
    int ping_fail_count = 0;

    auto remount_segment = [this]() {
        // This lock must be held until the remount rpc is finished,
        // otherwise there will be corner cases, e.g., a segment is
        // unmounted successfully first, and then remounted again in
        // this thread.
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        std::vector<Segment> segments;
        for (auto it : mounted_segments_) {
            auto& segment = it.second;
            segments.emplace_back(segment);
        }
        auto remount_result = master_client_.ReMountSegment(segments);
        if (!remount_result) {
            ErrorCode err = remount_result.error();
            LOG(ERROR) << "Failed to remount segments: " << err;
        }
        // Re-publish Transfer Engine segment descriptors to the HTTP
        // metadata server.  When Master (which hosts the HTTP metadata
        // server in the same process) is killed and restarted, all
        // in-memory KV entries are lost.  ReMountSegment above only
        // restores Master-side allocation state; it does NOT write back
        // the transport-level segment descriptors.  Without this, remote
        // peers get HTTP 404 when querying our segment descriptor and
        // data transfers fail.
        auto metadata = transfer_engine_->getMetadata();
        if (metadata) {
            int rc = metadata->updateLocalSegmentDesc();
            if (rc != 0) {
                LOG(ERROR) << "Failed to re-publish segment descriptor "
                           << "to metadata server, rc=" << rc
                           << ", will retry in next heartbeat cycle";
                segment_desc_publish_pending_.store(true);
            } else {
                segment_desc_publish_pending_.store(false);
            }
            // Also re-publish RPC meta entry (mooncake/rpc_meta/<hostname>).
            // Remote peers need this to locate our RDMA RPC port for
            // handshake.  Like segment descriptors, this entry is lost
            // when the HTTP metadata server is cleared on Master restart.
            rc = metadata->rePublishRpcMetaEntry(local_hostname_);
            if (rc != 0) {
                LOG(ERROR) << "Failed to re-publish RPC meta entry "
                           << "to metadata server, rc=" << rc
                           << ", will retry in next heartbeat cycle";
                rpc_meta_publish_pending_.store(true);
            } else {
                rpc_meta_publish_pending_.store(false);
            }
        }
        // Note: LOCAL_DISK segment remount is NOT done here.
        // It is handled by FileStorage::Heartbeat() when it detects
        // SEGMENT_NOT_FOUND, which also triggers ScanMeta to
        // re-register offloaded object metadata.
    };
    // Use another thread to remount segments to avoid blocking the ping
    // thread
    std::future<void> remount_segment_future;

    while (storage_heartbeat_running_.load()) {
        // Join the remount segment thread if it is ready
        if (remount_segment_future.valid() &&
            remount_segment_future.wait_for(std::chrono::seconds(0)) ==
                std::future_status::ready) {
            remount_segment_future = std::future<void>();
        }

        // Ping master
        auto ping_result = master_client_.Ping();
        if (ping_result) {
            // Reset ping failure count
            ping_fail_count = 0;
            last_ping_success_.store(true);
            auto& ping_response = ping_result.value();
            if (ping_response.client_status == ClientStatus::NEED_REMOUNT &&
                !remount_segment_future.valid()) {
                // Ensure at most one remount segment thread is running
                remount_segment_future =
                    std::async(std::launch::async, remount_segment);
            } else if (segment_desc_publish_pending_.load() &&
                       !remount_segment_future.valid()) {
                // Previous remount succeeded but updateLocalSegmentDesc()
                // failed (e.g. transient HTTP error).  Retry it directly
                // without re-running ReMountSegment.
                auto metadata = transfer_engine_->getMetadata();
                if (metadata) {
                    int rc = metadata->updateLocalSegmentDesc();
                    if (rc != 0) {
                        LOG(ERROR)
                            << "Retry: failed to re-publish segment "
                            << "descriptor to metadata server, rc=" << rc;
                    } else {
                        LOG(INFO) << "Retry: successfully re-published "
                                  << "segment descriptor to metadata server";
                        segment_desc_publish_pending_.store(false);
                    }
                }
            } else if (rpc_meta_publish_pending_.load() &&
                       !remount_segment_future.valid()) {
                // Previous remount succeeded but rePublishRpcMetaEntry()
                // failed.  Retry it directly.
                auto metadata = transfer_engine_->getMetadata();
                if (metadata) {
                    int rc = metadata->rePublishRpcMetaEntry(local_hostname_);
                    if (rc != 0) {
                        LOG(ERROR)
                            << "Retry: failed to re-publish RPC "
                            << "meta entry to metadata server, rc=" << rc;
                    } else {
                        LOG(INFO) << "Retry: successfully re-published "
                                  << "RPC meta entry to metadata server";
                        rpc_meta_publish_pending_.store(false);
                    }
                }
            }

            std::this_thread::sleep_for(
                std::chrono::milliseconds(success_ping_interval_ms));
            continue;
        }

        ping_fail_count++;
        last_ping_success_.store(false);
        if (ping_fail_count < max_ping_fail_count) {
            LOG(ERROR) << "Failed to ping master";
            std::this_thread::sleep_for(
                std::chrono::milliseconds(fail_ping_interval_ms));
            continue;
        }

        // Exceeded ping failure threshold. Reconnect based on mode.
        if (leader_coordinator_) {
            LOG(ERROR)
                << "Failed to ping master for " << ping_fail_count
                << " times; fetching latest master view and reconnecting";
            auto current_view = leader_coordinator_->ReadCurrentView();
            if (!current_view) {
                LOG(ERROR) << "Failed to get new master view: "
                           << toString(current_view.error());
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(fail_ping_interval_ms));
                continue;
            }
            if (!current_view.value().has_value()) {
                LOG(WARNING) << "No active master view is published yet";
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(fail_ping_interval_ms));
                continue;
            }

            const auto& next_view = current_view.value().value();
            auto err = SwitchLeader(next_view);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to connect to master "
                           << next_view.leader_address << ": " << toString(err);
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(fail_ping_interval_ms));
                continue;
            }

            LOG(INFO) << "Reconnected to master " << next_view.leader_address;
            ping_fail_count = 0;
        } else {
            const std::string current_master_address = direct_master_address_;
            LOG(ERROR) << "Failed to ping master for " << ping_fail_count
                       << " times (non-HA); reconnecting to "
                       << current_master_address;
            // Closed connections are replaced by the pool itself. Reusing the
            // address pool also bounds retention while the master is down.
            auto err = master_client_.Connect(current_master_address);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Reconnect failed to " << current_master_address
                           << ": " << toString(err);
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(fail_ping_interval_ms));
                continue;
            }
            LOG(INFO) << "Reconnected to master " << current_master_address;
            last_ping_success_.store(true);
            ping_fail_count = 0;
        }
    }
    // Explicitly wait for the remount segment thread to finish
    if (remount_segment_future.valid()) {
        remount_segment_future.wait();
    }
}

ErrorCode Client::FindFirstCompleteReplica(
    const std::vector<Replica::Descriptor>& replica_list,
    Replica::Descriptor& replica) {
    // Find the first complete replica
    for (size_t i = 0; i < replica_list.size(); ++i) {
        if (replica_list[i].status == ReplicaStatus::COMPLETE) {
            replica = replica_list[i];
            return ErrorCode::OK;
        }
    }

    // No complete replica found
    return ErrorCode::INVALID_REPLICA;
}

tl::expected<Replica::Descriptor, ErrorCode> Client::GetPreferredReplica(
    const std::vector<Replica::Descriptor>& replica_list) {
    if (replica_list.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (mounted_segments_.empty() || replica_list.size() == 1) {
        return replica_list[0];
    }

    std::unordered_set<std::string> local_endpoints;
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        for (const auto& [segment_id, segment] : mounted_segments_) {
            local_endpoints.insert(segment.te_endpoint);
        }
    }

    // Prefer local MEMORY replicas first
    for (const auto& rep : replica_list) {
        if (rep.is_memory_replica()) {
            const auto& mem_desc = rep.get_memory_descriptor();
            const std::string& endpoint =
                mem_desc.buffer_descriptor.transport_endpoint_;
            if (local_endpoints.count(endpoint)) {
                return rep;
            }
        }
    }

    // Then prefer local NOF_SSD replicas
    for (const auto& rep : replica_list) {
        if (rep.is_nof_replica()) {
            const auto& nof_desc = rep.get_nof_descriptor();
            const std::string& endpoint =
                nof_desc.buffer_descriptor.transport_endpoint_;
            if (local_endpoints.count(endpoint)) {
                return rep;
            }
        }
    }

    return replica_list[0];
}

size_t Client::GetLocalHotCacheSizeFromEnv() {
    if (const char* ev_size = std::getenv("MC_STORE_LOCAL_HOT_CACHE_SIZE")) {
        std::string ev_size_str(ev_size);
        std::string error_msg = "Invalid MC_STORE_LOCAL_HOT_CACHE_SIZE='" +
                                ev_size_str + "', disable local hot cache";
        // Check for negative values
        if (!ev_size_str.empty() && ev_size_str[0] == '-') {
            LOG(WARNING) << error_msg;
            return 0;
        }
        try {
            unsigned long long v = std::stoull(ev_size_str, nullptr, 10);
            if (v > 0) {
                return static_cast<size_t>(v);
            } else {
                LOG(WARNING) << error_msg;
                return 0;
            }
        } catch (const std::exception&) {
            LOG(WARNING) << error_msg;
            return 0;
        }
    }
    return 0;
}

size_t Client::GetLocalHotBlockSizeFromEnv(size_t default_value) {
    if (const char* ev_block_size =
            std::getenv("MC_STORE_LOCAL_HOT_BLOCK_SIZE")) {
        std::string ev_block_size_str(ev_block_size);
        std::string error_msg = "Invalid MC_STORE_LOCAL_HOT_BLOCK_SIZE='" +
                                ev_block_size_str +
                                "', using default block size";
        // Check for negative values
        if (!ev_block_size_str.empty() && ev_block_size_str[0] == '-') {
            LOG(WARNING) << error_msg;
            return default_value;
        }
        try {
            unsigned long long v = std::stoull(ev_block_size_str, nullptr, 10);
            if (v > 0) {
                return static_cast<size_t>(v);
            } else {
                LOG(WARNING) << error_msg;
                return default_value;
            }
        } catch (const std::exception&) {
            LOG(WARNING) << error_msg;
            return default_value;
        }
    }
    return default_value;
}

ErrorCode Client::InitLocalHotCache() {
    hot_cache_handler_.reset();
    UnregisterLocalHotCacheMemory();
    hot_cache_.reset();
    admission_sketch_.reset();

    if (object_checksum_enabled_) {
        return ErrorCode::OK;
    }

    // Defaults: hot cache is disabled unless MC_STORE_LOCAL_HOT_CACHE_SIZE is
    // set to a positive value; when enabled, default block size is 16MB and
    // thread_num is 2.
    size_t block_size = 16 * 1024 * 1024;  // 16MB default block size
    size_t thread_num = 2;

    // Read MC_STORE_LOCAL_HOT_CACHE_SIZE from environment
    size_t total_cache = GetLocalHotCacheSizeFromEnv();
    if (total_cache == 0) {
        // Environment variable not set or invalid, disable cache
        return ErrorCode::OK;
    }

    // Read MC_STORE_LOCAL_HOT_BLOCK_SIZE from environment
    block_size = GetLocalHotBlockSizeFromEnv(block_size);

    // MC_STORE_LOCAL_HOT_CACHE_USE_SHM: "1" enables memfd-backed shm (default
    // off). When enabled, hot cache is shareable with dummy clients via IPC.
    bool use_shm = false;
    if (const char* ev = std::getenv("MC_STORE_LOCAL_HOT_CACHE_USE_SHM")) {
        use_shm = (std::string(ev) == "1");
    }

    // Enable hot cache
    {
        hot_cache_ =
            std::make_shared<LocalHotCache>(total_cache, block_size, use_shm);
        // Check if cache initialization was successful
        if (hot_cache_->GetCacheSize() == 0) {
            LOG(ERROR)
                << "Local hot cache creation failed: no blocks allocated. "
                << "total_cache=" << total_cache;
            hot_cache_.reset();
            hot_cache_handler_.reset();
            admission_sketch_.reset();
            return ErrorCode::INVALID_PARAMS;
        }

        int rc = transfer_engine_->registerLocalMemory(
            hot_cache_->GetBaseAddress(), hot_cache_->GetTotalSize(),
            kWildcardLocation, true, true);
        if (rc != 0) {
            LOG(ERROR)
                << "Failed to register local hot cache memory with transfer "
                   "engine, base="
                << hot_cache_->GetBaseAddress()
                << ", size=" << hot_cache_->GetTotalSize() << ", ret=" << rc;
            hot_cache_.reset();
            hot_cache_handler_.reset();
            admission_sketch_.reset();
            hot_cache_memory_registered_ = false;
            return ErrorCode::INVALID_PARAMS;
        }
        hot_cache_memory_registered_ = true;

        LOG(INFO) << "Local hot cache enabled with cache size=" << total_cache
                  << ", block size=" << block_size
                  << ", block amount=" << hot_cache_->GetCacheSize()
                  << ", shm=" << (use_shm ? "on" : "off")
                  << ", transfer engine registered=on";
        // Create async handler with 2 worker threads
        hot_cache_handler_ =
            std::make_unique<LocalHotCacheHandler>(hot_cache_, thread_num);
        admission_sketch_ = std::make_unique<CountMinSketch>();

        // MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD: minimum CMS count before a
        // key is admitted to hot cache (default 2).
        if (const char* ev =
                std::getenv("MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD")) {
            std::string ev_str(ev);
            std::string error_msg =
                "Invalid MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD='" + ev_str +
                "', using default";
            try {
                unsigned long long v = std::stoull(ev_str, nullptr, 10);
                if (v > 0 && v <= 255) {
                    admission_threshold_ = static_cast<uint8_t>(v);
                } else {
                    LOG(WARNING) << error_msg;
                }
            } catch (const std::exception&) {
                LOG(WARNING) << error_msg;
            }
        }
    }
    return ErrorCode::OK;
}

void Client::UnregisterLocalHotCacheMemory() {
    if (!(hot_cache_ && hot_cache_memory_registered_)) {
        return;
    }
    if (!transfer_engine_) {
        hot_cache_memory_registered_ = false;
        return;
    }

    int rc = transfer_engine_->unregisterLocalMemory(
        hot_cache_->GetBaseAddress(), true);
    if (rc != 0 && rc != ERR_ADDRESS_NOT_REGISTERED) {
        LOG(ERROR)
            << "Failed to unregister local hot cache memory from transfer "
               "engine, base="
            << hot_cache_->GetBaseAddress()
            << ", size=" << hot_cache_->GetTotalSize() << ", ret=" << rc;
    }
    hot_cache_memory_registered_ = false;
}

void Client::ProcessSlicesAsync(const std::string& key,
                                const std::vector<Slice>& slices,
                                const Replica::Descriptor& replica) {
    if (!(hot_cache_ && replica.is_memory_replica())) {
        return;
    }

    // Skip local data unless hot cache is in shm mode (shared with dummy
    // clients who need local data cached for zero-copy access).
    if (!hot_cache_->IsShm() && IsReplicaOnLocalMemory(replica)) {
        return;
    }

    // Identify TE transfer slices (non-local) and submit async put tasks
    for (size_t i = 0; i < slices.size(); ++i) {
        if (!hot_cache_handler_->SubmitPutTask(key, slices[i])) {
            LOG(ERROR) << "Failed to submit hot cache put task for key=" << key
                       << " slice_idx=" << i;
            return;
        }
    }
}

bool Client::IsReplicaOnLocalMemory(const Replica::Descriptor& replica) {
    if (!replica.is_memory_replica()) {
        return false;
    }
    const auto replica_transfer_endpoint =
        replica.get_memory_descriptor().buffer_descriptor.transport_endpoint_;
    if (metadata_connstring_ == P2PHANDSHAKE) {
        return replica_transfer_endpoint == GetTransportEndpoint();
    }
    return local_hostname_ == replica_transfer_endpoint;
}

}  // namespace mooncake
