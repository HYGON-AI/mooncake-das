// Copyright (c) 2026 Hygon Information Technology Co., Ltd.
// SPDX-License-Identifier: Apache-2.0
// Modified by Hygon Information Technology Co., Ltd., 2026.

#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <numa.h>
#include <numaif.h>
#include <pthread.h>
#include <signal.h>
#include <thread>
#include <stop_token>

#include <dlfcn.h>  // for dlsym (Python detection)
#include <cstdlib>  // for atexit
#include <algorithm>
#include <cerrno>
#include <cctype>
#include <charconv>
#include <cstring>
#include <functional>
#include <limits>
#include <optional>
#include <vector>

#include <ylt/reflection/user_reflect_macro.hpp>
#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>

#include "real_client.h"
#include "scatter_utils.h"
#include "registered_pinned_memory.h"
#include "client_buffer.h"
#include "replica_selection.h"
#include "common.h"
#include "config.h"
#include "store_rpc_client_io_context.h"
#include "bool_parser.h"
#include "environ.h"
#include "integer_parser.h"
#include "mutex.h"
#include "types.h"
#include "utils.h"
#include "rpc_types.h"
#include "file_storage.h"
#include "environment_variables.h"
#include "device/accelerator_registry.h"
#include "default_config.h"
#include "runtime_log_config.h"
#include "uds_transport.h"
#include "device/cuda_ipc_buffer.h"
#include "shm_helper.h"
#include "memory_location.h"
#ifdef USE_NOF
#include "spdk/spdk_wrapper.h"
#endif
#ifdef USE_ASCEND_DIRECT
#include "acl/acl_rt.h"
#include "transport/ascend_transport/ascend_direct_transport/context_manager.h"
#endif
#ifdef USE_CUDA
#include <cuda_runtime.h>
#endif
#ifdef USE_INTRA_NVLINK
#include "gpu_vendor/intra_nvlink.h"
#endif
#if defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
#include "ascend_allocator.h"
#endif

DEFINE_bool(enable_http_server, false,
            "Enable embedded HTTP server for health check and metrics.");
DEFINE_int32(http_port, 9300,
             "Port for client HTTP server "
             "(only effective when --enable_http_server=true).");

namespace mooncake {

namespace {
constexpr std::chrono::seconds kIpcRequestRecvTimeout{5};
std::atomic<uint64_t> g_dfs_read_trace_id{0};

struct HttpLoggingUpdateRequest {
    std::optional<std::string> minloglevel;
    std::optional<int> vlog_level;
    std::optional<std::string> ylt_severity;
    std::optional<bool> dfs_read_trace;
};
YLT_REFL(HttpLoggingUpdateRequest, minloglevel, vlog_level, ylt_severity,
         dfs_read_trace);

struct HttpLoggingResponse {
    bool success{true};
    std::string minloglevel;
    int vlog_level{0};
    std::string ylt_severity;
    bool dfs_read_trace{false};
};
YLT_REFL(HttpLoggingResponse, success, minloglevel, vlog_level, ylt_severity,
         dfs_read_trace);

void FillLoggingResponse(HttpLoggingResponse* response) {
    const auto config = GetRuntimeLogConfig();
    response->minloglevel = config.minloglevel;
    response->vlog_level = config.vlog_level;
    response->ylt_severity = config.ylt_severity;
    response->dfs_read_trace = config.dfs_read_trace;
}

uint64_t NextDfsReadTraceId() {
    uint64_t current = g_dfs_read_trace_id.load(std::memory_order_relaxed);
    while (true) {
        const uint64_t next =
            current == std::numeric_limits<uint64_t>::max() ? 1 : current + 1;
        if (g_dfs_read_trace_id.compare_exchange_weak(
                current, next, std::memory_order_relaxed,
                std::memory_order_relaxed)) {
            return next;
        }
    }
}

size_t DivideRoundUp(size_t value, size_t divisor) {
    return value / divisor + (value % divisor != 0);
}

size_t GetNextSegmentSize(size_t remaining,
                          const std::optional<size_t> &split_limit,
                          size_t alignment) {
    if (!split_limit.has_value()) return remaining;
    const size_t aligned_limit = (*split_limit / alignment) * alignment;
    if (aligned_limit == 0) return 0;
    const size_t segment_count = DivideRoundUp(remaining, aligned_limit);
    const size_t balanced_size = DivideRoundUp(remaining, segment_count);
    return std::min(remaining,
                    DivideRoundUp(balanced_size, alignment) * alignment);
}

std::string DirectSourceForReplica(const Replica::Descriptor &replica) {
    if (replica.is_memory_replica() || replica.is_nof_replica()) {
        return DirectStorageMetricSource(ReplicaType::MEMORY);
    }
    if (replica.is_disk_replica() || replica.is_dfs_replica()) {
        // Legacy DISK and descriptor-based DFS both represent the shared
        // distributed filesystem tier for direct-storage metrics.
        return DirectStorageMetricSource(ReplicaType::DFS);
    }
    return "unknown";
}

uint64_t SumSliceBytes(
    const std::unordered_map<std::string, std::vector<Slice>> &objects) {
    uint64_t total = 0;
    for (const auto &[_, slices] : objects) {
        for (const auto &slice : slices) {
            total += slice.size;
        }
    }
    return total;
}

bool IsHostStoreSegmentProtocol(const std::string &protocol) {
    return protocol.empty() || protocol == "tcp" || protocol == "rdma" ||
           protocol == "efa" || protocol == "cxi" || protocol == "rpc_only";
}

#ifdef USE_ASCEND_DIRECT
// Split standalone store capacity across NPUs: cap each mount at total/n.
size_t AgentModeStoreChunkCap(size_t total_size) {
    const uint32_t n = ContextManager::getInstance().getDeviceCount();
    if (n <= 1 || total_size == 0) {
        return 0;
    }
    return total_size / n;
}

bool RestoreAgentModeDeviceZero() {
    if (!ContextManager::getInstance().setCurrentContext(0)) {
        LOG(ERROR) << "Failed to restore current context for device 0";
        return false;
    }
    return true;
}
#endif

std::shared_ptr<RegisteredPinnedRegion> TryPinStoreSegment(
    void *ptr, size_t size, const std::string &protocol,
    const char *segment_owner) {
    if (!IsHostStoreSegmentProtocol(protocol)) return nullptr;
    return RegisteredPinnedMemoryManager::instance().try_pin(
        ptr, size,
        std::string("Store segment ") + segment_owner +
            " protocol=" + protocol);
}

bool ReleasePinnedRegionForFree(
    std::shared_ptr<RegisteredPinnedRegion> &pinned_region,
    const char *segment_owner) {
    if (!pinned_region) return true;
    const bool safe_to_free = pinned_region->release();
    pinned_region.reset();
    if (!safe_to_free) {
        LOG(ERROR) << "Leaking " << segment_owner
                   << " backing memory because cudaHostUnregister failed";
    }
    return safe_to_free;
}

bool ReleasePinnedRegionsForFree(
    std::vector<std::shared_ptr<RegisteredPinnedRegion>> &pinned_regions,
    const char *segment_owner) {
    bool safe_to_free = true;
    for (auto &pinned_region : pinned_regions) {
        safe_to_free &=
            ReleasePinnedRegionForFree(pinned_region, segment_owner);
    }
    pinned_regions.clear();
    return safe_to_free;
}

template <typename T>
std::vector<int> ToPyResults(
    const std::vector<tl::expected<T, ErrorCode>> &results) {
    std::vector<int> converted;
    converted.reserve(results.size());
    for (const auto &result : results) converted.push_back(to_py_ret(result));
    return converted;
}

tl::expected<std::vector<std::vector<Slice>>, ErrorCode>
BuildNestedSlicesFromBuffers(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &all_sizes) {
    if (keys.size() != all_buffers.size() ||
        all_buffers.size() != all_sizes.size()) {
        LOG(ERROR) << "Mismatched sizes for keys, buffers, and sizes";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::vector<std::vector<Slice>> batched_slices(keys.size());
    for (size_t i = 0; i < all_buffers.size(); ++i) {
        const auto &buffers = all_buffers[i];
        const auto &sizes = all_sizes[i];
        if (buffers.size() != sizes.size()) {
            LOG(ERROR) << "Mismatched buffers and sizes of key:" << keys[i];
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        batched_slices[i].reserve(buffers.size());
        for (size_t j = 0; j < buffers.size(); ++j) {
            batched_slices[i].emplace_back(Slice{buffers[j], sizes[j]});
        }
    }
    return batched_slices;
}

tl::expected<std::vector<Slice>, ErrorCode> StageWriteSlices(
    const std::shared_ptr<ClientBufferAllocator> &allocator,
    const std::vector<Slice> &slices,
    std::vector<BufferHandle> &staging_handles) {
    if (!allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    size_t total_size = 0;
    for (const auto &slice : slices) {
        if (slice.size > std::numeric_limits<size_t>::max() - total_size) {
            return tl::unexpected(ErrorCode::BUFFER_OVERFLOW);
        }
        total_size += slice.size;
    }
    auto allocation = allocator->allocate(total_size);
    if (!allocation) {
        return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    auto runtime_accelerator =
        device::GetAcceleratorRegistry().RuntimeAccelerators();
    char *destination = static_cast<char *>(allocation->ptr());
    std::vector<Slice> staged_slices;
    staged_slices.reserve(slices.size());
    size_t offset = 0;
    for (const auto &slice : slices) {
        if (!runtime_accelerator.CopyToHost(destination + offset, slice.ptr,
                                            slice.size)) {
            LOG(ERROR) << "Failed to stage non-local write buffer";
            return tl::unexpected(ErrorCode::TRANSFER_FAIL);
        }
        staged_slices.emplace_back(destination + offset, slice.size);
        offset += slice.size;
    }
    staging_handles.emplace_back(std::move(*allocation));
    return staged_slices;
}

using BatchWriteMethod = std::vector<tl::expected<void, ErrorCode>> (Client::*)(
    const std::vector<ObjectKey> &, std::vector<std::vector<Slice>> &,
    const ReplicateConfig &, const Client::WriteBufferStager &);

std::vector<tl::expected<void, ErrorCode>> BatchWriteFromMultiBuffers(
    const std::shared_ptr<Client> &client, const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &all_sizes,
    const ReplicateConfig &config,
    const std::shared_ptr<ClientBufferAllocator> &allocator,
    bool stage_nonlocal, BatchWriteMethod write) {
    if (!client) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    auto batched_slices =
        BuildNestedSlicesFromBuffers(keys, all_buffers, all_sizes);
    if (!batched_slices) {
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(batched_slices.error()));
    }

    std::vector<BufferHandle> staging_handles;
    staging_handles.reserve(stage_nonlocal ? keys.size() : 0);
    Client::WriteBufferStager stager;
    if (stage_nonlocal) {
        stager = [&](const std::vector<Slice> &slices) {
            return StageWriteSlices(allocator, slices, staging_handles);
        };
    }
    return ((*client).*write)(keys, batched_slices.value(), config, stager);
}

#ifdef USE_ASCEND_DIRECT
bool checkAcl(aclError result, const char *message) {
    if (result != ACL_ERROR_NONE) {
        const char *errMsg = aclGetRecentErrMsg();
        LOG(ERROR) << message << " (Error code: " << result << " - " << errMsg
                   << ")";
        return false;
    }
    return true;
}

tl::expected<void, ErrorCode> set_context_if_needed(const std::string &protocol,
                                                    int32_t physical_device_id,
                                                    const char *action) {
    if (protocol != "ascend" || !globalConfig().ascend_agent_mode) {
        return {};
    }
    if (physical_device_id == kInvalidPhysicalDeviceId) {
        LOG(ERROR) << "Missing physical device id for " << action;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!ContextManager::getInstance().setCurrentContextByPhysicalId(
            physical_device_id)) {
        LOG(ERROR) << "Failed to set current context for physical device "
                   << physical_device_id << " during " << action;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}
#endif

size_t sum_value_sizes(const std::vector<std::span<const char>> &values) {
    size_t total = 0;
    for (const auto &value : values) {
        total += value.size_bytes();
    }
    return total;
}

size_t sum_sizes(const std::vector<size_t> &sizes) {
    size_t total = 0;
    for (size_t size : sizes) {
        total += size;
    }
    return total;
}

size_t sum_successful_sizes(const std::vector<int> &results,
                            const std::vector<size_t> &sizes) {
    size_t total = 0;
    for (size_t i = 0; i < results.size() && i < sizes.size(); ++i) {
        if (results[i] == 0) {
            total += sizes[i];
        }
    }
    return total;
}

size_t sum_successful_nested_sizes(
    const std::vector<int> &results,
    const std::vector<std::vector<size_t>> &nested_sizes) {
    size_t total = 0;
    for (size_t i = 0; i < results.size() && i < nested_sizes.size(); ++i) {
        if (results[i] == 0) {
            total += sum_sizes(nested_sizes[i]);
        }
    }
    return total;
}

size_t sum_positive_results(const std::vector<int64_t> &results) {
    size_t total = 0;
    for (int64_t result : results) {
        if (result > 0) {
            total += static_cast<size_t>(result);
        }
    }
    return total;
}

size_t sum_positive_results(const std::vector<int> &results) {
    size_t total = 0;
    for (int result : results) {
        if (result > 0) {
            total += static_cast<size_t>(result);
        }
    }
    return total;
}

size_t sum_positive_ranges(
    const std::vector<std::vector<std::vector<int64_t>>> &results) {
    size_t total = 0;
    for (const auto &key_rows : results) {
        for (const auto &row : key_rows) {
            total += sum_positive_results(row);
        }
    }
    return total;
}

size_t sum_buffer_handle_sizes(
    const std::vector<std::shared_ptr<BufferHandle>> &buffers) {
    size_t total = 0;
    for (const auto &buffer : buffers) {
        if (buffer != nullptr) {
            total += buffer->size();
        }
    }
    return total;
}

ErrorCode scatter_transfer_error(const Status &status) {
    return status.IsInvalidArgument() ? ErrorCode::INVALID_PARAMS
                                      : ErrorCode::TRANSFER_FAIL;
}

// SelectBestReplica and the replica-scoring helpers live in
// replica_selection.h (included above) so they can be unit-tested directly.
using mooncake::SelectBestReplica;

// Build a QueryResult containing only the chosen replica so that
// Client::Get / Client::BatchGet (which internally call
// FindFirstCompleteReplica) cannot pick a different replica type.
inline QueryResult FilterQueryResult(const QueryResult &qr,
                                     const Replica::Descriptor &replica,
                                     bool include_object_checksum = true) {
    return QueryResult(
        {replica}, qr.lease_timeout,
        include_object_checksum ? qr.object_checksum : std::nullopt);
}

inline bool SameAllocatedBufferDescriptor(
    const AllocatedBuffer::Descriptor &lhs,
    const AllocatedBuffer::Descriptor &rhs) {
    return lhs.size_ == rhs.size_ &&
           lhs.buffer_address_ == rhs.buffer_address_ &&
           lhs.protocol_ == rhs.protocol_ &&
           lhs.transport_endpoint_ == rhs.transport_endpoint_;
}

// Session-cached bytes can survive a lease refresh only when the fresh query
// still identifies the exact same replica generation and storage location.
// The object key alone is not an immutable-content contract.
inline bool SameReplicaForSessionCache(const Replica::Descriptor &lhs,
                                      const Replica::Descriptor &rhs) {
    if (lhs.id != rhs.id || lhs.status != rhs.status) return false;
    if (lhs.is_memory_replica() && rhs.is_memory_replica()) {
        return SameAllocatedBufferDescriptor(
            lhs.get_memory_descriptor().buffer_descriptor,
            rhs.get_memory_descriptor().buffer_descriptor);
    }
    if (lhs.is_nof_replica() && rhs.is_nof_replica()) {
        return SameAllocatedBufferDescriptor(
            lhs.get_nof_descriptor().buffer_descriptor,
            rhs.get_nof_descriptor().buffer_descriptor);
    }
    if (lhs.is_disk_replica() && rhs.is_disk_replica()) {
        const auto &left = lhs.get_disk_descriptor();
        const auto &right = rhs.get_disk_descriptor();
        return left.file_path == right.file_path &&
               left.object_size == right.object_size;
    }
    if (lhs.is_dfs_replica() && rhs.is_dfs_replica()) {
        const auto &left = lhs.get_dfs_descriptor();
        const auto &right = rhs.get_dfs_descriptor();
        return left.file_path == right.file_path &&
               left.offset == right.offset &&
               left.object_size == right.object_size &&
               left.aligned_size == right.aligned_size &&
               left.shard_idx == right.shard_idx;
    }
    return false;
}

inline bool CompatibleSessionCacheRefresh(const QueryResult &old_session,
                                          const QueryResult &fresh_session) {
    if (old_session.replicas.size() != 1 ||
        fresh_session.replicas.size() != 1 ||
        !SameReplicaForSessionCache(old_session.replicas.front(),
                                    fresh_session.replicas.front())) {
        return false;
    }
    return !old_session.object_checksum || !fresh_session.object_checksum ||
           *old_session.object_checksum == *fresh_session.object_checksum;
}

inline bool SameGetSessionSnapshot(const QueryResult &lhs,
                                   const QueryResult &rhs) {
    if (lhs.lease_timeout != rhs.lease_timeout ||
        lhs.object_checksum != rhs.object_checksum ||
        lhs.replicas.size() != rhs.replicas.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.replicas.size(); ++i) {
        if (!SameReplicaForSessionCache(lhs.replicas[i], rhs.replicas[i])) {
            return false;
        }
    }
    return true;
}

// Shared object-byte range overflow check (same semantics as
// execute_ranged_read / session ranged get-put).
inline bool is_object_range_overflow(size_t offset, size_t size, size_t limit) {
    return size > limit || offset > limit - size;
}

inline const Replica::Descriptor *SelectCompleteMemoryReplica(
    const std::vector<Replica::Descriptor> &replicas,
    const std::unordered_set<std::string> &local_endpoints) {
    const Replica::Descriptor *first_memory = nullptr;
    for (const auto &r : replicas) {
        if (r.status != ReplicaStatus::COMPLETE || !r.is_memory_replica()) {
            continue;
        }
        if (local_endpoints.count(r.get_memory_descriptor()
                                      .buffer_descriptor.transport_endpoint_)) {
            return &r;
        }
        if (!first_memory) {
            first_memory = &r;
        }
    }
    return first_memory;
}

inline const Replica::Descriptor *SelectSessionReplica(
    const std::vector<Replica::Descriptor> &replicas,
    const std::unordered_set<std::string> &local_endpoints, bool allow_dfs) {
    if (const auto *memory =
            SelectCompleteMemoryReplica(replicas, local_endpoints)) {
        return memory;
    }

    if (!allow_dfs) return nullptr;
    for (const auto &replica : replicas) {
        if (replica.status == ReplicaStatus::COMPLETE &&
            replica.is_dfs_replica()) {
            return &replica;
        }
    }
    return nullptr;
}

tl::expected<QueryResult, ErrorCode> ApplyDfsReadVisibility(
    const tl::expected<QueryResult, ErrorCode> &query_result,
    bool allow_dfs) {
    if (!query_result) {
        return tl::make_unexpected(query_result.error());
    }
    if (allow_dfs) return query_result.value();

    std::vector<Replica::Descriptor> memory_replicas;
    for (const auto &replica : query_result->replicas) {
        if (replica.status == ReplicaStatus::COMPLETE &&
            replica.is_memory_replica()) {
            memory_replicas.push_back(replica);
        }
    }
    if (memory_replicas.empty()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    return QueryResult(std::move(memory_replicas), query_result->lease_timeout,
                       query_result->object_checksum);
}

inline bool HasMemoryReplica(const std::vector<Replica::Descriptor> &replicas) {
    for (const auto &r : replicas) {
        if (r.is_memory_replica()) {
            return true;
        }
    }
    return false;
}

size_t DfsH2dStreamCount() {
    constexpr size_t kDefaultStreams = 4;
    const char *value = std::getenv("MC_STORE_DFS_H2D_STREAMS");
    if (!value || value[0] == '\0') return kDefaultStreams;

    errno = 0;
    char *end = nullptr;
    const unsigned long long parsed = std::strtoull(value, &end, 10);
    const bool digits_only =
        std::all_of(value, value + std::strlen(value), [](unsigned char c) {
            return std::isdigit(c) != 0;
        });
    if (!digits_only || errno != 0 || end == value || *end != '\0' ||
        (parsed != 1 && parsed != 2 && parsed != 4)) {
        LOG(WARNING) << "Invalid MC_STORE_DFS_H2D_STREAMS='" << value
                     << "', using " << kDefaultStreams;
        return kDefaultStreams;
    }
    return static_cast<size_t>(parsed);
}

bool AlignUp(size_t value, size_t alignment, size_t *aligned) {
    const size_t remainder = value % alignment;
    if (remainder == 0) {
        *aligned = value;
        return true;
    }
    const size_t padding = alignment - remainder;
    if (value > std::numeric_limits<size_t>::max() - padding) return false;
    *aligned = value + padding;
    return true;
}

// Removing a prefetched buffer can return the last reference to its arena
// region. Keep the shared handle alive until the caller has dropped
// session_mutex_ so region release and its accounting run outside the lock.
void DetachPrefetchedSessionBuffer(
    std::unordered_map<std::string, PrefetchedSessionBuffer> &cache,
    const std::string &key,
    std::vector<std::shared_ptr<BufferHandle>> &detached_buffers) {
    auto it = cache.find(key);
    if (it == cache.end()) return;
    detached_buffers.push_back(std::move(it->second.buffer_handle));
    cache.erase(it);
}

}  // namespace

class RealClient::DfsH2dStreamPool {
   private:
    struct DeviceKey {
        const device::AcceleratorDevice *device = nullptr;
        int32_t device_id = -1;

        bool operator<(const DeviceKey &other) const {
            if (device != other.device) {
                return std::less<const device::AcceleratorDevice *>{}(
                    device, other.device);
            }
            return device_id < other.device_id;
        }
    };

    struct DeviceState {
        struct SyncWorker {
            std::mutex mutex;
            std::condition_variable task_cv;
            std::condition_variable completion_cv;
            std::thread thread;
            bool has_task = false;
            bool stopping = false;
            bool ready = false;
            bool result = false;
        };

        DeviceState(const device::AcceleratorDevice *device, int32_t device_id)
            : device(device), device_id(device_id) {}

        ~DeviceState() { StopSyncWorkers(); }

        bool StartSyncWorkers() {
            try {
                sync_workers.reserve(streams.size());
                for (void *stream : streams) {
                    auto worker = std::make_unique<SyncWorker>();
                    SyncWorker *worker_ptr = worker.get();
                    worker->thread = std::thread([this, worker_ptr, stream] {
                        device->SetContext(device_id);
                        {
                            std::lock_guard<std::mutex> lock(
                                worker_ptr->mutex);
                            worker_ptr->ready = true;
                        }
                        worker_ptr->completion_cv.notify_one();
                        while (true) {
                            std::unique_lock<std::mutex> lock(
                                worker_ptr->mutex);
                            worker_ptr->task_cv.wait(lock, [worker_ptr] {
                                return worker_ptr->has_task ||
                                       worker_ptr->stopping;
                            });
                            if (worker_ptr->stopping &&
                                !worker_ptr->has_task) {
                                break;
                            }
                            lock.unlock();
                            bool result = false;
                            try {
                                result = device->SynchronizeStream(stream);
                            } catch (const std::exception &error) {
                                LOG(ERROR)
                                    << "Persistent DFS H2D sync worker failed: "
                                    << error.what();
                            } catch (...) {
                                LOG(ERROR) << "Persistent DFS H2D sync worker "
                                              "failed";
                            }
                            lock.lock();
                            worker_ptr->result = result;
                            worker_ptr->has_task = false;
                            lock.unlock();
                            worker_ptr->completion_cv.notify_one();
                        }
                    });
                    sync_workers.push_back(std::move(worker));
                    std::unique_lock<std::mutex> lock(worker_ptr->mutex);
                    worker_ptr->completion_cv.wait(
                        lock, [worker_ptr] { return worker_ptr->ready; });
                }
            } catch (const std::exception &error) {
                LOG(ERROR) << "Failed to create persistent DFS H2D sync "
                              "worker: "
                           << error.what();
                StopSyncWorkers();
                return false;
            } catch (...) {
                LOG(ERROR) << "Failed to create persistent DFS H2D sync "
                              "worker";
                StopSyncWorkers();
                return false;
            }
            return true;
        }

        bool SynchronizeStreams(const std::vector<uint8_t> &used_streams,
                                std::vector<uint8_t> *results) {
            if (!results || used_streams.size() != sync_workers.size()) {
                return false;
            }
            results->assign(sync_workers.size(), 1);
            for (size_t i = 0; i < sync_workers.size(); ++i) {
                if (!used_streams[i]) continue;
                auto &worker = *sync_workers[i];
                std::lock_guard<std::mutex> lock(worker.mutex);
                if (worker.stopping || worker.has_task) return false;
            }
            for (size_t i = 0; i < sync_workers.size(); ++i) {
                if (!used_streams[i]) continue;
                auto &worker = *sync_workers[i];
                std::lock_guard<std::mutex> lock(worker.mutex);
                worker.has_task = true;
            }
            for (size_t i = 0; i < sync_workers.size(); ++i) {
                if (used_streams[i]) sync_workers[i]->task_cv.notify_one();
            }
            for (size_t i = 0; i < sync_workers.size(); ++i) {
                if (!used_streams[i]) continue;
                auto &worker = *sync_workers[i];
                std::unique_lock<std::mutex> lock(worker.mutex);
                worker.completion_cv.wait(
                    lock, [&worker] { return !worker.has_task; });
                (*results)[i] = worker.result;
            }
            return true;
        }

        void StopSyncWorkers() {
            for (auto &worker : sync_workers) {
                {
                    std::lock_guard<std::mutex> lock(worker->mutex);
                    worker->stopping = true;
                }
                worker->task_cv.notify_one();
            }
            for (auto &worker : sync_workers) {
                if (worker->thread.joinable()) worker->thread.join();
            }
            sync_workers.clear();
        }

        const device::AcceleratorDevice *device;
        int32_t device_id;
        std::mutex mutex;
        std::vector<void *> streams;
        std::vector<std::unique_ptr<SyncWorker>> sync_workers;
        bool initialized = false;
    };

   public:
    class Lease {
       public:
        Lease(std::shared_ptr<DeviceState> state,
              std::unique_lock<std::mutex> lock)
            : state_(std::move(state)), lock_(std::move(lock)) {}

        const device::AcceleratorDevice *device() const {
            return state_->device;
        }
        int32_t device_id() const { return state_->device_id; }
        const std::vector<void *> &streams() const { return state_->streams; }
        bool SynchronizeStreams(const std::vector<uint8_t> &used_streams,
                                std::vector<uint8_t> *results) {
            return state_->SynchronizeStreams(used_streams, results);
        }

       private:
        std::shared_ptr<DeviceState> state_;
        std::unique_lock<std::mutex> lock_;
    };

    explicit DfsH2dStreamPool(size_t streams_per_device)
        : streams_per_device_(streams_per_device) {}

    ~DfsH2dStreamPool() {
        if (!Shutdown()) {
            LOG(ERROR)
                << "Failed to synchronize a DFS H2D stream during shutdown";
        }
    }

    std::unique_ptr<Lease> Acquire(
        const device::AcceleratorDevice *accelerator, int32_t device_id) {
        std::shared_ptr<DeviceState> state;
        std::unique_lock<std::mutex> lock(mutex_);
        if (shutting_down_) return nullptr;
        DeviceKey key{accelerator, device_id};
        auto [it, inserted] = states_.try_emplace(key);
        if (inserted) {
            it->second =
                std::make_shared<DeviceState>(accelerator, device_id);
        }
        state = it->second;

        // Keep the map lock until this lease owns the device state. Shutdown
        // can then swap and destroy states only after every lease is returned.
        std::unique_lock<std::mutex> state_lock(state->mutex);
        lock.unlock();
        state->device->SetContext(state->device_id);
        if (!state->initialized) {
            state->streams.reserve(streams_per_device_);
            for (size_t i = 0; i < streams_per_device_; ++i) {
                void *stream = nullptr;
                if (!state->device->CreateStream(&stream)) break;
                state->streams.push_back(stream);
            }
            if (!state->StartSyncWorkers()) {
                for (void *stream : state->streams) {
                    state->device->DestroyStream(stream);
                }
                state->streams.clear();
                return nullptr;
            }
            state->initialized = true;
        }
        return std::make_unique<Lease>(state, std::move(state_lock));
    }

    bool Shutdown() {
        std::map<DeviceKey, std::shared_ptr<DeviceState>> states;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (shutting_down_) {
                shutdown_cv_.wait(lock,
                                  [this] { return shutdown_complete_; });
                return shutdown_succeeded_;
            }
            shutting_down_ = true;
            states.swap(states_);
        }

        bool succeeded = true;
        for (auto &[_, state] : states) {
            std::lock_guard<std::mutex> state_lock(state->mutex);
            std::vector<uint8_t> results;
            const std::vector<uint8_t> used_streams(state->streams.size(), 1);
            if (!state->SynchronizeStreams(used_streams, &results)) {
                succeeded = false;
            } else {
                for (uint8_t result : results) succeeded &= result != 0;
            }
            state->StopSyncWorkers();
            state->device->SetContext(state->device_id);
            for (void *stream : state->streams) {
                state->device->DestroyStream(stream);
            }
            state->streams.clear();
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            shutdown_succeeded_ = succeeded;
            shutdown_complete_ = true;
        }
        shutdown_cv_.notify_all();
        return succeeded;
    }

   private:
    const size_t streams_per_device_;
    std::mutex mutex_;
    std::condition_variable shutdown_cv_;
    std::map<DeviceKey, std::shared_ptr<DeviceState>> states_;
    bool shutting_down_ = false;
    bool shutdown_complete_ = false;
    bool shutdown_succeeded_ = true;
};

class RealClient::DfsAsyncScatterContext {
   private:
    struct Target {
        const device::AcceleratorDevice *device = nullptr;
        int32_t device_id = -1;

        bool operator==(const Target &other) const {
            return device == other.device && device_id == other.device_id;
        }
        bool operator<(const Target &other) const {
            if (device != other.device) {
                return std::less<const device::AcceleratorDevice *>{}(
                    device, other.device);
            }
            return device_id < other.device_id;
        }
    };

    struct CachedRegion {
        uintptr_t base = 0;
        size_t size = 0;
        Target target;
    };

    struct CopyOperation {
        void *dst = nullptr;
        const void *src = nullptr;
        const void *src_device = nullptr;
        size_t size = 0;
        Target target;
        size_t first_range = 0;
        size_t range_count = 0;
    };

    struct ActiveDevice {
        std::unique_ptr<DfsH2dStreamPool::Lease> lease;
        std::vector<std::vector<size_t>> operations_by_stream;
        std::vector<uint8_t> used_streams;
    };

   public:
    explicit DfsAsyncScatterContext(RealClient &owner,
                                    DfsH2dStreamPool &stream_pool,
                                    bool collect_metrics)
        : owner_(owner),
          stream_pool_(stream_pool),
          runtime_(device::GetAcceleratorRegistry().RuntimeAccelerators()),
          collect_metrics_(collect_metrics) {}

    ~DfsAsyncScatterContext() {
        if (!submitted_ || synchronized_) return;
        for (auto &active : active_devices_) {
            std::vector<uint8_t> stream_results;
            if (!active.lease->SynchronizeStreams(active.used_streams,
                                                  &stream_results)) {
                LOG(ERROR) << "Failed to dispatch DFS H2D stream "
                              "synchronization during cleanup";
            }
        }
    }

    bool AddCopy(void *dst, const void *src, const void *src_device,
                 size_t size, size_t result_index) {
        ++original_range_count_;
        if (size == 0) return false;

        Target target;
        const uintptr_t address = reinterpret_cast<uintptr_t>(dst);
        bool region_resolved = false;
        auto region_it = region_cache_.upper_bound(address);
        if (region_it != region_cache_.begin()) {
            --region_it;
            if (ContainsAddress(region_it->second, address)) {
                if (collect_metrics_) ++region_cache_hits_;
                target = region_it->second.target;
                region_resolved = true;
            }
        }
        if (!region_resolved) {
            if (collect_metrics_) ++region_cache_misses_;
            const auto region = owner_.resolve_writable_buffer_region(dst);
            if (region && IsUsableRegion(*region)) {
                target = QueryTarget(dst);
                const uintptr_t base = reinterpret_cast<uintptr_t>(region->base);
                region_cache_.insert_or_assign(
                    base, CachedRegion{base, region->size, target});
            } else {
                auto target_it = target_cache_.find(dst);
                if (target_it != target_cache_.end()) {
                    if (collect_metrics_) ++pointer_cache_hits_;
                    target = target_it->second;
                } else {
                    if (collect_metrics_) ++unregistered_pointer_queries_;
                    target = QueryTarget(dst);
                    target_cache_.emplace(dst, target);
                }
            }
        }

        const size_t range_index = range_results_.size();
        range_results_.push_back(result_index);
        // Keep merges within one result entry so all of that entry's writes
        // retain their original order on a single stream.
        if (!operations_.empty() &&
            range_results_[operations_.back().first_range] == result_index &&
            CanMerge(operations_.back(), dst, src, src_device, size,
                     target)) {
            operations_.back().size += size;
            ++operations_.back().range_count;
            return target.device != nullptr;
        }
        operations_.push_back(
            CopyOperation{dst, src, src_device, size, target, range_index, 1});
        return target.device != nullptr;
    }

    bool Submit() {
        if (submitted_) return failed_results_.empty();
        submitted_ = true;

        std::map<Target, std::vector<size_t>> device_operations;
        for (size_t i = 0; i < operations_.size(); ++i) {
            const auto &operation = operations_[i];
            if (!operation.target.device) {
                if (collect_metrics_) {
                    ++host_copy_ops_;
                    host_copy_ranges_ += operation.range_count;
                }
                std::memcpy(operation.dst, operation.src, operation.size);
                continue;
            }
            if (collect_metrics_) {
                ++device_copy_ops_;
                device_copy_ranges_ += operation.range_count;
            }
            device_operations[operation.target].push_back(i);
        }

        active_devices_.reserve(device_operations.size());
        for (const auto &[target, operation_indices] : device_operations) {
            ActiveDevice active;
            active.lease =
                stream_pool_.Acquire(target.device, target.device_id);
            if (!active.lease) {
                for (size_t operation_index : operation_indices) {
                    MarkFailed(operation_index);
                }
                continue;
            }

            const auto &streams = active.lease->streams();
            if (streams.empty()) {
                for (size_t operation_index : operation_indices) {
                    const auto &operation = operations_[operation_index];
                    if (!target.device->Copy(
                            operation.dst, operation.src, operation.size,
                            device::CopyDirection::kHostToDevice)) {
                        MarkFailed(operation_index);
                    }
                }
                continue;
            }

            active.operations_by_stream.resize(streams.size());
            active.used_streams.resize(streams.size());
            active_devices_.push_back(std::move(active));
            auto &owned_active = active_devices_.back();
            size_t next_stream = 0;
            std::unordered_map<size_t, size_t> result_streams;
            for (size_t operation_index : operation_indices) {
                const auto &operation = operations_[operation_index];
                const size_t result_index =
                    range_results_[operation.first_range];
                auto [stream_it, inserted] =
                    result_streams.try_emplace(result_index);
                if (inserted) {
                    stream_it->second = next_stream++ % streams.size();
                }
                const size_t stream_index = stream_it->second;
                owned_active.used_streams[stream_index] = true;
                owned_active.operations_by_stream[stream_index].push_back(
                    operation_index);
            }

            // Submit one batch per stream. Unsupported backends use the
            // default implementation, preserving the original DMA behavior.
            for (size_t stream_index = 0; stream_index < streams.size();
                 ++stream_index) {
                auto &stream_operations =
                    owned_active.operations_by_stream[stream_index];
                if (stream_operations.empty()) continue;
                std::vector<device::HostCopyRange> ranges;
                ranges.reserve(stream_operations.size());
                for (size_t operation_index : stream_operations) {
                    const auto &operation = operations_[operation_index];
                    ranges.push_back(device::HostCopyRange{
                        operation.dst, operation.src, operation.size,
                        operation.src_device});
                }
                bool used_h2d_kernel = false;
                if (target.device->CopyFromHostBatchAsync(
                        ranges, streams[stream_index], &used_h2d_kernel)) {
                    if (collect_metrics_ && used_h2d_kernel) {
                        ++h2d_kernel_batches_;
                    }
                    continue;
                }
                for (size_t operation_index : stream_operations) {
                    MarkFailed(operation_index);
                }
            }
        }
        return failed_results_.empty();
    }

    bool Synchronize() {
        if (synchronized_) return failed_results_.empty();
        if (!submitted_) Submit();

        for (auto &active : active_devices_) {
            std::vector<uint8_t> stream_results;
            if (!active.lease->SynchronizeStreams(active.used_streams,
                                                  &stream_results)) {
                for (size_t i = 0; i < active.used_streams.size(); ++i) {
                    if (!active.used_streams[i]) continue;
                    for (size_t operation_index :
                         active.operations_by_stream[i]) {
                        MarkFailed(operation_index);
                    }
                }
                continue;
            }
            for (size_t i = 0; i < stream_results.size(); ++i) {
                if (!active.used_streams[i] || stream_results[i]) continue;
                for (size_t operation_index :
                     active.operations_by_stream[i]) {
                    MarkFailed(operation_index);
                }
            }
        }
        active_devices_.clear();
        synchronized_ = true;
        return failed_results_.empty();
    }

    bool EntryFailed(size_t result_index) const {
        return failed_results_.find(result_index) != failed_results_.end();
    }

    size_t original_range_count() const { return original_range_count_; }
    size_t merged_range_count() const { return operations_.size(); }
    size_t pointer_query_count() const { return pointer_query_count_; }
    size_t pointer_cache_hits() const { return pointer_cache_hits_; }
    uint64_t pointer_query_us() const {
        return static_cast<uint64_t>(std::chrono::duration_cast<
                                         std::chrono::microseconds>(
                                         pointer_query_duration_)
                                         .count());
    }
    size_t region_cache_hits() const { return region_cache_hits_; }
    size_t region_cache_misses() const { return region_cache_misses_; }
    size_t unregistered_pointer_queries() const {
        return unregistered_pointer_queries_;
    }
    size_t host_copy_ops() const { return host_copy_ops_; }
    size_t device_copy_ops() const { return device_copy_ops_; }
    size_t host_copy_ranges() const { return host_copy_ranges_; }
    size_t device_copy_ranges() const { return device_copy_ranges_; }
    size_t h2d_kernel_batches() const { return h2d_kernel_batches_; }

   private:
    static bool IsUsableRegion(const RealClient::WritableBufferRegion &region) {
        return region.base != nullptr && region.size != 0;
    }

    static bool ContainsAddress(const CachedRegion &region, uintptr_t address) {
        if (region.size == 0 || address < region.base) return false;
        const uintptr_t offset = address - region.base;
        if (sizeof(size_t) > sizeof(uintptr_t) &&
            region.size > static_cast<size_t>(
                              std::numeric_limits<uintptr_t>::max())) {
            return true;
        }
        return offset < static_cast<uintptr_t>(region.size);
    }

    Target QueryTarget(void *dst) {
        Target target;
        device::PointerInfo info{};
        if (collect_metrics_) {
            const auto query_start = std::chrono::steady_clock::now();
            target.device = runtime_.FindDeviceForPointer(dst, &info);
            pointer_query_duration_ +=
                std::chrono::steady_clock::now() - query_start;
            ++pointer_query_count_;
        } else {
            target.device = runtime_.FindDeviceForPointer(dst, &info);
        }
        if (target.device) target.device_id = info.device_id;
        return target;
    }

    static bool AreContiguous(const void *begin, size_t size,
                              const void *next) {
        const uintptr_t address = reinterpret_cast<uintptr_t>(begin);
        return address <= std::numeric_limits<uintptr_t>::max() - size &&
               address + size == reinterpret_cast<uintptr_t>(next);
    }

    static bool CanMerge(const CopyOperation &previous, const void *dst,
                         const void *src, const void *src_device, size_t size,
                         const Target &target) {
        const bool source_aliases_match =
            (previous.src_device == nullptr && src_device == nullptr) ||
            (previous.src_device != nullptr && src_device != nullptr &&
             AreContiguous(previous.src_device, previous.size, src_device));
        return previous.target == target && source_aliases_match &&
               previous.size <=
                   std::numeric_limits<size_t>::max() - size &&
               AreContiguous(previous.src, previous.size, src) &&
               AreContiguous(previous.dst, previous.size, dst);
    }

    void MarkFailed(size_t operation_index) {
        const auto &operation = operations_[operation_index];
        for (size_t i = 0; i < operation.range_count; ++i) {
            failed_results_.insert(
                range_results_[operation.first_range + i]);
        }
    }

    RealClient &owner_;
    DfsH2dStreamPool &stream_pool_;
    device::RuntimeAccelerator runtime_;
    bool collect_metrics_ = false;
    std::map<uintptr_t, CachedRegion> region_cache_;
    std::unordered_map<void *, Target> target_cache_;
    std::vector<size_t> range_results_;
    std::vector<CopyOperation> operations_;
    std::vector<ActiveDevice> active_devices_;
    std::unordered_set<size_t> failed_results_;
    size_t original_range_count_ = 0;
    bool submitted_ = false;
    bool synchronized_ = false;
    size_t pointer_query_count_ = 0;
    size_t pointer_cache_hits_ = 0;
    std::chrono::steady_clock::duration pointer_query_duration_{};
    size_t region_cache_hits_ = 0;
    size_t region_cache_misses_ = 0;
    size_t unregistered_pointer_queries_ = 0;
    size_t host_copy_ops_ = 0;
    size_t device_copy_ops_ = 0;
    size_t host_copy_ranges_ = 0;
    size_t device_copy_ranges_ = 0;
    size_t h2d_kernel_batches_ = 0;
};

PyClient::~PyClient() {}

bool RealClient::map_dummy_range_in_shm(const MappedShm &shm,
                                        uint64_t dummy_addr, size_t offset,
                                        size_t size, void *&out_real) const {
    if (dummy_addr < shm.dummy_base_addr) {
        return false;
    }
    const uint64_t base_offset = dummy_addr - shm.dummy_base_addr;
    if (base_offset > shm.shm_size || offset > shm.shm_size - base_offset) {
        return false;
    }
    const size_t write_offset = static_cast<size_t>(base_offset) + offset;
    if (write_offset < base_offset || write_offset > shm.shm_size ||
        size > shm.shm_size - write_offset) {
        return false;
    }
    out_real = reinterpret_cast<void *>(dummy_addr + shm.shm_addr_offset +
                                        static_cast<uint64_t>(offset));
    return true;
}

bool RealClient::map_dummy_buffer_to_real(const ShmContext &shm_ctx,
                                          uint64_t dummy_addr, size_t buf_size,
                                          const MappedShm *&last_hit_shm,
                                          void *&out_real,
                                          size_t *out_capacity) const {
    if (last_hit_shm && map_dummy_range_in_shm(*last_hit_shm, dummy_addr, 0,
                                               buf_size, out_real)) {
        if (out_capacity) {
            *out_capacity = last_hit_shm->shm_size -
                            (dummy_addr - last_hit_shm->dummy_base_addr);
        }
        return true;
    }
    for (const auto &shm : shm_ctx.mapped_shms) {
        if (map_dummy_range_in_shm(shm, dummy_addr, 0, buf_size, out_real)) {
            last_hit_shm = &shm;
            if (out_capacity) {
                *out_capacity =
                    shm.shm_size - (dummy_addr - shm.dummy_base_addr);
            }
            return true;
        }
    }
    return false;
}

bool RealClient::map_dummy_buffer_range_to_real(const ShmContext &shm_ctx,
                                                uint64_t dummy_addr,
                                                size_t dst_offset, size_t size,
                                                void *&out_real) const {
    for (const auto &shm : shm_ctx.mapped_shms) {
        if (map_dummy_range_in_shm(shm, dummy_addr, dst_offset, size,
                                   out_real)) {
            return true;
        }
    }
    return false;
}

tl::expected<std::vector<void *>, ErrorCode>
RealClient::map_dummy_addrs_to_real_ptrs(
    const ShmContext &context, const std::vector<uint64_t> &dummy_addrs,
    const std::vector<size_t> &sizes, const UUID &client_id,
    std::vector<size_t> *capacities) const {
    if (dummy_addrs.size() != sizes.size()) {
        LOG(ERROR) << "Mismatched dummy_addrs and sizes, client_id="
                   << client_id;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::vector<void *> buffers;
    buffers.reserve(dummy_addrs.size());
    if (capacities) {
        capacities->clear();
        capacities->reserve(dummy_addrs.size());
    }
    const MappedShm *last_hit_shm = nullptr;
    for (size_t i = 0; i < dummy_addrs.size(); ++i) {
        void *real_ptr = nullptr;
        size_t capacity = 0;
        if (!map_dummy_buffer_to_real(context, dummy_addrs[i], sizes[i],
                                      last_hit_shm, real_ptr,
                                      capacities ? &capacity : nullptr)) {
            LOG(ERROR) << "Dummy buffer at " << dummy_addrs[i] << " (size "
                       << sizes[i]
                       << ") not found in any mapped shared memory, client_id="
                       << client_id;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        buffers.push_back(real_ptr);
        if (capacities) capacities->push_back(capacity);
    }
    return buffers;
}

tl::expected<std::vector<std::vector<void *>>, ErrorCode>
RealClient::map_dummy_nested_addrs_to_real_ptrs(
    const ShmContext &context,
    const std::vector<std::vector<uint64_t>> &dummy_all_buffers,
    const std::vector<std::vector<size_t>> &all_sizes,
    const std::vector<std::string> &keys, const UUID &client_id) const {
    if (keys.size() != dummy_all_buffers.size() ||
        keys.size() != all_sizes.size()) {
        LOG(ERROR) << "Mismatched sizes for keys, dummy buffers, and sizes";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::vector<std::vector<void *>> real_all_buffers;
    real_all_buffers.reserve(keys.size());
    const MappedShm *last_hit_shm = nullptr;
    for (size_t ki = 0; ki < keys.size(); ++ki) {
        const auto &row_addrs = dummy_all_buffers[ki];
        const auto &row_sizes = all_sizes[ki];
        if (row_addrs.size() != row_sizes.size()) {
            LOG(ERROR) << "Mismatched buffers and sizes for key: " << keys[ki];
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        std::vector<void *> row_real;
        row_real.reserve(row_addrs.size());
        for (size_t j = 0; j < row_addrs.size(); ++j) {
            void *real_ptr = nullptr;
            if (!map_dummy_buffer_to_real(context, row_addrs[j], row_sizes[j],
                                          last_hit_shm, real_ptr)) {
                LOG(ERROR)
                    << "Dummy buffer at " << row_addrs[j]
                    << " not found in any mapped shared memory for client "
                    << client_id;
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            }
            row_real.push_back(real_ptr);
        }
        real_all_buffers.push_back(std::move(row_real));
    }
    return real_all_buffers;
}

// ResourceTracker implementation using singleton pattern
// Use a deliberately leaked heap object to avoid static destruction
// order issues with atexit/signal handlers during process teardown.
ResourceTracker &ResourceTracker::getInstance() {
    static ResourceTracker *instance = new ResourceTracker();
    return *instance;
}

ResourceTracker::ResourceTracker() {
    // In embedded environments (e.g. Python), the host runtime owns signal
    // handling.  Blocking SIGINT with pthread_sigmask (done inside
    // startSignalThread) would prevent Python from raising KeyboardInterrupt,
    // causing the process to hang on Ctrl-C.  Detect Python at runtime via
    // dlsym so we don't need to include <Python.h> or change any public API.
    if (!dlsym(RTLD_DEFAULT, "Py_IsInitialized")) {
        // Standalone C/C++ process – install our own signal handling.
        startSignalThread();
    }

    // Register exit handler as a backstop (works in both standalone and
    // embedded modes).
    std::atexit(exitHandler);
}

ResourceTracker::~ResourceTracker() {
    // Cleanup is handled by exitHandler
}

void ResourceTracker::registerInstance(
    const std::shared_ptr<PyClient> &instance) {
    MutexLocker locker(&mutex_);
    instances_.push_back(instance);
}

void ResourceTracker::cleanupAllResources() {
    // Ensure this runs only once
    bool expected = false;
    if (!cleaned_.compare_exchange_strong(expected, true,
                                          std::memory_order_acq_rel)) {
        return;
    }

    MutexLocker locker(&mutex_);

    for (auto &wp : instances_) {
        if (auto sp = wp.lock()) {
            LOG(INFO) << "Cleaning up DistributedObjectStore instance";
            sp->tearDownAll();
        }
    }
}

void ResourceTracker::signalHandler(int signal) {
    // Legacy path (kept for compatibility if handlers are ever installed).
    // Prefer dedicated signal thread; avoid doing heavy work here.
    getInstance().cleanupAllResources();
    struct sigaction sa;
    sa.sa_handler = SIG_DFL;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(signal, &sa, nullptr);
    raise(signal);
}

void ResourceTracker::exitHandler() { getInstance().cleanupAllResources(); }

void ResourceTracker::startSignalThread() {
    std::call_once(signal_once_, [this]() {
        // Wait signal thread start
        std::promise<void> ready;
        auto ready_future = ready.get_future();

        // Block signals in this thread; new threads inherit this mask
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGINT);
        sigaddset(&set, SIGTERM);
        sigaddset(&set, SIGHUP);
        sigaddset(&set, SIGUSR1);  // used to interrupt sigwait on stop
        pthread_sigmask(SIG_BLOCK, &set, nullptr);

        signal_thread_ = std::jthread([set, ready = std::move(ready)](
                                          const std::stop_token &st) mutable {
            // Register a stop callback to interrupt sigwait via SIGUSR1
            pthread_t self = pthread_self();
            std::stop_callback cb(st,
                                  [self]() { pthread_kill(self, SIGUSR1); });
            ready.set_value();
            for (;;) {
                int sig = 0;
                int rc = sigwait(&set, &sig);
                if (rc != 0) {
                    LOG(ERROR) << "sigwait failed: " << strerror(rc);
                    continue;
                }

                if (sig == SIGUSR1) {
                    if (st.stop_requested()) {
                        break;  // graceful stop
                    }
                    continue;  // spurious
                }

                // Perform cleanup in normal thread context
                LOG(INFO) << "Received signal " << sig
                          << ", cleaning up resources";
                ResourceTracker::getInstance().cleanupAllResources();

                // Restore default action and re-raise to terminate normally
                struct sigaction sa;
                sa.sa_handler = SIG_DFL;
                sigemptyset(&sa.sa_mask);
                sa.sa_flags = 0;
                sigaction(sig, &sa, nullptr);

                // Unblock the signal before raising it so it can be
                // delivered immediately
                sigset_t unblock_set;
                sigemptyset(&unblock_set);
                sigaddset(&unblock_set, sig);
                int ret = pthread_sigmask(SIG_UNBLOCK, &unblock_set, nullptr);
                if (ret != 0) {
                    LOG(ERROR) << "Failed to unblock signal " << sig
                               << " before raising: " << strerror(ret);
                    _exit(EXIT_FAILURE);
                }
                raise(sig);

                break;  // Should not reach due to process termination
            }
        });
        ready_future.wait();  // Ensure thread is ready
    });
}

RealClient::RealClient() {
    // Initialize logging severity (leave as before)
    mooncake::init_ylt_log_level();
    const char *hp = std::getenv("MC_STORE_USE_HUGEPAGE");
    use_hugepage_ = (hp != nullptr);
    const size_t h2d_streams_per_device = DfsH2dStreamCount();
    dfs_h2d_stream_pool_ =
        std::make_unique<DfsH2dStreamPool>(h2d_streams_per_device);
    LOG(INFO) << "DFS staging config: h2d_streams_per_device="
              << h2d_streams_per_device;
}

RealClient::~RealClient() {
    if (offload_rpc_server_) {
        offload_rpc_server_->stop();
        offload_rpc_server_.reset();
    }
    // Ensure resources are cleaned even if not explicitly closed
    stop_http_server();
    tearDownAll_internal();
}

std::shared_ptr<RealClient> RealClient::create() {
    auto sp = std::shared_ptr<RealClient>(new RealClient());
    ResourceTracker::getInstance().registerInstance(sp);
    return sp;
}

tl::expected<void, ErrorCode> RealClient::setup_ascend_internal(
    size_t local_buffer_size) {
#ifdef USE_ASCEND_DIRECT
    // Initialize ContextManager for dummy-real mode
    if (!ContextManager::getInstance().initialize()) {
        LOG(ERROR) << "Failed to initialize ContextManager";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    LOG(INFO) << "ContextManager initialized with "
              << ContextManager::getInstance().getDeviceCount() << " device(s)";
#endif
    return {};
}

tl::expected<void, ErrorCode> RealClient::setup_internal(
    const std::string &local_hostname, const std::string &metadata_server,
    size_t global_segment_size, size_t local_buffer_size,
    const std::string &protocol, const std::string &rdma_devices,
    const std::string &master_server_addr,
    const std::shared_ptr<TransferEngine> &transfer_engine,
    const std::string &ipc_socket_path, int local_rpc_port,
    bool enable_ssd_offload, bool start_offload_rpc_server,
    const std::string &ssd_offload_path, const std::string &tenant_id,
    bool enable_client_http_server, int client_http_port,
    bool enable_dfs_prefetch) {
    prefetch_arena_available_.store(false, std::memory_order_release);
    prefetch_arena_stats_.reset();
    this->protocol = protocol;
    this->ipc_socket_path_ = ipc_socket_path;
    const bool should_use_hugepage =
        use_hugepage_ && this->protocol != "ubshmem";
#ifdef USE_ASCEND_DIRECT
    if (protocol == "ascend" && globalConfig().ascend_agent_mode) {
        auto ascend_setup = setup_ascend_internal(local_buffer_size);
        if (!ascend_setup) return ascend_setup;
        if (!ContextManager::getInstance().setCurrentContext(0)) {
            LOG(ERROR) << "Failed to set current context for device " << 0;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }
#endif

#ifdef USE_NOF
    if (!SpdkWrapper::GetInstance().InitializeEnv()) {
        LOG(ERROR) << "spdk env init fail";
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
#endif

    std::optional<std::string> device_name =
        ((rdma_devices.empty() || rdma_devices == "auto-discovery")
             ? std::nullopt
             : std::make_optional(rdma_devices));

    // Validate required parameters
    if (local_hostname.empty()) {
        LOG(ERROR) << "local_hostname is empty";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Check if hostname already contains a port
    const std::string &hostname = local_hostname;
    bool user_specified_port = hasExplicitPort(hostname);

    if (user_specified_port) {
        // User specified port, no retry needed
        this->local_hostname = local_hostname;
        this->local_rpc_addr = buildHostNameWithPort(
            getHostNameWithoutPort(hostname), local_rpc_port);
        auto client_opt = mooncake::Client::Create(
            this->local_hostname, metadata_server, protocol, device_name,
            master_server_addr, transfer_engine, {{"client_mode", "real"}},
            tenant_id);
        if (!client_opt) {
            LOG(ERROR) << "Failed to create client";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        client_ = *client_opt;
    } else {
        // Auto port binding with retry on metadata registration failure
        const int kMaxRetries =
            Environ::GetInt("MC_STORE_CLIENT_SETUP_RETRIES", 20);
        const int rawMinPort =
            Environ::GetInt("MC_STORE_CLIENT_MIN_PORT", 12300);
        const int rawMaxPort =
            Environ::GetInt("MC_STORE_CLIENT_MAX_PORT", 14300);
        constexpr int kDefaultMinPort = 12300;
        constexpr int kDefaultMaxPort = 14300;
        auto [minPort, maxPort] = ValidatePortRange(
            rawMinPort, rawMaxPort, kDefaultMinPort, kDefaultMaxPort);
        bool success = false;

        for (int retry = 0; retry < kMaxRetries; ++retry) {
            // Create port binder to hold a port
            port_binder_ = std::make_unique<AutoPortBinder>(minPort, maxPort);
            int port = port_binder_->getPort();
            if (port < 0) {
                LOG(WARNING) << "Failed to bind available port, retry "
                             << (retry + 1) << "/" << kMaxRetries;
                port_binder_.reset();
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }

            this->local_hostname = buildHostNameWithPort(hostname, port);
            this->local_rpc_addr =
                buildHostNameWithPort(hostname, local_rpc_port);
            auto client_opt = mooncake::Client::Create(
                this->local_hostname, metadata_server, protocol, device_name,
                master_server_addr, transfer_engine, {{"client_mode", "real"}},
                tenant_id);
            if (client_opt) {
                client_ = *client_opt;
                success = true;
                LOG(INFO) << "Successfully created client on port " << port
                          << " after " << (retry + 1) << " attempt(s)";
                break;
            }

            // Failed to create client (possibly due to metadata registration
            // conflict), release port and retry with a different port
            LOG(WARNING) << "Failed to create client on port " << port
                         << ", retry " << (retry + 1) << "/" << kMaxRetries;
            port_binder_.reset();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        if (!success) {
            LOG(ERROR) << "Failed to create client after " << kMaxRetries
                       << " retries";
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }

    // Local_buffer_size is allowed to be 0, but we only register memory when
    // local_buffer_size > 0. Invoke ibv_reg_mr() with size=0 is UB, and may
    // fail in some rdma implementations.
    // Dummy Client can create shm and share it with Real Client, so Real Client
    // can create client buffer allocator on the shared memory later.
    bool use_spdk_dma_for_client_buffer = false;
#ifdef USE_NOF
    use_spdk_dma_for_client_buffer = true;
#endif
    client_buffer_allocator_ = ClientBufferAllocator::create(
        local_buffer_size, this->protocol, should_use_hugepage,
        use_spdk_dma_for_client_buffer);

    if (local_buffer_size > 0 && protocol != "cxl") {
        LOG(INFO) << "Registering local memory: " << local_buffer_size
                  << " bytes";
        auto result = client_->RegisterLocalMemory(
            client_buffer_allocator_->getBase(), local_buffer_size,
            kWildcardLocation, false, true);
        if (!result.has_value()) {
            LOG(ERROR) << "Failed to register local memory: "
                       << toString(result.error());
            return tl::unexpected(result.error());
        }
        {
            std::unique_lock<std::shared_mutex> lock(registered_buffer_mutex_);
            local_buffer_region_ = WritableBufferRegion{
                .base = client_buffer_allocator_->getBase(),
                .size = local_buffer_size,
                .offset = 0,
            };
        }
    } else {
        LOG(INFO) << "Local buffer size is 0, skip registering local memory";
    }

    // If global_segment_size is 0, skip mount segment. Transports with a
    // registration limit split it into balanced chunks; other transports use
    // one segment.
    if (protocol == "cxl") {
        size_t cxl_dev_size = 0;
        const char *env = std::getenv("MC_CXL_DEV_SIZE");
        if (env) {
            cxl_dev_size =
                TryParseInteger<size_t>(env, {.trim_ascii_whitespace = true,
                                              .allow_leading_plus = true})
                    .value_or(0);
        } else {
            LOG(FATAL) << "MC_CXL_DEV_SIZE not set";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }

        void *ptr = client_->GetBaseAddr();
        LOG(INFO) << "Mounting CXL segment: " << cxl_dev_size << " bytes, "
                  << ptr;
        auto mount_result = client_->MountSegment(ptr, cxl_dev_size, protocol);
        if (!mount_result.has_value()) {
            LOG(ERROR) << "Failed to mount segment: "
                       << toString(mount_result.error());
            return tl::unexpected(mount_result.error());
        }

    } else {
        uint64_t total_glbseg_size = global_segment_size;  // For logging
        uint64_t current_glbseg_size = 0;                  // For logging

        auto split_limit = GetTransportRegistrationLimit(protocol);
        const size_t alignment = facebook::cachelib::Slab::kSize;
#ifdef USE_ASCEND_DIRECT
        if (protocol == "ascend" && globalConfig().ascend_agent_mode) {
            const size_t cap = AgentModeStoreChunkCap(global_segment_size);
            if (cap > 0) {
                if (!split_limit.has_value() || cap < *split_limit) {
                    split_limit = cap;
                }
                LOG(INFO) << "Agent-mode store chunk cap: " << cap
                          << " bytes across "
                          << ContextManager::getInstance().getDeviceCount()
                          << " device(s)";
            }
        }
#endif

        // For RDMA, auto-discover NUMA nodes with NICs and distribute
        // global_segment across them for full NIC utilization.
        std::vector<int> seg_numa_nodes;
        if (protocol == "rdma") {
            seg_numa_nodes = client_->GetNicNumaNodes();
            if (seg_numa_nodes.size() > 1) {
                std::string nodes_str;
                for (size_t i = 0; i < seg_numa_nodes.size(); ++i) {
                    if (i) nodes_str += ",";
                    nodes_str += std::to_string(seg_numa_nodes[i]);
                }
                LOG(INFO) << "NUMA-segmented mode: NIC NUMA nodes=["
                          << nodes_str << "]";
            } else {
                seg_numa_nodes.clear();
            }
        }

        const bool parallel_hugetlb_population =
            protocol == "rdma" && should_use_hugepage;

        while (global_segment_size > 0) {
            size_t segment_size =
                GetNextSegmentSize(global_segment_size, split_limit, alignment);
            if (segment_size == 0) {
                LOG(ERROR) << "Registration limit is smaller than segment "
                              "alignment";
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            }
            global_segment_size -= segment_size;

            size_t mapped_size = segment_size;
            void *ptr = nullptr;
            std::string seg_location = kWildcardLocation;

            if (!seg_numa_nodes.empty()) {
                // NUMA-segmented allocation: contiguous VMA, per-region binding
                size_t page_sz = should_use_hugepage
                                     ? get_hugepage_size_from_env()
                                     : static_cast<size_t>(getpagesize());
                mapped_size =
                    align_up(segment_size, page_sz * seg_numa_nodes.size());
                ptr = allocate_buffer_numa_segments(mapped_size, seg_numa_nodes,
                                                    page_sz);
                seg_location = buildSegmentsLocation(page_sz, seg_numa_nodes);
            } else if (should_use_hugepage) {
                mapped_size =
                    align_up(segment_size, get_hugepage_size_from_env());
                ptr = allocate_buffer_mmap_memory(mapped_size,
                                                  get_hugepage_size_from_env(),
                                                  parallel_hugetlb_population);
#if defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
            } else if ((protocol == "ascend" || protocol == "ubshmem") &&
                       globalConfig().ascend_use_fabric_mem) {
                size_t actual_size = 0;
                ptr = ascend_allocate_memory_best_effort(
                    segment_size, this->protocol, &actual_size);
                if (ptr) {
                    mapped_size = actual_size;
                }
#endif
            } else {
                ptr = allocate_buffer_allocator_memory(segment_size,
                                                       this->protocol);
            }

            if (!ptr) {
                LOG(ERROR) << "Failed to allocate segment memory";
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            }
            const size_t mount_size =
                split_limit.has_value() ? segment_size : mapped_size;
            current_glbseg_size += mount_size;
            LOG(INFO) << "Mounting segment: " << mount_size << " bytes, "
                      << current_glbseg_size << " of " << total_glbseg_size;

            if (this->protocol == "ascend" || this->protocol == "ubshmem") {
                ascend_segment_ptrs_.emplace_back(
                    ptr, AscendSegmentDeleter{this->protocol});
            } else if (this->protocol == "sunrise_link") {
#if defined(USE_SUNRISE)
                sunrise_segment_ptrs_.emplace_back(ptr,
                                                   SunriseSegmentDeleter{});
#else
                LOG(ERROR)
                    << "sunrise_link protocol requires USE_SUNRISE build";
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
#endif
            } else if (this->protocol == "ub") {
                ub_segment_ptrs_.emplace_back(ptr,
                                              UbSegmentDeleter{mapped_size});
            } else if (!seg_numa_nodes.empty() || should_use_hugepage) {
                // NUMA-segmented or hugepage: track as mmap allocation for
                // munmap cleanup
                hugepage_segment_ptrs_.emplace_back(
                    ptr, HugepageSegmentDeleter{mapped_size});
            } else {
#ifdef USE_VRAM_SEGMENT
                vram_segment_ptrs_.emplace_back(ptr);
#else
                segment_ptrs_.emplace_back(ptr);
#endif
            }

            // Populate HugeTLB pages in parallel immediately before transfer-
            // engine registration. NUMA mappings use node-local workers for
            // each mbind region; direct mappings use the generic worker pool.
            if (parallel_hugetlb_population) {
                if (!seg_numa_nodes.empty()) {
                    populate_hugetlb_numa_mapping(ptr, mapped_size,
                                                  seg_numa_nodes);
                } else if (!is_mmap_arena_allocation(ptr)) {
                    populate_hugetlb_mapping(ptr, mapped_size);
                }
            }

            auto pinned_region =
                TryPinStoreSegment(ptr, mapped_size, this->protocol, "setup");
            auto mount_result =
                client_->MountSegment(ptr, mount_size, protocol, seg_location);
            if (!mount_result.has_value()) {
                if (!ReleasePinnedRegionForFree(pinned_region,
                                                "Store setup segment")) {
                    setup_segment_memory_must_leak_ = true;
                }
                LOG(ERROR) << "Failed to mount segment: "
                           << toString(mount_result.error());
                return tl::unexpected(mount_result.error());
            }
            if (pinned_region) {
                setup_segment_pinned_regions_.push_back(
                    std::move(pinned_region));
            }
        }
        if (total_glbseg_size == 0) {
            LOG(INFO) << "Global segment size is 0, skip mounting segment";
        }
#ifdef USE_ASCEND_DIRECT
        if (protocol == "ascend" && globalConfig().ascend_agent_mode &&
            !RestoreAgentModeDeviceZero()) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
#endif
    }

    // Start IPC server to accept FD from dummy clients
    if (!ipc_socket_path_.empty()) {
        if (start_ipc_server() != 0) {
            LOG(ERROR) << "Failed to start IPC server at " << ipc_socket_path_;
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        LOG(INFO) << "Starting IPC server at " << ipc_socket_path_;
    }
    if (enable_ssd_offload && start_offload_rpc_server) {
        // Start RPC server for offload operations (batch_get / release_buffer).
        // Use port 0 to let the OS auto-allocate an available port.
        offload_rpc_server_ =
            std::make_unique<coro_rpc::coro_rpc_server>(1, 0, "0.0.0.0");
        offload_rpc_server_
            ->register_handler<&RealClient::batch_get_offload_object>(this);
        offload_rpc_server_
            ->register_handler<&RealClient::release_offload_buffer>(this);
        offload_rpc_server_->async_start();
        auto err = offload_rpc_server_->get_errc();
        if (err) {
            LOG(ERROR) << "Failed to start offload RPC server: "
                       << err.message();
            offload_rpc_server_.reset();
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        offload_rpc_port_ = offload_rpc_server_->port();
        LOG(INFO) << "Offload RPC server started on port " << offload_rpc_port_;

        // Build local_rpc_addr from hostname + auto-allocated port
        this->local_rpc_addr = buildHostNameWithPort(
            getHostNameWithoutPort(this->local_hostname), offload_rpc_port_);
    }
    if (enable_ssd_offload) {
        auto file_storage_config = FileStorageConfig::FromEnvironment();
        if (!ssd_offload_path.empty()) {
            file_storage_config.storage_filepath = ssd_offload_path;
        }
        if (!enable_dfs_prefetch) {
            file_storage_config.pinned_prefetch_arena_size = 0;
        }
        file_storage_ = std::make_shared<FileStorage>(
            file_storage_config, client_, this->local_rpc_addr,
            client_->GetSsdMetricPtr());
        auto init_result = file_storage_->Init();
        if (!init_result) {
            LOG(ERROR) << "file storage init failed with error: "
                       << init_result.error();
            return init_result;
        }
        const bool prefetch_arena_available =
            enable_dfs_prefetch && file_storage_->HasPinnedPrefetchArena();
        if (prefetch_arena_available) {
            prefetch_arena_stats_ = std::make_shared<PrefetchArenaStats>();
        }
        prefetch_arena_available_.store(prefetch_arena_available,
                                        std::memory_order_release);
    }
    client_requester_ = std::make_shared<ClientRequester>();
    const bool should_start_http_server =
        enable_client_http_server || FLAGS_enable_http_server;
    const int selected_http_port =
        enable_client_http_server ? client_http_port : FLAGS_http_port;
    if (should_start_http_server) {
        // Port zero asks the listening socket to allocate an available port.
        // Independent SGLang instances can then enable HTTP metrics without
        // sharing a rank-derived fixed port.
        if (selected_http_port < 0 || selected_http_port > 65535) {
            LOG(ERROR) << "Invalid client HTTP server port: "
                       << selected_http_port;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (start_http_server(selected_http_port) != 0) {
            LOG(WARNING) << "Failed to start client HTTP server on port "
                         << selected_http_port
                         << "; continuing without HTTP endpoints";
        }
    }

    return {};
}

int RealClient::setup_real(
    const std::string &local_hostname, const std::string &metadata_server,
    size_t global_segment_size, size_t local_buffer_size,
    const std::string &protocol, const std::string &rdma_devices,
    const std::string &master_server_addr,
    const std::shared_ptr<TransferEngine> &transfer_engine,
    const std::string &ipc_socket_path, bool enable_ssd_offload,
    const std::string &ssd_offload_path, const std::string &tenant_id,
    bool enable_client_http_server, int client_http_port,
    bool enable_dfs_prefetch) {
    return to_py_ret(setup_internal(
        local_hostname, metadata_server, global_segment_size, local_buffer_size,
        protocol, rdma_devices, master_server_addr, transfer_engine,
        ipc_socket_path, 50052, enable_ssd_offload, true, ssd_offload_path,
        tenant_id, enable_client_http_server, client_http_port,
        enable_dfs_prefetch));
}

namespace {
// Helper to get value from config dict with default
inline std::string get_config(const ConfigDict &config, const std::string &key,
                              const std::string &default_value = "") {
    auto it = config.find(key);
    return (it != config.end()) ? it->second : default_value;
}

inline std::optional<size_t> get_config_size(const ConfigDict &config,
                                             const std::string &key,
                                             size_t default_value) {
    auto it = config.find(key);
    if (it == config.end()) {
        return default_value;
    }

    auto parsed_size_opt = try_string_to_byte_size(it->second);
    if (!parsed_size_opt.has_value()) {
        LOG(ERROR) << "Invalid size value for config key '" << key
                   << "': " << it->second;
        return std::nullopt;
    }
    return static_cast<size_t>(parsed_size_opt.value());
}

inline bool get_config_bool(const ConfigDict &config, const std::string &key,
                            bool default_value) {
    auto it = config.find(key);
    if (it == config.end()) {
        return default_value;
    }

    const auto parsed = TryParseBool(it->second);
    if (parsed.has_value()) {
        return *parsed;
    }

    LOG(WARNING) << "Invalid boolean value for config key '" << key
                 << "': " << it->second << ", using default: " << default_value;
    return default_value;
}

inline std::optional<int> get_config_int(const ConfigDict &config,
                                         const std::string &key,
                                         int default_value) {
    auto it = config.find(key);
    if (it == config.end()) {
        return default_value;
    }

    const auto parsed_value =
        TryParseInteger<int>(it->second, {.trim_ascii_whitespace = true});
    if (!parsed_value.has_value()) {
        LOG(ERROR) << "Invalid integer value for config key '" << key
                   << "': " << it->second;
        return std::nullopt;
    }
    return *parsed_value;
}
}  // namespace

tl::expected<void, ErrorCode> RealClient::setup_internal(
    const ConfigDict &config) {
    // Extract required parameters (no defaults)
    std::string local_hostname = get_config(config, CONFIG_KEY_LOCAL_HOSTNAME);
    std::string metadata_server =
        get_config(config, CONFIG_KEY_METADATA_SERVER);

    // Validate required parameters
    if (local_hostname.empty()) {
        LOG(ERROR) << "Missing required config: " << CONFIG_KEY_LOCAL_HOSTNAME;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (metadata_server.empty()) {
        LOG(ERROR) << "Missing required config: " << CONFIG_KEY_METADATA_SERVER;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Extract optional parameters with defaults
    auto global_segment_size_opt = get_config_size(
        config, CONFIG_KEY_GLOBAL_SEGMENT_SIZE, DEFAULT_GLOBAL_SEGMENT_SIZE);
    if (!global_segment_size_opt.has_value()) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto local_buffer_size_opt = get_config_size(
        config, CONFIG_KEY_LOCAL_BUFFER_SIZE, DEFAULT_LOCAL_BUFFER_SIZE);
    if (!local_buffer_size_opt.has_value()) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    size_t global_segment_size = global_segment_size_opt.value();
    size_t local_buffer_size = local_buffer_size_opt.value();
    std::string protocol =
        get_config(config, CONFIG_KEY_PROTOCOL, DEFAULT_PROTOCOL);
    std::string rdma_devices = get_config(config, CONFIG_KEY_RDMA_DEVICES);
    std::string master_server_addr = get_config(
        config, CONFIG_KEY_MASTER_SERVER_ADDR, DEFAULT_MASTER_SERVER_ADDR);
    std::string ipc_socket_path =
        get_config(config, CONFIG_KEY_IPC_SOCKET_PATH);

    // A size of 0 keeps the pure client/server setup semantics.
    // global_segment_size is a total capacity; protocols with a registration
    // limit split it into mountable chunks below.
    auto validate_min_size = [](const char *key, size_t value) {
        if (value != 0 && value < MIN_SEGMENT_SIZE) {
            LOG(ERROR) << "Invalid " << key << ": " << value
                       << ", must be 0 or at least " << MIN_SEGMENT_SIZE;
            return false;
        }
        return true;
    };
    auto validate_single_segment_size = [](const char *key, size_t value) {
        if ((value != 0 && value < MIN_SEGMENT_SIZE) ||
            value > MAX_SEGMENT_SIZE) {
            LOG(ERROR) << "Invalid " << key << ": " << value
                       << ", must be 0 or between " << MIN_SEGMENT_SIZE
                       << " and " << MAX_SEGMENT_SIZE;
            return false;
        }
        return true;
    };
    if (!validate_min_size(CONFIG_KEY_GLOBAL_SEGMENT_SIZE,
                           global_segment_size) ||
        !validate_single_segment_size(CONFIG_KEY_LOCAL_BUFFER_SIZE,
                                      local_buffer_size)) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::string ssd_offload_path = get_config(config, "ssd_offload_path");
    std::string tenant_id = get_config(config, CONFIG_KEY_TENANT_ID, "default");
    bool enable_ssd_offload =
        get_config_bool(config, "enable_ssd_offload", false);
    bool enable_client_http_server =
        get_config_bool(config, CONFIG_KEY_ENABLE_CLIENT_HTTP_SERVER, false);
    bool enable_dfs_prefetch =
        get_config_bool(config, "enable_dfs_prefetch", true);
    auto client_http_port_opt = get_config_int(
        config, CONFIG_KEY_CLIENT_HTTP_PORT, DEFAULT_CLIENT_HTTP_PORT);
    if (!client_http_port_opt.has_value()) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    int client_http_port = client_http_port_opt.value();

    return setup_internal(local_hostname, metadata_server, global_segment_size,
                          local_buffer_size, protocol, rdma_devices,
                          master_server_addr, nullptr, ipc_socket_path, 50052,
                          enable_ssd_offload, true, ssd_offload_path, tenant_id,
                          enable_client_http_server, client_http_port,
                          enable_dfs_prefetch);
}

tl::expected<void, ErrorCode> RealClient::initAll_internal(
    const std::string &protocol_, const std::string &device_name,
    size_t mount_segment_size) {
    if (client_) {
        LOG(ERROR) << "Client is already initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    uint64_t buffer_allocator_size = 1024 * 1024 * 1024;
    return setup_internal("localhost:12345", "127.0.0.1:2379",
                          mount_segment_size, buffer_allocator_size, protocol_,
                          device_name);
}

int RealClient::initAll(const std::string &protocol_,
                        const std::string &device_name,
                        size_t mount_segment_size) {
    return to_py_ret(
        initAll_internal(protocol_, device_name, mount_segment_size));
}

tl::expected<void, ErrorCode> RealClient::tearDownAll_internal() {
    // Ensure cleanup executes once across destructor/close/signal paths
    bool expected = false;
    if (!closed_.compare_exchange_strong(expected, true,
                                         std::memory_order_acq_rel)) {
        return {};
    }

    stop_ipc_server();
    stop_dummy_client_monitor();
    stop_http_server();
    // New callers observe closed_ and return after entering this shared
    // lifecycle region. Taking it exclusively drains existing DFS callers
    // before streams and their pinned sources are released.
    std::unique_lock<std::shared_mutex> dfs_read_lock(
        dfs_read_lifecycle_mutex_);
    dfs_read_shutting_down_ = true;

    dfs_h2d_stream_pool_.reset();

    std::vector<std::shared_ptr<BufferHandle>> detached_prefetch_buffers;
    {
        std::lock_guard<std::mutex> lock(session_mutex_);
        if (prefetch_arena_available_.load(std::memory_order_acquire)) {
            detached_prefetch_buffers.reserve(
                get_session_prefetch_cache_.size());
            for (auto &entry : get_session_prefetch_cache_) {
                detached_prefetch_buffers.push_back(
                    std::move(entry.second.buffer_handle));
            }
        }
        get_session_prefetch_cache_.clear();
        prefetch_arena_available_.store(false, std::memory_order_release);
        prefetch_arena_stats_.reset();
    }
    if (!client_) {
        // Not initialized or already cleaned; treat as success for idempotence
        return {};
    }
    if (client_buffer_allocator_ && client_buffer_allocator_->size() > 0 &&
        protocol != "cxl") {
        auto unregister_result = client_->unregisterLocalMemory(
            client_buffer_allocator_->getBase(), true);
        if (!unregister_result) {
            LOG(WARNING)
                << "Failed to unregister client local buffer on tear down: "
                << toString(unregister_result.error());
        }
        std::unique_lock<std::shared_mutex> lock(registered_buffer_mutex_);
        local_buffer_region_.reset();
    }

    // Reset all resources
    client_.reset();
    ReleaseAllMountedSegmentRecords();
    ReleaseAllAllocatedSegmentRecords();
    client_buffer_allocator_.reset();
    port_binder_.reset();
    const bool setup_segments_safe_to_free = ReleasePinnedRegionsForFree(
        setup_segment_pinned_regions_, "Store setup segment");
    if (!setup_segments_safe_to_free || setup_segment_memory_must_leak_) {
        for (auto &ptr : hugepage_segment_ptrs_) ptr.release();
        for (auto &ptr : segment_ptrs_) ptr.release();
        setup_segment_memory_must_leak_ = false;
    }
    hugepage_segment_ptrs_.clear();
    segment_ptrs_.clear();
    ub_segment_ptrs_.clear();
#ifdef USE_VRAM_SEGMENT
    vram_segment_ptrs_.clear();
#endif
#if defined(USE_SUNRISE)
    sunrise_segment_ptrs_.clear();
#endif
    local_hostname = "";
    device_name = "";
    protocol = "";
    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto shm_it = shm_contexts_.begin();
    while (shm_it != shm_contexts_.end()) {
        auto &context = shm_it->second;
        context.client_buffer_allocator.reset();

        // Iterate over all mapped_shms to unmap them
        for (auto &seg : context.mapped_shms) {
            if (seg.shm_buffer == nullptr) {
                continue;
            }
            if (seg.is_ascend) {
                teardown_ascend_shm_buffer(seg);
            } else if (munmap(seg.shm_buffer, seg.shm_size) != 0) {
                LOG(ERROR) << "Failed to unmap shm: " << seg.shm_name
                           << ", error: " << strerror(errno);
            }
            seg.shm_buffer = nullptr;
        }
        context.mapped_shms.clear();

        shm_it = shm_contexts_.erase(shm_it);
    }
    return {};
}

int RealClient::tearDownAll() { return to_py_ret(tearDownAll_internal()); }

int RealClient::mountSegment(const std::string &path, size_t offset,
                             size_t size, const std::string &protocol,
                             const std::string &location,
                             std::vector<std::string> &out_segment_ids) {
    if (!client_) {
        LOG(ERROR) << "Client not initialized";
        return -1;
    }

    size_t page_size = sysconf(_SC_PAGESIZE);

    int fd = open(path.c_str(), O_RDWR);
    if (fd < 0) {
        LOG(ERROR) << "open failed for " << path << ": " << strerror(errno);
        return -1;
    }

    struct stat st;
    if (fstat(fd, &st) != 0) {
        LOG(ERROR) << "fstat failed for " << path << ": " << strerror(errno);
        close(fd);
        return -1;
    }

    if (offset > (size_t)st.st_size || size > (size_t)st.st_size - offset) {
        LOG(ERROR) << "requested range [offset=" << offset << ", size=" << size
                   << "] exceeds file size " << st.st_size;
        close(fd);
        return -1;
    }

    if (offset % page_size != 0) {
        LOG(ERROR) << "offset " << offset
                   << " is not page-aligned (page_size=" << page_size << ")";
        close(fd);
        return -1;
    }

    const auto split_limit = GetTransportRegistrationLimit(protocol);
    size_t remaining = size;
    size_t current_offset = offset;
    std::vector<std::string> mounted_ids;
    std::vector<MountedSegmentRecord> mounted_records;

    while (remaining > 0) {
        size_t chunk_size =
            GetNextSegmentSize(remaining, split_limit, page_size);
        if (chunk_size == 0) break;

        void *ptr = mmap(nullptr, chunk_size, PROT_READ | PROT_WRITE,
                         MAP_SHARED, fd, current_offset);
        if (ptr == MAP_FAILED) {
            LOG(ERROR) << "mmap failed for " << path << ": " << strerror(errno);
            break;
        }

        auto result =
            client_->MountSegmentAndGetId(ptr, chunk_size, protocol, location);
        if (!result.has_value()) {
            LOG(ERROR) << "MountSegmentAndGetId failed";
            munmap(ptr, chunk_size);
            break;
        }

        std::string segment_id = UuidToString(result.value());
        mounted_ids.push_back(segment_id);
        mounted_records.push_back({ptr, chunk_size, path});

        remaining -= chunk_size;
        current_offset += chunk_size;
    }

    close(fd);

    // If not all chunks were mounted, roll back successfully mounted ones
    if (remaining > 0) {
        for (size_t i = 0; i < mounted_ids.size(); ++i) {
            UUID id;
            if (StringToUuid(mounted_ids[i], id)) {
                client_->UnmountSegmentById(id);
            }
            if (mounted_records[i].mmap_base) {
                munmap(mounted_records[i].mmap_base, mounted_records[i].size);
            }
        }
        out_segment_ids.clear();
        return -1;
    }

    {
        std::lock_guard<std::mutex> lock(mounted_segment_records_mutex_);
        for (size_t i = 0; i < mounted_ids.size(); ++i) {
            mounted_segment_records_[mounted_ids[i]] = mounted_records[i];
        }
    }
    out_segment_ids = std::move(mounted_ids);
    return 0;
}

void RealClient::ReleaseMountedSegmentRecord(const std::string &segment_id) {
    MountedSegmentRecord record;
    bool found = false;
    {
        std::lock_guard<std::mutex> lock(mounted_segment_records_mutex_);
        auto it = mounted_segment_records_.find(segment_id);
        if (it != mounted_segment_records_.end()) {
            record = it->second;
            mounted_segment_records_.erase(it);
            found = true;
        }
    }
    if (found && record.mmap_base) {
        munmap(record.mmap_base, record.size);
    }
}

void RealClient::ReleaseAllMountedSegmentRecords() {
    std::vector<MountedSegmentRecord> records;
    {
        std::lock_guard<std::mutex> lock(mounted_segment_records_mutex_);
        records.reserve(mounted_segment_records_.size());
        for (auto &entry : mounted_segment_records_) {
            records.push_back(entry.second);
        }
        mounted_segment_records_.clear();
    }
    for (auto &record : records) {
        if (record.mmap_base) {
            munmap(record.mmap_base, record.size);
        }
    }
}

void RealClient::FreeAllocatedStoreSegment(AllocatedSegmentRecord &record) {
    if (record.base && ReleasePinnedRegionForFree(record.pinned_region,
                                                  "allocated Store segment")) {
        free_memory(record.protocol, record.base);
    }
}

void RealClient::ReleaseAllocatedSegmentRecord(const std::string &segment_id) {
    AllocatedSegmentRecord record;
    bool found = false;
    {
        std::lock_guard<std::mutex> lock(allocated_segment_records_mutex_);
        auto it = allocated_segment_records_.find(segment_id);
        if (it != allocated_segment_records_.end()) {
            record = it->second;
            allocated_segment_records_.erase(it);
            found = true;
        }
    }
    if (found && record.base) {
        FreeAllocatedStoreSegment(record);
    }
}

void RealClient::ReleaseAllAllocatedSegmentRecords() {
    std::unordered_map<std::string, AllocatedSegmentRecord> records;
    {
        std::lock_guard<std::mutex> lock(allocated_segment_records_mutex_);
        records.swap(allocated_segment_records_);
    }
    for (auto &entry : records) {
        FreeAllocatedStoreSegment(entry.second);
    }
}

int RealClient::unmountSegment(const std::vector<std::string> &segment_ids,
                               uint64_t grace_period_seconds) {
    if (!client_) {
        LOG(ERROR) << "Client not initialized";
        return -1;
    }

    uint64_t grace_period_ms = grace_period_seconds * 1000;
    int first_error = 0;
    struct SegmentToUnmount {
        std::string segment_id;
        UUID id;
    };
    std::vector<SegmentToUnmount> to_unmount;
    std::vector<std::pair<std::string, MountedSegmentRecord>> to_cleanup;
    {
        std::lock_guard<std::mutex> lock(mounted_segment_records_mutex_);
        for (const auto &segment_id : segment_ids) {
            UUID id;
            if (!StringToUuid(segment_id, id)) {
                LOG(ERROR) << "Invalid segment_id: " << segment_id;
                if (first_error == 0) first_error = -1;
                continue;
            }

            auto it = mounted_segment_records_.find(segment_id);
            if (it == mounted_segment_records_.end()) {
                LOG(ERROR) << "segment_id not found in mounted records: "
                           << segment_id;
                if (first_error == 0) first_error = -1;
                continue;
            }

            to_unmount.push_back({segment_id, id});
        }
    }

    for (auto &entry : to_unmount) {
        std::function<void(const UUID &)> cleanup_callback;
        if (grace_period_ms != 0) {
            cleanup_callback = [this](const UUID &cleanup_id) {
                ReleaseMountedSegmentRecord(UuidToString(cleanup_id));
            };
        }
        auto result = client_->UnmountSegmentById(entry.id, grace_period_ms,
                                                  std::move(cleanup_callback));
        if (!result.has_value()) {
            LOG(ERROR) << "UnmountSegmentById failed for " << entry.segment_id;
            if (first_error == 0) {
                first_error = static_cast<int>(result.error());
            }
            continue;  // Don't release local resources on failure
        }

        // For immediate unmount, clean up local mmap/fd right away.
        // For graceful unmount, local mmap/fd is kept until the segment
        // is actually removed or the client destructor runs.
        if (grace_period_ms == 0) {
            std::lock_guard<std::mutex> lock(mounted_segment_records_mutex_);
            auto it = mounted_segment_records_.find(entry.segment_id);
            if (it != mounted_segment_records_.end()) {
                to_cleanup.emplace_back(entry.segment_id, it->second);
                mounted_segment_records_.erase(it);
            }
        }
    }

    for (auto &p : to_cleanup) {
        if (p.second.mmap_base) {
            munmap(p.second.mmap_base, p.second.size);
        }
    }

    return first_error;
}

int RealClient::allocateAndMountSegment(
    size_t size, const std::string &protocol, const std::string &location,
    std::vector<std::string> &out_segment_ids, size_t *out_allocated_size) {
    if (!client_) {
        LOG(ERROR) << "Client not initialized";
        return -1;
    }

    if (size == 0) {
        LOG(ERROR) << "size is 0";
        return -1;
    }

    const size_t slab_size = facebook::cachelib::Slab::kSize;
    // Check overflow before aligning up to slab_size
    if (size > std::numeric_limits<size_t>::max() - (slab_size - 1)) {
        LOG(ERROR) << "size " << size
                   << " overflows when aligning to slab_size";
        return -1;
    }
    // Round up total size to slab_size multiple
    size_t aligned_total_size =
        ((size + slab_size - 1) / slab_size) * slab_size;
    const auto split_limit = GetTransportRegistrationLimit(protocol);
    size_t remaining = aligned_total_size;
    std::vector<std::string> mounted_ids;
    std::vector<AllocatedSegmentRecord> allocated_records;

    while (remaining > 0) {
        size_t chunk_size =
            GetNextSegmentSize(remaining, split_limit, slab_size);
        if (chunk_size == 0) break;

        void *ptr = allocate_buffer_allocator_memory(chunk_size, protocol);
        if (!ptr) {
            LOG(ERROR) << "allocate_buffer_allocator_memory failed for size "
                       << chunk_size;
            break;
        }

        auto pinned_region =
            TryPinStoreSegment(ptr, chunk_size, protocol, "allocated");
        auto result =
            client_->MountSegmentAndGetId(ptr, chunk_size, protocol, location);
        if (!result.has_value()) {
            LOG(ERROR) << "MountSegmentAndGetId failed";
            if (ReleasePinnedRegionForFree(pinned_region,
                                           "allocated Store segment")) {
                free_memory(protocol, ptr);
            }
            break;
        }

        std::string segment_id = UuidToString(result.value());
        mounted_ids.push_back(segment_id);
        allocated_records.push_back(
            {ptr, chunk_size, protocol, std::move(pinned_region)});

        remaining -= chunk_size;
    }

    if (remaining > 0) {
        for (size_t i = 0; i < mounted_ids.size(); ++i) {
            UUID id;
            if (StringToUuid(mounted_ids[i], id)) {
                client_->UnmountSegmentById(id);
            }
            if (allocated_records[i].base) {
                FreeAllocatedStoreSegment(allocated_records[i]);
            }
        }
        out_segment_ids.clear();
        return -1;
    }

    {
        std::lock_guard<std::mutex> lock(allocated_segment_records_mutex_);
        for (size_t i = 0; i < mounted_ids.size(); ++i) {
            allocated_segment_records_[mounted_ids[i]] = allocated_records[i];
        }
    }
    if (out_allocated_size) {
        *out_allocated_size = aligned_total_size;
    }
    out_segment_ids = std::move(mounted_ids);
    return 0;
}

int RealClient::unmountAndFreeSegment(
    const std::vector<std::string> &segment_ids,
    uint64_t grace_period_seconds) {
    if (!client_) {
        LOG(ERROR) << "Client not initialized";
        return -1;
    }

    uint64_t grace_period_ms = grace_period_seconds * 1000;
    int first_error = 0;
    struct SegmentToUnmount {
        std::string segment_id;
        UUID id;
    };
    std::vector<SegmentToUnmount> to_unmount;
    std::vector<std::pair<std::string, AllocatedSegmentRecord>> to_cleanup;
    {
        std::lock_guard<std::mutex> lock(allocated_segment_records_mutex_);
        for (const auto &segment_id : segment_ids) {
            UUID id;
            if (!StringToUuid(segment_id, id)) {
                LOG(ERROR) << "Invalid segment_id: " << segment_id;
                if (first_error == 0) first_error = -1;
                continue;
            }

            auto it = allocated_segment_records_.find(segment_id);
            if (it == allocated_segment_records_.end()) {
                LOG(ERROR) << "segment_id not found in allocated records: "
                           << segment_id;
                if (first_error == 0) first_error = -1;
                continue;
            }

            to_unmount.push_back({segment_id, id});
        }
    }

    for (auto &entry : to_unmount) {
        std::function<void(const UUID &)> cleanup_callback;
        if (grace_period_ms != 0) {
            cleanup_callback = [this](const UUID &cleanup_id) {
                ReleaseAllocatedSegmentRecord(UuidToString(cleanup_id));
            };
        }
        auto result = client_->UnmountSegmentById(entry.id, grace_period_ms,
                                                  std::move(cleanup_callback));
        if (!result.has_value()) {
            LOG(ERROR) << "UnmountSegmentById failed for " << entry.segment_id;
            if (first_error == 0) {
                first_error = static_cast<int>(result.error());
            }
            continue;  // Don't release local resources on failure
        }

        if (grace_period_ms == 0) {
            std::lock_guard<std::mutex> lock(allocated_segment_records_mutex_);
            auto it = allocated_segment_records_.find(entry.segment_id);
            if (it != allocated_segment_records_.end()) {
                to_cleanup.emplace_back(entry.segment_id, it->second);
                allocated_segment_records_.erase(it);
            }
        }
    }

    for (auto &p : to_cleanup) {
        FreeAllocatedStoreSegment(p.second);
    }

    return first_error;
}

int RealClient::drainLocalDiskSegment(uint64_t grace_period_seconds) {
    if (!client_) {
        LOG(ERROR) << "Client not initialized";
        return -1;
    }
    if (!file_storage_) {
        LOG(WARNING) << "action=drain_local_disk_segment, "
                        "warn=ssd_offload_not_enabled";
        return 0;
    }
    auto result =
        file_storage_->DrainLocalDiskSegment(grace_period_seconds * 1000);
    return result ? 0 : -1;
}

int RealClient::health_check() {
    if (closed_.load()) return HC_NOT_INITIALIZED;
    if (!client_) return HC_NOT_INITIALIZED;
    if (!client_->is_ping_healthy()) return HC_MASTER_UNREACHABLE;
    return HC_HEALTHY;
}

std::vector<int> RealClient::probe_dfs_health() {
    const int read_error = probe_dfs_read();
    const int write_error = probe_dfs_write();
    return {read_error, write_error,
            file_storage_ && file_storage_->IsDfsBackendReady() ? 1 : 0};
}

int RealClient::probe_dfs_read() {
    if (!file_storage_) return toInt(ErrorCode::NOT_SUPPORTED);
    const auto result = file_storage_->ProbeDfsRead();
    return result ? 0 : toInt(result.error());
}

int RealClient::probe_dfs_write() {
    if (!file_storage_) return toInt(ErrorCode::NOT_SUPPORTED);
    const auto result = file_storage_->ProbeDfsWrite();
    return result ? 0 : toInt(result.error());
}

async_simple::coro::Lazy<int> RealClient::probe_dfs_read_rpc() {
    // DFS calls may block indefinitely in the kernel. Keep them off the
    // coro_rpc I/O thread so a stuck read cannot prevent a write RPC from
    // being accepted and dispatched.
    auto result = co_await coro_io::post([this]() { return probe_dfs_read(); });
    co_return result.value();
}

async_simple::coro::Lazy<int> RealClient::probe_dfs_write_rpc() {
    // Use a separate worker task from read. The backend itself has no shared
    // probe lock, and failed initialization uses a non-blocking try-lock.
    auto result =
        co_await coro_io::post([this]() { return probe_dfs_write(); });
    co_return result.value();
}

int RealClient::set_dfs_access_policy(bool allow_read, bool allow_write) {
    if (!client_) return toInt(ErrorCode::INVALID_PARAMS);
    client_->SetDfsAccessPolicy(allow_read, allow_write);
    return 0;
}

int RealClient::set_service_endpoint(const std::string& endpoint) {
    if (!client_) return toInt(ErrorCode::INVALID_PARAMS);
    return client_->SetServiceEndpoint(endpoint);
}

int RealClient::start_http_server(int port) {
    using namespace coro_http;

    if (http_server_) {
        LOG(WARNING) << "Client HTTP server is already running";
        return 0;
    }

    http_server_ = std::make_unique<coro_http_server>(/*thread_num=*/1, port);

    http_server_->set_http_handler<GET>(
        "/health", [this](coro_http_request &req, coro_http_response &resp) {
            int code = health_check();
            std::string status_str;
            switch (code) {
                case HC_HEALTHY:
                    status_str = "healthy";
                    break;
                case HC_NOT_INITIALIZED:
                    status_str = "not_initialized";
                    break;
                case HC_MASTER_UNREACHABLE:
                    status_str = "master_unreachable";
                    break;
                default:
                    status_str = "unknown";
                    break;
            }
            std::string body = "{\"status\":\"" + status_str +
                               "\",\"code\":" + std::to_string(code) + "}";
            resp.add_header("Content-Type", "application/json");
            auto http_status = (code == HC_HEALTHY)
                                   ? status_type::ok
                                   : status_type::service_unavailable;
            resp.set_status_and_content(http_status, std::move(body));
        });

    http_server_->set_http_handler<GET>(
        "/metrics", [this](coro_http_request &req, coro_http_response &resp) {
            if (!client_) {
                resp.set_status_and_content(status_type::service_unavailable,
                                            "client not initialized");
                return;
            }
            auto result = client_->SerializeMetrics();
            if (!result) {
                resp.set_status_and_content(status_type::service_unavailable,
                                            "metrics not available");
                return;
            }
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, std::move(*result));
        });

    http_server_->set_http_handler<GET>(
        "/metrics/summary",
        [this](coro_http_request &req, coro_http_response &resp) {
            if (!client_) {
                resp.set_status_and_content(status_type::service_unavailable,
                                            "client not initialized");
                return;
            }
            auto result = client_->GetSummaryMetrics();
            if (!result) {
                resp.set_status_and_content(status_type::service_unavailable,
                                            "metrics not available");
                return;
            }
            resp.add_header("Content-Type", "text/plain");
            resp.set_status_and_content(status_type::ok, std::move(*result));
        });

    http_server_->set_http_handler<GET>(
        "/api/v1/logging",
        [this](coro_http_request &req, coro_http_response &resp) {
            handle_get_logging(req, resp);
        });
    http_server_->set_http_handler<PUT>(
        "/api/v1/logging",
        [this](coro_http_request &req, coro_http_response &resp) {
            handle_update_logging(req, resp);
        });

    auto ec = http_server_->async_start();
    if (ec.hasResult()) {
        LOG(WARNING) << "Failed to start HTTP server on port " << port;
        http_server_.reset();
        return -1;
    }
    LOG(INFO) << "Client HTTP server started on port " << http_server_->port();
    return 0;
}

void RealClient::handle_get_logging(
    coro_http::coro_http_request&, coro_http::coro_http_response& resp) {
    HttpLoggingResponse response;
    FillLoggingResponse(&response);
    std::string body;
    struct_json::to_json(response, body);
    resp.add_header("Content-Type", "application/json; charset=utf-8");
    resp.set_status_and_content(coro_http::status_type::ok, std::move(body));
}

void RealClient::handle_update_logging(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    HttpLoggingUpdateRequest request;
    try {
        struct_json::from_json(request, req.get_body());
    } catch (const std::exception& e) {
        resp.set_status_and_content(
            coro_http::status_type::bad_request,
            std::string("Invalid JSON body: ") + e.what());
        return;
    }

    RuntimeLogConfigUpdate update{
        .minloglevel = request.minloglevel,
        .vlog_level = request.vlog_level,
        .ylt_severity = request.ylt_severity,
        .dfs_read_trace = request.dfs_read_trace,
    };
    std::string error;
    if (!UpdateRuntimeLogConfig(update, &error)) {
        resp.set_status_and_content(coro_http::status_type::bad_request,
                                    std::move(error));
        return;
    }

    HttpLoggingResponse response;
    FillLoggingResponse(&response);
    std::string body;
    struct_json::to_json(response, body);
    resp.add_header("Content-Type", "application/json; charset=utf-8");
    resp.set_status_and_content(coro_http::status_type::ok, std::move(body));
}

void RealClient::stop_http_server() {
    if (http_server_) {
        http_server_->stop();
        http_server_.reset();
        LOG(INFO) << "Client HTTP server stopped";
    }
}

tl::expected<void, ErrorCode> RealClient::put_internal(
    const std::string &key, std::span<const char> value,
    const ReplicateConfig &config,
    const std::shared_ptr<ClientBufferAllocator> &client_buffer_allocator) {
    if (config.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_buffer_allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto alloc_result = client_buffer_allocator->allocate(value.size_bytes());
    if (!alloc_result) {
        LOG(ERROR) << "Failed to allocate buffer for put operation, key: "
                   << key << ", value size: " << value.size();
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto &buffer_handle = *alloc_result;
    auto scatter_result = gather_maybe_device_to_host(
        buffer_handle.ptr(), value.data(), value.size_bytes(), "put:" + key);
    if (!scatter_result) {
        return tl::unexpected(scatter_result.error());
    }

    std::vector<Slice> slices = split_into_slices(buffer_handle);

    auto put_result = client_->Put(key, slices, config);
    if (!put_result) {
        return tl::unexpected(put_result.error());
    }

    return {};
}

tl::expected<void, ErrorCode> RealClient::put_dummy_helper(
    const std::string &key, std::span<const char> value,
    const ReplicateConfig &config, const UUID &client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto &context = it->second;

    return put_internal(key, value, config, context.client_buffer_allocator);
}

int RealClient::put(const std::string &key, std::span<const char> value,
                    const ReplicateConfig &config) {
    auto result = execute_timed_operation<tl::expected<void, ErrorCode>>(
        [&]() {
            return put_internal(key, value, config, client_buffer_allocator_);
        },
        [](const auto &ret) { return ret.has_value(); },
        [&](uint64_t latency_us, const auto &) {
            client_->ObserveTransferOperation(TransferOperationKind::kWrite,
                                              "put", value.size_bytes(),
                                              latency_us);
        });
    return to_py_ret(result);
}

tl::expected<void, ErrorCode> RealClient::put_batch_internal(
    const std::vector<std::string> &keys,
    const std::vector<std::span<const char>> &values,
    const ReplicateConfig &config,
    const std::shared_ptr<ClientBufferAllocator> &client_buffer_allocator) {
    if (config.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (keys.size() != values.size()) {
        LOG(ERROR) << "Key and value size mismatch";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_buffer_allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::vector<BufferHandle> buffer_handles;
    std::unordered_map<std::string, std::vector<Slice>> batched_slices;
    batched_slices.reserve(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        auto &key = keys[i];
        auto &value = values[i];
        auto alloc_result =
            client_buffer_allocator->allocate(value.size_bytes());
        if (!alloc_result) {
            LOG(ERROR)
                << "Failed to allocate buffer for put_batch operation, key: "
                << key << ", value size: " << value.size();
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto &buffer_handle = *alloc_result;
        auto scatter_result =
            gather_maybe_device_to_host(buffer_handle.ptr(), value.data(),
                                        value.size_bytes(), "put_batch:" + key);
        if (!scatter_result) {
            return tl::unexpected(scatter_result.error());
        }
        auto slices = split_into_slices(buffer_handle);
        buffer_handles.emplace_back(std::move(*alloc_result));
        batched_slices.emplace(key, std::move(slices));
    }

    // Convert unordered_map to vector format expected by BatchPut
    std::vector<std::vector<mooncake::Slice>> ordered_batched_slices;
    ordered_batched_slices.reserve(keys.size());
    for (const auto &key : keys) {
        auto it = batched_slices.find(key);
        if (it != batched_slices.end()) {
            ordered_batched_slices.emplace_back(it->second);
        } else {
            LOG(ERROR) << "Missing slices for key: " << key;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    auto results = client_->BatchPut(keys, ordered_batched_slices, config);

    // Check if any operations failed
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i]) {
            return tl::unexpected(results[i].error());
        }
    }
    return {};
}

tl::expected<void, ErrorCode> RealClient::put_batch_dummy_helper(
    const std::vector<std::string> &keys,
    const std::vector<std::span<const char>> &values,
    const ReplicateConfig &config, const UUID &client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto &context = it->second;

    return put_batch_internal(keys, values, config,
                              context.client_buffer_allocator);
}

int RealClient::put_batch(const std::vector<std::string> &keys,
                          const std::vector<std::span<const char>> &values,
                          const ReplicateConfig &config) {
    auto result = execute_timed_operation<tl::expected<void, ErrorCode>>(
        [&]() {
            return put_batch_internal(keys, values, config,
                                      client_buffer_allocator_);
        },
        [](const auto &ret) { return ret.has_value(); },
        [&](uint64_t latency_us, const auto &) {
            client_->ObserveTransferOperation(
                TransferOperationKind::kWrite, "put_batch",
                sum_value_sizes(values), latency_us);
        });
    return to_py_ret(result);
}

tl::expected<void, ErrorCode> RealClient::put_parts_internal(
    const std::string &key, const std::vector<std::span<const char>> &values,
    const ReplicateConfig &config,
    const std::shared_ptr<ClientBufferAllocator> &client_buffer_allocator) {
    if (config.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_buffer_allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Calculate total size needed
    size_t total_size = 0;
    for (const auto &value : values) {
        total_size += value.size_bytes();
    }

    if (total_size == 0) {
        LOG(WARNING) << "Attempting to put empty data for key: " << key;
        return {};
    }

    // Allocate buffer using the new allocator
    auto alloc_result = client_buffer_allocator->allocate(total_size);
    if (!alloc_result) {
        LOG(ERROR) << "Failed to allocate buffer for put_parts operation, key: "
                   << key << ", total size: " << total_size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto &buffer_handle = *alloc_result;

    // Copy all parts into the contiguous buffer
    size_t offset = 0;
    for (const auto &value : values) {
        auto scatter_result = gather_maybe_device_to_host(
            static_cast<char *>(buffer_handle.ptr()) + offset, value.data(),
            value.size_bytes(), "put_multi_value");
        if (!scatter_result) {
            return tl::unexpected(scatter_result.error());
        }
        offset += value.size_bytes();
    }

    // Split into slices
    std::vector<Slice> slices = split_into_slices(buffer_handle);

    // Perform the put operation - buffer_handle will be automatically released
    auto put_result = client_->Put(key, slices, config);
    if (!put_result) {
        LOG(ERROR) << "Put operation failed with error: "
                   << toString(put_result.error());
        return tl::unexpected(put_result.error());
    }

    return {};
}

tl::expected<void, ErrorCode> RealClient::put_parts_dummy_helper(
    const std::string &key, std::vector<std::span<const char>> values,
    const ReplicateConfig &config, const UUID &client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto &context = it->second;

    return put_parts_internal(key, values, config,
                              context.client_buffer_allocator);
}

int RealClient::put_parts(const std::string &key,
                          std::vector<std::span<const char>> values,
                          const ReplicateConfig &config) {
    auto result = execute_timed_operation<tl::expected<void, ErrorCode>>(
        [&]() {
            return put_parts_internal(key, values, config,
                                      client_buffer_allocator_);
        },
        [](const auto &ret) { return ret.has_value(); },
        [&](uint64_t latency_us, const auto &) {
            client_->ObserveTransferOperation(
                TransferOperationKind::kWrite, "put_parts",
                sum_value_sizes(values), latency_us);
        });
    return to_py_ret(result);
}

tl::expected<void, ErrorCode> RealClient::remove_internal(
    const std::string &key, bool force) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto remove_result = client_->Remove(key, force);
    if (!remove_result) {
        return tl::unexpected(remove_result.error());
    }
    return {};
}

int RealClient::remove(const std::string &key, bool force) {
    return to_py_ret(remove_internal(key, force));
}

tl::expected<long, ErrorCode> RealClient::removeByRegex_internal(
    const std::string &str, bool force) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return client_->RemoveByRegex(str, force);
}

long RealClient::removeByRegex(const std::string &str, bool force) {
    return to_py_ret(removeByRegex_internal(str, force));
}

tl::expected<int64_t, ErrorCode> RealClient::removeAll_internal(bool force) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return client_->RemoveAll(force);
}

long RealClient::removeAll(bool force) {
    return to_py_ret(removeAll_internal(force));
}

std::vector<tl::expected<void, ErrorCode>> RealClient::batchRemove_internal(
    const std::vector<std::string> &keys, bool force) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    return client_->BatchRemove(keys, force);
}

std::vector<int> RealClient::batchRemove(const std::vector<std::string> &keys,
                                         bool force) {
    auto results = batchRemove_internal(keys, force);
    std::vector<int> ret;
    ret.reserve(results.size());

    for (const auto &item : results) {
        if (item.has_value()) {
            ret.push_back(0);
        } else {
            ret.push_back(toInt(item.error()));
        }
    }
    return ret;
}

tl::expected<bool, ErrorCode> RealClient::isExist_internal(
    const std::string &key) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (client_->DfsReadAllowed()) return client_->IsExist(key);
    auto results = batchIsExist_internal({key});
    if (results.size() != 1) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return std::move(results.front());
}

int RealClient::isExist(const std::string &key) {
    auto result = isExist_internal(key);

    if (result.has_value()) {
        return *result ? 1 : 0;  // 1 if exists, 0 if not
    } else {
        return toInt(result.error());
    }
}

std::vector<int> RealClient::batchIsExist(
    const std::vector<std::string> &keys) {
    auto internal_results = batchIsExist_internal(keys);
    std::vector<int> results;
    results.reserve(internal_results.size());

    for (const auto &result : internal_results) {
        if (result.has_value()) {
            results.push_back(result.value() ? 1 : 0);  // 1 if exists, 0 if not
        } else {
            results.push_back(toInt(result.error()));
        }
    }

    return results;
}

tl::expected<int64_t, ErrorCode> RealClient::getSize_internal(
    const std::string &key) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto query_result = client_->Query(key);

    if (!query_result) {
        return tl::unexpected(query_result.error());
    }

    const std::vector<Replica::Descriptor> &replica_list =
        query_result.value().replicas;

    // Calculate total size from all replicas' handles
    int64_t total_size = 0;
    if (!replica_list.empty()) {
        auto &replica = replica_list[0];
        total_size = calculate_total_size(replica);
    } else {
        LOG(ERROR) << "Internal error: replica_list is empty";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);  // Internal error
    }

    return total_size;
}

int64_t RealClient::getSize(const std::string &key) {
    return to_py_ret(getSize_internal(key));
}

tl::expected<void, ErrorCode> RealClient::map_shm_internal(
    int fd, uint64_t dummy_base_addr, size_t shm_size, bool is_local_buffer,
    const UUID &client_id) {
    return map_shm_internal_with_device(fd, dummy_base_addr, shm_size,
                                        is_local_buffer,
                                        kInvalidPhysicalDeviceId, client_id);
}

tl::expected<void, ErrorCode> RealClient::map_shm_internal_with_device(
    int fd, uint64_t dummy_base_addr, size_t shm_size, bool is_local_buffer,
    int32_t physical_device_id, const UUID &client_id) {
    std::stringstream addr_stream;
    addr_stream << "0x" << std::hex << dummy_base_addr;

    std::string shm_name =
        std::string(MOONCAKE_SHM_NAME) + "_" + std::to_string(client_id.first) +
        "_" + std::to_string(client_id.second) + "_" + addr_stream.str();
    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);

    // Check if this shm is already mapped
    auto context_it = shm_contexts_.find(client_id);
    if (context_it != shm_contexts_.end()) {
        for (const auto &shm : context_it->second.mapped_shms) {
            if (shm.dummy_base_addr ==
                static_cast<uintptr_t>(dummy_base_addr)) {
                LOG(INFO) << "Segment already mapped: " << shm_name;
                if (fd >= 0) close(fd);
                return {};
            }
        }
    }

    if (fd < 0) {
        LOG(ERROR) << "Invalid file descriptor: " << fd;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

#ifdef USE_ASCEND_DIRECT
    auto context_result = set_context_if_needed(protocol, physical_device_id,
                                                "POSIX shm registration");
    if (!context_result) {
        close(fd);
        return context_result;
    }
#endif

    // Map shared memory from FD
    void *shm_buffer =
        mmap(nullptr, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (shm_buffer == MAP_FAILED) {
        LOG(ERROR) << "Failed to map shared memory from fd: " << fd
                   << ", name: " << shm_name << ", error: " << strerror(errno);
        close(fd);
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    close(fd);  // Close FD after mapping

    MappedShm shm{};
    shm.shm_name = shm_name;
    shm.shm_buffer = shm_buffer;
    shm.shm_size = shm_size;
    shm.dummy_base_addr = static_cast<uintptr_t>(dummy_base_addr);
    shm.shm_addr_offset = reinterpret_cast<uintptr_t>(shm_buffer) -
                          reinterpret_cast<uintptr_t>(dummy_base_addr);
    shm.is_ascend =
        false;  // memfd/POSIX path; teardown differs from Ascend VMM/IPC
    shm.device_id = physical_device_id;

    auto &context = shm_contexts_[client_id];

    if (shm_size > 0) {
        auto result = client_->RegisterLocalMemory(
            shm.shm_buffer, shm_size, kWildcardLocation, false, true);
        if (!result.has_value()) {
            LOG(ERROR) << "Failed to register memory";
            munmap(shm_buffer, shm_size);
            return tl::unexpected(result.error());
        }
    }

    if (is_local_buffer) {
        if (context.client_buffer_allocator) {
            LOG(ERROR) << "A local buffer is already mapped for this "
                          "client shared memory.";
            munmap(shm_buffer, shm_size);
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
        context.client_buffer_allocator =
            ClientBufferAllocator::create(shm_buffer, shm_size, this->protocol);
    }

    context.mapped_shms.push_back(std::move(shm));

    LOG(INFO) << "Mapped new shared memory: " << shm_name
              << ", size: " << shm_size;
    return {};
}

tl::expected<void, ErrorCode> RealClient::unmap_shm_internal(
    const UUID &client_id) {
    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto &context = it->second;
    context.client_buffer_allocator.reset();

    for (auto &shm : context.mapped_shms) {
        if (shm.shm_buffer) {
            auto rc = client_->unregisterLocalMemory(shm.shm_buffer, true);
            if (!rc) {
                LOG(ERROR) << "Failed to unregister memory";
                munmap(shm.shm_buffer, shm.shm_size);
                context.mapped_shms.clear();
                shm_contexts_.erase(it);
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            munmap(shm.shm_buffer, shm.shm_size);
        }
    }
    context.mapped_shms.clear();
    shm_contexts_.erase(it);
    return {};
}

tl::expected<void, ErrorCode> RealClient::ascend_shm_internal(
    uint64_t dummy_base_addr, size_t vmm_size, bool is_local_buffer,
    const std::string &shareable_handle_bytes, int32_t device_id,
    const UUID &client_id) {
#ifdef USE_ASCEND_DIRECT
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (shareable_handle_bytes.size() != sizeof(aclrtMemFabricHandle)) {
        LOG(ERROR) << "Invalid fabric handle bytes size: "
                   << shareable_handle_bytes.size();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto &context = shm_contexts_[client_id];
    for (const auto &region : context.mapped_shms) {
        if (region.dummy_base_addr == static_cast<uintptr_t>(dummy_base_addr)) {
            LOG(INFO) << "VMM region already mapped for dummy address";
            return {};
        }
    }

    if (!ContextManager::getInstance().setCurrentContextByPhysicalId(
            device_id)) {
        LOG(ERROR) << "Failed to set current context for physical device "
                   << device_id;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    aclrtMemFabricHandle export_handle = {};
    memcpy(&export_handle, shareable_handle_bytes.data(),
           sizeof(export_handle));
    aclrtDrvMemHandle imported_handle = nullptr;
    if (!checkAcl(aclrtMemImportFromShareableHandleV2(
                      &export_handle, ACL_MEM_SHARE_HANDLE_TYPE_FABRIC,
                      ACL_RT_VMM_EXPORT_FLAG_DEFAULT, &imported_handle),
                  "Failed to import shareable handle")) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    void *mapped_va = nullptr;
    if (!checkAcl(aclrtReserveMemAddress(&mapped_va, vmm_size, 0, nullptr, 1),
                  "Failed to reserve VMM address")) {
        (void)aclrtFreePhysical(imported_handle);
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (!checkAcl(aclrtMapMem(mapped_va, vmm_size, 0, imported_handle, 0),
                  "Failed to map imported VMM handle")) {
        (void)aclrtReleaseMemAddress(mapped_va);
        (void)aclrtFreePhysical(imported_handle);
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    MappedShm region;
    region.shm_name = "vmm_" + std::to_string(client_id.first) + "_" +
                      std::to_string(client_id.second);
    region.shm_buffer = mapped_va;
    region.shm_size = vmm_size;
    region.dummy_base_addr = static_cast<uintptr_t>(dummy_base_addr);
    region.shm_addr_offset = reinterpret_cast<uintptr_t>(mapped_va) -
                             reinterpret_cast<uintptr_t>(dummy_base_addr);
    region.is_ascend = true;
    region.is_ipc = false;
    region.vmm_handle = reinterpret_cast<uint64_t>(imported_handle);
    region.device_id = device_id;

    if (is_local_buffer) {
        if (context.client_buffer_allocator) {
            (void)aclrtUnmapMem(mapped_va);
            (void)aclrtReleaseMemAddress(mapped_va);
            (void)aclrtFreePhysical(imported_handle);
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
        context.client_buffer_allocator =
            ClientBufferAllocator::create(mapped_va, vmm_size, this->protocol);
    }
    context.mapped_shms.push_back(std::move(region));
#endif
    return {};
}

tl::expected<void, ErrorCode> RealClient::ascend_ipc_shm_internal(
    uint64_t dummy_base_addr, size_t mem_size, bool is_local_buffer,
    const std::string &ipc_key_bytes, int32_t device_id,
    const UUID &client_id) {
#ifdef USE_ASCEND_DIRECT
    constexpr size_t kIPCKeyLen = 65;
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (ipc_key_bytes.size() != kIPCKeyLen) {
        LOG(ERROR) << "Invalid IPC key size: " << ipc_key_bytes.size();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto &context = shm_contexts_[client_id];
    for (const auto &region : context.mapped_shms) {
        if (region.dummy_base_addr == static_cast<uintptr_t>(dummy_base_addr)) {
            LOG(INFO) << "IPC region already mapped for dummy address";
            return {};
        }
    }

    if (!ContextManager::getInstance().setCurrentContextByPhysicalId(
            device_id)) {
        LOG(ERROR) << "Failed to set current context for physical device "
                   << device_id;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    void *mapped_va = nullptr;
    auto ret =
        aclrtIpcMemImportByKey(&mapped_va, ipc_key_bytes.data(),
                               ACL_RT_IPC_MEM_IMPORT_FLAG_ENABLE_PEER_ACCESS);
    if (ret != ACL_ERROR_NONE) {
        LOG(ERROR) << "aclrtIpcMemImportByKey failed, ret=" << ret
                   << ", errmsg: " << aclGetRecentErrMsg();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (!globalConfig().ascend_use_fabric_mem) {
        auto result = client_->RegisterLocalMemory(mapped_va, mem_size, "npu",
                                                   false, true);
        if (!result.has_value()) {
            LOG(ERROR) << "Failed to register memory";
            (void)aclrtIpcMemClose(ipc_key_bytes.data());
            return tl::unexpected(result.error());
        }
        LOG(INFO) << "RegisterLocalMemory for ipc shm, va:" << mapped_va;
    }

    MappedShm region;
    region.shm_name = "ipc_" + std::to_string(client_id.first) + "_" +
                      std::to_string(client_id.second);
    region.shm_buffer = mapped_va;
    region.shm_size = mem_size;
    region.dummy_base_addr = static_cast<uintptr_t>(dummy_base_addr);
    region.shm_addr_offset = reinterpret_cast<uintptr_t>(mapped_va) -
                             static_cast<uintptr_t>(dummy_base_addr);
    region.is_ascend = true;
    region.is_ipc = true;
    region.ipc_key_data = ipc_key_bytes;
    region.device_id = device_id;

    if (is_local_buffer) {
        if (context.client_buffer_allocator) {
            (void)aclrtIpcMemClose(ipc_key_bytes.data());
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
        context.client_buffer_allocator =
            ClientBufferAllocator::create(mapped_va, mem_size, this->protocol);
    }

    context.mapped_shms.push_back(std::move(region));
    LOG(INFO) << "IPC memory mapped: dummy_addr=0x" << std::hex
              << dummy_base_addr << ", real_addr=" << mapped_va << std::dec
              << ", size=" << mem_size << ", device_id=" << device_id;
#endif
    return {};
}

void RealClient::teardown_ascend_shm_buffer(MappedShm &shm) {
#ifdef USE_ASCEND_DIRECT
    (void)ContextManager::getInstance().setCurrentContextByPhysicalId(
        shm.device_id);
    if (shm.shm_size > 0 && client_) {
        auto res = client_->unregisterLocalMemory(shm.shm_buffer, true);
        if (!res) {
            LOG(WARNING)
                << "Failed to unregister local memory for shared memory: "
                << shm.shm_name << ", error: " << toString(res.error());
        }
    }
    if (shm.is_ipc) {
        if (!shm.ipc_key_data.empty()) {
            (void)aclrtIpcMemClose(shm.ipc_key_data.data());
        }
        LOG(INFO) << "Free ipc mem suc.";
        shm.ipc_key_data.clear();
    } else {
        aclrtUnmapMem(shm.shm_buffer);
        aclrtReleaseMemAddress(shm.shm_buffer);
        if (shm.vmm_handle != 0) {
            aclrtFreePhysical(
                reinterpret_cast<aclrtDrvMemHandle>(shm.vmm_handle));
        }
        shm.vmm_handle = 0;
        LOG(INFO) << "Free vmm mem suc.";
    }
#endif
}

tl::expected<void, ErrorCode> RealClient::ascend_unmap_shm_internal(
    const UUID &client_id) {
    LOG(INFO) << "[ascend_unmap_shm] client_id=" << client_id
              << ", action=unmapping";
    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "[ascend_unmap_shm] client_id=" << client_id
                   << ", error=shm_not_mapped";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto &context = it->second;
    context.client_buffer_allocator.reset();

    // Move regions out before cleanup so failure paths cannot erase the map
    // entry while iterating (undefined behavior) or double-erase at the end.
    std::vector<MappedShm> shms = std::move(context.mapped_shms);
    context.mapped_shms.clear();

    for (auto &shm : shms) {
        if (!shm.shm_buffer) {
            continue;
        }
        if (shm.is_ascend) {
            teardown_ascend_shm_buffer(shm);
        } else {
#ifdef USE_ASCEND_DIRECT
            auto context_result = set_context_if_needed(protocol, shm.device_id,
                                                        "POSIX shm cleanup");
            if (!context_result) {
                LOG(WARNING) << "Skip unregisterLocalMemory for POSIX shm: "
                             << shm.shm_name
                             << ", error: " << toString(context_result.error());
            } else
#endif
                // Host POSIX shm mapped via map_shm_internal (memfd); not
                // Ascend VMM/IPC.
                if (shm.shm_size > 0 && client_) {
                    auto res =
                        client_->unregisterLocalMemory(shm.shm_buffer, true);
                    if (!res) {
                        LOG(WARNING) << "Failed to unregister local memory for "
                                        "POSIX shm: "
                                     << shm.shm_name
                                     << ", error: " << toString(res.error());
                    }
                }
            if (munmap(shm.shm_buffer, shm.shm_size) != 0) {
                LOG(ERROR) << "Failed to munmap POSIX shared memory: "
                           << shm.shm_name << ", error: " << strerror(errno);
            } else {
                LOG(INFO) << "Unmapped POSIX shared memory: " << shm.shm_name
                          << ", size: " << shm.shm_size;
            }
        }
    }
    shm_contexts_.erase(it);
    return {};
}

tl::expected<bool, ErrorCode> RealClient::is_shm_mapped_internal(
    uint64_t dummy_base_addr, const UUID &client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto context_it = shm_contexts_.find(client_id);
    if (context_it == shm_contexts_.end()) {
        return false;
    }

    const auto target_addr = static_cast<uintptr_t>(dummy_base_addr);
    const auto &mapped_shms = context_it->second.mapped_shms;
    return std::any_of(mapped_shms.begin(), mapped_shms.end(),
                       [target_addr](const MappedShm &shm) {
                           return shm.dummy_base_addr == target_addr;
                       });
}

tl::expected<void, ErrorCode> RealClient::unregister_shm_buffer_internal(
    uint64_t dummy_base_addr, const UUID &client_id) {
    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto &context = it->second;

    // Find the shm corresponding to this dummy address
    auto shm_it = context.mapped_shms.end();
    for (auto sit = context.mapped_shms.begin();
         sit != context.mapped_shms.end(); ++sit) {
        if (dummy_base_addr == sit->dummy_base_addr) {
            shm_it = sit;
            break;
        }
    }

    if (shm_it == context.mapped_shms.end()) {
        std::stringstream addr_stream;
        addr_stream << "0x" << std::hex << dummy_base_addr;
        LOG(ERROR) << "Share memory not found for dummy address: "
                   << addr_stream.str() << " (client_id: " << client_id << ")";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Unmap and clean up the shm
    if (shm_it->shm_buffer) {
        if (shm_it->is_ascend) {
            teardown_ascend_shm_buffer(*shm_it);
        } else {
#ifdef USE_ASCEND_DIRECT
            auto context_result = set_context_if_needed(
                protocol, shm_it->device_id, "POSIX shm unregister");
            if (!context_result) {
                return context_result;
            }
#endif
            auto rc = client_->unregisterLocalMemory(shm_it->shm_buffer, true);
            if (!rc) {
                LOG(ERROR) << "Failed to unregister memory: "
                           << shm_it->shm_name;
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            if (munmap(shm_it->shm_buffer, shm_it->shm_size) != 0) {
                LOG(ERROR) << "Failed to munmap shared memory: "
                           << shm_it->shm_name
                           << ", error: " << strerror(errno);
            } else {
                LOG(INFO) << "Unmapped and cleaned up shared memory: "
                          << shm_it->shm_name << ", size: " << shm_it->shm_size;
            }
        }
    }

    // Remove shm from list
    context.mapped_shms.erase(shm_it);

    return {};
}

// Implementation of get_buffer_internal method
std::shared_ptr<BufferHandle> RealClient::get_buffer_internal(
    const std::string &key,
    const std::shared_ptr<ClientBufferAllocator> &client_buffer_allocator) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return nullptr;
    }
    if (!client_buffer_allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return nullptr;
    }

    // Query the object info
    auto query_result = client_->Query(key);
    if (!query_result) {
        if (query_result.error() == ErrorCode::OBJECT_NOT_FOUND ||
            query_result.error() == ErrorCode::REPLICA_IS_NOT_READY) {
            return nullptr;
        }
        LOG(ERROR) << "Query failed for key: " << key
                   << " with error: " << toString(query_result.error());
        return nullptr;
    }

    const std::vector<Replica::Descriptor> &replica_list =
        query_result.value().replicas;
    if (replica_list.empty()) {
        LOG(ERROR) << "Empty replica list for key: " << key;
        return nullptr;
    }

    // Select best replica: prefer local MEMORY, any MEMORY, local NOF, any NOF,
    // LOCAL_DISK, DFS, then DISK.
    // LOCAL_DISK data is on a remote node's SSD — must use offload RPC.
    // MEMORY / DISK / DFS are handled via client_->Get below.
    auto local_endpoints = client_->GetLocalEndpoints();
    const auto *best_replica = SelectBestReplica(replica_list, local_endpoints);
    if (!best_replica) {
        LOG(ERROR) << "No usable replica for key: " << key;
        return nullptr;
    }

    const auto &replica = *best_replica;
    uint64_t total_length = calculate_total_size(replica);

    if (total_length == 0) {
        return nullptr;
    }

    // Allocate buffer
    auto alloc_result = client_buffer_allocator->allocate(total_length);
    if (!alloc_result) {
        LOG(ERROR) << "Failed to allocate buffer for get_buffer, key: " << key;
        return nullptr;
    }

    auto buffer_handle =
        std::make_shared<BufferHandle>(std::move(*alloc_result));

    if (best_replica->is_local_disk_replica()) {
        // LOCAL_DISK: data is on remote node's SSD. Use offload RPC.
        const auto &endpoint =
            best_replica->get_local_disk_descriptor().transport_endpoint;
        std::unordered_map<std::string, std::vector<Slice>> objects;
        objects.emplace(
            key, std::vector<Slice>{{buffer_handle->ptr(), total_length}});
        auto read_result =
            batch_get_into_offload_object_internal(endpoint, objects);
        if (!read_result) {
            LOG(ERROR) << "SSD read failed for key '" << key
                       << "': " << toString(read_result.error());
            return nullptr;
        }
        auto checksum_result =
            client_->VerifyObjectChecksum(key, objects.at(key), total_length,
                                          query_result.value().object_checksum);
        if (!checksum_result) {
            LOG(ERROR) << "SSD checksum verification failed for key '" << key
                       << "': " << toString(checksum_result.error());
            return nullptr;
        }
        return buffer_handle;
    }

    // MEMORY / DISK / DFS: use client_->Get. FilterQueryResult ensures
    // Client::Get internal FindFirstCompleteReplica can only see
    // the replica we selected, preventing accidental LOCAL_DISK picks.
    auto runtime_accelerator =
        device::GetAcceleratorRegistry().RuntimeAccelerators();
    if ((replica.is_disk_replica() || replica.is_dfs_replica()) &&
        runtime_accelerator.FindDeviceForPointer(buffer_handle->ptr())) {
        LOG(WARNING) << (replica.is_dfs_replica() ? "DFS" : "DISK")
                     << " replica for key '" << key
                     << "' received a device pointer from the allocator; "
                     << "file I/O cannot write to GPU memory — read will fail. "
                     << "Ensure client_buffer_allocator_ returns host memory.";
    }

    std::vector<Slice> slices;
    allocateSlices(slices, replica, buffer_handle->ptr());
    auto filtered_qr = FilterQueryResult(query_result.value(), replica);
    auto get_result = client_->Get(key, filtered_qr, slices);
    if (!get_result) {
        LOG(ERROR) << "Get failed for key: " << key
                   << " with error: " << toString(get_result.error());
        return nullptr;
    }

    return buffer_handle;
}

// Implementation of get_buffer method
std::shared_ptr<BufferHandle> RealClient::get_buffer(const std::string &key) {
    return execute_timed_operation<std::shared_ptr<BufferHandle>>(
        [&]() { return get_buffer_internal(key, client_buffer_allocator_); },
        [](const auto &buffer) { return buffer != nullptr; },
        [&](uint64_t latency_us, const auto &buffer) {
            client_->ObserveTransferOperation(TransferOperationKind::kRead,
                                              "get_buffer", buffer->size(),
                                              latency_us);
        });
}

tl::expected<std::tuple<uint64_t, size_t>, ErrorCode>
RealClient::acquire_hot_cache(const std::string &key) {
    if (!client_ || !client_->IsHotCacheEnabled()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto hot_cache = client_->GetHotCache();
    HotMemBlock *blk = hot_cache->GetHotKey(key);
    if (!blk) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    size_t offset = hot_cache->GetBlockOffset(blk->addr);
    if (offset == SIZE_MAX) {
        hot_cache->ReleaseHotKey(key);
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return std::make_tuple(static_cast<uint64_t>(offset), blk->size);
}

tl::expected<void, ErrorCode> RealClient::release_hot_cache(
    const std::string &key) {
    if (!client_ || !client_->IsHotCacheEnabled()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    client_->GetHotCache()->ReleaseHotKey(key);
    return {};
}

tl::expected<std::tuple<uint64_t, size_t>, ErrorCode>
RealClient::acquire_buffer_dummy(const std::string &key,
                                 const UUID &client_id) {
    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto &context = it->second;

    auto buffer_handle =
        get_buffer_internal(key, context.client_buffer_allocator);
    if (!buffer_handle) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    uint64_t real_addr = reinterpret_cast<uint64_t>(buffer_handle->ptr());
    size_t buf_size = buffer_handle->size();

    for (const auto &shm : context.mapped_shms) {
        uint64_t shm_start = reinterpret_cast<uint64_t>(shm.shm_buffer);
        uint64_t shm_end = shm_start + shm.shm_size;
        if (real_addr >= shm_start && real_addr < shm_end) {
            uint64_t dummy_addr = real_addr - shm.shm_addr_offset;
            context.active_handles[dummy_addr] = std::move(buffer_handle);
            return std::make_tuple(dummy_addr, buf_size);
        }
    }

    return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
}

tl::expected<void, ErrorCode> RealClient::release_buffer_dummy(
    uint64_t dummy_addr, const UUID &client_id) {
    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    it->second.active_handles.erase(dummy_addr);
    return {};
}

tl::expected<std::tuple<uint64_t, size_t>, ErrorCode>
RealClient::allocate_buffer_dummy(size_t size, const UUID &client_id) {
    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto &context = it->second;
    if (!context.client_buffer_allocator) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto alloc_result = context.client_buffer_allocator->allocate(size);
    if (!alloc_result) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    auto handle = std::make_shared<BufferHandle>(std::move(*alloc_result));
    const uint64_t real_addr = reinterpret_cast<uint64_t>(handle->ptr());
    const size_t allocated_size = handle->size();
    for (const auto &shm : context.mapped_shms) {
        const uint64_t shm_start = reinterpret_cast<uint64_t>(shm.shm_buffer);
        const uint64_t shm_end = shm_start + shm.shm_size;
        if (real_addr >= shm_start && allocated_size <= shm_end - real_addr) {
            const uint64_t dummy_addr = real_addr - shm.shm_addr_offset;
            context.active_handles[dummy_addr] = std::move(handle);
            return std::make_tuple(dummy_addr, allocated_size);
        }
    }

    return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
}

std::vector<tl::expected<std::tuple<uint64_t, size_t>, ErrorCode>>
RealClient::batch_acquire_hot_cache(const std::vector<std::string> &keys) {
    std::vector<tl::expected<std::tuple<uint64_t, size_t>, ErrorCode>> results;
    results.reserve(keys.size());

    if (!client_ || !client_->IsHotCacheEnabled()) {
        for (size_t i = 0; i < keys.size(); ++i) {
            results.push_back(tl::make_unexpected(ErrorCode::INVALID_PARAMS));
        }
        return results;
    }

    auto hot_cache = client_->GetHotCache();
    for (const auto &key : keys) {
        HotMemBlock *blk = hot_cache->GetHotKey(key);
        if (!blk) {
            results.push_back(tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
            continue;
        }
        size_t offset = hot_cache->GetBlockOffset(blk->addr);
        if (offset == SIZE_MAX) {
            hot_cache->ReleaseHotKey(key);
            results.push_back(tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
            continue;
        }
        results.push_back(
            std::make_tuple(static_cast<uint64_t>(offset), blk->size));
    }
    return results;
}

tl::expected<void, ErrorCode> RealClient::batch_release_hot_cache(
    const std::vector<std::string> &keys) {
    if (!client_ || !client_->IsHotCacheEnabled()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto hot_cache = client_->GetHotCache();
    for (const auto &key : keys) {
        hot_cache->ReleaseHotKey(key);
    }
    return {};
}

std::vector<tl::expected<std::tuple<uint64_t, size_t>, ErrorCode>>
RealClient::batch_acquire_buffer_dummy(const std::vector<std::string> &keys,
                                       const UUID &client_id) {
    std::vector<tl::expected<std::tuple<uint64_t, size_t>, ErrorCode>> results(
        keys.size(), tl::make_unexpected(ErrorCode::INTERNAL_ERROR));

    std::unique_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto ctx_it = shm_contexts_.find(client_id);
    if (ctx_it == shm_contexts_.end()) {
        for (auto &r : results)
            r = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        return results;
    }

    // Use batch_get_buffer_internal with dummy's allocator
    lock.unlock();
    auto handles =
        batch_get_buffer_internal(keys, ctx_it->second.client_buffer_allocator);
    lock.lock();

    // Re-validate context after re-lock
    ctx_it = shm_contexts_.find(client_id);
    if (ctx_it == shm_contexts_.end()) {
        for (auto &r : results)
            r = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        return results;
    }
    auto &ctx = ctx_it->second;

    for (size_t i = 0; i < keys.size(); ++i) {
        if (!handles[i]) {
            results[i] = tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            continue;
        }
        uint64_t real_addr = reinterpret_cast<uint64_t>(handles[i]->ptr());
        size_t buf_size = handles[i]->size();

        bool found = false;
        for (const auto &shm : ctx.mapped_shms) {
            uint64_t start = reinterpret_cast<uint64_t>(shm.shm_buffer);
            if (real_addr >= start && real_addr < start + shm.shm_size) {
                uint64_t dummy_addr = real_addr - shm.shm_addr_offset;
                ctx.active_handles[dummy_addr] = std::move(handles[i]);
                results[i] = std::make_tuple(dummy_addr, buf_size);
                found = true;
                break;
            }
        }
        if (!found) {
            results[i] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }
    return results;
}

// Implementation of batch_get_buffer_internal method
std::vector<std::shared_ptr<BufferHandle>>
RealClient::batch_get_buffer_internal(
    const std::vector<std::string> &keys,
    const std::shared_ptr<ClientBufferAllocator> &client_buffer_allocator) {
    std::vector<std::shared_ptr<BufferHandle>> final_results(keys.size(),
                                                             nullptr);

    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return final_results;
    }

    if (keys.empty()) {
        return final_results;
    }

    // 1. Query metadata for all keys
    auto query_results = client_->BatchQuery(keys);

    // 2. Prepare for batch get: filter valid keys and prepare buffers
    struct KeyOp {
        size_t original_index;
        std::string key;
        QueryResult query_result;
        std::unique_ptr<BufferHandle> buffer_handle;
        std::vector<Slice> slices;
    };
    struct DiskKeyOp {
        size_t original_index;
        std::string key;
        QueryResult query_result;
        std::unique_ptr<BufferHandle> buffer_handle;
        uint64_t total_size;
    };
    std::vector<KeyOp> valid_ops;
    std::vector<DiskKeyOp> disk_ops;
    valid_ops.reserve(keys.size());

    auto local_endpoints = client_->GetLocalEndpoints();
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto &key = keys[i];

        if (!query_results[i]) {
            if (query_results[i].error() != ErrorCode::OBJECT_NOT_FOUND &&
                query_results[i].error() != ErrorCode::REPLICA_IS_NOT_READY) {
                LOG(ERROR) << "Query failed for key '" << key
                           << "': " << toString(query_results[i].error());
            }
            continue;
        }

        auto query_result_values = query_results[i].value();
        if (query_result_values.replicas.empty()) {
            LOG(ERROR) << "Empty replica list for key: " << key;
            continue;
        }

        // Select best replica: prefer local MEMORY, any MEMORY, local NOF,
        // any NOF, LOCAL_DISK, DFS, then DISK.
        const auto *best_replica =
            SelectBestReplica(query_result_values.replicas, local_endpoints);
        if (!best_replica) {
            LOG(ERROR) << "No usable replica for key: " << key;
            continue;
        }
        const auto replica = *best_replica;
        uint64_t total_size = calculate_total_size(replica);
        if (total_size == 0) {
            continue;
        }

        auto &allocator = client_buffer_allocator ? client_buffer_allocator
                                                  : client_buffer_allocator_;
        auto alloc_result = allocator->allocate(total_size);
        if (!alloc_result) {
            LOG(ERROR) << "Failed to allocate buffer for key: " << key;
            continue;
        }

        auto buffer_handle =
            std::make_unique<BufferHandle>(std::move(*alloc_result));
        std::vector<Slice> slices;
        allocateSlices(slices, replica, buffer_handle->ptr());

        if (replica.is_local_disk_replica()) {
            // LOCAL_DISK: buffer is allocated and registered via
            // client_buffer_allocator_, route to SSD RPC path below.
            disk_ops.emplace_back(DiskKeyOp{
                .original_index = i,
                .key = key,
                .query_result = FilterQueryResult(query_result_values, replica),
                .buffer_handle = std::move(buffer_handle),
                .total_size = total_size});
            continue;
        }

        // File-backed replicas use file I/O which can only write to
        // CPU-addressable memory. If the allocator ever returns device memory,
        // the read will fail.
        auto runtime_accelerator =
            device::GetAcceleratorRegistry().RuntimeAccelerators();
        if ((replica.is_disk_replica() || replica.is_dfs_replica()) &&
            runtime_accelerator.FindDeviceForPointer(buffer_handle->ptr())) {
            LOG(WARNING)
                << (replica.is_dfs_replica() ? "DFS" : "DISK")
                << " replica for key '" << key
                << "' received a device pointer from the allocator; "
                << "file I/O cannot write to GPU memory — read will fail. "
                << "Ensure client_buffer_allocator_ returns host memory.";
        }
        valid_ops.emplace_back(KeyOp{
            .original_index = i,
            .key = key,
            .query_result = FilterQueryResult(query_result_values, replica),
            .buffer_handle = std::move(buffer_handle),
            .slices = std::move(slices)});
    }

    if (valid_ops.empty() && disk_ops.empty()) {
        return final_results;
    }

    // 3. Execute batch get for memory/disk replicas
    if (!valid_ops.empty()) {
        std::vector<std::string> batch_keys;
        std::vector<QueryResult> batch_query_results;
        std::unordered_map<std::string, std::vector<Slice>> batch_slices;
        batch_keys.reserve(valid_ops.size());
        batch_query_results.reserve(valid_ops.size());

        for (auto &op : valid_ops) {
            batch_keys.push_back(op.key);
            batch_query_results.push_back(op.query_result);
            batch_slices[op.key] = op.slices;
        }

        auto batch_get_results =
            client_->BatchGet(batch_keys, batch_query_results, batch_slices);

        // 4. Process results and create BufferHandles
        for (size_t i = 0; i < valid_ops.size(); ++i) {
            if (batch_get_results[i]) {
                auto &op = valid_ops[i];
                final_results[op.original_index] =
                    std::make_shared<BufferHandle>(
                        std::move(*op.buffer_handle));
            } else {
                LOG(ERROR) << "BatchGet failed for key '" << valid_ops[i].key
                           << "': " << toString(batch_get_results[i].error());
            }
        }
    }

    // 5. Execute batch get for LOCAL_DISK replicas via SSD RPC
    if (!disk_ops.empty()) {
        // Group by transport endpoint
        std::unordered_map<std::string,
                           std::unordered_map<std::string, std::vector<Slice>>>
            offload_objects;
        // Build key -> disk_ops index for result lookup
        std::unordered_map<std::string, size_t> disk_key_to_idx;

        for (size_t idx = 0; idx < disk_ops.size(); ++idx) {
            auto &op = disk_ops[idx];
            // Find the LOCAL_DISK replica — replicas may be in any order.
            const Replica::Descriptor *replica_ptr = nullptr;
            for (const auto &r : op.query_result.replicas) {
                if (r.is_local_disk_replica()) {
                    replica_ptr = &r;
                    break;
                }
            }
            if (!replica_ptr) {
                LOG(ERROR) << "No LOCAL_DISK replica found for key: " << op.key;
                continue;
            }
            const auto &replica = *replica_ptr;
            offload_objects[replica.get_local_disk_descriptor()
                                .transport_endpoint]
                .emplace(op.key, std::vector<Slice>{
                                     {op.buffer_handle->ptr(), op.total_size}});
            disk_key_to_idx[op.key] = idx;
        }

        for (auto &[endpoint, objects] : offload_objects) {
            if (objects.empty()) continue;
            auto read_result =
                batch_get_into_offload_object_internal(endpoint, objects);
            for (auto &[key, slices] : objects) {
                auto idx_it = disk_key_to_idx.find(key);
                if (idx_it == disk_key_to_idx.end()) continue;
                auto &op = disk_ops[idx_it->second];
                if (read_result) {
                    auto checksum_result = client_->VerifyObjectChecksum(
                        key, slices, op.total_size,
                        op.query_result.object_checksum);
                    if (!checksum_result) {
                        LOG(ERROR)
                            << "SSD checksum verification failed for key '"
                            << key
                            << "': " << toString(checksum_result.error());
                        continue;
                    }
                    final_results[op.original_index] =
                        std::make_shared<BufferHandle>(
                            std::move(*op.buffer_handle));
                } else {
                    LOG(ERROR) << "SSD read failed for key '" << key
                               << "': " << toString(read_result.error());
                }
            }
        }
    }

    return final_results;
}

// Implementation of batch_get_buffer method
std::vector<std::shared_ptr<BufferHandle>> RealClient::batch_get_buffer(
    const std::vector<std::string> &keys) {
    return execute_timed_operation<std::vector<std::shared_ptr<BufferHandle>>>(
        [&]() { return batch_get_buffer_internal(keys); },
        [](const auto &) { return true; },
        [&](uint64_t latency_us, const auto &buffers) {
            client_->ObserveTransferOperation(
                TransferOperationKind::kRead, "batch_get_buffer",
                sum_buffer_handle_sizes(buffers), latency_us);
        });
}

tl::expected<void, ErrorCode> RealClient::register_buffer_internal(
    void *buffer, size_t size) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto result = client_->RegisterLocalMemory(buffer, size, kWildcardLocation,
                                               false, true);
    if (!result) {
        return result;
    }
    {
        std::unique_lock<std::shared_mutex> lock(registered_buffer_mutex_);
        registered_buffer_sizes_[buffer] = size;
    }
    return result;
}

int RealClient::register_buffer(void *buffer, size_t size) {
    return to_py_ret(register_buffer_internal(buffer, size));
}

tl::expected<void, ErrorCode> RealClient::unregister_buffer_internal(
    void *buffer) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto unregister_result = client_->unregisterLocalMemory(buffer, true);
    if (!unregister_result) {
        LOG(ERROR) << "Unregister buffer failed with error: "
                   << toString(unregister_result.error());
        return tl::unexpected(unregister_result.error());
    }
    {
        std::unique_lock<std::shared_mutex> lock(registered_buffer_mutex_);
        registered_buffer_sizes_.erase(buffer);
    }
    return {};
}

int RealClient::unregister_buffer(void *buffer) {
    return to_py_ret(unregister_buffer_internal(buffer));
}

std::optional<RealClient::WritableBufferRegion>
RealClient::resolve_writable_buffer_region(void *buffer) const {
    std::shared_lock<std::shared_mutex> lock(registered_buffer_mutex_);
    const auto target = reinterpret_cast<uintptr_t>(buffer);
    for (const auto &[registered_buffer, registered_size] :
         registered_buffer_sizes_) {
        const auto base = reinterpret_cast<uintptr_t>(registered_buffer);
        if (target < base) {
            continue;
        }
        const auto offset = target - base;
        if (offset < registered_size) {
            return WritableBufferRegion{
                .base = registered_buffer,
                .size = registered_size,
                .offset = static_cast<size_t>(offset),
            };
        }
    }
    if (local_buffer_region_.has_value()) {
        const auto base =
            reinterpret_cast<uintptr_t>(local_buffer_region_->base);
        if (target >= base) {
            const auto offset = target - base;
            if (offset < local_buffer_region_->size) {
                WritableBufferRegion region = *local_buffer_region_;
                region.offset = static_cast<size_t>(offset);
                return region;
            }
        }
    }
    return std::nullopt;
}

tl::expected<RealClient::RangedReadMetadata, ErrorCode>
RealClient::resolve_ranged_read_metadata(
    const std::string &key, const QueryResultCache *query_result_cache) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (query_result_cache) {
        auto cached = query_result_cache->find(key);
        if (cached != query_result_cache->end() &&
            (!cached->second || !cached->second->IsLeaseExpired())) {
            return build_ranged_read_metadata_from_query_result(key,
                                                                cached->second);
        }
    }
    return build_ranged_read_metadata_from_query_result(key,
                                                        client_->Query(key));
}

tl::expected<int64_t, ErrorCode> RealClient::execute_ranged_read(
    const std::string &key, void *buffer, size_t dst_offset, size_t src_offset,
    size_t size, const RangedReadMetadata &metadata,
    bool size_is_buffer_capacity, bool verify_checksum) {
    const auto &query_result = metadata.query_result;
    const auto &replica = metadata.replica;
    const uint64_t total_size = metadata.total_size;

    if (size_is_buffer_capacity) {
        if (size < total_size) {
            LOG(ERROR) << "User buffer too small. Required: " << total_size
                       << ", provided: " << size;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        size = total_size;
    } else if (is_object_range_overflow(src_offset, size,
                                        static_cast<size_t>(total_size))) {
        LOG(ERROR) << "Range overflow: src_offset=" << src_offset
                   << " + size=" << size << " > total=" << total_size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (src_offset == 0 && size == total_size) {
        // LOCAL_DISK full-object read: destination buffer is passed
        // directly to SSD RPC (same pattern as single-buffer batch_get_into;
        // GPU buffers are pre-registered by vLLM via register_buffer).
        if (replica.is_local_disk_replica()) {
            const auto &endpoint =
                replica.get_local_disk_descriptor().transport_endpoint;
            std::unordered_map<std::string, std::vector<Slice>> objects;
            objects.emplace(
                key, std::vector<Slice>{
                         {static_cast<char *>(buffer) + dst_offset, size}});
            auto result =
                batch_get_into_offload_object_internal(endpoint, objects);
            if (!result) return tl::unexpected(result.error());
            if (verify_checksum) {
                auto checksum_result = client_->VerifyObjectChecksum(
                    key, objects.at(key), total_size,
                    query_result.object_checksum);
                if (!checksum_result) {
                    return tl::unexpected(checksum_result.error());
                }
            }
            return static_cast<int64_t>(total_size);
        }

        if (replica.is_disk_replica() || replica.is_dfs_replica()) {
            // File-backed full read: use a contiguous temp buffer, then
            // scatter to the requested destination.
            const char *replica_type =
                replica.is_dfs_replica() ? "DFS" : "DISK";
            auto alloc_result = client_buffer_allocator_->allocate(total_size);
            if (!alloc_result) {
                LOG(ERROR) << "Failed to allocate temp buffer for "
                           << replica_type << " full read, key: " << key
                           << ", size: " << total_size;
                return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }
            BufferHandle tmp_handle(std::move(*alloc_result));
            std::vector<mooncake::Slice> tmp_slices;
            allocateSlices(tmp_slices, replica, tmp_handle.ptr());
            auto filtered_qr =
                FilterQueryResult(query_result, replica, verify_checksum);
            auto get_result = client_->Get(key, filtered_qr, tmp_slices);
            if (!get_result) {
                LOG(ERROR) << replica_type << " Get failed for key: " << key
                           << " with error: " << toString(get_result.error());
                return tl::unexpected(get_result.error());
            }
            void *dst = static_cast<char *>(buffer) + dst_offset;
            const void *src = tmp_handle.ptr();
            if (auto r = scatter_host_to_maybe_device(
                    dst, src, total_size,
                    std::string(replica_type) + " full read, key: " + key);
                !r) {
                return tl::unexpected(r.error());
            }
            return static_cast<int64_t>(total_size);
        }

        auto runtime_accelerator =
            device::GetAcceleratorRegistry().RuntimeAccelerators();
        void *dst = static_cast<char *>(buffer) + dst_offset;
        if (runtime_accelerator.FindDeviceForPointer(dst) &&
            (!client_->CanUseLocalMemcpy(replica) ||
             client_->IsHotCacheEnabled())) {
            if (!client_buffer_allocator_) {
                LOG(ERROR) << "Client buffer allocator is not provided";
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            }
            auto alloc_result = client_buffer_allocator_->allocate(total_size);
            if (!alloc_result) {
                LOG(ERROR) << "Failed to allocate temp buffer for GPU memory "
                           << "read, key: " << key << ", size: " << total_size;
                return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }
            BufferHandle tmp_handle(std::move(*alloc_result));
            std::vector<mooncake::Slice> tmp_slices;
            allocateSlices(tmp_slices, replica, tmp_handle.ptr());

            auto filtered_qr =
                FilterQueryResult(query_result, replica, verify_checksum);
            auto get_result = client_->Get(key, filtered_qr, tmp_slices);
            if (!get_result) {
                LOG(ERROR) << "Get failed for key: " << key
                           << " with error: " << toString(get_result.error());
                return tl::unexpected(get_result.error());
            }
            if (auto r = scatter_host_to_maybe_device(
                    dst, tmp_handle.ptr(), total_size,
                    "MEMORY full read, key: " + key);
                !r) {
                return tl::unexpected(r.error());
            }
            return static_cast<int64_t>(total_size);
        }

        std::vector<mooncake::Slice> slices;
        allocateSlices(slices, replica, dst);

        auto filtered_qr =
            FilterQueryResult(query_result, replica, verify_checksum);
        auto get_result = client_->Get(key, filtered_qr, slices);
        if (!get_result) {
            LOG(ERROR) << "Get failed for key: " << key
                       << " with error: " << toString(get_result.error());
            return tl::unexpected(get_result.error());
        }
        return static_cast<int64_t>(total_size);
    }

    // Partial file-backed read: allocate temp buffer, invoke read_op to
    // fill it, then scatter [src_offset, src_offset+size) to dst.
    //
    // buf_size controls how much to allocate / read. DISK/DFS must use
    // total_size (allocateSlices requires full-object slices).
    // LOCAL_DISK can use src_offset + size (offload RPC transfers
    // sequentially from remote offset 0).
    auto partial_disk_read =
        [&](auto &&read_op,
            size_t buf_size) -> tl::expected<int64_t, ErrorCode> {
        auto alloc_result = client_buffer_allocator_->allocate(buf_size);
        if (!alloc_result) {
            LOG(ERROR) << "Failed to allocate temp buffer for ranged "
                       << "file-backed "
                       << "read, key: " << key << ", size: " << buf_size;
            return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        BufferHandle tmp_handle(std::move(*alloc_result));
        auto read_result = read_op(tmp_handle.ptr());
        if (!read_result) return tl::unexpected(read_result.error());
        void *dst = static_cast<char *>(buffer) + dst_offset;
        const void *src =
            static_cast<const char *>(tmp_handle.ptr()) + src_offset;
        if (auto r = scatter_host_to_maybe_device(
                dst, src, size, "ranged file-backed read, key: " + key);
            !r) {
            return tl::unexpected(r.error());
        }
        return static_cast<int64_t>(size);
    };

    if (replica.is_local_disk_replica()) {
        const auto &endpoint =
            replica.get_local_disk_descriptor().transport_endpoint;
        void *dst = static_cast<char *>(buffer) + dst_offset;
        std::unordered_map<std::string, std::vector<Slice>> objects{
            {key, {{dst, size}}}};
        if (can_use_pinned_restore_arena(endpoint, objects)) {
            if (total_size > uint64_t(std::numeric_limits<int64_t>::max())) {
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            }
            const OffloadReadRange read_range{src_offset,
                                              static_cast<int64_t>(total_size)};
            auto result = batch_get_into_offload_object_internal(
                endpoint, objects, &read_range);
            if (!result) return tl::unexpected(result.error());
            return static_cast<int64_t>(size);
        }

        // LOCAL_DISK: offload RPC transfers sequentially from remote offset
        // 0, so we only need src_offset + size bytes (not total_size).
        return partial_disk_read(
            [&](void *tmp_buf) -> tl::expected<void, ErrorCode> {
                std::unordered_map<std::string, std::vector<Slice>> objects;
                objects.emplace(
                    key, std::vector<Slice>{{static_cast<char *>(tmp_buf),
                                             src_offset + size}});
                return batch_get_into_offload_object_internal(endpoint,
                                                              objects);
            },
            src_offset + size);
    }

    if (replica.is_disk_replica() || replica.is_dfs_replica()) {
        // DISK/DFS: client_->Get + allocateSlices requires full-object slices,
        // so we must allocate total_size.
        const char *replica_type = replica.is_dfs_replica() ? "DFS" : "DISK";
        return partial_disk_read(
            [&](void *tmp_buf) -> tl::expected<void, ErrorCode> {
                std::vector<mooncake::Slice> tmp_slices;
                allocateSlices(tmp_slices, replica, tmp_buf);
                auto filtered_qr =
                    FilterQueryResult(query_result, replica, false);
                auto get_result = client_->Get(key, filtered_qr, tmp_slices);
                if (!get_result) {
                    LOG(ERROR)
                        << replica_type << " Get failed for key: " << key
                        << " with error: " << toString(get_result.error());
                    return tl::unexpected(get_result.error());
                }
                return {};
            },
            total_size);
    }

    if (!replica.is_memory_replica()) {
        LOG(ERROR) << "ranged reads only support memory/file-backed replicas";
        return tl::unexpected(ErrorCode::INVALID_REPLICA);
    }

    auto runtime_accelerator =
        device::GetAcceleratorRegistry().RuntimeAccelerators();
    void *dst = static_cast<char *>(buffer) + dst_offset;
    auto filtered_qr = FilterQueryResult(query_result, replica, false);
    if (runtime_accelerator.FindDeviceForPointer(dst) &&
        !client_->CanUseLocalMemcpy(replica)) {
        if (!client_buffer_allocator_) {
            LOG(ERROR) << "Client buffer allocator is not provided";
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto alloc_result = client_buffer_allocator_->allocate(size);
        if (!alloc_result) {
            LOG(ERROR) << "Failed to allocate temp buffer for GPU ranged "
                       << "read, key: " << key << ", size: " << size;
            return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        BufferHandle tmp_handle(std::move(*alloc_result));
        std::vector<Slice> tmp_slices;
        tmp_slices.emplace_back(Slice{tmp_handle.ptr(), size});

        auto get_result =
            client_->Get(key, filtered_qr, tmp_slices, src_offset);
        if (!get_result) {
            return tl::unexpected(get_result.error());
        }
        if (auto r = scatter_host_to_maybe_device(
                dst, tmp_handle.ptr(), size, "MEMORY ranged read, key: " + key);
            !r) {
            return tl::unexpected(r.error());
        }
        return static_cast<int64_t>(size);
    }

    std::vector<Slice> slices;
    slices.emplace_back(Slice{dst, size});

    auto get_result = client_->Get(key, filtered_qr, slices, src_offset);
    if (!get_result) {
        return tl::unexpected(get_result.error());
    }
    return static_cast<int64_t>(size);
}

tl::expected<int64_t, ErrorCode> RealClient::get_into_range_internal(
    const std::string &key, void *buffer, size_t dst_offset, size_t src_offset,
    size_t size, bool size_is_buffer_capacity, bool verify_checksum) {
    auto metadata_result = resolve_ranged_read_metadata(key);
    if (!metadata_result) {
        if ((metadata_result.error() == ErrorCode::OBJECT_NOT_FOUND ||
             metadata_result.error() == ErrorCode::REPLICA_IS_NOT_READY) &&
            src_offset == 0) {
            VLOG(1) << "Object not found for key: " << key;
        }
        return tl::unexpected(metadata_result.error());
    }

    return execute_ranged_read(key, buffer, dst_offset, src_offset, size,
                               metadata_result.value(), size_is_buffer_capacity,
                               verify_checksum);
}

int64_t RealClient::get_into(const std::string &key, void *buffer,
                             size_t size) {
    auto result = execute_timed_operation<tl::expected<int64_t, ErrorCode>>(
        [&]() {
            return get_into_range_internal(key, buffer, 0, 0, size, true, true);
        },
        [](const auto &ret) { return ret.has_value(); },
        [&](uint64_t latency_us, const auto &ret) {
            client_->ObserveTransferOperation(
                TransferOperationKind::kRead, "get_into",
                static_cast<uint64_t>(ret.value()), latency_us);
        });
    return to_py_ret(result);
}

std::vector<std::vector<std::vector<tl::expected<int64_t, ErrorCode>>>>
RealClient::get_into_ranges_internal(
    const std::vector<void *> &buffers,
    const std::vector<std::vector<std::string>> &all_keys,
    const std::vector<std::vector<std::vector<size_t>>> &all_dst_offsets,
    const std::vector<std::vector<std::vector<size_t>>> &all_src_offsets,
    const std::vector<std::vector<std::vector<size_t>>> &all_sizes,
    const std::vector<size_t> *buffer_capacities,
    const QueryResultCache *query_result_cache) {
    auto results = build_ranged_read_internal_error_results(
        buffers.size(), all_keys, all_dst_offsets, ErrorCode::INVALID_PARAMS);
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return results;
    }

    const size_t buffer_count = buffers.size();
    if (buffer_count != all_keys.size() ||
        buffer_count != all_dst_offsets.size() ||
        buffer_count != all_src_offsets.size() ||
        buffer_count != all_sizes.size() ||
        (buffer_capacities && buffer_capacities->size() != buffer_count)) {
        LOG(ERROR) << "get_into_ranges: top-level size mismatch";
        return results;
    }

    std::vector<size_t> capacities = buffer_capacities
                                         ? *buffer_capacities
                                         : std::vector<size_t>(buffer_count);
    if (!buffer_capacities) {
        for (size_t i = 0; i < buffer_count; ++i) {
            auto region = resolve_writable_buffer_region(buffers[i]);
            if (region) capacities[i] = region->size - region->offset;
        }
    }

    std::unordered_map<std::string, tl::expected<RangedReadMetadata, ErrorCode>>
        metadata_cache;
    auto metadata_for = [&](const std::string &key) -> auto & {
        auto found = metadata_cache.find(key);
        if (found != metadata_cache.end()) return found->second;
        return metadata_cache
            .emplace(key, resolve_ranged_read_metadata(key, query_result_cache))
            .first->second;
    };

    auto runtime_accelerator =
        device::GetAcceleratorRegistry().RuntimeAccelerators();
    struct ScatterLease {
        std::chrono::steady_clock::time_point expires_at;
        std::optional<ErrorCode> error;
    };
    std::unordered_map<std::string, ScatterLease> scatter_leases;
    std::vector<TransferEngine::ScatterTransferRange> memory_transfers;
    for (size_t i = 0; i < buffer_count; ++i) {
        if (!buffers[i] || (!buffer_capacities && capacities[i] == 0)) {
            continue;
        }
        const auto &keys = all_keys[i];
        const auto &dst_groups = all_dst_offsets[i];
        const auto &src_groups = all_src_offsets[i];
        const auto &size_groups = all_sizes[i];
        if (keys.size() != dst_groups.size() ||
            keys.size() != src_groups.size() ||
            keys.size() != size_groups.size()) {
            continue;
        }

        for (size_t j = 0; j < keys.size(); ++j) {
            const auto &dst_offsets = dst_groups[j];
            const auto &src_offsets = src_groups[j];
            const auto &sizes = size_groups[j];
            auto &range_results = results[i][j];
            if (dst_offsets.size() != src_offsets.size() ||
                dst_offsets.size() != sizes.size()) {
                continue;
            }
            auto &metadata_result = metadata_for(keys[j]);
            if (!metadata_result) {
                std::fill(range_results.begin(), range_results.end(),
                          tl::unexpected(metadata_result.error()));
                continue;
            }

            const auto &metadata = metadata_result.value();
            if (metadata.replica.is_memory_replica()) {
                if (client_->CanUseLocalMemcpy(metadata.replica) &&
                    runtime_accelerator.FindDeviceForPointer(buffers[i])) {
                    // Planning cache entries may be close to expiry. Renew and
                    // reselect the replica before copying into device memory.
                    auto refresh_result = resolve_ranged_read_metadata(keys[j]);
                    if (!refresh_result) {
                        std::fill(range_results.begin(), range_results.end(),
                                  tl::unexpected(refresh_result.error()));
                        continue;
                    }
                    std::optional<RangedReadMetadata> refreshed_metadata;
                    refreshed_metadata.emplace(std::move(*refresh_result));
                    auto lease_refresh_at =
                        [](const RangedReadMetadata &value) {
                            const auto now = std::chrono::steady_clock::now();
                            return now +
                                   (value.query_result.lease_timeout - now) / 2;
                        };
                    auto refresh_at = lease_refresh_at(*refreshed_metadata);
                    for (size_t k = 0; k < range_results.size(); ++k) {
                        // Renew halfway through the remaining lease in long
                        // batches.
                        const size_t dst_offset = dst_offsets[k];
                        if (dst_offset > capacities[i] ||
                            sizes[k] > capacities[i] - dst_offset) {
                            continue;
                        }
                        if (std::chrono::steady_clock::now() >= refresh_at) {
                            auto next_refresh_result =
                                resolve_ranged_read_metadata(keys[j]);
                            if (!next_refresh_result) {
                                std::fill(range_results.begin() + k,
                                          range_results.end(),
                                          tl::unexpected(
                                              next_refresh_result.error()));
                                break;
                            }
                            refreshed_metadata.emplace(
                                std::move(*next_refresh_result));
                            refresh_at = lease_refresh_at(*refreshed_metadata);
                        }
                        range_results[k] = execute_ranged_read(
                            keys[j], buffers[i], dst_offset, src_offsets[k],
                            sizes[k], *refreshed_metadata, false, false);
                    }
                    continue;
                }
                const auto &handle =
                    metadata.replica.get_memory_descriptor().buffer_descriptor;
                auto [lease_it, inserted] = scatter_leases.try_emplace(keys[j]);
                if (inserted)
                    lease_it->second.expires_at =
                        metadata.query_result.lease_timeout;
                memory_transfers.push_back(TransferEngine::ScatterTransferRange{
                    .opcode = TransferRequest::READ,
                    .remote_segment = handle.transport_endpoint_,
                    .remote_base_offset = handle.buffer_address_,
                    .remote_size = handle.size_,
                    .local_buffer = buffers[i],
                    .local_capacity = capacities[i],
                    .local_offsets = dst_offsets,
                    .remote_offsets = src_offsets,
                    .lengths = sizes,
                    .on_fragment_complete =
                        [results = &range_results, sizes = &sizes,
                         lease = &lease_it->second](size_t k,
                                                    const Status &status) {
                            if (status.ok() && !lease->error.has_value() &&
                                std::chrono::steady_clock::now() <
                                    lease->expires_at) {
                                (*results)[k] =
                                    static_cast<int64_t>((*sizes)[k]);
                                return;
                            }
                            const auto error = lease->error.value_or(
                                status.ok() ? ErrorCode::LEASE_EXPIRED
                                            : scatter_transfer_error(status));
                            (*results)[k] = tl::unexpected(error);
                        },
                });
                continue;
            }

            for (size_t k = 0; k < range_results.size(); ++k) {
                const size_t dst_offset = dst_offsets[k];
                if (dst_offset > capacities[i] ||
                    sizes[k] > capacities[i] - dst_offset) {
                    continue;
                }

                range_results[k] = execute_ranged_read(
                    keys[j], buffers[i], dst_offset, src_offsets[k], sizes[k],
                    metadata, false, false);
            }
        }
    }

    auto next_refresh_delay = [&]() {
        const auto now = std::chrono::steady_clock::now();
        auto delay = std::chrono::nanoseconds::max();
        for (const auto &[key, lease] : scatter_leases) {
            (void)key;
            if (lease.error.has_value()) continue;
            const auto remaining =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    lease.expires_at - now);
            delay = std::min(delay, std::max(remaining / 2,
                                             std::chrono::nanoseconds::zero()));
        }
        return delay;
    };
    auto refresh_leases = [&]() {
        std::vector<std::string> keys;
        keys.reserve(scatter_leases.size());
        for (const auto &[key, lease] : scatter_leases)
            if (!lease.error.has_value()) keys.push_back(key);
        auto refreshed = client_->BatchQuery(keys);
        for (size_t i = 0; i < refreshed.size(); ++i) {
            auto &lease = scatter_leases.at(keys[i]);
            if (!refreshed[i]) {
                lease.error = refreshed[i].error();
                continue;
            }
            lease.expires_at = refreshed[i]->lease_timeout;
        }
    };

    // Planning may consume most of a short lease; renew before submission.
    if (!scatter_leases.empty()) refresh_leases();
    auto operation = client_->SubmitScatter(memory_transfers);
    if (!operation.has_value()) {
        const auto failure =
            Status::InvalidArgument("TransferSubmitter not initialized");
        for (const auto &transfer : memory_transfers) {
            for (size_t i = 0; i < transfer.lengths.size(); ++i)
                transfer.on_fragment_complete(i, failure);
        }
        return results;
    }

    while (true) {
        const auto delay = next_refresh_delay();
        const auto status = delay == std::chrono::nanoseconds::max()
                                ? operation->wait()
                                : operation->waitFor(delay);
        if (!status.IsClock()) break;
        refresh_leases();
    }
    return results;
}

std::vector<std::vector<std::vector<int64_t>>> RealClient::get_into_ranges(
    const std::vector<void *> &buffers,
    const std::vector<std::vector<std::string>> &all_keys,
    const std::vector<std::vector<std::vector<size_t>>> &all_dst_offsets,
    const std::vector<std::vector<std::vector<size_t>>> &all_src_offsets,
    const std::vector<std::vector<std::vector<size_t>>> &all_sizes,
    const QueryResultCache *query_result_cache) {
    auto results =
        execute_timed_operation<std::vector<std::vector<std::vector<int64_t>>>>(
            [&]() {
                return convert_ranged_read_results(get_into_ranges_internal(
                    buffers, all_keys, all_dst_offsets, all_src_offsets,
                    all_sizes, nullptr, query_result_cache));
            },
            [](const auto &) { return true; },
            [&](uint64_t latency_us, const auto &ret) {
                client_->ObserveTransferOperation(
                    TransferOperationKind::kRead, "get_into_ranges",
                    sum_positive_ranges(ret), latency_us);
            });
    return results;
}

std::vector<tl::expected<QueryResult, ErrorCode>> RealClient::batch_query(
    const std::vector<std::string> &keys) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<tl::expected<QueryResult, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    const bool allow_dfs = client_->DfsReadAllowed();
    auto query_results = client_->BatchQuery(keys);
    if (allow_dfs) return query_results;

    std::vector<tl::expected<QueryResult, ErrorCode>> filtered;
    filtered.reserve(query_results.size());
    for (const auto &query_result : query_results) {
        filtered.push_back(ApplyDfsReadVisibility(query_result, false));
    }
    return filtered;
}

tl::expected<RealClient::RangedReadMetadata, ErrorCode>
RealClient::build_ranged_read_metadata_from_query_result(
    const std::string &key, tl::expected<QueryResult, ErrorCode> query_result) {
    if (!query_result) {
        if (query_result.error() == ErrorCode::OBJECT_NOT_FOUND ||
            query_result.error() == ErrorCode::REPLICA_IS_NOT_READY) {
            return tl::unexpected(query_result.error());
        }
        LOG(ERROR) << "Query failed for key: " << key
                   << " with error: " << toString(query_result.error());
        return tl::unexpected(query_result.error());
    }

    const auto &replica_list = query_result->replicas;
    if (replica_list.empty()) {
        LOG(ERROR) << "Internal error: replica_list is empty";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto local_endpoints = client_->GetLocalEndpoints();
    const auto *best_replica = SelectBestReplica(replica_list, local_endpoints);
    if (!best_replica) {
        LOG(ERROR) << "No usable replica for key: " << key;
        return tl::unexpected(ErrorCode::INVALID_REPLICA);
    }

    auto query_value = std::move(query_result.value());
    auto replica = *best_replica;
    return RangedReadMetadata{.query_result = std::move(query_value),
                              .replica = std::move(replica),
                              .total_size = calculate_total_size(replica)};
}

std::string RealClient::get_hostname() const { return local_hostname; }

std::vector<int> RealClient::batch_put_from(
    const std::vector<std::string> &keys, const std::vector<void *> &buffers,
    const std::vector<size_t> &sizes, const ReplicateConfig &config) {
    auto internal_results =
        execute_timed_operation<std::vector<tl::expected<void, ErrorCode>>>(
            [&]() {
                return batch_put_from_internal(keys, buffers, sizes, config);
            },
            [](const auto &) { return true; },
            [&](uint64_t latency_us, const auto &ret) {
                std::vector<int> py_results;
                py_results.reserve(ret.size());
                for (const auto &item : ret) {
                    py_results.push_back(to_py_ret(item));
                }
                client_->ObserveTransferOperation(
                    TransferOperationKind::kWrite, "batch_put_from",
                    sum_successful_sizes(py_results, sizes), latency_us);
            });
    std::vector<int> results;
    results.reserve(internal_results.size());

    for (const auto &result : internal_results) {
        results.push_back(to_py_ret(result));
    }

    return results;
}

std::vector<tl::expected<void, ErrorCode>>
RealClient::batch_put_from_dummy_helper(
    const std::vector<std::string> &keys,
    const std::vector<uint64_t> &dummy_buffers,
    const std::vector<size_t> &sizes, const ReplicateConfig &config,
    int32_t device_id, const UUID &client_id) {
    // Set the device context for the current thread
#ifdef USE_ASCEND_DIRECT
    if (!ContextManager::getInstance().setCurrentContextByPhysicalId(
            device_id)) {
        LOG(ERROR) << "Failed to set context for physical device " << device_id;
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
#endif

    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    auto &context = it->second;

    auto buffers_result =
        map_dummy_addrs_to_real_ptrs(context, dummy_buffers, sizes, client_id);
    if (!buffers_result) {
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(buffers_result.error()));
    }
    return batch_put_from_internal(keys, buffers_result.value(), sizes, config);
}

std::vector<tl::expected<void, ErrorCode>> RealClient::batch_put_from_internal(
    const std::vector<std::string> &keys, const std::vector<void *> &buffers,
    const std::vector<size_t> &sizes, const ReplicateConfig &config) {
    if (config.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    if (keys.size() != buffers.size() || keys.size() != sizes.size()) {
        LOG(ERROR) << "Mismatched sizes for keys, buffers, and sizes";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    std::unordered_map<std::string, std::vector<mooncake::Slice>> all_slices;

    // Create slices from user buffers
    for (size_t i = 0; i < keys.size(); ++i) {
        const std::string &key = keys[i];
        void *buffer = buffers[i];
        size_t size = sizes[i];

        all_slices[key] = split_into_slices(buffer, size);
    }

    std::vector<std::vector<mooncake::Slice>> ordered_batched_slices;
    ordered_batched_slices.reserve(keys.size());
    for (const auto &key : keys) {
        auto it = all_slices.find(key);
        if (it != all_slices.end()) {
            ordered_batched_slices.emplace_back(it->second);
        } else {
            LOG(ERROR) << "Missing slices for key: " << key;
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
    }

    // Call client BatchPut and return the vector<expected> directly
    return client_->BatchPut(keys, ordered_batched_slices, config);
}

tl::expected<void, ErrorCode> RealClient::put_from_internal(
    const std::string &key, void *buffer, size_t size,
    const ReplicateConfig &config) {
    // NOTE: The buffer address must resolve to Store-managed registered
    // memory for zero-copy RDMA operations to work correctly
    if (config.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (size == 0) {
        LOG(WARNING) << "Attempting to put empty data for key: " << key;
        return {};
    }

    // Create slices directly from the user buffer
    std::vector<mooncake::Slice> slices = split_into_slices(buffer, size);

    auto put_result = client_->Put(key, slices, config);
    if (!put_result) {
        return tl::unexpected(put_result.error());
    }

    return {};
}

int RealClient::put_from(const std::string &key, void *buffer, size_t size,
                         const ReplicateConfig &config) {
    auto result = execute_timed_operation<tl::expected<void, ErrorCode>>(
        [&]() { return put_from_internal(key, buffer, size, config); },
        [](const auto &ret) { return ret.has_value(); },
        [&](uint64_t latency_us, const auto &) {
            client_->ObserveTransferOperation(TransferOperationKind::kWrite,
                                              "put_from", size, latency_us);
        });
    return to_py_ret(result);
}

// --- Upsert implementations ---

tl::expected<void, ErrorCode> RealClient::upsert_internal(
    const std::string &key, std::span<const char> value,
    const ReplicateConfig &config,
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator) {
    if (config.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_buffer_allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto alloc_result = client_buffer_allocator->allocate(value.size_bytes());
    if (!alloc_result) {
        LOG(ERROR) << "Failed to allocate buffer for upsert operation, key: "
                   << key << ", value size: " << value.size();
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto &buffer_handle = *alloc_result;
    auto scatter_result = gather_maybe_device_to_host(
        buffer_handle.ptr(), value.data(), value.size_bytes(), "upsert:" + key);
    if (!scatter_result) {
        return tl::unexpected(scatter_result.error());
    }

    std::vector<Slice> slices = split_into_slices(buffer_handle);

    auto result = client_->Upsert(key, slices, config);
    if (!result) {
        return tl::unexpected(result.error());
    }
    return {};
}

int RealClient::upsert(const std::string &key, std::span<const char> value,
                       const ReplicateConfig &config) {
    auto result = execute_timed_operation<tl::expected<void, ErrorCode>>(
        [&]() {
            return upsert_internal(key, value, config,
                                   client_buffer_allocator_);
        },
        [](const auto &ret) { return ret.has_value(); },
        [&](uint64_t latency_us, const auto &) {
            client_->ObserveTransferOperation(TransferOperationKind::kWrite,
                                              "upsert", value.size_bytes(),
                                              latency_us);
        });
    return to_py_ret(result);
}

tl::expected<void, ErrorCode> RealClient::upsert_dummy_helper(
    const std::string &key, std::span<const char> value,
    const ReplicateConfig &config, const UUID &client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto &context = it->second;
    return upsert_internal(key, value, config, context.client_buffer_allocator);
}

tl::expected<void, ErrorCode> RealClient::upsert_from_internal(
    const std::string &key, void *buffer, size_t size,
    const ReplicateConfig &config) {
    if (config.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (size == 0) {
        LOG(WARNING) << "Attempting to upsert empty data for key: " << key;
        return {};
    }

    std::vector<mooncake::Slice> slices = split_into_slices(buffer, size);

    auto result = client_->Upsert(key, slices, config);
    if (!result) {
        return tl::unexpected(result.error());
    }
    return {};
}

int RealClient::upsert_from(const std::string &key, void *buffer, size_t size,
                            const ReplicateConfig &config) {
    auto result = execute_timed_operation<tl::expected<void, ErrorCode>>(
        [&]() { return upsert_from_internal(key, buffer, size, config); },
        [](const auto &ret) { return ret.has_value(); },
        [&](uint64_t latency_us, const auto &) {
            client_->ObserveTransferOperation(TransferOperationKind::kWrite,
                                              "upsert_from", size, latency_us);
        });
    return to_py_ret(result);
}

std::vector<tl::expected<void, ErrorCode>>
RealClient::batch_upsert_from_internal(const std::vector<std::string> &keys,
                                       const std::vector<void *> &buffers,
                                       const std::vector<size_t> &sizes,
                                       const ReplicateConfig &config) {
    if (config.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    if (keys.size() != buffers.size() || keys.size() != sizes.size()) {
        LOG(ERROR) << "Mismatched sizes for keys, buffers, and sizes";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    std::vector<std::vector<mooncake::Slice>> ordered_batched_slices;
    ordered_batched_slices.reserve(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        ordered_batched_slices.emplace_back(
            split_into_slices(buffers[i], sizes[i]));
    }

    return client_->BatchUpsert(keys, ordered_batched_slices, config);
}

std::vector<int> RealClient::batch_upsert_from(
    const std::vector<std::string> &keys, const std::vector<void *> &buffers,
    const std::vector<size_t> &sizes, const ReplicateConfig &config) {
    auto internal_results =
        execute_timed_operation<std::vector<tl::expected<void, ErrorCode>>>(
            [&]() {
                return batch_upsert_from_internal(keys, buffers, sizes, config);
            },
            [](const auto &) { return true; },
            [&](uint64_t latency_us, const auto &ret) {
                std::vector<int> py_results;
                py_results.reserve(ret.size());
                for (const auto &item : ret) {
                    py_results.push_back(to_py_ret(item));
                }
                client_->ObserveTransferOperation(
                    TransferOperationKind::kWrite, "batch_upsert_from",
                    sum_successful_sizes(py_results, sizes), latency_us);
            });
    std::vector<int> results;
    results.reserve(internal_results.size());
    for (const auto &result : internal_results) {
        results.push_back(to_py_ret(result));
    }
    return results;
}

tl::expected<void, ErrorCode> RealClient::upsert_from_dummy_helper(
    const std::string &key, uint64_t dummy_buffer, size_t size,
    const ReplicateConfig &config, const UUID &client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto &context = it->second;

    void *real_buffer = nullptr;
    const MappedShm *last_hit_shm = nullptr;
    if (!map_dummy_buffer_to_real(context, dummy_buffer, size, last_hit_shm,
                                  real_buffer)) {
        LOG(ERROR) << "Dummy buffer at " << dummy_buffer << " (size " << size
                   << ") not found in any mapped shared memory for client "
                   << client_id;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    return upsert_from_internal(key, real_buffer, size, config);
}

std::vector<tl::expected<void, ErrorCode>>
RealClient::batch_upsert_from_dummy_helper(
    const std::vector<std::string> &keys,
    const std::vector<uint64_t> &dummy_buffers,
    const std::vector<size_t> &sizes, const ReplicateConfig &config,
    const UUID &client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    auto &context = it->second;

    std::vector<void *> buffers;
    buffers.reserve(dummy_buffers.size());
    const MappedShm *last_hit_shm = nullptr;

    for (size_t i = 0; i < dummy_buffers.size(); ++i) {
        void *real_ptr = nullptr;
        if (!map_dummy_buffer_to_real(context, dummy_buffers[i], sizes[i],
                                      last_hit_shm, real_ptr)) {
            LOG(ERROR) << "Dummy buffer at " << dummy_buffers[i] << " (size "
                       << sizes[i]
                       << ") not found in any mapped shared memory for client "
                       << client_id;
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        buffers.push_back(real_ptr);
    }
    return batch_upsert_from_internal(keys, buffers, sizes, config);
}

tl::expected<void, ErrorCode> RealClient::upsert_parts_internal(
    const std::string &key, std::vector<std::span<const char>> values,
    const ReplicateConfig &config,
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator) {
    if (config.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_buffer_allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    size_t total_size = 0;
    for (const auto &value : values) {
        total_size += value.size_bytes();
    }
    if (total_size == 0) {
        LOG(WARNING) << "Attempting to upsert empty data for key: " << key;
        return {};
    }

    auto alloc_result = client_buffer_allocator->allocate(total_size);
    if (!alloc_result) {
        LOG(ERROR)
            << "Failed to allocate buffer for upsert_parts operation, key: "
            << key << ", total size: " << total_size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto &buffer_handle = *alloc_result;
    size_t offset = 0;
    for (const auto &value : values) {
        auto scatter_result = gather_maybe_device_to_host(
            static_cast<char *>(buffer_handle.ptr()) + offset, value.data(),
            value.size_bytes(), "upsert_parts:" + key);
        if (!scatter_result) {
            return tl::unexpected(scatter_result.error());
        }
        offset += value.size_bytes();
    }

    std::vector<Slice> slices = split_into_slices(buffer_handle);

    auto result = client_->Upsert(key, slices, config);
    if (!result) {
        LOG(ERROR) << "Upsert operation failed with error: "
                   << toString(result.error());
        return tl::unexpected(result.error());
    }
    return {};
}

int RealClient::upsert_parts(const std::string &key,
                             std::vector<std::span<const char>> values,
                             const ReplicateConfig &config) {
    auto result = execute_timed_operation<tl::expected<void, ErrorCode>>(
        [&]() {
            return upsert_parts_internal(key, values, config,
                                         client_buffer_allocator_);
        },
        [](const auto &ret) { return ret.has_value(); },
        [&](uint64_t latency_us, const auto &) {
            client_->ObserveTransferOperation(
                TransferOperationKind::kWrite, "upsert_parts",
                sum_value_sizes(values), latency_us);
        });
    return to_py_ret(result);
}

tl::expected<void, ErrorCode> RealClient::upsert_parts_dummy_helper(
    const std::string &key, std::vector<std::span<const char>> values,
    const ReplicateConfig &config, const UUID &client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto &context = it->second;
    return upsert_parts_internal(key, values, config,
                                 context.client_buffer_allocator);
}

tl::expected<void, ErrorCode> RealClient::upsert_batch_internal(
    const std::vector<std::string> &keys,
    const std::vector<std::span<const char>> &values,
    const ReplicateConfig &config,
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator) {
    if (config.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (keys.size() != values.size()) {
        LOG(ERROR) << "Key and value size mismatch";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!client_buffer_allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::vector<BufferHandle> buffer_handles;
    std::unordered_map<std::string, std::vector<Slice>> batched_slices;
    batched_slices.reserve(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        auto &key = keys[i];
        auto &value = values[i];
        auto alloc_result =
            client_buffer_allocator->allocate(value.size_bytes());
        if (!alloc_result) {
            LOG(ERROR)
                << "Failed to allocate buffer for upsert_batch operation, key: "
                << key << ", value size: " << value.size();
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto &buffer_handle = *alloc_result;
        auto scatter_result = gather_maybe_device_to_host(
            buffer_handle.ptr(), value.data(), value.size_bytes(),
            "upsert_batch:" + key);
        if (!scatter_result) {
            return tl::unexpected(scatter_result.error());
        }
        auto slices = split_into_slices(buffer_handle);
        buffer_handles.emplace_back(std::move(*alloc_result));
        batched_slices.emplace(key, std::move(slices));
    }

    // Convert unordered_map to vector format expected by BatchUpsert
    std::vector<std::vector<mooncake::Slice>> ordered_batched_slices;
    ordered_batched_slices.reserve(keys.size());
    for (const auto &key : keys) {
        auto it = batched_slices.find(key);
        if (it != batched_slices.end()) {
            ordered_batched_slices.emplace_back(it->second);
        } else {
            LOG(ERROR) << "Missing slices for key: " << key;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    auto results = client_->BatchUpsert(keys, ordered_batched_slices, config);

    // Check if any operations failed
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i]) {
            return tl::unexpected(results[i].error());
        }
    }
    return {};
}

tl::expected<void, ErrorCode> RealClient::upsert_batch_dummy_helper(
    const std::vector<std::string> &keys,
    const std::vector<std::span<const char>> &values,
    const ReplicateConfig &config, const UUID &client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto &context = it->second;

    return upsert_batch_internal(keys, values, config,
                                 context.client_buffer_allocator);
}

int RealClient::upsert_batch(const std::vector<std::string> &keys,
                             const std::vector<std::span<const char>> &values,
                             const ReplicateConfig &config) {
    auto result = execute_timed_operation<tl::expected<void, ErrorCode>>(
        [&]() {
            return upsert_batch_internal(keys, values, config,
                                         client_buffer_allocator_);
        },
        [](const auto &ret) { return ret.has_value(); },
        [&](uint64_t latency_us, const auto &) {
            client_->ObserveTransferOperation(
                TransferOperationKind::kWrite, "upsert_batch",
                sum_value_sizes(values), latency_us);
        });
    return to_py_ret(result);
}

// --- End Upsert implementations ---

std::vector<int64_t> RealClient::batch_get_into(
    const std::vector<std::string> &keys, const std::vector<void *> &buffers,
    const std::vector<size_t> &sizes) {
    auto internal_results =
        execute_timed_operation<std::vector<tl::expected<int64_t, ErrorCode>>>(
            [&]() { return batch_get_into_internal(keys, buffers, sizes); },
            [](const auto &) { return true; },
            [&](uint64_t latency_us, const auto &ret) {
                std::vector<int64_t> py_results;
                py_results.reserve(ret.size());
                for (const auto &item : ret) {
                    py_results.push_back(to_py_ret(item));
                }
                client_->ObserveTransferOperation(
                    TransferOperationKind::kRead, "batch_get_into",
                    sum_positive_results(py_results), latency_us);
            });
    std::vector<int64_t> results;
    results.reserve(internal_results.size());

    for (const auto &result : internal_results) {
        results.push_back(to_py_ret(result));
    }

    return results;
}

async_simple::coro::Lazy<std::vector<tl::expected<int64_t, ErrorCode>>>
RealClient::batch_get_into_dummy_helper(
    const std::vector<std::string> &keys,
    const std::vector<uint64_t> &dummy_buffers,
    const std::vector<size_t> &sizes, int32_t device_id,
    const UUID &client_id) {
    // Hold shared_lock for the entire operation to prevent SHM from being
    // unmapped while batch_get_into_internal is using the translated buffers.
    auto lock = std::make_shared<std::shared_lock<std::shared_mutex>>(
        dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        co_return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    auto &context = it->second;

    auto buffers_result =
        map_dummy_addrs_to_real_ptrs(context, dummy_buffers, sizes, client_id);
    if (!buffers_result) {
        co_return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(buffers_result.error()));
    }

    // Run batch_get_into_internal (which may block on offload RPC + SSD I/O)
    // on a dedicated thread pool so the coro_rpc IO thread stays free for ping.
    //
    // Pack everything the worker-thread call needs into a single heap-owned
    // state object so the lambda captures only a raw pointer (trivially
    // copyable, trivially destructible). Avoids GCC 11 coroutine + coro_io
    // double-destruction interactions when non-trivial captures end up in
    // both the OUTER coroutine frame and the post_helper chain.
    struct CallState {
        std::vector<std::string> keys;
        std::vector<size_t> sizes;
        std::vector<void *> buffers;
        std::shared_ptr<std::shared_lock<std::shared_mutex>> lock;
        int32_t device_id;
    };
    auto state = std::make_unique<CallState>();
    state->keys = keys;
    state->sizes = sizes;
    state->buffers = std::move(buffers_result.value());
    state->lock = std::move(lock);
    state->device_id = device_id;

    auto *s = state.get();
    auto try_result = co_await coro_io::post([this, s]() {
#ifdef USE_ASCEND_DIRECT
        auto context_result =
            set_context_if_needed(protocol, s->device_id, "batch_get worker");
        if (!context_result) {
            return std::vector<tl::expected<int64_t, ErrorCode>>(
                s->keys.size(), tl::unexpected(context_result.error()));
        }
#endif
        return batch_get_into_internal(s->keys, s->buffers, s->sizes);
    });
    co_return try_result.value();
}

std::vector<tl::expected<void, ErrorCode>>
RealClient::batch_put_from_multi_buffers_dummy_helper(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<uint64_t>> &dummy_all_buffers,
    const std::vector<std::vector<size_t>> &all_sizes,
    const ReplicateConfig &config, int32_t device_id, const UUID &client_id) {
#ifdef USE_ASCEND_DIRECT
    if (!ContextManager::getInstance().setCurrentContextByPhysicalId(
            device_id)) {
        LOG(ERROR) << "Failed to set context for physical device " << device_id;
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
#endif

    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    auto &context = it->second;

    auto real_buffers_result = map_dummy_nested_addrs_to_real_ptrs(
        context, dummy_all_buffers, all_sizes, keys, client_id);
    if (!real_buffers_result) {
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(real_buffers_result.error()));
    }

    return batch_put_from_multi_buffers_internal(
        keys, real_buffers_result.value(), all_sizes, config);
}

std::vector<tl::expected<void, ErrorCode>>
RealClient::batch_put_from_cuda_ipc_dummy_helper(
    const std::vector<CudaIpcWriteRequest> &requests,
    const ReplicateConfig &config, const UUID &client_id) {
    return batch_write_from_cuda_ipc_dummy_helper(requests, config, client_id,
                                                  false);
}

std::vector<tl::expected<void, ErrorCode>>
RealClient::batch_upsert_from_cuda_ipc_dummy_helper(
    const std::vector<CudaIpcWriteRequest> &requests,
    const ReplicateConfig &config, const UUID &client_id) {
    return batch_write_from_cuda_ipc_dummy_helper(requests, config, client_id,
                                                  true);
}

std::vector<tl::expected<void, ErrorCode>>
RealClient::batch_write_from_cuda_ipc_dummy_helper(
    const std::vector<CudaIpcWriteRequest> &requests,
    const ReplicateConfig &config, const UUID &client_id, bool is_upsert) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return std::vector<tl::expected<void, ErrorCode>>(
            requests.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    const ShmContext &context = it->second;
    const bool has_metadata =
        !requests.empty() && requests[0].metadata.size != 0;
    for (const auto &request : requests) {
        if ((request.metadata.size != 0) != has_metadata) {
            LOG(ERROR) << "Mixed cuda ipc metadata batches are not supported";
            return std::vector<tl::expected<void, ErrorCode>>(
                requests.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
    }

    std::vector<device::CudaIpcBufferMapping> payload_mappings;
    std::vector<std::string> keys;
    std::vector<std::vector<void *>> all_buffers;
    std::vector<std::vector<size_t>> all_sizes;
    payload_mappings.reserve(requests.size());
    keys.reserve(requests.size());
    all_buffers.reserve(requests.size());
    all_sizes.reserve(requests.size());

    const MappedShm *last_hit_shm = nullptr;
    for (const auto &request : requests) {
        auto mapping = device::CudaIpcBufferMapping::Open(request.payload);
        if (!mapping) {
            return std::vector<tl::expected<void, ErrorCode>>(
                requests.size(), tl::unexpected(mapping.error()));
        }
        payload_mappings.push_back(std::move(*mapping));
        void *payload_ptr = payload_mappings.back().ptr();
        keys.push_back(request.key);
        if (has_metadata) {
            void *metadata_ptr = nullptr;
            if (!map_dummy_buffer_to_real(
                    context, request.metadata.ptr,
                    static_cast<size_t>(request.metadata.size), last_hit_shm,
                    metadata_ptr)) {
                LOG(ERROR) << "Dummy metadata buffer at "
                           << request.metadata.ptr << " (size "
                           << request.metadata.size
                           << ") not found in any mapped shared memory, "
                           << "client_id=" << client_id;
                return std::vector<tl::expected<void, ErrorCode>>(
                    requests.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
            }
            all_buffers.push_back({metadata_ptr, payload_ptr});
            all_sizes.push_back({static_cast<size_t>(request.metadata.size),
                                 static_cast<size_t>(request.payload.size)});
        } else {
            all_buffers.push_back({payload_ptr});
            all_sizes.push_back({static_cast<size_t>(request.payload.size)});
        }
    }

    if (is_upsert) {
        return batch_upsert_from_multi_buffers_internal(
            keys, all_buffers, all_sizes, config, true);
    }
    return batch_put_from_multi_buffers_internal(keys, all_buffers, all_sizes,
                                                 config, true);
}

std::vector<tl::expected<int64_t, ErrorCode>>
RealClient::batch_get_into_cuda_ipc_dummy_helper(
    const std::vector<CudaIpcReadRequest> &requests, const UUID &client_id) {
    {
        std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
        if (shm_contexts_.find(client_id) == shm_contexts_.end()) {
            LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
            return std::vector<tl::expected<int64_t, ErrorCode>>(
                requests.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
    }

    std::vector<tl::expected<int64_t, ErrorCode>> results(
        requests.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));

    for (size_t i = 0; i < requests.size(); ++i) {
        const auto &request = requests[i];
        if (request.size == 0) {
            results[i] = 0;
            continue;
        }
        auto mapping = device::CudaIpcBufferMapping::Open(request.destination);
        if (!mapping) {
            results[i] = tl::unexpected(mapping.error());
            continue;
        }
        results[i] = get_into_range_internal(
            request.key, mapping->ptr(), 0,
            static_cast<size_t>(request.source_offset),
            static_cast<size_t>(request.size), false, false);
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>>
RealClient::batch_upsert_from_multi_buffers_dummy_helper(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<uint64_t>> &dummy_all_buffers,
    const std::vector<std::vector<size_t>> &all_sizes,
    const ReplicateConfig &config, int32_t device_id, const UUID &client_id) {
#ifdef USE_ASCEND_DIRECT
    if (!ContextManager::getInstance().setCurrentContextByPhysicalId(
            device_id)) {
        LOG(ERROR) << "Failed to set context for physical device " << device_id;
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
#endif

    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    auto &context = it->second;

    auto real_buffers_result = map_dummy_nested_addrs_to_real_ptrs(
        context, dummy_all_buffers, all_sizes, keys, client_id);
    if (!real_buffers_result) {
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(real_buffers_result.error()));
    }

    return batch_upsert_from_multi_buffers_internal(
        keys, real_buffers_result.value(), all_sizes, config);
}

std::vector<tl::expected<int64_t, ErrorCode>>
RealClient::batch_get_into_multi_buffers_dummy_helper(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<uint64_t>> &dummy_all_buffers,
    const std::vector<std::vector<size_t>> &all_sizes,
    bool prefer_alloc_in_same_node, int32_t device_id, const UUID &client_id) {
#ifdef USE_ASCEND_DIRECT
    if (!ContextManager::getInstance().setCurrentContextByPhysicalId(
            device_id)) {
        LOG(ERROR) << "Failed to set context for physical device " << device_id;
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
#endif

    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    auto &context = it->second;

    auto real_buffers_result = map_dummy_nested_addrs_to_real_ptrs(
        context, dummy_all_buffers, all_sizes, keys, client_id);
    if (!real_buffers_result) {
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(real_buffers_result.error()));
    }

    return batch_get_into_multi_buffers_internal(
        keys, real_buffers_result.value(), all_sizes,
        prefer_alloc_in_same_node);
}

tl::expected<int64_t, ErrorCode> RealClient::get_into_range_shm_helper(
    const std::string &key, uint64_t dummy_buffer, size_t dst_offset,
    size_t src_offset, size_t size, bool size_is_buffer_capacity,
    bool verify_checksum, const UUID &client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto &context = it->second;

    void *real_buffer = nullptr;
    if (!map_dummy_buffer_range_to_real(context, dummy_buffer, dst_offset, size,
                                        real_buffer)) {
        LOG(ERROR) << "Dummy buffer at " << dummy_buffer
                   << " (dst_offset=" << dst_offset << ", size=" << size
                   << ") not found in any mapped shared memory for client "
                   << client_id;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    return get_into_range_internal(key, real_buffer, 0, src_offset, size,
                                   size_is_buffer_capacity, verify_checksum);
}

std::vector<std::vector<std::vector<tl::expected<int64_t, ErrorCode>>>>
RealClient::get_into_ranges_shm_helper(
    const std::vector<uint64_t> &dummy_buffers,
    const std::vector<std::vector<std::string>> &all_keys,
    const std::vector<std::vector<std::vector<size_t>>> &all_dst_offsets,
    const std::vector<std::vector<std::vector<size_t>>> &all_src_offsets,
    const std::vector<std::vector<std::vector<size_t>>> &all_sizes,
    const std::map<std::string, CachedQueryResultResponse>
        &cached_query_results,
    int32_t device_id, const UUID &client_id) {
#ifdef USE_ASCEND_DIRECT
    if (!ContextManager::getInstance().setCurrentContextByPhysicalId(
            device_id)) {
        LOG(ERROR) << "Failed to set context for physical device " << device_id;
        return std::vector<
            std::vector<std::vector<tl::expected<int64_t, ErrorCode>>>>(
            dummy_buffers.size(),
            std::vector<std::vector<tl::expected<int64_t, ErrorCode>>>(
                1, std::vector<tl::expected<int64_t, ErrorCode>>(
                       1, tl::unexpected(ErrorCode::INVALID_PARAMS))));
    }
#endif

    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
    auto it = shm_contexts_.find(client_id);
    if (it == shm_contexts_.end()) {
        LOG(ERROR) << "client_id=" << client_id << ", error=shm_not_mapped";
        return build_ranged_read_internal_error_results(
            dummy_buffers.size(), all_keys, all_dst_offsets,
            ErrorCode::INVALID_PARAMS);
    }

    std::vector<size_t> capacities;
    auto real_buffers_result = map_dummy_addrs_to_real_ptrs(
        it->second, dummy_buffers, std::vector<size_t>(dummy_buffers.size()),
        client_id, &capacities);
    if (!real_buffers_result) {
        return build_ranged_read_internal_error_results(
            dummy_buffers.size(), all_keys, all_dst_offsets,
            real_buffers_result.error());
    }

    auto query_result_cache =
        build_query_result_cache_from_cached_results(cached_query_results);
    return get_into_ranges_internal(
        real_buffers_result.value(), all_keys, all_dst_offsets, all_src_offsets,
        all_sizes, &capacities,
        query_result_cache.empty() ? nullptr : &query_result_cache);
}

std::vector<tl::expected<int64_t, ErrorCode>>
RealClient::batch_get_into_internal(const std::vector<std::string> &keys,
                                    const std::vector<void *> &buffers,
                                    const std::vector<size_t> &sizes) {
    [[maybe_unused]] auto start_time = std::chrono::steady_clock::now();
    // Validate preconditions
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    if (keys.size() != buffers.size() || keys.size() != sizes.size()) {
        LOG(ERROR) << "Input vector sizes mismatch: keys=" << keys.size()
                   << ", buffers=" << buffers.size()
                   << ", sizes=" << sizes.size();
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    const size_t num_keys = keys.size();
    std::vector<tl::expected<int64_t, ErrorCode>> results(num_keys);

    if (num_keys == 0) {
        return results;
    }

    // Query metadata for all keys
    const auto query_results = client_->BatchQuery(keys);

    // Process each key individually and prepare for batch transfer
    struct ValidKeyInfo {
        std::string key;
        size_t original_index;
        QueryResult query_result;
        std::vector<Slice> slices;
        uint64_t total_size;
    };
    struct DiskKeyInfo {
        std::string key;
        size_t original_index;
        QueryResult query_result;
        Replica::Descriptor replica;
        void *dst_buffer;
        uint64_t total_size;
    };

    std::vector<ValidKeyInfo> valid_operations;
    std::unordered_map<std::string, ValidKeyInfo> valid_local_disk_operations;
    std::vector<DiskKeyInfo> disk_operations;
    valid_operations.reserve(num_keys);

    auto local_endpoints = client_->GetLocalEndpoints();
    for (size_t i = 0; i < num_keys; ++i) {
        const auto &key = keys[i];

        // Handle query failures
        if (!query_results[i]) {
            const auto error = query_results[i].error();
            results[i] = tl::unexpected(error);
            if (error != ErrorCode::OBJECT_NOT_FOUND &&
                error != ErrorCode::REPLICA_IS_NOT_READY) {
                LOG(ERROR) << "Query failed for key '" << key
                           << "': " << toString(error);
            }
            continue;
        }

        // Validate replica list
        auto query_result_values = query_results[i].value();
        if (query_result_values.replicas.empty()) {
            LOG(ERROR) << "Empty replica list for key: " << key;
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }

        // Select best replica: prefer local MEMORY, any MEMORY, local NOF,
        // any NOF, LOCAL_DISK, DFS, then DISK.
        const auto *best_replica =
            SelectBestReplica(query_result_values.replicas, local_endpoints);
        if (!best_replica) {
            LOG(ERROR) << "No usable replica for key: " << key;
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }

        // Calculate required buffer size
        const auto replica = *best_replica;
        uint64_t total_size = calculate_total_size(replica);

        // Validate buffer capacity
        if (sizes[i] < total_size) {
            LOG(ERROR) << "Buffer too small for key '" << key
                       << "': required=" << total_size
                       << ", available=" << sizes[i];
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        if (replica.is_local_disk_replica()) {
            std::vector<Slice> key_slices;
            allocateSlices(key_slices, replica, buffers[i]);
            valid_local_disk_operations.emplace(
                key,
                ValidKeyInfo{.key = key,
                             .original_index = i,
                             .query_result = std::move(query_result_values),
                             .slices = std::move(key_slices),
                             .total_size = total_size});
            results[i] = static_cast<int64_t>(total_size);
            continue;
        }
        if (replica.is_disk_replica() || replica.is_dfs_replica()) {
            // File-backed replicas cannot safely read directly into arbitrary
            // user buffers. Use a contiguous temp buffer, then scatter.
            disk_operations.emplace_back(
                DiskKeyInfo{.key = key,
                            .original_index = i,
                            .query_result = std::move(query_result_values),
                            .replica = replica,
                            .dst_buffer = buffers[i],
                            .total_size = total_size});
            results[i] = static_cast<int64_t>(total_size);
            continue;
        }
        // MEMORY / NOF: RDMA directly to user buffer.
        std::vector<Slice> key_slices;
        allocateSlices(key_slices, replica, buffers[i]);
        valid_operations.push_back(
            {.key = key,
             .original_index = i,
             .query_result = FilterQueryResult(query_result_values, replica),
             .slices = std::move(key_slices),
             .total_size = total_size});

        // Set success result (actual bytes transferred)
        results[i] = static_cast<int64_t>(total_size);
    }

    // Early return if no valid operations
    if (valid_operations.empty() && valid_local_disk_operations.empty() &&
        disk_operations.empty()) {
        return results;
    }

    // Prepare batch transfer data structures
    std::vector<std::string> batch_keys;
    std::vector<QueryResult> batch_query_results;
    std::unordered_map<std::string, std::vector<Slice>> batch_slices;

    batch_keys.reserve(valid_operations.size());
    batch_query_results.reserve(valid_operations.size());

    for (const auto &op : valid_operations) {
        batch_keys.push_back(op.key);
        batch_query_results.push_back(op.query_result);
        batch_slices[op.key] = op.slices;
    }
    if (!valid_operations.empty()) {
        // Execute batch transfer
        const auto batch_get_results =
            client_->BatchGet(batch_keys, batch_query_results, batch_slices);

        // Process transfer results
        for (size_t j = 0; j < batch_get_results.size(); ++j) {
            const auto &op = valid_operations[j];

            if (!batch_get_results[j]) {
                const auto error = batch_get_results[j].error();
                LOG(ERROR) << "BatchGet failed for key '" << op.key
                           << "': " << toString(error);
                results[op.original_index] = tl::unexpected(error);
            }
        }
    }

    // ---- File-backed replicas: BatchGet into temp buffers, then scatter ----
    if (!disk_operations.empty()) {
        std::vector<std::string> disk_batch_keys;
        std::vector<QueryResult> disk_batch_qrs;
        std::unordered_map<std::string, std::vector<Slice>> disk_batch_slices;
        std::vector<size_t> disk_batch_indices;
        std::unordered_map<std::string, std::unique_ptr<BufferHandle>>
            disk_temp_handles;

        for (size_t di = 0; di < disk_operations.size(); ++di) {
            auto &op = disk_operations[di];
            const auto &replica = op.replica;
            const char *replica_type =
                replica.is_dfs_replica() ? "DFS" : "DISK";
            auto alloc_result =
                client_buffer_allocator_->allocate(op.total_size);
            if (!alloc_result) {
                LOG(ERROR) << "Failed to allocate temp buffer for "
                           << replica_type << " read, key: " << op.key
                           << ", size: " << op.total_size;
                results[op.original_index] =
                    tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
                continue;
            }
            auto handle =
                std::make_unique<BufferHandle>(std::move(*alloc_result));
            std::vector<Slice> disk_slices;
            allocateSlices(disk_slices, replica, handle->ptr());
            disk_batch_keys.push_back(op.key);
            disk_batch_qrs.push_back(
                FilterQueryResult(op.query_result, replica));
            disk_batch_slices[op.key] = std::move(disk_slices);
            disk_batch_indices.push_back(di);
            disk_temp_handles.emplace(op.key, std::move(handle));
        }

        if (!disk_batch_keys.empty()) {
            auto disk_results = client_->BatchGet(
                disk_batch_keys, disk_batch_qrs, disk_batch_slices);

            for (size_t di = 0; di < disk_batch_indices.size(); ++di) {
                const auto &key = disk_batch_keys[di];
                auto &op = disk_operations[disk_batch_indices[di]];
                auto handle_it = disk_temp_handles.find(key);
                const char *replica_type =
                    op.replica.is_dfs_replica() ? "DFS" : "DISK";
                if (!disk_results[di]) {
                    LOG(ERROR)
                        << replica_type << " BatchGet failed for key '" << key
                        << "': " << toString(disk_results[di].error());
                    results[op.original_index] =
                        tl::unexpected(disk_results[di].error());
                    continue;
                }
                if (auto r = scatter_host_to_maybe_device(
                        op.dst_buffer,
                        static_cast<char *>(handle_it->second->ptr()),
                        op.total_size,
                        std::string(replica_type) + " read, key: " + key);
                    !r) {
                    results[op.original_index] = tl::make_unexpected(r.error());
                }
            }
        }
    }

    // Prepare batch transfer data structures
    std::unordered_map<std::string,
                       std::unordered_map<std::string, std::vector<Slice>>>
        offload_objects;

    for (const auto &op_it : valid_local_disk_operations) {
        // Find the LOCAL_DISK replica from the list — replicas may be in
        // any order from Master.
        const Replica::Descriptor *replica_ptr = nullptr;
        for (const auto &r : op_it.second.query_result.replicas) {
            if (r.is_local_disk_replica()) {
                replica_ptr = &r;
                break;
            }
        }
        if (!replica_ptr) {
            LOG(ERROR) << "No LOCAL_DISK replica found for key: "
                       << op_it.first;
            continue;
        }
        const auto &replica = *replica_ptr;
        auto [store_segment_it, _] = offload_objects.try_emplace(
            replica.get_local_disk_descriptor().transport_endpoint);
        store_segment_it->second.emplace(op_it.first, op_it.second.slices);
    }

    [[maybe_unused]] size_t offload_object_count = 0;
    [[maybe_unused]] auto start_read_store_time =
        std::chrono::steady_clock::now();
    for (auto &offload_objects_it : offload_objects) {
        offload_object_count += offload_objects_it.second.size();
        auto batch_get_offload_result = batch_get_into_offload_object_internal(
            offload_objects_it.first, offload_objects_it.second);
        if (!batch_get_offload_result) {
            LOG(ERROR) << "Batch get store object failed with error: "
                       << batch_get_offload_result.error();
            for (const auto &offload_object_it : offload_objects_it.second) {
                results[valid_local_disk_operations.at(offload_object_it.first)
                            .original_index] =
                    tl::make_unexpected(batch_get_offload_result.error());
            }
            continue;
        }
        for (const auto &offload_object_it : offload_objects_it.second) {
            const auto &op =
                valid_local_disk_operations.at(offload_object_it.first);
            auto checksum_result = client_->VerifyObjectChecksum(
                offload_object_it.first, offload_object_it.second,
                op.total_size, op.query_result.object_checksum);
            if (!checksum_result) {
                results[op.original_index] =
                    tl::make_unexpected(checksum_result.error());
            }
        }
    }

    auto end_time = std::chrono::steady_clock::now();
    [[maybe_unused]] auto elapsed_time =
        std::chrono::duration_cast<std::chrono::microseconds>(end_time -
                                                              start_time)
            .count();
    [[maybe_unused]] auto read_store_time =
        std::chrono::duration_cast<std::chrono::microseconds>(
            end_time - start_read_store_time)
            .count();
    // LOG(INFO) << "Time taken for batch_get_into: " << elapsed_time
    //           << "us, read store: " << read_store_time
    //           << "us, with memory key count: " << valid_operations.size()
    //           << ", offload key count: " << offload_object_count;

    return results;
}

std::vector<tl::expected<bool, ErrorCode>> RealClient::batchIsExist_internal(
    const std::vector<std::string> &keys) {
    std::shared_lock<std::shared_mutex> dfs_read_lock(
        dfs_read_lifecycle_mutex_);
    if (closed_.load(std::memory_order_acquire) ||
        dfs_read_shutting_down_ || !client_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<tl::expected<bool, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    if (keys.empty()) {
        LOG(WARNING) << "Empty keys vector provided to batchIsExist_internal";
        return std::vector<tl::expected<bool, ErrorCode>>();
    }

    if (client_->DfsReadAllowed()) {
        return client_->BatchIsExist(keys);
    }

    return client_->BatchIsExistByReplicaType(keys, ReplicaType::MEMORY);
}

int RealClient::put_from_with_metadata(const std::string &key, void *buffer,
                                       void *metadata_buffer, size_t size,
                                       size_t metadata_size,
                                       const ReplicateConfig &config) {
    const auto start_time = std::chrono::steady_clock::now();
    // NOTE: The buffer address must resolve to Store-managed registered
    // memory for zero-copy RDMA operations to work correctly
    if (config.prefer_alloc_in_same_node) {
        LOG(ERROR) << "prefer_alloc_in_same_node is not supported.";
        return -1;
    }
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return -1;
    }

    if (size == 0) {
        LOG(WARNING) << "Attempting to put empty data for key: " << key;
        return 0;
    }

    // Create slices directly from the user buffer
    std::vector<mooncake::Slice> slices =
        split_into_slices(metadata_buffer, metadata_size);
    auto data_slices = split_into_slices(buffer, size);
    slices.insert(slices.end(), data_slices.begin(), data_slices.end());
    auto put_result = client_->Put(key, slices, config);
    if (!put_result) {
        LOG(ERROR) << "Put operation failed with error: "
                   << toString(put_result.error());
        return -toInt(put_result.error());
    }

    client_->ObserveTransferOperation(
        TransferOperationKind::kWrite, "put_from_with_metadata",
        size + metadata_size, elapsed_us_since(start_time));
    return 0;
}

std::vector<int> RealClient::batch_put_from_multi_buffers(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &sizes,
    const ReplicateConfig &config) {
    return batch_put_from_multi_buffers(keys, all_buffers, sizes, config,
                                        false);
}

std::vector<int> RealClient::batch_put_from_multi_buffers(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &sizes,
    const ReplicateConfig &config, bool stage_nonlocal) {
    auto internal_results =
        execute_timed_operation<std::vector<tl::expected<void, ErrorCode>>>(
            [&]() {
                return batch_put_from_multi_buffers_internal(
                    keys, all_buffers, sizes, config, stage_nonlocal);
            },
            [](const auto &) { return true; },
            [&](uint64_t latency_us, const auto &ret) {
                std::vector<int> py_results;
                py_results.reserve(ret.size());
                for (const auto &item : ret) {
                    py_results.push_back(to_py_ret(item));
                }
                client_->ObserveTransferOperation(
                    TransferOperationKind::kWrite,
                    "batch_put_from_multi_buffers",
                    sum_successful_nested_sizes(py_results, sizes), latency_us);
            });
    return ToPyResults(internal_results);
}

std::vector<int> RealClient::batch_get_session_start(
    const std::vector<std::string> &keys) {
    return batch_get_session_start_with_sources(keys).codes;
}

GetSessionStartResult
RealClient::batch_get_session_start_with_sources(
    const std::vector<std::string> &keys) {
    GetSessionStartResult result{
        std::vector<int>(
            keys.size(), static_cast<int>(toInt(ErrorCode::INVALID_PARAMS))),
        std::vector<std::string>(keys.size(), "unknown"), false};
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return result;
    }
    if (keys.empty()) return result;

    // Master interaction only here: query replicas + lease.
    const auto query_results = client_->BatchQuery(keys);
    if (query_results.size() != keys.size()) {
        LOG(ERROR) << "Session query result size mismatch: expected="
                   << keys.size() << ", got=" << query_results.size();
        result.codes.assign(keys.size(),
                            static_cast<int>(toInt(ErrorCode::RPC_FAIL)));
        return result;
    }
    auto local_endpoints = client_->GetLocalEndpoints();
    const bool record_access = client_->MetricsEnabled();
    result.all_memory = true;

    const bool prefetch_cache_enabled = dfs_prefetch_arena_available();
    std::vector<std::shared_ptr<BufferHandle>> detached_prefetch_buffers;
    if (prefetch_cache_enabled) {
        detached_prefetch_buffers.reserve(keys.size());
    }
    std::lock_guard<std::mutex> lock(session_mutex_);
    for (size_t i = 0; i < keys.size(); ++i) {
        if (record_access) {
            get_session_access_records_.erase(keys[i]);
        }
        // A get session owns its prefetch buffer. Starting a new lease must
        // not inherit pinned bytes from an older lookup of the same key.
        if (prefetch_cache_enabled) {
            DetachPrefetchedSessionBuffer(get_session_prefetch_cache_, keys[i],
                                          detached_prefetch_buffers);
        }
        if (!query_results[i]) {
            result.all_memory = false;
            result.codes[i] =
                static_cast<int>(toInt(query_results[i].error()));
            get_sessions_.erase(keys[i]);
            continue;
        }

        const auto &query_result = query_results[i].value();
        if (query_result.IsLeaseExpired()) {
            result.all_memory = false;
            result.codes[i] =
                static_cast<int>(toInt(ErrorCode::LEASE_EXPIRED));
            get_sessions_.erase(keys[i]);
            continue;
        }

        const auto *replica = SelectSessionReplica(
            query_result.replicas, local_endpoints,
            client_->DfsReadAllowed());
        if (!replica) {
            result.all_memory = false;
            LOG(ERROR) << "No supported complete session replica for key: "
                       << keys[i];
            result.codes[i] =
                static_cast<int>(toInt(ErrorCode::INVALID_REPLICA));
            get_sessions_.erase(keys[i]);
            continue;
        }
        result.all_memory &= replica->is_memory_replica();

        // QueryResult members are const: erase + emplace (no operator=).
        get_sessions_.erase(keys[i]);
        get_sessions_.emplace(keys[i],
                              FilterQueryResult(query_result, *replica));
        result.sources[i] = DirectSourceForReplica(*replica);
        result.codes[i] = 0;
    }
    return result;
}

bool RealClient::all_get_sessions_memory(
    const std::vector<std::string> &keys) const {
    if (keys.empty()) return false;
    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(session_mutex_);
    for (const auto &key : keys) {
        const auto it = get_sessions_.find(key);
        if (it == get_sessions_.end() || it->second.IsLeaseExpired(now) ||
            it->second.replicas.size() != 1 ||
            !it->second.replicas.front().is_memory_replica())
            return false;
    }
    return true;
}

std::vector<int> RealClient::batch_get_session_refresh(
    const std::vector<std::string> &keys) {
    std::vector<int> results(
        keys.size(), static_cast<int>(toInt(ErrorCode::INVALID_PARAMS)));
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return results;
    }
    if (keys.empty()) return {};

    struct PendingRefresh {
        std::string key;
        size_t result_index;
        QueryResult old_session;
    };
    const bool prefetch_cache_enabled = dfs_prefetch_arena_available();
    std::vector<PendingRefresh> pending;
    pending.reserve(keys.size());
    std::vector<std::shared_ptr<BufferHandle>> detached_prefetch_buffers;
    if (prefetch_cache_enabled) {
        detached_prefetch_buffers.reserve(keys.size());
    }
    std::vector<size_t> duplicate_of(keys.size(), keys.size());
    std::unordered_map<std::string, size_t> first_index_by_key;
    first_index_by_key.reserve(keys.size());

    {
        std::lock_guard<std::mutex> lock(session_mutex_);
        for (size_t i = 0; i < keys.size(); ++i) {
            const auto [first_it, inserted] =
                first_index_by_key.emplace(keys[i], i);
            if (!inserted) {
                duplicate_of[i] = first_it->second;
                continue;
            }

            auto session_it = get_sessions_.find(keys[i]);
            if (session_it == get_sessions_.end()) {
                // Do not leave an orphaned pinned object if the session was
                // already retired by a concurrent expiry/end path.
                if (prefetch_cache_enabled) {
                    DetachPrefetchedSessionBuffer(
                        get_session_prefetch_cache_, keys[i],
                        detached_prefetch_buffers);
                }
                continue;
            }
            pending.push_back(
                PendingRefresh{keys[i], i, session_it->second});
        }
    }

    if (!pending.empty()) {
        std::vector<std::string> query_keys;
        query_keys.reserve(pending.size());
        for (const auto &entry : pending) query_keys.push_back(entry.key);

        // Unlike batch_get_session_start, this fresh metadata query is only
        // committed after checking that the existing session is unchanged.
        const auto fresh_queries = client_->BatchQuery(query_keys);
        const auto local_endpoints = client_->GetLocalEndpoints();
        for (size_t i = 0; i < pending.size(); ++i) {
            const auto &entry = pending[i];
            ErrorCode refresh_error = ErrorCode::OK;
            std::optional<QueryResult> refreshed_session;

            if (fresh_queries.size() != pending.size()) {
                refresh_error = ErrorCode::INTERNAL_ERROR;
            } else if (!fresh_queries[i]) {
                refresh_error = fresh_queries[i].error();
            } else {
                const auto &fresh_query = fresh_queries[i].value();
                if (fresh_query.IsLeaseExpired()) {
                    refresh_error = ErrorCode::LEASE_EXPIRED;
                } else {
                    const auto *replica = SelectSessionReplica(
                        fresh_query.replicas, local_endpoints,
                        client_->DfsReadAllowed());
                    if (!replica) {
                        refresh_error = ErrorCode::INVALID_REPLICA;
                    } else {
                        refreshed_session.emplace(
                            FilterQueryResult(fresh_query, *replica));
                    }
                }
            }

            std::lock_guard<std::mutex> lock(session_mutex_);
            auto current = get_sessions_.find(entry.key);
            if (current == get_sessions_.end() ||
                !SameGetSessionSnapshot(current->second,
                                        entry.old_session)) {
                // A concurrent end/start/refresh owns the current map entry.
                // Never erase or overwrite that newer session.
                results[entry.result_index] =
                    static_cast<int>(toInt(ErrorCode::INVALID_PARAMS));
                continue;
            }

            if (refresh_error == ErrorCode::OK &&
                refreshed_session->IsLeaseExpired()) {
                refresh_error = ErrorCode::LEASE_EXPIRED;
            }

            if (prefetch_cache_enabled) {
                auto cache_it = get_session_prefetch_cache_.find(entry.key);
                if (refresh_error == ErrorCode::OK &&
                    cache_it != get_session_prefetch_cache_.end()) {
                    const auto &cached = cache_it->second;
                    if (!cached.buffer_handle || !cached.buffer_handle->ptr() ||
                        cached.total_size > cached.buffer_handle->size() ||
                        !CompatibleSessionCacheRefresh(current->second,
                                                       *refreshed_session)) {
                        // Prefetched host bytes belong to the old exact replica
                        // generation. Fail closed instead of pairing them with a
                        // new replica or an unverifiable descriptor.
                        refresh_error = ErrorCode::INVALID_REPLICA;
                    }
                }
            }

            if (refresh_error != ErrorCode::OK) {
                get_sessions_.erase(current);
                if (prefetch_cache_enabled) {
                    DetachPrefetchedSessionBuffer(
                        get_session_prefetch_cache_, entry.key,
                        detached_prefetch_buffers);
                }
                get_session_access_records_.erase(entry.key);
                results[entry.result_index] =
                    static_cast<int>(toInt(refresh_error));
                continue;
            }

            const std::string source =
                DirectSourceForReplica(refreshed_session->replicas.front());
            get_sessions_.erase(current);
            get_sessions_.emplace(entry.key, std::move(*refreshed_session));
            auto access_it = get_session_access_records_.find(entry.key);
            if (access_it != get_session_access_records_.end()) {
                access_it->second.source = source;
            }
            results[entry.result_index] = 0;
        }
    }

    for (size_t i = 0; i < duplicate_of.size(); ++i) {
        if (duplicate_of[i] < keys.size()) {
            results[i] = results[duplicate_of[i]];
        }
    }
    return results;
}

bool RealClient::dfs_prefetch_arena_available() const {
    return prefetch_arena_available_.load(std::memory_order_acquire);
}

bool RealClient::disable_dfs_prefetch() {
    // This is a startup-only operation. The exclusive lifecycle lock drains
    // any DFS range read that could be allocating from the arena. Callers
    // must also serialize against session creation, which uses session_mutex_
    // but does not take this lifecycle lock.
    std::unique_lock<std::shared_mutex> dfs_read_lock(
        dfs_read_lifecycle_mutex_);
    if (!prefetch_arena_available_.load(std::memory_order_acquire) &&
        (!file_storage_ || !file_storage_->HasPinnedPrefetchArena())) {
        return true;
    }
    {
        std::lock_guard<std::mutex> session_lock(session_mutex_);
        if (!get_sessions_.empty() || !get_session_access_records_.empty() ||
            !get_session_prefetch_cache_.empty() || !put_sessions_.empty()) {
            return false;
        }
    }

    if (file_storage_ && !file_storage_->ReleasePinnedPrefetchArena()) {
        return false;
    }
    prefetch_arena_available_.store(false, std::memory_order_release);
    prefetch_arena_stats_.reset();
    return true;
}

std::string RealClient::dfs_prefetch_arena_status() const {
    if (!file_storage_) {
        return "unavailable: no FileStorage (SSD offload disabled)";
    }
    return file_storage_->PinnedPrefetchArenaStatus();
}

std::shared_ptr<BufferHandle> RealClient::AllocatePrefetchArenaRegion(
    size_t size, size_t alignment) {
    auto stats = prefetch_arena_stats_;
    if (!dfs_prefetch_arena_available() || !stats) {
        if (stats) {
            stats->alloc_fail.fetch_add(1, std::memory_order_relaxed);
        }
        if (stats && !stats->unavailable_logged.exchange(true)) {
            LOG(WARNING) << "DFS prefetch staging arena unavailable ("
                         << dfs_prefetch_arena_status()
                         << "); waiting-queue DFS prefetch reads will fail";
        } else if (!stats) {
            LOG_EVERY_N(WARNING, 64)
                << "DFS prefetch staging arena unavailable ("
                << dfs_prefetch_arena_status()
                << "); waiting-queue DFS prefetch reads will fail";
        }
        return nullptr;
    }

    auto allocation =
        file_storage_->AllocatePinnedPrefetchBuffer(size, alignment);
    const auto report = file_storage_->PinnedPrefetchArenaReport();
    const uint64_t capacity = file_storage_->PinnedPrefetchArenaCapacity();
    const uint64_t occupied =
        capacity > report.totalFreeSpace ? capacity - report.totalFreeSpace : 0;
    std::shared_ptr<BufferHandle> region;
    if (!allocation) {
        const uint64_t failures =
            stats->alloc_fail.fetch_add(1, std::memory_order_relaxed) + 1;
        LOG_EVERY_N(WARNING, 64)
            << "DFS prefetch arena exhausted: request_bytes=" << size
            << ", occupied_bytes=" << occupied
            << ", free_bytes=" << report.totalFreeSpace
            << ", largest_free_region_lower_bound=" << report.largestFreeRegion
            << ", capacity=" << capacity << ", failures=" << failures;
    } else {
        stats->alloc_ok.fetch_add(1, std::memory_order_relaxed);
        stats->requested_bytes_in_use.fetch_add(size,
                                                std::memory_order_relaxed);
        uint64_t peak =
            stats->occupied_bytes_observed_peak.load(std::memory_order_relaxed);
        while (occupied > peak &&
               !stats->occupied_bytes_observed_peak.compare_exchange_weak(
                   peak, occupied, std::memory_order_relaxed)) {
        }
        // The root region is shared by every key view of this read. Its
        // deleter runs after the last view is gone: returning the region to
        // the arena first, then the accounting. Only the stats object is
        // captured, so release never depends on FileStorage lifetime.
        region = std::shared_ptr<BufferHandle>(
            new BufferHandle(std::move(*allocation)),
            [stats, size](BufferHandle *handle) {
                delete handle;
                stats->requested_bytes_in_use.fetch_sub(
                    size, std::memory_order_relaxed);
            });
    }
    uint64_t largest_min =
        stats->largest_free_region_min.load(std::memory_order_relaxed);
    while (
        report.largestFreeRegion < largest_min &&
        !stats->largest_free_region_min.compare_exchange_weak(
            largest_min, report.largestFreeRegion, std::memory_order_relaxed)) {
    }

    // Always-on summary, at most every 30 s while prefetch is active.
    constexpr int64_t kSummaryIntervalNs = 30LL * 1000 * 1000 * 1000;
    const int64_t now_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count();
    int64_t last_ns = stats->last_summary_ns.load(std::memory_order_relaxed);
    if (now_ns - last_ns >= kSummaryIntervalNs &&
        stats->last_summary_ns.compare_exchange_strong(
            last_ns, now_ns, std::memory_order_relaxed)) {
        LOG(INFO)
            << "DFS prefetch arena summary: capacity=" << capacity
            << ", alloc_ok=" << stats->alloc_ok.load(std::memory_order_relaxed)
            << ", alloc_fail="
            << stats->alloc_fail.load(std::memory_order_relaxed)
            << ", requested_bytes_in_use="
            << stats->requested_bytes_in_use.load(std::memory_order_relaxed)
            << ", occupied_bytes=" << occupied
            << ", occupied_bytes_observed_peak="
            << stats->occupied_bytes_observed_peak.load(
                   std::memory_order_relaxed)
            << ", largest_free_region_lower_bound=" << report.largestFreeRegion
            << ", largest_free_region_min="
            << stats->largest_free_region_min.load(std::memory_order_relaxed);
    }
    return region;
}

std::vector<int> RealClient::batch_get_session_prefetch(
    const std::vector<std::string> &keys) {
    std::vector<int> results(
        keys.size(), static_cast<int>(toInt(ErrorCode::INVALID_PARAMS)));
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return results;
    }
    if (keys.empty()) return {};

    auto now = std::chrono::steady_clock::now();
    std::vector<std::shared_ptr<BufferHandle>> detached_prefetch_buffers;
    detached_prefetch_buffers.reserve(keys.size());
    std::vector<NonMemReadEntry> entries;
    entries.reserve(keys.size());
    std::vector<size_t> duplicate_of(keys.size(), keys.size());
    std::unordered_map<std::string, size_t> first_index_by_key;
    first_index_by_key.reserve(keys.size());

    {
        std::lock_guard<std::mutex> lock(session_mutex_);
        for (size_t i = 0; i < keys.size(); ++i) {
            const auto [first_it, inserted] =
                first_index_by_key.emplace(keys[i], i);
            if (!inserted) {
                duplicate_of[i] = first_it->second;
                continue;
            }

            auto session_it = get_sessions_.find(keys[i]);
            if (session_it == get_sessions_.end()) continue;
            if (session_it->second.IsLeaseExpired(now)) {
                results[i] =
                    static_cast<int>(toInt(ErrorCode::LEASE_EXPIRED));
                get_sessions_.erase(session_it);
                DetachPrefetchedSessionBuffer(
                    get_session_prefetch_cache_, keys[i],
                    detached_prefetch_buffers);
                continue;
            }
            if (session_it->second.replicas.empty()) {
                results[i] =
                    static_cast<int>(toInt(ErrorCode::INVALID_REPLICA));
                continue;
            }

            const auto &replica = session_it->second.replicas.front();
            if (!replica.is_dfs_replica()) {
                // The ordinary session range-get path handles memory replicas.
                results[i] = 0;
                continue;
            }
            results[i] = 0;
            entries.push_back(NonMemReadEntry{
                keys[i], i, replica, session_it->second, {}, {}, {},
                session_it->second.lease_timeout});
        }
    }

    std::vector<NonMemReadEntry *> dfs_entries;
    dfs_entries.reserve(entries.size());
    for (auto &entry : entries) dfs_entries.push_back(&entry);
    if (!dfs_entries.empty()) {
        // This call blocks only its caller. SGLang submits it from a bounded
        // background prefetch worker, while scheduler admission waits for the
        // prefetch to reach READY on every rank before consuming the session.
        const uint64_t trace_id =
            dfs_read_trace_enabled() ? NextDfsReadTraceId() : uint64_t{0};
        process_session_disk_dfs_reads(dfs_entries, results, trace_id,
                                       /*prefetch_only=*/true);
    }

    // A concurrent session end or lease expiry can retire the buffer while
    // the DFS read is in progress. Never report READY unless the session
    // still owns a complete object view when this method returns.
    {
        std::lock_guard<std::mutex> lock(session_mutex_);
        auto completed_at = std::chrono::steady_clock::now();
        for (const auto &entry : entries) {
            const size_t i = entry.original_idx;
            if (results[i] != 0) continue;

            auto session_it = get_sessions_.find(entry.key);
            if (session_it == get_sessions_.end()) {
                results[i] =
                    static_cast<int>(toInt(ErrorCode::INVALID_PARAMS));
                continue;
            }
            if (!CompatibleSessionCacheRefresh(entry.query_result,
                                               session_it->second)) {
                // A concurrent lease refresh may have replaced the selected
                // replica while the DFS read was in flight. Do not report this
                // old read as READY for the new session or leave its bytes in
                // the key-indexed prefetch buffer.
                DetachPrefetchedSessionBuffer(
                    get_session_prefetch_cache_, entry.key,
                    detached_prefetch_buffers);
                results[i] =
                    static_cast<int>(toInt(ErrorCode::INVALID_REPLICA));
                continue;
            }
            if (session_it->second.IsLeaseExpired(completed_at)) {
                results[i] =
                    static_cast<int>(toInt(ErrorCode::LEASE_EXPIRED));
                get_sessions_.erase(session_it);
                DetachPrefetchedSessionBuffer(
                    get_session_prefetch_cache_, entry.key,
                    detached_prefetch_buffers);
                continue;
            }

            auto cache_it = get_session_prefetch_cache_.find(entry.key);
            const auto &descriptor = entry.replica.get_dfs_descriptor();
            const uint64_t source_size =
                descriptor.aligned_size > 0 ? descriptor.aligned_size
                                            : descriptor.object_size;
            if (cache_it == get_session_prefetch_cache_.end() ||
                !cache_it->second.buffer_handle ||
                !cache_it->second.buffer_handle->ptr() ||
                source_size < descriptor.object_size ||
                source_size > std::numeric_limits<size_t>::max() ||
                static_cast<size_t>(source_size) >
                    cache_it->second.buffer_handle->size()) {
                results[i] =
                    static_cast<int>(toInt(ErrorCode::INTERNAL_ERROR));
            }
        }
    }

    for (size_t i = 0; i < duplicate_of.size(); ++i) {
        if (duplicate_of[i] < keys.size()) {
            results[i] = results[duplicate_of[i]];
        }
    }
    return results;
}

void RealClient::record_prefetched_tokens(uint64_t tokens) {
    if (client_ != nullptr) {
        client_->ObservePrefetchedTokens(tokens);
    }
}

bool RealClient::validate_session_range_batch_arguments(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &all_sizes,
    const std::vector<std::vector<size_t>> &all_src_offsets) const {
    if (client_ && keys.size() == all_buffers.size() &&
        keys.size() == all_sizes.size() &&
        keys.size() == all_src_offsets.size()) {
        return true;
    }
    LOG(ERROR) << "Invalid get ranges args";
    return false;
}

std::vector<SessionRangeReadRequest>
RealClient::prepare_session_range_read_requests(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &all_sizes,
    const std::vector<std::vector<size_t>> &all_src_offsets,
    std::vector<int> &results, SessionRangeReadContext &context) {
    std::vector<SessionRangeReadRequest> requests;
    requests.reserve(keys.size());
    if (context.record_access) {
        context.access_sources.resize(keys.size());
    }

    const bool prefetch_cache_enabled = dfs_prefetch_arena_available();
    std::vector<std::shared_ptr<BufferHandle>> detached_prefetch_buffers;
    if (prefetch_cache_enabled) {
        detached_prefetch_buffers.reserve(keys.size());
    }
    std::lock_guard<std::mutex> lock(session_mutex_);
    // Retire orphaned prefetch buffers if an exceptional session path skipped
    // its normal cleanup.
    if (prefetch_cache_enabled) {
        for (auto it = get_session_prefetch_cache_.begin();
             it != get_session_prefetch_cache_.end();) {
            if (get_sessions_.find(it->first) == get_sessions_.end()) {
                ++context.cache_evicted_count;
                client_->ObserveDirectSessionCacheEviction();
                detached_prefetch_buffers.push_back(
                    std::move(it->second.buffer_handle));
                it = get_session_prefetch_cache_.erase(it);
            } else {
                ++it;
            }
        }
    }
    context.cache_gc_done = std::chrono::steady_clock::now();

    auto now = std::chrono::steady_clock::now();
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto &buffers = all_buffers[i];
        const auto &sizes = all_sizes[i];
        const auto &offsets = all_src_offsets[i];
        if (buffers.size() != sizes.size() ||
            buffers.size() != offsets.size()) {
            continue;
        }

        auto session_it = get_sessions_.find(keys[i]);
        if (session_it == get_sessions_.end()) {
            continue;
        }
        if (session_it->second.replicas.size() != 1) {
            results[i] = static_cast<int>(toInt(ErrorCode::INVALID_REPLICA));
            continue;
        }

        const auto &replica = session_it->second.replicas.front();
        if (context.record_access) {
            context.access_sources[i] = DirectSourceForReplica(replica);
        }
        if (session_it->second.IsLeaseExpired(now)) {
            if (context.record_access) {
                const std::string source = DirectSourceForReplica(replica);
                if (source != "unknown") {
                    auto [record_it, inserted] =
                        get_session_access_records_.try_emplace(
                            keys[i],
                            GetSessionAccessRecord{source, false, 0, {}});
                    record_it->second.success = false;
                }
            }
            get_sessions_.erase(session_it);
            if (prefetch_cache_enabled) {
                DetachPrefetchedSessionBuffer(
                    get_session_prefetch_cache_, keys[i],
                    detached_prefetch_buffers);
            }
            results[i] = static_cast<int>(toInt(ErrorCode::LEASE_EXPIRED));
            continue;
        }

        uint64_t replica_limit = 0;
        if (replica.is_memory_replica()) {
            replica_limit =
                replica.get_memory_descriptor().buffer_descriptor.size_;
        } else if (replica.is_dfs_replica()) {
            replica_limit = calculate_total_size(replica);
        }
        if (replica.is_memory_replica() || replica.is_dfs_replica()) {
            bool overflow = false;
            for (size_t j = 0; j < buffers.size(); ++j) {
                if (is_object_range_overflow(offsets[j], sizes[j],
                                             replica_limit)) {
                    overflow = true;
                    break;
                }
            }
            if (overflow) {
                results[i] = static_cast<int>(toInt(ErrorCode::INVALID_PARAMS));
                continue;
            }
        }

        results[i] = 0;
        requests.push_back(SessionRangeReadRequest{
            keys[i], i, replica, session_it->second,
            std::vector<void *>(buffers.begin(), buffers.end()),
            std::vector<size_t>(sizes.begin(), sizes.end()),
            std::vector<size_t>(offsets.begin(), offsets.end()),
            session_it->second.lease_timeout});
    }
    return requests;
}

SessionRangeReadPlan RealClient::classify_session_range_read_requests(
    std::vector<SessionRangeReadRequest> requests,
    std::vector<int> &results) const {
    SessionRangeReadPlan plan;
    plan.memory_requests.reserve(requests.size());
    plan.dfs_requests.reserve(requests.size());
    for (auto &request : requests) {
        if (request.replica.is_memory_replica()) {
            plan.memory_requests.push_back(std::move(request));
        } else if (request.replica.is_dfs_replica()) {
            plan.dfs_requests.push_back(std::move(request));
        } else {
            results[request.original_idx] =
                static_cast<int>(toInt(ErrorCode::INVALID_REPLICA));
        }
    }
    return plan;
}

void RealClient::execute_session_memory_range_reads(
    const std::vector<SessionRangeReadRequest> &requests,
    std::vector<int> &results) {
    if (requests.empty()) {
        return;
    }

    std::vector<Replica::Descriptor> replicas;
    std::vector<std::vector<Slice>> slices;
    std::vector<std::vector<uint64_t>> src_offsets;
    replicas.reserve(requests.size());
    slices.reserve(requests.size());
    src_offsets.reserve(requests.size());
    for (const auto &request : requests) {
        replicas.push_back(request.replica);
        std::vector<Slice> entry_slices;
        std::vector<uint64_t> entry_offsets;
        entry_slices.reserve(request.buffers.size());
        entry_offsets.reserve(request.src_offsets.size());
        for (size_t i = 0; i < request.buffers.size(); ++i) {
            entry_slices.emplace_back(
                Slice{request.buffers[i], request.sizes[i]});
            entry_offsets.push_back(
                static_cast<uint64_t>(request.src_offsets[i]));
        }
        slices.push_back(std::move(entry_slices));
        src_offsets.push_back(std::move(entry_offsets));
    }

    const auto transfer =
        client_->BatchTransferReadRanges(replicas, slices, src_offsets);
    std::lock_guard<std::mutex> lock(session_mutex_);
    const auto now = std::chrono::steady_clock::now();
    if (transfer.size() != requests.size()) {
        LOG(ERROR) << "Session memory range result size mismatch: expected="
                   << requests.size() << ", got=" << transfer.size();
    }
    const size_t completed = std::min(transfer.size(), requests.size());
    for (size_t i = 0; i < completed; ++i) {
        const auto &request = requests[i];
        if (transfer[i]) {
            if (now >= request.lease_deadline) {
                results[request.original_idx] =
                    static_cast<int>(toInt(ErrorCode::LEASE_EXPIRED));
                get_sessions_.erase(request.key);
            } else {
                results[request.original_idx] =
                    static_cast<int>(transfer[i].value());
            }
        } else {
            results[request.original_idx] =
                static_cast<int>(toInt(transfer[i].error()));
        }
    }
    for (size_t i = completed; i < requests.size(); ++i) {
        results[requests[i].original_idx] =
            static_cast<int>(toInt(ErrorCode::INTERNAL_ERROR));
    }
}

void RealClient::record_session_range_accesses(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &all_sizes,
    const std::vector<std::vector<size_t>> &all_src_offsets,
    const std::vector<int> &results, const SessionRangeReadContext &context) {
    if (!context.record_access) {
        return;
    }

    std::lock_guard<std::mutex> lock(session_mutex_);
    for (size_t i = 0; i < keys.size(); ++i) {
        auto session_it = get_sessions_.find(keys[i]);
        if (session_it == get_sessions_.end()) {
            auto record_it = get_session_access_records_.find(keys[i]);
            if (record_it != get_session_access_records_.end()) {
                record_it->second.success = false;
            }
            if (results[i] ==
                    static_cast<int>(toInt(ErrorCode::LEASE_EXPIRED)) &&
                context.access_sources[i] != "unknown") {
                auto [insert_it, inserted] =
                    get_session_access_records_.try_emplace(
                        keys[i], GetSessionAccessRecord{
                                     context.access_sources[i], false, 0, {}});
                insert_it->second.success = false;
            }
            continue;
        }
        if (session_it->second.replicas.empty() ||
            all_buffers[i].size() != all_sizes[i].size() ||
            all_buffers[i].size() != all_src_offsets[i].size()) {
            continue;
        }

        const std::string source =
            DirectSourceForReplica(session_it->second.replicas.front());
        if (source == "unknown") {
            continue;
        }

        auto [record_it, inserted] = get_session_access_records_.try_emplace(
            keys[i], GetSessionAccessRecord{source, true, 0, {}});
        auto &record = record_it->second;
        if (!inserted && record.source != source) {
            record.success = false;
        }
        uint64_t expected_bytes = 0;
        for (size_t size : all_sizes[i]) {
            expected_bytes += size;
        }
        const bool range_success =
            results[i] >= 0 &&
            static_cast<uint64_t>(results[i]) == expected_bytes;
        record.success = record.success && range_success;
        if (range_success) {
            for (size_t j = 0; j < all_sizes[i].size(); ++j) {
                const auto range =
                    std::make_pair(static_cast<uint64_t>(all_src_offsets[i][j]),
                                   static_cast<uint64_t>(all_sizes[i][j]));
                if (record.ranges.insert(range).second) {
                    record.bytes += all_sizes[i][j];
                }
            }
        }
    }
}

void RealClient::trace_session_range_reads(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<size_t>> &all_sizes, size_t memory_read_count,
    size_t dfs_read_count, const SessionRangeReadContext &context) const {
    if (context.trace_id == 0) {
        return;
    }

    uint64_t total_bytes = 0;
    uint64_t total_ranges = 0;
    for (const auto &sizes : all_sizes) {
        for (size_t size : sizes) {
            total_bytes += size;
            ++total_ranges;
        }
    }
    auto elapsed_us = [](const auto &start, const auto &end) {
        return std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                     start)
            .count();
    };
    LOG(INFO) << "batch_get_into_multi_buffer_ranges: trace_id="
              << context.trace_id << ", keys=" << keys.size()
              << ", total_ranges=" << total_ranges
              << ", total_bytes=" << total_bytes
              << ", mem_reads=" << memory_read_count
              << ", cache_evicted=" << context.cache_evicted_count
              << ", dfs_reads=" << dfs_read_count << ", gc_us="
              << elapsed_us(context.timing_start, context.cache_gc_done)
              << ", session_mem_us="
              << elapsed_us(context.cache_gc_done, context.memory_done)
              << ", disk_access_us="
              << elapsed_us(context.memory_done, context.access_done)
              << ", total_us="
              << elapsed_us(context.timing_start, context.access_done);
}

std::vector<int> RealClient::batch_get_into_multi_buffer_ranges(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &all_sizes,
    const std::vector<std::vector<size_t>> &all_src_offsets) {
    std::vector<int> results(
        keys.size(), static_cast<int>(toInt(ErrorCode::INVALID_PARAMS)));
    if (!validate_session_range_batch_arguments(keys, all_buffers, all_sizes,
                                                all_src_offsets)) {
        return results;
    }

    SessionRangeReadContext context;
    context.timing_start = std::chrono::steady_clock::now();
    context.cache_gc_done = context.timing_start;
    context.trace_id =
        dfs_read_trace_enabled() ? NextDfsReadTraceId() : uint64_t{0};
    context.record_access = client_->MetricsEnabled();

    auto requests = prepare_session_range_read_requests(
        keys, all_buffers, all_sizes, all_src_offsets, results, context);
    auto plan =
        classify_session_range_read_requests(std::move(requests), results);
    execute_session_memory_range_reads(plan.memory_requests, results);
    context.memory_done = std::chrono::steady_clock::now();
    execute_session_dfs_range_reads(plan.dfs_requests, results,
                                    context.trace_id);
    record_session_range_accesses(keys, all_buffers, all_sizes, all_src_offsets,
                                  results, context);
    context.access_done = std::chrono::steady_clock::now();
    trace_session_range_reads(keys, all_sizes, plan.memory_requests.size(),
                              plan.dfs_requests.size(), context);
    return results;
}

int RealClient::batch_get_session_end(const std::vector<std::string> &keys) {
    struct DirectAccessObservation {
        std::string source;
        bool success;
        uint64_t bytes;
    };
    const bool record_access = client_ && client_->MetricsEnabled();
    std::vector<DirectAccessObservation> access_observations;
    if (record_access) {
        access_observations.reserve(keys.size());
    }
    const bool prefetch_cache_enabled = dfs_prefetch_arena_available();
    std::vector<std::shared_ptr<BufferHandle>> detached_prefetch_buffers;
    if (prefetch_cache_enabled) {
        detached_prefetch_buffers.reserve(keys.size());
    }
    {
        std::lock_guard<std::mutex> lock(session_mutex_);
        std::unordered_set<std::string> ended_keys;
        if (record_access) {
            ended_keys.reserve(keys.size());
        }
        for (const auto &key : keys) {
            if (record_access) {
                if (!ended_keys.insert(key).second) continue;
                auto access_it = get_session_access_records_.find(key);
                if (access_it != get_session_access_records_.end()) {
                    const auto &record = access_it->second;
                    access_observations.push_back(
                        {record.source, record.success,
                         record.success ? record.bytes : 0});
                    get_session_access_records_.erase(access_it);
                }
            }
            get_sessions_.erase(key);
            if (prefetch_cache_enabled) {
                DetachPrefetchedSessionBuffer(
                    get_session_prefetch_cache_, key,
                    detached_prefetch_buffers);
            }
        }
    }
    for (const auto &observation : access_observations) {
        client_->ObserveDirectAccess(observation.source, observation.success,
                                     observation.bytes);
    }
    return 0;
}

void RealClient::process_session_disk_dfs_reads(
    std::vector<NonMemReadEntry *> &entries, std::vector<int> &results,
    uint64_t trace_id, bool prefetch_only) {
    std::vector<SessionRangeReadRequest> requests;
    requests.reserve(entries.size());
    for (const auto *entry : entries) {
        requests.push_back(*entry);
    }
    execute_session_dfs_range_reads(requests, results, trace_id, prefetch_only);
}

void RealClient::execute_session_dfs_range_reads(
    std::vector<SessionRangeReadRequest> &requests, std::vector<int> &results,
    uint64_t trace_id, bool prefetch_only) {
    if (requests.empty()) return;

    std::vector<SessionRangeReadRequest *> entries;
    entries.reserve(requests.size());
    for (auto &request : requests) entries.push_back(&request);

    std::shared_lock<std::shared_mutex> dfs_read_lock(
        dfs_read_lifecycle_mutex_);
    if (closed_.load(std::memory_order_acquire) || dfs_read_shutting_down_ ||
        !dfs_h2d_stream_pool_) {
        for (auto *entry : entries) {
            results[entry->original_idx] =
                static_cast<int>(toInt(ErrorCode::TRANSFER_FAIL));
        }
        return;
    }

    if (prefetch_only) {
        execute_session_dfs_range_reads_batch(
            entries, results, trace_id, true, 1, 1, 0, 0);
        return;
    }

    constexpr size_t kDefaultRestoreChunkSize = size_t{1} << 30;
    using Variables = FileStorageEnvironmentVariables;
    const int64_t configured_chunk_size = Environ::ReadOr(
        Variables::MC_STORE_DFS_RESTORE_CHUNK_SIZE_BYTES,
        static_cast<int64_t>(kDefaultRestoreChunkSize));
    const size_t chunk_limit_bytes =
        configured_chunk_size > 0
            ? static_cast<size_t>(configured_chunk_size)
            : kDefaultRestoreChunkSize;
    if (configured_chunk_size <= 0) {
        LOG(WARNING) << Variables::MC_STORE_DFS_RESTORE_CHUNK_SIZE_BYTES.name
                     << " must be positive; using default "
                     << kDefaultRestoreChunkSize;
    }

    // Match the full-object layout used below: DFS objects read aligned_size
    // (including padding), with an alignment gap between staging slots.
    const size_t dfs_alignment =
        GetEnvOr<size_t>("MOONCAKE_DFS_ALIGNMENT", 4096);
    auto get_staging_size = [&](SessionRangeReadRequest *entry,
                                size_t *staging_size) {
        const uint64_t total_size = calculate_total_size(entry->replica);
        uint64_t source_size = total_size;
        if (entry->replica.is_dfs_replica()) {
            const auto &descriptor = entry->replica.get_dfs_descriptor();
            source_size = descriptor.aligned_size > 0
                              ? descriptor.aligned_size
                              : descriptor.object_size;
        }
        if (source_size < total_size ||
            source_size > std::numeric_limits<size_t>::max() ||
            dfs_alignment == 0) {
            return false;
        }
        size_t aligned_size = static_cast<size_t>(source_size);
        if (!entry->replica.is_dfs_replica() &&
            !AlignUp(aligned_size, dfs_alignment, &aligned_size)) {
            return false;
        }
        *staging_size = aligned_size;
        return true;
    };

    std::vector<std::vector<SessionRangeReadRequest *>> chunks;
    chunks.reserve(entries.size());
    std::vector<SessionRangeReadRequest *> chunk;
    chunk.reserve(entries.size());
    size_t chunk_arena_size = 0;
    size_t request_planned_bytes = 0;
    auto add_planned_bytes = [&](size_t bytes) {
        if (bytes > std::numeric_limits<size_t>::max() -
                        request_planned_bytes) {
            request_planned_bytes = std::numeric_limits<size_t>::max();
        } else {
            request_planned_bytes += bytes;
        }
    };
    auto flush_chunk = [&]() {
        if (chunk.empty()) return;
        add_planned_bytes(chunk_arena_size);
        chunks.push_back(chunk);
        chunk.clear();
        chunk_arena_size = 0;
    };
    for (auto *entry : entries) {
        size_t staging_size = 0;
        if (!get_staging_size(entry, &staging_size)) {
            flush_chunk();
            chunks.push_back({entry});
            continue;
        }
        size_t aligned_offset = 0;
        if (!AlignUp(chunk_arena_size, dfs_alignment, &aligned_offset) ||
            staging_size > std::numeric_limits<size_t>::max() -
                               aligned_offset) {
            flush_chunk();
            chunks.push_back({entry});
            continue;
        }
        size_t next_size = aligned_offset + staging_size;
        if (!chunk.empty() && next_size > chunk_limit_bytes) {
            flush_chunk();
            next_size = staging_size;
        }
        if (staging_size > chunk_limit_bytes) {
            flush_chunk();
            chunks.push_back({entry});
            add_planned_bytes(staging_size);
            continue;
        }
        chunk.push_back(entry);
        chunk_arena_size = next_size;
    }
    flush_chunk();

    for (size_t i = 0; i < chunks.size(); ++i) {
        execute_session_dfs_range_reads_batch(
            chunks[i], results, trace_id, false, i + 1, chunks.size(),
            chunk_limit_bytes, request_planned_bytes);
    }
}

void RealClient::execute_session_dfs_range_reads_batch(
    std::vector<SessionRangeReadRequest *> &entries,
    std::vector<int> &results, uint64_t trace_id, bool prefetch_only,
    size_t chunk_index, size_t chunk_count, size_t chunk_limit_bytes,
    size_t request_planned_bytes) {
    const auto timing_start = std::chrono::steady_clock::now();
    const bool trace_enabled = trace_id != 0;
    const size_t input_entries = entries.size();
    size_t prefetch_cache_hits = 0;
    size_t prefetch_h2d_entries = 0;
    uint64_t prefetch_h2d_bytes = 0;
    size_t fallback_dfs_h2d_entries = 0;
    uint64_t fallback_dfs_h2d_bytes = 0;
    const bool prefetch_cache_enabled = dfs_prefetch_arena_available();
    std::vector<std::shared_ptr<BufferHandle>> detached_prefetch_buffers;
    if (prefetch_cache_enabled) {
        detached_prefetch_buffers.reserve(entries.size());
    }
    size_t arena_capacity = 0;
    bool pinned_restore_arena_used = false;
    bool dfs_read_success = false;
    uint64_t dfs_read_bytes = 0;
    uint64_t dfs_requested_bytes = 0;
    std::chrono::steady_clock::time_point t_scatter_plan_done = timing_start;
    std::chrono::steady_clock::time_point t_scatter_submit_start =
        timing_start;
    std::chrono::steady_clock::time_point t_scatter_submit_done = timing_start;
    std::chrono::steady_clock::time_point t_scatter_sync_start = timing_start;
    std::chrono::steady_clock::time_point t_scatter_sync_done = timing_start;
    struct PendingScatterResult {
        SessionRangeReadRequest *entry;
        size_t transferred;
    };
    std::vector<PendingScatterResult> pending_scatter_results;
    pending_scatter_results.reserve(entries.size());
    std::vector<std::shared_ptr<BufferHandle>> inflight_handles;
    inflight_handles.reserve(entries.size());
    uint64_t scatter_bytes = 0;
    DfsAsyncScatterContext async_scatter(*this, *dfs_h2d_stream_pool_,
                                         trace_enabled);

    auto valid_source_handle = [](const SessionRangeReadRequest *entry,
                                  const std::shared_ptr<BufferHandle> &handle) {
        uint64_t source_size = calculate_total_size(entry->replica);
        if (entry->replica.is_dfs_replica()) {
            const auto &descriptor = entry->replica.get_dfs_descriptor();
            source_size = descriptor.aligned_size > 0
                              ? descriptor.aligned_size
                              : descriptor.object_size;
        }
        return handle && handle->ptr() &&
               source_size >= calculate_total_size(entry->replica) &&
               source_size <= std::numeric_limits<size_t>::max() &&
               static_cast<size_t>(source_size) <= handle->size();
    };

    auto queue_scatter = [&](SessionRangeReadRequest *entry,
                             const std::shared_ptr<BufferHandle> &handle,
                             bool from_prefetch_cache) {
        if (prefetch_only) return;
        size_t transferred = 0;
        uint64_t h2d_transferred = 0;
        for (size_t j = 0; j < entry->sizes.size(); ++j) {
            scatter_bytes += entry->sizes[j];
            transferred += entry->sizes[j];
            const void *src = static_cast<const char *>(handle->ptr()) +
                              entry->src_offsets[j];
            const void *src_device =
                handle->device_ptr()
                    ? static_cast<const char *>(handle->device_ptr()) +
                          entry->src_offsets[j]
                    : nullptr;
            const bool device_copy = async_scatter.AddCopy(
                entry->buffers[j], src, src_device, entry->sizes[j],
                entry->original_idx);
            if (trace_enabled && device_copy) {
                h2d_transferred += entry->sizes[j];
            }
        }
        // These counters describe device-target ranges added to this
        // operation's H2D plan, not full object sizes or completion bytes.
        // Keep byte accumulation behind the existing opt-in trace switch.
        if (trace_enabled && h2d_transferred > 0) {
            if (from_prefetch_cache) {
                ++prefetch_h2d_entries;
                prefetch_h2d_bytes += h2d_transferred;
            } else if (entry->replica.is_dfs_replica()) {
                ++fallback_dfs_h2d_entries;
                fallback_dfs_h2d_bytes += h2d_transferred;
            }
        }
        inflight_handles.push_back(handle);
        pending_scatter_results.push_back(
            PendingScatterResult{entry, transferred});
    };

    auto queue_loaded_object =
        [&](SessionRangeReadRequest *entry,
            const std::shared_ptr<BufferHandle> &handle) {
        bool buffered_for_session = false;
        if (prefetch_only) {
            std::lock_guard<std::mutex> lock(session_mutex_);
            if (get_sessions_.find(entry->key) != get_sessions_.end()) {
                auto cache_it = get_session_prefetch_cache_.find(entry->key);
                if (cache_it != get_session_prefetch_cache_.end()) {
                    // Move the old handle out before replacing it so a
                    // repeated prefetch cannot free pinned memory under the
                    // session lock.
                    detached_prefetch_buffers.push_back(
                        std::move(cache_it->second.buffer_handle));
                    cache_it->second = PrefetchedSessionBuffer{
                        handle, calculate_total_size(entry->replica)};
                } else {
                    get_session_prefetch_cache_.emplace(
                        entry->key,
                        PrefetchedSessionBuffer{
                            handle, calculate_total_size(entry->replica)});
                }
                buffered_for_session = true;
            }
        }
        if (prefetch_only && !buffered_for_session) {
            results[entry->original_idx] =
                static_cast<int>(toInt(ErrorCode::INVALID_PARAMS));
            return;
        }
        queue_scatter(entry, handle, /*from_prefetch_cache=*/false);
    };

    // Only waiting-queue prefetch populates this cache. Skip cache probes
    // and the miss-vector allocation when the prefetch arena is unavailable.
    if (prefetch_cache_enabled) {
        std::vector<SessionRangeReadRequest *> miss_entries;
        miss_entries.reserve(entries.size());
        for (auto *entry : entries) {
            std::shared_ptr<BufferHandle> cached_handle;
            if (entry->replica.is_dfs_replica()) {
                std::lock_guard<std::mutex> lock(session_mutex_);
                auto prefetch_it = get_session_prefetch_cache_.find(entry->key);
                if (prefetch_it != get_session_prefetch_cache_.end()) {
                    cached_handle = prefetch_it->second.buffer_handle;
                }
            }
            if (!cached_handle) {
                client_->ObserveDirectSessionCache(false);
                miss_entries.push_back(entry);
                continue;
            }
            client_->ObserveDirectSessionCache(true);
            if (results[entry->original_idx] == 0) {
                if (valid_source_handle(entry, cached_handle)) {
                    ++prefetch_cache_hits;
                    queue_scatter(entry, cached_handle,
                                  /*from_prefetch_cache=*/true);
                } else {
                    LOG(ERROR) << "DFS prefetch buffer is smaller than the "
                                  "replica, key: "
                               << entry->key;
                    {
                        std::lock_guard<std::mutex> lock(session_mutex_);
                        DetachPrefetchedSessionBuffer(
                            get_session_prefetch_cache_, entry->key,
                            detached_prefetch_buffers);
                    }
                    miss_entries.push_back(entry);
                }
            }
        }
        entries = std::move(miss_entries);
    }
    const auto t_cache_hit_done = std::chrono::steady_clock::now();

    // 2. Cache miss: allocate + BatchGet
    std::vector<std::string> disk_batch_keys;
    std::vector<QueryResult> disk_batch_qrs;
    std::unordered_map<std::string, std::vector<Slice>> disk_batch_slices;
    std::unordered_map<std::string, std::shared_ptr<BufferHandle>>
        disk_temp_handles;

    struct ArenaView {
        SessionRangeReadRequest *entry;
        size_t offset;
        size_t size;
    };

    const size_t dfs_alignment =
        GetEnvOr<size_t>("MOONCAKE_DFS_ALIGNMENT", 4096);
    std::vector<ArenaView> arena_views;
    arena_views.reserve(entries.size());
    size_t arena_size = 0;

    for (auto *entry_ptr : entries) {
        auto &entry = *entry_ptr;
        const uint64_t total_size = calculate_total_size(entry.replica);
        uint64_t source_size = total_size;
        if (entry.replica.is_dfs_replica()) {
            const auto &descriptor = entry.replica.get_dfs_descriptor();
            source_size = descriptor.aligned_size > 0
                              ? descriptor.aligned_size
                              : descriptor.object_size;
        }
        if (source_size < total_size ||
            source_size > std::numeric_limits<size_t>::max()) {
            LOG(ERROR) << "DFS object is too large for a host arena, key: "
                       << entry.key << ", size: " << total_size;
            results[entry.original_idx] =
                static_cast<int>(toInt(ErrorCode::INVALID_PARAMS));
            continue;
        }

        size_t aligned_offset = 0;
        // DFS iovecs read descriptor.aligned_size, including trailing padding.
        // Keep that full range inside the view while aligning each slot start
        // for O_DIRECT. Other disk replicas only read their logical size.
        const size_t source_bytes = static_cast<size_t>(source_size);
        size_t aligned_object_size = source_bytes;
        if (dfs_alignment == 0 ||
            (!entry.replica.is_dfs_replica() &&
             !AlignUp(source_bytes, dfs_alignment, &aligned_object_size)) ||
            !AlignUp(arena_size, dfs_alignment, &aligned_offset) ||
            aligned_object_size >
                std::numeric_limits<size_t>::max() - aligned_offset) {
            LOG(ERROR) << "DFS pinned arena size overflow, key: " << entry.key;
            results[entry.original_idx] =
                static_cast<int>(toInt(ErrorCode::INVALID_PARAMS));
            continue;
        }
        if (!prefetch_only &&
            (aligned_offset > chunk_limit_bytes ||
             aligned_object_size > chunk_limit_bytes - aligned_offset)) {
            LOG(ERROR) << "DFS restore entry exceeds staging chunk limit, key: "
                       << entry.key << ", entry_bytes=" << aligned_object_size
                       << ", chunk_limit_bytes=" << chunk_limit_bytes;
            results[entry.original_idx] =
                static_cast<int>(toInt(ErrorCode::NO_AVAILABLE_HANDLE));
            continue;
        }
        arena_views.push_back(
            ArenaView{entry_ptr, aligned_offset, aligned_object_size});
        arena_size = aligned_offset + aligned_object_size;
    }

    std::shared_ptr<BufferHandle> arena;
    bool prefetch_arena_used = false;
    bool prefetch_arena_alloc_fail = false;
    if (!arena_views.empty()) {
        if (prefetch_only) {
            // Prefetch buffers remain in get_session_prefetch_cache_ until the
            // session ends, so they come only from the dedicated prefetch
            // arena: never the request-scoped restore arena and never a
            // runtime pinned allocation.
            arena = AllocatePrefetchArenaRegion(arena_size, dfs_alignment);
            prefetch_arena_used = arena != nullptr;
            prefetch_arena_alloc_fail = arena == nullptr;
        } else {
            std::optional<BufferHandle> allocation;
            if (file_storage_) {
                allocation = file_storage_->AllocatePinnedStagingBuffer(
                    arena_size, dfs_alignment);
                pinned_restore_arena_used = allocation.has_value();
            }
            if (!allocation && client_buffer_allocator_) {
                allocation = client_buffer_allocator_->allocate_aligned(
                    arena_size, dfs_alignment);
            }
            if (allocation) {
                arena = std::make_shared<BufferHandle>(std::move(*allocation));
            }
        }
        if (arena) {
            arena_capacity = arena->size();
        }
        if (!arena) {
            // Prefetch failures are reported (rate-limited) by
            // AllocatePrefetchArenaRegion.
            if (!prefetch_only) {
                LOG(ERROR) << "DFS staging arena allocation failed, size: "
                           << arena_size << ", pinned_restore_available="
                           << (file_storage_ &&
                               file_storage_->HasPinnedRestoreArena());
            }
            for (const auto &view : arena_views) {
                results[view.entry->original_idx] =
                    static_cast<int>(toInt(ErrorCode::NO_AVAILABLE_HANDLE));
            }
            arena_views.clear();
            arena.reset();
        }
    }

    for (const auto &view : arena_views) {
        auto *view_ptr = static_cast<char *>(arena->ptr()) + view.offset;
        void *view_device_ptr =
            arena->device_ptr()
                ? static_cast<char *>(arena->device_ptr()) + view.offset
                : nullptr;
        auto handle = std::make_shared<BufferHandle>(
            view_ptr, view.size, [arena]() { (void)arena; },
            view_device_ptr);

        std::vector<Slice> disk_slices;
        allocateSlices(disk_slices, view.entry->replica, handle->ptr());

        disk_batch_keys.push_back(view.entry->key);
        disk_batch_qrs.push_back(
            FilterQueryResult(view.entry->query_result, view.entry->replica));
        disk_batch_slices[view.entry->key] = std::move(disk_slices);
        disk_temp_handles.emplace(view.entry->key, std::move(handle));
        if (trace_enabled) {
            for (size_t size : view.entry->sizes) {
                dfs_requested_bytes += size;
            }
        }
    }

    // Buffer allocation done here; what follows is the DFS read (BatchGet) and
    // then the scatter. Splitting alloc from read tells a slow
    // alloc_batch_get_us apart: allocation churn vs actual disk I/O.
    const auto t_alloc_done = std::chrono::steady_clock::now();

    if (disk_batch_keys.empty()) {
        // An empty batch is a successful no-op unless arena construction had
        // already failed and left a non-zero requested arena size.
        dfs_read_success = arena_size == 0;
    }
    auto t_io_done = std::chrono::steady_clock::now();
    if (!disk_batch_keys.empty()) {
        const auto io_start = std::chrono::steady_clock::now();
        auto disk_results = client_->BatchGet(disk_batch_keys, disk_batch_qrs,
                                              disk_batch_slices);
        const double io_duration =
            std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                          io_start)
                .count();
        dfs_read_success =
            disk_results.size() == disk_batch_keys.size() &&
            std::all_of(disk_results.begin(), disk_results.end(),
                        [](const auto &result) { return result.has_value(); });
        dfs_read_bytes = 0;
        if (disk_results.size() == disk_batch_keys.size()) {
            for (size_t i = 0; i < disk_results.size(); ++i) {
                if (!disk_results[i]) continue;
                auto slices_it = disk_batch_slices.find(disk_batch_keys[i]);
                if (slices_it == disk_batch_slices.end()) continue;
                for (const auto &slice : slices_it->second) {
                    dfs_read_bytes += slice.size;
                }
            }
        }
        t_io_done = std::chrono::steady_clock::now();
        client_->ObserveDirectIo(
            "read", DirectStorageMetricSource(ReplicaType::DFS),
            dfs_read_success,
            dfs_read_bytes, io_duration);

        // Build key -> entry map for O(1) lookup
        std::unordered_map<std::string, SessionRangeReadRequest *> entry_map;
        for (auto *entry_ptr : entries) {
            entry_map[entry_ptr->key] = entry_ptr;
        }

        // Build a copy plan before submitting any DMA. Destination pointers are
        // resolved once, adjacent compatible ranges are merged, and operations
        // are grouped by accelerator and physical device.
        for (size_t di = 0; di < disk_batch_keys.size(); ++di) {
            const auto &key = disk_batch_keys[di];
            auto handle_it = disk_temp_handles.find(key);
            if (handle_it == disk_temp_handles.end()) {
                continue;
            }

            // A short backend response is a failure for the missing entries;
            // do not index past the returned vector.
            if (di >= disk_results.size()) {
                auto map_it = entry_map.find(key);
                if (map_it != entry_map.end()) {
                    results[map_it->second->original_idx] =
                        static_cast<int>(toInt(ErrorCode::INTERNAL_ERROR));
                }
                continue;
            }

            if (!disk_results[di]) {
                auto map_it = entry_map.find(key);
                if (map_it != entry_map.end()) {
                    results[map_it->second->original_idx] =
                        static_cast<int>(toInt(disk_results[di].error()));
                }
                continue;
            }

            auto map_it = entry_map.find(key);
            if (map_it == entry_map.end()) {
                continue;
            }
            SessionRangeReadRequest *entry = map_it->second;
            std::shared_ptr<BufferHandle> handle = std::move(handle_it->second);
            if (!valid_source_handle(entry, handle)) {
                LOG(ERROR) << "DFS read buffer is smaller than the replica, "
                              "key: "
                           << key;
                results[entry->original_idx] =
                    static_cast<int>(toInt(ErrorCode::INTERNAL_ERROR));
                continue;
            }
            queue_loaded_object(entry, handle);
        }
    }

    t_scatter_plan_done = std::chrono::steady_clock::now();
    t_scatter_submit_start = t_scatter_plan_done;
    bool scatter_succeeded = true;
    if (!prefetch_only) {
        async_scatter.Submit();
        t_scatter_submit_done = std::chrono::steady_clock::now();
        t_scatter_sync_start = t_scatter_submit_done;
        scatter_succeeded = async_scatter.Synchronize();
        t_scatter_sync_done = std::chrono::steady_clock::now();
    } else {
        // A host-prefetch completes at DFS read completion. Do not submit or
        // synchronize any accelerator work until the admission range-get.
        t_scatter_submit_done = t_scatter_submit_start;
        t_scatter_sync_start = t_scatter_submit_done;
        t_scatter_sync_done = t_scatter_sync_start;
    }
    const uint64_t scatter_ranges = async_scatter.original_range_count();
    const uint64_t scatter_merged_ranges = async_scatter.merged_range_count();

    for (const auto &pending : pending_scatter_results) {
        const size_t result_index = pending.entry->original_idx;
        if (async_scatter.EntryFailed(result_index)) {
            results[result_index] =
                static_cast<int>(toInt(ErrorCode::TRANSFER_FAIL));
            continue;
        }
        results[result_index] = static_cast<int>(pending.transferred);
    }
    if (!scatter_succeeded) {
        LOG(ERROR) << "One or more DFS H2D stream operations failed";
    }

    // Inflight references are released only after explicit stream
    // synchronization, preserving the synchronous batch-get API contract.
    inflight_handles.clear();
    const auto t_end = std::chrono::steady_clock::now();
    auto elapsed_us = [](const auto &a, const auto &b) {
        return std::chrono::duration_cast<std::chrono::microseconds>(b - a)
            .count();
    };
    if (trace_enabled) {
        // One line per batch: alloc/dfs_read/scatter split plus scatter volume,
        // so a slow stage can be attributed to bandwidth (bytes/us near the H2D
        // ceiling) or to overhead (many tiny ranges, few bytes).
        const uint64_t scatter_us = elapsed_us(t_io_done, t_end);
        const uint64_t scatter_plan_us =
            elapsed_us(t_io_done, t_scatter_plan_done);
        const uint64_t scatter_submit_us =
            elapsed_us(t_scatter_submit_start, t_scatter_submit_done);
        const uint64_t scatter_sync_us =
            elapsed_us(t_scatter_sync_start, t_scatter_sync_done);
        const uint64_t scatter_finalize_us =
            elapsed_us(t_scatter_sync_done, t_end);
        double scatter_gbps = 0.0;
        if (scatter_us > 0) {
            // bytes / us == MB/s; /1000 -> GB/s.
            scatter_gbps =
                static_cast<double>(scatter_bytes) / scatter_us / 1000.0;
        }
        double read_amplification = -1.0;
        if (dfs_read_success && dfs_requested_bytes > 0) {
            read_amplification = static_cast<double>(dfs_read_bytes) /
                                 static_cast<double>(dfs_requested_bytes);
        }
        LOG(INFO)
            << "execute_session_dfs_range_reads: trace_id=" << trace_id
            << ", restore_chunk=" << chunk_index << "/" << chunk_count
            << ", restore_chunk_limit_bytes=" << chunk_limit_bytes
            << ", restore_chunk_plan_total_bytes=" << request_planned_bytes
            << ", arena_peak_requested_bytes=" << arena_size
            << ", entries=" << entries.size()
            << ", input_entries=" << input_entries
            << ", prefetch_cache_hits=" << prefetch_cache_hits
            << ", prefetch_h2d_entries=" << prefetch_h2d_entries
            << ", prefetch_h2d_bytes=" << prefetch_h2d_bytes
            << ", fallback_dfs_h2d_entries=" << fallback_dfs_h2d_entries
            << ", fallback_dfs_h2d_bytes=" << fallback_dfs_h2d_bytes
            << ", dfs_batch_entries=" << disk_batch_keys.size()
            << ", cache_hit_us=" << elapsed_us(timing_start, t_cache_hit_done)
            << ", alloc_us=" << elapsed_us(t_cache_hit_done, t_alloc_done)
            << ", dfs_read_us=" << elapsed_us(t_alloc_done, t_io_done)
            << ", scatter_us=" << scatter_us
            << ", total_us=" << elapsed_us(timing_start, t_end)
            << ", arena_requested_bytes=" << arena_size
            << ", arena_capacity=" << arena_capacity
            << ", pinned_restore_arena_used=" << pinned_restore_arena_used
            << ", prefetch_only=" << prefetch_only
            << ", prefetch_arena_used=" << prefetch_arena_used
            << ", prefetch_arena_alloc_fail=" << prefetch_arena_alloc_fail
            << ", dfs_read_success=" << dfs_read_success
            << ", dfs_read_bytes=" << dfs_read_bytes
            << ", dfs_requested_bytes=" << dfs_requested_bytes
            << ", scatter_ranges=" << scatter_ranges
            << ", scatter_merged_ranges=" << scatter_merged_ranges
            << ", scatter_bytes=" << scatter_bytes
            << ", read_amplification=" << read_amplification
            << ", pointer_query_count=" << async_scatter.pointer_query_count()
            << ", pointer_cache_hits=" << async_scatter.pointer_cache_hits()
            << ", pointer_query_us=" << async_scatter.pointer_query_us()
            << ", region_cache_hits=" << async_scatter.region_cache_hits()
            << ", region_cache_misses=" << async_scatter.region_cache_misses()
            << ", unregistered_pointer_queries="
            << async_scatter.unregistered_pointer_queries()
            << ", host_copy_ops=" << async_scatter.host_copy_ops()
            << ", device_copy_ops=" << async_scatter.device_copy_ops()
            << ", host_copy_ranges=" << async_scatter.host_copy_ranges()
            << ", device_copy_ranges=" << async_scatter.device_copy_ranges()
            << ", h2d_kernel_batches=" << async_scatter.h2d_kernel_batches()
            << ", scatter_plan_us=" << scatter_plan_us
            << ", scatter_submit_us=" << scatter_submit_us
            << ", scatter_sync_us=" << scatter_sync_us
            << ", scatter_finalize_us=" << scatter_finalize_us
            << ", scatter_GBps=" << scatter_gbps;
    }
}

std::vector<int> RealClient::batch_put_session_start(
    const std::vector<std::string> &keys, const std::vector<size_t> &sizes,
    const ReplicateConfig &config) {
    std::vector<int> results(keys.size(), 0);
    if (!client_ || keys.size() != sizes.size()) {
        LOG(ERROR) << "Invalid batch_put_session_start args";
        return std::vector<int>(
            keys.size(), static_cast<int>(toInt(ErrorCode::INVALID_PARAMS)));
    }
    if (config.group_ids.has_value() &&
        config.group_ids->size() != keys.size()) {
        LOG(ERROR) << "batch_put_session_start: group_ids.size()="
                   << config.group_ids->size()
                   << ", keys.size()=" << keys.size()
                   << ", error=invalid_group_ids";
        return std::vector<int>(
            keys.size(), static_cast<int>(toInt(ErrorCode::INVALID_PARAMS)));
    }

    // Session put only writes MEMORY replicas. Reliable configs that require
    // NoF completion cannot be finalized correctly on this path.
    const auto write_mode = DetermineReplicaWriteMode(config);
    if (config.nof_replica_num > 0 &&
        write_mode != ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
        LOG(ERROR) << "batch_put_session_start rejects reliable NoF configs: "
                   << "session path never writes NoF "
                   << "(nof_replica_num=" << config.nof_replica_num
                   << ", replica_num=" << config.replica_num << ")";
        return std::vector<int>(
            keys.size(), static_cast<int>(toInt(ErrorCode::INVALID_PARAMS)));
    }

    std::vector<std::string> start_keys;
    std::vector<uint64_t> start_sizes;
    std::vector<size_t> start_indices;
    {
        std::lock_guard<std::mutex> lock(session_mutex_);
        for (size_t i = 0; i < keys.size(); ++i) {
            if (put_sessions_.count(keys[i]) != 0) {
                LOG(ERROR) << "Put session already exists for key: " << keys[i];
                results[i] = static_cast<int>(toInt(ErrorCode::INVALID_PARAMS));
                continue;
            }
            start_keys.push_back(keys[i]);
            start_sizes.push_back(static_cast<uint64_t>(sizes[i]));
            start_indices.push_back(i);
        }
    }
    if (start_keys.empty()) {
        return results;
    }

    // start_keys may be a subset when some keys already have sessions; keep
    // group_ids aligned with the filtered key list.
    ReplicateConfig start_config = config;
    if (start_config.group_ids.has_value()) {
        std::vector<std::string> filtered_group_ids;
        filtered_group_ids.reserve(start_indices.size());
        for (size_t idx : start_indices) {
            filtered_group_ids.push_back(start_config.group_ids->at(idx));
        }
        start_config.group_ids = std::move(filtered_group_ids);
    }

    // Same first half as Client::BatchPut (StartBatchPut → master
    // BatchPutStart).
    auto start_responses =
        client_->StartBatchPutForSizes(start_keys, start_sizes, start_config);
    std::vector<std::string> keys_to_revoke;
    std::vector<std::string> hot_keys_to_remove;
    {
        std::lock_guard<std::mutex> lock(session_mutex_);
        for (size_t j = 0; j < start_keys.size(); ++j) {
            const size_t i = start_indices[j];
            if (!start_responses[j]) {
                results[i] =
                    static_cast<int>(toInt(start_responses[j].error()));
                continue;
            }
            auto replicas = std::move(start_responses[j].value());
            if (!HasMemoryReplica(replicas)) {
                hot_keys_to_remove.push_back(keys[i]);
                keys_to_revoke.push_back(keys[i]);
                results[i] =
                    static_cast<int>(toInt(ErrorCode::INVALID_REPLICA));
                continue;
            }
            put_sessions_[keys[i]] = PutSessionEntry{
                .replicas = std::move(replicas),
                .object_size = static_cast<uint64_t>(sizes[i]),
                .write_mode = write_mode,
            };
        }
    }
    // Master RPCs and hot-cache updates run outside session_mutex_.
    if (client_->GetHotCache() && !hot_keys_to_remove.empty()) {
        client_->GetHotCache()->RemoveHotKeys(hot_keys_to_remove);
    }
    if (!keys_to_revoke.empty()) {
        (void)client_->BatchPutRevoke(keys_to_revoke);
    }
    return results;
}

std::vector<int> RealClient::batch_put_from_multi_buffer_ranges(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &all_sizes,
    const std::vector<std::vector<size_t>> &all_dst_offsets) {
    std::vector<int> results(
        keys.size(), static_cast<int>(toInt(ErrorCode::INVALID_PARAMS)));
    if (!client_ || keys.size() != all_buffers.size() ||
        keys.size() != all_sizes.size() ||
        keys.size() != all_dst_offsets.size()) {
        LOG(ERROR) << "Invalid put ranges args";
        return results;
    }

    // RealClient owns session state (overflow check vs object_size, replica
    // lookup); the parallel replicated transfer is delegated to Client.
    std::vector<std::vector<Replica::Descriptor>> replicas_per_entry;
    std::vector<std::vector<Slice>> slices;
    std::vector<std::vector<uint64_t>> dst_offsets;
    std::vector<size_t> idx_map;  // batch entry -> original key index
    std::vector<std::string> inflight_keys;

    {
        std::lock_guard<std::mutex> lock(session_mutex_);
        for (size_t i = 0; i < keys.size(); ++i) {
            const auto &buffers = all_buffers[i];
            const auto &sizes = all_sizes[i];
            const auto &offsets = all_dst_offsets[i];
            if (buffers.size() != sizes.size() ||
                buffers.size() != offsets.size()) {
                continue;
            }
            auto it = put_sessions_.find(keys[i]);
            if (it == put_sessions_.end() || !it->second.writable) {
                continue;
            }
            bool overflow = false;
            std::vector<Slice> entry_slices;
            std::vector<uint64_t> entry_offsets;
            entry_slices.reserve(buffers.size());
            entry_offsets.reserve(buffers.size());
            for (size_t j = 0; j < buffers.size(); ++j) {
                if (is_object_range_overflow(
                        offsets[j], sizes[j],
                        static_cast<size_t>(it->second.object_size))) {
                    overflow = true;
                    break;
                }
                entry_slices.emplace_back(Slice{buffers[j], sizes[j]});
                entry_offsets.push_back(static_cast<uint64_t>(offsets[j]));
            }
            if (overflow) {
                continue;  // results[i] stays INVALID_PARAMS
            }
            ++it->second.inflight_transfers;
            inflight_keys.push_back(keys[i]);
            replicas_per_entry.push_back(it->second.replicas);
            slices.push_back(std::move(entry_slices));
            dst_offsets.push_back(std::move(entry_offsets));
            idx_map.push_back(i);
        }
    }

    if (replicas_per_entry.empty()) {
        return results;
    }

    // Ensure end/revoke can make progress even if transfer throws.
    struct InflightGuard {
        RealClient *self;
        std::vector<std::string> keys;
        ~InflightGuard() {
            if (!self) {
                return;
            }
            std::lock_guard<std::mutex> lock(self->session_mutex_);
            for (const auto &key : keys) {
                auto it = self->put_sessions_.find(key);
                if (it == self->put_sessions_.end()) {
                    continue;
                }
                if (it->second.inflight_transfers > 0) {
                    --it->second.inflight_transfers;
                }
            }
            self->session_cv_.notify_all();
        }
    } inflight_guard{this, std::move(inflight_keys)};

    auto transfer = client_->BatchTransferWriteRanges(replicas_per_entry,
                                                      slices, dst_offsets);

    for (size_t k = 0; k < transfer.size(); ++k) {
        const size_t i = idx_map[k];
        if (transfer[k]) {
            results[i] = static_cast<int>(transfer[k].value());
        } else {
            results[i] = static_cast<int>(toInt(transfer[k].error()));
        }
    }
    return results;
}

std::vector<int> RealClient::batch_put_session_end(
    const std::vector<std::string> &keys) {
    std::vector<int> results(
        keys.size(), static_cast<int>(toInt(ErrorCode::INVALID_PARAMS)));
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return results;
    }

    // Session put only writes memory replicas (BatchTransferWriteRanges skips
    // NoF). For FLEXIBLE_DUAL_REPLICA, finalize MEMORY then revoke leftover NoF
    // — same split FinalizeBatchPut uses when only memory transfers succeed.
    // Reliable configs with NoF are rejected at session start.
    std::vector<std::string> end_keys;
    std::vector<size_t> end_indices;
    std::vector<char> has_nof;  // parallel to end_keys
    {
        std::unique_lock<std::mutex> lock(session_mutex_);
        for (size_t i = 0; i < keys.size(); ++i) {
            auto it = put_sessions_.find(keys[i]);
            if (it == put_sessions_.end()) {
                results[i] = static_cast<int>(toInt(ErrorCode::INVALID_PARAMS));
                continue;
            }
            bool nof = false;
            for (const auto &replica : it->second.replicas) {
                if (replica.is_nof_replica()) {
                    nof = true;
                    break;
                }
            }
            // Seal first so concurrent range writes cannot start.
            it->second.writable = false;
            // MEMORY+NoF revoke fallback is only valid for flexible dual mode.
            if (nof && it->second.write_mode !=
                           ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
                LOG(ERROR)
                    << "batch_put_session_end: key=" << keys[i]
                    << " has NoF replicas under non-flexible write mode; "
                    << "use batch_put_session_revoke";
                results[i] = static_cast<int>(toInt(ErrorCode::INVALID_PARAMS));
                continue;
            }
            end_keys.push_back(keys[i]);
            end_indices.push_back(i);
            has_nof.push_back(static_cast<char>(nof));
        }
        if (!end_keys.empty()) {
            session_cv_.wait(lock, [&] {
                for (const auto &key : end_keys) {
                    auto it = put_sessions_.find(key);
                    if (it != put_sessions_.end() &&
                        it->second.inflight_transfers > 0) {
                        return false;
                    }
                }
                return true;
            });
        }
    }
    if (end_keys.empty()) {
        return results;
    }

    // Session put path does not compute checksums; leave object_checksum unset.
    std::vector<ObjectMeta> end_metas;
    end_metas.reserve(end_keys.size());
    for (const auto &key : end_keys) {
        end_metas.emplace_back(ObjectMeta{key, std::nullopt});
    }
    auto end_responses = client_->BatchPutEnd(end_metas, ReplicaType::MEMORY);
    if (end_responses.size() != end_keys.size()) {
        // Ambiguous Master state: keep sessions so caller can retry end/revoke.
        for (size_t j = 0; j < end_keys.size(); ++j) {
            results[end_indices[j]] =
                static_cast<int>(toInt(ErrorCode::RPC_FAIL));
        }
        return results;
    }

    std::vector<std::string> nof_revoke_keys;
    std::vector<size_t> nof_revoke_end_indices;
    nof_revoke_keys.reserve(end_keys.size());
    nof_revoke_end_indices.reserve(end_keys.size());
    for (size_t j = 0; j < end_keys.size(); ++j) {
        const size_t i = end_indices[j];
        if (!end_responses[j]) {
            // Keep session for retry/revoke; do not erase on per-key failure.
            results[i] = static_cast<int>(toInt(end_responses[j].error()));
            continue;
        }
        if (has_nof[j]) {
            nof_revoke_keys.push_back(end_keys[j]);
            nof_revoke_end_indices.push_back(j);
            continue;
        }
        {
            std::lock_guard<std::mutex> lock(session_mutex_);
            put_sessions_.erase(end_keys[j]);
        }
        results[i] = 0;
    }

    if (nof_revoke_keys.empty()) {
        return results;
    }

    auto nof_revoke_responses =
        client_->BatchPutRevoke(nof_revoke_keys, ReplicaType::NOF_SSD);
    if (nof_revoke_responses.size() != nof_revoke_keys.size()) {
        for (size_t k = 0; k < nof_revoke_keys.size(); ++k) {
            const size_t j = nof_revoke_end_indices[k];
            results[end_indices[j]] =
                static_cast<int>(toInt(ErrorCode::RPC_FAIL));
            // Keep session: MEMORY end may have succeeded; retry can re-end
            // (idempotent) and re-revoke NoF.
        }
        return results;
    }
    for (size_t k = 0; k < nof_revoke_keys.size(); ++k) {
        const size_t j = nof_revoke_end_indices[k];
        const size_t i = end_indices[j];
        if (!nof_revoke_responses[k] &&
            nof_revoke_responses[k].error() != ErrorCode::OBJECT_NOT_FOUND) {
            results[i] =
                static_cast<int>(toInt(nof_revoke_responses[k].error()));
            continue;
        }
        {
            std::lock_guard<std::mutex> lock(session_mutex_);
            put_sessions_.erase(nof_revoke_keys[k]);
        }
        results[i] = 0;
    }
    return results;
}

std::vector<int> RealClient::batch_put_session_revoke(
    const std::vector<std::string> &keys) {
    std::vector<int> results(
        keys.size(), static_cast<int>(toInt(ErrorCode::INVALID_PARAMS)));
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return results;
    }

    std::vector<std::string> revoke_keys;
    std::vector<size_t> revoke_indices;
    {
        std::unique_lock<std::mutex> lock(session_mutex_);
        for (size_t i = 0; i < keys.size(); ++i) {
            auto it = put_sessions_.find(keys[i]);
            if (it == put_sessions_.end()) {
                results[i] = static_cast<int>(toInt(ErrorCode::INVALID_PARAMS));
                continue;
            }
            // Seal session and wait for outstanding range writes before free.
            it->second.writable = false;
            revoke_keys.push_back(keys[i]);
            revoke_indices.push_back(i);
        }
        if (!revoke_keys.empty()) {
            session_cv_.wait(lock, [&] {
                for (const auto &key : revoke_keys) {
                    auto it = put_sessions_.find(key);
                    if (it != put_sessions_.end() &&
                        it->second.inflight_transfers > 0) {
                        return false;
                    }
                }
                return true;
            });
        }
    }
    if (revoke_keys.empty()) {
        return results;
    }

    // Same path FinalizeBatchPut uses on transfer failure.
    if (client_->GetHotCache() && !revoke_keys.empty()) {
        client_->GetHotCache()->RemoveHotKeys(revoke_keys);
    }
    auto revoke_responses =
        client_->BatchPutRevoke(revoke_keys, ReplicaType::ALL);
    std::lock_guard<std::mutex> lock(session_mutex_);
    if (revoke_responses.size() != revoke_keys.size()) {
        // Ambiguous Master state: keep sessions so caller can retry revoke.
        for (size_t j = 0; j < revoke_keys.size(); ++j) {
            results[revoke_indices[j]] =
                static_cast<int>(toInt(ErrorCode::RPC_FAIL));
        }
        return results;
    }
    for (size_t j = 0; j < revoke_keys.size(); ++j) {
        const size_t i = revoke_indices[j];
        if (!revoke_responses[j]) {
            if (revoke_responses[j].error() == ErrorCode::OBJECT_NOT_FOUND) {
                // Nothing left on Master; drop the local session.
                put_sessions_.erase(revoke_keys[j]);
                results[i] = 0;
            } else {
                // Keep session for retry.
                results[i] =
                    static_cast<int>(toInt(revoke_responses[j].error()));
            }
            continue;
        }
        put_sessions_.erase(revoke_keys[j]);
        results[i] = 0;
    }
    return results;
}

std::vector<int> RealClient::batch_upsert_from_multi_buffers(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &sizes,
    const ReplicateConfig &config) {
    return batch_upsert_from_multi_buffers(keys, all_buffers, sizes, config,
                                           false);
}

std::vector<int> RealClient::batch_upsert_from_multi_buffers(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &sizes,
    const ReplicateConfig &config, bool stage_nonlocal) {
    auto internal_results =
        execute_timed_operation<std::vector<tl::expected<void, ErrorCode>>>(
            [&]() {
                return batch_upsert_from_multi_buffers_internal(
                    keys, all_buffers, sizes, config, stage_nonlocal);
            },
            [](const auto &) { return true; },
            [&](uint64_t latency_us, const auto &ret) {
                auto py_results = ToPyResults(ret);
                client_->ObserveTransferOperation(
                    TransferOperationKind::kWrite,
                    "batch_upsert_from_multi_buffers",
                    sum_successful_nested_sizes(py_results, sizes), latency_us);
            });
    return ToPyResults(internal_results);
}

std::vector<tl::expected<void, ErrorCode>>
RealClient::batch_upsert_from_multi_buffers_internal(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &all_sizes,
    const ReplicateConfig &config, bool stage_nonlocal) {
    return BatchWriteFromMultiBuffers(client_, keys, all_buffers, all_sizes,
                                      config, client_buffer_allocator_,
                                      stage_nonlocal, &Client::BatchUpsert);
}

std::vector<tl::expected<void, ErrorCode>>
RealClient::batch_put_from_multi_buffers_internal(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &all_sizes,
    const ReplicateConfig &config, bool stage_nonlocal) {
    return BatchWriteFromMultiBuffers(client_, keys, all_buffers, all_sizes,
                                      config, client_buffer_allocator_,
                                      stage_nonlocal, &Client::BatchPut);
}

std::vector<int> RealClient::batch_get_into_multi_buffers(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &all_sizes,
    bool prefer_alloc_in_same_node) {
    auto internal_results =
        execute_timed_operation<std::vector<tl::expected<int64_t, ErrorCode>>>(
            [&]() {
                return batch_get_into_multi_buffers_internal(
                    keys, all_buffers, all_sizes, prefer_alloc_in_same_node);
            },
            [](const auto &) { return true; },
            [&](uint64_t latency_us, const auto &ret) {
                std::vector<int> py_results;
                py_results.reserve(ret.size());
                for (const auto &item : ret) {
                    py_results.push_back(to_py_ret(item));
                }
                client_->ObserveTransferOperation(
                    TransferOperationKind::kRead,
                    "batch_get_into_multi_buffers",
                    sum_positive_results(py_results), latency_us);
            });
    std::vector<int> results;
    results.reserve(internal_results.size());

    for (const auto &result : internal_results) {
        results.push_back(to_py_ret(result));
    }
    return results;
}

std::vector<tl::expected<int64_t, ErrorCode>>
RealClient::batch_get_into_multi_buffers_internal(
    const std::vector<std::string> &keys,
    const std::vector<std::vector<void *>> &all_buffers,
    const std::vector<std::vector<size_t>> &all_sizes,
    bool prefer_alloc_in_same_node) {
    // Validate preconditions
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    if (keys.size() != all_buffers.size() || keys.size() != all_sizes.size()) {
        LOG(ERROR) << "Input vector sizes mismatch: keys=" << keys.size()
                   << ", buffers=" << all_buffers.size()
                   << ", sizes=" << all_sizes.size();
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    const size_t num_keys = keys.size();
    std::vector<tl::expected<int64_t, ErrorCode>> results;
    results.reserve(num_keys);
    if (num_keys == 0) {
        return results;
    }
    // Query metadata for all keys
    const auto query_results = client_->BatchQuery(keys);
    // Process each key individually and prepare for batch transfer
    struct ValidKeyInfo {
        std::string key;
        size_t original_index;
        QueryResult query_result;
        std::vector<Slice> slices;
        uint64_t total_size;
    };

    struct DiskKeyInfo {
        std::string key;
        size_t original_index;
        QueryResult query_result;
        std::vector<void *> buffers;
        std::vector<size_t> sizes;
        uint64_t total_size;
        Replica::Descriptor replica;
        bool is_local_disk;  // true=LOCAL_DISK (offload RPC), false=DISK/DFS
                             // (BatchGet)
    };

    std::vector<ValidKeyInfo> valid_operations;
    std::unordered_map<std::string, DiskKeyInfo> valid_local_disk_ops;
    valid_operations.reserve(num_keys);
    auto local_endpoints = client_->GetLocalEndpoints();
    for (size_t i = 0; i < num_keys; ++i) {
        const auto &key = keys[i];
        // Handle query failures
        if (!query_results[i]) {
            const auto error = query_results[i].error();
            results.emplace_back(tl::unexpected(error));
            if (error != ErrorCode::OBJECT_NOT_FOUND) {
                LOG(ERROR) << "Query failed for key '" << key
                           << "': " << toString(error);
            }
            continue;
        }
        // Validate replica list
        auto query_result_values = query_results[i].value();
        if (query_result_values.replicas.empty()) {
            LOG(ERROR) << "Empty replica list for key: " << key;
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_REPLICA));
            continue;
        }
        // Select best replica: prefer local MEMORY, any MEMORY, local NOF,
        // any NOF, LOCAL_DISK, DFS, then DISK. Master may return multiple
        // replicas in any order, so always scan.
        const auto *best_replica =
            SelectBestReplica(query_result_values.replicas, local_endpoints);
        if (!best_replica) {
            LOG(ERROR) << "No usable replica for key: " << key;
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_REPLICA));
            continue;
        }
        const auto replica = *best_replica;
        uint64_t total_size = calculate_total_size(replica);
        const auto &sizes = all_sizes[i];
        uint64_t dst_total_size = 0;
        for (auto &size : sizes) {
            dst_total_size += size;
        }
        if (dst_total_size < total_size) {
            LOG(ERROR) << "Buffer too small for key '" << key
                       << "': required=" << total_size
                       << ", available=" << dst_total_size;
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }
        // Create slices for this key's buffer
        const auto &buffers = all_buffers[i];
        std::vector<Slice> key_slices;
        key_slices.reserve(buffers.size());
        if (replica.is_memory_replica() || replica.is_nof_replica()) {
            // MEMORY / NOF: RDMA directly to the user buffers.
            for (size_t j = 0; j < buffers.size(); ++j) {
                key_slices.emplace_back(Slice{buffers[j], sizes[j]});
            }
        } else if (replica.is_local_disk_replica() ||
                   replica.is_disk_replica() || replica.is_dfs_replica()) {
            // LOCAL_DISK: GPU buffers passed directly as scatter-gather slices
            // (zero-copy). DISK/DFS use a contiguous temp buffer at read time.
            valid_local_disk_ops.emplace(
                key,
                DiskKeyInfo{.key = key,
                            .original_index = i,
                            .query_result = std::move(query_result_values),
                            .buffers = all_buffers[i],
                            .sizes = all_sizes[i],
                            .total_size = total_size,
                            .replica = replica,
                            .is_local_disk = replica.is_local_disk_replica()});
            results.emplace_back(static_cast<int64_t>(total_size));
            continue;
        } else {
            LOG(ERROR) << "Unsupported replica type for key: " << key;
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }

        valid_operations.push_back(
            {.key = key,
             .original_index = i,
             .query_result = FilterQueryResult(query_result_values, replica),
             .slices = std::move(key_slices),
             .total_size = total_size});
        // Set success result (actual bytes transferred)
        results.emplace_back(static_cast<int64_t>(total_size));
    }
    // Early return if no valid operations
    if (valid_operations.empty() && valid_local_disk_ops.empty()) {
        return results;
    }

    // ---- Memory/NOF replica: existing BatchGet path ----
    if (!valid_operations.empty()) {
        std::vector<std::string> batch_keys;
        std::vector<QueryResult> batch_query_results;
        std::unordered_map<std::string, std::vector<Slice>> batch_slices;
        batch_keys.reserve(valid_operations.size());
        batch_query_results.reserve(valid_operations.size());
        for (auto &op : valid_operations) {
            batch_keys.push_back(op.key);
            batch_query_results.push_back(op.query_result);
            batch_slices[op.key] = op.slices;
        }

        auto batch_get_results =
            client_->BatchGet(batch_keys, batch_query_results, batch_slices,
                              prefer_alloc_in_same_node);

        for (size_t j = 0; j < batch_get_results.size(); ++j) {
            const auto &op = valid_operations[j];
            if (!batch_get_results[j]) {
                const auto error = batch_get_results[j].error();
                LOG(ERROR) << "BatchGet failed for key '" << op.key
                           << "': " << toString(error);
                results[op.original_index] = tl::unexpected(error);
            }
        }
    }

    // ---- LOCAL_DISK / file-backed replica read paths ----
    if (!valid_local_disk_ops.empty()) {
        // LOCAL_DISK: pass user GPU buffers directly as scatter-gather slices.
        // vLLM pre-registers all GPU KV-cache memory with TransferEngine via
        // register_buffer(), so the offload RDMA can scatter the on-disk blob
        // into non-contiguous per-layer GPU destinations natively — no temp
        // CPU buffer or H2D copy needed.
        {
            std::unordered_map<
                std::string,
                std::unordered_map<std::string, std::vector<Slice>>>
                offload_objects;

            for (auto &[key, op] : valid_local_disk_ops) {
                if (!op.is_local_disk) continue;
                const auto &replica = op.replica;
                if (!replica.is_local_disk_replica()) {
                    LOG(ERROR)
                        << "No LOCAL_DISK replica found for key: " << key;
                    results[op.original_index] =
                        tl::make_unexpected(ErrorCode::INVALID_REPLICA);
                    continue;
                }
                std::vector<Slice> user_slices;
                user_slices.reserve(op.buffers.size());
                size_t slice_total = 0;
                for (size_t j = 0; j < op.buffers.size(); ++j) {
                    user_slices.push_back(Slice{op.buffers[j], op.sizes[j]});
                    slice_total += op.sizes[j];
                }
                if (slice_total < op.total_size) {
                    LOG(ERROR) << "Slice size too small for key " << key
                               << ": slices=" << slice_total
                               << ", total=" << op.total_size;
                    results[op.original_index] =
                        tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    continue;
                }
                offload_objects[replica.get_local_disk_descriptor()
                                    .transport_endpoint]
                    .emplace(key, std::move(user_slices));
            }

            for (auto &[endpoint, objects] : offload_objects) {
                if (objects.empty()) continue;
                auto read_result =
                    batch_get_into_offload_object_internal(endpoint, objects);
                // On success: results[original_index] was already pre-filled
                // with total_size when valid_local_disk_ops was built; nothing
                // to update. Only overwrite on failure.
                if (!read_result) {
                    for (auto &[key, slices] : objects) {
                        auto disk_it = valid_local_disk_ops.find(key);
                        if (disk_it == valid_local_disk_ops.end()) continue;
                        LOG(ERROR) << "SSD read failed for key '" << key
                                   << "': " << toString(read_result.error());
                        results[disk_it->second.original_index] =
                            tl::make_unexpected(read_result.error());
                    }
                    continue;
                }
                for (auto &[key, slices] : objects) {
                    auto disk_it = valid_local_disk_ops.find(key);
                    if (disk_it == valid_local_disk_ops.end()) continue;
                    auto &op = disk_it->second;
                    auto checksum_result = client_->VerifyObjectChecksum(
                        key, slices, op.total_size,
                        op.query_result.object_checksum);
                    if (!checksum_result) {
                        results[op.original_index] =
                            tl::make_unexpected(checksum_result.error());
                    }
                }
            }
        }

        // DISK/DFS: one batched BatchGet into temp buffers, then scatter.
        {
            // Scatter temp buffer -> user multi_buffers (GPU or host).
            // Returns false and sets results[original_index] on error.
            auto scatter_to_buffers = [&](const std::string &key, char *src,
                                          const DiskKeyInfo &op) -> bool {
                size_t offset = 0;
                for (size_t j = 0; j < op.buffers.size(); ++j) {
                    if (offset >= op.total_size) break;
                    size_t sz =
                        std::min(op.sizes[j],
                                 static_cast<size_t>(op.total_size - offset));
                    void *dst = op.buffers[j];
                    const char *replica_type =
                        op.replica.is_dfs_replica() ? "DFS" : "DISK";
                    if (auto r = scatter_host_to_maybe_device(
                            dst, src + offset, sz,
                            std::string(replica_type) +
                                " scatter, key: " + key);
                        !r) {
                        results[op.original_index] =
                            tl::make_unexpected(r.error());
                        return false;
                    }
                    offset += sz;
                }
                return true;
            };

            std::vector<std::string> disk_batch_keys;
            std::vector<QueryResult> disk_batch_qrs;
            std::unordered_map<std::string, std::vector<Slice>>
                disk_batch_slices;
            std::vector<std::string> disk_key_order;
            std::unordered_map<std::string, std::unique_ptr<BufferHandle>>
                temp_handles;

            for (auto &[key, op] : valid_local_disk_ops) {
                if (op.is_local_disk) continue;
                const auto &replica = op.replica;
                const char *replica_type =
                    replica.is_dfs_replica() ? "DFS" : "DISK";
                auto alloc_result =
                    client_buffer_allocator_->allocate(op.total_size);
                if (!alloc_result) {
                    LOG(ERROR)
                        << "Failed to allocate temp buffer for " << replica_type
                        << " read, key: " << key << ", size: " << op.total_size;
                    results[op.original_index] =
                        tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
                    continue;
                }
                auto handle =
                    std::make_unique<BufferHandle>(std::move(*alloc_result));
                std::vector<Slice> disk_slices;
                allocateSlices(disk_slices, replica, handle->ptr());
                disk_batch_keys.push_back(key);
                disk_batch_qrs.push_back(
                    FilterQueryResult(op.query_result, replica));
                disk_batch_slices[key] = std::move(disk_slices);
                disk_key_order.push_back(key);
                temp_handles.emplace(key, std::move(handle));
            }

            if (!disk_batch_keys.empty()) {
                auto disk_results = client_->BatchGet(
                    disk_batch_keys, disk_batch_qrs, disk_batch_slices,
                    prefer_alloc_in_same_node);

                for (size_t di = 0; di < disk_key_order.size(); ++di) {
                    const auto &key = disk_key_order[di];
                    auto &op = valid_local_disk_ops.at(key);
                    auto handle_it = temp_handles.find(key);
                    const char *replica_type =
                        op.replica.is_dfs_replica() ? "DFS" : "DISK";
                    if (!disk_results[di]) {
                        LOG(ERROR)
                            << replica_type << " BatchGet failed for key '"
                            << key
                            << "': " << toString(disk_results[di].error());
                        results[op.original_index] =
                            tl::make_unexpected(disk_results[di].error());
                        continue;
                    }
                    if (!scatter_to_buffers(
                            key, static_cast<char *>(handle_it->second->ptr()),
                            op))
                        continue;
                }
            }
            // temp_handles: BufferHandle RAII releases allocator memory
        }
    }

    return results;
}

tl::expected<PingResponse, ErrorCode> RealClient::ping(const UUID &client_id) {
    std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);

    if (!client_) {
        LOG(ERROR) << "client_id=" << client_id << ", error=client_not_ready";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    ClientStatus client_status =
        client_->is_ping_healthy() ? ClientStatus::OK : ClientStatus::UNDEFINED;

    PodUUID pod_client_id = {client_id.first, client_id.second};
    if (!dummy_client_ping_queue_.push(pod_client_id)) {
        // Queue is full
        LOG(ERROR) << "client_id=" << client_id
                   << ", error=dummy_client_ping_queue_";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return PingResponse(view_version_, client_status);
}

void RealClient::dummy_client_monitor_func() {
    std::unordered_map<UUID, std::chrono::steady_clock::time_point,
                       boost::hash<UUID>>
        client_ttl;
    while (dummy_client_monitor_running_) {
        auto now = std::chrono::steady_clock::now();

        // Update the client ttl
        PodUUID pod_client_id;
        while (dummy_client_ping_queue_.pop(pod_client_id)) {
            UUID client_id = {pod_client_id.first, pod_client_id.second};
            client_ttl[client_id] =
                now + std::chrono::seconds(dummy_client_live_ttl_sec_);
        }

        // Find out expired clients
        std::vector<UUID> expired_clients;
        for (auto it = client_ttl.begin(); it != client_ttl.end();) {
            if (it->second < now) {
                LOG(INFO) << "client_id=" << it->first
                          << ", action=client_expired";
                expired_clients.push_back(it->first);
                it = client_ttl.erase(it);
            } else {
                ++it;
            }
        }

        // Update the client status to NEED_REMOUNT
        if (!expired_clients.empty()) {
            for (auto &client_id : expired_clients) {
                // Unmap mapped_shms associated with this client
                if (globalConfig().ascend_agent_mode) {
                    ascend_unmap_shm_internal(client_id);
                } else {
                    unmap_shm_internal(client_id);
                }
            }
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kDummyClientMonitorSleepMs));
    }
}

int RealClient::start_dummy_client_monitor() {
    // Start client monitor thread in all modes so TTL/heartbeat works
    dummy_client_monitor_running_ = true;
    dummy_client_monitor_thread_ =
        std::thread(&RealClient::dummy_client_monitor_func, this);
    if (!dummy_client_monitor_thread_.joinable()) {
        LOG(ERROR) << "Failed to start dummy_client_monitor_thread";
        return -1;
    }
    LOG(INFO) << "start dummy_client_monitor_thread";
    return 0;
}

void RealClient::stop_dummy_client_monitor() {
    dummy_client_monitor_running_ = false;
    if (dummy_client_monitor_thread_.joinable()) {
        dummy_client_monitor_thread_.join();
        LOG(INFO) << "dummy_client_monitor_thread stopped";
    }
}
int RealClient::start_ipc_server() {
    uds_acceptor_ = std::make_unique<UdsAcceptor>(ipc_socket_path_);
    uds_acceptor_->registerHandler([this](UdsConnection &connection) {
        auto timeout_result = connection.setRecvTimeout(kIpcRequestRecvTimeout);
        if (!timeout_result) {
            LOG(ERROR) << timeout_result.error();
            return;
        }

        IpcRequestType req_type;
        if (connection.recvRaw(&req_type, sizeof(req_type)) != 0) {
            LOG(ERROR) << "Failed to read IPC request type";
            return;
        }

        if (req_type == IPC_SHM_REGISTER) {
            handle_ipc_shm_register(connection);
        } else if (req_type == IPC_SHM_FD_REQUEST) {
            handle_ipc_shm_fd_request(connection);
        } else {
            LOG(ERROR) << "Unknown IPC request type: " << req_type;
        }
    });
    auto start_result = uds_acceptor_->start();
    if (!start_result) {
        LOG(ERROR) << start_result.error();
        uds_acceptor_.reset();
        return -1;
    }
    return 0;
}

int RealClient::stop_ipc_server() {
    if (uds_acceptor_) {
        uds_acceptor_->stop();
        uds_acceptor_.reset();
    }
    return 0;
}

void RealClient::handle_ipc_shm_register(UdsConnection &connection) {
    ShmRegisterRequest req;
    int fd = connection.recvFd(&req, sizeof(req));
    if (fd < 0) {
        LOG(ERROR) << "Failed to receive fd for SHM_REGISTER";
        return;
    }

    UUID client_id{req.client_id_first, req.client_id_second};
    auto result = map_shm_internal_with_device(
        fd, req.dummy_base_addr, req.shm_size, req.is_local_buffer,
        req.device_id, client_id);

    int status = result.has_value() ? 0 : -1;
    connection.sendRaw(&status, sizeof(status));
}

void RealClient::handle_ipc_shm_fd_request(UdsConnection &connection) {
    ShmFdRequest req;
    if (connection.recvRaw(&req, sizeof(req)) != 0) {
        LOG(ERROR) << "Failed to read ShmFdRequest payload";
        return;
    }

    ShmFdResponse resp{};
    resp.status = -1;
    resp.shm_size = 0;

    // Validate client_id against registered dummy clients
    UUID client_id{req.client_id_first, req.client_id_second};
    {
        std::shared_lock<std::shared_mutex> lock(dummy_client_mutex_);
        if (shm_contexts_.find(client_id) == shm_contexts_.end()) {
            LOG(ERROR) << "Unregistered client_id in fd request: "
                       << client_id.first << ":" << client_id.second;
            connection.sendRaw(&resp, sizeof(resp));
            return;
        }
    }

    // Currently only SHM_SEG_HOT_CACHE is supported
    if (req.segment_type != SHM_SEG_HOT_CACHE) {
        LOG(ERROR) << "Unknown segment_type: " << req.segment_type;
        connection.sendRaw(&resp, sizeof(resp));
        return;
    }

    if (!client_ || !client_->IsHotCacheEnabled()) {
        LOG(ERROR) << "Hot cache not available for fd request";
        connection.sendRaw(&resp, sizeof(resp));
        return;
    }

    auto hot_cache = client_->GetHotCache();
    auto seg = hot_cache->GetShmSegment();
    if (!seg || seg->fd < 0) {
        LOG(ERROR) << "Hot cache shm segment not available";
        connection.sendRaw(&resp, sizeof(resp));
        return;
    }

    resp.status = 0;
    resp.shm_size = seg->size;

    if (connection.sendFd(seg->fd, &resp, sizeof(resp)) < 0) {
        LOG(ERROR) << "Failed to send hot cache fd to dummy client";
    }
}

std::vector<Replica::Descriptor> RealClient::get_replica_desc(
    const std::string &key) {
    const bool allow_dfs = client_->DfsReadAllowed();
    auto query_result =
        ApplyDfsReadVisibility(client_->Query(key), allow_dfs);
    if (!query_result) {
        std::vector<Replica::Descriptor> replica_list = {};
        if (query_result.error() == ErrorCode::OBJECT_NOT_FOUND ||
            query_result.error() == ErrorCode::REPLICA_IS_NOT_READY) {
            LOG(ERROR) << "Object not found for key: " << key;
        } else {
            LOG(ERROR) << "Query failed for key: " << key
                       << " with error: " << toString(query_result.error());
        }
        return replica_list;
    }
    const std::vector<Replica::Descriptor> &replica_list =
        query_result.value().replicas;
    if (replica_list.empty()) {
        LOG(ERROR) << "Empty replica list for key: " << key;
    }
    return replica_list;
}

std::map<std::string, std::vector<Replica::Descriptor>>
RealClient::batch_get_replica_desc(const std::vector<std::string> &keys) {
    auto query_results = batch_query(keys);
    std::map<std::string, std::vector<Replica::Descriptor>> replica_map;
    if (query_results.size() != keys.size()) {
        LOG(ERROR) << "Batch query response size mismatch in "
                      "batch_get_allocated_buffer_desc: expected "
                   << keys.size() << ", got " << query_results.size() << ".";
        return replica_map;
    }

    for (size_t i = 0; i < query_results.size(); ++i) {
        if (query_results[i]) {
            replica_map[keys[i]] = query_results[i].value().replicas;
        } else {
            LOG(ERROR) << "batch_get_replica failed for key: " << keys[i]
                       << " with error: " << toString(query_results[i].error());
        }
    }
    return replica_map;
}

std::vector<CachedQueryResultResponse> RealClient::batch_get_query_results(
    const std::vector<std::string> &keys) {
    if (!client_) {
        LOG(ERROR) << "batch_get_query_results: client not initialized";
        return std::vector<CachedQueryResultResponse>(
            keys.size(), CachedQueryResultResponse(ErrorCode::INVALID_PARAMS));
    }
    auto query_results = batch_query(keys);
    std::vector<CachedQueryResultResponse> cached_results;
    cached_results.reserve(keys.size());
    if (query_results.size() != keys.size()) {
        LOG(ERROR) << "Batch query response size mismatch in "
                      "batch_get_query_results: expected "
                   << keys.size() << ", got " << query_results.size() << ".";
        return std::vector<CachedQueryResultResponse>(
            keys.size(), CachedQueryResultResponse(ErrorCode::RPC_FAIL));
    }

    const auto now = std::chrono::steady_clock::now();
    for (const auto &query_result : query_results) {
        cached_results.push_back(
            to_cached_query_result_response(query_result, now));
    }
    return cached_results;
}

std::vector<std::string> RealClient::batch_replica_clear(
    const std::vector<std::string> &keys, const std::string &segment_name) {
    if (!client_) {
        LOG(ERROR) << "batch_replica_clear: client not initialized";
        return {};
    }
    auto result =
        client_->BatchReplicaClear(keys, client_->getClientId(), segment_name);
    if (result) {
        return result.value();
    }
    LOG(ERROR) << "batch_replica_clear failed: " << toString(result.error());
    return {};
}

tl::expected<UUID, ErrorCode> RealClient::create_copy_task(
    const std::string &key, const std::vector<std::string> &targets) {
    return client_->CreateCopyTask(key, targets);
}

tl::expected<UUID, ErrorCode> RealClient::create_move_task(
    const std::string &key, const std::string &source,
    const std::string &target) {
    return client_->CreateMoveTask(key, source, target);
}

tl::expected<QueryTaskResponse, ErrorCode> RealClient::query_task(
    const UUID &task_id) {
    return client_->QueryTask(task_id);
}
async_simple::coro::Lazy<tl::expected<BatchGetOffloadObjectResponse, ErrorCode>>
RealClient::batch_get_offload_object(const std::vector<std::string> &keys,
                                     const std::vector<int64_t> &sizes) {
    // Run SSD I/O on a dedicated thread pool so the coro_rpc IO thread is
    // free to handle ping and other RPCs. Same heap-owned state pattern as
    // batch_get_into_dummy_helper: the lambda captures only a raw pointer.
    struct CallState {
        std::vector<std::string> keys;
        std::vector<int64_t> sizes;
        std::shared_ptr<FileStorage> file_storage;
    };
    auto state = std::make_unique<CallState>();
    state->keys = keys;
    state->sizes = sizes;
    state->file_storage = file_storage_;
    auto *s = state.get();
    auto try_result = co_await coro_io::post(
        [s]() { return s->file_storage->BatchGet(s->keys, s->sizes); });
    auto result = try_result.value();
    if (!result) {
        LOG(ERROR) << "Batch get offload object failed,err_code = "
                   << result.error();
        co_return tl::make_unexpected(result.error());
    }
    co_return BatchGetOffloadObjectResponse(
        result.value().batch_id, std::move(result.value().pointers),
        client_->GetSegmentEndpoint(),
        file_storage_->config_.client_buffer_gc_ttl_ms);
}

bool RealClient::release_offload_buffer(uint64_t batch_id) {
    if (!file_storage_) {
        LOG(WARNING)
            << "release_offload_buffer called but file_storage_ is null";
        return false;
    }
    return file_storage_->ReleaseBuffer(batch_id);
}

bool RealClient::can_use_pinned_restore_arena(
    const std::string &target_rpc_service_addr,
    const std::unordered_map<std::string, std::vector<Slice>> &objects) const {
    if (!file_storage_ || target_rpc_service_addr != local_rpc_addr ||
        !file_storage_->HasPinnedRestoreArena()) {
        return false;
    }
    auto accelerators = device::GetAcceleratorRegistry().RuntimeAccelerators();
    bool has_data = false;
    for (const auto &object : objects) {
        for (const auto &slice : object.second) {
            if (slice.size && !accelerators.FindDeviceForPointer(slice.ptr)) {
                return false;
            }
            has_data |= slice.size != 0;
        }
    }
    return has_data;
}

tl::expected<void, ErrorCode>
RealClient::batch_get_into_offload_object_internal(
    const std::string &target_rpc_service_addr,
    std::unordered_map<std::string, std::vector<Slice>> &objects,
    const OffloadReadRange *read_range) {
    offload_rpc_read_count_.fetch_add(1, std::memory_order_relaxed);
    auto start_time = std::chrono::steady_clock::now();
    std::vector<std::string> keys;
    std::vector<std::string> storage_keys;
    std::vector<int64_t> sizes;
    if (read_range && objects.size() != 1) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const TenantId tenant_id(client_->tenant_id());
    for (const auto &object_it : objects) {
        keys.emplace_back(object_it.first);
        storage_keys.emplace_back(tenant_id.MakeScopedKey(object_it.first));
        uint64_t total = 0;
        for (const auto &slice : object_it.second) {
            if (slice.size > std::numeric_limits<uint64_t>::max() - total) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            total += slice.size;
        }
        if (total > uint64_t(std::numeric_limits<int64_t>::max())) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        int64_t storage_size = static_cast<int64_t>(total);
        if (read_range) {
            storage_size = read_range->restore_size;
            if (storage_size < 0 ||
                read_range->source_offset > uint64_t(storage_size) ||
                total > uint64_t(storage_size) - read_range->source_offset) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
        }
        sizes.emplace_back(storage_size);
    }

    const bool local_batch =
        can_use_pinned_restore_arena(target_rpc_service_addr, objects);
    std::optional<FileStorage::LocalBatchResult> local_owner;
    auto response =
        [&]() -> tl::expected<BatchGetOffloadObjectResponse, ErrorCode> {
        if (!local_batch) {
            return client_requester_->batch_get_offload_object(
                target_rpc_service_addr, storage_keys, sizes);
        }
        auto result = file_storage_->BatchGetLocal(storage_keys, sizes);
        if (!result) return tl::make_unexpected(result.error());
        local_owner.emplace(std::move(result.value()));
        return BatchGetOffloadObjectResponse(0,
                                             std::move(local_owner->pointers),
                                             client_->GetSegmentEndpoint(), 0);
    }();
    if (!response) {
        LOG(ERROR) << "Batch get offload object failed with error: "
                   << response.error();
        return tl::make_unexpected(response.error());
    }

    const auto release_buffer = [&]() {
        if (!local_batch) {
            client_requester_->release_offload_buffer(target_rpc_service_addr,
                                                      response->batch_id);
        }
    };
    struct ReleaseGuard {
        const decltype(release_buffer) &release;
        ~ReleaseGuard() { release(); }
    } release_guard{release_buffer};
    if (response->pointers.size() != keys.size()) {
        LOG(ERROR) << "Pointer count mismatch from owner: expected="
                   << keys.size() << ", got=" << response->pointers.size();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (read_range) {
        if (response->pointers[0] >
            std::numeric_limits<uint64_t>::max() - read_range->source_offset) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        response->pointers[0] += read_range->source_offset;
    }
    auto result = client_->BatchGetOffloadObject(
        response->transfer_engine_addr, keys, response->pointers, objects,
        local_batch ? OffloadBufferAccess::kLocalAddress
                    : OffloadBufferAccess::kTransferEngine);
    auto end_time = std::chrono::steady_clock::now();
    auto elapsed_time = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                              start_time)
            .count());
    LOG(INFO) << "Time taken for batch_get_into_offload_object_internal: "
              << elapsed_time
              << "ms, with target_rpc_service_addr: " << target_rpc_service_addr
              << ", key size: " << objects.size()
              << ", batch_id: " << response->batch_id
              << ", gc ttl: " << response->gc_ttl_ms << "ms.";

    if (!result) {
        LOG(ERROR) << "Batch get into offload object failed with error: "
                   << result.error();
        return result;
    }
    if (!local_batch && elapsed_time >= response->gc_ttl_ms) {
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
    }
    return {};
}

ClientRequester::ClientRequester() {
    coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config pool_conf{};
#ifdef YLT_ENABLE_IBV
    const char *value = std::getenv("MC_RPC_PROTOCOL");
    if (value && std::string_view(value) == "rdma") {
        pool_conf.client_config.socket_config =
            coro_io::ib_socket_t::config_t{};
    }
#endif
    // Configure reasonable retry limits for SSD offload RPC connections.
    // - connect_retry_count: Maximum connection retry attempts (default: 3)
    // - reconnect_wait_time: Wait time between retries (default: 1000ms)
    // - host_alive_detect_duration: Duration for background alive detection.
    //   Set to 0 to disable infinite background reconnection attempts when
    //   a Store node goes down. This prevents continuous "Connection refused"
    //   logs. When Master cleans up stale local_disk replicas (via
    //   CleanupStaleHandles), new requests won't route to dead nodes anyway.
    pool_conf.connect_retry_count = 3;
    pool_conf.reconnect_wait_time = std::chrono::milliseconds{1000};
    pool_conf.host_alive_detect_duration = std::chrono::milliseconds{0};

    // Honour the same timeout overrides as the master pool. Without this the
    // connect timeout stays at coro_rpc's built-in 30s, so a read that picks a
    // peer which is gone blocks for connect_retry_count * 30s plus the waits
    // between retries -- 91s with the defaults above -- and no configuration
    // can shorten it. Defaults are unchanged when the variables are unset.
    detail::ApplyRpcTimeoutEnvOverrides(pool_conf.client_config);

    client_pools_ =
        std::make_shared<coro_io::client_pools<coro_rpc::coro_rpc_client>>(
            pool_conf, GetStoreRpcClientIoContextPool());
}

tl::expected<BatchGetOffloadObjectResponse, ErrorCode>
ClientRequester::batch_get_offload_object(const std::string &client_addr,
                                          const std::vector<std::string> &keys,
                                          const std::vector<int64_t> &sizes) {
    auto result =
        invoke_rpc<&RealClient::batch_get_offload_object,
                   BatchGetOffloadObjectResponse>(client_addr, keys, sizes);
    if (!result) {
        LOG(ERROR)
            << "Failed to invoke batch_get_offload_object, client_addr = "
            << client_addr << ", error is: " << result.error();
    }
    return result;
}

void ClientRequester::release_offload_buffer(const std::string &client_addr,
                                             uint64_t batch_id) {
    // Fire-and-forget: attempt to release buffer, log errors but don't block
    auto result = invoke_rpc<&RealClient::release_offload_buffer, bool>(
        client_addr, batch_id);
    if (!result) {
        // This is expected in some cases (e.g., network issues, buffer already
        // GC'd) Log at INFO level since GC will eventually clean up anyway
        VLOG(1) << "Failed to release_offload_buffer for batch_id=" << batch_id
                << " at " << client_addr
                << " (will be GC'd): " << result.error();
    } else {
        VLOG(1) << "Successfully released buffer for batch_id=" << batch_id
                << " at " << client_addr;
    }
}

template <auto ServiceMethod, typename ReturnType, typename... Args>
tl::expected<ReturnType, ErrorCode> ClientRequester::invoke_rpc(
    const std::string &client_addr, Args &&...args) {
    auto client_pool = client_pools_->at(client_addr);
    return async_simple::coro::syncAwait(
        [&]() -> async_simple::coro::Lazy<tl::expected<ReturnType, ErrorCode>> {
            auto ret = co_await client_pool->send_request(
                [&](coro_io::client_reuse_hint,
                    coro_rpc::coro_rpc_client &client) {
                    return client.send_request<ServiceMethod>(
                        std::forward<Args>(args)...);
                });
            if (!ret.has_value()) {
                LOG(ERROR) << "Dummy Client not available";
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            auto result = co_await std::move(ret.value());
            if (!result) {
                if (result.error().code == coro_rpc::errc::timed_out) {
                    LOG(ERROR) << "RPC call timed out: " << result.error().msg;
                    co_return tl::make_unexpected(ErrorCode::RPC_TIMEOUT);
                }
                LOG(ERROR) << "RPC call failed: " << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
}
}  // namespace mooncake
