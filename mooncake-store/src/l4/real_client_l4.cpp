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

#include "l4/dfs_h2d_stream_pool.h"
#include "l4/real_client_read_internal.h"

namespace mooncake {

namespace l4 {

namespace {
std::atomic<uint64_t> g_dfs_read_trace_id{0};
}  // namespace

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

bool SameAllocatedBufferDescriptor(
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
bool SameReplicaForSessionCache(const Replica::Descriptor &lhs,
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
               left.shard_idx == right.shard_idx &&
               left.MasterEpoch() == right.MasterEpoch();
    }
    return false;
}

bool CompatibleSessionCacheRefresh(const QueryResult &old_session,
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

bool SameGetSessionSnapshot(const QueryResult &lhs,
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

}  // namespace l4

using l4::NextDfsReadTraceId;
using l4::DirectSourceForReplica;
using l4::CompatibleSessionCacheRefresh;
using l4::SameGetSessionSnapshot;
using l4::AlignUp;
using l4::DetachPrefetchedSessionBuffer;
using l4::FilterQueryResult;
using l4::SelectSessionReplica;

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


void RealClient::SetDfsReadChunkSize(size_t chunk_size) {
    dfs_read_chunk_size_ = chunk_size;
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
                    refresh_error =
                        ErrorCode::SESSION_REFRESH_QUERY_LEASE_EXPIRED;
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
                refresh_error = ErrorCode::SESSION_REFRESH_COMMIT_LEASE_EXPIRED;
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
    std::unique_lock<std::shared_mutex> dfs_read_lock(
        dfs_read_lifecycle_mutex_);
    {
        std::lock_guard<std::mutex> session_lock(session_mutex_);
        if (!get_sessions_.empty() || !get_session_access_records_.empty() ||
            !get_session_prefetch_cache_.empty() || !put_sessions_.empty()) {
            return false;
        }
    }

    prefetch_arena_available_.store(false, std::memory_order_release);
    return true;
}

std::string RealClient::dfs_prefetch_arena_status() const {
    if (!file_storage_) {
        return "unavailable: no FileStorage (SSD offload disabled)";
    }
    if (!file_storage_->HasPinnedRestoreArena()) {
        return "unavailable: unified DFS buffer is not initialized";
    }
    if (file_storage_->PrefetchCapacityRatio() <= 0.0) {
        return "disabled: prefetch capacity ratio is zero";
    }
    if (file_storage_->UnifiedPrefetchLimitBytes() == 0) {
        return "disabled: prefetch limit is zero after read/write protection";
    }
    return prefetch_arena_available_.load(std::memory_order_acquire)
               ? "ready"
               : "disabled";
}

std::shared_ptr<BufferHandle> RealClient::AllocatePrefetchArenaRegion(
    size_t size, size_t alignment) {
    if (!dfs_prefetch_arena_available() || !file_storage_) return nullptr;
    auto allocation = file_storage_->AllocatePinnedPrefetchBuffer(size, alignment);
    if (!allocation) {
        if (client_) {
            client_->ObserveUnifiedDfsBufferAllocationFailure(
                DfsBufferUsageKind::kPrefetch);
        }
        LOG(ERROR) << "DFS_UNIFIED_BUFFER_ALLOCATION_FAILED kind=prefetch size="
                   << size << " alignment=" << alignment;
        return nullptr;
    }
    return std::make_shared<BufferHandle>(std::move(*allocation));
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
                results[i] = static_cast<int>(
                    toInt(ErrorCode::SESSION_PREFETCH_LEASE_EXPIRED));
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
            if ((results[i] == static_cast<int>(toInt(
                                   ErrorCode::SESSION_RANGE_LEASE_EXPIRED)) ||
                 results[i] ==
                     static_cast<int>(toInt(
                         ErrorCode::SESSION_MEMORY_TRANSFER_LEASE_EXPIRED))) &&
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



ErrorCode RealClient::execute_session_dfs_range_reads_batch(
    std::vector<SessionRangeReadRequest *> &entries,
    std::vector<int> &results, uint64_t trace_id, bool prefetch_only,
    size_t chunk_index, size_t chunk_count, size_t chunk_limit_bytes,
    size_t request_planned_bytes) {
    const auto timing_start = std::chrono::steady_clock::now();
    const auto chunk_entries = entries;
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
        bool from_prefetch_cache;
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
            PendingScatterResult{entry, transferred, from_prefetch_cache});
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
    std::vector<std::vector<Slice>> disk_batch_slices;
    std::vector<std::shared_ptr<BufferHandle>> disk_temp_handles;

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
            // Cached prefetch views retain their unified buffer allocation
            // until session end and count against the prefetch admission limit.
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
            const bool has_dfs_reads = std::any_of(
                arena_views.begin(), arena_views.end(), [](const auto &view) {
                    return view.entry->replica.is_dfs_replica();
                });
            // Enforce an active unified budget while preserving ordinary reads
            // when it is explicitly disabled or this batch only uses local SSD.
            const bool unified_budget_active =
                has_dfs_reads && file_storage_ &&
                file_storage_->HasPinnedRestoreArena();
            if (!allocation && !unified_budget_active &&
                client_buffer_allocator_) {
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
                if (client_) {
                    client_->ObserveUnifiedDfsBufferAllocationFailure(
                        DfsBufferUsageKind::kRead);
                }
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
        // A key can appear in multiple range requests with different targets.
        // Keep each arena view aligned with its own read and completion.
        disk_batch_slices.push_back(std::move(disk_slices));
        disk_temp_handles.push_back(std::move(handle));
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
        std::vector<std::shared_ptr<void>> disk_buffer_owners(
            disk_temp_handles.begin(), disk_temp_handles.end());
        const auto io_start = std::chrono::steady_clock::now();
        auto disk_results = client_->BatchGet(disk_batch_keys, disk_batch_qrs,
                                              disk_batch_slices, false,
                                              &disk_buffer_owners);
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
                for (const auto &slice : disk_batch_slices[i]) {
                    dfs_read_bytes += slice.size;
                }
            }
        }
        t_io_done = std::chrono::steady_clock::now();
        client_->ObserveDirectIo(
            "read", DirectStorageMetricSource(ReplicaType::DFS),
            dfs_read_success,
            dfs_read_bytes, io_duration);
        client_->ObserveL4Read(prefetch_only, dfs_read_success, dfs_read_bytes,
                               io_duration);

        // Build a copy plan before submitting any DMA. Destination pointers are
        // resolved once, adjacent compatible ranges are merged, and operations
        // are grouped by accelerator and physical device.
        for (size_t di = 0; di < disk_batch_keys.size(); ++di) {
            const auto &key = disk_batch_keys[di];
            auto *entry = arena_views[di].entry;

            // A short backend response is a failure for the missing entries;
            // do not index past the returned vector.
            if (di >= disk_results.size()) {
                results[entry->original_idx] =
                    static_cast<int>(toInt(ErrorCode::INTERNAL_ERROR));
                continue;
            }

            if (!disk_results[di]) {
                results[entry->original_idx] =
                    static_cast<int>(toInt(disk_results[di].error()));
                continue;
            }

            auto handle = std::move(disk_temp_handles[di]);
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
        // Count useful payload only after the scatter has completed. Host
        // prefetch alone never reaches this point or counts as a restore.
        if (pending.entry->replica.is_dfs_replica()) {
            client_->ObserveL4Restore(pending.from_prefetch_cache,
                                      pending.entry->sizes.size(),
                                      pending.transferred);
        }
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

    ErrorCode chunk_error = ErrorCode::OK;
    for (const auto *entry : chunk_entries) {
        const int result = results[entry->original_idx];
        if (result >= 0) continue;
        const auto error = static_cast<ErrorCode>(result);
        if (error == ErrorCode::DFS_BATCH_GET_TIMEOUT ||
            error == ErrorCode::DFS_BATCH_GET_ADMISSION_TIMEOUT) {
            return error;
        }
        if (chunk_error == ErrorCode::OK) chunk_error = error;
    }
    return chunk_error;
}

void RealClient::DetachDfsPrefetchBuffers(
    std::vector<std::shared_ptr<BufferHandle>> &detached_prefetch_buffers) {
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
    }
}

void RealClient::CollectOrphanedDfsPrefetchBuffers(
    bool prefetch_cache_enabled, SessionRangeReadContext &context,
    std::vector<std::shared_ptr<BufferHandle>> &detached_prefetch_buffers) {
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
}

std::vector<std::vector<SessionRangeReadRequest *>>
RealClient::PlanDfsReadChunks(
    const std::vector<SessionRangeReadRequest *> &entries,
    size_t chunk_limit_bytes, size_t &request_planned_bytes) {
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
    return chunks;
}

}  // namespace mooncake
