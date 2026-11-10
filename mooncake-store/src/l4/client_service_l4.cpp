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
#include <iterator>
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

namespace {

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

}  // namespace



void Client::ObserveL4Read(bool prefetch, bool success, uint64_t bytes,
                           double duration_seconds) {
    if (metrics_ != nullptr) {
        metrics_->ObserveL4Read(prefetch, success, bytes, duration_seconds);
    }
    std::lock_guard<std::mutex> lock(service_metrics_mutex_);
    if (service_metrics_snapshot_.service_endpoint.empty()) return;
    auto& l4 = service_metrics_snapshot_.l4;
    const size_t index = (prefetch ? 0 : 2) + (success ? 0 : 1);
    ++l4.read_operations[index];
    l4.read_bytes[index] += bytes;
    l4.read_duration_us[index] += static_cast<uint64_t>(duration_seconds * 1e6);
    const auto bucket =
        std::lower_bound(kL4ReadLatencySecondsBuckets.begin(),
                         kL4ReadLatencySecondsBuckets.end(), duration_seconds);
    ++l4.read_duration_buckets[index]
                              [bucket - kL4ReadLatencySecondsBuckets.begin()];
}

void Client::ObserveL4Restore(bool prefetched, uint64_t ranges,
                              uint64_t bytes) {
    if (metrics_ != nullptr) {
        metrics_->ObserveL4Restore(prefetched, ranges, bytes);
    }
    std::lock_guard<std::mutex> lock(service_metrics_mutex_);
    if (service_metrics_snapshot_.service_endpoint.empty()) return;
    const size_t index = prefetched ? 0 : 1;
    service_metrics_snapshot_.l4.restore_ranges[index] += ranges;
    service_metrics_snapshot_.l4.restore_bytes[index] += bytes;
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
    if (original.object_checksum != refreshed.object_checksum ||
        original.resolved_tenant_id != refreshed.resolved_tenant_id) {
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
                           original.object_checksum,
                           original.resolved_tenant_id);
    }
    return tl::unexpected(ErrorCode::DFS_STALE_HANDLE);
}

void Client::AsyncDfsWriteContext::ReleaseStagingBuffer() {
    staging_buffer.reset();
}

Client::AsyncDfsWriteContext::~AsyncDfsWriteContext() {
    slices.clear();
    ReleaseStagingBuffer();
}

bool Client::StageDfsWriteData(
    AsyncDfsWriteContext& context,
    const std::vector<const std::vector<Slice>*>& slice_lists) {
    if (!context.staging_buffer || !context.staging_buffer->ptr()) return false;

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

            if (staging_offset > context.staging_buffer->size() ||
                slice.size > context.staging_buffer->size() - staging_offset) {
                LOG(ERROR) << "DFS write staging arena exhausted for key "
                           << context.keys[i];
                return false;
            }
            char* destination =
                static_cast<char*>(context.staging_buffer->ptr()) +
                staging_offset;
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

    // Count physical backend outcomes before the publication deadline can
    // turn an otherwise successful write into a logical timeout.
    if (auto* dfs_metric = GetDfsMetricPtr()) {
        const auto successful_keys = static_cast<int64_t>(std::count(outcomes.begin(), outcomes.end(), ErrorCode::OK));
        if (successful_keys > 0) {
            dfs_metric->ObserveWrite(successful_keys, successful_bytes,
                                     static_cast<uint64_t>(write_duration_seconds * 1e6));
        }
        for (auto outcome : outcomes) {
            if (outcome != ErrorCode::OK) dfs_metric->RecordWriteErrors(toString(outcome));
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
    context->ReleaseStagingBuffer();

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

void Client::DrainAsyncDfsWrites() {
    dfs_writes_shutting_down_.store(true, std::memory_order_release);
    std::unique_lock<std::mutex> lock(dfs_inflight_mutex_);
    // Wait for tasks that still use master_client_ / dfs_storage_backend_ so
    // they cannot outlive the Client and access freed state.
    dfs_inflight_cv_.wait(lock, [this] { return dfs_inflight_writes_ == 0; });
}

tl::expected<DfsBucketSnapshotResponse, ErrorCode>
Client::GetDfsBucketSnapshot(const UUID& last_epoch, uint64_t last_version) {
    return master_client_.GetDfsBucketSnapshot(client_id_, last_epoch,
                                                last_version);
}

void Client::SetDfsWriteChunkSize(size_t chunk_size) {
    dfs_write_chunk_size_ = chunk_size;
}

void Client::SetDfsWriteAlignment(size_t alignment) {
    if (alignment != 0) {
        dfs_write_alignment_ = alignment;
    }
}

void Client::SetUnifiedDfsBuffer(
    std::shared_ptr<ClientBufferAllocator> allocator, size_t capacity) {
    unified_buffer_allocator_ = std::move(allocator);
    unified_buffer_capacity_ = capacity;
    if (!unified_buffer_allocator_ || unified_buffer_capacity_ == 0) return;
    if (!unified_buffer_log_running_.exchange(true)) {
        unified_buffer_log_thread_ =
            std::thread(&Client::LogUnifiedDfsBufferUsage, this);
    }
}

void Client::LogUnifiedDfsBufferUsage() {
    std::unique_lock<std::mutex> lock(unified_buffer_log_mutex_);
    while (unified_buffer_log_running_.load(std::memory_order_acquire)) {
        unified_buffer_log_cv_.wait_for(
            lock, std::chrono::seconds(5), [this] {
                return !unified_buffer_log_running_.load(
                    std::memory_order_acquire);
            });
        if (!unified_buffer_log_running_.load(std::memory_order_acquire)) {
            break;
        }
        const auto allocator = unified_buffer_allocator_;
        const auto tracker = allocator ? allocator->usage_tracker() : nullptr;
        const auto snapshot = tracker ? tracker->Snapshot()
                                      : DfsBufferUsageSnapshot{};
        const double capacity = static_cast<double>(unified_buffer_capacity_);
        const auto utilization = [capacity](uint64_t bytes) {
            return capacity == 0.0 ? 0.0 : bytes * 100.0 / capacity;
        };
        size_t write_backlog = 0;
        {
            std::lock_guard<std::mutex> inflight_lock(dfs_inflight_mutex_);
            write_backlog = dfs_inflight_writes_;
        }
        if (metrics_) {
            metrics_->unified_buffer_metric.capacity_bytes.update(
                static_cast<int64_t>(unified_buffer_capacity_));
            metrics_->unified_buffer_metric.read_bytes.update(
                static_cast<int64_t>(snapshot.read_bytes));
            metrics_->unified_buffer_metric.prefetch_bytes.update(
                static_cast<int64_t>(snapshot.prefetch_bytes));
            metrics_->unified_buffer_metric.write_bytes.update(
                static_cast<int64_t>(snapshot.write_bytes));
            metrics_->unified_buffer_metric.total_bytes.update(
                static_cast<int64_t>(snapshot.total_bytes));
            metrics_->unified_buffer_metric.peak_total_bytes.update(
                static_cast<int64_t>(snapshot.peak_total_bytes));
            metrics_->unified_buffer_metric.write_backlog.update(
                static_cast<int64_t>(write_backlog));
        }
        if (!dfs_read_trace_enabled()) continue;
        LOG(INFO) << "action=dfs_unified_buffer_usage scope=process"
                  << " cap=" << unified_buffer_capacity_
                  << " rd=" << snapshot.read_bytes
                  << " pf=" << snapshot.prefetch_bytes
                  << " wr=" << snapshot.write_bytes
                  << " total=" << snapshot.total_bytes
                  << " rd_util=" << std::fixed << std::setprecision(1)
                  << utilization(snapshot.read_bytes) << "%"
                  << " pf_util=" << utilization(snapshot.prefetch_bytes) << "%"
                  << " wr_util=" << utilization(snapshot.write_bytes) << "%"
                  << " total_util=" << utilization(snapshot.total_bytes) << "%"
                  << " peak=" << snapshot.peak_total_bytes
                  << " peak_util="
                  << utilization(snapshot.peak_total_bytes) << "%"
                  << " wr_backlog=" << write_backlog;
    }
}

void Client::SetDfsBatchTimeoutFixedMs(uint64_t timeout_ms) {
    const uint64_t old_timeout_ms = dfs_batch_timeout_fixed_ms_.exchange(
        timeout_ms, std::memory_order_acq_rel);
    if (dfs_storage_backend_) {
        dfs_storage_backend_->SetBatchTimeoutFixedMs(timeout_ms);
    }
    if (old_timeout_ms != timeout_ms) {
        LOG(INFO) << "Updated DFS batch timeout fixed budget from "
                  << old_timeout_ms << "ms to " << timeout_ms << "ms";
    }
}

tl::expected<std::string, ErrorCode> Client::FetchDfsRootIdentity() {
    auto config = master_client_.GetStorageConfig();
    if (!config) return tl::make_unexpected(config.error());
    if (config->dfs_root_identity.empty()) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    return config->dfs_root_identity;
}

void Client::StopDfsWritesAndBufferLogging() {
    // Stop accepting new asynchronous DFS writes before draining tasks already
    // submitted. Their BufferHandles retain the unified allocator.
    unified_buffer_log_running_.store(false, std::memory_order_release);
    unified_buffer_log_cv_.notify_all();
    if (unified_buffer_log_thread_.joinable()) {
        unified_buffer_log_thread_.join();
    }
    if (unified_buffer_allocator_) {
        unified_buffer_allocator_->ShutdownAllocationWaiters();
    }
    // They report their result through master_client_ and write through
    // dfs_storage_backend_, so they must not outlive this object.
    DrainAsyncDfsWrites();
    write_thread_pool_.stop();
    StopServiceMetricsReporter();
}

}  // namespace mooncake
