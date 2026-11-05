#include "l4/distributed_storage_internal.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/dfs_log_limiter.h"

#include <algorithm>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iterator>
#include <limits>
#include <mutex>
#include <optional>
#include <sstream>
#include <string_view>
#include <sys/stat.h>
#include <system_error>
#include <thread>
#include <unordered_set>

#include "environ.h"
#include "storage/distributed/bucket_entry_layout.h"
#include "storage/distributed/dfs_io_timeout.h"
#include "storage/distributed/immutable_bucket_allocator.h"
#include "storage/distributed/dfs_root_identity.h"
#include "storage/distributed/dfs_read_probe.h"
#include "storage/distributed/shard_allocator.h"
#include "thread_pool.h"
#include "types.h"
#include "utils.h"

namespace mooncake {

namespace {

tl::expected<uint64_t, ErrorCode> CalculateSliceBytes(
    std::span<const Slice> slices) {
    uint64_t total = 0;
    for (const auto& slice : slices) {
        if ((slice.ptr == nullptr && slice.size != 0) ||
            slice.size > std::numeric_limits<uint64_t>::max() - total) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        total += slice.size;
    }
    return total;
}

std::optional<int> ParseShardFileName(std::string_view name) {
    constexpr std::string_view prefix = "dfs_shard_";
    constexpr std::string_view suffix = ".data";
    if (!name.starts_with(prefix) || !name.ends_with(suffix)) {
        return std::nullopt;
    }
    name.remove_prefix(prefix.size());
    name.remove_suffix(suffix.size());
    if (name.size() < 2) return std::nullopt;

    int index = 0;
    for (char digit : name) {
        if (digit < '0' || digit > '9' ||
            index > (std::numeric_limits<int>::max() - (digit - '0')) / 10) {
            return std::nullopt;
        }
        index = index * 10 + (digit - '0');
    }
    return index;
}


constexpr char kDfsHealthProbeFilePrefix[] = ".mooncake_dfs_health_probe_";

using l4::kBucketFilePrefix;
using l4::kBucketDataSuffix;

bool IsDfsDescriptorRangeValid(const DistributedFSDescriptor& desc,
                               const DistributedStorageConfig& config) {
    if (config.alignment == 0 || desc.object_size == 0 ||
        desc.aligned_size < desc.object_size ||
        desc.offset % config.alignment != 0 ||
        desc.aligned_size % config.alignment != 0) {
        return false;
    }
    if (desc.offset > config.shard_capacity ||
        desc.aligned_size > config.shard_capacity - desc.offset) {
        return false;
    }

    constexpr uint64_t kMaxFileOffset =
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    return desc.offset <= kMaxFileOffset &&
           desc.aligned_size <= kMaxFileOffset - desc.offset;
}

/**
 * @brief Validate a BUCKET-mode descriptor.
 *
 * A bucket descriptor points directly at an alignment-aligned object start;
 *
 * the reserved size covers the object plus trailing alignment padding.
 */
bool IsBucketDescriptorRangeValid(const DistributedFSDescriptor& desc,
                                  const std::string& key,
                                  const DistributedStorageConfig& config) {
    if (desc.object_size == 0 || desc.shard_idx < 0) return false;
    if (key.empty()) return false;

    auto layout = RebuildBucketEntryLayout(desc.offset, desc.object_size,
                                           config.alignment);
    if (!layout) return false;
    if (layout->offset != desc.offset) return false;
    if (layout->aligned_size != desc.aligned_size) return false;
    if (layout->end() > config.bucket_capacity) return false;

    constexpr uint64_t kMaxFileOffset =
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    return layout->offset <= kMaxFileOffset &&
           layout->aligned_size <= kMaxFileOffset - layout->offset;
}

}  // namespace

DistributedStorageBackend::DistributedStorageBackend(
    const FileStorageConfig& file_storage_config,
    const DistributedStorageConfig& distributed_config,
    std::unique_ptr<FileSystemAdapter> fs_adapter)
    : DistributedStorageBackend(file_storage_config, distributed_config,
                                std::move(fs_adapter), nullptr, {}) {}

DistributedStorageBackend::DistributedStorageBackend(
    const FileStorageConfig& file_storage_config,
    const DistributedStorageConfig& distributed_config,
    std::unique_ptr<FileSystemAdapter> fs_adapter,
    std::string expected_root_identity)
    : DistributedStorageBackend(file_storage_config, distributed_config,
                                std::move(fs_adapter), nullptr,
                                std::move(expected_root_identity)) {}

DistributedStorageBackend::DistributedStorageBackend(
    const FileStorageConfig& file_storage_config,
    const DistributedStorageConfig& distributed_config,
    std::unique_ptr<FileSystemAdapter> fs_adapter,
    std::unique_ptr<ObjectStorageAdapter> object_storage_adapter,
    std::string expected_root_identity)
    : StorageBackendInterface(file_storage_config),
      fs_adapter_(std::move(fs_adapter)),
      object_storage_adapter_(std::move(object_storage_adapter)),
      distributed_config_(distributed_config),
      batch_timeout_fixed_ms_(distributed_config.batch_timeout_fixed_ms),
      expected_root_identity_(std::move(expected_root_identity)),
      root_dir_(distributed_config.fsdir),
      descriptor_root_dir_(distributed_config.fsdir) {
    CHECK((fs_adapter_ != nullptr) != (object_storage_adapter_ != nullptr))
        << "DistributedStorageBackend: exactly one I/O adapter is required";
    if (object_storage_adapter_) {
        storage_mode_ = DistributedStorageMode::kObjectStorage;
    }
    health_probe_path_prefix_ = root_dir_ + "/" + kDfsHealthProbeFilePrefix +
                                UuidToString(generate_uuid());
}

DistributedStorageBackend::~DistributedStorageBackend() {
    for (auto& [_, shard] : shard_files_) {
        if (shard && fs_adapter_) {
            const int write_fd = shard->write_fd.load();
            if (write_fd >= 0) {
                fs_adapter_->CloseFile(write_fd);
            }
            if (shard->read_fd >= 0 && shard->read_fd != write_fd) {
                fs_adapter_->CloseFile(shard->read_fd);
            }
            shard->read_fd = -1;
            shard->write_fd.store(-1);
        }
    }
    if (fs_adapter_ && !active_health_probe_path_.empty()) {
        (void)fs_adapter_->DeleteFile(active_health_probe_path_);
    }
    // Drop cached bucket handles before the adapter goes away; each handle
    // closes its own fd in its destructor via the adapter pointer it holds.
    {
        std::lock_guard<std::mutex> lock(bucket_cache_mutex_);
        bucket_id_cache_.clear();
        bucket_id_direct_cache_.clear();
    }
    batch_read_pool_.reset();
    if (fs_adapter_) fs_adapter_->Shutdown();
}

tl::expected<void, ErrorCode> DistributedStorageBackend::Init() {
    std::lock_guard<std::mutex> lock(init_mutex_);
    return InitLocked();
}

tl::expected<void, ErrorCode> DistributedStorageBackend::InitLocked() {
    if (initialized_.load(std::memory_order_acquire)) {
        LOG(WARNING) << "DistributedStorageBackend is already initialized";
        return ValidateRootIdentity();
    }

    if (UsesObjectStorage()) {
        auto init_result = object_storage_adapter_->Init();
        if (!init_result) return init_result;
        if (distributed_config_.enable_health_check) {
            auto health = object_storage_adapter_->CheckHealth();
            if (!health) return health;
        }
        initialized_.store(true, std::memory_order_release);
        LOG(INFO) << "DistributedStorageBackend initialized, object adapter="
                  << object_storage_adapter_->GetName();
        return {};
    }

    std::error_code ec;
    if (!std::filesystem::is_directory(root_dir_, ec) || ec) {
        LOG(ERROR) << "DFS root directory is unavailable: " << root_dir_;
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }

    auto init_result = fs_adapter_->Init(root_dir_);
    if (!init_result) {
        const auto error = init_result.error();
        ResetFailedInitializationLocked();
        return tl::make_unexpected(error);
    }

    // Initialization is serialized and no batch I/O can run until it succeeds.
    // Retry the control plane when DFS was disabled at client startup, without
    // adding RPCs to business I/O or trusting the local marker as authority.
    if (expected_root_identity_.empty() && root_identity_provider_) {
        auto identity = root_identity_provider_();
        if (identity && !identity->empty() &&
            identity->size() <= kMaxDfsRootIdentitySize) {
            expected_root_identity_ = std::move(*identity);
        }
    }
    auto identity_result = ValidateRootIdentity();
    if (!identity_result) {
        LOG(ERROR) << "DFS root identity validation failed for " << root_dir_;
        ResetFailedInitializationLocked();
        return identity_result;
    }

    if (IsBucketMode()) {
        return InitBucketModeLocked();
    }

    // Published descriptors alone identify shards; never create or preopen them.
    initialized_.store(true, std::memory_order_release);
    return {};
}

tl::expected<DistributedStorageBackend::ResolvedTarget, ErrorCode>
DistributedStorageBackend::ResolveTarget(
    const DistributedFSDescriptor& descriptor, const std::string& key,
    bool read_only) {
    // BUCKET mode: the descriptor carries an allocator-chosen path, so it must
    // be validated before it is used to open anything.
    if (descriptor.shard_idx < 0) {
        LOG(ERROR) << "Invalid DFS bucket id " << descriptor.shard_idx
                   << " for key " << key;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const std::string expected_path =
        descriptor_root_dir_ + "/" + kBucketFilePrefix +
        ImmutableBucketAllocator::FormatBucketId(descriptor.shard_idx) +
        kBucketDataSuffix;
    // The master's mount path may not exist on this client.
    if (std::filesystem::path(descriptor.file_path).lexically_normal() !=
        std::filesystem::path(expected_path).lexically_normal()) {
        LOG(ERROR) << "DFS bucket path mismatch for key " << key
                   << ", descriptor=" << descriptor.file_path
                   << ", expected=" << expected_path;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!IsBucketDescriptorRangeValid(descriptor, key, distributed_config_)) {
        LOG(ERROR) << "Invalid DFS bucket descriptor for key " << key
                   << ", offset=" << descriptor.offset
                   << ", object_size=" << descriptor.object_size
                   << ", aligned_size=" << descriptor.aligned_size
                   << ", bucket_capacity="
                   << distributed_config_.bucket_capacity;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto handle = read_only ? GetOrOpenBucketDirect(descriptor.shard_idx,
                                                    descriptor.MasterEpoch())
                            : GetOrOpenBucket(descriptor.shard_idx,
                                              descriptor.MasterEpoch());
    if (!handle && read_only) {
        if (handle.error() != ErrorCode::NOT_SUPPORTED) {
            return tl::make_unexpected(handle.error());
        }
        handle =
            GetOrOpenBucket(descriptor.shard_idx, descriptor.MasterEpoch());
        read_only = false;
    }
    if (!handle) return tl::make_unexpected(handle.error());
    auto& shared = handle.value();
    // BUCKET mode needs no per-fd lock: the allocator hands each object a
    // unique, non-overlapping on-disk range, so concurrent writers and readers
    // never touch the same bytes.
    return ResolvedTarget{shared->fd, nullptr, read_only, shared};
}

tl::expected<int64_t, ErrorCode> DistributedStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::function<ErrorCode(const std::vector<std::string>& keys,
                            std::vector<StorageObjectMetadata>& metadatas)>
        complete_handler,
    EvictionHandler eviction_handler) {
    if (!UsesObjectStorage()) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (eviction_handler) {
        LOG_FIRST_N(WARNING, 1)
            << "DistributedStorageBackend does not support eviction, "
               "eviction_handler ignored";
    }

    std::vector<std::string> success_keys;
    std::vector<StorageObjectMetadata> success_metas;
    std::vector<ObjectPutRequest> requests;
    // Own descriptors through PutBatch; each request only borrows an array.
    std::vector<std::vector<iovec>> iov_storage;
    std::vector<size_t> request_sizes;
    requests.reserve(batch_object.size());
    iov_storage.reserve(batch_object.size());
    request_sizes.reserve(batch_object.size());
    for (const auto& [key, slices] : batch_object) {
        if (slices.size() >
            static_cast<size_t>(std::numeric_limits<int>::max())) {
            LOG(WARNING) << "Failed to offload key " << key
                         << ": slice count exceeds INT_MAX";
            continue;
        }
        std::vector<iovec> iovs;
        iovs.reserve(slices.size());
        size_t total_size = 0;
        bool total_size_overflow = false;
        for (const auto& slice : slices) {
            if (slice.size > std::numeric_limits<size_t>::max() - total_size) {
                total_size_overflow = true;
                break;
            }
            iovs.push_back({slice.ptr, slice.size});
            total_size += slice.size;
        }
        if (total_size_overflow) {
            LOG(WARNING) << "Failed to offload key " << key
                         << ": total slice size overflows size_t";
            continue;
        }

        iov_storage.push_back(std::move(iovs));
        const auto& stored_iovs = iov_storage.back();
        requests.push_back(
            {key, stored_iovs.data(), static_cast<int>(stored_iovs.size())});
        request_sizes.push_back(total_size);
    }

    auto results = object_storage_adapter_->PutBatch(requests);
    if (results.size() != requests.size()) {
        LOG(ERROR)
            << "Object storage returned an invalid PUT batch result count";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i]) {
            LOG(WARNING) << "Failed to offload key " << requests[i].logical_key
                         << ": " << static_cast<int>(results[i].error());
            continue;
        }
        success_keys.push_back(requests[i].logical_key);
        success_metas.emplace_back(
            -1, 0, static_cast<int64_t>(requests[i].logical_key.size()),
            static_cast<int64_t>(request_sizes[i]), "");
    }

    if (complete_handler && !success_keys.empty()) {
        auto err = complete_handler(success_keys, success_metas);
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }
    return static_cast<int64_t>(success_keys.size());
}

struct DistributedStorageBackend::DfsIoOp {
    size_t index;  // Position in the caller's request batch.
    const std::string& key;
    const DistributedFSDescriptor& descriptor;
    std::vector<iovec> iovs;
    uint64_t size;
    int64_t file_offset = 0;
};

tl::expected<void, ErrorCode> DistributedStorageBackend::ExecuteSingleIo(
    DfsIoOp& op, bool write) {
    auto shard = GetOrOpenShard(op.descriptor, write);
    if (!shard) {
        LOG(ERROR) << "Failed to open DFS shard for key " << op.key << ": "
                   << shard.error();
        return tl::make_unexpected(shard.error());
    }
    std::lock_guard lock((*shard)->mutex);
    if ((write ? (*shard)->write_fd.load() : (*shard)->read_fd) < 0) {
        auto reopened = ReopenShardLocked(**shard, write);
        if (!reopened) return reopened;
    }
    const auto count = static_cast<int>(op.iovs.size());
    const auto offset = op.file_offset;
    auto result = write ? fs_adapter_->WriteAt((*shard)->write_fd.load(),
                                               op.iovs.data(), count, offset)
                        : fs_adapter_->ReadAt((*shard)->read_fd, op.iovs.data(),
                                              count, offset);
    const char* op_name = write ? "write" : "read";
    if (!result && fs_adapter_->SupportsFdReopen()) {
        // A cached descriptor can go stale (fd invalidated, shard file
        // rotated). Reopen once under the shard lock and retry before
        // surfacing the failure. A short transfer below is treated as genuine
        // corruption and not retried. The lock already serializes shard I/O, so
        // the reopen adds no cost to the success path. Only adapters whose I/O
        // faults are fd-recoverable (POSIX) opt in via SupportsFdReopen.
        LOG(WARNING) << "DFS " << op_name << " failed for key " << op.key
                     << ", error=" << result.error() << ", reopening shard "
                     << op.descriptor.shard_idx;
        auto reopen = ReopenShardLocked(**shard, write);
        if (reopen) {
            result = write ? fs_adapter_->WriteAt((*shard)->write_fd.load(),
                                                   op.iovs.data(), count, offset)
                           : fs_adapter_->ReadAt((*shard)->read_fd,
                                                  op.iovs.data(), count, offset);
        } else {
            LOG(ERROR) << "DFS shard " << op.descriptor.shard_idx
                       << " reopen failed for key " << op.key
                       << ", error=" << reopen.error();
        }
    }
    if (!result) {
        LOG(WARNING) << "DFS " << op_name << " failed for key " << op.key
                     << ", error=" << result.error();
        return tl::make_unexpected(result.error());
    }
    if (*result != op.size) {
        LOG(WARNING) << "DFS short " << op_name << " for key " << op.key
                     << ", expected=" << op.size << ", actual=" << *result;
        return tl::make_unexpected(write ? ErrorCode::FILE_WRITE_FAIL
                                         : ErrorCode::FILE_READ_FAIL);
    }
    return {};
}

void DistributedStorageBackend::ExecuteShardBatchIo(
    std::vector<DfsIoOp>& ops,
    std::vector<tl::expected<void, ErrorCode>>& results, bool write) {
    std::vector<FdIoRequest> io_requests;
    std::vector<DfsIoOp*> io_ops;
    io_requests.reserve(ops.size());
    io_ops.reserve(ops.size());
    for (auto& op : ops) {
        auto shard = GetOrOpenShard(op.descriptor, write);
        if (!shard) {
            LOG(ERROR) << "Failed to open DFS shard for key " << op.key << ": "
                       << shard.error();
            results[op.index] = tl::make_unexpected(shard.error());
            continue;
        }
        io_requests.push_back({(write ? (*shard)->write_fd.load() : (*shard)->read_fd), op.iovs.data(),
                               static_cast<int>(op.iovs.size()),
                               op.file_offset});
        io_ops.push_back(&op);
    }
    // Shard I/O fails short transfers.
    SubmitBatchIo(io_requests, io_ops, results, write);
}

void DistributedStorageBackend::SubmitBatchIo(
    const std::vector<FdIoRequest>& io_requests,
    const std::vector<DfsIoOp*>& io_ops,
    std::vector<tl::expected<void, ErrorCode>>& results, bool write) {
    if (io_requests.empty()) return;
    const char* op_name = write ? "write" : "read";
    auto io_results = write ? fs_adapter_->BatchWriteAt(io_requests)
                            : fs_adapter_->BatchReadAt(io_requests);
    if (io_results.size() != io_requests.size()) {
        LOG(ERROR) << "Filesystem adapter returned an invalid batch " << op_name
                   << " result count";
        for (auto* op : io_ops) {
            results[op->index] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return;
    }
    for (size_t i = 0; i < io_results.size(); ++i) {
        auto& op = *io_ops[i];
        const auto& result = io_results[i];
        if (!result) {
            LOG(WARNING) << "DFS " << op_name << " failed for key " << op.key
                         << ", error=" << result.error();
            results[op.index] = tl::make_unexpected(result.error());
        } else if (*result == op.size) {
            continue;
        } else {
            LOG(WARNING) << "DFS short " << op_name << " for key " << op.key
                         << ", expected=" << op.size << ", actual=" << *result;
            results[op.index] = tl::make_unexpected(
                write ? ErrorCode::FILE_WRITE_FAIL : ErrorCode::FILE_READ_FAIL);
        }
    }
}

std::vector<tl::expected<void, ErrorCode>>
DistributedStorageBackend::BatchWrite(
    const std::vector<DfsWriteRequest>& requests) {
    return BatchWriteBlocking(requests);
}

std::vector<tl::expected<void, ErrorCode>>
DistributedStorageBackend::BatchWriteBlocking(
    const std::vector<DfsWriteRequest>& requests) {
    std::vector<tl::expected<void, ErrorCode>> results(requests.size());

    if (!initialized_.load(std::memory_order_acquire)) {
        return std::vector<tl::expected<void, ErrorCode>>(
            requests.size(), tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
    }
    if (UsesObjectStorage()) {
        results.assign(requests.size(), tl::expected<void, ErrorCode>{});
        std::vector<ObjectPutRequest> object_requests;
        std::vector<std::vector<iovec>> iov_storage;
        std::vector<size_t> request_indices;
        object_requests.reserve(requests.size());
        iov_storage.reserve(requests.size());
        request_indices.reserve(requests.size());
        for (size_t index = 0; index < requests.size(); ++index) {
            const auto& request = requests[index];
            const auto& desc = request.descriptor;
            auto total_size = CalculateSliceBytes(request.slices);
            if (request.slices.empty() || !total_size ||
                request.slices.size() >
                    static_cast<size_t>(std::numeric_limits<int>::max()) ||
                desc.object_size == 0 || *total_size != desc.object_size) {
                results[index] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                continue;
            }
            auto& iovs = iov_storage.emplace_back();
            iovs.reserve(request.slices.size());
            for (const auto& slice : request.slices)
                iovs.push_back({slice.ptr, slice.size});
            object_requests.push_back(
                {request.key, iovs.data(), static_cast<int>(iovs.size())});
            request_indices.push_back(index);
        }

        auto object_results =
            object_storage_adapter_->PutBatch(object_requests);
        if (object_results.size() != object_requests.size()) {
            for (size_t index : request_indices) {
                results[index] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            return results;
        }
        for (size_t index = 0; index < object_results.size(); ++index) {
            results[request_indices[index]] = std::move(object_results[index]);
        }
        return results;
    }

    if (!initialized_.load(std::memory_order_acquire)) {
        results.assign(requests.size(),
                       tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
        return results;
    }
    auto initialized = ValidateRootIdentity();
    if (!initialized) {
        LOG(ERROR) << "DistributedStorageBackend is unavailable: "
                   << initialized.error();
        results.assign(requests.size(),
                       tl::make_unexpected(initialized.error()));
        return results;
    }

    // Build validated entry payloads first, including the reserved zero
    // padding, then issue one write per contiguous bounded-size run. SHARD
    // keeps the original one-request path because its allocator does not
    // promise contiguity.
    if (IsBucketMode()) {
        if (requests.empty()) return results;
        const auto epoch = requests.front().descriptor.MasterEpoch();
        if (std::any_of(requests.begin(), requests.end(),
                        [&](const auto& request) {
                            return request.descriptor.MasterEpoch() != epoch;
                        })) {
            results.assign(requests.size(),
                           tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            return results;
        }
        auto guard = AcquireBucketEpoch(epoch);
        if (!guard) {
            results.assign(requests.size(), tl::make_unexpected(guard.error()));
            return results;
        }
        return BatchWriteBucket(requests);
    }
    return BatchWriteShard(requests);
}

std::vector<tl::expected<void, ErrorCode>>
DistributedStorageBackend::BatchWriteShard(
    const std::vector<DfsWriteRequest>& requests) {
    std::vector<tl::expected<void, ErrorCode>> results(requests.size());

    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        results.assign(requests.size(),
                       tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
        return results;
    }

    // Shard I/O reuses cached fds and fails short transfers.
    // Execution mode: adapters that opt in (currently 3FS) collect validated
    // writes for submission after the loop. Other adapters (including POSIX)
    // execute each write in the loop, preserving shard I/O locking.
    const bool batch_io = fs_adapter_->SupportsBatchIo();
    std::vector<DfsIoOp> ops;
    if (batch_io) ops.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        const auto& desc = request.descriptor;
        const bool range_valid =
            IsDfsDescriptorRangeValid(desc, distributed_config_);
        if (!range_valid) {
            LOG(ERROR) << "Invalid DFS descriptor range for key " << request.key
                       << ", offset=" << desc.offset
                       << ", object_size=" << desc.object_size
                       << ", aligned_size=" << desc.aligned_size
                       << ", capacity="
                       << distributed_config_.shard_capacity;
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        if (request.slices.size() >
            static_cast<size_t>(std::numeric_limits<int>::max())) {
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        std::vector<iovec> iovs;
        iovs.reserve(request.slices.size());
        uint64_t total_size = 0;
        bool invalid = false;
        for (const auto& slice : request.slices) {
            if ((!slice.ptr && slice.size > 0) ||
                slice.size >
                    std::numeric_limits<uint64_t>::max() - total_size) {
                invalid = true;
                break;
            }
            total_size += slice.size;
            iovs.push_back({slice.ptr, slice.size});
        }
        if (invalid || total_size != desc.object_size) {
            LOG(WARNING) << "Invalid DFS write request for key " << request.key
                         << ", expected=" << desc.object_size
                         << ", actual=" << total_size;
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        DfsIoOp op{i,          request.key,
                   desc,       std::move(iovs),
                   total_size, static_cast<int64_t>(desc.offset)};
        if (batch_io) {
            ops.push_back(std::move(op));
        } else {
            results[i] = ExecuteSingleIo(op, true);
        }
    }

    if (!batch_io) return results;
    ExecuteShardBatchIo(ops, results, true);
    return results;
}

std::vector<tl::expected<void, ErrorCode>> DistributedStorageBackend::BatchReadShard(
    const std::vector<DfsReadRequest>& requests) {
    std::vector<tl::expected<void, ErrorCode>> results(requests.size());

    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        results.assign(requests.size(),
                       tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
        return results;
    }

    // Shard I/O reuses cached fds and fails short transfers.
    // Execution mode: adapters that opt in (currently 3FS) collect validated
    // reads for submission after the loop. Other adapters (including POSIX)
    // execute each read in the loop, preserving shard I/O locking.
    const bool batch_io = fs_adapter_->SupportsBatchIo();
    std::vector<DfsIoOp> ops;
    if (batch_io) ops.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        const auto& desc = request.descriptor;
        const bool range_valid =
            IsDfsDescriptorRangeValid(desc, distributed_config_);
        if (!range_valid) {
            LOG(ERROR) << "Invalid DFS descriptor range for key " << request.key
                       << ", offset=" << desc.offset
                       << ", object_size=" << desc.object_size
                       << ", aligned_size=" << desc.aligned_size
                       << ", capacity="
                       << distributed_config_.shard_capacity;
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        if (desc.object_size > std::numeric_limits<size_t>::max() ||
            request.slices.size() >
                static_cast<size_t>(std::numeric_limits<int>::max())) {
            LOG(ERROR) << "DFS read request exceeds platform limits for key "
                       << request.key << ", object_size=" << desc.object_size
                       << ", slice_count=" << request.slices.size();
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        std::vector<iovec> iovs;
        iovs.reserve(request.slices.size());
        bool invalid = false;
        uint64_t io_bytes = desc.object_size;
        uint64_t value_offset = 0;
        if (request.range_read) {
            value_offset = request.value_offset;
            if (value_offset > desc.object_size) {
                invalid = true;
            }
            size_t remaining =
                invalid ? 0
                        : static_cast<size_t>(desc.object_size - value_offset);
            size_t window_bytes = 0;
            for (const auto& slice : request.slices) {
                if (invalid) break;
                if ((!slice.ptr && slice.size > 0) || slice.size > remaining) {
                    invalid = true;
                    break;
                }
                if (slice.size == 0) continue;
                iovs.push_back({slice.ptr, slice.size});
                window_bytes += slice.size;
                remaining -= slice.size;
            }
            io_bytes = window_bytes;
        } else {
            size_t remaining = static_cast<size_t>(desc.object_size);
            for (const auto& slice : request.slices) {
                if (!slice.ptr && slice.size > 0) {
                    invalid = true;
                    break;
                }
                if (remaining == 0 || slice.size == 0) {
                    continue;
                }
                const size_t read_size = std::min(slice.size, remaining);
                iovs.push_back({slice.ptr, read_size});
                remaining -= read_size;
            }
            invalid = invalid || remaining != 0;
        }
        if (invalid ||
            desc.offset > std::numeric_limits<uint64_t>::max() - value_offset ||
            desc.offset + value_offset >
                static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            LOG(WARNING) << "Invalid DFS read request for key " << request.key
                         << ", expected capacity at least=" << desc.object_size
                         << ", value_offset=" << value_offset;
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        // A window can sit entirely inside a truncated object. Reading only
        // that window would report success. Reject the read when the file no
        // longer holds the whole value, without reading the tail.
        if (request.range_read) {
            auto file_size = fs_adapter_->GetFileSize(desc.file_path);
            if (!file_size) {
                results[i] = tl::make_unexpected(file_size.error());
                continue;
            }
            const uint64_t object_end = desc.offset + desc.object_size;
            if (static_cast<uint64_t>(*file_size) < object_end) {
                LOG(WARNING)
                    << "DFS range read object extends past end of file, key="
                    << request.key << ", file_size=" << *file_size
                    << ", object_end=" << object_end;
                results[i] = tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                continue;
            }
        }
        DfsIoOp op{i,        request.key,
                   desc,     std::move(iovs),
                   io_bytes, static_cast<int64_t>(desc.offset + value_offset)};
        if (batch_io) {
            ops.push_back(std::move(op));
        } else {
            results[i] = ExecuteSingleIo(op, false);
        }
    }

    if (!batch_io) return results;
    ExecuteShardBatchIo(ops, results, false);
    return results;
}

std::vector<tl::expected<void, ErrorCode>>
DistributedStorageBackend::BatchReadBlocking(
    const std::vector<DfsReadRequest>& requests) {
    const auto timing_start = std::chrono::steady_clock::now();
    std::vector<tl::expected<void, ErrorCode>> results(
        requests.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    if (!initialized_.load(std::memory_order_acquire)) {
        return std::vector<tl::expected<void, ErrorCode>>(
            requests.size(), tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
    }
    if (UsesObjectStorage()) {
        results.assign(requests.size(), tl::expected<void, ErrorCode>{});
        std::vector<ObjectGetRequest> object_requests;
        std::vector<std::vector<char>> staging_buffers;
        std::vector<size_t> staging_request_positions;
        std::vector<size_t> request_indices;
        object_requests.reserve(requests.size());
        staging_buffers.reserve(requests.size());
        staging_request_positions.reserve(requests.size());
        request_indices.reserve(requests.size());
        for (size_t index = 0; index < requests.size(); ++index) {
            const auto& request = requests[index];
            const auto& desc = request.descriptor;
            auto capacity = CalculateSliceBytes(request.slices);
            if (request.slices.empty() || !capacity ||
                desc.object_size > std::numeric_limits<size_t>::max() ||
                desc.object_size == 0 || *capacity < desc.object_size) {
                results[index] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                continue;
            }
            void* buffer = request.slices.front().ptr;
            size_t staging_position = std::numeric_limits<size_t>::max();
            if (request.slices.size() > 1) {
                staging_position = staging_buffers.size();
                staging_buffers.emplace_back(
                    static_cast<size_t>(desc.object_size));
                buffer = staging_buffers.back().data();
            }
            object_requests.push_back(
                {request.key, buffer, static_cast<size_t>(desc.object_size)});
            staging_request_positions.push_back(staging_position);
            request_indices.push_back(index);
        }

        auto object_results =
            object_storage_adapter_->GetBatch(object_requests);
        if (object_results.size() != object_requests.size()) {
            for (size_t index : request_indices) {
                results[index] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            return results;
        }
        for (size_t i = 0; i < object_results.size(); ++i) {
            const size_t request_index = request_indices[i];
            if (!object_results[i]) {
                results[request_index] =
                    tl::make_unexpected(object_results[i].error());
                continue;
            }
            if (*object_results[i] != object_requests[i].size) {
                results[request_index] =
                    tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                continue;
            }
            const size_t staging_position = staging_request_positions[i];
            if (staging_position != std::numeric_limits<size_t>::max()) {
                const auto& buffer = staging_buffers[staging_position];
                size_t offset = 0;
                for (const auto& slice : requests[request_index].slices) {
                    if (offset == buffer.size()) break;
                    const size_t length =
                        std::min(slice.size, buffer.size() - offset);
                    std::memcpy(slice.ptr, buffer.data() + offset, length);
                    offset += length;
                }
            }
            results[request_index] = {};
        }
        return results;
    }

    if (!initialized_.load(std::memory_order_acquire)) {
        std::fill(results.begin(), results.end(),
                  tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
        return results;
    }
    auto initialized = ValidateRootIdentity();
    if (!initialized) {
        LOG(ERROR) << "DistributedStorageBackend is unavailable: "
                   << initialized.error();
        std::fill(results.begin(), results.end(),
                  tl::make_unexpected(initialized.error()));
        return results;
    }

    if (!IsBucketMode()) return BatchReadShard(requests);

    if (requests.empty()) return results;
    const auto epoch = requests.front().descriptor.MasterEpoch();
    if (std::any_of(requests.begin(), requests.end(), [&](const auto& request) {
            return request.descriptor.MasterEpoch() != epoch;
        })) {
        // The client can refresh this mixed batch once, without changing local
        // epoch state based on whichever request happened to come first.
        std::fill(results.begin(), results.end(),
                  tl::make_unexpected(ErrorCode::DFS_STALE_HANDLE));
        return results;
    }
    auto guard = AcquireBucketEpoch(epoch);
    if (!guard) {
        std::fill(results.begin(), results.end(),
                  tl::make_unexpected(guard.error()));
        return results;
    }

    auto tasks = PrepareReadTasks(requests, results);
    const auto prepare_done = std::chrono::steady_clock::now();
    ExecuteReadTasks(tasks, requests, results);

    if (dfs_read_trace_enabled()) {
        const int64_t prepare_us =
            std::chrono::duration_cast<std::chrono::microseconds>(prepare_done - timing_start)
                .count();
        const int64_t read_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - prepare_done)
                .count();
        uint64_t total_bytes = 0;
        for (const auto& request : requests) {
            total_bytes += request.descriptor.object_size;
        }
        double gbps = 0.0;
        if (read_us > 0) {
            // bytes / us == MB/s; /1000 -> GB/s.
            gbps = static_cast<double>(total_bytes) / read_us / 1000.0;
        }
        LOG(INFO) << "BatchRead: requests=" << requests.size()
                  << " tasks=" << tasks.size() << " bytes=" << total_bytes
                  << " prepare_us=" << prepare_us
                  << " read_us=" << read_us
                  << " bandwidth_GBps=" << gbps;
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> DistributedStorageBackend::BatchRead(
    const std::vector<DfsReadRequest>& requests) {
    if (UsesObjectStorage()) return BatchReadBlocking(requests);
    if (requests.empty()) return {};
    const auto request_start = std::chrono::steady_clock::now();
    uint64_t total_bytes = 0;
    uint64_t inflight_bytes = 0;
    bool all_buffers_owned = true;
    for (const auto& request : requests) {
        if (!request.range_read) {
            SaturatingAddBytes(request.descriptor.object_size, &total_bytes);
        }
        for (const auto& slice : request.slices) {
            SaturatingAddBytes(slice.size, &inflight_bytes);
            if (request.range_read) SaturatingAddBytes(slice.size, &total_bytes);
            if (slice.size != 0 && !request.buffer_owner) {
                all_buffers_owned = false;
            }
        }
    }
    if (dfs_read_timed_out_.load(std::memory_order_acquire)) {
        LOG_EVERY_N(WARNING, 64)
            << "DFS BatchGet rejected by timeout latch: "
            << "reason=previous_read_timeout_latched, requests="
            << requests.size() << ", total_bytes=" << total_bytes
            << ", inflight_bytes=" << inflight_bytes
            << ", configured_timeout_ms="
            << distributed_config_.batch_get_timeout_ms
            << ", max_inflight_batches="
            << distributed_config_.batch_get_max_inflight_batches
            << ", max_inflight_bytes="
            << distributed_config_.batch_get_max_inflight_bytes
            << ", error=" << ErrorCode::DFS_BATCH_GET_TIMEOUT;
        return std::vector<tl::expected<void, ErrorCode>>(
            requests.size(),
            tl::make_unexpected(ErrorCode::DFS_BATCH_GET_TIMEOUT));
    }
    const auto base_timeout_ms = distributed_config_.batch_get_timeout_ms;
    const auto timeout_ms = DfsBatchGetTimeoutMs(
        base_timeout_ms, total_bytes,
        batch_timeout_fixed_ms_.load(std::memory_order_acquire));
    const auto admission_deadline =
        base_timeout_ms == 0
            ? std::chrono::steady_clock::time_point::max()
            : DfsIoDeadlineFromNow(timeout_ms);
    const ErrorCode admission_result =
        AcquireInflightReadUntil(inflight_bytes, admission_deadline);
    if (admission_result != ErrorCode::OK) {
        LOG_EVERY_N(WARNING, 64)
            << "DFS BatchGet admission rejected: reason="
            << (admission_result == ErrorCode::DFS_BATCH_GET_TIMEOUT
                    ? "dfs_read_timeout_latched"
                    : "inflight_capacity_deadline_expired")
            << ", request_bytes=" << inflight_bytes << ", max_batches="
            << distributed_config_.batch_get_max_inflight_batches
            << ", max_bytes="
            << distributed_config_.batch_get_max_inflight_bytes
            << ", error=" << admission_result;
        return std::vector<tl::expected<void, ErrorCode>>(
            requests.size(), tl::make_unexpected(admission_result));
    }

    if (base_timeout_ms == 0 || !all_buffers_owned) {
        if (base_timeout_ms != 0 && !all_buffers_owned) {
            LOG_EVERY_N(WARNING, 64)
                << "DFS BatchGet destination has no lifetime owner; using "
                   "blocking I/O to prevent buffer reuse while preadv is "
                   "still running";
        }
        try {
            auto results = BatchReadBlocking(requests);
            ReleaseInflightRead(inflight_bytes);
            return results;
        } catch (...) {
            ReleaseInflightRead(inflight_bytes);
            throw;
        }
    }
    // Admission waiting and DFS I/O have different failure semantics. Start
    // the I/O deadline only after capacity has been acquired so queueing behind
    // healthy reads can never set the backend-wide DFS timeout latch.
    const auto deadline = DfsIoDeadlineFromNow(timeout_ms);
    struct TimedReadContext {
        std::mutex mutex;
        std::condition_variable cv;
        bool done = false;
        std::chrono::steady_clock::time_point completed_at;
        std::vector<DfsReadRequest> requests;
        std::vector<tl::expected<void, ErrorCode>> results;
        std::function<void()> release_inflight;

        ~TimedReadContext() {
            if (release_inflight) release_inflight();
        }
    };
    std::shared_ptr<TimedReadContext> context;
    try {
        context = std::make_shared<TimedReadContext>();
        // Preserve the request metadata for the detached worker, but read
        // directly into the caller-provided slices. This keeps aligned pinned
        // buffers on the O_DIRECT zero-copy path and avoids a private buffer
        // allocation plus a completion-time memcpy for every object. Production
        // callers attach the staging allocation through buffer_owner, and this
        // copied request quarantines it until the underlying read really ends.
        context->requests = requests;
    } catch (const std::bad_alloc&) {
        ReleaseInflightRead(inflight_bytes);
        return std::vector<tl::expected<void, ErrorCode>>(
            requests.size(),
            tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE));
    }

    std::shared_ptr<DistributedStorageBackend> self;
    try {
        self = shared_from_this();
    } catch (const std::bad_weak_ptr&) {
        // Test-only/legacy stack ownership cannot safely outlive this call.
        // Keep the old blocking lifetime contract in that configuration.
        try {
            auto results = BatchReadBlocking(requests);
            ReleaseInflightRead(inflight_bytes);
            return results;
        } catch (...) {
            ReleaseInflightRead(inflight_bytes);
            throw;
        }
    }
    // Keep the backend alive through quota release too; relying on the
    // destruction order of separate thread-lambda captures would leave a
    // narrow use-after-free window during shutdown.
    try {
        context->release_inflight = [owner = self, inflight_bytes]() {
            owner->ReleaseInflightRead(inflight_bytes);
        };
    } catch (const std::bad_alloc&) {
        ReleaseInflightRead(inflight_bytes);
        return std::vector<tl::expected<void, ErrorCode>>(
            requests.size(),
            tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE));
    }

    if (std::chrono::steady_clock::now() >= deadline) {
        SetDfsReadTimedOut(true);
        const auto elapsed_ms = std::chrono::duration_cast<
            std::chrono::milliseconds>(std::chrono::steady_clock::now() -
                                       request_start)
                                     .count();
        LOG_EVERY_N(WARNING, 64)
            << "DFS BatchGet timed out before worker launch: "
            << "reason=deadline_expired_after_admission, requests="
            << requests.size() << ", total_bytes=" << total_bytes
            << ", inflight_bytes=" << inflight_bytes
            << ", timeout_ms=" << timeout_ms
            << ", elapsed_ms=" << elapsed_ms
            << ", max_inflight_batches="
            << distributed_config_.batch_get_max_inflight_batches
            << ", max_inflight_bytes="
            << distributed_config_.batch_get_max_inflight_bytes
            << ", error=" << ErrorCode::DFS_BATCH_GET_TIMEOUT;
        return std::vector<tl::expected<void, ErrorCode>>(
            requests.size(),
            tl::make_unexpected(ErrorCode::DFS_BATCH_GET_TIMEOUT));
    }
    try {
        std::thread([self = std::move(self), context]() {
            std::vector<tl::expected<void, ErrorCode>> results;
            try {
                results = self->BatchReadBlocking(context->requests);
            } catch (const std::bad_alloc&) {
                results.assign(
                    context->requests.size(),
                    tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE));
            } catch (const std::exception& e) {
                LOG(ERROR) << "Timed DFS BatchGet worker failed: " << e.what();
                results.assign(
                    context->requests.size(),
                    tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
            } catch (...) {
                LOG(ERROR) << "Timed DFS BatchGet worker failed";
                results.assign(
                    context->requests.size(),
                    tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
            }
            {
                std::lock_guard lock(context->mutex);
                context->results = std::move(results);
                context->completed_at = std::chrono::steady_clock::now();
                context->done = true;
            }
            context->cv.notify_all();
        }).detach();
    } catch (const std::system_error&) {
        return std::vector<tl::expected<void, ErrorCode>>(
            requests.size(),
            tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE));
    }

    std::unique_lock lock(context->mutex);
    const bool completed_before_deadline = context->cv.wait_until(
        lock, deadline, [&context] { return context->done; });
    const auto finished_at = std::chrono::steady_clock::now();
    if (!completed_before_deadline || context->completed_at > deadline) {
        SetDfsReadTimedOut(true);
        const auto elapsed_ms = std::chrono::duration_cast<
            std::chrono::milliseconds>(finished_at - request_start)
                                     .count();
        const auto worker_elapsed_ms =
            context->done
                ? std::chrono::duration_cast<std::chrono::milliseconds>(
                      context->completed_at - request_start)
                      .count()
                : -1;
        LOG_EVERY_N(WARNING, 64)
            << "DFS BatchGet I/O timed out: "
            << "reason="
            << (completed_before_deadline ? "worker_completed_after_deadline"
                                          : "worker_not_completed_by_deadline")
            << ", requests=" << requests.size()
            << ", total_bytes=" << total_bytes
            << ", inflight_bytes=" << inflight_bytes
            << ", timeout_ms=" << timeout_ms
            << ", elapsed_ms=" << elapsed_ms
            << ", worker_elapsed_ms=" << worker_elapsed_ms
            << ", max_inflight_batches="
            << distributed_config_.batch_get_max_inflight_batches
            << ", max_inflight_bytes="
            << distributed_config_.batch_get_max_inflight_bytes
            << ", error=" << ErrorCode::DFS_BATCH_GET_TIMEOUT;
        return std::vector<tl::expected<void, ErrorCode>>(
            requests.size(),
            tl::make_unexpected(ErrorCode::DFS_BATCH_GET_TIMEOUT));
    }
    return context->results;
}

tl::expected<void, ErrorCode> DistributedStorageBackend::BatchLoad(
    std::unordered_map<std::string, Slice>& batched_slices) {
    if (!UsesObjectStorage()) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    std::vector<ObjectGetRequest> requests;
    requests.reserve(batched_slices.size());
    for (auto& [key, slice] : batched_slices) {
        requests.push_back({key, slice.ptr, slice.size});
    }
    auto results = object_storage_adapter_->GetBatch(requests);
    if (results.size() != requests.size()) {
        LOG(ERROR)
            << "Object storage returned an invalid GET batch result count";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i]) {
            return tl::make_unexpected(results[i].error());
        }
        if (*results[i] != requests[i].size) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
    }
    return {};
}

tl::expected<bool, ErrorCode> DistributedStorageBackend::IsExist(
    const std::string& key) {
    if (!UsesObjectStorage()) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return object_storage_adapter_->Exists(key);
}

tl::expected<bool, ErrorCode> DistributedStorageBackend::IsEnableOffloading() {
    return UsesObjectStorage();
}

tl::expected<void, ErrorCode> DistributedStorageBackend::ScanMeta(
    const std::function<
        ErrorCode(const std::vector<std::string>& keys,
                  std::vector<StorageObjectMetadata>& metadatas)>& handler) {
    if (!UsesObjectStorage()) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    std::vector<std::string> batch_keys;
    std::vector<StorageObjectMetadata> batch_metas;
    const size_t batch_limit = static_cast<size_t>(std::max<int64_t>(
        1, file_storage_config_.scanmeta_iterator_keys_limit));

    auto key_infos = object_storage_adapter_->ListKeys();
    if (!key_infos) {
        LOG(ERROR) << "Failed to list keys from object storage adapter: "
                   << static_cast<int>(key_infos.error());
        return tl::make_unexpected(key_infos.error());
    }

    for (const auto& info : *key_infos) {
        batch_keys.push_back(info.logical_key);
        batch_metas.emplace_back(-1, 0,
                                 static_cast<int64_t>(info.logical_key.size()),
                                 static_cast<int64_t>(info.size), "");
        if (batch_keys.size() >= batch_limit) {
            auto err = handler(batch_keys, batch_metas);
            if (err != ErrorCode::OK) return tl::make_unexpected(err);
            batch_keys.clear();
            batch_metas.clear();
        }
    }
    if (!batch_keys.empty()) {
        auto err = handler(batch_keys, batch_metas);
        if (err != ErrorCode::OK) return tl::make_unexpected(err);
    }
    return {};
}

tl::expected<DistributedStorageBackend::ShardFile*, ErrorCode>
DistributedStorageBackend::GetOrOpenShard(
    const DistributedFSDescriptor& descriptor, bool write) {
    if (descriptor.shard_idx < 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    ShardFile* shard = nullptr;
    {
        std::lock_guard cache_lock(shard_files_mutex_);
        auto existing = shard_files_.find(descriptor.shard_idx);
        if (existing != shard_files_.end()) shard = existing->second.get();
    }
    if (shard) {
        std::lock_guard shard_lock(shard->mutex);
        // Only the exact path validated when this fd was opened can bypass
        // normalization. Aliases and changed descriptors take the full
        // validation path below. Cache entries live until backend teardown.
        if ((write ? shard->write_fd.load() : shard->read_fd) >= 0 &&
            shard->path == descriptor.file_path) {
            return shard;
        }
    }

    const auto path =
        std::filesystem::path(descriptor.file_path).lexically_normal();
    auto root = std::filesystem::path(root_dir_).lexically_normal();
    if (root.filename().empty()) root = root.parent_path();
    auto parent = path.parent_path();
    if (parent.empty()) parent = ".";
    const auto index = ParseShardFileName(path.filename().string());
    if (!index || *index != descriptor.shard_idx || parent != root) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (!shard) {
        std::lock_guard cache_lock(shard_files_mutex_);
        auto existing = shard_files_.find(descriptor.shard_idx);
        if (existing == shard_files_.end()) {
            existing = shard_files_
                           .emplace(descriptor.shard_idx,
                                    std::make_unique<ShardFile>())
                           .first;
        }
        shard = existing->second.get();
    }

    // File validation and opening may block. Serialize them only with this
    // shard's initialization and non-batched I/O, leaving other cached shards
    // available.
    std::lock_guard shard_lock(shard->mutex);
    auto file_path = path.string();
    if (!shard->path.empty() && shard->path != file_path) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if ((write ? shard->write_fd.load() : shard->read_fd) >= 0) {
        if (shard->path != file_path) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        return shard;
    }

    auto fd = ValidateAndOpenShardFile(file_path, !write);
    if (!fd) return tl::make_unexpected(fd.error());
    // Failed attempts keep fd == -1 so a later request can retry
    // initialization.
    shard->path = std::move(file_path);
    if (write) {
        shard->write_fd.store(*fd);
        if (shard->read_fd < 0) shard->read_fd = *fd;
    } else {
        shard->read_fd = *fd;
    }
    return shard;
}

tl::expected<int, ErrorCode>
DistributedStorageBackend::ValidateAndOpenShardFile(
    const std::string& file_path, bool read_only) {
    // The master prepares new shards before publishing their descriptors.
    // Validate capacity before opening through a non-creating entry point.
    auto file_size = fs_adapter_->GetFileSize(file_path);
    if (!file_size) return tl::make_unexpected(file_size.error());
    if (*file_size != distributed_config_.shard_capacity) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto root = std::filesystem::path(root_dir_).lexically_normal();
    if (root.filename().empty()) root = root.parent_path();
    const auto path = std::filesystem::path(file_path);
    std::error_code ec;
    const auto canonical_root = std::filesystem::canonical(root, ec);
    if (ec) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    const auto canonical_path = std::filesystem::canonical(path, ec);
    if (ec) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    if (canonical_path.parent_path() != canonical_root ||
        canonical_path.filename() != path.filename()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return read_only ? fs_adapter_->OpenFileReadOnly(file_path)
                     : fs_adapter_->OpenExistingFile(file_path);
}

tl::expected<void, ErrorCode> DistributedStorageBackend::ReopenShardLocked(
    ShardFile& shard, bool write) {
    const int fd = write ? shard.write_fd.load() : shard.read_fd;
    if (fd >= 0) {
        (void)fs_adapter_->CloseFile(fd);
        if (shard.read_fd == fd) shard.read_fd = -1;
        if (shard.write_fd.load() == fd) shard.write_fd.store(-1);
    }
    auto reopened = ValidateAndOpenShardFile(shard.path, !write);
    if (!reopened) return tl::make_unexpected(reopened.error());
    if (write) {
        shard.write_fd.store(*reopened);
        if (shard.read_fd < 0) shard.read_fd = *reopened;
    } else {
        shard.read_fd = *reopened;
    }
    return {};
}

ObjectStorageIoResults DistributedStorageBackend::BatchDeleteObjects(
    std::span<const std::string> logical_keys) {
    if (!initialized_ || !UsesObjectStorage() || !object_storage_adapter_) {
        return ObjectStorageIoResults(
            logical_keys.size(),
            tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
    }
    return object_storage_adapter_->DeleteBatch(logical_keys);
}

}  // namespace mooncake
