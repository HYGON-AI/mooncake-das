/**
 * Private DFS data-plane extension.
 *
 * Owns the read/write merge algorithm, the batch-boundary policy, and the
 * storage write-handle contract. The public tree only knows the C ABI in
 * ext/mooncake_private_ext_abi.h and must keep working without this library.
 */

#include "ext/mooncake_private_ext_abi.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <condition_variable>
#include <cstdint>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include "storage_ioctl_private.h"

namespace {

// Bound one merged run so a single syscall round trip never pins a large
// temporary buffer or a long latency.
constexpr uint64_t kMaxMergedRun = 4ULL * 1024 * 1024;
// Upper bound on iovecs per syscall: the kernel caps IOV_MAX at 1024.
constexpr size_t kMaxIovChunk = 1024;
// Batch boundary applied inside a bucket. Kept private together with the
// rollover policy that depends on it.
constexpr uint64_t kBatchAlignment = 512ULL * 1024;
// Smallest alignment the direct-I/O bounce path relies on.
constexpr size_t kDirectAlignment = 4096;

// Unified DFS buffer admission policy. Kept private: the protection depth, the
// prefetch cap and the high-usage watermark are the tuning this extension
// exists to own.
constexpr uint64_t kReadProtectionChunks = 4;
constexpr uint64_t kWriteProtectionChunks = 1;
constexpr double kPrefetchCapacityRatio = 0.5;
constexpr uint32_t kHighWatermarkNumerator = 8;
constexpr uint32_t kHighWatermarkDenominator = 10;

uint64_t ReadPositiveEnv(const char* name, uint64_t fallback) {
    const char* raw = std::getenv(name);
    if (raw == nullptr || *raw == '\0') return fallback;
    errno = 0;
    char* end = nullptr;
    const long long value = std::strtoll(raw, &end, 10);
    if (errno != 0 || end == raw || *end != '\0' || value <= 0) return fallback;
    return static_cast<uint64_t>(value);
}

// Boolean parser with the same rules as mooncake-common/bool_parser.h
// TryParseBool: ASCII case-insensitive, trimmed, and it accepts 1/0,
// true/false, yes/no, on/off and enable/disable. Anything else keeps the
// default. Kept local because the private provider only links against the ABI
// header and must not depend on the public build tree.
std::optional<bool> ParseBool(std::string_view value) {
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.front()))) {
        value.remove_prefix(1);
    }
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.back()))) {
        value.remove_suffix(1);
    }
    if (value.empty()) return std::nullopt;
    const auto equals_ci = [value](std::string_view token) {
        if (token.size() != value.size()) return false;
        for (size_t i = 0; i < token.size(); ++i) {
            if (std::tolower(static_cast<unsigned char>(value[i])) !=
                std::tolower(static_cast<unsigned char>(token[i]))) {
                return false;
            }
        }
        return true;
    };
    if (equals_ci("1") || equals_ci("true") || equals_ci("yes") ||
        equals_ci("on") || equals_ci("enable")) {
        return true;
    }
    if (equals_ci("0") || equals_ci("false") || equals_ci("no") ||
        equals_ci("off") || equals_ci("disable")) {
        return false;
    }
    return std::nullopt;
}

bool EnvFlag(const char* name, bool fallback) {
    const char* raw = std::getenv(name);
    if (raw == nullptr) return fallback;
    const auto parsed = ParseBool(raw);
    return parsed.has_value() ? *parsed : fallback;
}

double ReadRatioEnv(const char* name, double fallback) {
    const char* raw = std::getenv(name);
    if (raw == nullptr || *raw == '\0') return fallback;
    errno = 0;
    char* end = nullptr;
    const double value = std::strtod(raw, &end);
    if (errno != 0 || end == raw || *end != '\0' || !std::isfinite(value) ||
        value < 0.0 || value > 1.0) {
        return fallback;
    }
    return value;
}

uint64_t ScaleBytes(uint64_t bytes, uint64_t count) {
    if (bytes == 0 || count == 0) return 0;
    return bytes > UINT64_MAX / count ? UINT64_MAX : bytes * count;
}

uint64_t BatchAlignment() {
    static const uint64_t alignment = [] {
        const char* raw = std::getenv("MOONCAKE_DFS_BUCKET_BATCH_ALIGNMENT");
        if (raw == nullptr || *raw == '\0') return kBatchAlignment;
        errno = 0;
        char* end = nullptr;
        // Parse as signed: strtoull would turn "-1" into 2^64-1 and silently
        // disable the boundary instead of rejecting it.
        const long long value = std::strtoll(raw, &end, 10);
        if (errno != 0 || end == raw || *end != '\0' || value < 0) {
            return kBatchAlignment;
        }
        // Zero keeps the baseline per-key layout.
        return static_cast<uint64_t>(value);
    }();
    return alignment;
}

bool MergingAllowed() {
    static const bool allowed = [] {
        // Pre-change default is disabled: merging only turns on with an
        // explicit enabled value (1/true/on).
        return EnvFlag("MOONCAKE_DFS_BATCH_READ_MERGE_ENABLED", false);
    }();
    return allowed;
}

size_t BatchReadThreads() {
    static const size_t threads = [] {
        // Pre-change default is a fixed 128 threads (validated to [1, 256]).
        const size_t fallback = 128;
        const char* raw = std::getenv("MOONCAKE_DFS_BATCH_READ_THREADS");
        if (raw == nullptr || raw[0] == '\0') return fallback;
        errno = 0;
        char* end = nullptr;
        const long long value = std::strtoll(raw, &end, 10);
        if (errno != 0 || end == raw || *end != '\0' || value < 1) {
            return fallback;
        }
        return std::min<size_t>(256, static_cast<size_t>(value));
    }();
    return threads;
}

struct BatchState {
    std::mutex mutex;
    std::condition_variable cv;
    size_t remaining = 0;
};

class WorkerPool {
   public:
    static WorkerPool& Instance() {
        static WorkerPool* pool = new WorkerPool();
        return *pool;
    }

    // Runs every job and blocks until all of them finished.
    void Run(std::vector<std::function<void()>> jobs) {
        if (jobs.empty()) return;
        if (!EnsureStarted()) {
            // No worker could be started: run inline so the caller still
            // makes progress instead of waiting forever.
            for (auto& job : jobs) {
                try {
                    job();
                } catch (...) {
                }
            }
            return;
        }
        auto state = std::make_shared<BatchState>();
        state->remaining = jobs.size();
        size_t queued = 0;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (; queued < jobs.size(); ++queued) {
                try {
                    std::function<void()> wrapper =
                        [job = jobs[queued], state]() {
                            try {
                                job();
                            } catch (...) {
                                // Never let a job escape: a thrown exception
                                // must still settle the batch, otherwise Run
                                // would wait forever.
                            }
                            std::lock_guard<std::mutex> done(state->mutex);
                            if (--state->remaining == 0) state->cv.notify_all();
                        };
                    queue_.push_back(std::move(wrapper));
                } catch (...) {
                    // Allocation failed while enqueuing. Keep the jobs that
                    // are already queued and run the rest on this thread, so
                    // no job ever outlives the caller's stack.
                    break;
                }
            }
        }
        if (queued < jobs.size()) {
            for (size_t i = queued; i < jobs.size(); ++i) {
                try {
                    jobs[i]();
                } catch (...) {
                }
            }
            std::lock_guard<std::mutex> done(state->mutex);
            state->remaining -= jobs.size() - queued;
        }
        cv_.notify_all();
        std::unique_lock<std::mutex> lock(state->mutex);
        state->cv.wait(lock, [&state] { return state->remaining == 0; });
    }

   private:
    WorkerPool() = default;

    // Returns true when jobs can be dispatched to background workers.
    bool EnsureStarted() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (started_) return true;
        if (synchronous_fallback_) return false;
        size_t count = BatchReadThreads();
        if (count == 0) count = 1;
        workers_.reserve(count);
        try {
            for (size_t i = 0; i < count; ++i) {
                workers_.emplace_back([this]() { Loop(); });
            }
        } catch (...) {
            // Keep whatever threads were created; decide below.
        }
        if (workers_.empty()) {
            synchronous_fallback_ = true;
            return false;
        }
        started_ = true;
        return true;
    }

    void Loop() {
        while (true) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this] { return !queue_.empty(); });
                job = std::move(queue_.front());
                queue_.pop_front();
            }
            try {
                job();
            } catch (...) {
            }
        }
    }

    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<std::function<void()>> queue_;
    std::vector<std::thread> workers_;
    bool started_ = false;
    bool synchronous_fallback_ = false;
};

struct Run {
    int64_t fd = -1;
    uint64_t offset = 0;
    bool direct = false;
    uint64_t total = 0;
    std::vector<iovec> iovs;
    std::vector<size_t> request_indices;
};

uint64_t AlignUp(uint64_t value, uint64_t alignment) {
    const uint64_t remainder = value % alignment;
    return remainder == 0 ? value : value + (alignment - remainder);
}

// Positional read/write that walks the iov chain and handles short transfers.
// On failure `*io_errno` receives the errno of the failing syscall (or 0 for a
// short read at EOF), never a stale value left on the worker thread.
bool PositionalIo(Run& run, bool write, int* io_errno) {
    std::vector<iovec> pending;
    pending.reserve(std::min(kMaxIovChunk, run.iovs.size()));
    uint64_t done = 0;
    size_t index = 0;
    uint64_t consumed = 0;
    while (done < run.total && index < run.iovs.size()) {
        pending.clear();
        pending.push_back(
            {static_cast<char*>(run.iovs[index].iov_base) + consumed,
             run.iovs[index].iov_len - consumed});
        for (size_t j = index + 1;
             j < run.iovs.size() && pending.size() < kMaxIovChunk; ++j) {
            pending.push_back(run.iovs[j]);
        }
        ssize_t transferred = 0;
        do {
            transferred =
                write ? ::pwritev(run.fd, pending.data(),
                                  static_cast<int>(pending.size()),
                                  static_cast<off_t>(run.offset + done))
                      : ::preadv(run.fd, pending.data(),
                                 static_cast<int>(pending.size()),
                                 static_cast<off_t>(run.offset + done));
        } while (transferred < 0 && errno == EINTR);
        if (transferred <= 0) {
            *io_errno = transferred < 0 ? errno : 0;
            return false;
        }

        uint64_t advanced = static_cast<uint64_t>(transferred);
        done += advanced;
        while (advanced != 0 && index < run.iovs.size()) {
            const uint64_t available = run.iovs[index].iov_len - consumed;
            const uint64_t step = std::min<uint64_t>(advanced, available);
            consumed += step;
            advanced -= step;
            if (consumed == run.iovs[index].iov_len) {
                ++index;
                consumed = 0;
            }
        }
    }
    return done == run.total;
}

// Direct I/O cannot address an unaligned buffer, so stage through an aligned
// bounce buffer. The caller guarantees aligned offset and length before we get
// here.
bool DirectIo(Run& run, bool write, int* io_errno) {
    // Zero-copy fast path: a block-aligned offset with block-aligned buffers
    // and lengths can be handed straight to preadv/pwritev, matching the
    // pre-change direct read path. Only unaligned runs are staged.
    if (run.offset % kDirectAlignment == 0) {
        bool aligned = true;
        for (const auto& iov : run.iovs) {
            if (reinterpret_cast<uintptr_t>(iov.iov_base) % kDirectAlignment !=
                    0 ||
                iov.iov_len % kDirectAlignment != 0) {
                aligned = false;
                break;
            }
        }
        if (aligned) return PositionalIo(run, write, io_errno);
    }
    void* raw = nullptr;
    const int alloc_error =
        ::posix_memalign(&raw, kDirectAlignment,
                         static_cast<size_t>(run.total));
    if (alloc_error != 0) {
        // posix_memalign returns the error number instead of setting errno.
        *io_errno = alloc_error;
        return false;
    }
    std::unique_ptr<void, decltype(&::free)> bounce(raw, &::free);
    char* buffer = static_cast<char*>(raw);

    if (write) {
        size_t offset = 0;
        for (const auto& iov : run.iovs) {
            std::memcpy(buffer + offset, iov.iov_base, iov.iov_len);
            offset += iov.iov_len;
        }
    }

    iovec iov{buffer, static_cast<size_t>(run.total)};
    uint64_t done = 0;
    while (done < run.total) {
        ssize_t transferred = 0;
        do {
            transferred = write
                              ? ::pwritev(run.fd, &iov, 1,
                                          static_cast<off_t>(run.offset + done))
                              : ::preadv(run.fd, &iov, 1,
                                         static_cast<off_t>(run.offset + done));
        } while (transferred < 0 && errno == EINTR);
        if (transferred <= 0) {
            *io_errno = transferred < 0 ? errno : 0;
            return false;
        }
        done += static_cast<uint64_t>(transferred);
        iov.iov_base = buffer + done;
        iov.iov_len = static_cast<size_t>(run.total - done);
    }

    if (!write) {
        size_t offset = 0;
        for (const auto& target : run.iovs) {
            std::memcpy(target.iov_base, buffer + offset, target.iov_len);
            offset += target.iov_len;
        }
    }
    return true;
}

int ExecuteRun(Run& run, bool write, int* io_errno) {
    *io_errno = 0;
    if (run.total == 0) return MOONCAKE_EXT_STATUS_OK;
    if (run.direct) {
        if ((run.offset % kDirectAlignment) != 0 ||
            (run.total % kDirectAlignment) != 0) {
            // The baseline adapter bounces unaligned direct I/O; ask the
            // caller to take that path rather than guessing here. This is a
            // shape rejection, not an I/O failure, so errno stays 0.
            return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
        }
        return DirectIo(run, write, io_errno) ? MOONCAKE_EXT_STATUS_OK
                                             : MOONCAKE_EXT_STATUS_IO_ERROR;
    }
    return PositionalIo(run, write, io_errno) ? MOONCAKE_EXT_STATUS_OK
                                              : MOONCAKE_EXT_STATUS_IO_ERROR;
}

int IoBatchImpl(void*, const mooncake_ext_io_request* requests,
            mooncake_ext_io_result* results, size_t request_count) {
    if (requests == nullptr || results == nullptr) {
        return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
    }
    for (size_t i = 0; i < request_count; ++i) {
        results[i].status = MOONCAKE_EXT_STATUS_IO_ERROR;
        results[i].error_code = 0;
        results[i].bytes_done = 0;
    }
    if (request_count == 0) return MOONCAKE_EXT_STATUS_OK;
    const uint32_t op = requests[0].op;
    if (op != MOONCAKE_EXT_IO_READ && op != MOONCAKE_EXT_IO_WRITE) {
        return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
    }
    for (size_t i = 1; i < request_count; ++i) {
        if (requests[i].op != op) return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
    }

    std::vector<size_t> order(request_count);
    for (size_t i = 0; i < request_count; ++i) order[i] = i;
    std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
        if (requests[a].fd != requests[b].fd) {
            return requests[a].fd < requests[b].fd;
        }
        return requests[a].offset < requests[b].offset;
    });

    std::vector<Run> runs;
    runs.reserve(request_count);
    for (size_t position : order) {
        const auto& request = requests[position];
        Run run;
        run.fd = request.fd;
        run.offset = request.offset;
        run.direct = request.direct_io != 0;
        run.total = request.total_size;
        run.iovs.reserve(request.iovec_count);
        for (uint64_t i = 0; i < request.iovec_count; ++i) {
            run.iovs.push_back({request.iovecs[i].base,
                                static_cast<size_t>(request.iovecs[i].length)});
        }
        run.request_indices.push_back(position);

        const bool can_merge =
            (request.op == MOONCAKE_EXT_IO_WRITE || MergingAllowed()) &&
            request.mergeable != 0 && !runs.empty() &&
            runs.back().fd == run.fd && runs.back().direct == run.direct &&
            runs.back().offset + runs.back().total == run.offset &&
            runs.back().total + run.total <= kMaxMergedRun;
        if (can_merge) {
            auto& previous = runs.back();
            previous.total += run.total;
            previous.iovs.insert(previous.iovs.end(), run.iovs.begin(),
                                 run.iovs.end());
            previous.request_indices.push_back(position);
            continue;
        }
        runs.push_back(std::move(run));
    }

    const bool write = requests[0].op == MOONCAKE_EXT_IO_WRITE;
    // Start from the failure state: a run whose job never executes (or throws)
    // must never be reported as successful.
    std::vector<int> run_status(runs.size(), MOONCAKE_EXT_STATUS_IO_ERROR);
    std::vector<int> run_errno(runs.size(), 0);
    std::vector<std::function<void()>> jobs;
    jobs.reserve(runs.size());
    for (size_t i = 0; i < runs.size(); ++i) {
        jobs.push_back([&, i]() {
            int status = MOONCAKE_EXT_STATUS_IO_ERROR;
            int status_errno = 0;
            try {
                status = ExecuteRun(runs[i], write, &status_errno);
            } catch (...) {
                // An exception (e.g. bad_alloc) means the run did not finish:
                // report an I/O failure instead of leaving the initial value
                // to be misread as success by the caller.
                status = MOONCAKE_EXT_STATUS_IO_ERROR;
                status_errno = 0;
            }
            run_status[i] = status;
            run_errno[i] =
                status == MOONCAKE_EXT_STATUS_OK ? 0 : status_errno;
        });
    }
    if (write) {
        // Pre-split parity: writes are merged into runs and issued
        // sequentially in the calling thread. Fanning the runs out across the
        // worker pool makes several threads write the same bucket file at
        // once, and the DFS serialises them on per-file range locks, which is
        // measurably slower than one sequential merged pass.
        for (auto& job : jobs) job();
    } else if (jobs.size() == 1) {
        // One run needs no pool hop.
        jobs.front()();
    } else {
        WorkerPool::Instance().Run(std::move(jobs));
    }

    for (size_t i = 0; i < runs.size(); ++i) {
        for (size_t position : runs[i].request_indices) {
            results[position].status = run_status[i];
            results[position].error_code =
                run_status[i] == MOONCAKE_EXT_STATUS_OK ? 0 : run_errno[i];
            results[position].bytes_done =
                run_status[i] == MOONCAKE_EXT_STATUS_OK
                    ? requests[position].total_size
                    : 0;
        }
    }
    return MOONCAKE_EXT_STATUS_OK;
}

// C ABI boundary: never let a C++ exception cross into the public tree.
int IoBatch(void* handle, const mooncake_ext_io_request* requests,
            mooncake_ext_io_result* results, size_t request_count) {
    try {
        return IoBatchImpl(handle, requests, results, request_count);
    } catch (...) {
        if (results != nullptr) {
            for (size_t i = 0; i < request_count; ++i) {
                results[i].status = MOONCAKE_EXT_STATUS_IO_ERROR;
                results[i].error_code = 0;
                results[i].bytes_done = 0;
            }
        }
        return MOONCAKE_EXT_STATUS_IO_ERROR;
    }
}

int AlignBatchOffset(void*, uint64_t append_offset, uint64_t capacity,
                     uint64_t object_size, uint64_t* out_offset,
                     uint64_t* out_alignment, uint32_t* fits) {
    if (out_offset == nullptr || out_alignment == nullptr || fits == nullptr ||
        object_size == 0) {
        return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
    }
    const uint64_t alignment = BatchAlignment();
    const auto compact_fits = [&] {
        return append_offset <= capacity &&
                       object_size <= capacity - append_offset
                   ? 1
                   : 0;
    };
    if (alignment == 0) {
        // Zero explicitly keeps the baseline compact layout, with no hint.
        *out_offset = append_offset;
        *out_alignment = 0;
        *fits = compact_fits();
        return MOONCAKE_EXT_STATUS_OK;
    }
    if (alignment > capacity || capacity % alignment != 0) {
        // The boundary is a placement hint, not a correctness requirement.
        // Report the boundary as configured instead of masking it as 0: the
        // caller validates it, warns once at startup, and keeps the compact
        // layout. Returning 0 here would silently disable the feature.
        *out_offset = append_offset;
        *out_alignment = alignment;
        *fits = compact_fits();
        return MOONCAKE_EXT_STATUS_OK;
    }
    *out_alignment = alignment;
    const uint64_t aligned = AlignUp(append_offset, alignment);
    *out_offset = aligned;
    *fits = aligned <= capacity && object_size <= capacity - aligned ? 1 : 0;
    return MOONCAKE_EXT_STATUS_OK;
}

int ConfigureWriteFd(void*, int fd, uint64_t max_write_bytes) {
    static const bool enabled =
        EnvFlag("MOONCAKE_DFS_POSIX_WRITE_CONTEXT_ENABLED", true);
    if (!enabled) {
        // Pre-change behavior when disabled: no ioctl, mtime updated by the
        // filesystem on every write. Report a distinguishable "disabled"
        // status so the caller does not log that the optimization is active.
        return MOONCAKE_EXT_STATUS_DISABLED;
    }
#if defined(__linux__)
    if (fd < 0) return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
    mooncake::storage_ioctl::WriteContext context{};
    context.is_iorank = 0;
    context.size = max_write_bytes;
    context.stage_flags = mooncake::storage_ioctl::kWriteStageSkipTime |
                          mooncake::storage_ioctl::kWriteStageNoExtendLock;
    if (::ioctl(fd, STORAGE_MPIO_SET_WRCONTEXT, &context) == 0) {
        return MOONCAKE_EXT_STATUS_OK;
    }
    const int saved_errno = errno;
    if (saved_errno == ENOTTY || saved_errno == EINVAL ||
        saved_errno == EOPNOTSUPP) {
        return MOONCAKE_EXT_STATUS_NOT_SUPPORTED;
    }
    return MOONCAKE_EXT_STATUS_IO_ERROR;
#else
    (void)fd;
    (void)max_write_bytes;
    return MOONCAKE_EXT_STATUS_NOT_SUPPORTED;
#endif
}

int RefreshFileMtime(void*, int fd) {
#if defined(__linux__)
    if (fd < 0) return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
    unsigned int arg = 0;
    if (::ioctl(fd, STORAGE_UPDATE_TIME, &arg) >= 0) {
        return MOONCAKE_EXT_STATUS_OK;
    }
    const int saved_errno = errno;
    if (saved_errno == ENOENT) return MOONCAKE_EXT_STATUS_OK;
    if (saved_errno == ENOTTY || saved_errno == EINVAL ||
        saved_errno == EOPNOTSUPP) {
        return MOONCAKE_EXT_STATUS_NOT_SUPPORTED;
    }
    return MOONCAKE_EXT_STATUS_IO_ERROR;
#else
    (void)fd;
    return MOONCAKE_EXT_STATUS_NOT_SUPPORTED;
#endif
}

int BufferPolicy(void*, const mooncake_ext_buffer_policy_query* query,
                 mooncake_ext_buffer_policy* result) {
    if (query == nullptr || result == nullptr || query->capacity_bytes == 0) {
        return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
    }
    const uint64_t read_chunks = ReadPositiveEnv(
        "MC_STORE_DFS_READ_PROTECTION_CHUNKS", kReadProtectionChunks);
    const uint64_t write_chunks = ReadPositiveEnv(
        "MC_STORE_DFS_WRITE_PROTECTION_CHUNKS", kWriteProtectionChunks);
    const double prefetch_ratio = ReadRatioEnv(
        "MC_STORE_DFS_PREFETCH_CAPACITY_RATIO", kPrefetchCapacityRatio);

    // Reads may never hold back more than half the buffer: the rest has to
    // serve prefetch and write staging.
    result->read_min_protection_bytes =
        std::min(ScaleBytes(query->read_chunk_bytes, read_chunks),
                 query->capacity_bytes / 2);
    result->write_min_protection_bytes =
        ScaleBytes(query->write_chunk_bytes, write_chunks);
    result->prefetch_limit_bytes = static_cast<uint64_t>(
        static_cast<double>(query->capacity_bytes) * prefetch_ratio);
    result->high_watermark_numerator = kHighWatermarkNumerator;
    result->high_watermark_denominator = kHighWatermarkDenominator;
    return MOONCAKE_EXT_STATUS_OK;
}

}  // namespace

extern "C" __attribute__((visibility("default"))) int
mooncake_dfe_extension_get(mooncake_dfe_ext* extension) {
    if (extension == nullptr ||
        extension->abi_version != MOONCAKE_DFE_ABI_VERSION ||
        extension->struct_size != sizeof(mooncake_dfe_ext)) {
        return MOONCAKE_EXT_STATUS_INVALID_ARGUMENT;
    }
    extension->user_data = nullptr;
    extension->io_batch = &IoBatch;
    extension->align_batch_offset = &AlignBatchOffset;
    extension->configure_write_fd = &ConfigureWriteFd;
    extension->refresh_file_mtime = &RefreshFileMtime;
    extension->buffer_policy = &BufferPolicy;
    return MOONCAKE_EXT_STATUS_OK;
}
