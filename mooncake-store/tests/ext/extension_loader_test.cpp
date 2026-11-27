// Contract tests for the public side of the extension loader.
//
// DfeProvider is a process-wide singleton, so each scenario runs in a forked
// child: the parent never touches the provider, and every child starts from an
// unloaded state with its own environment.

#include <sys/wait.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "ext/dfe_loader.h"
#include "gtest/gtest.h"

#ifndef FAKE_DFE_PROVIDER_DIR
#error "FAKE_DFE_PROVIDER_DIR must point at the fake provider built by CMake"
#endif

namespace {

using mooncake::ext::DfeProvider;

#define CHILD_CHECK(condition)          \
    do {                                \
        if (!(condition)) {             \
            ::_exit(__LINE__);          \
        }                               \
    } while (0)

void PrepareEnv() {
    ::unsetenv("MOONCAKE_EXT_DIR");
    ::unsetenv("FAKE_DFE_BAD_ABI");
    ::setenv("MOONCAKE_ENABLE_EXT", "1", 1);
}

// Runs `body` in a child process. Returns the child's exit status.
int RunInChild(void (*body)()) {
    const pid_t pid = ::fork();
    if (pid == 0) {
        body();
        ::_exit(0);
    }
    int status = 0;
    ::waitpid(pid, &status, 0);
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    return -1;
}

// Exercises every helper the public side may call when a real provider loads.
void LoadedScenario() {
    PrepareEnv();
    ::setenv("MOONCAKE_EXT_DIR", FAKE_DFE_PROVIDER_DIR, 1);
    const DfeProvider& provider = DfeProvider::Instance();
    CHILD_CHECK(provider.available());
    CHILD_CHECK(provider.state() == "loaded");

    // io_batch must reach the provider and report the exact byte count.
    std::vector<char> a(4096, '\xAA');
    std::vector<char> b(4096, '\xAA');
    mooncake_ext_iovec iovecs[2] = {{a.data(), a.size()}, {b.data(), b.size()}};
    mooncake_ext_io_request request{};
    request.fd = -1;
    request.offset = 2 * 4096;
    request.total_size = a.size() + b.size();
    request.op = MOONCAKE_EXT_IO_READ;
    request.direct_io = 0;
    request.mergeable = 1;
    request.worker_count = 0;
    request.iovecs = iovecs;
    request.iovec_count = 2;
    mooncake_ext_io_result result{};
    CHILD_CHECK(provider.IoBatch(&request, &result, 1));
    CHILD_CHECK(result.status == MOONCAKE_EXT_STATUS_OK);
    CHILD_CHECK(result.bytes_done == request.total_size);
    CHILD_CHECK(a[0] == '\x02' && a[4095] == '\x02');
    CHILD_CHECK(b[0] == '\x02' && b[4095] == '\x02');

    // The placement hook returns the provider's boundary and a fit verdict.
    uint64_t offset = 0;
    uint64_t alignment = 0;
    bool fits = false;
    CHILD_CHECK(provider.AlignBatchOffset(1000, 1u << 20, 4096, &offset,
                                          &alignment, &fits));
    CHILD_CHECK(offset == 65536);
    CHILD_CHECK(alignment == 65536);
    CHILD_CHECK(fits);
    CHILD_CHECK(provider.AlignBatchOffset((1u << 20) - 8192, 1u << 20, 4096,
                                          &offset, &alignment, &fits));
    CHILD_CHECK(!fits);

    // Write-handle and mtime hooks forward the status verbatim.
    CHILD_CHECK(provider.ConfigureWriteFd(0, 1u << 20) ==
                MOONCAKE_EXT_STATUS_OK);
    CHILD_CHECK(provider.ConfigureWriteFd(-1, 1u << 20) ==
                MOONCAKE_EXT_STATUS_INVALID_ARGUMENT);
    CHILD_CHECK(provider.RefreshFileMtime(0) == MOONCAKE_EXT_STATUS_OK);

    // The admission policy is fully owned by the provider.
    mooncake_ext_buffer_policy_query query{};
    query.capacity_bytes = 1u << 20;
    query.read_chunk_bytes = 4096;
    query.write_chunk_bytes = 4096;
    mooncake_ext_buffer_policy policy{};
    CHILD_CHECK(provider.BufferPolicy(query, &policy));
    CHILD_CHECK(policy.read_min_protection_bytes == 3 * 4096);
    CHILD_CHECK(policy.write_min_protection_bytes == 2 * 4096);
    CHILD_CHECK(policy.prefetch_limit_bytes == (1u << 20) / 3);
    CHILD_CHECK(policy.high_watermark_numerator == 7);
    CHILD_CHECK(policy.high_watermark_denominator == 10);
}

// No library found anywhere: the public side must keep its baseline behavior.
void AbsentScenario() {
    PrepareEnv();
    ::setenv("MOONCAKE_EXT_DIR", "/nonexistent", 1);
    const DfeProvider& provider = DfeProvider::Instance();
    if (provider.available()) {
        // A real provider is installed in the default location of this machine;
        // the fallback path cannot be exercised here without removing it.
        return;
    }
    CHILD_CHECK(provider.state() == "absent");

    mooncake_ext_io_request request{};
    mooncake_ext_io_result result{};
    mooncake_ext_iovec iovec{};
    request.iovecs = &iovec;
    request.iovec_count = 1;
    CHILD_CHECK(!provider.IoBatch(&request, &result, 1));

    uint64_t offset = 0;
    uint64_t alignment = 0;
    bool fits = false;
    CHILD_CHECK(!provider.AlignBatchOffset(0, 1u << 20, 4096, &offset,
                                           &alignment, &fits));
    CHILD_CHECK(provider.ConfigureWriteFd(0, 1u << 20) ==
                MOONCAKE_EXT_STATUS_UNAVAILABLE);
    CHILD_CHECK(provider.RefreshFileMtime(0) ==
                MOONCAKE_EXT_STATUS_UNAVAILABLE);

    mooncake_ext_buffer_policy_query query{};
    mooncake_ext_buffer_policy policy{};
    CHILD_CHECK(!provider.BufferPolicy(query, &policy));
}

void DisabledScenario() {
    PrepareEnv();
    ::setenv("MOONCAKE_ENABLE_EXT", "0", 1);
    ::setenv("MOONCAKE_EXT_DIR", FAKE_DFE_PROVIDER_DIR, 1);
    const DfeProvider& provider = DfeProvider::Instance();
    CHILD_CHECK(!provider.available());
    CHILD_CHECK(provider.state() == "disabled");
}

void AbiMismatchScenario() {
    PrepareEnv();
    ::setenv("MOONCAKE_EXT_DIR", FAKE_DFE_PROVIDER_DIR, 1);
    ::setenv("FAKE_DFE_BAD_ABI", "1", 1);
    const DfeProvider& provider = DfeProvider::Instance();
    CHILD_CHECK(!provider.available());
    CHILD_CHECK(provider.state() == "abi_mismatch");
}

TEST(ExtensionLoaderTest, LoadedProviderSatisfiesContract) {
    EXPECT_EQ(RunInChild(&LoadedScenario), 0);
}

TEST(ExtensionLoaderTest, MissingLibraryFallsBackCleanly) {
    EXPECT_EQ(RunInChild(&AbsentScenario), 0);
}

TEST(ExtensionLoaderTest, DisabledByEnvironment) {
    EXPECT_EQ(RunInChild(&DisabledScenario), 0);
}

TEST(ExtensionLoaderTest, AbiMismatchIsRejected) {
    EXPECT_EQ(RunInChild(&AbiMismatchScenario), 0);
}

}  // namespace
