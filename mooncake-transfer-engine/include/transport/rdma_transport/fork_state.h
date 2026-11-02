// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Helpers around rdma-core's fork protection.
//
// When ibv_fork_init() has enabled fork protection, libibverbs applies
// madvise(MADV_DONTFORK) to the range of every MR created through ibv_reg_mr().
// A stack that does not issue the matching MADV_DOFORK on ibv_dereg_mr()
// leaves those VMAs permanently split and eventually exhausts
// vm.max_map_count (Mooncake#3639, kvcache-ai#3660).
//
// Only ranges backed by an ordinary host VMA can carry that state: dma-buf
// imports keep a device iova in mr->addr, and device or managed memory that
// falls back to ibv_reg_mr() (peermem) has no host VMA at all, so madvise()
// on those ranges is rejected by the kernel. Hence the decision helpers here.

#ifndef MOONCAKE_TRANSPORT_RDMA_TRANSPORT_FORK_STATE_H
#define MOONCAKE_TRANSPORT_RDMA_TRANSPORT_FORK_STATE_H

#include <sys/mman.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>

#include <glog/logging.h>

#include "cuda_alike.h"

namespace mooncake {

// Round [addr, addr + length) outwards to whole pages, exactly the way
// rdma-core's ibv_madvise_range() does before touching a range. madvise()
// requires a page aligned start, so a registered interior pointer has to be
// widened here or the call fails with EINVAL.
inline void alignForkRange(void *addr, size_t length, uintptr_t &start,
                           uintptr_t &end) {
    static const size_t page_size = [] {
        long size = sysconf(_SC_PAGESIZE);
        return size > 0 ? static_cast<size_t>(size) : size_t{4096};
    }();
    if (addr == nullptr || length == 0) {
        start = end = 0;
        return;
    }
    start = reinterpret_cast<uintptr_t>(addr) & ~(page_size - 1);
    end = (reinterpret_cast<uintptr_t>(addr) + length + page_size - 1) &
          ~(page_size - 1);
}

// Whether a registration needs its fork state rolled back on unregister.
// libibverbs only runs the MADV_DONTFORK bookkeeping for ibv_reg_mr(), and
// only a host mapping can hold the resulting VM_DONTCOPY, so all three
// conditions have to hold.
inline bool forkStateShouldBeRestored(bool registered_by_ibv_reg_mr,
                                      bool export_reports_host_vma,
                                      bool host_backed) {
    return registered_by_ibv_reg_mr && export_reports_host_vma && host_backed;
}

// Whether `addr` is backed by an ordinary host mapping. Accelerator memory has
// no host VMA (or a device mapping whose flags the kernel refuses to change),
// so unregistering must leave it alone.
inline bool isHostBackedRange(void *addr) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||    \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) ||   \
    defined(USE_COREX) || defined(USE_SUPA) || defined(USE_SUNRISE)
    // A host without accelerators must not pay a failing probe per MR.
    static const bool device_present = [] {
        int device_count = 0;
        return cudaGetDeviceCount(&device_count) == cudaSuccess &&
               device_count > 0;
    }();
    if (device_present) {
        cudaPointerAttributes attributes;
        // Unregistered host memory reports success with a non-device type on
        // current runtimes and fails on older ones; treat both as host and stay
        // silent, madvise() is the final arbiter.
        if (cudaPointerGetAttributes(&attributes, addr) == cudaSuccess &&
            attributes.type == cudaMemoryTypeDevice)
            return false;
    }
#endif
    return true;
}

// Undo the MADV_DONTFORK applied at registration time. Callers must only reach
// here for a host-backed MR (see forkStateShouldBeRestored); a failure is then
// a genuine one and is reported at WARNING, as before this change.
inline void restoreForkState(void *addr, size_t length) {
    uintptr_t start = 0;
    uintptr_t end = 0;
    alignForkRange(addr, length, start, end);
    if (end <= start) return;
    if (madvise(reinterpret_cast<void *>(start), end - start, MADV_DOFORK) == 0)
        return;
    PLOG(WARNING) << "Failed to restore fork state for memory region at "
                  << reinterpret_cast<void *>(start) << " (" << (end - start)
                  << " bytes), deregister already succeeded";
}

}  // namespace mooncake

#endif  // MOONCAKE_TRANSPORT_RDMA_TRANSPORT_FORK_STATE_H
