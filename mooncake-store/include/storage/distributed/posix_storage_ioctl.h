#pragma once

/**
 * @brief Storage-side ioctl contract used to skip per-write metadata work.
 *
 * These definitions mirror the ABI the target storage stack exposes: a write
 * handle is told, once per open, which metadata updates to skip and how large a
 * single write can be. The struct layout is part of the ABI because `_IOWR`
 * encodes `sizeof(struct)`, so the field order and padding must match the
 * kernel-side definition exactly.
 *
 * On a filesystem without the extension every command fails with ENOTTY and
 * callers keep the default behavior.
 */

#if defined(__linux__)

#include <linux/types.h>
#include <sys/ioctl.h>

#include <cstdint>

namespace mooncake {
namespace storage_ioctl {

// Skip the atime update that a write would otherwise trigger.
inline constexpr uint32_t kWriteStageSkipTime = 0x01;
// Skip the write-side file-size refresh.
inline constexpr uint32_t kWriteStageSkipSize = 0x02;
// Skip the per-write range lock.
inline constexpr uint32_t kWriteStageSkipLock = 0x04;
// Forbid extending the file's range lock beyond what is already held.
inline constexpr uint32_t kWriteStageNoExtendLock = 0x08;

/**
 * @brief Per-open write contract for one file handle.
 *
 * `is_iorank` marks the single process that owns the file's global size/time;
 * `size` is the largest write this handle will issue.
 */
struct WriteContext {
    uint64_t pad[2];
    uint32_t chksize;
    uint8_t stripe_width;
    uint8_t pad2[3];
    uint32_t is_iorank;
    uint32_t stage_flags;
    uint64_t size;
};

}  // namespace storage_ioctl
}  // namespace mooncake

#define STORAGE_IOC_MAGIC_S 'S'
// "Forbid the current file's range-lock extension and return its striping
// information."
#define STORAGE_MPIO_SET_WRCONTEXT \
    _IOWR(STORAGE_IOC_MAGIC_S, 6, struct mooncake::storage_ioctl::WriteContext)

#define STORAGE_IOC_MAGIC_P 'p'
#define STORAGE_IOCTL_NR 0xa0
#define STORAGE_UPDATE_TIME \
    _IOW(STORAGE_IOC_MAGIC_P, STORAGE_IOCTL_NR + 41, unsigned int)

#endif  // __linux__
