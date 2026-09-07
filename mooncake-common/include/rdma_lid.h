// Copyright (c) 2026 Hygon Information Technology Co., Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>

namespace mooncake {

// Keep the existing layout on ordinary IB/RoCE builds. SHCA uses 17-bit LIDs.
#ifdef USE_SHCA
using RdmaLid = uint32_t;
#else
using RdmaLid = uint16_t;
#endif

}  // namespace mooncake
