#pragma once

#include <optional>
#include <string>

namespace mooncake {

struct RuntimeLogConfig {
    std::string minloglevel;
    int vlog_level{0};
    std::string ylt_severity;
    bool dfs_read_trace{false};
};

struct RuntimeLogConfigUpdate {
    std::optional<std::string> minloglevel;
    std::optional<int> vlog_level;
    std::optional<std::string> ylt_severity;
    std::optional<bool> dfs_read_trace;
};

RuntimeLogConfig GetRuntimeLogConfig();

bool UpdateRuntimeLogConfig(const RuntimeLogConfigUpdate& update,
                            std::string* error);

bool GetDfsReadTraceEnabled();
void SetDfsReadTraceEnabled(bool enabled);

}  // namespace mooncake
