#pragma once

#include <optional>
#include <string>

namespace mooncake {

struct RuntimeLogConfig {
    std::string minloglevel;
    int vlog_level{0};
    std::string ylt_severity;
};

struct RuntimeLogConfigUpdate {
    std::optional<std::string> minloglevel;
    std::optional<int> vlog_level;
    std::optional<std::string> ylt_severity;
};

RuntimeLogConfig GetRuntimeLogConfig();

bool UpdateRuntimeLogConfig(const RuntimeLogConfigUpdate& update,
                            std::string* error);

}  // namespace mooncake
