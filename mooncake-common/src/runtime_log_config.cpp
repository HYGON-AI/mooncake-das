#include "runtime_log_config.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdlib>
#include <mutex>
#include <string_view>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <ylt/easylog.hpp>

namespace mooncake {
namespace {

std::string ToLower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

bool ParseGlogLevel(const std::string& value, int* level,
                    std::string* normalized) {
    const auto lower = ToLower(value);
    if (lower == "info") {
        *level = google::INFO;
        *normalized = "INFO";
    } else if (lower == "warning" || lower == "warn") {
        *level = google::WARNING;
        *normalized = "WARNING";
    } else if (lower == "error") {
        *level = google::ERROR;
        *normalized = "ERROR";
    } else if (lower == "fatal") {
        *level = google::FATAL;
        *normalized = "FATAL";
    } else {
        return false;
    }
    return true;
}

easylog::Severity ParseYltLevel(const std::string& value, std::string* normalized) {
    const auto lower = ToLower(value);
    if (lower == "trace") {
        *normalized = "TRACE";
        return easylog::Severity::TRACE;
    }
    if (lower == "debug") {
        *normalized = "DEBUG";
        return easylog::Severity::DEBUG;
    }
    if (lower == "info") {
        *normalized = "INFO";
        return easylog::Severity::INFO;
    }
    if (lower == "warn" || lower == "warning") {
        *normalized = "WARN";
        return easylog::Severity::WARN;
    }
    if (lower == "error") {
        *normalized = "ERROR";
        return easylog::Severity::ERROR;
    }
    if (lower == "critical") {
        *normalized = "CRITICAL";
        return easylog::Severity::CRITICAL;
    }
    return easylog::Severity::WARN;
}

bool IsValidYltLevel(const std::string& value) {
    const auto lower = ToLower(value);
    return lower == "trace" || lower == "debug" || lower == "info" ||
           lower == "warn" || lower == "warning" || lower == "error" ||
           lower == "critical";
}

std::mutex& RuntimeLogMutex() {
    static std::mutex mutex;
    return mutex;
}

bool ReadDfsReadTraceEnvValue() {
    const char* val = std::getenv("MC_STORE_DFS_READ_TRACE");
    if (!val || val[0] == '\0') return false;
    std::string value(val);
    value = ToLower(std::move(value));
    return value != "0" && value != "false" && value != "off";
}

std::atomic<bool> g_dfs_read_trace_enabled{ReadDfsReadTraceEnvValue()};

}  // namespace

RuntimeLogConfig GetRuntimeLogConfig() {
    std::lock_guard lock(RuntimeLogMutex());
    RuntimeLogConfig config;
    switch (FLAGS_minloglevel) {
        case google::INFO:
            config.minloglevel = "INFO";
            break;
        case google::WARNING:
            config.minloglevel = "WARNING";
            break;
        case google::ERROR:
            config.minloglevel = "ERROR";
            break;
        default:
            config.minloglevel = "FATAL";
            break;
    }
    config.vlog_level = FLAGS_v;
    switch (easylog::get_min_severity()) {
        case easylog::Severity::TRACE:
            config.ylt_severity = "TRACE";
            break;
        case easylog::Severity::DEBUG:
            config.ylt_severity = "DEBUG";
            break;
        case easylog::Severity::INFO:
            config.ylt_severity = "INFO";
            break;
        case easylog::Severity::WARN:
            config.ylt_severity = "WARN";
            break;
        case easylog::Severity::ERROR:
            config.ylt_severity = "ERROR";
            break;
        default:
            config.ylt_severity = "CRITICAL";
            break;
    }
    config.dfs_read_trace = GetDfsReadTraceEnabled();
    return config;
}

bool UpdateRuntimeLogConfig(const RuntimeLogConfigUpdate& update,
                            std::string* error) {
    std::lock_guard lock(RuntimeLogMutex());

    int minloglevel = FLAGS_minloglevel;
    std::string minloglevel_name;
    if (update.minloglevel.has_value() &&
        !ParseGlogLevel(*update.minloglevel, &minloglevel, &minloglevel_name)) {
        *error = "minloglevel must be INFO, WARNING, ERROR, or FATAL";
        return false;
    }
    if (update.vlog_level.has_value() &&
        (*update.vlog_level < 0 || *update.vlog_level > 2)) {
        *error = "vlog_level must be between 0 and 2";
        return false;
    }
    if (update.ylt_severity.has_value() &&
        !IsValidYltLevel(*update.ylt_severity)) {
        *error = "ylt_severity must be TRACE, DEBUG, INFO, WARN, ERROR, or CRITICAL";
        return false;
    }

    if (update.minloglevel.has_value()) {
        const auto result = google::SetCommandLineOption(
            "minloglevel", std::to_string(minloglevel).c_str());
        if (result.empty()) {
            *error = "failed to update glog minloglevel";
            return false;
        }
    }
    if (update.vlog_level.has_value()) {
        const auto result = google::SetCommandLineOption(
            "v", std::to_string(*update.vlog_level).c_str());
        if (result.empty()) {
            *error = "failed to update glog vlog_level";
            return false;
        }
        google::SetVLOGLevel("*", *update.vlog_level);
    }
    if (update.ylt_severity.has_value()) {
        std::string normalized;
        easylog::set_min_severity(
            ParseYltLevel(*update.ylt_severity, &normalized));
    }
    if (update.dfs_read_trace.has_value()) {
        SetDfsReadTraceEnabled(*update.dfs_read_trace);
    }
    return true;
}

bool GetDfsReadTraceEnabled() {
    return g_dfs_read_trace_enabled.load(std::memory_order_relaxed);
}

void SetDfsReadTraceEnabled(bool enabled) {
    g_dfs_read_trace_enabled.store(enabled, std::memory_order_relaxed);
}

}  // namespace mooncake
