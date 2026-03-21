#pragma once
// ============================================================================
// APEX-DB Logger (spdlog wrapper)
// ============================================================================

#include <spdlog/spdlog.h>
#include <memory>

namespace apex {

class Logger {
public:
    static void init(const std::string& name = "apex",
                     spdlog::level::level_enum level = spdlog::level::info);

    static std::shared_ptr<spdlog::logger>& get();

private:
    static std::shared_ptr<spdlog::logger> s_logger;
};

// Convenience macros
#define APEX_TRACE(...)    SPDLOG_LOGGER_TRACE(apex::Logger::get(), __VA_ARGS__)
#define APEX_DEBUG(...)    SPDLOG_LOGGER_DEBUG(apex::Logger::get(), __VA_ARGS__)
#define APEX_INFO(...)     SPDLOG_LOGGER_INFO(apex::Logger::get(), __VA_ARGS__)
#define APEX_WARN(...)     SPDLOG_LOGGER_WARN(apex::Logger::get(), __VA_ARGS__)
#define APEX_ERROR(...)    SPDLOG_LOGGER_ERROR(apex::Logger::get(), __VA_ARGS__)
#define APEX_CRITICAL(...) SPDLOG_LOGGER_CRITICAL(apex::Logger::get(), __VA_ARGS__)

} // namespace apex
