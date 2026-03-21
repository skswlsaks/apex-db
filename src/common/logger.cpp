// ============================================================================
// APEX-DB Logger Implementation
// ============================================================================

#include "apex/common/logger.h"
#include <spdlog/sinks/stdout_color_sinks.h>

namespace apex {

std::shared_ptr<spdlog::logger> Logger::s_logger;

void Logger::init(const std::string& name, spdlog::level::level_enum level) {
    s_logger = spdlog::stdout_color_mt(name);
    s_logger->set_level(level);
    s_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%f] [%^%l%$] [%s:%#] %v");
    SPDLOG_LOGGER_INFO(s_logger, "APEX-DB Logger initialized");
}

std::shared_ptr<spdlog::logger>& Logger::get() {
    if (!s_logger) {
        init();
    }
    return s_logger;
}

} // namespace apex
