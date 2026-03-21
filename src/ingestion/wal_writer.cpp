// ============================================================================
// Layer 2: WAL Writer Implementation
// ============================================================================

#include "apex/ingestion/wal_writer.h"
#include "apex/common/logger.h"
#include <stdexcept>

namespace apex::ingestion {

WALWriter::WALWriter(const std::string& path)
    : file_(path, std::ios::binary | std::ios::app)
{
    if (!file_.is_open()) {
        throw std::runtime_error("WALWriter: cannot open " + path);
    }
    APEX_INFO("WAL opened: {}", path);
}

WALWriter::~WALWriter() {
    if (file_.is_open()) {
        file_.flush();
        file_.close();
    }
}

bool WALWriter::write(const TickMessage& msg) {
    std::lock_guard<std::mutex> lock(mutex_);
    file_.write(reinterpret_cast<const char*>(&msg), sizeof(TickMessage));
    if (!file_.good()) {
        APEX_ERROR("WAL write failed at message #{}", count_);
        return false;
    }
    ++count_;
    return true;
}

void WALWriter::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    file_.flush();
}

} // namespace apex::ingestion
