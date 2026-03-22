// ============================================================================
// APEX-DB: HTTP API Server Implementation
// ============================================================================
// cpp-httplib 기반 경량 HTTP 서버
// ClickHouse 호환 포트 8123
// ============================================================================

// httplib은 헤더 전용 — cpp에서만 include (컴파일 속도 최적화)
#include "third_party/httplib.h"
#include "apex/server/http_server.h"
#include "apex/core/pipeline.h"

#include <sstream>
#include <string>
#include <cstdio>

namespace apex::server {

// ============================================================================
// 생성자
// ============================================================================
HttpServer::HttpServer(apex::sql::QueryExecutor& executor, uint16_t port)
    : executor_(executor)
    , port_(port)
    , svr_(std::make_unique<httplib::Server>())
{
    setup_routes();
}

HttpServer::~HttpServer() {
    if (running_.load()) stop();
}

// ============================================================================
// 라우트 설정
// ============================================================================
void HttpServer::setup_routes() {
    // ========== GET /ping — 헬스체크 ==========
    svr_->Get("/ping", [](const httplib::Request& /*req*/,
                           httplib::Response& res) {
        res.set_content("Ok\n", "text/plain");
    });

    // ========== GET /stats — 파이프라인 통계 ==========
    svr_->Get("/stats", [this](const httplib::Request& /*req*/,
                                httplib::Response& res) {
        auto json = build_stats_json(executor_.stats());
        res.set_content(json, "application/json");
    });

    // ========== POST / — SQL 쿼리 실행 (ClickHouse 호환) ==========
    svr_->Post("/", [this](const httplib::Request& req,
                            httplib::Response& res) {
        if (req.body.empty()) {
            res.status = 400;
            res.set_content(build_error_json("Empty query body"), "application/json");
            return;
        }

        auto result = executor_.execute(req.body);

        if (!result.ok()) {
            res.status = 400;
            res.set_content(build_error_json(result.error), "application/json");
            return;
        }

        res.set_content(build_json_response(result), "application/json");
    });

    // ========== GET / — SQL 쿼리 (query 파라미터) ==========
    svr_->Get("/", [this](const httplib::Request& req,
                           httplib::Response& res) {
        auto q = req.get_param_value("query");
        if (q.empty()) {
            res.set_content(R"({"status":"ok","engine":"APEX-DB"})", "application/json");
            return;
        }

        auto result = executor_.execute(q);
        if (!result.ok()) {
            res.status = 400;
            res.set_content(build_error_json(result.error), "application/json");
            return;
        }

        res.set_content(build_json_response(result), "application/json");
    });
}

// ============================================================================
// start() — 블로킹
// ============================================================================
void HttpServer::start() {
    running_.store(true);
    svr_->listen("0.0.0.0", static_cast<int>(port_));
    running_.store(false);
}

// ============================================================================
// start_async() — 백그라운드 스레드
// ============================================================================
void HttpServer::start_async() {
    running_.store(true);
    thread_ = std::thread([this]() {
        svr_->listen("0.0.0.0", static_cast<int>(port_));
        running_.store(false);
    });
    // 서버가 실제로 리슨 시작할 때까지 잠시 대기
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
}

// ============================================================================
// stop()
// ============================================================================
void HttpServer::stop() {
    svr_->stop();
    if (thread_.joinable()) thread_.join();
    running_.store(false);
}

// ============================================================================
// JSON 응답 빌더
// ============================================================================
std::string HttpServer::build_json_response(
    const apex::sql::QueryResultSet& result)
{
    std::ostringstream os;
    os << "{";

    // columns 배열
    os << "\"columns\":[";
    for (size_t i = 0; i < result.column_names.size(); ++i) {
        if (i > 0) os << ",";
        os << "\"" << result.column_names[i] << "\"";
    }
    os << "],";

    // data 배열 (2D)
    os << "\"data\":[";
    for (size_t r = 0; r < result.rows.size(); ++r) {
        if (r > 0) os << ",";
        os << "[";
        const auto& row = result.rows[r];
        for (size_t c = 0; c < row.size(); ++c) {
            if (c > 0) os << ",";
            os << row[c];
        }
        os << "]";
    }
    os << "],";

    // 메타데이터
    os << "\"rows\":" << result.rows.size() << ",";
    os << "\"rows_scanned\":" << result.rows_scanned << ",";

    // 소수점 2자리로 제한
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%.2f", result.execution_time_us);
    os << "\"execution_time_us\":" << buf;

    os << "}";
    return os.str();
}

// ============================================================================
// 에러 JSON
// ============================================================================
std::string HttpServer::build_error_json(const std::string& msg) {
    // 간단한 JSON 이스케이프 (따옴표 처리)
    std::string escaped;
    for (char c : msg) {
        if (c == '"')       escaped += "\\\"";
        else if (c == '\\') escaped += "\\\\";
        else if (c == '\n') escaped += "\\n";
        else                escaped += c;
    }
    return R"({"error":")" + escaped + R"("})";
}

// ============================================================================
// 통계 JSON
// ============================================================================
std::string HttpServer::build_stats_json(
    const apex::core::PipelineStats& stats)
{
    std::ostringstream os;
    os << "{"
       << "\"ticks_ingested\":"   << stats.ticks_ingested.load()   << ","
       << "\"ticks_stored\":"     << stats.ticks_stored.load()     << ","
       << "\"ticks_dropped\":"    << stats.ticks_dropped.load()    << ","
       << "\"queries_executed\":" << stats.queries_executed.load() << ","
       << "\"total_rows_scanned\":" << stats.total_rows_scanned.load()
       << "}";
    return os.str();
}

} // namespace apex::server
