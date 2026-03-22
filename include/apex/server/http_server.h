#pragma once
// ============================================================================
// APEX-DB: HTTP API Server
// ============================================================================
// cpp-httplib 기반 경량 HTTP 서버
// ClickHouse 호환 포트 8123 — Grafana/클라이언트 연동 가능
//
// Endpoints:
//   POST /        — SQL 쿼리 실행 (body: SQL 문자열)
//   GET  /ping    — 헬스체크
//   GET  /stats   — 파이프라인 통계
// ============================================================================

#include "apex/sql/executor.h"
#include <cstdint>
#include <string>
#include <memory>
#include <thread>
#include <atomic>

// httplib 포워드 선언 (include를 cpp에서만 함 — 컴파일 속도)
namespace httplib { class Server; }

namespace apex::server {

// ============================================================================
// HttpServer: HTTP API 서버
// ============================================================================
class HttpServer {
public:
    explicit HttpServer(apex::sql::QueryExecutor& executor,
                        uint16_t port = 8123);
    ~HttpServer();

    // Non-copyable
    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;

    /// 블로킹 실행 (메인 스레드 사용)
    void start();

    /// 백그라운드 스레드로 실행
    void start_async();

    /// 서버 중지
    void stop();

    /// 현재 포트
    uint16_t port() const { return port_; }

    /// 실행 중 여부
    bool running() const { return running_.load(); }

private:
    // 라우트 등록
    void setup_routes();

    // 응답 JSON 빌더
    static std::string build_json_response(
        const apex::sql::QueryResultSet& result);

    static std::string build_error_json(const std::string& msg);
    static std::string build_stats_json(
        const apex::core::PipelineStats& stats);

    apex::sql::QueryExecutor& executor_;
    uint16_t                  port_;
    std::unique_ptr<httplib::Server> svr_;
    std::thread               thread_;
    std::atomic<bool>         running_{false};
};

} // namespace apex::server
