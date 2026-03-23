#pragma once
// ============================================================================
// APEX-DB: HTTP API Server
// ============================================================================
// cpp-httplib based lightweight HTTP/HTTPS server.
// ClickHouse compatible port 8123 — works with Grafana/CH clients.
//
// Endpoints:
//   POST /        — Execute SQL query (body: SQL string)
//   GET  /ping    — Health check (ClickHouse compatible)
//   GET  /health  — Kubernetes liveness probe
//   GET  /ready   — Kubernetes readiness probe
//   GET  /stats   — Pipeline statistics
//   GET  /metrics — Prometheus metrics (OpenMetrics format)
//
// Security:
//   TLS/HTTPS     — Enabled via TlsConfig (cert + key PEM paths)
//   Authentication — API Key (Bearer) or JWT/SSO (OIDC)
//   Authorization  — RBAC: admin/writer/reader/analyst/metrics roles
// ============================================================================

#include "apex/sql/executor.h"
#include "apex/auth/auth_manager.h"
#include "apex/auth/query_tracker.h"
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <memory>
#include <thread>
#include <atomic>
#include <unordered_map>
#include <vector>

// Forward declaration — httplib is included only in the .cpp (compile speed)
namespace httplib { class Server; }

namespace apex::server {

// ============================================================================
// ConnectionInfo — active client session tracking (.z.po/.z.pc equivalent)
// ============================================================================
struct ConnectionInfo {
    std::string remote_addr;        // IP:port of the client
    std::string user;               // subject from auth (or remote_addr if no auth)
    int64_t     connected_at_ns = 0; // epoch-ns of first request
    int64_t     last_active_ns  = 0; // epoch-ns of most recent request
    uint64_t    query_count     = 0; // number of requests from this session
};

// ============================================================================
// HttpServer
// ============================================================================
class HttpServer {
public:
    /// Construct without authentication (development / trusted network mode).
    explicit HttpServer(apex::sql::QueryExecutor& executor,
                        uint16_t port = 8123);

    /// Construct with TLS and/or authentication.
    explicit HttpServer(apex::sql::QueryExecutor& executor,
                        uint16_t port,
                        apex::auth::TlsConfig tls,
                        std::shared_ptr<apex::auth::AuthManager> auth = nullptr);

    ~HttpServer();

    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;

    /// Start blocking (uses calling thread).
    void start();

    /// Start on a background thread.
    void start_async();

    /// Stop the server.
    void stop();

    uint16_t port()    const { return port_; }
    bool     running() const { return running_.load(); }

    /// Mark the server as ready (called after pipeline initialization).
    void set_ready(bool ready) { ready_.store(ready); }

    /// Set query execution timeout (0 = disabled).
    void set_query_timeout_ms(uint32_t ms) { query_timeout_ms_ = ms; }

    // ---- Connection hooks (.z.po / .z.pc equivalent) ----------------------

    /// Called when a new client session is detected (first request from addr).
    void set_on_connect(std::function<void(const ConnectionInfo&)> fn) {
        on_connect_ = std::move(fn);
    }

    /// Called when a session ends (Connection: close detected from client).
    void set_on_disconnect(std::function<void(const ConnectionInfo&)> fn) {
        on_disconnect_ = std::move(fn);
    }

    /// Return snapshot of all currently tracked sessions.
    std::vector<ConnectionInfo> list_sessions() const;

    /// Evict sessions that have been inactive for longer than timeout_ms.
    /// Returns the number of sessions evicted.
    size_t evict_idle_sessions(int64_t timeout_ms);

    // ---- Extensible /metrics providers ------------------------------------

    /// Register a callback that contributes additional Prometheus/OpenMetrics
    /// text to the GET /metrics response.  The function must return a
    /// newline-terminated string in OpenMetrics text format.
    ///
    /// Example (Kafka consumer):
    ///   server.add_metrics_provider([&consumer]() {
    ///       return apex::feeds::KafkaConsumer::format_prometheus(
    ///           "market-data", consumer.stats());
    ///   });
    ///
    /// Thread-safe: providers are called while holding an internal mutex.
    void add_metrics_provider(std::function<std::string()> provider);

private:
    void setup_routes();
    void setup_auth_middleware();
    void setup_admin_routes();
    void setup_session_tracking();

    // Track a request from remote_addr; fires on_connect_ / on_disconnect_.
    // is_closing=true when client sends "Connection: close".
    void track_session(const std::string& remote_addr, bool is_closing);

    // Execute a query with optional timeout and QueryTracker registration.
    // subject is the identity string (remote_addr when auth is off).
    apex::sql::QueryResultSet run_query_with_tracking(
        const std::string& sql, const std::string& subject);

    static std::string build_json_response(
        const apex::sql::QueryResultSet& result);
    static std::string build_error_json(const std::string& msg);
    static std::string build_stats_json(
        const apex::core::PipelineStats& stats);
    std::string build_prometheus_metrics() const;

    apex::sql::QueryExecutor&                executor_;
    uint16_t                                 port_;
    apex::auth::TlsConfig                    tls_;
    std::shared_ptr<apex::auth::AuthManager> auth_;
    std::unique_ptr<httplib::Server>         svr_;
    std::thread                              thread_;
    std::atomic<bool>                        running_{false};
    std::atomic<bool>                        ready_{false};
    uint32_t                                 query_timeout_ms_{0};
    apex::auth::QueryTracker                 query_tracker_;

    // Session registry
    mutable std::mutex                                     sessions_mu_;
    std::unordered_map<std::string, ConnectionInfo>        sessions_;
    std::function<void(const ConnectionInfo&)>             on_connect_;
    std::function<void(const ConnectionInfo&)>             on_disconnect_;

    // Extensible /metrics providers
    mutable std::mutex                                     providers_mu_;
    std::vector<std::function<std::string()>>              metrics_providers_;
};

} // namespace apex::server
