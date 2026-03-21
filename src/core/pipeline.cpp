// ============================================================================
// APEX-DB: End-to-End Integration Pipeline — Implementation
// ============================================================================
// Layer 1 (Storage) + Layer 2 (Ingestion) + Layer 3 (Execution) 통합
//
// 아키텍처:
//   외부 -> ingest_tick() -> TickPlant (MPMC Queue)
//                              |
//                         [drain_thread]
//                              |
//                         store_tick() -> PartitionManager -> ColumnVectors
//                                                                 |
//                    query_vwap() / query_filter_sum() -----------+
//                              -> VectorizedEngine (벡터화 연산)
// ============================================================================

#include "apex/core/pipeline.h"
#include "apex/common/logger.h"

#include <algorithm>
#include <chrono>

namespace apex::core {

// ============================================================================
// 스키마 상수: 파티션에 생성할 컬럼 이름
// ============================================================================
static constexpr const char* COL_TIMESTAMP = "timestamp";
static constexpr const char* COL_PRICE     = "price";
static constexpr const char* COL_VOLUME    = "volume";
static constexpr const char* COL_MSG_TYPE  = "msg_type";

// ============================================================================
// 내부 헬퍼: 고해상도 타이머 (nanosecond)
// ============================================================================
static inline int64_t pipeline_now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::high_resolution_clock::now().time_since_epoch()
    ).count();
}

// ============================================================================
// 생성자 / 소멸자
// ============================================================================
ApexPipeline::ApexPipeline(PipelineConfig config)
    : config_(config)
    , partition_mgr_(config.arena_size_per_partition)
{
    APEX_INFO("ApexPipeline 초기화 (arena={}MB, batch={})",
              config.arena_size_per_partition / (1024*1024),
              config.drain_batch_size);
}

ApexPipeline::~ApexPipeline() {
    if (running_.load(std::memory_order_acquire)) {
        stop();
    }
}

// ============================================================================
// start / stop
// ============================================================================
void ApexPipeline::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true,
                                          std::memory_order_release,
                                          std::memory_order_relaxed)) {
        APEX_WARN("ApexPipeline::start() — 이미 실행 중");
        return;
    }

    drain_thread_ = std::thread([this]() { drain_loop(); });
    APEX_INFO("ApexPipeline 시작 완료");
}

void ApexPipeline::stop() {
    running_.store(false, std::memory_order_release);
    if (drain_thread_.joinable()) {
        drain_thread_.join();
    }

    // 남은 큐 아이템 동기 플러시
    const size_t remaining = drain_sync();
    APEX_INFO("ApexPipeline 중지 (잔여 flush={})", remaining);
}

// ============================================================================
// ingest_tick: 외부 틱 수신 (Thread-safe, lock-free)
// ============================================================================
bool ApexPipeline::ingest_tick(TickMessage msg) {
    const int64_t t0 = pipeline_now_ns();
    const bool ok = tick_plant_.ingest(std::move(msg));
    if (ok) {
        stats_.ticks_ingested.fetch_add(1, std::memory_order_relaxed);
    } else {
        stats_.ticks_dropped.fetch_add(1, std::memory_order_relaxed);
    }
    stats_.last_ingest_latency_ns.store(
        pipeline_now_ns() - t0, std::memory_order_relaxed);
    return ok;
}

// ============================================================================
// store_tick: 틱 → ColumnStore 저장 (드레인 스레드에서만 호출)
// ============================================================================
void ApexPipeline::store_tick(const TickMessage& msg) {
    // 파티션 가져오기 (없으면 자동 생성)
    Partition& partition = partition_mgr_.get_or_create(msg.symbol_id, msg.recv_ts);

    // 파티션 최초 접근 시 스키마 초기화
    if (partition.get_column(COL_TIMESTAMP) == nullptr) {
        partition.add_column(COL_TIMESTAMP, ColumnType::TIMESTAMP_NS);
        partition.add_column(COL_PRICE,     ColumnType::INT64);
        partition.add_column(COL_VOLUME,    ColumnType::INT64);
        partition.add_column(COL_MSG_TYPE,  ColumnType::INT32);

        // partition_index_ 업데이트
        {
            std::lock_guard<std::mutex> lk(partition_index_mu_);
            partition_index_[msg.symbol_id].push_back(&partition);
        }

        stats_.partitions_created.fetch_add(1, std::memory_order_relaxed);
        APEX_DEBUG("파티션 스키마 초기화: symbol={}", msg.symbol_id);
    }

    // 컬럼에 데이터 append
    partition.get_column(COL_TIMESTAMP)->append<int64_t>(msg.recv_ts);
    partition.get_column(COL_PRICE    )->append<int64_t>(msg.price);
    partition.get_column(COL_VOLUME   )->append<int64_t>(msg.volume);
    partition.get_column(COL_MSG_TYPE )->append<int32_t>(
        static_cast<int32_t>(msg.msg_type));

    stats_.ticks_stored.fetch_add(1, std::memory_order_relaxed);
}

// ============================================================================
// drain_loop: 백그라운드 드레인 스레드
// ============================================================================
void ApexPipeline::drain_loop() {
    APEX_DEBUG("드레인 스레드 시작");
    while (running_.load(std::memory_order_acquire)) {
        size_t drained = 0;
        for (size_t i = 0; i < config_.drain_batch_size; ++i) {
            auto msg = tick_plant_.consume();
            if (!msg.has_value()) break;
            store_tick(*msg);
            ++drained;
        }

        if (drained == 0) {
            std::this_thread::sleep_for(
                std::chrono::microseconds(config_.drain_sleep_us));
        }
    }
    APEX_DEBUG("드레인 스레드 종료");
}

// ============================================================================
// drain_sync: 동기 드레인 (테스트/벤치용)
// ============================================================================
size_t ApexPipeline::drain_sync(size_t max_items) {
    size_t count = 0;
    while (count < max_items) {
        auto msg = tick_plant_.consume();
        if (!msg.has_value()) break;
        store_tick(*msg);
        ++count;
    }
    return count;
}

// ============================================================================
// find_partitions: symbol에 대한 모든 파티션 포인터 반환
// ============================================================================
std::vector<Partition*> ApexPipeline::find_partitions(SymbolId symbol) const {
    std::lock_guard<std::mutex> lk(partition_index_mu_);
    auto it = partition_index_.find(symbol);
    if (it == partition_index_.end()) return {};
    return it->second;
}

// ============================================================================
// build_snapshot: 파티션에서 ColumnSnapshot 빌드
// ============================================================================
ApexPipeline::ColumnSnapshot ApexPipeline::build_snapshot(
    Partition* part, const std::string& extra_col_name
) const {
    ColumnSnapshot snap;

    auto* ts_col  = part->get_column(COL_TIMESTAMP);
    auto* px_col  = part->get_column(COL_PRICE);
    auto* vol_col = part->get_column(COL_VOLUME);
    if (!ts_col || !px_col || !vol_col) return snap;

    snap.count      = ts_col->size();
    snap.timestamps = static_cast<const int64_t*>(ts_col->raw_data());
    snap.prices     = static_cast<const int64_t*>(px_col->raw_data());
    snap.volumes    = static_cast<const int64_t*>(vol_col->raw_data());

    if (!extra_col_name.empty()) {
        auto* col = part->get_column(extra_col_name);
        if (col) {
            snap.extra_col = static_cast<const int64_t*>(col->raw_data());
        }
    }

    return snap;
}

// ============================================================================
// query_vwap: VWAP 쿼리
// ============================================================================
QueryResult ApexPipeline::query_vwap(
    SymbolId symbol, Timestamp from, Timestamp to
) {
    const int64_t t0 = pipeline_now_ns();

    const auto partitions = find_partitions(symbol);
    if (partitions.empty()) {
        return QueryResult{
            .type = QueryResult::Type::ERROR,
            .error_msg = "no data for symbol"
        };
    }

    __int128 pv_sum    = 0;
    int64_t  v_sum     = 0;
    size_t   total_rows = 0;

    const bool full_scan = (from == 0 && to == INT64_MAX);

    for (Partition* part : partitions) {
        const auto snap = build_snapshot(part, "");
        if (snap.count == 0) continue;

        // 파티션 전체가 time range 밖이면 skip
        if (!full_scan) {
            if (snap.timestamps[snap.count - 1] < from) continue;
            if (snap.timestamps[0] > to) continue;
        }

        if (full_scan) {
            // 최적화 경로: 전체 스캔
            // 컴파일러가 자동 벡터화 (auto-vectorization)
            for (size_t i = 0; i < snap.count; ++i) {
                pv_sum += static_cast<__int128>(snap.prices[i]) * snap.volumes[i];
                v_sum  += snap.volumes[i];
            }
            total_rows += snap.count;
        } else {
            // Time range 필터 경로
            for (size_t i = 0; i < snap.count; ++i) {
                if (snap.timestamps[i] >= from && snap.timestamps[i] <= to) {
                    pv_sum += static_cast<__int128>(snap.prices[i]) * snap.volumes[i];
                    v_sum  += snap.volumes[i];
                    ++total_rows;
                }
            }
        }
    }

    QueryResult r;
    r.type = QueryResult::Type::VWAP;
    r.value = (v_sum == 0) ? 0.0
              : static_cast<double>(pv_sum) / static_cast<double>(v_sum);
    r.rows_scanned = total_rows;
    r.latency_ns   = pipeline_now_ns() - t0;

    stats_.queries_executed.fetch_add(1, std::memory_order_relaxed);
    stats_.total_rows_scanned.fetch_add(total_rows, std::memory_order_relaxed);

    return r;
}

// ============================================================================
// query_filter_sum: Filter + Sum 쿼리
// ============================================================================
QueryResult ApexPipeline::query_filter_sum(
    SymbolId symbol,
    const std::string& column,
    int64_t threshold,
    Timestamp from,
    Timestamp to
) {
    const int64_t t0 = pipeline_now_ns();

    const auto partitions = find_partitions(symbol);
    if (partitions.empty()) {
        return QueryResult{
            .type = QueryResult::Type::ERROR,
            .error_msg = "no data for symbol"
        };
    }

    int64_t total_sum  = 0;
    size_t  total_rows = 0;

    const bool full_scan = (from == 0 && to == INT64_MAX);

    // SelectionVector: DATABLOCK_ROWS 크기
    SelectionVector sel(DATABLOCK_ROWS);

    for (Partition* part : partitions) {
        const auto snap = build_snapshot(part, column);
        if (snap.count == 0) continue;

        // 쿼리 컬럼 결정 (price or volume or extra)
        const int64_t* col_data = nullptr;
        if (column == COL_PRICE) {
            col_data = snap.prices;
        } else if (column == COL_VOLUME) {
            col_data = snap.volumes;
        } else if (snap.extra_col) {
            col_data = snap.extra_col;
        } else {
            col_data = snap.prices; // fallback
        }

        if (!col_data) continue;

        if (full_scan) {
            // 벡터화 블록 처리 (8192 row chunks)
            size_t offset = 0;
            while (offset < snap.count) {
                const size_t block = std::min(DATABLOCK_ROWS, snap.count - offset);
                filter_gt_i64(col_data + offset, block, threshold, sel);
                total_sum  += sum_i64_selected(col_data + offset, sel);
                total_rows += sel.size();
                offset     += block;
            }
        } else {
            // Time range 적용
            for (size_t i = 0; i < snap.count; ++i) {
                if (snap.timestamps[i] >= from && snap.timestamps[i] <= to) {
                    if (col_data[i] > threshold) {
                        total_sum += col_data[i];
                        ++total_rows;
                    }
                }
            }
        }
    }

    QueryResult r;
    r.type     = QueryResult::Type::SUM;
    r.ivalue   = total_sum;
    r.value    = static_cast<double>(total_sum);
    r.rows_scanned = total_rows;
    r.latency_ns   = pipeline_now_ns() - t0;

    stats_.queries_executed.fetch_add(1, std::memory_order_relaxed);
    stats_.total_rows_scanned.fetch_add(total_rows, std::memory_order_relaxed);

    return r;
}

// ============================================================================
// query_count
// ============================================================================
QueryResult ApexPipeline::query_count(
    SymbolId symbol, Timestamp from, Timestamp to
) {
    const int64_t t0 = pipeline_now_ns();

    const auto partitions = find_partitions(symbol);
    size_t total = 0;

    const bool full_scan = (from == 0 && to == INT64_MAX);

    for (Partition* part : partitions) {
        const auto snap = build_snapshot(part, "");
        if (snap.count == 0) continue;

        if (full_scan) {
            total += snap.count;
        } else {
            for (size_t i = 0; i < snap.count; ++i) {
                if (snap.timestamps[i] >= from && snap.timestamps[i] <= to) {
                    ++total;
                }
            }
        }
    }

    QueryResult r;
    r.type         = QueryResult::Type::COUNT;
    r.ivalue       = static_cast<int64_t>(total);
    r.value        = static_cast<double>(total);
    r.rows_scanned = total;
    r.latency_ns   = pipeline_now_ns() - t0;

    stats_.queries_executed.fetch_add(1, std::memory_order_relaxed);
    stats_.total_rows_scanned.fetch_add(total, std::memory_order_relaxed);

    return r;
}

// ============================================================================
// total_stored_rows
// ============================================================================
size_t ApexPipeline::total_stored_rows() const {
    std::lock_guard<std::mutex> lk(partition_index_mu_);
    size_t total = 0;
    for (const auto& [sym, parts] : partition_index_) {
        for (const Partition* p : parts) {
            total += p->num_rows();
        }
    }
    return total;
}

} // namespace apex::core
