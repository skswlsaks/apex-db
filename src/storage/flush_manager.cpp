// ============================================================================
// Layer 1: FlushManager — 구현
// ============================================================================

#include "apex/storage/flush_manager.h"

#include <bit>
#include <chrono>

namespace apex::storage {

// ============================================================================
// 생성자 / 소멸자
// ============================================================================
FlushManager::FlushManager(PartitionManager& pm,
                            HDBWriter&         writer,
                            FlushConfig        config)
    : pm_(pm)
    , writer_(writer)
    , config_(config)
{
    APEX_INFO("FlushManager 초기화: threshold={:.0f}%, interval={}ms, compression={}",
              config_.memory_threshold * 100.0,
              config_.check_interval_ms,
              config_.enable_compression ? "LZ4" : "OFF");
}

FlushManager::~FlushManager() {
    if (running_.load(std::memory_order_acquire)) {
        stop();
    }
}

// ============================================================================
// start / stop
// ============================================================================
void FlushManager::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true,
                                          std::memory_order_release,
                                          std::memory_order_relaxed)) {
        APEX_WARN("FlushManager::start() — 이미 실행 중");
        return;
    }

    flush_thread_ = std::thread([this]() { flush_loop(); });
    APEX_INFO("FlushManager 백그라운드 스레드 시작");
}

void FlushManager::stop() {
    running_.store(false, std::memory_order_release);
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }
    APEX_INFO("FlushManager 중지 (총 플러시={}, 총 바이트={})",
              stat_partitions_flushed_.load(),
              stat_bytes_written_.load());
}

// ============================================================================
// flush_loop: 백그라운드 모니터링 루프
// ============================================================================
void FlushManager::flush_loop() {
    APEX_DEBUG("FlushManager 루프 시작");

    while (running_.load(std::memory_order_acquire)) {
        // 인터벌 대기 (1ms 단위 폴링으로 stop() 반응성 확보)
        const uint32_t total_ms = config_.check_interval_ms;
        uint32_t waited = 0;
        while (waited < total_ms && running_.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            ++waited;
        }

        if (!running_.load(std::memory_order_acquire)) break;

        // 현재 타임스탬프로 오래된 파티션 자동 봉인
        const int64_t current_ts = now_ns();
        pm_.seal_old_partitions(current_ts, config_.auto_seal_age_hours);

        // 메모리 압력 확인 후 플러시 여부 결정
        // PartitionManager에 직접 접근 불가하므로 HDBWriter stats 기반으로 판단
        // 실제 프로덕션에선 arena utilization을 집계해야 함
        // 여기서는 항상 SEALED 파티션을 플러시하는 단순 전략 사용
        const size_t flushed = do_flush_sealed();
        if (flushed > 0) {
            stat_flush_triggers_.fetch_add(1, std::memory_order_relaxed);
            APEX_DEBUG("FlushManager: 자동 플러시 {} 파티션", flushed);
        }
    }

    APEX_DEBUG("FlushManager 루프 종료");
}

// ============================================================================
// flush_now: 수동 즉시 플러시
// ============================================================================
size_t FlushManager::flush_now() {
    APEX_INFO("FlushManager::flush_now() 수동 트리거");
    const size_t flushed = do_flush_sealed();
    stat_manual_flushes_.fetch_add(1, std::memory_order_relaxed);
    return flushed;
}

// ============================================================================
// do_flush_sealed: 내부 공통 플러시 로직
// ============================================================================
size_t FlushManager::do_flush_sealed() {
    // PartitionManager에서 SEALED 파티션 목록 가져와서 처리
    // PartitionManager의 sealed_partitions()가 없으므로
    // seal_old_partitions(0, 0) 방식으로 전체 SEALED 확인은 불가능
    // → PartitionManager에 새 메서드 필요

    // SEALED 파티션 수집 (PartitionManager public API 활용)
    // 현재 API: seal_old_partitions()은 새로 봉인된 것만 반환
    // 기존에 SEALED된 것들은 partitions_ 맵을 직접 순회해야 함
    // → 여기서는 get_sealed_partitions() 확장 메서드를 사용
    auto sealed = pm_.get_sealed_partitions();

    size_t count = 0;
    for (Partition* part : sealed) {
        if (!part) continue;

        // FLUSHING 상태로 전이 (원자적이지 않지만 단일 flush 스레드)
        part->set_state(Partition::State::FLUSHING);

        const size_t bytes = writer_.flush_partition(*part);

        if (bytes > 0) {
            // 플러시 완료 → 아레나 회수
            part->set_state(Partition::State::FLUSHED);

            if (config_.reclaim_after_flush) {
                part->reclaim_arena();
            }

            stat_partitions_flushed_.fetch_add(1, std::memory_order_relaxed);
            stat_bytes_written_.fetch_add(bytes, std::memory_order_relaxed);
            stat_last_flush_ns_.store(now_ns(), std::memory_order_relaxed);
            ++count;
        } else {
            // 플러시 실패 → SEALED 상태로 복원 (재시도 가능)
            APEX_WARN("do_flush_sealed: 플러시 실패, SEALED 상태 복원");
            part->set_state(Partition::State::SEALED);
        }
    }

    return count;
}

// ============================================================================
// stats: 통계 스냅샷
// ============================================================================
FlushStats FlushManager::stats() const {
    FlushStats s;
    s.partitions_flushed  = stat_partitions_flushed_.load(std::memory_order_relaxed);
    s.total_bytes_written = stat_bytes_written_.load(std::memory_order_relaxed);
    s.flush_triggers      = stat_flush_triggers_.load(std::memory_order_relaxed);
    s.manual_flushes      = stat_manual_flushes_.load(std::memory_order_relaxed);
    s.last_flush_ns       = stat_last_flush_ns_.load(std::memory_order_relaxed);

    // double 비트캐스트 복원
    const uint64_t bits = stat_last_memory_ratio_bits_.load(std::memory_order_relaxed);
    s.last_memory_ratio = std::bit_cast<double>(bits);

    return s;
}

// ============================================================================
// now_ns: 현재 시간 (나노초)
// ============================================================================
int64_t FlushManager::now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

} // namespace apex::storage
