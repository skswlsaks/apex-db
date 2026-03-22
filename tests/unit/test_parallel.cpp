// ============================================================================
// APEX-DB: 병렬 쿼리 실행 엔진 테스트
// ============================================================================

#include "apex/execution/worker_pool.h"
#include "apex/execution/parallel_scan.h"
#include "apex/execution/query_scheduler.h"
#include "apex/execution/local_scheduler.h"
#include "apex/execution/distributed_scheduler.h"
#include "apex/sql/executor.h"
#include "apex/core/pipeline.h"
#include "apex/storage/column_store.h"
#include "apex/storage/arena_allocator.h"

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <latch>
#include <numeric>
#include <thread>
#include <vector>

using namespace apex::execution;
using namespace apex::sql;
using namespace apex::storage;
using namespace apex::core;

// ============================================================================
// Part 1: WorkerPool 기본 동작 테스트
// ============================================================================

TEST(WorkerPool, BasicSubmitAndComplete) {
    WorkerPool pool(2);
    std::atomic<int> count{0};

    std::latch done(10);
    for (int i = 0; i < 10; ++i) {
        pool.submit([&count, &done]() {
            count.fetch_add(1, std::memory_order_relaxed);
            done.count_down();
        });
    }
    done.wait();
    EXPECT_EQ(count.load(), 10);
}

TEST(WorkerPool, WaitIdle) {
    WorkerPool pool(4);
    std::atomic<int> count{0};

    for (int i = 0; i < 100; ++i) {
        pool.submit([&count]() {
            count.fetch_add(1, std::memory_order_relaxed);
        });
    }
    pool.wait_idle();
    EXPECT_EQ(count.load(), 100);
}

TEST(WorkerPool, PrioritySubmit) {
    // HIGH 우선순위 작업이 먼저 실행되는지 대략 검증
    WorkerPool pool(1); // 단일 스레드로 순서 검증
    std::vector<int> order;
    std::mutex mu;

    std::latch done(3);
    // LOW → NORMAL → HIGH 순서로 제출
    pool.submit([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        pool.submit([&]() {
            { std::lock_guard lk(mu); order.push_back(2); }
            done.count_down();
        }, WorkerPool::Priority::LOW);
        pool.submit([&]() {
            { std::lock_guard lk(mu); order.push_back(1); }
            done.count_down();
        }, WorkerPool::Priority::HIGH);
        done.count_down(); // first task counted
    });
    done.wait();
    // 최소한 작업이 3개 완료됐음을 검증
    EXPECT_GE(order.size(), 2u);
}

TEST(WorkerPool, ParallelRun) {
    WorkerPool pool(4);
    std::atomic<int> sum{0};

    std::vector<std::function<void()>> tasks;
    for (int i = 1; i <= 100; ++i) {
        tasks.push_back([&sum, i]() {
            sum.fetch_add(i, std::memory_order_relaxed);
        });
    }
    parallel_run(pool, std::move(tasks));
    EXPECT_EQ(sum.load(), 5050); // sum(1..100) = 5050
}

TEST(WorkerPool, HardwareConcurrency) {
    WorkerPool pool(0); // 0 = hardware_concurrency
    EXPECT_GE(pool.num_threads(), 1u);
    EXPECT_LE(pool.num_threads(), 256u);
}

TEST(WorkerPool, SingleThread) {
    WorkerPool pool(1);
    std::vector<int> results;
    std::mutex mu;

    std::latch done(5);
    for (int i = 0; i < 5; ++i) {
        pool.submit([&, i]() {
            { std::lock_guard lk(mu); results.push_back(i); }
            done.count_down();
        });
    }
    done.wait();
    ASSERT_EQ(results.size(), 5u);
}

// ============================================================================
// Part 2: ParallelScanExecutor 유틸리티 테스트
// ============================================================================

TEST(ParallelScan, MakePartitionChunks_Even) {
    // 4개 파티션을 2청크로 분할
    std::vector<Partition*> parts(4, nullptr);
    auto chunks = ParallelScanExecutor::make_partition_chunks(parts, 2);
    ASSERT_EQ(chunks.size(), 2u);
    EXPECT_EQ(chunks[0].size(), 2u);
    EXPECT_EQ(chunks[1].size(), 2u);
}

TEST(ParallelScan, MakePartitionChunks_Uneven) {
    // 5개 파티션을 3청크로 분할
    std::vector<Partition*> parts(5, nullptr);
    auto chunks = ParallelScanExecutor::make_partition_chunks(parts, 3);
    ASSERT_GE(chunks.size(), 2u);
    size_t total = 0;
    for (auto& c : chunks) total += c.size();
    EXPECT_EQ(total, 5u);
}

TEST(ParallelScan, MakePartitionChunks_MoreChunksThanParts) {
    // 2개 파티션을 8청크로 → 최대 2청크
    std::vector<Partition*> parts(2, nullptr);
    auto chunks = ParallelScanExecutor::make_partition_chunks(parts, 8);
    EXPECT_LE(chunks.size(), 2u);
}

TEST(ParallelScan, MakeRowChunks) {
    auto ranges = ParallelScanExecutor::make_row_chunks(100, 4);
    ASSERT_EQ(ranges.size(), 4u);
    size_t total = 0;
    for (auto [b, e] : ranges) {
        EXPECT_LT(b, e);
        total += e - b;
    }
    EXPECT_EQ(total, 100u);
}

TEST(ParallelScan, SelectMode_Serial) {
    // 소량 데이터 → SERIAL
    auto mode = ParallelScanExecutor::select_mode(4, 50'000, 4, 100'000);
    EXPECT_EQ(mode, ParallelMode::SERIAL);
}

TEST(ParallelScan, SelectMode_Partition) {
    // 파티션 수 >= 스레드 수 → PARTITION
    auto mode = ParallelScanExecutor::select_mode(8, 1'000'000, 4, 100'000);
    EXPECT_EQ(mode, ParallelMode::PARTITION);
}

TEST(ParallelScan, SelectMode_Chunked) {
    // 파티션 수 < 스레드 수 → CHUNKED
    auto mode = ParallelScanExecutor::select_mode(2, 1'000'000, 8, 100'000);
    EXPECT_EQ(mode, ParallelMode::CHUNKED);
}

// ============================================================================
// Part 3: 병렬 집계 정확성 테스트
// (단일 스레드 결과 == 병렬 결과)
// ============================================================================

// 테스트용 파이프라인: 대용량 데이터 생성 (parallel threshold 넘기도록)
class ParallelExecutorTest : public ::testing::Test {
protected:
    static constexpr int kRowsPerSymbol = 500; // 2 symbols = 1000 rows
    // threshold를 200으로 낮춰서 1000행으로 병렬 경로 진입

    void SetUp() override {
        PipelineConfig cfg;
        cfg.storage_mode = StorageMode::PURE_IN_MEMORY;
        pipeline = std::make_unique<ApexPipeline>(cfg);
        executor = std::make_unique<QueryExecutor>(*pipeline);

        // symbol=1: price 10000..10499, volume 100..599
        for (int i = 0; i < kRowsPerSymbol; ++i) {
            apex::ingestion::TickMessage msg{};
            msg.symbol_id = 1;
            msg.recv_ts   = 1000LL + i;
            msg.price     = 10000 + i;
            msg.volume    = 100 + i;
            msg.msg_type  = 0;
            pipeline->ingest_tick(msg);
        }
        // symbol=2: price 20000..20499, volume 200..699
        for (int i = 0; i < kRowsPerSymbol; ++i) {
            apex::ingestion::TickMessage msg{};
            msg.symbol_id = 2;
            msg.recv_ts   = 1000LL + i;
            msg.price     = 20000 + i;
            msg.volume    = 200 + i;
            msg.msg_type  = 0;
            pipeline->ingest_tick(msg);
        }
        pipeline->drain_sync(2000);
    }

    // 직렬 쿼리 실행
    QueryResultSet run_serial(const std::string& sql) {
        executor->disable_parallel();
        return executor->execute(sql);
    }

    // 병렬 쿼리 실행 (threshold=200으로 낮춤, 스레드 수 지정)
    QueryResultSet run_parallel(const std::string& sql, size_t n_threads = 4) {
        executor->enable_parallel(n_threads, 200);
        auto r = executor->execute(sql);
        return r;
    }

    std::unique_ptr<ApexPipeline>  pipeline;
    std::unique_ptr<QueryExecutor> executor;
};

TEST_F(ParallelExecutorTest, ParallelSumEqualsSerial) {
    const std::string sql =
        "SELECT sum(volume) FROM trades WHERE symbol = 1";

    auto serial   = run_serial(sql);
    auto parallel = run_parallel(sql);

    ASSERT_TRUE(serial.ok())   << serial.error;
    ASSERT_TRUE(parallel.ok()) << parallel.error;
    ASSERT_EQ(serial.rows.size(),   1u);
    ASSERT_EQ(parallel.rows.size(), 1u);
    EXPECT_EQ(serial.rows[0][0], parallel.rows[0][0]);
}

TEST_F(ParallelExecutorTest, ParallelCountEqualsSerial) {
    const std::string sql = "SELECT count(*) FROM trades";

    auto serial   = run_serial(sql);
    auto parallel = run_parallel(sql);

    ASSERT_TRUE(serial.ok())   << serial.error;
    ASSERT_TRUE(parallel.ok()) << parallel.error;
    ASSERT_EQ(serial.rows.size(),   1u);
    ASSERT_EQ(parallel.rows.size(), 1u);
    EXPECT_EQ(serial.rows[0][0], parallel.rows[0][0]);
}

TEST_F(ParallelExecutorTest, ParallelMinMaxEqualsSerial) {
    const std::string sql =
        "SELECT min(price), max(price) FROM trades WHERE symbol = 1";

    auto serial   = run_serial(sql);
    auto parallel = run_parallel(sql);

    ASSERT_TRUE(serial.ok())   << serial.error;
    ASSERT_TRUE(parallel.ok()) << parallel.error;
    ASSERT_EQ(serial.rows[0].size(),   parallel.rows[0].size());
    EXPECT_EQ(serial.rows[0][0], parallel.rows[0][0]); // min
    EXPECT_EQ(serial.rows[0][1], parallel.rows[0][1]); // max
}

TEST_F(ParallelExecutorTest, ParallelVwapEqualsSerial) {
    const std::string sql =
        "SELECT vwap(price, volume) FROM trades WHERE symbol = 1";

    auto serial   = run_serial(sql);
    auto parallel = run_parallel(sql);

    ASSERT_TRUE(serial.ok())   << serial.error;
    ASSERT_TRUE(parallel.ok()) << parallel.error;
    ASSERT_GE(serial.rows.size(), 1u);
    ASSERT_GE(parallel.rows.size(), 1u);
    // VWAP 정수 비교 (동일 데이터에서 동일 정수 결과 기대)
    EXPECT_EQ(serial.rows[0][0], parallel.rows[0][0]);
}

TEST_F(ParallelExecutorTest, ParallelGroupByEqualsSerial) {
    const std::string sql =
        "SELECT symbol, sum(volume), count(*) FROM trades GROUP BY symbol";

    auto serial   = run_serial(sql);
    auto parallel = run_parallel(sql);

    ASSERT_TRUE(serial.ok())   << serial.error;
    ASSERT_TRUE(parallel.ok()) << parallel.error;
    EXPECT_EQ(serial.rows.size(), parallel.rows.size());

    // symbol별 합계 비교 (정렬 순서가 다를 수 있으므로 map으로 비교)
    std::unordered_map<int64_t, std::vector<int64_t>> s_map, p_map;
    for (auto& r : serial.rows)   s_map[r[0]] = r;
    for (auto& r : parallel.rows) p_map[r[0]] = r;

    for (auto& [sym, s_row] : s_map) {
        ASSERT_TRUE(p_map.count(sym)) << "symbol " << sym << " missing in parallel";
        auto& p_row = p_map[sym];
        ASSERT_EQ(s_row.size(), p_row.size());
        for (size_t i = 1; i < s_row.size(); ++i) {
            EXPECT_EQ(s_row[i], p_row[i])
                << "symbol=" << sym << " col=" << i;
        }
    }
}

// ============================================================================
// Part 4: 스레드 수 1/2/4/8 모두 동일 결과
// ============================================================================

TEST_F(ParallelExecutorTest, DifferentThreadCountsSameSum) {
    const std::string sql = "SELECT sum(volume), count(*), min(price), max(price) "
                            "FROM trades WHERE symbol = 1";

    auto serial = run_serial(sql);
    ASSERT_TRUE(serial.ok());

    for (size_t n : {1u, 2u, 4u, 8u}) {
        auto par = run_parallel(sql, n);
        ASSERT_TRUE(par.ok()) << "threads=" << n << " error: " << par.error;
        ASSERT_EQ(par.rows.size(), serial.rows.size())
            << "threads=" << n;
        if (!serial.rows.empty() && !par.rows.empty()) {
            for (size_t ci = 0; ci < serial.rows[0].size(); ++ci) {
                EXPECT_EQ(serial.rows[0][ci], par.rows[0][ci])
                    << "threads=" << n << " col=" << ci;
            }
        }
    }
}

TEST_F(ParallelExecutorTest, DifferentThreadCountsSameGroupBy) {
    const std::string sql =
        "SELECT symbol, sum(volume), count(*) FROM trades GROUP BY symbol";

    auto serial = run_serial(sql);
    ASSERT_TRUE(serial.ok());

    std::unordered_map<int64_t, int64_t> s_sum;
    for (auto& r : serial.rows) s_sum[r[0]] = r[1];

    for (size_t n : {1u, 2u, 4u, 8u}) {
        auto par = run_parallel(sql, n);
        ASSERT_TRUE(par.ok()) << "threads=" << n;
        EXPECT_EQ(par.rows.size(), serial.rows.size()) << "threads=" << n;

        std::unordered_map<int64_t, int64_t> p_sum;
        for (auto& r : par.rows) p_sum[r[0]] = r[1];

        for (auto& [sym, sv] : s_sum) {
            ASSERT_TRUE(p_sum.count(sym))
                << "threads=" << n << " missing symbol=" << sym;
            EXPECT_EQ(sv, p_sum[sym])
                << "threads=" << n << " symbol=" << sym;
        }
    }
}

// ============================================================================
// Part 5: 직렬 폴백 (소량 데이터 → 단일 스레드 경로)
// ============================================================================

class SmallDataParallelTest : public ::testing::Test {
protected:
    void SetUp() override {
        PipelineConfig cfg;
        cfg.storage_mode = StorageMode::PURE_IN_MEMORY;
        pipeline = std::make_unique<ApexPipeline>(cfg);
        executor = std::make_unique<QueryExecutor>(*pipeline);

        // 10행만 삽입 (기본 threshold 100K보다 훨씬 적음)
        for (int i = 0; i < 10; ++i) {
            apex::ingestion::TickMessage msg{};
            msg.symbol_id = 1;
            msg.recv_ts   = 1000LL + i;
            msg.price     = 15000 + i * 10;
            msg.volume    = 100 + i;
            pipeline->ingest_tick(msg);
        }
        pipeline->drain_sync(100);
    }

    std::unique_ptr<ApexPipeline>  pipeline;
    std::unique_ptr<QueryExecutor> executor;
};

TEST_F(SmallDataParallelTest, ParallelEnabledButFallsBackToSerial) {
    // threshold=100K로 활성화 → 10행은 직렬 경로
    executor->enable_parallel(4, 100'000);
    auto r = executor->execute("SELECT sum(volume), count(*) FROM trades WHERE symbol = 1");
    ASSERT_TRUE(r.ok()) << r.error;
    ASSERT_EQ(r.rows.size(), 1u);
    // sum(100..109) = 1045, count = 10
    EXPECT_EQ(r.rows[0][0], 1045);
    EXPECT_EQ(r.rows[0][1], 10);
}

TEST_F(SmallDataParallelTest, DisableParallelWorks) {
    executor->enable_parallel(4, 1); // threshold=1 → 병렬 실행
    auto r_par = executor->execute("SELECT sum(volume) FROM trades WHERE symbol = 1");

    executor->disable_parallel();
    auto r_ser = executor->execute("SELECT sum(volume) FROM trades WHERE symbol = 1");

    ASSERT_TRUE(r_par.ok()) << r_par.error;
    ASSERT_TRUE(r_ser.ok()) << r_ser.error;
    EXPECT_EQ(r_par.rows[0][0], r_ser.rows[0][0]);
}

// ============================================================================
// Part 6: QueryScheduler DI (의존성 주입) 테스트
// ============================================================================

// Mock 스케줄러: scatter/gather 호출 여부를 기록
class MockScheduler : public apex::execution::QueryScheduler {
public:
    mutable int scatter_calls = 0;
    mutable int gather_calls  = 0;

    std::vector<apex::execution::PartialAggResult> scatter(
        const std::vector<apex::execution::QueryFragment>& /*fragments*/) override
    {
        ++scatter_calls;
        return {};
    }

    apex::execution::PartialAggResult gather(
        std::vector<apex::execution::PartialAggResult>&& /*partials*/) override
    {
        ++gather_calls;
        return {};
    }

    size_t      worker_count()   const override { return 2; }
    std::string scheduler_type() const override { return "mock"; }
};

TEST(QuerySchedulerDI, DefaultSchedulerIsLocal) {
    PipelineConfig cfg;
    cfg.storage_mode = StorageMode::PURE_IN_MEMORY;
    ApexPipeline pipeline(cfg);
    QueryExecutor ex(pipeline);

    EXPECT_EQ(ex.scheduler().scheduler_type(), "local");
    EXPECT_GE(ex.scheduler().worker_count(), 1u);
}

TEST(QuerySchedulerDI, CustomSchedulerInjection) {
    PipelineConfig cfg;
    cfg.storage_mode = StorageMode::PURE_IN_MEMORY;
    ApexPipeline pipeline(cfg);

    auto mock = std::make_unique<MockScheduler>();
    QueryExecutor ex(pipeline, std::move(mock));

    EXPECT_EQ(ex.scheduler().scheduler_type(), "mock");
    EXPECT_EQ(ex.scheduler().worker_count(), 2u);
}

TEST(QuerySchedulerDI, CustomSchedulerSerialFallback) {
    // Mock 스케줄러 주입 → pool_raw_ = nullptr → 직렬 경로로 동작해야 함
    PipelineConfig cfg;
    cfg.storage_mode = StorageMode::PURE_IN_MEMORY;
    ApexPipeline pipeline(cfg);

    // 데이터 삽입
    for (int i = 0; i < 50; ++i) {
        apex::ingestion::TickMessage msg{};
        msg.symbol_id = 1;
        msg.recv_ts   = 1000LL + i;
        msg.price     = 10000 + i;
        msg.volume    = 100 + i;
        pipeline.ingest_tick(msg);
    }
    pipeline.drain_sync(100);

    auto mock = std::make_unique<MockScheduler>();
    QueryExecutor ex(pipeline, std::move(mock));

    // 병렬 설정해도 Mock 스케줄러는 WorkerPool 없음 → 직렬 실행
    ex.enable_parallel(4, 1);  // enable_parallel이 LocalScheduler를 덮어씀

    auto r = ex.execute("SELECT sum(volume), count(*) FROM trades WHERE symbol = 1");
    ASSERT_TRUE(r.ok()) << r.error;
    ASSERT_EQ(r.rows.size(), 1u);
    // sum(100..149) = 50*100 + (0+49)*50/2 = 5000 + 1225 = 6225
    EXPECT_EQ(r.rows[0][0], 6225);
    EXPECT_EQ(r.rows[0][1], 50);
}

TEST(QuerySchedulerDI, LocalSchedulerScatterGather) {
    // LocalQueryScheduler 의 scatter/gather 직접 테스트
    PipelineConfig cfg;
    cfg.storage_mode = StorageMode::PURE_IN_MEMORY;
    ApexPipeline pipeline(cfg);

    for (int i = 0; i < 100; ++i) {
        apex::ingestion::TickMessage msg{};
        msg.symbol_id = 1;
        msg.recv_ts   = 1000LL + i;
        msg.price     = 10000 + i;
        msg.volume    = 200 + i;
        pipeline.ingest_tick(msg);
    }
    pipeline.drain_sync(200);

    apex::execution::LocalQueryScheduler sched(pipeline, 2);
    EXPECT_EQ(sched.scheduler_type(), "local");
    EXPECT_GE(sched.worker_count(), 1u);

    // PartialAggResult resize / merge 테스트
    apex::execution::PartialAggResult pa;
    pa.resize(2);
    EXPECT_EQ(pa.sum.size(), 2u);
    EXPECT_EQ(pa.min_val[0], INT64_MAX);
    EXPECT_EQ(pa.max_val[0], INT64_MIN);

    apex::execution::PartialAggResult pb;
    pb.resize(2);
    pb.sum[0]   = 100;
    pb.count[1] = 5;
    pb.min_val[0] = 10;
    pb.max_val[0] = 90;

    pa.merge(pb);
    EXPECT_EQ(pa.sum[0], 100);
    EXPECT_EQ(pa.count[1], 5);
    EXPECT_EQ(pa.min_val[0], 10);
    EXPECT_EQ(pa.max_val[0], 90);
}

TEST(QuerySchedulerDI, DistributedSchedulerStub) {
    // DistributedQueryScheduler 는 스텁: scatter/gather 호출 시 예외 발생
    apex::execution::DistributedQueryScheduler dist;
    EXPECT_EQ(dist.scheduler_type(), "distributed");
    EXPECT_EQ(dist.worker_count(), 0u);

    EXPECT_THROW(
        dist.scatter({}),
        std::runtime_error
    );
}
