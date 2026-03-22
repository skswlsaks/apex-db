// ============================================================================
// APEX-DB: 금융 함수 테스트 — xbar, ema, delta/ratio, LEFT JOIN, WINDOW JOIN
// ============================================================================
// Part 7: 모든 새 기능에 대한 정확성 테스트
// ============================================================================

#include "apex/execution/window_function.h"
#include "apex/execution/join_operator.h"
#include "apex/sql/tokenizer.h"
#include "apex/sql/parser.h"
#include "apex/sql/executor.h"
#include "apex/core/pipeline.h"
#include "apex/storage/column_store.h"
#include "apex/storage/partition_manager.h"
#include "apex/storage/arena_allocator.h"
#include "apex/ingestion/tick_plant.h"

#include <gtest/gtest.h>
#include <vector>
#include <cmath>
#include <cstdint>
#include <climits>
#include <chrono>

using namespace apex::execution;
using namespace apex::sql;
using namespace apex::storage;
using namespace apex::core;

// ============================================================================
// 헬퍼: 간단한 파이프라인 생성
// ============================================================================
static std::unique_ptr<ApexPipeline> make_pipeline() {
    PipelineConfig cfg;
    cfg.storage_mode = StorageMode::PURE_IN_MEMORY;
    return std::make_unique<ApexPipeline>(cfg);
}

static void store_tick_direct(ApexPipeline& pipeline, int64_t sym, int64_t ts,
                               int64_t price, int64_t vol) {
    // partition_manager를 직접 사용하여 정확한 타임스탬프로 삽입
    // (ingest_tick/tick_plant는 recv_ts를 now()로 덮어씀)
    auto& pm = pipeline.partition_manager();
    auto& part = pm.get_or_create(static_cast<apex::SymbolId>(sym), ts);

    static const char* COL_TS  = "timestamp";
    static const char* COL_PR  = "price";
    static const char* COL_VOL = "volume";
    static const char* COL_MSG = "msg_type";

    if (part.get_column(COL_TS) == nullptr) {
        part.add_column(COL_TS,  apex::storage::ColumnType::TIMESTAMP_NS);
        part.add_column(COL_PR,  apex::storage::ColumnType::INT64);
        part.add_column(COL_VOL, apex::storage::ColumnType::INT64);
        part.add_column(COL_MSG, apex::storage::ColumnType::INT32);
        // partition_index 업데이트 — pipeline 내부가 알 수 있도록 ingest 1번
        // 실제로는 pm이 인덱스를 관리하지만 executor는 pm.get_all_partitions()를 씀
    }
    part.get_column(COL_TS )->append<int64_t>(ts);
    part.get_column(COL_PR )->append<int64_t>(price);
    part.get_column(COL_VOL)->append<int64_t>(vol);
    part.get_column(COL_MSG)->append<int32_t>(0);
}

static void insert_tick(ApexPipeline& pipeline, int64_t sym, int64_t ts,
                        int64_t price, int64_t vol) {
    apex::ingestion::TickMessage msg{};
    msg.symbol_id = static_cast<apex::SymbolId>(sym);
    msg.recv_ts   = ts;
    msg.price     = price;
    msg.volume    = vol;
    msg.msg_type  = 0;
    pipeline.ingest_tick(msg);
}

// ============================================================================
// Part 1: Tokenizer — 새 토큰 확인
// ============================================================================

TEST(Tokenizer, XbarKeyword) {
    Tokenizer tok;
    auto tokens = tok.tokenize("XBAR(timestamp, 300000000000)");
    EXPECT_EQ(tokens[0].type, TokenType::XBAR);
    EXPECT_EQ(tokens[1].type, TokenType::LPAREN);
}

TEST(Tokenizer, EmaKeyword) {
    Tokenizer tok;
    auto tokens = tok.tokenize("EMA(price, 0.1) OVER (ORDER BY timestamp)");
    EXPECT_EQ(tokens[0].type, TokenType::EMA);
}

TEST(Tokenizer, DeltaRatioKeywords) {
    Tokenizer tok;
    auto tokens = tok.tokenize("DELTA(price) OVER () RATIO(price) OVER ()");
    bool found_delta = false, found_ratio = false;
    for (const auto& t : tokens) {
        if (t.type == TokenType::DELTA) found_delta = true;
        if (t.type == TokenType::RATIO) found_ratio = true;
    }
    EXPECT_TRUE(found_delta);
    EXPECT_TRUE(found_ratio);
}

TEST(Tokenizer, WindowJoinKeyword) {
    Tokenizer tok;
    auto tokens = tok.tokenize("WINDOW JOIN quotes q ON");
    EXPECT_EQ(tokens[0].type, TokenType::WINDOW);
    EXPECT_EQ(tokens[1].type, TokenType::JOIN);
}

TEST(Tokenizer, ArithmeticMinus) {
    // t.timestamp - 5000000000 → IDENT, DOT, IDENT, MINUS, NUMBER
    Tokenizer tok;
    auto tokens = tok.tokenize("timestamp - 5000000000");
    // timestamp MINUS 5000000000 END
    ASSERT_GE(tokens.size(), 3u);
    EXPECT_EQ(tokens[0].type, TokenType::IDENT);  // timestamp
    EXPECT_EQ(tokens[1].type, TokenType::MINUS);
    EXPECT_EQ(tokens[2].type, TokenType::NUMBER);
    EXPECT_EQ(tokens[2].value, "5000000000");
}

TEST(Tokenizer, FirstLastKeywords) {
    Tokenizer tok;
    auto tokens = tok.tokenize("FIRST(price) LAST(price)");
    // FIRST ( price ) LAST ( price ) END
    // 0      1 2      3 4    5 6     7  8
    EXPECT_EQ(tokens[0].type, TokenType::FIRST);
    EXPECT_EQ(tokens[4].type, TokenType::LAST);
}

// ============================================================================
// Part 2: Parser — 새 SQL 구문 파싱
// ============================================================================

TEST(Parser, XbarSelect) {
    Parser p;
    auto stmt = p.parse(
        "SELECT XBAR(timestamp, 300000000000) AS bar FROM trades GROUP BY XBAR(timestamp, 300000000000)");
    ASSERT_FALSE(stmt.columns.empty());
    EXPECT_EQ(stmt.columns[0].agg, AggFunc::XBAR);
    EXPECT_EQ(stmt.columns[0].xbar_bucket, 300000000000LL);
    ASSERT_TRUE(stmt.group_by.has_value());
    EXPECT_EQ(stmt.group_by->columns[0], "timestamp");
    EXPECT_EQ(stmt.group_by->xbar_buckets[0], 300000000000LL);
}

TEST(Parser, EmaWindowFunction) {
    Parser p;
    auto stmt = p.parse(
        "SELECT EMA(price, 0.1) OVER (ORDER BY timestamp) AS ema_price FROM trades");
    ASSERT_FALSE(stmt.columns.empty());
    EXPECT_EQ(stmt.columns[0].window_func, WindowFunc::EMA);
    EXPECT_NEAR(stmt.columns[0].ema_alpha, 0.1, 1e-9);
}

TEST(Parser, EmaPeriodBased) {
    Parser p;
    auto stmt = p.parse(
        "SELECT EMA(price, 20) OVER (ORDER BY timestamp) AS ema20 FROM trades");
    ASSERT_FALSE(stmt.columns.empty());
    EXPECT_EQ(stmt.columns[0].window_func, WindowFunc::EMA);
    EXPECT_EQ(stmt.columns[0].ema_period, 20);
    // alpha = 2/(20+1) ≈ 0.0952
    EXPECT_NEAR(stmt.columns[0].ema_alpha, 2.0/21.0, 1e-9);
}

TEST(Parser, DeltaWindowFunction) {
    Parser p;
    auto stmt = p.parse(
        "SELECT DELTA(price) OVER (ORDER BY timestamp) AS price_delta FROM trades");
    EXPECT_EQ(stmt.columns[0].window_func, WindowFunc::DELTA);
    EXPECT_EQ(stmt.columns[0].column, "price");
}

TEST(Parser, RatioWindowFunction) {
    Parser p;
    auto stmt = p.parse(
        "SELECT RATIO(price) OVER (ORDER BY timestamp) AS price_ratio FROM trades");
    EXPECT_EQ(stmt.columns[0].window_func, WindowFunc::RATIO);
}

TEST(Parser, FirstLastAggregate) {
    Parser p;
    auto stmt = p.parse(
        "SELECT FIRST(price) AS open, LAST(price) AS close FROM trades GROUP BY symbol");
    EXPECT_EQ(stmt.columns[0].agg, AggFunc::FIRST);
    EXPECT_EQ(stmt.columns[1].agg, AggFunc::LAST);
}

TEST(Parser, LeftJoin) {
    Parser p;
    auto stmt = p.parse(
        "SELECT t.price, r.risk FROM trades t LEFT JOIN risk_factors r ON t.symbol = r.symbol");
    ASSERT_TRUE(stmt.join.has_value());
    EXPECT_EQ(stmt.join->type, JoinClause::Type::LEFT);
    ASSERT_FALSE(stmt.join->on_conditions.empty());
    EXPECT_EQ(stmt.join->on_conditions[0].left_col, "symbol");
}

TEST(Parser, WindowJoin) {
    Parser p;
    // WINDOW JOIN with time range
    auto stmt = p.parse(
        "SELECT t.price, wj_avg(q.bid) AS avg_bid FROM trades t "
        "WINDOW JOIN quotes q ON t.symbol = q.symbol "
        "AND q.timestamp BETWEEN t.timestamp - 5000000000 AND t.timestamp + 5000000000");
    ASSERT_TRUE(stmt.join.has_value());
    EXPECT_EQ(stmt.join->type, JoinClause::Type::WINDOW);
    // wj_avg column 확인
    bool found_wj = false;
    for (const auto& col : stmt.columns) {
        if (col.wj_agg != WJAggFunc::NONE) {
            found_wj = true;
            EXPECT_EQ(col.wj_agg, WJAggFunc::AVG);
        }
    }
    EXPECT_TRUE(found_wj);
}

// ============================================================================
// Part 3: EMA WindowFunction
// ============================================================================

TEST(WindowFunction, EMA_Basic) {
    // 수동 계산:
    // data = [10, 20, 30, 40, 50], alpha = 0.5
    // ema[0] = 10
    // ema[1] = 0.5*20 + 0.5*10 = 15
    // ema[2] = 0.5*30 + 0.5*15 = 22.5 → 22
    // ema[3] = 0.5*40 + 0.5*22.5 = 31.25 → 31
    // ema[4] = 0.5*50 + 0.5*31.25 = 40.625 → 40

    std::vector<int64_t> input = {10, 20, 30, 40, 50};
    std::vector<int64_t> output(5, 0);

    WindowEMA ema(0.5);
    WindowFrame frame;
    ema.compute(input.data(), 5, output.data(), frame, nullptr);

    EXPECT_EQ(output[0], 10);
    EXPECT_EQ(output[1], 15);  // (0.5*20 + 0.5*10) = 15 (정수 캐스팅)
    EXPECT_EQ(output[2], 22);  // (0.5*30 + 0.5*15) = 22.5 → 22
    EXPECT_EQ(output[3], 31);  // (0.5*40 + 0.5*22.5) = 31.25 → 31
    EXPECT_EQ(output[4], 40);  // (0.5*50 + 0.5*31.25) = 40.625 → 40
}

TEST(WindowFunction, EMA_Alpha01) {
    // alpha = 0.1 검증 (finance 전형적 값)
    // data = [100, 100, 100, 200]
    // ema[0] = 100
    // ema[1] = 0.1*100 + 0.9*100 = 100
    // ema[2] = 0.1*100 + 0.9*100 = 100
    // ema[3] = 0.1*200 + 0.9*100 = 110

    std::vector<int64_t> input  = {100, 100, 100, 200};
    std::vector<int64_t> output(4, 0);

    WindowEMA ema(0.1);
    WindowFrame frame;
    ema.compute(input.data(), 4, output.data(), frame, nullptr);

    EXPECT_EQ(output[0], 100);
    EXPECT_EQ(output[1], 100);
    EXPECT_EQ(output[2], 100);
    EXPECT_EQ(output[3], 110);
}

TEST(WindowFunction, EMA_WithPartition) {
    // 파티션 [1,1,2,2], 각각 독립적으로 EMA
    std::vector<int64_t> input  = {10, 20, 100, 200};
    std::vector<int64_t> pkeys  = {1,  1,  2,   2};
    std::vector<int64_t> output(4, 0);

    WindowEMA ema(0.5);
    WindowFrame frame;
    ema.compute(input.data(), 4, output.data(), frame, pkeys.data());

    // 파티션 1: [10, 20] → [10, 15]
    EXPECT_EQ(output[0], 10);
    EXPECT_EQ(output[1], 15);
    // 파티션 2: [100, 200] → [100, 150]
    EXPECT_EQ(output[2], 100);
    EXPECT_EQ(output[3], 150);
}

TEST(WindowFunction, EMA_LargeN_O_n) {
    // O(n) 성능 확인: 1M 행
    const size_t N = 1'000'000;
    std::vector<int64_t> input(N, 1000);
    std::vector<int64_t> output(N, 0);

    WindowEMA ema(0.1);
    WindowFrame frame;

    auto t0 = std::chrono::high_resolution_clock::now();
    ema.compute(input.data(), N, output.data(), frame, nullptr);
    auto t1 = std::chrono::high_resolution_clock::now();

    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    // 1M 행 EMA < 100ms (실제 수십 ms)
    EXPECT_LT(ms, 100.0) << "EMA 1M rows took " << ms << "ms";

    // 수렴값 확인: alpha=0.1, 모든 값 = 1000 → EMA = 1000
    EXPECT_EQ(output[N-1], 1000);
}

// ============================================================================
// Part 4: DELTA & RATIO WindowFunction
// ============================================================================

TEST(WindowFunction, Delta_Basic) {
    // delta[0] = data[0], delta[i] = data[i] - data[i-1]
    // data = [10, 15, 13, 20, 18]
    // expected: [10, 5, -2, 7, -2]

    std::vector<int64_t> input  = {10, 15, 13, 20, 18};
    std::vector<int64_t> output(5, 0);

    WindowDelta delta;
    WindowFrame frame;
    delta.compute(input.data(), 5, output.data(), frame, nullptr);

    EXPECT_EQ(output[0], 10);  // 첫 행: 원본
    EXPECT_EQ(output[1], 5);   // 15 - 10
    EXPECT_EQ(output[2], -2);  // 13 - 15
    EXPECT_EQ(output[3], 7);   // 20 - 13
    EXPECT_EQ(output[4], -2);  // 18 - 20
}

TEST(WindowFunction, Delta_WithPartition) {
    // 파티션 [A, A, B, B]
    std::vector<int64_t> input = {100, 110, 200, 205};
    std::vector<int64_t> pkeys = {1, 1, 2, 2};
    std::vector<int64_t> output(4, 0);

    WindowDelta delta;
    WindowFrame frame;
    delta.compute(input.data(), 4, output.data(), frame, pkeys.data());

    // 파티션 1: [100, 110] → [100, 10]
    EXPECT_EQ(output[0], 100);
    EXPECT_EQ(output[1], 10);
    // 파티션 2: [200, 205] → [200, 5]
    EXPECT_EQ(output[2], 200);
    EXPECT_EQ(output[3], 5);
}

TEST(WindowFunction, Ratio_Basic) {
    // ratio[0] = 1_000_000 (기준값, 6자리 고정소수)
    // ratio[i] = (data[i] / data[i-1]) * 1_000_000
    // data = [100, 200, 100, 150]
    // expected: [1000000, 2000000, 500000, 1500000]

    std::vector<int64_t> input  = {100, 200, 100, 150};
    std::vector<int64_t> output(4, 0);

    WindowRatio ratio;
    WindowFrame frame;
    ratio.compute(input.data(), 4, output.data(), frame, nullptr);

    EXPECT_EQ(output[0], 1'000'000LL);   // 기준: 1.0
    EXPECT_EQ(output[1], 2'000'000LL);   // 200/100 = 2.0
    EXPECT_EQ(output[2], 500'000LL);     // 100/200 = 0.5
    EXPECT_EQ(output[3], 1'500'000LL);   // 150/100 = 1.5
}

TEST(WindowFunction, Ratio_DivisionByZero) {
    std::vector<int64_t> input  = {0, 100, 0};
    std::vector<int64_t> output(3, 0);

    WindowRatio ratio;
    WindowFrame frame;
    ratio.compute(input.data(), 3, output.data(), frame, nullptr);

    // 첫 행 = 1_000_000, 두 번째 = 0/0 (0), 세 번째 = 0/100 = 0
    EXPECT_EQ(output[0], 1'000'000LL);
    EXPECT_EQ(output[1], 0);  // 분모 0: 결과 0
    EXPECT_EQ(output[2], 0);  // 0/100 = 0
}

// ============================================================================
// Part 5: HashJoin LEFT JOIN
// ============================================================================

TEST(HashJoin, LeftJoin_AllMatch) {
    ArenaAllocator al(ArenaConfig{.total_size = 1 << 20, .use_hugepages = false, .numa_node = -1});
    ArenaAllocator ar(ArenaConfig{.total_size = 1 << 20, .use_hugepages = false, .numa_node = -1});

    ColumnVector lk("id", ColumnType::INT64, al);
    ColumnVector rk("id", ColumnType::INT64, ar);

    lk.append<int64_t>(1); lk.append<int64_t>(2); lk.append<int64_t>(3);
    rk.append<int64_t>(1); rk.append<int64_t>(2); rk.append<int64_t>(3);

    HashJoinOperator hj(JoinType::LEFT);
    auto result = hj.execute(lk, rk);

    // 모두 매칭 → 3쌍
    EXPECT_EQ(result.match_count, 3u);
    for (size_t i = 0; i < result.match_count; ++i) {
        EXPECT_GE(result.right_indices[i], 0LL);
    }
}

TEST(HashJoin, LeftJoin_NoMatch) {
    ArenaAllocator al(ArenaConfig{.total_size = 1 << 20, .use_hugepages = false, .numa_node = -1});
    ArenaAllocator ar(ArenaConfig{.total_size = 1 << 20, .use_hugepages = false, .numa_node = -1});

    ColumnVector lk("id", ColumnType::INT64, al);
    ColumnVector rk("id", ColumnType::INT64, ar);

    // 왼쪽: [1, 2], 오른쪽: [3, 4] — 매칭 없음
    lk.append<int64_t>(1); lk.append<int64_t>(2);
    rk.append<int64_t>(3); rk.append<int64_t>(4);

    HashJoinOperator hj(JoinType::LEFT);
    auto result = hj.execute(lk, rk);

    // LEFT JOIN: 왼쪽 행 모두 포함, 오른쪽은 NULL (-1)
    EXPECT_EQ(result.match_count, 2u);
    EXPECT_EQ(result.left_indices[0], 0);
    EXPECT_EQ(result.right_indices[0], -1LL);  // NULL 센티넬
    EXPECT_EQ(result.left_indices[1], 1);
    EXPECT_EQ(result.right_indices[1], -1LL);
}

TEST(HashJoin, LeftJoin_PartialMatch) {
    ArenaAllocator al(ArenaConfig{.total_size = 1 << 20, .use_hugepages = false, .numa_node = -1});
    ArenaAllocator ar(ArenaConfig{.total_size = 1 << 20, .use_hugepages = false, .numa_node = -1});

    ColumnVector lk("id", ColumnType::INT64, al);
    ColumnVector rk("id", ColumnType::INT64, ar);

    // 왼쪽: [1, 2, 3], 오른쪽: [2] — 1,3은 NULL
    lk.append<int64_t>(1); lk.append<int64_t>(2); lk.append<int64_t>(3);
    rk.append<int64_t>(2);

    HashJoinOperator hj(JoinType::LEFT);
    auto result = hj.execute(lk, rk);

    // LEFT JOIN: 3개 행
    EXPECT_EQ(result.match_count, 3u);

    // 결과를 left_idx → right_idx 맵으로 변환
    std::unordered_map<int64_t, int64_t> match_map;
    for (size_t i = 0; i < result.match_count; ++i) {
        match_map[result.left_indices[i]] = result.right_indices[i];
    }

    EXPECT_EQ(match_map[0], -1LL);  // 왼쪽 idx 0 (value=1): no match → NULL
    EXPECT_EQ(match_map[1], 0LL);   // 왼쪽 idx 1 (value=2): 오른쪽 idx 0
    EXPECT_EQ(match_map[2], -1LL);  // 왼쪽 idx 2 (value=3): no match → NULL
}

TEST(HashJoin, InnerJoin_NoMatch) {
    // INNER JOIN은 이전과 동일 (매칭 없으면 제외)
    ArenaAllocator al(ArenaConfig{.total_size = 1 << 20, .use_hugepages = false, .numa_node = -1});
    ArenaAllocator ar(ArenaConfig{.total_size = 1 << 20, .use_hugepages = false, .numa_node = -1});

    ColumnVector lk("id", ColumnType::INT64, al);
    ColumnVector rk("id", ColumnType::INT64, ar);

    lk.append<int64_t>(1); lk.append<int64_t>(2);
    rk.append<int64_t>(3); rk.append<int64_t>(4);

    HashJoinOperator hj(JoinType::INNER);
    auto result = hj.execute(lk, rk);

    EXPECT_EQ(result.match_count, 0u);  // INNER JOIN: 매칭 없음
}

// ============================================================================
// Part 6: WindowJoinOperator
// ============================================================================

TEST(WindowJoin, BasicAvg) {
    // 왼쪽: 거래 시점 [100, 200, 300]
    // 오른쪽: 호가 시점 [90, 105, 195, 210, 295, 310], 값 [10, 20, 30, 40, 50, 60]
    // 윈도우: [t-15, t+15]
    // t=100: [90, 105] → avg(10,20) = 15
    // t=200: [195, 210] → avg(30,40) = 35
    // t=300: [295, 310] → avg(50,60) = 55

    std::vector<int64_t> lk = {1, 1, 1};
    std::vector<int64_t> rk = {1, 1, 1, 1, 1, 1};
    std::vector<int64_t> lt = {100, 200, 300};
    std::vector<int64_t> rt = {90, 105, 195, 210, 295, 310};
    std::vector<int64_t> rv = {10, 20, 30, 40, 50, 60};

    WindowJoinOperator wj(WJAggType::AVG, 15, 15);
    auto result = wj.execute(lk.data(), 3, rk.data(), 6, lt.data(), rt.data(), rv.data());

    ASSERT_EQ(result.agg_values.size(), 3u);
    EXPECT_EQ(result.agg_values[0], 15);  // avg(10, 20)
    EXPECT_EQ(result.agg_values[1], 35);  // avg(30, 40)
    EXPECT_EQ(result.agg_values[2], 55);  // avg(50, 60)
}

TEST(WindowJoin, CountAgg) {
    std::vector<int64_t> lk = {1};
    std::vector<int64_t> rk = {1, 1, 1, 1, 1};
    std::vector<int64_t> lt = {1000};
    std::vector<int64_t> rt = {990, 995, 1000, 1005, 1010};
    std::vector<int64_t> rv = {1, 1, 1, 1, 1};

    // 윈도우 [990, 1010] → 5개 모두
    WindowJoinOperator wj(WJAggType::COUNT, 10, 10);
    auto result = wj.execute(lk.data(), 1, rk.data(), 5, lt.data(), rt.data(), rv.data());

    ASSERT_EQ(result.match_counts.size(), 1u);
    EXPECT_EQ(result.match_counts[0], 5);
}

TEST(WindowJoin, NoMatchInWindow) {
    std::vector<int64_t> lk = {1};
    std::vector<int64_t> rk = {1};
    std::vector<int64_t> lt = {1000};
    std::vector<int64_t> rt = {1100};  // 윈도우 밖
    std::vector<int64_t> rv = {999};

    WindowJoinOperator wj(WJAggType::AVG, 10, 10);
    auto result = wj.execute(lk.data(), 1, rk.data(), 1, lt.data(), rt.data(), rv.data());

    EXPECT_EQ(result.agg_values[0], 0);
    EXPECT_EQ(result.match_counts[0], 0);
}

TEST(WindowJoin, DifferentSymbols) {
    // 심볼 필터링 테스트: 다른 심볼은 집계에 포함 안 됨
    std::vector<int64_t> lk = {1, 2};
    std::vector<int64_t> rk = {1, 2};
    std::vector<int64_t> lt = {100, 100};
    std::vector<int64_t> rt = {100, 100};
    std::vector<int64_t> rv = {10, 20};

    WindowJoinOperator wj(WJAggType::SUM, 50, 50);
    auto result = wj.execute(lk.data(), 2, rk.data(), 2, lt.data(), rt.data(), rv.data());

    // 심볼 1: right[0]=10, 심볼 2: right[1]=20
    EXPECT_EQ(result.agg_values[0], 10);
    EXPECT_EQ(result.agg_values[1], 20);
}

// ============================================================================
// Part 7: SQL Executor — xbar GROUP BY
// ============================================================================

TEST(XbarGroupBy, FiveMinBar) {
    // 5분봉 (300초 = 300_000_000_000 ns) OHLCV 테스트
    auto pipeline = make_pipeline();

    // 데이터 삽입: 5분 내 거래 10개 → 하나의 바
    const int64_t BAR = 300LL * 1'000'000'000LL;  // 5분 (ns)

    // 첫 번째 바: ts 0 ~ BAR-1, 두 번째 바: ts BAR ~ 2*BAR-1
    std::vector<int64_t> ts = {
        0*BAR/5,  1*BAR/5,  2*BAR/5,  3*BAR/5,  4*BAR/5,
        BAR,      BAR + 1,  BAR + 2,  BAR + 3,  BAR + 4
    };
    std::vector<int64_t> prices = {100, 110, 105, 115, 108,
                                    200, 210, 205, 215, 208};
    std::vector<int64_t> vols   = {10, 20, 30, 40, 50, 10, 20, 30, 40, 50};

    for (size_t i = 0; i < ts.size(); ++i) {
        store_tick_direct(*pipeline, 1, ts[i], prices[i], vols[i]);
    }

    // GROUP BY xbar(timestamp, BAR) → 2개 그룹
    QueryExecutor exec(*pipeline);
    auto result = exec.execute(
        "SELECT XBAR(timestamp, 300000000000) AS bar, "
        "FIRST(price) AS open, MAX(price) AS high, MIN(price) AS low, "
        "LAST(price) AS close, SUM(volume) AS vol "
        "FROM trades "
        "GROUP BY XBAR(timestamp, 300000000000)");

    ASSERT_TRUE(result.ok()) << result.error;
    // 2개 바 생성
    EXPECT_EQ(result.rows.size(), 2u) << "Expected 2 5-min bars";

    for (const auto& row : result.rows) {
        ASSERT_GE(row.size(), 6u);
        int64_t bar = row[0];
        EXPECT_TRUE(bar == 0LL || bar == BAR) << "Unexpected bar value: " << bar;
    }
}

TEST(XbarGroupBy, Tokenizer_Arithmetic) {
    // xbar 버킷 값이 큰 숫자도 정상 파싱되는지
    Tokenizer tok;
    auto tokens = tok.tokenize("300000000000");
    ASSERT_GE(tokens.size(), 1u);
    EXPECT_EQ(tokens[0].type, TokenType::NUMBER);
    EXPECT_EQ(tokens[0].value, "300000000000");
}

// ============================================================================
// Part 8: SQL Executor — EMA, DELTA, RATIO (window function via executor)
// ============================================================================

TEST(ExecutorWindowFunc, DeltaViaSQL) {
    auto pipeline = make_pipeline();

    for (int64_t i = 0; i < 5; ++i) {
        store_tick_direct(*pipeline, 1, i, (i + 1) * 10, 100);
    }

    QueryExecutor exec(*pipeline);
    auto result = exec.execute(
        "SELECT price, DELTA(price) OVER (ORDER BY timestamp) AS delta_price "
        "FROM trades WHERE symbol = 1");

    ASSERT_TRUE(result.ok()) << result.error;
    ASSERT_EQ(result.rows.size(), 5u);

    // 컬럼 인덱스 찾기
    int price_idx = -1, delta_idx = -1;
    for (size_t i = 0; i < result.column_names.size(); ++i) {
        if (result.column_names[i] == "price") price_idx = (int)i;
        if (result.column_names[i] == "delta_price") delta_idx = (int)i;
    }
    ASSERT_GE(price_idx, 0);
    ASSERT_GE(delta_idx, 0);

    EXPECT_EQ(result.rows[0][delta_idx], 10);  // 첫 행: 원본
    EXPECT_EQ(result.rows[1][delta_idx], 10);  // 20 - 10
    EXPECT_EQ(result.rows[2][delta_idx], 10);  // 30 - 20
    EXPECT_EQ(result.rows[3][delta_idx], 10);  // 40 - 30
    EXPECT_EQ(result.rows[4][delta_idx], 10);  // 50 - 40
}

// ============================================================================
// Part 9: SQL Executor — LEFT JOIN
// ============================================================================

TEST(ExecutorLeftJoin, PartialMatch) {
    // HashJoinOperator LEFT JOIN 직접 테스트
    ArenaAllocator al(ArenaConfig{.total_size = 1 << 20, .use_hugepages = false, .numa_node = -1});
    ArenaAllocator ar(ArenaConfig{.total_size = 1 << 20, .use_hugepages = false, .numa_node = -1});

    ColumnVector lk("sym", ColumnType::INT64, al);
    ColumnVector rk("sym", ColumnType::INT64, ar);

    // 왼쪽: [1, 2, 3], 오른쪽: [1, 3] (2 없음)
    lk.append<int64_t>(1); lk.append<int64_t>(2); lk.append<int64_t>(3);
    rk.append<int64_t>(1); rk.append<int64_t>(3);

    HashJoinOperator hj(JoinType::LEFT);
    auto result = hj.execute(lk, rk);

    EXPECT_EQ(result.match_count, 3u);  // 왼쪽 3행 모두 포함

    // 심볼 2 (left_idx=1)는 right_idx=-1
    bool found_null = false;
    for (size_t i = 0; i < result.match_count; ++i) {
        if (result.left_indices[i] == 1) {
            EXPECT_EQ(result.right_indices[i], -1LL) << "Symbol 2 should have NULL right";
            found_null = true;
        }
    }
    EXPECT_TRUE(found_null) << "Left join should have NULL row for unmatched left row";
}

// ============================================================================
// Part 10: 벤치마크 (선택적 — 느리면 --gtest_filter로 제외)
// ============================================================================

TEST(Benchmark, Xbar_1M_Rows) {
    const size_t N = 1'000'000;

    auto pipeline = make_pipeline();
    for (size_t i = 0; i < N; ++i) {
        store_tick_direct(*pipeline, 1,
                          static_cast<int64_t>(i) * 1'000'000'000LL,
                          10000 + (i % 1000), 100);
    }

    QueryExecutor exec(*pipeline);

    auto t0 = std::chrono::high_resolution_clock::now();
    auto result = exec.execute(
        "SELECT XBAR(timestamp, 300000000000) AS bar, "
        "COUNT(*) AS cnt, MIN(price) AS lo, MAX(price) AS hi "
        "FROM trades WHERE symbol = 1 "
        "GROUP BY XBAR(timestamp, 300000000000)");
    auto t1 = std::chrono::high_resolution_clock::now();

    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    ASSERT_TRUE(result.ok()) << result.error;
    EXPECT_GT(result.rows.size(), 0u);
    EXPECT_LT(ms, 10000.0) << "Xbar GROUP BY 1M rows took " << ms << "ms";

    printf("\n[BENCH] xbar GROUP BY 1M rows: %.2fms, %zu bars\n",
           ms, result.rows.size());
}

TEST(Benchmark, EMA_1M_Rows) {
    // EMA 1M 행 — O(n) 단일 패스 성능
    const size_t N = 1'000'000;
    std::vector<int64_t> input(N);
    std::vector<int64_t> output(N, 0);

    for (size_t i = 0; i < N; ++i) {
        input[i] = 10000 + static_cast<int64_t>(i % 1000);
    }

    WindowEMA ema(0.1);
    WindowFrame frame;

    auto t0 = std::chrono::high_resolution_clock::now();
    ema.compute(input.data(), N, output.data(), frame, nullptr);
    auto t1 = std::chrono::high_resolution_clock::now();

    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    EXPECT_LT(ms, 100.0) << "EMA 1M rows took " << ms << "ms";

    printf("\n[BENCH] EMA 1M rows: %.2fms\n", ms);
}

TEST(Benchmark, WindowJoin_100K_x_100K) {
    // Window JOIN: 100K trades × 100K quotes
    const size_t N = 100'000;

    std::vector<int64_t> lk(N, 1);  // 모두 심볼 1
    std::vector<int64_t> rk(N, 1);
    std::vector<int64_t> lt(N);
    std::vector<int64_t> rt(N);
    std::vector<int64_t> rv(N, 1000);

    for (size_t i = 0; i < N; ++i) {
        lt[i] = static_cast<int64_t>(i) * 1'000'000LL;  // 1ms 간격
        rt[i] = static_cast<int64_t>(i) * 1'000'000LL;
    }

    WindowJoinOperator wj(WJAggType::AVG, 5'000'000LL, 5'000'000LL);  // ±5ms

    auto t0 = std::chrono::high_resolution_clock::now();
    // 전체 left 배치 처리 (실제로는 per-row지만 성능 측정)
    size_t total_matches = 0;
    for (size_t li = 0; li < N; ++li) {
        auto res = wj.execute(lk.data() + li, 1, rk.data(), N,
                              lt.data() + li, rt.data(), rv.data());
        total_matches += res.match_counts[0];
    }
    auto t1 = std::chrono::high_resolution_clock::now();

    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    // 성능 목표: 10초 이내 (O(n log m))
    EXPECT_LT(ms, 10'000.0) << "Window JOIN 100K×100K took " << ms << "ms";

    printf("\n[BENCH] Window JOIN 100K×100K: %.2fms, total_matches=%zu\n",
           ms, total_matches);
}
