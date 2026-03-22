# ============================================================================
# APEX-DB Tests
# ============================================================================

# --- Unit Tests ---
add_executable(apex_tests
    unit/test_arena_allocator.cpp
    unit/test_column_store.cpp
    unit/test_ring_buffer.cpp
    unit/test_tick_plant.cpp
    unit/test_vectorized_engine.cpp
    unit/test_hdb.cpp
    unit/test_cluster.cpp
    unit/test_cxl_backend.cpp
    unit/test_sql.cpp
    unit/test_join_window.cpp
    unit/test_financial_functions.cpp
    unit/test_parallel.cpp
)
target_include_directories(apex_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/src/cluster   # shm_backend.h 접근
)
target_link_libraries(apex_tests PRIVATE
    apex_storage
    apex_ingestion
    apex_execution
    apex_core
    apex_cluster
    apex_sql
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(apex_tests)

# --- Feed Handler Tests ---
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/feeds/test_fix_parser.cpp")
    add_executable(test_feeds
        feeds/test_fix_parser.cpp
        feeds/test_nasdaq_itch.cpp
    )
    target_include_directories(test_feeds PRIVATE
        ${CMAKE_SOURCE_DIR}/include
    )
    target_link_libraries(test_feeds PRIVATE
        apex_feeds
        apex_common
        GTest::gtest_main
    )
    gtest_discover_tests(test_feeds)
endif()

# --- Feed Handler Benchmarks ---
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/feeds/benchmark_feed_handlers.cpp")
    add_executable(bench_feeds
        feeds/benchmark_feed_handlers.cpp
    )
    target_include_directories(bench_feeds PRIVATE
        ${CMAKE_SOURCE_DIR}/include
    )
    target_link_libraries(bench_feeds PRIVATE
        apex_feeds
        apex_common
        GTest::gtest_main
    )
    target_compile_options(bench_feeds PRIVATE -O3 -march=native)
endif()

# --- Migration Tests ---
add_executable(test_migration
    migration/test_q_to_sql.cpp
    migration/test_hdb_loader.cpp
    migration/test_clickhouse.cpp
    migration/test_duckdb.cpp
    migration/test_timescaledb.cpp
)
target_include_directories(test_migration PRIVATE
    ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(test_migration PRIVATE
    apex_migration
    apex_common
    GTest::gtest_main
)
gtest_discover_tests(test_migration)

