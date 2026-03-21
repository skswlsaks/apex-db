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
)
target_link_libraries(apex_tests PRIVATE
    apex_storage
    apex_ingestion
    apex_execution
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(apex_tests)
