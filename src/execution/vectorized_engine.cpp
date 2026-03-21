// ============================================================================
// Layer 3: Vectorized Engine Implementation (Highway SIMD)
// ============================================================================

#include "apex/execution/vectorized_engine.h"
#include "apex/common/logger.h"

// Highway SIMD — will be used for vectorized filter/agg in Phase 2
// #include <hwy/highway.h>

namespace apex::execution {

// ============================================================================
// SelectionVector
// ============================================================================
SelectionVector::SelectionVector(size_t max_size)
    : indices_(std::make_unique<uint32_t[]>(max_size))
{
}

// ============================================================================
// Scalar fallback implementations (Highway SIMD versions below)
// ============================================================================

void filter_gt_i64(
    const int64_t* column_data,
    size_t num_rows,
    int64_t threshold,
    SelectionVector& result
) {
    result.reset();
    // TODO: Highway SIMD 최적화 (AVX-512 gather + mask compress)
    // 현재: scalar fallback
    for (size_t i = 0; i < num_rows; ++i) {
        if (column_data[i] > threshold) {
            result.add(static_cast<uint32_t>(i));
        }
    }
}

int64_t sum_i64(const int64_t* column_data, size_t num_rows) {
    // TODO: Highway SIMD 벡터화 (HWY_NAMESPACE::ReduceSum)
    int64_t total = 0;
    for (size_t i = 0; i < num_rows; ++i) {
        total += column_data[i];
    }
    return total;
}

int64_t sum_i64_selected(
    const int64_t* column_data,
    const SelectionVector& selection
) {
    int64_t total = 0;
    for (size_t i = 0; i < selection.size(); ++i) {
        total += column_data[selection[i]];
    }
    return total;
}

double vwap(
    const int64_t* prices,
    const int64_t* volumes,
    size_t num_rows
) {
    // VWAP = Σ(price × volume) / Σ(volume)
    // TODO: Highway SIMD MulAdd 파이프라인
    __int128 pv_sum = 0;
    int64_t  v_sum  = 0;
    for (size_t i = 0; i < num_rows; ++i) {
        pv_sum += static_cast<__int128>(prices[i]) * volumes[i];
        v_sum  += volumes[i];
    }
    if (v_sum == 0) return 0.0;
    return static_cast<double>(pv_sum) / static_cast<double>(v_sum);
}

} // namespace apex::execution
