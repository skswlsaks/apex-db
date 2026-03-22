// ============================================================================
// APEX-DB: SQL Query Executor Implementation
// ============================================================================
// SelectStmt AST를 ApexPipeline API로 변환 실행
// ============================================================================

#include "apex/sql/executor.h"
#include "apex/sql/parser.h"
#include "apex/execution/join_operator.h"
#include "apex/execution/vectorized_engine.h"
#include "apex/common/logger.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <unordered_map>
#include <stdexcept>

namespace apex::sql {

using namespace apex::core;
using namespace apex::storage;
using namespace apex::execution;

// ============================================================================
// 고해상도 타이머
// ============================================================================
static inline double now_us() {
    return std::chrono::duration<double, std::micro>(
        std::chrono::high_resolution_clock::now().time_since_epoch()
    ).count();
}

// ============================================================================
// 생성자
// ============================================================================
QueryExecutor::QueryExecutor(ApexPipeline& pipeline)
    : pipeline_(pipeline) {}

const PipelineStats& QueryExecutor::stats() const {
    return pipeline_.stats();
}

// ============================================================================
// 메인 실행 진입점
// ============================================================================
QueryResultSet QueryExecutor::execute(const std::string& sql) {
    double t0 = now_us();
    try {
        Parser parser;
        SelectStmt stmt = parser.parse(sql);
        auto result = exec_select(stmt);
        result.execution_time_us = now_us() - t0;
        return result;
    } catch (const std::exception& e) {
        QueryResultSet err;
        err.error = e.what();
        err.execution_time_us = now_us() - t0;
        return err;
    }
}

// ============================================================================
// SELECT 실행 디스패처
// ============================================================================
QueryResultSet QueryExecutor::exec_select(const SelectStmt& stmt) {
    // WHERE symbol = N 조건 추출 (파티션 레벨 필터링)
    int64_t sym_filter = -1;
    if (has_where_symbol(stmt, sym_filter, stmt.from_alias)) {
        // symbol 기반 파티션 필터링
        auto& pm = pipeline_.partition_manager();
        auto left_parts = pm.get_partitions_for_symbol(
            static_cast<apex::SymbolId>(sym_filter));

        // ASOF JOIN
        if (stmt.join.has_value() && stmt.join->type == JoinClause::Type::ASOF) {
            auto right_parts = find_partitions(stmt.join->table);
            return exec_asof_join(stmt, left_parts, right_parts);
        }

        bool has_agg = false;
        for (const auto& col : stmt.columns) {
            if (col.agg != AggFunc::NONE) { has_agg = true; break; }
        }

        if (has_agg && stmt.group_by.has_value()) {
            return exec_group_agg(stmt, left_parts);
        } else if (has_agg) {
            return exec_agg(stmt, left_parts);
        } else {
            return exec_simple_select(stmt, left_parts);
        }
    }

    // 심볼 필터 없음 → 전체 파티션
    auto left_parts = find_partitions(stmt.from_table);

    // ASOF JOIN
    if (stmt.join.has_value() && stmt.join->type == JoinClause::Type::ASOF) {
        auto right_parts = find_partitions(stmt.join->table);
        return exec_asof_join(stmt, left_parts, right_parts);
    }

    // 집계 함수가 있는지 체크
    bool has_agg = false;
    for (const auto& col : stmt.columns) {
        if (col.agg != AggFunc::NONE) { has_agg = true; break; }
    }

    if (has_agg && stmt.group_by.has_value()) {
        return exec_group_agg(stmt, left_parts);
    } else if (has_agg) {
        return exec_agg(stmt, left_parts);
    } else {
        return exec_simple_select(stmt, left_parts);
    }
}

// ============================================================================
// 파티션 목록 조회 — WHERE symbol = N 조건이 있으면 해당 심볼 파티션만 반환
// ============================================================================
std::vector<Partition*> QueryExecutor::find_partitions(
    const std::string& /*table_name*/)
{
    auto& pm = pipeline_.partition_manager();

    // WHERE symbol = N 조건 추출 시도
    // (exec_select에서 stmt를 전달받지 않으므로 모든 파티션 반환 후
    //  symbol 필터링은 eval_where에서 처리)
    return pm.get_all_partitions();
}

// ============================================================================
// 컬럼 데이터 포인터 조회
// ============================================================================
const int64_t* QueryExecutor::get_col_data(
    const Partition& part,
    const std::string& col_name) const
{
    const ColumnVector* cv = part.get_column(col_name);
    if (!cv) return nullptr;
    return static_cast<const int64_t*>(cv->raw_data());
}

// ============================================================================
// WHERE Expr 평가 — 행 인덱스 벡터 반환
// 참고: APEX 파티션은 symbol 컬럼이 없고 PartitionKey.symbol_id로 식별됨
//       "symbol" 조건은 파티션 필터링에 사용되고, 여기서는 column 평가만 수행
// ============================================================================
std::vector<uint32_t> QueryExecutor::eval_expr(
    const std::shared_ptr<Expr>& expr,
    const Partition& part,
    size_t num_rows,
    const std::string& default_alias)
{
    if (!expr) {
        // 조건 없음 → 전체 선택
        std::vector<uint32_t> all(num_rows);
        for (size_t i = 0; i < num_rows; ++i) all[i] = static_cast<uint32_t>(i);
        return all;
    }

    switch (expr->kind) {
        case Expr::Kind::AND: {
            auto left  = eval_expr(expr->left,  part, num_rows, default_alias);
            auto right = eval_expr(expr->right, part, num_rows, default_alias);
            // 교집합 (정렬된 두 벡터의 교집합)
            std::vector<uint32_t> result;
            result.reserve(std::min(left.size(), right.size()));
            std::set_intersection(left.begin(), left.end(),
                                  right.begin(), right.end(),
                                  std::back_inserter(result));
            return result;
        }
        case Expr::Kind::OR: {
            auto left  = eval_expr(expr->left,  part, num_rows, default_alias);
            auto right = eval_expr(expr->right, part, num_rows, default_alias);
            std::vector<uint32_t> result;
            result.reserve(left.size() + right.size());
            std::set_union(left.begin(), left.end(),
                           right.begin(), right.end(),
                           std::back_inserter(result));
            return result;
        }
        case Expr::Kind::BETWEEN: {
            // "symbol" 컬럼은 파티션 키 — 이미 파티션 필터링에서 처리됨 → 전체 반환
            if (expr->column == "symbol") {
                // 파티션 레벨에서 이미 필터됨
                std::vector<uint32_t> all(num_rows);
                for (size_t i = 0; i < num_rows; ++i) all[i] = static_cast<uint32_t>(i);
                return all;
            }
            const int64_t* data = get_col_data(part, expr->column);
            if (!data) return {};
            std::vector<uint32_t> result;
            for (size_t i = 0; i < num_rows; ++i) {
                if (data[i] >= expr->lo && data[i] <= expr->hi) {
                    result.push_back(static_cast<uint32_t>(i));
                }
            }
            return result;
        }
        case Expr::Kind::COMPARE: {
            // "symbol" 컬럼은 파티션 키 — 이미 파티션 필터링에서 처리됨
            if (expr->column == "symbol") {
                // 파티션 자체가 이미 이 symbol의 데이터만 갖고 있음 → 전체 반환
                std::vector<uint32_t> all(num_rows);
                for (size_t i = 0; i < num_rows; ++i) all[i] = static_cast<uint32_t>(i);
                return all;
            }
            const int64_t* data = get_col_data(part, expr->column);
            if (!data) return {};
            int64_t val = expr->value;
            std::vector<uint32_t> result;
            result.reserve(num_rows / 4);
            switch (expr->op) {
                case CompareOp::EQ:
                    for (size_t i = 0; i < num_rows; ++i)
                        if (data[i] == val) result.push_back(static_cast<uint32_t>(i));
                    break;
                case CompareOp::NE:
                    for (size_t i = 0; i < num_rows; ++i)
                        if (data[i] != val) result.push_back(static_cast<uint32_t>(i));
                    break;
                case CompareOp::GT:
                    for (size_t i = 0; i < num_rows; ++i)
                        if (data[i] > val) result.push_back(static_cast<uint32_t>(i));
                    break;
                case CompareOp::LT:
                    for (size_t i = 0; i < num_rows; ++i)
                        if (data[i] < val) result.push_back(static_cast<uint32_t>(i));
                    break;
                case CompareOp::GE:
                    for (size_t i = 0; i < num_rows; ++i)
                        if (data[i] >= val) result.push_back(static_cast<uint32_t>(i));
                    break;
                case CompareOp::LE:
                    for (size_t i = 0; i < num_rows; ++i)
                        if (data[i] <= val) result.push_back(static_cast<uint32_t>(i));
                    break;
            }
            return result;
        }
    }
    return {};
}

std::vector<uint32_t> QueryExecutor::eval_where(
    const SelectStmt& stmt,
    const Partition& part,
    size_t num_rows)
{
    if (!stmt.where.has_value()) {
        std::vector<uint32_t> all(num_rows);
        for (size_t i = 0; i < num_rows; ++i) all[i] = static_cast<uint32_t>(i);
        return all;
    }
    return eval_expr(stmt.where->expr, part, num_rows, stmt.from_alias);
}

// ============================================================================
// 단순 SELECT (집계 없음)
// ============================================================================
QueryResultSet QueryExecutor::exec_simple_select(
    const SelectStmt& stmt,
    const std::vector<Partition*>& partitions)
{
    QueryResultSet result;
    size_t rows_scanned = 0;

    // SELECT * 처리
    bool is_star = stmt.columns.size() == 1 && stmt.columns[0].is_star;

    // 첫 번째 파티션으로 컬럼명 결정
    if (!partitions.empty()) {
        auto* part = partitions[0];
        if (is_star) {
            for (const auto& cv : part->columns()) {
                result.column_names.push_back(cv->name());
                result.column_types.push_back(cv->type());
            }
        } else {
            for (const auto& sel : stmt.columns) {
                result.column_names.push_back(
                    sel.alias.empty() ? sel.column : sel.alias);
                result.column_types.push_back(ColumnType::INT64);
            }
        }
    }

    for (auto* part : partitions) {
        size_t n = part->num_rows();
        rows_scanned += n;

        auto sel_indices = eval_where(stmt, *part, n);

        // LIMIT 체크
        size_t limit = stmt.limit.value_or(INT64_MAX);

        for (uint32_t idx : sel_indices) {
            if (result.rows.size() >= limit) break;

            std::vector<int64_t> row;
            if (is_star) {
                for (const auto& cv : part->columns()) {
                    const int64_t* d = static_cast<const int64_t*>(cv->raw_data());
                    row.push_back(d ? d[idx] : 0);
                }
            } else {
                for (const auto& sel : stmt.columns) {
                    const int64_t* d = get_col_data(*part, sel.column);
                    row.push_back(d ? d[idx] : 0);
                }
            }
            result.rows.push_back(std::move(row));
        }
    }

    result.rows_scanned = rows_scanned;
    return result;
}

// ============================================================================
// 집계 실행 (GROUP BY 없음)
// ============================================================================
QueryResultSet QueryExecutor::exec_agg(
    const SelectStmt& stmt,
    const std::vector<Partition*>& partitions)
{
    QueryResultSet result;
    size_t rows_scanned = 0;

    // 집계 결과 저장
    std::vector<int64_t>  i_accum(stmt.columns.size(), 0);
    std::vector<double>   d_accum(stmt.columns.size(), 0.0);
    std::vector<int64_t>  cnt(stmt.columns.size(), 0);
    std::vector<int64_t>  minv(stmt.columns.size(), INT64_MAX);
    std::vector<int64_t>  maxv(stmt.columns.size(), INT64_MIN);
    // VWAP: sum(price*vol), sum(vol)
    std::vector<double>   vwap_pv(stmt.columns.size(), 0.0);
    std::vector<int64_t>  vwap_v(stmt.columns.size(), 0);

    for (auto* part : partitions) {
        size_t n = part->num_rows();
        rows_scanned += n;
        auto sel_indices = eval_where(stmt, *part, n);

        for (size_t ci = 0; ci < stmt.columns.size(); ++ci) {
            const auto& col = stmt.columns[ci];
            const int64_t* data = get_col_data(*part, col.column);

            switch (col.agg) {
                case AggFunc::COUNT:
                    cnt[ci] += static_cast<int64_t>(sel_indices.size());
                    break;
                case AggFunc::SUM:
                    if (data) {
                        for (auto idx : sel_indices) i_accum[ci] += data[idx];
                    }
                    break;
                case AggFunc::AVG:
                    if (data) {
                        for (auto idx : sel_indices) {
                            d_accum[ci] += static_cast<double>(data[idx]);
                            cnt[ci]++;
                        }
                    }
                    break;
                case AggFunc::MIN:
                    if (data) {
                        for (auto idx : sel_indices)
                            minv[ci] = std::min(minv[ci], data[idx]);
                    }
                    break;
                case AggFunc::MAX:
                    if (data) {
                        for (auto idx : sel_indices)
                            maxv[ci] = std::max(maxv[ci], data[idx]);
                    }
                    break;
                case AggFunc::VWAP: {
                    const int64_t* vdata = get_col_data(*part, col.agg_arg2);
                    if (data && vdata) {
                        for (auto idx : sel_indices) {
                            vwap_pv[ci] += static_cast<double>(data[idx])
                                         * static_cast<double>(vdata[idx]);
                            vwap_v[ci]  += vdata[idx];
                        }
                    }
                    break;
                }
                case AggFunc::NONE:
                    break;
            }
        }
    }

    // 결과 행 조립
    std::vector<int64_t> row(stmt.columns.size());
    for (size_t ci = 0; ci < stmt.columns.size(); ++ci) {
        const auto& col = stmt.columns[ci];
        std::string name = col.alias.empty()
            ? (col.column.empty() ? "*" : col.column)
            : col.alias;
        result.column_names.push_back(name);
        result.column_types.push_back(ColumnType::INT64);

        switch (col.agg) {
            case AggFunc::COUNT: row[ci] = cnt[ci]; break;
            case AggFunc::SUM:   row[ci] = i_accum[ci]; break;
            case AggFunc::AVG:
                row[ci] = cnt[ci] > 0
                    ? static_cast<int64_t>(d_accum[ci] / cnt[ci])
                    : 0;
                break;
            case AggFunc::MIN:
                row[ci] = (minv[ci] == INT64_MAX) ? 0 : minv[ci];
                break;
            case AggFunc::MAX:
                row[ci] = (maxv[ci] == INT64_MIN) ? 0 : maxv[ci];
                break;
            case AggFunc::VWAP:
                row[ci] = vwap_v[ci] > 0
                    ? static_cast<int64_t>(vwap_pv[ci] / vwap_v[ci])
                    : 0;
                break;
            case AggFunc::NONE: row[ci] = 0; break;
        }
    }

    result.rows.push_back(std::move(row));
    result.rows_scanned = rows_scanned;
    return result;
}

// ============================================================================
// GROUP BY + 집계
// ============================================================================
QueryResultSet QueryExecutor::exec_group_agg(
    const SelectStmt& stmt,
    const std::vector<Partition*>& partitions)
{
    QueryResultSet result;
    size_t rows_scanned = 0;

    const auto& gb = stmt.group_by.value();
    // 그룹 키 컬럼 (첫 번째 GROUP BY 컬럼만 지원)
    const std::string& group_col = gb.columns[0];

    // 그룹별 집계 상태
    struct GroupState {
        int64_t  sum     = 0;
        int64_t  count   = 0;
        double   avg_sum = 0.0;
        int64_t  minv    = INT64_MAX;
        int64_t  maxv    = INT64_MIN;
        double   vwap_pv = 0.0;
        int64_t  vwap_v  = 0;
    };
    std::unordered_map<int64_t, std::vector<GroupState>> groups;

    for (auto* part : partitions) {
        size_t n = part->num_rows();
        rows_scanned += n;
        auto sel_indices = eval_where(stmt, *part, n);

        // GROUP BY 키: symbol은 파티션 키, 그 외는 컬럼에서 읽음
        const int64_t* gdata = nullptr;
        int64_t symbol_gkey  = static_cast<int64_t>(part->key().symbol_id);
        bool is_symbol_group = (group_col == "symbol");

        if (!is_symbol_group) {
            gdata = get_col_data(*part, group_col);
            if (!gdata) continue;
        }

        for (auto idx : sel_indices) {
            int64_t gkey = is_symbol_group ? symbol_gkey : gdata[idx];
            auto& states = groups[gkey];
            if (states.empty()) {
                states.resize(stmt.columns.size());
            }

            for (size_t ci = 0; ci < stmt.columns.size(); ++ci) {
                const auto& col = stmt.columns[ci];
                auto& gs = states[ci];

                if (col.agg == AggFunc::NONE) continue; // GROUP BY 키 컬럼 자체

                const int64_t* data = get_col_data(*part, col.column);

                switch (col.agg) {
                    case AggFunc::COUNT: gs.count++; break;
                    case AggFunc::SUM:
                        if (data) gs.sum += data[idx];
                        break;
                    case AggFunc::AVG:
                        if (data) { gs.avg_sum += data[idx]; gs.count++; }
                        break;
                    case AggFunc::MIN:
                        if (data) gs.minv = std::min(gs.minv, data[idx]);
                        break;
                    case AggFunc::MAX:
                        if (data) gs.maxv = std::max(gs.maxv, data[idx]);
                        break;
                    case AggFunc::VWAP: {
                        const int64_t* vd = get_col_data(*part, col.agg_arg2);
                        if (data && vd) {
                            gs.vwap_pv += static_cast<double>(data[idx])
                                        * static_cast<double>(vd[idx]);
                            gs.vwap_v  += vd[idx];
                        }
                        break;
                    }
                    case AggFunc::NONE: break;
                }
            }
        }
    }

    // 컬럼 이름 설정
    result.column_names.push_back(group_col);
    result.column_types.push_back(ColumnType::INT64);
    for (size_t ci = 0; ci < stmt.columns.size(); ++ci) {
        const auto& col = stmt.columns[ci];
        if (col.agg == AggFunc::NONE) continue;
        std::string name = col.alias.empty()
            ? (col.column.empty() ? "*" : col.column)
            : col.alias;
        result.column_names.push_back(name);
        result.column_types.push_back(ColumnType::INT64);
    }

    // 그룹별 결과 행 생성
    for (auto& [gkey, states] : groups) {
        std::vector<int64_t> row;
        row.push_back(gkey);
        for (size_t ci = 0; ci < stmt.columns.size(); ++ci) {
            const auto& col = stmt.columns[ci];
            if (col.agg == AggFunc::NONE) continue;
            const auto& gs = states[ci];
            switch (col.agg) {
                case AggFunc::COUNT: row.push_back(gs.count); break;
                case AggFunc::SUM:   row.push_back(gs.sum);   break;
                case AggFunc::AVG:
                    row.push_back(gs.count > 0
                        ? static_cast<int64_t>(gs.avg_sum / gs.count) : 0);
                    break;
                case AggFunc::MIN:
                    row.push_back(gs.minv == INT64_MAX ? 0 : gs.minv);
                    break;
                case AggFunc::MAX:
                    row.push_back(gs.maxv == INT64_MIN ? 0 : gs.maxv);
                    break;
                case AggFunc::VWAP:
                    row.push_back(gs.vwap_v > 0
                        ? static_cast<int64_t>(gs.vwap_pv / gs.vwap_v) : 0);
                    break;
                case AggFunc::NONE: break;
            }
        }
        result.rows.push_back(std::move(row));
    }

    // LIMIT
    if (stmt.limit.has_value() && result.rows.size() > (size_t)stmt.limit.value()) {
        result.rows.resize(stmt.limit.value());
    }

    result.rows_scanned = rows_scanned;
    return result;
}

// ============================================================================
// ASOF JOIN 실행
// ============================================================================
QueryResultSet QueryExecutor::exec_asof_join(
    const SelectStmt& stmt,
    const std::vector<Partition*>& left_parts,
    const std::vector<Partition*>& right_parts)
{
    QueryResultSet result;
    size_t rows_scanned = 0;

    if (left_parts.empty() || right_parts.empty()) {
        return result;
    }

    // 첫 번째 파티션으로 처리 (단순화)
    auto* lp = left_parts[0];
    auto* rp = right_parts[0];
    size_t ln = lp->num_rows();
    size_t rn = rp->num_rows();
    rows_scanned = ln + rn;

    // JOIN 조건 분석
    // ON t.symbol = q.symbol AND t.timestamp >= q.timestamp
    std::string l_key_col, r_key_col;
    std::string l_time_col, r_time_col;

    for (const auto& cond : stmt.join->on_conditions) {
        if (cond.op == CompareOp::EQ) {
            l_key_col = cond.left_col;
            r_key_col = cond.right_col;
        } else if (cond.op == CompareOp::GE || cond.op == CompareOp::GT) {
            l_time_col = cond.left_col;
            r_time_col = cond.right_col;
        }
    }

    // 기본 컬럼명 fallback
    if (l_key_col.empty())  l_key_col  = "symbol";
    if (r_key_col.empty())  r_key_col  = "symbol";
    if (l_time_col.empty()) l_time_col = "timestamp";
    if (r_time_col.empty()) r_time_col = "timestamp";

    const ColumnVector* lk_cv = lp->get_column(l_key_col);
    const ColumnVector* rk_cv = rp->get_column(r_key_col);
    const ColumnVector* lt_cv = lp->get_column(l_time_col);
    const ColumnVector* rt_cv = rp->get_column(r_time_col);

    if (!lk_cv || !rk_cv || !lt_cv || !rt_cv) {
        result.error = "ASOF JOIN: required columns not found";
        return result;
    }

    // ASOF JOIN 실행
    AsofJoinOperator asof;
    JoinResult jres = asof.execute(*lk_cv, *rk_cv, lt_cv, rt_cv);

    // SELECT 컬럼 목록에서 컬럼명 결정
    const std::string l_alias = stmt.from_alias;
    const std::string r_alias = stmt.join->alias;

    for (const auto& sel : stmt.columns) {
        if (sel.is_star) {
            // t.* → 왼쪽 테이블 전체
            for (const auto& cv : lp->columns()) {
                result.column_names.push_back(cv->name());
                result.column_types.push_back(cv->type());
            }
            continue;
        }
        result.column_names.push_back(
            sel.alias.empty() ? sel.column : sel.alias);
        result.column_types.push_back(ColumnType::INT64);
    }

    // 매칭된 행 쌍으로 결과 행 조립
    size_t limit = stmt.limit.value_or(INT64_MAX);
    for (size_t i = 0; i < jres.match_count && result.rows.size() < limit; ++i) {
        int64_t li = jres.left_indices[i];
        int64_t ri = jres.right_indices[i];

        std::vector<int64_t> row;
        for (const auto& sel : stmt.columns) {
            if (sel.is_star) {
                for (const auto& cv : lp->columns()) {
                    const int64_t* d = static_cast<const int64_t*>(cv->raw_data());
                    row.push_back(d ? d[li] : 0);
                }
                continue;
            }
            // alias로 왼쪽/오른쪽 구분
            bool is_right = (!sel.table_alias.empty() && sel.table_alias == r_alias);
            if (is_right && ri >= 0) {
                const int64_t* d = get_col_data(*rp, sel.column);
                row.push_back(d ? d[ri] : 0);
            } else {
                const int64_t* d = get_col_data(*lp, sel.column);
                row.push_back(d ? d[li] : 0);
            }
        }
        result.rows.push_back(std::move(row));
    }

    result.rows_scanned = rows_scanned;
    return result;
}

// ============================================================================
// WHERE symbol 값 추출 (파티션 필터링 최적화용)
// ============================================================================
bool QueryExecutor::has_where_symbol(
    const SelectStmt& stmt,
    int64_t& out_sym,
    const std::string& /*alias*/) const
{
    if (!stmt.where.has_value()) return false;
    // 간단히: WHERE symbol = N 패턴만 추출
    auto find_sym = [&](const auto& self, const std::shared_ptr<Expr>& expr) -> bool {
        if (!expr) return false;
        if (expr->kind == Expr::Kind::COMPARE) {
            if (expr->column == "symbol" && expr->op == CompareOp::EQ) {
                out_sym = expr->value;
                return true;
            }
        }
        return self(self, expr->left) || self(self, expr->right);
    };
    return find_sym(find_sym, stmt.where->expr);
}

} // namespace apex::sql
