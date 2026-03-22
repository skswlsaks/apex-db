#pragma once
// ============================================================================
// APEX-DB: SQL AST (Abstract Syntax Tree)
// ============================================================================
// 파서가 생성하는 AST 노드 정의
// 지원 SQL:
//   SELECT [DISTINCT] col_expr [, col_expr ...] [AS alias]
//   FROM table [alias]
//   [ASOF | INNER | LEFT] JOIN table [alias] ON cond [AND cond ...]
//   [WHERE cond [AND|OR cond ...]]
//   [GROUP BY col [, col ...]]
//   [ORDER BY col [ASC|DESC] [, ...]]
//   [LIMIT n]
// ============================================================================

#include "apex/storage/column_store.h"  // ColumnType
#include <optional>
#include <string>
#include <vector>
#include <cstdint>
#include <memory>

namespace apex::sql {

// ============================================================================
// AggFunc: 집계 함수 종류
// ============================================================================
enum class AggFunc {
    NONE,   // 집계 없음 (일반 컬럼)
    COUNT,  // COUNT(*) or COUNT(col)
    SUM,
    AVG,
    MIN,
    MAX,
    VWAP,   // VWAP(price, volume) — 금융 특화
};

// ============================================================================
// SelectExpr: SELECT 목록의 단일 항목
// ============================================================================
struct SelectExpr {
    AggFunc     agg    = AggFunc::NONE;
    std::string table_alias;   // 없으면 ""
    std::string column;        // "*" 이면 전체, "1" 이면 COUNT(*)
    std::string agg_arg2;      // VWAP(price, volume)의 두 번째 인자
    std::string alias;         // AS alias
    bool        is_star = false; // SELECT *
};

// ============================================================================
// CompareOp: 비교 연산자
// ============================================================================
enum class CompareOp { EQ, NE, GT, LT, GE, LE };

// ============================================================================
// Expr: WHERE 조건의 단일 표현식
// ============================================================================
struct Expr {
    enum class Kind {
        COMPARE,     // col op val
        BETWEEN,     // col BETWEEN lo AND hi
        AND,         // left AND right
        OR,          // left OR right
    };

    Kind kind = Kind::COMPARE;

    // COMPARE 필드
    std::string table_alias;
    std::string column;
    CompareOp   op = CompareOp::EQ;
    int64_t     value = 0;         // 숫자 리터럴 (정수 기준)
    double      value_f = 0.0;     // 실수 리터럴
    bool        is_float = false;

    // BETWEEN 필드
    int64_t     lo = 0;
    int64_t     hi = 0;

    // AND / OR 결합
    std::shared_ptr<Expr> left;
    std::shared_ptr<Expr> right;
};

// ============================================================================
// WhereClause
// ============================================================================
struct WhereClause {
    std::shared_ptr<Expr> expr;
};

// ============================================================================
// JoinCondition: ON 절의 단일 조건
// ============================================================================
struct JoinCondition {
    // t.symbol = q.symbol 또는 t.timestamp >= q.timestamp
    std::string left_alias;
    std::string left_col;
    CompareOp   op = CompareOp::EQ;
    std::string right_alias;
    std::string right_col;
};

// ============================================================================
// JoinClause
// ============================================================================
struct JoinClause {
    enum class Type { INNER, ASOF, LEFT };

    Type                        type = Type::INNER;
    std::string                 table;
    std::string                 alias;
    std::vector<JoinCondition>  on_conditions;
};

// ============================================================================
// GroupByClause
// ============================================================================
struct GroupByClause {
    std::vector<std::string> columns;  // table_alias.col 또는 col
    std::vector<std::string> aliases;  // 대응되는 테이블 alias (없으면 "")
};

// ============================================================================
// OrderByClause
// ============================================================================
struct OrderByItem {
    std::string table_alias;
    std::string column;
    bool        asc = true;
};

struct OrderByClause {
    std::vector<OrderByItem> items;
};

// ============================================================================
// SelectStmt: 최상위 SELECT 문
// ============================================================================
struct SelectStmt {
    bool                        distinct = false;
    std::vector<SelectExpr>     columns;       // SELECT 목록
    std::string                 from_table;    // FROM 테이블명
    std::string                 from_alias;    // 테이블 별칭 (없으면 "")
    std::optional<JoinClause>   join;          // JOIN 절
    std::optional<WhereClause>  where;         // WHERE 절
    std::optional<GroupByClause> group_by;     // GROUP BY 절
    std::optional<OrderByClause> order_by;     // ORDER BY 절
    std::optional<int64_t>      limit;         // LIMIT n
};

} // namespace apex::sql
