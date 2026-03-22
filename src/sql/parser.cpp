// ============================================================================
// APEX-DB: SQL Parser Implementation
// ============================================================================
// 재귀 하강 파서 — yacc/bison 없이 순수 C++ 구현
// ============================================================================

#include "apex/sql/parser.h"
#include <stdexcept>
#include <algorithm>
#include <cstdlib>

namespace apex::sql {

// ============================================================================
// 토큰 유틸리티
// ============================================================================
const Token& Parser::current() const {
    if (pos_ < tokens_.size()) return tokens_[pos_];
    static Token end{TokenType::END, ""};
    return end;
}

const Token& Parser::peek(size_t offset) const {
    size_t idx = pos_ + offset;
    if (idx < tokens_.size()) return tokens_[idx];
    static Token end{TokenType::END, ""};
    return end;
}

bool Parser::at_end() const {
    return pos_ >= tokens_.size() || tokens_[pos_].type == TokenType::END;
}

bool Parser::check(TokenType t) const {
    return !at_end() && current().type == t;
}

bool Parser::match(TokenType t) {
    if (check(t)) { advance(); return true; }
    return false;
}

Token Parser::expect(TokenType t, const char* msg) {
    if (!check(t)) {
        throw std::runtime_error(
            std::string("Parser: expected ") + msg +
            " but got '" + current().value + "'");
    }
    Token tok = current();
    advance();
    return tok;
}

void Parser::advance() {
    if (pos_ < tokens_.size()) ++pos_;
}

// ============================================================================
// 메인 파싱 진입점
// ============================================================================
SelectStmt Parser::parse(const std::string& sql) {
    Tokenizer tok;
    tokens_ = tok.tokenize(sql);
    pos_ = 0;
    return parse_select();
}

// ============================================================================
// SELECT 파싱
// ============================================================================
SelectStmt Parser::parse_select() {
    expect(TokenType::SELECT, "SELECT");

    SelectStmt stmt;

    // DISTINCT
    if (match(TokenType::DISTINCT)) {
        stmt.distinct = true;
    }

    // SELECT 컬럼 목록
    // SELECT *  또는  SELECT col [, col ...]
    if (check(TokenType::STAR)) {
        advance();
        SelectExpr expr;
        expr.is_star = true;
        stmt.columns.push_back(expr);
    } else {
        stmt.columns.push_back(parse_select_expr());
        while (match(TokenType::COMMA)) {
            stmt.columns.push_back(parse_select_expr());
        }
    }

    // FROM
    expect(TokenType::FROM, "FROM");
    stmt.from_table = expect(TokenType::IDENT, "table name").value;

    // 테이블 별칭 (AS는 선택적)
    match(TokenType::AS); // 선택적 AS
    if (check(TokenType::IDENT)
        && current().value != "WHERE"
        && current().value != "JOIN"
        && current().value != "GROUP"
        && current().value != "ORDER"
        && current().value != "LIMIT"
        && current().type != TokenType::WHERE
        && current().type != TokenType::JOIN
        && current().type != TokenType::ASOF
        && current().type != TokenType::GROUP
        && current().type != TokenType::ORDER
        && current().type != TokenType::LIMIT
        && current().type != TokenType::INNER
        && current().type != TokenType::LEFT) {
        stmt.from_alias = current().value;
        advance();
    }

    // JOIN (ASOF JOIN, INNER JOIN, JOIN)
    if (check(TokenType::ASOF) || check(TokenType::JOIN)
        || check(TokenType::INNER) || check(TokenType::LEFT)) {
        stmt.join = parse_join();
    }

    // WHERE
    if (match(TokenType::WHERE)) {
        stmt.where = parse_where();
    }

    // GROUP BY
    if (check(TokenType::GROUP)) {
        advance();
        expect(TokenType::BY, "BY");
        stmt.group_by = parse_group_by();
    }

    // ORDER BY
    if (check(TokenType::ORDER)) {
        advance();
        expect(TokenType::BY, "BY");
        stmt.order_by = parse_order_by();
    }

    // LIMIT
    if (match(TokenType::LIMIT)) {
        stmt.limit = parse_integer_literal();
    }

    return stmt;
}

// ============================================================================
// SELECT 표현식 파싱 (집계 함수 또는 컬럼)
// ============================================================================
SelectExpr Parser::parse_select_expr() {
    SelectExpr expr;

    // 집계 함수: SUM(col), AVG(col), COUNT(*), VWAP(price, volume) 등
    auto parse_agg = [&](AggFunc func) {
        expr.agg = func;
        expect(TokenType::LPAREN, "(");
        if (func == AggFunc::COUNT && check(TokenType::STAR)) {
            advance();
            expr.column = "*";
        } else if (func == AggFunc::VWAP) {
            // VWAP(price, volume)
            std::string alias1, col1;
            parse_qualified_name(alias1, col1);
            expr.table_alias = alias1;
            expr.column = col1;
            expect(TokenType::COMMA, ",");
            std::string alias2, col2;
            parse_qualified_name(alias2, col2);
            expr.agg_arg2 = col2;
        } else {
            std::string alias, col;
            parse_qualified_name(alias, col);
            expr.table_alias = alias;
            expr.column = col;
        }
        expect(TokenType::RPAREN, ")");
    };

    switch (current().type) {
        case TokenType::SUM:   advance(); parse_agg(AggFunc::SUM);   break;
        case TokenType::AVG:   advance(); parse_agg(AggFunc::AVG);   break;
        case TokenType::COUNT: advance(); parse_agg(AggFunc::COUNT); break;
        case TokenType::MIN:   advance(); parse_agg(AggFunc::MIN);   break;
        case TokenType::MAX:   advance(); parse_agg(AggFunc::MAX);   break;
        case TokenType::VWAP:  advance(); parse_agg(AggFunc::VWAP);  break;
        case TokenType::STAR:
            advance();
            expr.is_star = true;
            break;
        default: {
            // 일반 컬럼: [alias.]col
            std::string alias, col;
            parse_qualified_name(alias, col);
            expr.table_alias = alias;
            if (col == "*") {
                expr.is_star = true;
            } else {
                expr.column = col;
            }
            break;
        }
    }

    // AS alias
    if (match(TokenType::AS)) {
        expr.alias = expect(TokenType::IDENT, "column alias").value;
    } else if (check(TokenType::IDENT)
               && current().value != "FROM"
               && current().type != TokenType::FROM) {
        // 암묵적 alias (AS 없이)
        // 여기서는 보수적으로 AS만 처리
    }

    return expr;
}

// ============================================================================
// JOIN 절 파싱
// ============================================================================
JoinClause Parser::parse_join() {
    JoinClause jc;

    // ASOF JOIN
    if (check(TokenType::ASOF)) {
        advance();
        expect(TokenType::JOIN, "JOIN");
        jc.type = JoinClause::Type::ASOF;
    }
    // INNER JOIN 또는 JOIN
    else if (match(TokenType::INNER)) {
        expect(TokenType::JOIN, "JOIN");
        jc.type = JoinClause::Type::INNER;
    }
    // LEFT [OUTER] JOIN
    else if (match(TokenType::LEFT)) {
        match(TokenType::OUTER); // optional
        expect(TokenType::JOIN, "JOIN");
        jc.type = JoinClause::Type::LEFT;
    }
    // JOIN (기본 INNER)
    else {
        expect(TokenType::JOIN, "JOIN");
        jc.type = JoinClause::Type::INNER;
    }

    jc.table = expect(TokenType::IDENT, "join table name").value;

    // 테이블 별칭
    match(TokenType::AS);
    if (check(TokenType::IDENT) && current().type != TokenType::ON) {
        jc.alias = current().value;
        advance();
    }

    // ON 절
    expect(TokenType::ON, "ON");

    // ON 조건 파싱: t.col op q.col [AND ...]
    auto parse_join_cond = [&]() {
        JoinCondition cond;
        parse_qualified_name(cond.left_alias, cond.left_col);
        cond.op = parse_compare_op();
        parse_qualified_name(cond.right_alias, cond.right_col);
        jc.on_conditions.push_back(std::move(cond));
    };

    parse_join_cond();
    while (match(TokenType::AND)) {
        parse_join_cond();
    }

    return jc;
}

// ============================================================================
// WHERE 절 파싱
// ============================================================================
WhereClause Parser::parse_where() {
    WhereClause wc;
    wc.expr = parse_expr();
    return wc;
}

// ============================================================================
// 표현식 파싱 (OR 레벨)
// ============================================================================
std::shared_ptr<Expr> Parser::parse_expr() {
    auto left = parse_and_expr();
    while (match(TokenType::OR)) {
        auto right = parse_and_expr();
        auto node = std::make_shared<Expr>();
        node->kind  = Expr::Kind::OR;
        node->left  = std::move(left);
        node->right = std::move(right);
        left = std::move(node);
    }
    return left;
}

// ============================================================================
// AND 레벨
// ============================================================================
std::shared_ptr<Expr> Parser::parse_and_expr() {
    auto left = parse_primary_expr();
    while (match(TokenType::AND)) {
        auto right = parse_primary_expr();
        auto node = std::make_shared<Expr>();
        node->kind  = Expr::Kind::AND;
        node->left  = std::move(left);
        node->right = std::move(right);
        left = std::move(node);
    }
    return left;
}

// ============================================================================
// 기본 조건 표현식
// ============================================================================
std::shared_ptr<Expr> Parser::parse_primary_expr() {
    // NOT 지원 (NOT col = val)
    if (match(TokenType::NOT)) {
        // 간단히 NOT을 무시하거나 NE로 처리
        // 여기서는 간단히 하위 표현식 파싱 후 반환
        auto sub = parse_primary_expr();
        return sub;
    }

    // 괄호
    if (match(TokenType::LPAREN)) {
        auto sub = parse_expr();
        expect(TokenType::RPAREN, ")");
        return sub;
    }

    // col [op val | BETWEEN lo AND hi]
    auto node = std::make_shared<Expr>();
    parse_qualified_name(node->table_alias, node->column);

    // BETWEEN
    if (match(TokenType::BETWEEN)) {
        node->kind = Expr::Kind::BETWEEN;
        node->lo   = parse_integer_literal();
        expect(TokenType::AND, "AND");
        node->hi   = parse_integer_literal();
        return node;
    }

    // 비교 연산자
    node->kind = Expr::Kind::COMPARE;
    node->op   = parse_compare_op();

    // 오른쪽 값 (숫자 또는 문자열)
    if (check(TokenType::NUMBER)) {
        std::string s = current().value;
        advance();
        // 실수 여부 체크
        if (s.find('.') != std::string::npos) {
            node->value_f = std::stod(s);
            node->is_float = true;
            node->value = static_cast<int64_t>(node->value_f);
        } else {
            node->value = std::stoll(s);
        }
    } else if (check(TokenType::STRING)) {
        // 문자열 → 숫자 변환 시도 (심볼 ID 등)
        std::string s = current().value;
        advance();
        try { node->value = std::stoll(s); }
        catch (...) { node->value = 0; }
    } else if (check(TokenType::IDENT) || check(TokenType::STAR)) {
        // 다른 컬럼과 비교 (조인 조건 등 — WHERE에서는 드물지만)
        node->value = 0;
        advance();
    } else {
        throw std::runtime_error(
            "Parser: expected value in WHERE condition, got '"
            + current().value + "'");
    }

    return node;
}

// ============================================================================
// GROUP BY 파싱
// ============================================================================
GroupByClause Parser::parse_group_by() {
    GroupByClause gc;
    std::string alias, col;
    parse_qualified_name(alias, col);
    gc.aliases.push_back(alias);
    gc.columns.push_back(col);

    while (match(TokenType::COMMA)) {
        parse_qualified_name(alias, col);
        gc.aliases.push_back(alias);
        gc.columns.push_back(col);
    }
    return gc;
}

// ============================================================================
// ORDER BY 파싱
// ============================================================================
OrderByClause Parser::parse_order_by() {
    OrderByClause oc;

    auto parse_item = [&]() {
        OrderByItem item;
        parse_qualified_name(item.table_alias, item.column);
        if (match(TokenType::DESC)) {
            item.asc = false;
        } else {
            match(TokenType::ASC); // 선택적
        }
        oc.items.push_back(std::move(item));
    };

    parse_item();
    while (match(TokenType::COMMA)) {
        parse_item();
    }
    return oc;
}

// ============================================================================
// 비교 연산자 파싱
// ============================================================================
CompareOp Parser::parse_compare_op() {
    switch (current().type) {
        case TokenType::EQ: advance(); return CompareOp::EQ;
        case TokenType::NE: advance(); return CompareOp::NE;
        case TokenType::GT: advance(); return CompareOp::GT;
        case TokenType::LT: advance(); return CompareOp::LT;
        case TokenType::GE: advance(); return CompareOp::GE;
        case TokenType::LE: advance(); return CompareOp::LE;
        default:
            throw std::runtime_error(
                "Parser: expected comparison operator, got '"
                + current().value + "'");
    }
}

// ============================================================================
// 한정된 이름 파싱: [alias.]col 또는 col
// ============================================================================
std::string Parser::parse_qualified_name(std::string& out_alias,
                                          std::string& out_col) {
    out_alias.clear();
    // IDENT [DOT IDENT]
    if (check(TokenType::IDENT)) {
        std::string first = current().value;
        advance();
        if (match(TokenType::DOT)) {
            // alias.col
            out_alias = first;
            if (check(TokenType::STAR)) {
                out_col = "*";
                advance();
            } else {
                out_col = expect(TokenType::IDENT, "column name").value;
            }
        } else {
            out_col = first;
        }
    } else if (check(TokenType::STAR)) {
        out_col = "*";
        advance();
    } else {
        throw std::runtime_error(
            "Parser: expected identifier, got '" + current().value + "'");
    }
    return out_col;
}

// ============================================================================
// 정수 리터럴 파싱
// ============================================================================
int64_t Parser::parse_integer_literal() {
    if (check(TokenType::NUMBER)) {
        std::string s = current().value;
        advance();
        return std::stoll(s);
    }
    throw std::runtime_error(
        "Parser: expected integer literal, got '" + current().value + "'");
}

} // namespace apex::sql
