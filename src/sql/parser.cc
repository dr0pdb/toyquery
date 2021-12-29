#include "sql/parser.h"

#include "common/macros.h"

namespace toyquery {
namespace sql {

absl::StatusOr<std::shared_ptr<SqlExpression>> Parser::Parse(int precedence) {
  ASSIGN_OR_RETURN(auto expr, ParsePrefix());
  while (precedence < nextPrecedence()) { ASSIGN_OR_RETURN(expr, ParseInfix(expr, nextPrecedence())); }
  return expr;
}

int Parser::nextPrecedence() {
  if (token_idx_ >= tokens_.size()) { return 0; }
  auto token = tokens_[token_idx_];

  switch (token.type_) {
    case TokenType::KEYWORD_AS:
    case TokenType::KEYWORD_ASC:
    case TokenType::KEYWORD_DESC: return 10;

    case TokenType::OPERATOR_LESS_THAN:
    case TokenType::OPERATOR_LESS_THAN_EQUAL_TO:
    case TokenType::OPERATOR_GREATER_THAN:
    case TokenType::OPERATOR_GREATER_THAN_EQUAL_TO:
    case TokenType::OPERATOR_EQUAL:
    case TokenType::OPERATOR_NOT_EQUAL: return 40;

    case TokenType::OPERATOR_PLUS:
    case TokenType::OPERATOR_MINUS: return 50;

    case TokenType::OPERATOR_ASTERISK:
    case TokenType::OPERATOR_SLASH: return 60;

    case TokenType::SYMBOL_LEFT_PAREN: return 70;

    default: return 0;
  }
}

absl::StatusOr<std::shared_ptr<SqlExpression>> Parser::ParsePrefix() { }

absl::StatusOr<std::shared_ptr<SqlExpression>> Parser::ParseInfix(std::shared_ptr<SqlExpression> left, int precedence) { }

}  // namespace sql
}  // namespace toyquery
