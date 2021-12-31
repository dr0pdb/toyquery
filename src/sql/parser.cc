#include "sql/parser.h"

#include "common/macros.h"
#include "common/utils.h"

namespace toyquery {
namespace sql {

using ::toyquery::common::ToDouble;
using ::toyquery::common::ToLong;

absl::StatusOr<std::shared_ptr<SqlExpression>> Parser::Parse(int precedence) {
  ASSIGN_OR_RETURN(auto expr, parsePrefix());
  RETURN_IF_NULL(expr);
  while (precedence < nextPrecedence()) { ASSIGN_OR_RETURN(expr, parseInfix(expr, nextPrecedence())); }
  return expr;
}

int Parser::nextPrecedence() {
  if (isAtEnd()) { return 0; }
  auto token = current();

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

absl::StatusOr<std::shared_ptr<SqlExpression>> Parser::parsePrefix() {
  if (isAtEnd()) { return nullptr; }

  auto token = current();
  advance();

  switch (token.type_) {
    case TokenType::KEYWORD_SELECT: return parseSelect();

    case TokenType::KEYWORD_MAX:
    case TokenType::KEYWORD_MIN:
    case TokenType::KEYWORD_SUM: {
      return std::make_shared<SqlIdentifier>(token.text_);
    }

    case TokenType::LITERAL_IDENTIFIER: return std::make_shared<SqlIdentifier>(token.text_);
    case TokenType::LITERAL_STRING: return std::make_shared<SqlString>(token.text_);

    case TokenType::LITERAL_LONG: {
      ASSIGN_OR_RETURN(auto val, ToLong(token.text_));
      return std::make_shared<SqlLong>(val);
    }
    case TokenType::LITERAL_DOUBLE: {
      ASSIGN_OR_RETURN(auto val, ToDouble(token.text_));
      return std::make_shared<SqlDouble>(val);
    }

    default: return absl::InvalidArgumentError(absl::StrCat(token.text_, " is unexpected"));
  }
}

absl::StatusOr<std::shared_ptr<SqlExpression>> Parser::parseInfix(std::shared_ptr<SqlExpression> left, int precedence) { }

absl::StatusOr<std::shared_ptr<SqlExpression>> Parser::parseSelect() { }

absl::Status Parser::expect(TokenType expected_type) { return absl::OkStatus(); }

Token Parser::current() { return tokens_[token_idx_]; }

void Parser::advance() { token_idx_++; }

bool Parser::isAtEnd() { return token_idx_ >= tokens_.size(); }

}  // namespace sql
}  // namespace toyquery
