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
    case TokenType::KEYWORD_CAST: return parseCast();

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

absl::StatusOr<std::shared_ptr<SqlExpression>> Parser::parseInfix(std::shared_ptr<SqlExpression> left, int precedence) {
  auto token = current();

  switch (token.type_) {
    case TokenType::OPERATOR_PLUS:
    case TokenType::OPERATOR_MINUS:
    case TokenType::OPERATOR_ASTERISK:
    case TokenType::OPERATOR_SLASH:
    case TokenType::OPERATOR_EQUAL:
    case TokenType::OPERATOR_GREATER_THAN:
    case TokenType::OPERATOR_GREATER_THAN_EQUAL_TO:
    case TokenType::OPERATOR_LESS_THAN:
    case TokenType::OPERATOR_LESS_THAN_EQUAL_TO:
    case TokenType::KEYWORD_AND:
    case TokenType::KEYWORD_OR: {
      advance();  // consume the token
      ASSIGN_OR_RETURN(auto right_expr, Parse(precedence));
      return std::make_shared<SqlBinaryExpression>(left, token.text_, right_expr);
    }

    case TokenType::KEYWORD_AS: {
      advance();  // consume the token
      ASSIGN_OR_RETURN(auto identifier, parseIdentifier());
      return std::make_shared<SqlAlias>(left, identifier);
    }

    case TokenType::KEYWORD_ASC:
    case TokenType::KEYWORD_DESC: {
      advance();  // consume token
      return std::make_shared<SqlSort>(left, token.type_ == TokenType::KEYWORD_ASC);
    }

    case TokenType::SYMBOL_LEFT_PAREN: {
      if (left->GetType() == SqlExpressionType::SqlIdentifier) {
        advance();  // consume token
        ASSIGN_OR_RETURN(auto args, parseExpressionList());
        return std::make_shared<SqlFunction>(std::static_pointer_cast<SqlIdentifier>(left)->id_, args);
      } else {
        return absl::InvalidArgumentError("unexpected left paren");
      }
    }

    default: return absl::InvalidArgumentError("invalid token type in parseInfix");
  }
}

absl::StatusOr<std::shared_ptr<SqlExpression>> Parser::parseSelect() {
  ASSIGN_OR_RETURN(auto projection, parseExpressionList());

  if (match(TokenType::KEYWORD_FROM)) {
    ASSIGN_OR_RETURN(auto table, parseExpression());

    std::shared_ptr<SqlExpression> filter_expr{ nullptr };
    if (match(TokenType::KEYWORD_WHERE)) { ASSIGN_OR_RETURN(filter_expr, parseExpression()); }

    std::vector<std::shared_ptr<SqlExpression>> group_by{};
    if (matchMultiple({ TokenType::KEYWORD_GROUP, TokenType::KEYWORD_BY })) {
      ASSIGN_OR_RETURN(group_by, parseExpressionList());
    }

    std::shared_ptr<SqlExpression> having{ nullptr };
    if (match(TokenType::KEYWORD_HAVING)) { ASSIGN_OR_RETURN(having, parseExpression()); }

    std::vector<std::shared_ptr<SqlSort>> order_by{};
    if (matchMultiple({ TokenType::KEYWORD_ORDER, TokenType::KEYWORD_BY })) { ASSIGN_OR_RETURN(order_by, parseOrder()); }

    return std::make_shared<SqlSelect>(
        projection, filter_expr, group_by, order_by, having, std::static_pointer_cast<SqlIdentifier>(table)->id_);
  } else {
    return absl::InvalidArgumentError(absl::StrCat(current().text_, " found, expected FROM"));
  }
}

absl::StatusOr<std::shared_ptr<SqlCast>> Parser::parseCast() {
  CHECK_OK_OR_RETURN(expect(TokenType::SYMBOL_LEFT_PAREN));
  ASSIGN_OR_RETURN(auto expr, parseExpression());
  auto alias = std::static_pointer_cast<SqlAlias>(expr);
  CHECK_OK_OR_RETURN(expect(TokenType::SYMBOL_RIGHT_PARENT));
  return std::make_shared<SqlCast>(alias->expr_, alias->alias_);
}

absl::StatusOr<std::vector<std::shared_ptr<SqlSort>>> Parser::parseOrder() {
  std::vector<std::shared_ptr<SqlSort>> sort_list{};

  ASSIGN_OR_RETURN(auto sort_expr, parseExpression());
  while (sort_expr != nullptr) {
    switch (sort_expr->GetType()) {
      case SqlExpressionType::SqlIdentifier: {
        sort_expr = std::make_shared<SqlSort>(sort_expr, true);
        break;
      }
      case SqlExpressionType::SqlSort: break;  // nothing to do
      default: return absl::InvalidArgumentError("invalid expression, expected ordering expression");
    }
    sort_list.push_back(std::static_pointer_cast<SqlSort>(sort_expr));

    if (current().type_ == TokenType::SYMBOL_COMMA) {
      advance();
    } else {
      break;
    }

    ASSIGN_OR_RETURN(sort_expr, parseExpression());
  }

  return sort_list;
}

absl::StatusOr<std::shared_ptr<SqlIdentifier>> Parser::parseIdentifier() {
  if (isAtEnd()) { return absl::InvalidArgumentError("end of token stream, expected an identifier"); }

  ASSIGN_OR_RETURN(auto expr, parseExpression());
  switch (expr->GetType()) {
    case SqlExpressionType::SqlIdentifier: return std::static_pointer_cast<SqlIdentifier>(expr);
    default: return absl::InvalidArgumentError("expected an identifier");
  }
}

absl::StatusOr<std::vector<std::shared_ptr<SqlExpression>>> Parser::parseExpressionList() {
  std::vector<std::shared_ptr<SqlExpression>> exprs;

  ASSIGN_OR_RETURN(auto expr, parseExpression());
  while (expr != nullptr) {
    exprs.push_back(expr);
    if (current().type_ == TokenType::SYMBOL_COMMA) {
      advance();
    } else {
      break;
    }

    ASSIGN_OR_RETURN(expr, parseExpression());
  }

  return exprs;
}

absl::StatusOr<std::shared_ptr<SqlExpression>> Parser::parseExpression() { Parse(0); }

bool Parser::match(TokenType expected_type) {
  auto token = current();
  if (token.type_ == expected_type) {
    advance();
    return true;
  }
  return false;
}

bool Parser::matchMultiple(std::vector<TokenType> expected_types) {
  for (int i = 0; i < expected_types.size(); i++) {
    if (peek(i).type_ != expected_types[i]) { return false; }
  }

  token_idx_ += expected_types.size();
  return true;
}

bool Parser::matchKeyword(absl::string_view keyword) {
  auto keyword_itr = keywords.find(keyword);
  auto token = current();

  if (keyword_itr != keywords.end() && token.type_ == keyword_itr->second) {
    advance();
    return true;
  }

  return false;
}

absl::Status Parser::expect(TokenType expected_type) {
  if (isAtEnd()) { return absl::InvalidArgumentError("unexpected end of token stream"); }

  auto token = current();
  if (token.type_ == expected_type) {
    advance();
    return absl::OkStatus();
  } else {
    return absl::InvalidArgumentError("unexpected token type");
  }
}

Token Parser::current() { return tokens_[token_idx_]; }

Token Parser::peek(int jump) {
  if (token_idx_ + jump >= tokens_.size()) { return tokens_.back(); }

  return tokens_[token_idx_ + jump];
}

void Parser::advance() { token_idx_++; }

bool Parser::isAtEnd() { return token_idx_ >= tokens_.size(); }

}  // namespace sql
}  // namespace toyquery
