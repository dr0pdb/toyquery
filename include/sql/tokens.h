#ifndef SQL_TOKENS_H
#define SQL_TOKENS_H

#include <unordered_map>

#include "absl/strings/string_view.h"

/**
 * @brief The sql tokens to depict queries.
 *
 * Since we only support queries and no modifications, the tokens are only for
 * select statements.
 */

namespace toyquery {
namespace sql {

/**
 * @brief The type of the lexical token.
 *
 */
enum class TokenType {
  // special
  SPECIAL_EOF,
  SPECIAL_ERROR,

  // literals
  LITERAL_TRUE,
  LITERAL_FALSE,
  LITERAL_LONG,
  LITERAL_DOUBLE,
  LITERAL_STRING,
  LITERAL_IDENTIFIER,

  // symbols
  SYMBOL_PERIOD,
  SYMBOL_COMMA,
  SYMBOL_LEFT_PAREN,
  SYMBOL_RIGHT_PARENT,
  SYMBOL_SEMICOLON,

  // operators
  OPERATOR_EQUAL_EQUALS,
  OPERATOR_EQUAL,
  OPERATOR_GREATER_THAN,
  OPERATOR_LESS_THAN,
  OPERATOR_PLUS,
  OPERATOR_MINUS,
  OPERATOR_ASTERISK,
  OPERATOR_SLASH,
  OPERATOR_CARET,
  OPERATOR_PERCENT,
  OPERATOR_EXCLAMATION,
  OPERATOR_QUESTION_MARK,
  OPERATOR_NOT_EQUAL,
  OPERATOR_LESS_THAN_EQUAL_TO,
  OPERATOR_GREATER_THAN_EQUAL_TO,
  OPERATOR_AND_AND,
  OPERATOR_OR_OR,

  // keywords
  KEYWORD_SELECT,
  KEYWORD_WHERE,
  KEYWORD_ORDER,
  KEYWORD_BY,
  KEYWORD_FROM,
  KEYWORD_AND,
  KEYWORD_OR,
  KEYWORD_AS,
  KEYWORD_ASC,
  KEYWORD_DESC,
  KEYWORD_MAX,
  KEYWORD_MIN,
  KEYWORD_SUM
};

static std::unordered_map<absl::string_view, TokenType> keywords = {
  { "SELECT", TokenType::KEYWORD_SELECT }, { "WHERE", TokenType::KEYWORD_WHERE }, { "ORDER", TokenType::KEYWORD_ORDER },
  { "BY", TokenType::KEYWORD_BY },         { "FROM", TokenType::KEYWORD_FROM },   { "AND", TokenType::KEYWORD_AND },
  { "OR", TokenType::KEYWORD_OR },         { "AS", TokenType::KEYWORD_AS },       { "ASC", TokenType::KEYWORD_ASC },
  { "DESC", TokenType::KEYWORD_DESC },     { "MAX", TokenType::KEYWORD_MAX },     { "MIN", TokenType::KEYWORD_MIN },
  { "SUM", TokenType::KEYWORD_SUM }
};

/**
 * @brief A single lexical token derived from the sql query string
 *
 */
struct Token {
  Token(absl::string_view text, TokenType type, int end_offset) : text_{ text }, type_{ type }, end_offset_{ end_offset } { }

  absl::string_view text_;
  TokenType type_;
  int end_offset_;
};

}  // namespace sql
}  // namespace toyquery

#endif  // SQL_TOKENS_H
