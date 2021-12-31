#ifndef SQL_PARSER_H
#define SQL_PARSER_H

#include <vector>

#include "absl/status/statusor.h"
#include "expressions.h"
#include "tokens.h"

namespace toyquery {
namespace sql {

/**
 * @brief The SQL parser
 *
 */
class Parser {
 public:
  Parser(std::vector<Token> tokens) : tokens_{ tokens } { }

  /**
   * @brief Parse the provided token stream into a sql expression
   *
   * @return absl::StatusOr<std::shared_ptr<SqlExpression>>: the parsed sql expression
   */
  absl::StatusOr<std::shared_ptr<SqlExpression>> Parse(int precedence = 0);

 private:
  // Get the precedence of the next token. Returns 0 if it is at the end of the token stream.
  int nextPrecedence();

  // Parse the next prefix operation
  absl::StatusOr<std::shared_ptr<SqlExpression>> parsePrefix();

  // Parse the next infix operation
  absl::StatusOr<std::shared_ptr<SqlExpression>> parseInfix(std::shared_ptr<SqlExpression> left, int precedence);

  absl::StatusOr<std::shared_ptr<SqlExpression>> parseSelect();

  absl::Status expect(TokenType expected_type);

  Token current();
  void advance();

  // Check if we are at the end of the token stream.
  bool isAtEnd();

  std::vector<Token> tokens_;
  int token_idx_{ 0 };
};

}  // namespace sql
}  // namespace toyquery

#endif  // SQL_PARSER_H