#ifndef SQL_TOKENIZER_H
#define SQL_TOKENIZER_H

#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "sql/tokens.h"

namespace toyquery {
namespace sql {

/**
 * @brief The tokenizer to convert the sql query string to lexical tokens.
 *
 */
class Tokenizer {
 public:
  /**
   * @brief Construct a new Tokenizer object
   *
   * @param source: the view into the source. Assumes that the source outlives the tokenizer.
   */
  Tokenizer(absl::string_view source);

  /**
   * @brief Tokenize the source into lexical tokens
   *
   * @return absl::StatusOr<std::vector<Token>>: the lexical tokens or status of the operation.
   */
  absl::StatusOr<std::vector<Token>> Tokenize();

 private:
  absl::StatusOr<Token> scanToken();
  absl::StatusOr<Token> string();
  absl::StatusOr<Token> number();
  absl::StatusOr<Token> identifier();
  void skipWhitespace();

  Token createToken(absl::string_view value, TokenType type);
  Token createToken(TokenType type);

  char advance();

  bool match(char c);
  char peek();
  char peekBy(int steps);

  bool isAtEnd();

  absl::string_view source_;

  // The current token range: [start_, current_)
  int start_{ 0 };    // the starting point of the current token
  int current_{ 0 };  // the next position to read the character from.
};

}  // namespace sql
}  // namespace toyquery

#endif  // SQL_TOKENIZER_H
