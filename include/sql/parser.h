#ifndef SQL_PARSER_H
#define SQL_PARSER_H

#include <vector>

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

 private:
  std::vector<Token> tokens_;
};

}  // namespace sql
}  // namespace toyquery

#endif  // SQL_PARSER_H