#include "sql/tokenizer.h"

#include "common/macros.h"

namespace toyquery {
namespace sql {

#define EOF -1

Tokenizer::Tokenizer(absl::string_view source) : source_{ std::move(source) } { }

absl::StatusOr<std::vector<Token>> Tokenizer::Tokenize() {
  std::vector<Token> tokens;

  while (!isAtEnd()) {
    start_ = current_;
    ASSIGN_OR_RETURN(auto t, scanToken());
    tokens.push_back(t);
  }

  tokens.push_back(Token("", TokenType::SPECIAL_EOF, source_.length()));
  return tokens;
}

absl::StatusOr<Token> Tokenizer::scanToken() {
  skipWhitespace();
  char c = advance();

  switch (c) {
    case '(': return createToken(TokenType::SYMBOL_LEFT_PAREN);
    case ')': return createToken(TokenType::SYMBOL_RIGHT_PARENT);

    case ',': return createToken(TokenType::SYMBOL_COMMA);
    case '.': return createToken(TokenType::SYMBOL_PERIOD);
    case ';': return createToken(TokenType::SYMBOL_SEMICOLON);

    case '-': return createToken(TokenType::OPERATOR_MINUS);
    case '+': return createToken(TokenType::OPERATOR_PLUS);
    case '*': return createToken(TokenType::OPERATOR_ASTERISK);
    case '/': return createToken(TokenType::OPERATOR_SLASH);

    case '!': return createToken(match('=') ? TokenType::OPERATOR_NOT_EQUAL : TokenType::OPERATOR_EXCLAMATION);
    case '=': return createToken(match('=') ? TokenType::OPERATOR_EQUAL_EQUALS : TokenType::OPERATOR_EQUAL);
    case '<': return createToken(match('=') ? TokenType::OPERATOR_LESS_THAN_EQUAL_TO : TokenType::OPERATOR_LESS_THAN);
    case '>': return createToken(match('=') ? TokenType::OPERATOR_GREATER_THAN_EQUAL_TO : TokenType::OPERATOR_GREATER_THAN);

    case '"': return string();

    default: {
      if (isdigit(c)) {
        return number();
      } else if (isalpha(c)) {
        return identifier();
      } else {
        return absl::FailedPreconditionError("Unknown identifier " + c);
      }
    }
  }
}

absl::StatusOr<Token> Tokenizer::string() {
  while (peek() != '"' && !isAtEnd()) { advance(); }

  if (isAtEnd()) { return absl::FailedPreconditionError("Unmatched \" in string."); }

  // the closing "
  advance();

  return createToken(source_.substr(start_ + 1, current_ - start_ - 2), TokenType::LITERAL_STRING);
}

absl::StatusOr<Token> Tokenizer::number() {
  while (isdigit(peek())) { advance(); }

  if (peek() == '.' && isdigit(peekBy(1))) {
    advance();  // consume the period '.'

    while (isdigit(peek())) { advance(); }
  }

  return createToken(TokenType::LITERAL_DOUBLE);
}

absl::StatusOr<Token> Tokenizer::identifier() {
  while (isalnum(peek())) { advance(); }

  absl::string_view text = source_.substr(start_, current_ - start_);
  auto type = TokenType::LITERAL_IDENTIFIER;
  if (keywords.find(text) != keywords.end()) { type = keywords[text]; }

  return createToken(type);
}

/**
 * Useful utilities for the lexer
 */

void Tokenizer::skipWhitespace() {
  while (true) {
    char c = peek();
    switch (c) {
      case ' ':
      case '\t':
      case '\n':
      case '\r': {
        advance();
        break;
      }
      default: return;
    }
  }
}

Token Tokenizer::createToken(TokenType token_type) {
  return Token(source_.substr(start_, current_ - start_), token_type, current_);
}

#define BOUNDARY_CHECK(param) \
  if (source_.length() >= current_ + param) { return EOF; }

char Tokenizer::advance() {
  BOUNDARY_CHECK(0);
  current_++;
  return source_.at(current_ - 1);
}

bool Tokenizer::match(char c) {
  char curr = peek();
  if (curr == c) { advance(); }
  return curr == c;
}

char Tokenizer::peek() { return peekBy(0); }

char Tokenizer::peekBy(int steps) {
  BOUNDARY_CHECK(steps);
  return source_.at(current_ + steps);
}

bool Tokenizer::isAtEnd() { return current_ >= source_.length(); }

#undef BOUNDARY_CHECK
#undef EOF

}  // namespace sql
}  // namespace toyquery
