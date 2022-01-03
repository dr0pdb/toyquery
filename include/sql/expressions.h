#ifndef SQL_EXPRESSIONS_H
#define SQL_EXPRESSIONS_H

#include <memory>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"

namespace toyquery {
namespace sql {

/**
 * @brief Indicates the type of the SqlExpression.
 */
enum class SqlExpressionType {
  SqlIdentifier,
  SqlBinaryExpression,
  SqlLong,
  SqlString,
  SqlDouble,
  SqlFunction,
  SqlAlias,
  SqlCast,
  SqlSort,
  SqlSelect
};

/**
 * @brief The base structure for all sql expressions.
 *
 */
struct SqlExpression {
  virtual ~SqlExpression() = default;

  /**
   * @brief Get the Type object
   *
   * @return SqlExpressionType: the type of sql expression
   */
  virtual SqlExpressionType GetType() = 0;

  /**
   * @brief Get the string representation for debugging
   *
   * @return std::string
   */
  virtual std::string ToString() = 0;
};

struct SqlIdentifier : public SqlExpression {
  SqlIdentifier(absl::string_view id);

  /**
   * @copydoc SqlExpression::GetType
   */
  SqlExpressionType GetType() override;

  /**
   * @copydoc SqlExpression::ToString
   */
  std::string ToString() override;

  absl::string_view id_;
};

struct SqlBinaryExpression : public SqlExpression {
  SqlBinaryExpression(std::shared_ptr<SqlExpression> left, absl::string_view op, std::shared_ptr<SqlExpression> right);

  /**
   * @copydoc SqlExpression::GetType
   */
  SqlExpressionType GetType() override;

  /**
   * @copydoc SqlExpression::ToString
   */
  std::string ToString() override;

  std::shared_ptr<SqlExpression> left_;
  absl::string_view op_;
  std::shared_ptr<SqlExpression> right_;
};

struct SqlLong : public SqlExpression {
  SqlLong(long value);

  /**
   * @copydoc SqlExpression::GetType
   */
  SqlExpressionType GetType() override;

  /**
   * @copydoc SqlExpression::ToString
   */
  std::string ToString() override;

  long value_;
};

struct SqlString : public SqlExpression {
  SqlString(absl::string_view value);

  /**
   * @copydoc SqlExpression::GetType
   */
  SqlExpressionType GetType() override;

  /**
   * @copydoc SqlExpression::ToString
   */
  std::string ToString() override;

  absl::string_view value_;
};

struct SqlDouble : public SqlExpression {
  SqlDouble(double value);

  /**
   * @copydoc SqlExpression::GetType
   */
  SqlExpressionType GetType() override;

  /**
   * @copydoc SqlExpression::ToString
   */
  std::string ToString() override;

  double value_;
};

struct SqlFunction : public SqlExpression {
  SqlFunction(absl::string_view id, std::vector<std::shared_ptr<SqlExpression>> args);

  /**
   * @copydoc SqlExpression::GetType
   */
  SqlExpressionType GetType() override;

  /**
   * @copydoc SqlExpression::ToString
   */
  std::string ToString() override;

  absl::string_view id_;
  std::vector<std::shared_ptr<SqlExpression>> args_;
};

struct SqlAlias : public SqlExpression {
  SqlAlias(std::shared_ptr<SqlExpression> expr, std::shared_ptr<SqlIdentifier> alias);

  /**
   * @copydoc SqlExpression::GetType
   */
  SqlExpressionType GetType() override;

  /**
   * @copydoc SqlExpression::ToString
   */
  std::string ToString() override;

  std::shared_ptr<SqlExpression> expr_;
  std::shared_ptr<SqlIdentifier> alias_;
};

struct SqlCast : public SqlExpression {
  SqlCast(std::shared_ptr<SqlExpression> expr, std::shared_ptr<SqlIdentifier> data_type);

  /**
   * @copydoc SqlExpression::GetType
   */
  SqlExpressionType GetType() override;

  /**
   * @copydoc SqlExpression::ToString
   */
  std::string ToString() override;

  std::shared_ptr<SqlExpression> expr_;
  std::shared_ptr<SqlIdentifier> data_type_;
};

struct SqlSort : public SqlExpression {
  SqlSort(std::shared_ptr<SqlExpression> expr, bool asc);

  /**
   * @copydoc SqlExpression::GetType
   */
  SqlExpressionType GetType() override;

  /**
   * @copydoc SqlExpression::ToString
   */
  std::string ToString() override;

  std::shared_ptr<SqlExpression> expr_;
  bool asc_;
};

struct SqlRelation : public SqlExpression { };

struct SqlSelect : public SqlRelation {
  SqlSelect(
      std::vector<std::shared_ptr<SqlExpression>> projection,
      std::shared_ptr<SqlExpression> selection,
      std::vector<std::shared_ptr<SqlExpression>> group_by,
      std::vector<std::shared_ptr<SqlSort>> order_by,
      std::shared_ptr<SqlExpression> having,
      absl::string_view table_name);

  /**
   * @copydoc SqlExpression::GetType
   */
  SqlExpressionType GetType() override;

  /**
   * @copydoc SqlExpression::ToString
   */
  std::string ToString() override;

  std::vector<std::shared_ptr<SqlExpression>> projection_;
  std::shared_ptr<SqlExpression> selection_;
  std::vector<std::shared_ptr<SqlExpression>> group_by_;
  std::vector<std::shared_ptr<SqlSort>> order_by_;
  std::shared_ptr<SqlExpression> having_;
  absl::string_view table_name_;
};

/**
 * @brief All the possible operators supported in the SqlBinaryExpression.
 *
 * Useful for switch cases.
 */
enum class SqlBinaryExpressionOperator {
  And,
  Or,
  Equal,
  NotEqual,
  GreaterThan,
  GreaterThanEquals,
  LessThan,
  LessThanEquals,
  Plus,
  Minus,
  Multiplication,
  Division,
  Modulo
};

static std::unordered_map<absl::string_view, SqlBinaryExpressionOperator> operators = {
  { "AND", SqlBinaryExpressionOperator::And },          { "OR", SqlBinaryExpressionOperator::Or },
  { "=", SqlBinaryExpressionOperator::Equal },          { "!=", SqlBinaryExpressionOperator::NotEqual },
  { ">", SqlBinaryExpressionOperator::GreaterThan },    { ">=", SqlBinaryExpressionOperator::GreaterThanEquals },
  { "<", SqlBinaryExpressionOperator::LessThan },       { "<=", SqlBinaryExpressionOperator::LessThanEquals },
  { "+", SqlBinaryExpressionOperator::Plus },           { "-", SqlBinaryExpressionOperator::Minus },
  { "*", SqlBinaryExpressionOperator::Multiplication }, { "/", SqlBinaryExpressionOperator::Division },
  { "%", SqlBinaryExpressionOperator::Modulo }
};

}  // namespace sql
}  // namespace toyquery

#endif  // SQL_EXPRESSIONS_H
