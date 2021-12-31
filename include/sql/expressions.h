#ifndef SQL_EXPRESSIONS_H
#define SQL_EXPRESSIONS_H

#include <memory>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"

namespace toyquery {
namespace sql {

/**
 * @brief The base structure for all sql expressions.
 *
 */
struct SqlExpression {
  virtual ~SqlExpression() = default;

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
   * @copydoc SqlExpression::ToString
   */
  std::string ToString() override;

  absl::string_view id_;
};

struct SqlBinaryExpression : public SqlExpression {
  SqlBinaryExpression(std::shared_ptr<SqlExpression> left, std::string op, std::shared_ptr<SqlExpression> right);

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
   * @copydoc SqlExpression::ToString
   */
  std::string ToString() override;

  long value_;
};

struct SqlString : public SqlExpression {
  SqlString(absl::string_view value);

  /**
   * @copydoc SqlExpression::ToString
   */
  std::string ToString() override;

  absl::string_view value_;
};

struct SqlDouble : public SqlExpression {
  SqlDouble(double value);

  /**
   * @copydoc SqlExpression::ToString
   */
  std::string ToString() override;

  double value_;
};

struct SqlFunction : public SqlExpression {
  SqlFunction(absl::string_view id, std::vector<std::shared_ptr<SqlExpression>> args);

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
   * @copydoc SqlExpression::ToString
   */
  std::string ToString() override;

  std::shared_ptr<SqlExpression> expr_;
  std::shared_ptr<SqlIdentifier> alias_;
};

struct SqlCast : public SqlExpression {
  SqlCast(std::shared_ptr<SqlExpression> expr, std::shared_ptr<SqlIdentifier> data_type);

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
      std::vector<std::shared_ptr<SqlExpression>> order_by,
      std::shared_ptr<SqlExpression> having,
      absl::string_view table_name);

  /**
   * @copydoc SqlExpression::ToString
   */
  std::string ToString() override;

  std::vector<std::shared_ptr<SqlExpression>> projection_;
  std::shared_ptr<SqlExpression> selection_;
  std::vector<std::shared_ptr<SqlExpression>> group_by_;
  std::vector<std::shared_ptr<SqlExpression>> order_by_;
  std::shared_ptr<SqlExpression> having_;
  absl::string_view table_name_;
};

}  // namespace sql
}  // namespace toyquery

#endif  // SQL_EXPRESSIONS_H
