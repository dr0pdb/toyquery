#ifndef LOGICALPLAN_LOGICALEXPRESSION_H
#define LOGICALPLAN_LOGICALEXPRESSION_H

#include <string>
#include <vector>

#include "arrow/api.h"
#include "common/macros.h"
#include "logicalplan.h"

namespace toyquery {
namespace logicalplan {

/**
 * @brief Base class for all logical expressions.
 *
 * The logical expression provides information needed during the planning phase such as the name and data type of the
 * expression.
 */
class LogicalExpression {
 public:
  LogicalExpression() = default;
  ~LogicalExpression();

  /**
   * @brief Get arrow::Field of value when evaluating this expression against the input.
   *
   * Get the metadata(arrow::Field) info of the value that will be produced when this expression is evaluated on the provided
   * logical plan.
   *
   * @param input: the logical plan on which this expression will be evaluated.
   * @return std::shared_ptr<arrow::Field>: the metadata information of the produced value.
   */
  virtual std::shared_ptr<arrow::Field> ToField(LogicalPlan input) = 0;

  /**
   * @brief Get string representation to print for debugging.
   *
   * @return std::string: the string representation of the expression.
   */
  virtual std::string ToString();

 private:
  DISALLOW_COPY_AND_ASSIGN(LogicalExpression);
};

/**
 * @brief A reference to a column of table.
 */
class ColumnExpression : public LogicalExpression {
 public:
  ColumnExpression(std::string name);
  ~ColumnExpression();

  /**
   * @brief Get arrow::Field of value when evaluating this expression against the input.
   *
   * Get the metadata(arrow::Field) info of the value that will be produced when this expression is evaluated on the provided
   * logical plan.
   *
   * @param input: the logical plan on which this expression will be evaluated.
   * @return std::shared_ptr<arrow::Field>: the metadata information of the produced value.
   */
  std::shared_ptr<arrow::Field> ToField(LogicalPlan input) override;

  /**
   * @brief Get string representation to print for debugging.
   *
   * @return std::string: the string representation of the expression.
   */
  std::string ToString() override;

 private:
  DISALLOW_COPY_AND_ASSIGN(ColumnExpression);

  std::string name_;
};

}  // namespace logicalplan
}  // namespace toyquery

#endif  // LOGICALPLAN_LOGICALEXPRESSION_H