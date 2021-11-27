#ifndef LOGICALPLAN_LOGICALPLAN_H
#define LOGICALPLAN_LOGICALPLAN_H

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "arrow/api.h"
#include "common/macros.h"
#include "datasource/datasource.h"
#include "logicalplan/logicalexpression.h"

namespace toyquery {
namespace logicalplan {

/**
 * @brief Base class for all logical plans.
 *
 */
class LogicalPlan {
 public:
  LogicalPlan() = default;

  virtual ~LogicalPlan() = 0;

  /**
   * @brief Get the schema of the logical plan.
   *
   * @return std::shared_ptr<arrow::Schema> Schema of the plan.
   */
  virtual absl::StatusOr<std::shared_ptr<arrow::Schema>> Schema() = 0;

  /**
   * @brief Get the children of the logical plan.
   *
   * @return std::vector<LogicalPlan>: the children of the logical plan.
   */
  virtual std::vector<std::shared_ptr<LogicalPlan>> Children() = 0;

  /**
   * @brief Get string representation to print for debugging.
   *
   * @return std::string: the string representation of the expression.
   */
  virtual std::string ToString();

 private:
  DISALLOW_COPY_AND_ASSIGN(LogicalPlan);
};

/**
 * @brief Scan logical plan scans over a datasource applying an optional projection.
 *
 */
class Scan : public LogicalPlan {
 public:
  Scan(std::string path, std::shared_ptr<toyquery::datasource::DataSource> source, std::vector<std::string> projection)
      : path_{ std::move(path) },
        source_{ std::move(source) },
        projection_{ std::move(projection) } { }

  ~Scan() = default;

  /**
   * @copydoc LogicalPlan::Schema()
   */
  absl::StatusOr<std::shared_ptr<arrow::Schema>> Schema() override;

  /**
   * @copydoc LogicalPlan::Children()
   */
  std::vector<std::shared_ptr<LogicalPlan>> Children() override;

  /**
   * @copydoc LogicalPlan::ToString()
   */
  std::string ToString() override;

 private:
  std::string path_;
  std::shared_ptr<toyquery::datasource::DataSource> source_;
  std::vector<std::string> projection_;
};

/**
 * @brief Projection logical plan applies projection on top of another logical plan.
 *
 * The fields referenced by the projection expressions should be present in the schema of the input plan.
 */
class Projection : public LogicalPlan {
 public:
  Projection(std::shared_ptr<LogicalPlan> input, std::vector<std::shared_ptr<LogicalExpression>> expr)
      : input_{ std::move(input) },
        expr_{ std::move(expr) } { }

  ~Projection() = default;

  /**
   * @copydoc LogicalPlan::Schema()
   */
  absl::StatusOr<std::shared_ptr<arrow::Schema>> Schema() override;

  /**
   * @copydoc LogicalPlan::Children()
   */
  std::vector<std::shared_ptr<LogicalPlan>> Children() override;

  /**
   * @copydoc LogicalPlan::ToString()
   */
  std::string ToString() override;

 private:
  std::shared_ptr<LogicalPlan> input_;
  std::vector<std::shared_ptr<LogicalExpression>> expr_;
};

/**
 * @brief Selection selects (filters) the output of the input plan based on a filter expression.
 *
 */
class Selection : public LogicalPlan {
 public:
  Selection(std::shared_ptr<LogicalPlan> input, std::shared_ptr<LogicalExpression> filter_expr)
      : input_{ std::move(input) },
        filter_expr_{ std::move(filter_expr) } { }

  ~Selection() = default;

  /**
   * @copydoc LogicalPlan::Schema()
   *
   * Selection doesn't alter the schema of the input.
   */
  absl::StatusOr<std::shared_ptr<arrow::Schema>> Schema() override;

  /**
   * @copydoc LogicalPlan::Children()
   */
  std::vector<std::shared_ptr<LogicalPlan>> Children() override;

  /**
   * @copydoc LogicalPlan::ToString()
   */
  std::string ToString() override;

 private:
  std::shared_ptr<LogicalPlan> input_;
  std::shared_ptr<LogicalExpression> filter_expr_;
};

/**
 * @brief Aggregation plan calculates aggregates of the underlying data emitted by the input plan.
 *
 */
class Aggregation : public LogicalPlan {
 public:
  Aggregation(
      std::shared_ptr<LogicalPlan> input,
      std::vector<std::shared_ptr<LogicalExpression>> grouping_expr,
      std::vector<std::shared_ptr<LogicalExpression>> aggregation_expr)
      : input_{ std::move(input) },
        grouping_expr_{ std::move(grouping_expr) },
        aggregation_expr_{ std::move(aggregation_expr) } { }

  ~Aggregation() = default;

  /**
   * @copydoc LogicalPlan::Schema()
   *
   * The schema of Aggregation expression is [grouping expression] + [aggregation expressions]
   */
  absl::StatusOr<std::shared_ptr<arrow::Schema>> Schema() override;

  /**
   * @copydoc LogicalPlan::Children()
   */
  std::vector<std::shared_ptr<LogicalPlan>> Children() override;

  /**
   * @copydoc LogicalPlan::ToString()
   */
  std::string ToString() override;

 private:
  std::shared_ptr<LogicalPlan> input_;
  std::vector<std::shared_ptr<LogicalExpression>> grouping_expr_;
  std::vector<std::shared_ptr<LogicalExpression>> aggregation_expr_;
};

}  // namespace logicalplan
}  // namespace toyquery

#endif  // LOGICALPLAN_LOGICALPLAN_H