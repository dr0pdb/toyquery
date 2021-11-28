#ifndef DATAFRAME_DATAFRAME_H
#define DATAFRAME_DATAFRAME_H

#include <vector>

#include "arrow/api.h"
#include "common/macros.h"
#include "logicalplan/logicalexpression.h"
#include "logicalplan/logicalplan.h"

namespace toyquery {
namespace dataframe {

using ::toyquery::logicalplan::LogicalExpression;
using ::toyquery::logicalplan::LogicalPlan;

/**
 * @brief An interface to easily create logical plans.
 *
 */
class DataFrame {
 public:
  DataFrame() = default;
  virtual ~DataFrame() = 0;

  /**
   * @brief Apply a projection
   *
   * @param expr the projection expressions
   * @return std::shared_ptr<DataFrame> the dataframe with projection applied
   */
  virtual std::shared_ptr<DataFrame> Project(std::vector<std::shared_ptr<LogicalExpression>> expr) = 0;

  /**
   * @brief Apply a filter on the dataframe
   *
   * @param expr the filter expression
   * @return std::shared_ptr<DataFrame> the dataframe with filter applied
   */
  virtual std::shared_ptr<DataFrame> Filter(std::shared_ptr<LogicalExpression> expr) = 0;

  /**
   * @brief Apply aggregation on the dataframe
   *
   * @param group_by the grouping expressions
   * @param aggregate_expr the aggregation expressions
   * @return std::shared_ptr<DataFrame> the dataframe after applying the aggregation plan
   */
  virtual std::shared_ptr<DataFrame> Aggregate(
      std::vector<std::shared_ptr<LogicalExpression>> group_by,
      std::vector<std::shared_ptr<LogicalExpression>> aggregate_expr) = 0;

  /**
   * @brief Get the schema of the dataframe
   *
   * @return std::shared_ptr<arrow::Schema> the schema of the dataframe
   */
  virtual absl::StatusOr<std::shared_ptr<arrow::Schema>> GetSchema() = 0;

  /**
   * @brief Get the logical plan of the dataframe
   *
   * @return std::shared_ptr<LogicalPlan> the logical plan
   */
  virtual std::shared_ptr<LogicalPlan> GetLogicalPlan() = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(DataFrame);
};

class DataFrameImpl : public DataFrame {
 public:
  DataFrameImpl(std::shared_ptr<LogicalPlan> plan) : plan_{ std::move(plan) } { }

  ~DataFrameImpl() override = default;

  /**
   * @copydoc DataFrame::Project
   */
  std::shared_ptr<DataFrame> Project(std::vector<std::shared_ptr<LogicalExpression>> expr) override;

  /**
   * @copydoc DataFrame::Filter
   */
  std::shared_ptr<DataFrame> Filter(std::shared_ptr<LogicalExpression> expr) override;

  /**
   * @copydoc DataFrame::Aggregate
   */
  std::shared_ptr<DataFrame> Aggregate(
      std::vector<std::shared_ptr<LogicalExpression>> group_by,
      std::vector<std::shared_ptr<LogicalExpression>> aggregate_expr) override;

  /**
   * @copydoc DataFrame::GetSchema
   */
  absl::StatusOr<std::shared_ptr<arrow::Schema>> GetSchema() override;

  /**
   * @copydoc DataFrame::GetLogicalPlan
   */
  std::shared_ptr<LogicalPlan> GetLogicalPlan() override;

 private:
  std::shared_ptr<LogicalPlan> plan_;
};

}  // namespace dataframe
}  // namespace toyquery

#endif  // DATAFRAME_DATAFRAME_H