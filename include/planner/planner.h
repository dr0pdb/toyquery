#ifndef PLANNER_PLANNER_H
#define PLANNER_PLANNER_H

#include <memory>

#include "absl/status/statusor.h"
#include "logicalplan/logicalplan.h"
#include "physicalplan/physicalplan.h"

namespace toyquery {
namespace planner {

/**
 * @brief QueryPlanner converts a logical plan to physical plan that can be executed by the query engine.
 *
 */
class QueryPlanner {
 public:
  /**
   * @brief Create a Physical Plan from the given Logical plan
   *
   * @param logical_plan: the given logical plan
   * @return absl::StatusOr<std::shared_ptr<toyquery::physicalplan::PhysicalPlan>>: the generated physical plan or error
   * status
   */
  absl::StatusOr<std::shared_ptr<toyquery::physicalplan::PhysicalPlan>> CreatePhysicalPlan(
      std::shared_ptr<toyquery::logicalplan::LogicalPlan> logical_plan);

  /**
   * @brief Create a Physical Expression from the given Logical expression
   *
   * @param logical_expr: the given logical expression
   * @param input_plan: the input logical plan
   * @return absl::StatusOr<std::shared_ptr<toyquery::physicalplan::PhysicalExpression>>: the generated physical expression
   * or error status
   */
  absl::StatusOr<std::shared_ptr<toyquery::physicalplan::PhysicalExpression>> CreatePhysicalExpression(
      std::shared_ptr<toyquery::logicalplan::LogicalExpression> logical_expr,
      std::shared_ptr<toyquery::logicalplan::LogicalPlan> input_plan);

 private:
  absl::StatusOr<std::shared_ptr<toyquery::physicalplan::AggregationExpression>> createAggregationExpression(
      std::shared_ptr<toyquery::logicalplan::AggregateExpression> logical_aggregation_expr,
      std::shared_ptr<toyquery::logicalplan::LogicalPlan> input_plan);
};

}  // namespace planner
}  // namespace toyquery

#endif  // PLANNER_PLANNER_H