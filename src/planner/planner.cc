#include "planner/planner.h"

namespace toyquery {
namespace planner {

absl::StatusOr<std::shared_ptr<toyquery::physicalplan::PhysicalPlan>> QueryPlanner::CreatePhysicalPlan(
    std::shared_ptr<toyquery::logicalplan::LogicalPlan> logical_plan) { }

absl::StatusOr<std::shared_ptr<toyquery::physicalplan::PhysicalExpression>> QueryPlanner::CreatePhysicalExpression(
    std::shared_ptr<toyquery::logicalplan::LogicalExpression> logical_expr,
    std::shared_ptr<toyquery::logicalplan::LogicalPlan> logical_plan) { }

}  // namespace planner
}  // namespace toyquery
