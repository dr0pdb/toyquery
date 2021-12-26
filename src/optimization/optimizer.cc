#include "optimization/optimizer.h"

#include "optimization/utils.h"

namespace toyquery {
namespace optimization {

namespace {

using ::toyquery::logicalplan::Aggregation;
using ::toyquery::logicalplan::LogicalPlan;
using ::toyquery::logicalplan::LogicalPlanType;
using ::toyquery::logicalplan::Projection;
using ::toyquery::logicalplan::Selection;

}  // namespace

absl::StatusOr<std::shared_ptr<LogicalPlan>> Optimizer::Optimize(std::shared_ptr<LogicalPlan> logical_plan) {
  std::vector<std::unique_ptr<OptimizerRule>> rules = { std::make_unique<ProjectionPushDownRule>() };

  std::shared_ptr<LogicalPlan> optimized_plan = logical_plan;
  for (auto& rule : rules) { ASSIGN_OR_RETURN(optimized_plan, rule->Optimize(optimized_plan)); }

  return optimized_plan;
}

absl::StatusOr<std::shared_ptr<LogicalPlan>> ProjectionPushDownRule::Optimize(std::shared_ptr<LogicalPlan> logical_plan) {
  std::unordered_set<std::string> column_names;
  return pushDown(logical_plan, column_names);
}

absl::StatusOr<std::shared_ptr<LogicalPlan>> ProjectionPushDownRule::pushDown(
    std::shared_ptr<LogicalPlan> logical_plan,
    std::unordered_set<std::string> column_names) {
  switch (logical_plan->Type()) {
    case LogicalPlanType::Scan: {
      break;
    }
    case LogicalPlanType::Projection: {
      auto projection_plan = std::static_pointer_cast<Projection>(logical_plan);
      CHECK_OK_OR_RETURN(ExtractColumns(projection_plan->expr_, projection_plan->input_, column_names));
      ASSIGN_OR_RETURN(auto new_input, pushDown(projection_plan->input_, column_names));
      return std::make_shared<Projection>(new_input, projection_plan->expr_);
    }
    case LogicalPlanType::Selection: {
      auto selection_plan = std::static_pointer_cast<Selection>(logical_plan);
      CHECK_OK_OR_RETURN(ExtractColumns(selection_plan->filter_expr_, selection_plan->input_, column_names));
      ASSIGN_OR_RETURN(auto new_input, pushDown(selection_plan->input_, column_names));
      return std::make_shared<Selection>(new_input, selection_plan->filter_expr_);
    }
    case LogicalPlanType::Aggregation: {
      auto aggregation_plan = std::static_pointer_cast<Aggregation>(logical_plan);

      CHECK_OK_OR_RETURN(ExtractColumns(aggregation_plan->grouping_expr_, aggregation_plan->input_, column_names));
      for (auto& a : aggregation_plan->aggregation_expr_) {
        CHECK_OK_OR_RETURN(ExtractColumns(a->expr_, aggregation_plan->input_, column_names));
      }

      ASSIGN_OR_RETURN(auto new_input, pushDown(aggregation_plan->input_, column_names));
      return std::make_shared<Aggregation>(new_input, aggregation_plan->grouping_expr_, aggregation_plan->aggregation_expr_);
    }
    default: return absl::InternalError("Unsupported logical plan for projection push down optimization");
  }

  return absl::InternalError("Unreachable code");
}

}  // namespace optimization
}  // namespace toyquery
