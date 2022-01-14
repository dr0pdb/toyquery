#include "planner/planner.h"

namespace toyquery {
namespace planner {

using ::toyquery::logicalplan::LogicalExpression;
using ::toyquery::logicalplan::LogicalExpressionType;
using ::toyquery::logicalplan::LogicalPlan;
using ::toyquery::logicalplan::LogicalPlanType;
using ::toyquery::physicalplan::AddExpression;
using ::toyquery::physicalplan::AggregationExpression;
using ::toyquery::physicalplan::AndExpression;
using ::toyquery::physicalplan::Cast;
using ::toyquery::physicalplan::Column;
using ::toyquery::physicalplan::DivideExpression;
using ::toyquery::physicalplan::EqExpression;
using ::toyquery::physicalplan::GreaterThanEqualsExpression;
using ::toyquery::physicalplan::GreaterThanExpression;
using ::toyquery::physicalplan::HashAggregation;
using ::toyquery::physicalplan::LessThanEqualsExpression;
using ::toyquery::physicalplan::LessThanExpression;
using ::toyquery::physicalplan::LiteralDouble;
using ::toyquery::physicalplan::LiteralLong;
using ::toyquery::physicalplan::LiteralString;
using ::toyquery::physicalplan::MultiplyExpression;
using ::toyquery::physicalplan::NeqExpression;
using ::toyquery::physicalplan::OrExpression;
using ::toyquery::physicalplan::PhysicalExpression;
using ::toyquery::physicalplan::Projection;
using ::toyquery::physicalplan::Scan;
using ::toyquery::physicalplan::Selection;
using ::toyquery::physicalplan::SubtractExpression;

absl::StatusOr<std::shared_ptr<toyquery::physicalplan::PhysicalPlan>> QueryPlanner::CreatePhysicalPlan(
    std::shared_ptr<toyquery::logicalplan::LogicalPlan> logical_plan) {
  switch (logical_plan->Type()) {
    case LogicalPlanType::Scan: {
      auto logical_scan = std::static_pointer_cast<toyquery::logicalplan::Scan>(logical_plan);
      return std::make_shared<Scan>(logical_scan->source_, logical_scan->projection_);
    }
    case LogicalPlanType::Selection: {
      auto logical_selection = std::static_pointer_cast<toyquery::logicalplan::Selection>(logical_plan);
      ASSIGN_OR_RETURN(auto input, CreatePhysicalPlan(logical_selection->input_));
      ASSIGN_OR_RETURN(
          auto filter_expr, CreatePhysicalExpression(logical_selection->filter_expr_, logical_selection->input_));
      return std::make_shared<Selection>(input, filter_expr);
    }
    case LogicalPlanType::Projection: {
      auto logical_projection = std::static_pointer_cast<toyquery::logicalplan::Projection>(logical_plan);

      ASSIGN_OR_RETURN(auto input, CreatePhysicalPlan(logical_projection->input_));
      std::vector<std::shared_ptr<PhysicalExpression>> projection_exprs;
      for (auto& logical_proj_expr : logical_projection->expr_) {
        ASSIGN_OR_RETURN(auto physical_proj_expr, CreatePhysicalExpression(logical_proj_expr, logical_projection->input_));
        projection_exprs.push_back(physical_proj_expr);
      }
      ASSIGN_OR_RETURN(auto schema, logical_projection->Schema());

      return std::make_shared<Projection>(input, schema, projection_exprs);
    }
    case LogicalPlanType::Aggregation: {
      auto logical_aggregation = std::static_pointer_cast<toyquery::logicalplan::Aggregation>(logical_plan);

      ASSIGN_OR_RETURN(auto input, CreatePhysicalPlan(logical_aggregation->input_));
      std::vector<std::shared_ptr<PhysicalExpression>> group_exprs;
      for (auto& logical_group_expr : logical_aggregation->grouping_expr_) {
        ASSIGN_OR_RETURN(
            auto physical_group_expr, CreatePhysicalExpression(logical_group_expr, logical_aggregation->input_));
        group_exprs.push_back(physical_group_expr);
      }

      std::vector<std::shared_ptr<AggregationExpression>> aggregation_exprs;
      for (auto& logical_aggregation_expr : logical_aggregation->aggregation_expr_) {
        ASSIGN_OR_RETURN(
            auto physical_aggregation_expr,
            createAggregationExpression(logical_aggregation_expr, logical_aggregation->input_));
        aggregation_exprs.push_back(physical_aggregation_expr);
      }

      ASSIGN_OR_RETURN(auto schema, logical_aggregation->Schema());
      return std::make_shared<HashAggregation>(input, schema, group_exprs, aggregation_exprs);
    }
    default: return absl::InvalidArgumentError("invalid type of logical plan");
  }

  return absl::InternalError("unreachable code");
}

absl::StatusOr<std::shared_ptr<AggregationExpression>> QueryPlanner::createAggregationExpression(
    std::shared_ptr<toyquery::logicalplan::AggregateExpression> logical_aggregation_expr,
    std::shared_ptr<toyquery::logicalplan::LogicalPlan> input_plan) { }

absl::StatusOr<std::shared_ptr<toyquery::physicalplan::PhysicalExpression>> QueryPlanner::CreatePhysicalExpression(
    std::shared_ptr<LogicalExpression> logical_expr,
    std::shared_ptr<LogicalPlan> input_plan) {
#define CAST_LOGICAL_EXPRESSION_TO_TYPE(lhs, expr, typ) lhs = std::static_pointer_cast<typ>(expr);

#define CONVERT_BINARY_LOGICAL_EXPRESS_TO_PHYSICAL(ltp, ptp)                               \
  auto casted_expr = std::static_pointer_cast<ltp>(logical_expr);                          \
  ASSIGN_OR_RETURN2(auto l, l, CreatePhysicalExpression(casted_expr->left_, input_plan));  \
  ASSIGN_OR_RETURN2(auto r, r, CreatePhysicalExpression(casted_expr->right_, input_plan)); \
  return std::make_shared<ptp>(l, r);

  switch (logical_expr->type()) {
    case LogicalExpressionType::LiteralString: {
      CAST_LOGICAL_EXPRESSION_TO_TYPE(auto string_expr, logical_expr, toyquery::logicalplan::LiteralString);
      return std::make_shared<LiteralString>(string_expr->value_);
    }
    case LogicalExpressionType::LiteralLong: {
      CAST_LOGICAL_EXPRESSION_TO_TYPE(auto long_expr, logical_expr, toyquery::logicalplan::LiteralLong);
      return std::make_shared<LiteralLong>(long_expr->value_);
    }
    case LogicalExpressionType::LiteralDouble: {
      CAST_LOGICAL_EXPRESSION_TO_TYPE(auto double_expr, logical_expr, toyquery::logicalplan::LiteralDouble);
      return std::make_shared<LiteralDouble>(double_expr->value_);
    }
    case LogicalExpressionType::ColumnIndex: {
      CAST_LOGICAL_EXPRESSION_TO_TYPE(auto column_idx_expr, logical_expr, toyquery::logicalplan::ColumnIndex);
      return std::make_shared<Column>(column_idx_expr->index_);
    }
    case LogicalExpressionType::Column: {
      CAST_LOGICAL_EXPRESSION_TO_TYPE(auto column_expr, logical_expr, toyquery::logicalplan::Column);
      ASSIGN_OR_RETURN(auto schema, input_plan->Schema());
      auto col_index = std::distance(
          schema->field_names().begin(),
          std::find(schema->field_names().begin(), schema->field_names().end(), column_expr->name_));
      if (col_index == schema->num_fields()) { return absl::InvalidArgumentError("column with the given name not found"); }
      return std::make_shared<Column>(col_index);
    }
    case LogicalExpressionType::Alias: {
      CAST_LOGICAL_EXPRESSION_TO_TYPE(auto alias_expr, logical_expr, toyquery::logicalplan::Alias);
      return CreatePhysicalExpression(alias_expr->expr_, input_plan);
    }
    case LogicalExpressionType::Cast: {
      CAST_LOGICAL_EXPRESSION_TO_TYPE(auto cast_expr, logical_expr, toyquery::logicalplan::Cast);
      ASSIGN_OR_RETURN(auto expr, CreatePhysicalExpression(cast_expr->expr_, input_plan));
      return std::make_shared<Cast>(expr, cast_expr->type_);
    }
    // Binary expressions
    case LogicalExpressionType::Eq: {
      CONVERT_BINARY_LOGICAL_EXPRESS_TO_PHYSICAL(toyquery::logicalplan::Eq, EqExpression);
    }
    case LogicalExpressionType::Neq: {
      CONVERT_BINARY_LOGICAL_EXPRESS_TO_PHYSICAL(toyquery::logicalplan::Neq, NeqExpression);
    }
    case LogicalExpressionType::Gt: {
      CONVERT_BINARY_LOGICAL_EXPRESS_TO_PHYSICAL(toyquery::logicalplan::Gt, GreaterThanExpression);
    }
    case LogicalExpressionType::GtEq: {
      CONVERT_BINARY_LOGICAL_EXPRESS_TO_PHYSICAL(toyquery::logicalplan::GtEq, GreaterThanEqualsExpression);
    }
    case LogicalExpressionType::Lt: {
      CONVERT_BINARY_LOGICAL_EXPRESS_TO_PHYSICAL(toyquery::logicalplan::Lt, LessThanExpression);
    }
    case LogicalExpressionType::LtEq: {
      CONVERT_BINARY_LOGICAL_EXPRESS_TO_PHYSICAL(toyquery::logicalplan::LtEq, LessThanEqualsExpression);
    }
    case LogicalExpressionType::And: {
      CONVERT_BINARY_LOGICAL_EXPRESS_TO_PHYSICAL(toyquery::logicalplan::And, AndExpression);
    }
    case LogicalExpressionType::Or: {
      CONVERT_BINARY_LOGICAL_EXPRESS_TO_PHYSICAL(toyquery::logicalplan::Or, OrExpression);
    }
    case LogicalExpressionType::Add: {
      CONVERT_BINARY_LOGICAL_EXPRESS_TO_PHYSICAL(toyquery::logicalplan::Add, AddExpression);
    }
    case LogicalExpressionType::Subtract: {
      CONVERT_BINARY_LOGICAL_EXPRESS_TO_PHYSICAL(toyquery::logicalplan::Subtract, SubtractExpression);
    }
    case LogicalExpressionType::Multiply: {
      CONVERT_BINARY_LOGICAL_EXPRESS_TO_PHYSICAL(toyquery::logicalplan::Multiply, MultiplyExpression);
    }
    case LogicalExpressionType::Divide: {
      CONVERT_BINARY_LOGICAL_EXPRESS_TO_PHYSICAL(toyquery::logicalplan::Divide, DivideExpression);
    }
    default: return absl::InvalidArgumentError("invalid logical expression to convert to a physical expression");
  }

#undef CAST_LITERAL_EXPRESSION_TO_TYPE
}

}  // namespace planner
}  // namespace toyquery
