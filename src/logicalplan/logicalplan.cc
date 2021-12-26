#include "logicalplan/logicalplan.h"

#include "common/arrow.h"
#include "common/macros.h"

namespace toyquery {
namespace logicalplan {

absl::StatusOr<std::shared_ptr<arrow::Schema>> Scan::Schema() {
  ASSIGN_OR_RETURN(auto schema, source_->Schema());
  if (projection_.empty()) { return schema; }
  return FilterSchema(schema, projection_);
}

std::vector<std::shared_ptr<LogicalPlan>> Scan::Children() { return {}; }

LogicalPlanType Scan::Type() { return LogicalPlanType::Scan; }

std::string Scan::ToString() { return "todo"; }

absl::StatusOr<std::shared_ptr<arrow::Schema>> Projection::Schema() {
  std::vector<std::shared_ptr<arrow::Field>> projected_fields;
  for (auto& expr : expr_) {
    ASSIGN_OR_RETURN(std::shared_ptr<arrow::Field> projected_field, expr->ToField(input_));
    projected_fields.push_back(projected_field);
  }

  auto endianness = input_->Schema().value()->endianness();
  return std::make_shared<arrow::Schema>(projected_fields, endianness);
}

std::vector<std::shared_ptr<LogicalPlan>> Projection::Children() { return {}; }

LogicalPlanType Projection::Type() { return LogicalPlanType::Projection; }

std::string Projection::ToString() { return "todo"; }

absl::StatusOr<std::shared_ptr<arrow::Schema>> Selection::Schema() { return input_->Schema(); }

std::vector<std::shared_ptr<LogicalPlan>> Selection::Children() { return { input_ }; }

LogicalPlanType Selection::Type() { return LogicalPlanType::Selection; }

std::string Selection::ToString() { return "todo"; }

absl::StatusOr<std::shared_ptr<arrow::Schema>> Aggregation::Schema() {
  std::vector<std::shared_ptr<arrow::Field>> output_fields;
  for (auto& expr : grouping_expr_) {
    ASSIGN_OR_RETURN(std::shared_ptr<arrow::Field> projected_field, expr->ToField(input_));
    output_fields.push_back(projected_field);
  }

  for (auto& expr : aggregation_expr_) {
    ASSIGN_OR_RETURN(std::shared_ptr<arrow::Field> projected_field, expr->ToField(input_));
    output_fields.push_back(projected_field);
  }

  auto endianness = input_->Schema().value()->endianness();
  return std::make_shared<arrow::Schema>(output_fields, endianness);
}

std::vector<std::shared_ptr<LogicalPlan>> Aggregation::Children() { return { input_ }; }

LogicalPlanType Aggregation::Type() { return LogicalPlanType::Aggregation; }

std::string Aggregation::ToString() { return "todo"; }

}  // namespace logicalplan
}  // namespace toyquery
