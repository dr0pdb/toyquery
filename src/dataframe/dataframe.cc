#include "dataframe/dataframe.h"

#include "logicalplan/logicalplan.h"

namespace toyquery {
namespace dataframe {

using ::toyquery::logicalplan::Aggregation;
using ::toyquery::logicalplan::Projection;
using ::toyquery::logicalplan::Selection;

std::shared_ptr<DataFrame> DataFrameImpl::Project(std::vector<std::shared_ptr<LogicalExpression>> expr) {
  return std::make_shared<DataFrameImpl>(std::make_shared<Projection>(plan_, expr));
}

std::shared_ptr<DataFrame> DataFrameImpl::Filter(std::shared_ptr<LogicalExpression> expr) {
  return std::make_shared<DataFrameImpl>(std::make_shared<Selection>(plan_, expr));
}

std::shared_ptr<DataFrame> DataFrameImpl::Aggregate(
    std::vector<std::shared_ptr<LogicalExpression>> group_by,
    std::vector<std::shared_ptr<LogicalExpression>> aggregate_expr) {
  return std::make_shared<DataFrameImpl>(std::make_shared<Aggregation>(plan_, group_by, aggregate_expr));
}

absl::StatusOr<std::shared_ptr<arrow::Schema>> DataFrameImpl::GetSchema() { return plan_->Schema(); }

std::shared_ptr<LogicalPlan> DataFrameImpl::GetLogicalPlan() { return plan_; }

}  // namespace dataframe
}  // namespace toyquery
