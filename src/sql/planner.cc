#include "sql/planner.h"

namespace toyquery {
namespace sql {

using ::toyquery::logicalplan::Column;
using ::toyquery::logicalplan::LogicalExpression;

absl::StatusOr<std::shared_ptr<toyquery::dataframe::DataFrame>> SqlPlanner::CreateDataFrame(
    std::shared_ptr<SqlSelect> select,
    std::map<absl::string_view, std::shared_ptr<toyquery::dataframe::DataFrame>> tables) {
  if (tables.find(select->table_name_) == tables.end()) {
    return absl::NotFoundError("table not found in the sql statement");
  }
  auto table = tables[select->table_name_];
}

absl::StatusOr<std::shared_ptr<toyquery::logicalplan::LogicalExpression>> SqlPlanner::createLogicalExpression(
    std::shared_ptr<SqlExpression> expr,
    std::shared_ptr<toyquery::dataframe::DataFrame> input) {
  switch (expr->GetType()) {
    case SqlExpressionType::SqlIdentifier:
      return std::make_shared<Column>(std::static_pointer_cast<SqlIdentifier>(expr)->id_);

    default: return absl::InvalidArgumentError("cannot create logical expression for the given sql expression.");
  }
}

}  // namespace sql
}  // namespace toyquery
