#ifndef SQL_PLANNER_H
#define SQL_PLANNER_H

#include <map>
#include <unordered_set>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "dataframe/dataframe.h"
#include "expressions.h"
#include "logicalplan/logicalexpression.h"

namespace toyquery {
namespace sql {

/**
 * @brief The SqlPlanner generates a logical plan from the SQL syntax trees.
 */
class SqlPlanner {
 public:
  /**
   * @brief Create a Data Frame object from the parsed sql select statements
   *
   * @param select: the parsed AST
   * @param tables: the tables registered
   * @return absl::StatusOr<std::shared_ptr<toyquery::dataframe::DataFrame>>: the created data frame
   */
  absl::StatusOr<std::shared_ptr<toyquery::dataframe::DataFrame>> CreateDataFrame(
      std::shared_ptr<SqlSelect> select,
      std::map<absl::string_view, std::shared_ptr<toyquery::dataframe::DataFrame>> tables);

 private:
  absl::StatusOr<std::unordered_set<absl::string_view>> getReferencedColumns(
      std::vector<std::shared_ptr<toyquery::logicalplan::LogicalExpression>> projection_exprs);

  absl::StatusOr<std::unordered_set<absl::string_view>> getReferencedColumnsBySelection(
      std::shared_ptr<SqlSelect> select,
      std::shared_ptr<toyquery::dataframe::DataFrame> table);

  absl::Status getColumnFromExpr(
      std::shared_ptr<toyquery::logicalplan::LogicalExpression> expr,
      std::unordered_set<absl::string_view>& accumulator);

  absl::StatusOr<int> countAggregationExpressions(
      std::vector<std::shared_ptr<toyquery::logicalplan::LogicalExpression>> projection_exprs);

  absl::StatusOr<std::shared_ptr<toyquery::logicalplan::LogicalExpression>> createLogicalExpression(
      std::shared_ptr<SqlExpression> expr,
      std::shared_ptr<toyquery::dataframe::DataFrame> input);

  absl::StatusOr<std::shared_ptr<arrow::DataType>> parseDataType(absl::string_view type_string);
};

}  // namespace sql
}  // namespace toyquery

#endif  // SQL_PLANNER_H
