#include "sql/planner.h"

namespace toyquery {
namespace sql {

using ::toyquery::logicalplan::Add;
using ::toyquery::logicalplan::AggregateExpression;
using ::toyquery::logicalplan::Alias;
using ::toyquery::logicalplan::And;
using ::toyquery::logicalplan::Avg;
using ::toyquery::logicalplan::BinaryExpression;
using ::toyquery::logicalplan::Cast;
using ::toyquery::logicalplan::Column;
using ::toyquery::logicalplan::Count;
using ::toyquery::logicalplan::Divide;
using ::toyquery::logicalplan::Eq;
using ::toyquery::logicalplan::Gt;
using ::toyquery::logicalplan::GtEq;
using ::toyquery::logicalplan::LiteralDouble;
using ::toyquery::logicalplan::LiteralLong;
using ::toyquery::logicalplan::LiteralString;
using ::toyquery::logicalplan::LogicalExpression;
using ::toyquery::logicalplan::LogicalExpressionType;
using ::toyquery::logicalplan::Lt;
using ::toyquery::logicalplan::LtEq;
using ::toyquery::logicalplan::Max;
using ::toyquery::logicalplan::Min;
using ::toyquery::logicalplan::Modulus;
using ::toyquery::logicalplan::Multiply;
using ::toyquery::logicalplan::Neq;
using ::toyquery::logicalplan::Or;
using ::toyquery::logicalplan::Subtract;
using ::toyquery::logicalplan::Sum;

absl::StatusOr<std::shared_ptr<toyquery::dataframe::DataFrame>> SqlPlanner::CreateDataFrame(
    std::shared_ptr<SqlSelect> select,
    std::map<absl::string_view, std::shared_ptr<toyquery::dataframe::DataFrame>> tables) {
  if (tables.find(select->table_name_) == tables.end()) {
    return absl::NotFoundError("table not found in the sql statement");
  }
  auto table = tables[select->table_name_];

  // convert all projections to logical expressions
  std::vector<std::shared_ptr<toyquery::logicalplan::LogicalExpression>> projection_exprs;
  for (auto& proj_expr : select->projection_) {
    ASSIGN_OR_RETURN(auto proj_logical_expr, createLogicalExpression(proj_expr, table));
    projection_exprs.push_back(std::move(proj_logical_expr));
  }

  // get all the columns referenced in the projection
  ASSIGN_OR_RETURN(auto referenced_columns, getReferencedColumns(projection_exprs));
}

absl::StatusOr<std::unordered_set<absl::string_view>> SqlPlanner::getReferencedColumns(
    std::vector<std::shared_ptr<toyquery::logicalplan::LogicalExpression>> projection_exprs) {
  std::unordered_set<absl::string_view> accum;
  for (auto& projection_expr : projection_exprs) { CHECK_OK_OR_RETURN(getColumnFromExpr(projection_expr, accum)); }
  return accum;
}

absl::Status SqlPlanner::getColumnFromExpr(
    std::shared_ptr<toyquery::logicalplan::LogicalExpression> expr,
    std::unordered_set<absl::string_view>& accumulator) {
  switch (expr->type()) {
    case LogicalExpressionType::Column: {
      auto col_expr = std::static_pointer_cast<Column>(expr);
      accumulator.insert(col_expr->name_);
      break;
    }
    case LogicalExpressionType::Alias: {
      auto alias_expr = std::static_pointer_cast<Alias>(expr);
      CHECK_OK_OR_RETURN(getColumnFromExpr(alias_expr->expr_, accumulator));
      break;
    }
    case LogicalExpressionType::Cast: {
      auto cast_expr = std::static_pointer_cast<Cast>(expr);
      CHECK_OK_OR_RETURN(getColumnFromExpr(cast_expr->expr_, accumulator));
      break;
    }

    // binary expressions
    case LogicalExpressionType::And:
    case LogicalExpressionType::Or:
    case LogicalExpressionType::Eq:
    case LogicalExpressionType::Neq:
    case LogicalExpressionType::Gt:
    case LogicalExpressionType::GtEq:
    case LogicalExpressionType::Lt:
    case LogicalExpressionType::LtEq:
    case LogicalExpressionType::Add:
    case LogicalExpressionType::Subtract:
    case LogicalExpressionType::Multiply:
    case LogicalExpressionType::Divide:
    case LogicalExpressionType::Modulus: {
      auto binary_expr = std::static_pointer_cast<BinaryExpression>(expr);
      CHECK_OK_OR_RETURN(getColumnFromExpr(binary_expr->left_, accumulator));
      CHECK_OK_OR_RETURN(getColumnFromExpr(binary_expr->right_, accumulator));
      break;
    }

    // aggregation expressions
    case LogicalExpressionType::Sum:
    case LogicalExpressionType::Avg:
    case LogicalExpressionType::Max:
    case LogicalExpressionType::Min:
    case LogicalExpressionType::Count: {
      auto aggr_expr = std::static_pointer_cast<AggregateExpression>(expr);
      CHECK_OK_OR_RETURN(getColumnFromExpr(aggr_expr->expr_, accumulator));
      break;
    }

    default: return absl::InvalidArgumentError("invalid logical expression for extracting columns from");
  }

  return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<toyquery::logicalplan::LogicalExpression>> SqlPlanner::createLogicalExpression(
    std::shared_ptr<SqlExpression> expr,
    std::shared_ptr<toyquery::dataframe::DataFrame> input) {
  switch (expr->GetType()) {
    case SqlExpressionType::SqlIdentifier:
      return std::make_shared<Column>(std::static_pointer_cast<SqlIdentifier>(expr)->id_);
    case SqlExpressionType::SqlAlias: {
      auto alias_expr = std::static_pointer_cast<SqlAlias>(expr);
      ASSIGN_OR_RETURN(auto internal_expr, createLogicalExpression(alias_expr->expr_, input));
      return std::make_shared<Alias>(internal_expr, alias_expr->alias_->id_);
    }

    case SqlExpressionType::SqlString:
      return std::make_shared<LiteralString>(std::static_pointer_cast<SqlString>(expr)->value_);
    case SqlExpressionType::SqlDouble:
      return std::make_shared<LiteralDouble>(std::static_pointer_cast<SqlDouble>(expr)->value_);
    case SqlExpressionType::SqlLong: return std::make_shared<LiteralLong>(std::static_pointer_cast<SqlLong>(expr)->value_);

    case SqlExpressionType::SqlBinaryExpression: {
      auto binary_expr = std::static_pointer_cast<SqlBinaryExpression>(expr);
      ASSIGN_OR_RETURN(auto left_expr, createLogicalExpression(binary_expr->left_, input));
      ASSIGN_OR_RETURN(auto right_expr, createLogicalExpression(binary_expr->right_, input));

      auto op_type = OPERATORS.find(binary_expr->op_);
      if (op_type == OPERATORS.end()) { return absl::InvalidArgumentError("invalid operator"); }
      switch (op_type->second) {
        // comparison operators
        case SqlBinaryExpressionOperator::Equal: return std::make_shared<Eq>(left_expr, right_expr);
        case SqlBinaryExpressionOperator::NotEqual: return std::make_shared<Neq>(left_expr, right_expr);
        case SqlBinaryExpressionOperator::GreaterThan: return std::make_shared<Gt>(left_expr, right_expr);
        case SqlBinaryExpressionOperator::GreaterThanEquals: return std::make_shared<GtEq>(left_expr, right_expr);
        case SqlBinaryExpressionOperator::LessThan: return std::make_shared<Lt>(left_expr, right_expr);
        case SqlBinaryExpressionOperator::LessThanEquals: return std::make_shared<LtEq>(left_expr, right_expr);

        // boolean operators
        case SqlBinaryExpressionOperator::And: return std::make_shared<And>(left_expr, right_expr);
        case SqlBinaryExpressionOperator::Or: return std::make_shared<Or>(left_expr, right_expr);

        // mathematical operators
        case SqlBinaryExpressionOperator::Plus: return std::make_shared<Add>(left_expr, right_expr);
        case SqlBinaryExpressionOperator::Minus: return std::make_shared<Subtract>(left_expr, right_expr);
        case SqlBinaryExpressionOperator::Multiplication: return std::make_shared<Multiply>(left_expr, right_expr);
        case SqlBinaryExpressionOperator::Division: return std::make_shared<Divide>(left_expr, right_expr);
        case SqlBinaryExpressionOperator::Modulo: return std::make_shared<Modulus>(left_expr, right_expr);
      }
      break;
    }

    case SqlExpressionType::SqlCast: {
      auto cast_expr = std::static_pointer_cast<SqlCast>(expr);
      ASSIGN_OR_RETURN(auto cast_input, createLogicalExpression(cast_expr->expr_, input));
      ASSIGN_OR_RETURN(auto data_type, parseDataType(cast_expr->data_type_->id_));
      return std::make_shared<Cast>(cast_input, data_type);
    }
    case SqlExpressionType::SqlFunction: {
      auto func_expr = std::static_pointer_cast<SqlFunction>(expr);
      auto func_id = FUNCTIONS.find(func_expr->id_);
      if (func_id == FUNCTIONS.end()) { return absl::InvalidArgumentError("invalid function"); }
      switch (func_id->second) {
        case SqlFunctionType::Min: {
          ASSIGN_OR_RETURN(auto min_input, createLogicalExpression(func_expr->args_[0], input));
          return std::make_shared<Min>(min_input);
        }
        case SqlFunctionType::Max: {
          ASSIGN_OR_RETURN(auto max_input, createLogicalExpression(func_expr->args_[0], input));
          return std::make_shared<Max>(max_input);
        }
        case SqlFunctionType::Sum: {
          ASSIGN_OR_RETURN(auto sum_input, createLogicalExpression(func_expr->args_[0], input));
          return std::make_shared<Sum>(sum_input);
        }
        case SqlFunctionType::Avg: {
          ASSIGN_OR_RETURN(auto avg_input, createLogicalExpression(func_expr->args_[0], input));
          return std::make_shared<Avg>(avg_input);
        }
        case SqlFunctionType::Count: {
          ASSIGN_OR_RETURN(auto count_input, createLogicalExpression(func_expr->args_[0], input));
          return std::make_shared<Count>(count_input);
        }
        default: return absl::InvalidArgumentError("invalid function id");
      }
    }

    default: return absl::InvalidArgumentError("cannot create logical expression for the given sql expression.");
  }
}

absl::StatusOr<std::shared_ptr<arrow::DataType>> SqlPlanner::parseDataType(absl::string_view type_string) {
  if (type_string.compare("double")) { return arrow::float64(); }

  return absl::InvalidArgumentError("invalid data type in cast expression");
}

}  // namespace sql
}  // namespace toyquery
