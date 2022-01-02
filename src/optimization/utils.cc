#include "optimization/utils.h"

#include "common/macros.h"

namespace toyquery {
namespace optimization {

namespace {

using ::toyquery::logicalplan::Alias;
using ::toyquery::logicalplan::BinaryExpression;
using ::toyquery::logicalplan::Cast;
using ::toyquery::logicalplan::Column;
using ::toyquery::logicalplan::ColumnIndex;
using ::toyquery::logicalplan::LogicalExpression;
using ::toyquery::logicalplan::LogicalExpressionType;
using ::toyquery::logicalplan::LogicalPlan;

}  // namespace

absl::Status ExtractColumns(
    std::vector<std::shared_ptr<LogicalExpression>> expressions,
    std::shared_ptr<LogicalPlan> input,
    std::unordered_set<std::string>& accumulator) {
  for (auto& expr : expressions) { CHECK_OK_OR_RETURN(ExtractColumns(expr, input, accumulator)); }
  return absl::OkStatus();
}

absl::Status ExtractColumns(
    std::shared_ptr<LogicalExpression>& expression,
    std::shared_ptr<LogicalPlan> input,
    std::unordered_set<std::string>& accumulator) {
  switch (expression->type()) {
    // column refs
    case LogicalExpressionType::Column: {
      auto col_expr = std::static_pointer_cast<Column>(expression);
      accumulator.insert(std::string(col_expr->name_));  // TODO: can we use absl::string_view?
      break;
    }
    case LogicalExpressionType::ColumnIndex: {
      auto col_index_expr = std::static_pointer_cast<ColumnIndex>(expression);
      ASSIGN_OR_RETURN(auto schema, input->Schema());
      accumulator.insert(schema->fields()[col_index_expr->index_]->name());
      break;
    }

    // misc.
    case LogicalExpressionType::Alias: {
      CHECK_OK_OR_RETURN(ExtractColumns(std::static_pointer_cast<Alias>(expression)->expr_, input, accumulator));
      break;
    }
    case LogicalExpressionType::Cast: {
      CHECK_OK_OR_RETURN(ExtractColumns(std::static_pointer_cast<Cast>(expression)->expr_, input, accumulator));
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
      auto binary_expr = std::static_pointer_cast<BinaryExpression>(expression);
      CHECK_OK_OR_RETURN(ExtractColumns(binary_expr->left_, input, accumulator));
      CHECK_OK_OR_RETURN(ExtractColumns(binary_expr->right_, input, accumulator));
      break;
    }

    // literals
    case LogicalExpressionType::LiteralDouble:
    case LogicalExpressionType::LiteralLong:
    case LogicalExpressionType::LiteralString: break;

    default: return absl::InternalError("Unsupported expression for extracting columns.");
  }

  return absl::OkStatus();
}

}  // namespace optimization
}  // namespace toyquery
