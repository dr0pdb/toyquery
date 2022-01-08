#ifndef LOGICALPLAN_UTILS_H
#define LOGICALPLAN_UTILS_H

#include <memory>

#include "logicalplan/logicalexpression.h"

namespace toyquery {
namespace logicalplan {

using ::toyquery::logicalplan::LogicalExpressionType;

/**
 * @brief Check if the given logical expression contains an aggregate expression.
 *
 * @param expr: the expr
 * @return boolean indicating if the given expr contains an aggregate expression
 */
bool IsAggregateExpression(std::shared_ptr<toyquery::logicalplan::LogicalExpression> expr) {
  switch (expr->type()) {
    case LogicalExpressionType::Sum:
    case LogicalExpressionType::Avg:
    case LogicalExpressionType::Max:
    case LogicalExpressionType::Min:
    case LogicalExpressionType::Count: {
      return true;
    }

    case LogicalExpressionType::Alias: {
      return IsAggregateExpression(std::static_pointer_cast<Alias>(expr)->expr_);
    }
    case LogicalExpressionType::Cast: {
      return IsAggregateExpression(std::static_pointer_cast<Cast>(expr)->expr_);
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
      return IsAggregateExpression(binary_expr->left_) || IsAggregateExpression(binary_expr->right_);
    }
  }

  return false;
}

}  // namespace logicalplan
}  // namespace toyquery

#endif  // LOGICALPLAN_UTILS_H