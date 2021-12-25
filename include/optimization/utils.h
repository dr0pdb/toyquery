#ifndef OPTIMIZATION_UTILS_H
#define OPTIMIZATION_UTILS_H

#include <memory>
#include <unordered_set>
#include <vector>

#include "logicalplan/logicalexpression.h"

namespace toyquery {
namespace optimization {

/**
 * @brief Extract all the columns referenced by the given expressions.
 *
 * @param expressions: the list of expressions
 * @return std::unordered_set<std::string>: the set of column names
 */
std::unordered_set<std::string> ExtractColumns(
    std::vector<std::shared_ptr<toyquery::logicalplan::LogicalExpression>> expressions);

/**
 * @brief Extract all the columns referenced by a single expression.
 *
 * @param expression: the expression
 * @param accumulator: the accumulator to accumulate the column names
 */
void ExtractColumns(
    std::shared_ptr<toyquery::logicalplan::LogicalExpression>& expression,
    std::unordered_set<std::string>& accumulator);

}  // namespace optimization
}  // namespace toyquery

#endif  // OPTIMIZATION_UTILS_H