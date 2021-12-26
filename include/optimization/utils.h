#ifndef OPTIMIZATION_UTILS_H
#define OPTIMIZATION_UTILS_H

#include <memory>
#include <unordered_set>
#include <vector>

#include "logicalplan/logicalexpression.h"
#include "logicalplan/logicalplan.h"

namespace toyquery {
namespace optimization {

/**
 * @brief Extract all the columns referenced by the given expressions.
 *
 * @param expressions: the list of expressions
 * @param input: the input expression
 * @param accumulator: the container in which to put the column names in
 * @return absl::Status: the status of the operation
 */
absl::Status ExtractColumns(
    std::vector<std::shared_ptr<toyquery::logicalplan::LogicalExpression>> expressions,
    std::shared_ptr<toyquery::logicalplan::LogicalPlan> input,
    std::unordered_set<std::string>& accumulator);

/**
 * @brief Extract all the columns referenced by a single expression.
 *
 * @param expression: the expression
 * @param input: the input expression
 * @param accumulator: the accumulator to accumulate the column names
 */
absl::Status ExtractColumns(
    std::shared_ptr<toyquery::logicalplan::LogicalExpression>& expression,
    std::shared_ptr<toyquery::logicalplan::LogicalPlan> input,
    std::unordered_set<std::string>& accumulator);

}  // namespace optimization
}  // namespace toyquery

#endif  // OPTIMIZATION_UTILS_H