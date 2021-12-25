#include "optimization/utils.h"

namespace toyquery {
namespace optimization {

std::unordered_set<std::string> ExtractColumns(
    std::vector<std::shared_ptr<toyquery::logicalplan::LogicalExpression>> expressions) {
  std::unordered_set<std::string> accumulator;
  for (auto& expr : expressions) { ExtractColumns(expr, accumulator); }
  return accumulator;
}

void ExtractColumns(
    std::shared_ptr<toyquery::logicalplan::LogicalExpression>& expression,
    std::unordered_set<std::string>& accumulator) { }

}  // namespace optimization
}  // namespace toyquery
