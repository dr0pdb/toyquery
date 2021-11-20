#ifndef COMMON_DEBUG_H
#define COMMON_DEBUG_H

#include <string>

#include "logicalplan/logicalplan.h"

namespace toyquery {

/**
 * @brief Format the logical plan for debugging.
 *
 * @param plan: the logical plan to format.
 * @param indent: the level of indentation to use.
 * @return std::string: the formatted string.
 */
std::string FormatLogicalPlan(toyquery::logicalplan::LogicalPlan plan, int indent = 0);

}  // namespace toyquery

#endif  // COMMON_DEBUG_H