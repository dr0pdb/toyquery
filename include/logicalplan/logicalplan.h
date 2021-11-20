#ifndef LOGICALPLAN_LOGICALPLAN_H
#define LOGICALPLAN_LOGICALPLAN_H

#include <string>
#include <vector>

#include "arrow/api.h"
#include "common/macros.h"

namespace toyquery {
namespace logicalplan {

/**
 * @brief Base class for all logical plans.
 *
 */
class LogicalPlan {
 public:
  LogicalPlan() = default;

  virtual ~LogicalPlan();

  /**
   * @brief Get the schema of the logical plan.
   *
   * @return std::shared_ptr<arrow::Schema> Schema of the plan.
   */
  virtual std::shared_ptr<arrow::Schema> Schema() = 0;

  /**
   * @brief Get the children of the logical plan.
   *
   * @return std::vector<LogicalPlan>: the children of the logical plan.
   */
  virtual std::vector<LogicalPlan> Children() = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(LogicalPlan);
};

}  // namespace logicalplan
}  // namespace toyquery

#endif  // LOGICALPLAN_LOGICALPLAN_H