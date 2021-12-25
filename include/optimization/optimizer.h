#ifndef OPTIMIZATION_OPTIMIZER_H
#define OPTIMIZATION_OPTIMIZER_H

#include "common/macros.h"
#include "logicalplan/logicalplan.h"

namespace toyquery {
namespace optimization {

/**
 * @brief Base class for all optimizer rules.
 *
 */
class OptimizerRule {
 public:
  OptimizerRule() = default;
  virtual ~OptimizerRule() = default;

  /**
   * @brief Optimize the given logical plan.
   *
   * @param logical_plan: the plan to optimize
   * @return std::shared_ptr<toyquery::logicalplan::LogicalPlan>: the optimized plan
   */
  virtual std::shared_ptr<toyquery::logicalplan::LogicalPlan> Optimize(
      std::shared_ptr<toyquery::logicalplan::LogicalPlan> logical_plan) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(OptimizerRule);
};

/**
 * @brief Rule to push the projections as much down as possible in query plan.
 *
 */
class ProjectionPushDownRule : public OptimizerRule {
 public:
  ProjectionPushDownRule() = default;
  ~ProjectionPushDownRule() override = default;

  /**
   * @copydoc OptimizerRule::Optimize
   */
  std::shared_ptr<toyquery::logicalplan::LogicalPlan> Optimize(
      std::shared_ptr<toyquery::logicalplan::LogicalPlan> logical_plan) override;

 private:
};

}  // namespace optimization
}  // namespace toyquery

#endif  // OPTIMIZATION_OPTIMIZER_H