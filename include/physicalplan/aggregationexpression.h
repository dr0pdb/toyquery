#ifndef PHYSICALPLAN_AGGREGATIONEXPRESSION_H
#define PHYSICALPLAN_AGGREGATIONEXPRESSION_H

#include <memory>

#include "physicalplan/accumulator.h"
#include "physicalplan/physicalexpression.h"

namespace toyquery {
namespace physicalplan {

/**
 * @brief Base class for Aggregation expressions such as Max, Min, Sum etc.
 *
 * The input to the aggregation expression is the expression which should be aggregated.
 * For eg: SUM (4 * COL_1 + 3 * COL_2), the input would be the 4 * COL_1 + 3 * COL_2 expression.
 */
class AggregationExpression {
 public:
  AggregationExpression(std::shared_ptr<PhysicalExpression> input) : input_{ input } { }

  virtual ~AggregationExpression() = default;

  /**
   * @brief Get the Input Expression
   *
   * @return std::shared_ptr<PhysicalExpression> the input expression
   */
  std::shared_ptr<PhysicalExpression> GetInputExpression() { return input_; }

  /**
   * @brief Create the accumulator to be used with the aggregation expression.
   *
   * @return absl::StatusOr<std::shared_ptr<Accumulator>> the accumulator
   */
  virtual absl::StatusOr<std::shared_ptr<Accumulator>> CreateAccumulator() = 0;

 private:
  std::shared_ptr<PhysicalExpression> input_;
};

/**
 * @brief Max aggregation expression
 *
 */
class MaxExpression : public AggregationExpression {
 public:
  MaxExpression(std::shared_ptr<PhysicalExpression> input) : AggregationExpression(input) { }

  /**
   * @copydoc AggregationExpression::CreateAccumulator
   */
  absl::StatusOr<std::shared_ptr<Accumulator>> CreateAccumulator() override { return std::make_shared<MaxAccumulator>(); }
};

/**
 * @brief Min aggregation expression
 *
 */
class MinExpression : public AggregationExpression {
 public:
  MinExpression(std::shared_ptr<PhysicalExpression> input) : AggregationExpression(input) { }

  /**
   * @copydoc AggregationExpression::CreateAccumulator
   */
  absl::StatusOr<std::shared_ptr<Accumulator>> CreateAccumulator() override { return std::make_shared<MinAccumulator>(); }
};

/**
 * @brief Sum aggregation expression
 *
 */
class SumExpression : public AggregationExpression {
 public:
  SumExpression(std::shared_ptr<PhysicalExpression> input) : AggregationExpression(input) { }

  /**
   * @copydoc AggregationExpression::CreateAccumulator
   */
  absl::StatusOr<std::shared_ptr<Accumulator>> CreateAccumulator() override { return std::make_shared<SumAccumulator>(); }
};

}  // namespace physicalplan
}  // namespace toyquery

#endif  // PHYSICALPLAN_AGGREGATIONEXPRESSION_H