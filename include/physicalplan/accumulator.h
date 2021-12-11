#ifndef PHYSICALPLAN_ACCUMULATOR_H
#define PHYSICALPLAN_ACCUMULATOR_H

#include <memory>

#include "absl/status/statusor.h"
#include "arrow/api.h"
#include "common/macros.h"
#include "common/status.h"

namespace toyquery {
namespace physicalplan {

/**
 * @brief Base class for accumulators which accumulate a set of values into one.
 *
 */
class Accumulator {
 public:
  Accumulator() = default;
  virtual ~Accumulator() = default;

  /**
   * @brief Accumulate the value.
   *
   * @return absl::Status the result of the operation
   */
  virtual absl::Status Accumulate(std::shared_ptr<arrow::Scalar> value) = 0;

  /**
   * @brief Obtain the final value accumulated.
   *
   * @return absl::StatusOr<std::shared_ptr<arrow::Scalar>> the final value
   */
  virtual absl::StatusOr<std::shared_ptr<arrow::Scalar>> FinalValue() = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(Accumulator);
};

/**
 * @brief Accumulator to calculate the max value among a set of values.
 *
 */
class MaxAccumulator : public Accumulator {
  MaxAccumulator() = default;

  /**
   * @copydoc Accumulator::Accumulate
   */
  absl::Status Accumulate(std::shared_ptr<arrow::Scalar> value) override;

  /**
   * @copydoc Accumulator::FinalValue
   */
  absl::StatusOr<std::shared_ptr<arrow::Scalar>> FinalValue() override;

 private:
  std::shared_ptr<arrow::Scalar> value_{};
};

}  // namespace physicalplan
}  // namespace toyquery

#endif  // PHYSICALPLAN_ACCUMULATOR_H