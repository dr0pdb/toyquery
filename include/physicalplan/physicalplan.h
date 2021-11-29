#ifndef PHYSICALPLAN_PHYSICALPLAN_H
#define PHYSICALPLAN_PHYSICALPLAN_H

#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arrow/api.h"
#include "common/macros.h"
#include "datasource/datasource.h"
#include "logicalplan/logicalexpression.h"

namespace toyquery {
namespace physicalplan {

/**
 * @brief Base class for all physical plans.
 *
 */
class PhysicalPlan {
 public:
  PhysicalPlan() = default;

  virtual ~PhysicalPlan() = 0;

  /**
   * @brief Get the schema of the physical plan.
   *
   * @return std::shared_ptr<arrow::Schema> Schema of the plan.
   */
  virtual absl::StatusOr<std::shared_ptr<arrow::Schema>> Schema() = 0;

  /**
   * @brief Get the children of the physical plan.
   *
   * @return std::vector<PhysicalPlan>: the children of the physical plan.
   */
  virtual std::vector<std::shared_ptr<PhysicalPlan>> Children() = 0;

  /**
   * @brief Prepare the physical plan.
   *
   * @return absl::Status: indicates the status of preparation
   */
  virtual absl::Status Prepare() = 0;

  /**
   * @brief Get the next record batch.
   *
   * @return absl::StatusOr<std::shared_ptr<arrow::RecordBatch>>: the next record batch if successful. Error status
   * otherwise.
   * @note The absl::NotFoundError indicates that all batches have been emitted.
   */
  virtual absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> Next() = 0;

  /**
   * @brief Get string representation to print for debugging.
   *
   * @return std::string: the string representation of the expression.
   */
  virtual std::string ToString();

 private:
  DISALLOW_COPY_AND_ASSIGN(PhysicalPlan);
};

}  // namespace physicalplan
}  // namespace toyquery

#endif  // PHYSICALPLAN_PHYSICALPLAN_H