#ifndef LOGICALPLAN_LOGICALPLAN_H
#define LOGICALPLAN_LOGICALPLAN_H

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "arrow/api.h"
#include "common/macros.h"
#include "datasource/datasource.h"

namespace toyquery {
namespace logicalplan {

/**
 * @brief Base class for all logical plans.
 *
 */
class LogicalPlan {
 public:
  LogicalPlan() = default;

  virtual ~LogicalPlan() = 0;

  /**
   * @brief Get the schema of the logical plan.
   *
   * @return std::shared_ptr<arrow::Schema> Schema of the plan.
   */
  virtual absl::StatusOr<std::shared_ptr<arrow::Schema>> Schema() = 0;

  /**
   * @brief Get the children of the logical plan.
   *
   * @return std::vector<LogicalPlan>: the children of the logical plan.
   */
  virtual std::vector<LogicalPlan> Children() = 0;

  /**
   * @brief Get string representation to print for debugging.
   *
   * @return std::string: the string representation of the expression.
   */
  virtual std::string ToString();

 private:
  DISALLOW_COPY_AND_ASSIGN(LogicalPlan);
};

class Scan : public LogicalPlan {
 public:
  Scan(std::string path, std::shared_ptr<toyquery::datasource::DataSource> source, std::vector<std::string> projection)
      : path_{ std::move(path) },
        source_{ std::move(source) },
        projection_{ std::move(projection) } { }

  ~Scan() = default;

  /**
   * @brief Get the schema of the logical plan.
   *
   * @return std::shared_ptr<arrow::Schema> Schema of the plan.
   */
  absl::StatusOr<std::shared_ptr<arrow::Schema>> Schema() override;

  /**
   * @brief Get the children of the logical plan.
   *
   * @return std::vector<LogicalPlan>: the children of the logical plan.
   */
  std::vector<LogicalPlan> Children() override;

  /**
   * @brief Get string representation to print for debugging.
   *
   * @return std::string: the string representation of the expression.
   */
  std::string ToString() override;

 private:
  std::string path_;
  std::shared_ptr<toyquery::datasource::DataSource> source_;
  std::vector<std::string> projection_;
};

}  // namespace logicalplan
}  // namespace toyquery

#endif  // LOGICALPLAN_LOGICALPLAN_H