#ifndef EXECUTION_EXECUTION_CONTEXT_H
#define EXECUTION_EXECUTION_CONTEXT_H

#include "absl/status/statusor.h"
#include "dataframe/dataframe.h"

namespace toyquery {
namespace execution {

using ::toyquery::dataframe::DataFrame;

/**
 * @brief Utility to create an initial dataframe from a datasource.
 *
 */
class ExecutionContext {
 public:
  ExecutionContext() = default;
  ~ExecutionContext() = default;

  /**
   * @brief Create a dataframe from a CSV file.
   *
   * @param filename: the csv file name.
   * @return absl::StatusOr<std::shared_ptr<DataFrame>>: the created dataframe.
   */
  absl::StatusOr<std::shared_ptr<DataFrame>> CSV(const std::string& filename);

  /**
   * @brief Create a dataframe from a Parquet file.
   *
   * @param filename: the parquet file name
   * @return absl::StatusOr<std::shared_ptr<DataFrame>>: the created dataframe.
   */
  absl::StatusOr<std::shared_ptr<DataFrame>> Parquet(const std::string& filename);

 private:
};

}  // namespace execution
}  // namespace toyquery

#endif  // EXECUTION_EXECUTION_CONTEXT_H