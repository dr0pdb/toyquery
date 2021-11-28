#include "execution/execution_context.h"

#include "dataframe/dataframe.h"

namespace toyquery {
namespace execution {

using ::toyquery::dataframe::DataFrameImpl;
using ::toyquery::logicalplan::Scan;

absl::StatusOr<std::shared_ptr<DataFrame>> ExecutionContext::CSV(const std::string& filename) { return absl::OkStatus(); }

absl::StatusOr<std::shared_ptr<DataFrame>> ExecutionContext::Parquet(const std::string& filename) {
  return absl::OkStatus();
}

}  // namespace execution
}  // namespace toyquery