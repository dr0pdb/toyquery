#include "logicalplan/logicalplan.h"

#include "common/arrow.h"

namespace toyquery {
namespace logicalplan {

absl::StatusOr<std::shared_ptr<arrow::Schema>> Scan::Schema() {
  auto schema = source_->Schema();
  if (projection_.empty()) { return schema; }
  return FilterSchema(schema, projection_);
}

std::vector<LogicalPlan> Scan::Children() { return {}; }

std::string Scan::ToString() { return "todo"; }

}  // namespace logicalplan
}  // namespace toyquery
