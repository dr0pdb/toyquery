#include "physicalplan/physicalplan.h"

#include "common/arrow.h"

namespace toyquery {
namespace physicalplan {

absl::StatusOr<std::shared_ptr<arrow::Schema>> Scan::Schema() {
  auto schema = data_source_->Schema();
  if (projection_.empty()) return schema;
  return FilterSchema(schema, projection_);
}

std::vector<std::shared_ptr<PhysicalPlan>> Scan::Children() { return {}; }

absl::Status Scan::Prepare() { }

absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> Scan::Next() { }

std::string Scan::ToString() { return "todo"; }

}  // namespace physicalplan
}  // namespace toyquery
