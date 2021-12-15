#include "physicalplan/physicalplan.h"

#include "common/arrow.h"
#include "common/status.h"

namespace toyquery {
namespace physicalplan {

using toyquery::common::GetMessageFromResult;

absl::StatusOr<std::shared_ptr<arrow::Schema>> Scan::Schema() {
  ASSIGN_OR_RETURN(auto schema, data_source_->Schema());
  if (projection_.empty()) return schema;
  return FilterSchema(schema, projection_);
}

std::vector<std::shared_ptr<PhysicalPlan>> Scan::Children() { return {}; }

absl::Status Scan::Prepare() {
  ASSIGN_OR_RETURN(batch_reader_, data_source_->Scan(projection_));
  return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> Scan::Next() {
  auto maybe_batch = batch_reader_->Next();
  if (!maybe_batch.ok()) { return absl::InternalError(GetMessageFromResult(maybe_batch)); }
  return *maybe_batch;
}

std::string Scan::ToString() { return "todo"; }

}  // namespace physicalplan
}  // namespace toyquery
