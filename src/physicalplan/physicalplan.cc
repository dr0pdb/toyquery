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

absl::StatusOr<std::shared_ptr<arrow::Schema>> Projection::Schema() { return schema_; }

std::vector<std::shared_ptr<PhysicalPlan>> Projection::Children() { return { input_ }; }

absl::Status Projection::Prepare() { return input_->Prepare(); }

absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> Projection::Next() {
  ASSIGN_OR_RETURN(auto batch, input_->Next());
  std::vector<std::shared_ptr<arrow::Array>> columns;

  for (auto& expr : projection_) {
    ASSIGN_OR_RETURN(auto col, expr->Evaluate(batch));
    columns.push_back(col);
  }

  return arrow::RecordBatch::Make(schema_, projection_.size(), columns);
}

std::string Projection::ToString() { return "todo"; }

}  // namespace physicalplan
}  // namespace toyquery
