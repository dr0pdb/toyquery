#include "physicalplan/physicalexpression.h"

namespace toyquery {
namespace physicalplan {

absl::StatusOr<std::shared_ptr<arrow::Array>> Column::Evaluate(const std::shared_ptr<arrow::RecordBatch> input) {
  if (idx_ < 0 || idx_ >= input->num_columns()) { return absl::OutOfRangeError("index out of range"); }
  return input->column(idx_);
}

std::string Column::ToString() { return "todo"; }

absl::StatusOr<std::shared_ptr<arrow::Array>> LiteralLong::Evaluate(const std::shared_ptr<arrow::RecordBatch> input) {
  arrow::Int64Builder builder;
  builder.Reserve(input->num_rows());
  for (int i = 0; i < input->num_rows(); i++) { builder.UnsafeAppend(val_); }

  auto array = builder.Finish();
  if (!array.ok()) { return absl::InternalError(array.status().detail()->ToString()); }
  return *array;
}

std::string LiteralLong::ToString() { return "todo"; }

absl::StatusOr<std::shared_ptr<arrow::Array>> LiteralDouble::Evaluate(const std::shared_ptr<arrow::RecordBatch> input) {
  arrow::DoubleBuilder builder;
  builder.Reserve(input->num_rows());
  for (int i = 0; i < input->num_rows(); i++) { builder.UnsafeAppend(val_); }

  auto array = builder.Finish();
  if (!array.ok()) { return absl::InternalError(array.status().detail()->ToString()); }
  return *array;
}

std::string LiteralDouble::ToString() { return "todo"; }

absl::StatusOr<std::shared_ptr<arrow::Array>> LiteralString::Evaluate(const std::shared_ptr<arrow::RecordBatch> input) {
  arrow::StringBuilder builder;
  builder.Reserve(input->num_rows());
  for (int i = 0; i < input->num_rows(); i++) { builder.UnsafeAppend(val_); }

  auto array = builder.Finish();
  if (!array.ok()) { return absl::InternalError(array.status().detail()->ToString()); }
  return *array;
}

std::string LiteralString::ToString() { return "todo"; }

}  // namespace physicalplan
}  // namespace toyquery
