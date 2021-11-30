#include "physicalplan/physicalexpression.h"

namespace toyquery {
namespace physicalplan {

namespace {

using ::toyquery::common::GetMessageFromResult;
using ::toyquery::common::GetMessageFromResultLeftOrRight;
using ::toyquery::common::GetMessageFromStatus;

}  // namespace

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

absl::StatusOr<std::shared_ptr<arrow::Array>> BooleanExpression::Evaluate(const std::shared_ptr<arrow::RecordBatch> input) {
  ASSIGN_OR_RETURN(auto ll, left_->Evaluate(input));
  ASSIGN_OR_RETURN(auto rr, right_->Evaluate(input));

  if (ll->length() != rr->length()) {
    return absl::InternalError("Boolean expression operands do not have the same number of columns");
  }
  if (!ll->type()->Equals(rr->type())) {
    return absl::InternalError("Boolean expression operands do not have the same type");
  }

  return Compare(ll, rr);
}

absl::StatusOr<std::shared_ptr<arrow::Array>> BooleanExpression::Compare(
    const std::shared_ptr<arrow::Array> left,
    const std::shared_ptr<arrow::Array> right) {
  arrow::BooleanBuilder builder;
  builder.Reserve(left->length());

  for (int i = 0; i < left->length(); i++) {
    auto ls = left->GetScalar(i);
    auto rs = right->GetScalar(i);

    if (!(ls.ok() && rs.ok())) { return absl::InternalError(GetMessageFromResultLeftOrRight(ls, rs)); }

    // delegate to specific implementation.
    auto val = EvaluateBooleanExpression(*ls, *rs, left->type());
    if (!val.ok()) { return absl::InternalError(GetMessageFromStatus(val.status())); }

    builder.UnsafeAppend(*val);
  }

  auto array = builder.Finish();
  if (!array.ok()) { return absl::InternalError(array.status().detail()->ToString()); }
  return *array;
}

absl::StatusOr<std::shared_ptr<arrow::Array>> BinaryExpression::Evaluate(const std::shared_ptr<arrow::RecordBatch> input) {
  ASSIGN_OR_RETURN(auto ll, left_->Evaluate(input));
  ASSIGN_OR_RETURN(auto rr, right_->Evaluate(input));

  if (ll->length() != rr->length()) {
    return absl::InternalError("Binary expression operands do not have the same number of columns");
  }
  if (!ll->type()->Equals(rr->type())) {
    return absl::InternalError("Binary expression operands do not have the same type");
  }

  return EvaluateBinaryExpression(ll, rr);
}

}  // namespace physicalplan
}  // namespace toyquery
