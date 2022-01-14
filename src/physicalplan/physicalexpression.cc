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
  for (int i = 0; i < input->num_rows(); i++) { builder.UnsafeAppend(std::string(val_)); }

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
    auto val = EvaluateBooleanExpression(*ls, *rs);
    if (!val.ok()) { return absl::InternalError(GetMessageFromStatus(val.status())); }

    builder.UnsafeAppend(*val);
  }

  auto array = builder.Finish();
  if (!array.ok()) { return absl::InternalError(GetMessageFromResult(array)); }
  return *array;
}

absl::StatusOr<bool> EqExpression::EvaluateBooleanExpression(
    const std::shared_ptr<arrow::Scalar> left,
    const std::shared_ptr<arrow::Scalar> right) {
  return left->Equals(right);
}

absl::StatusOr<bool> NeqExpression::EvaluateBooleanExpression(
    const std::shared_ptr<arrow::Scalar> left,
    const std::shared_ptr<arrow::Scalar> right) {
  return !left->Equals(right);
}

absl::StatusOr<bool> AndExpression::EvaluateBooleanExpression(
    const std::shared_ptr<arrow::Scalar> left,
    const std::shared_ptr<arrow::Scalar> right) {
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto lv, arrow::BooleanScalar, left);
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto rv, arrow::BooleanScalar, right);
  return lv && rv;
}

absl::StatusOr<bool> OrExpression::EvaluateBooleanExpression(
    const std::shared_ptr<arrow::Scalar> left,
    const std::shared_ptr<arrow::Scalar> right) {
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto lv, arrow::BooleanScalar, left);
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto rv, arrow::BooleanScalar, right);
  return lv || rv;
}

absl::StatusOr<bool> LessThanExpression::EvaluateBooleanExpression(
    const std::shared_ptr<arrow::Scalar> left,
    const std::shared_ptr<arrow::Scalar> right) {
#define COMPARE_LESS_THAN_ARROW_SCALER(tp)                 \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto lv, tp, left);  \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto rv, tp, right); \
  return lv < rv;

  switch (left->type->id()) {
    case arrow::Type::BOOL: {
      COMPARE_LESS_THAN_ARROW_SCALER(arrow::BooleanScalar);
    }
    case arrow::Type::INT64: {
      COMPARE_LESS_THAN_ARROW_SCALER(arrow::Int64Scalar);
    }
    case arrow::Type::DOUBLE: {
      COMPARE_LESS_THAN_ARROW_SCALER(arrow::DoubleScalar);
    }
    case arrow::Type::STRING: {
      COMPARE_LESS_THAN_ARROW_SCALER(arrow::StringScalar);
    }

    default: return absl::InternalError("Unsupported left/right operand type for LessThan expression.");
  }

#undef COMPARE_LESS_THAN_ARROW_SCALER
}

absl::StatusOr<bool> LessThanEqualsExpression::EvaluateBooleanExpression(
    const std::shared_ptr<arrow::Scalar> left,
    const std::shared_ptr<arrow::Scalar> right) {
#define COMPARE_LESS_THAN_EQUALS_ARROW_SCALER(tp)          \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto lv, tp, left);  \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto rv, tp, right); \
  return lv <= rv;

  switch (left->type->id()) {
    case arrow::Type::BOOL: {
      COMPARE_LESS_THAN_EQUALS_ARROW_SCALER(arrow::BooleanScalar);
    }
    case arrow::Type::INT64: {
      COMPARE_LESS_THAN_EQUALS_ARROW_SCALER(arrow::Int64Scalar);
    }
    case arrow::Type::DOUBLE: {
      COMPARE_LESS_THAN_EQUALS_ARROW_SCALER(arrow::DoubleScalar);
    }
    case arrow::Type::STRING: {
      COMPARE_LESS_THAN_EQUALS_ARROW_SCALER(arrow::StringScalar);
    }

    default: return absl::InternalError("Unsupported left/right operand type for LessThanEquals expression.");
  }

#undef COMPARE_LESS_THAN_EQUALS_ARROW_SCALER
}

absl::StatusOr<bool> GreaterThanExpression::EvaluateBooleanExpression(
    const std::shared_ptr<arrow::Scalar> left,
    const std::shared_ptr<arrow::Scalar> right) {
#define COMPARE_GREATER_THAN_ARROW_SCALER(tp)              \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto lv, tp, left);  \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto rv, tp, right); \
  return lv > rv;

  switch (left->type->id()) {
    case arrow::Type::BOOL: {
      COMPARE_GREATER_THAN_ARROW_SCALER(arrow::BooleanScalar);
    }
    case arrow::Type::INT64: {
      COMPARE_GREATER_THAN_ARROW_SCALER(arrow::Int64Scalar);
    }
    case arrow::Type::DOUBLE: {
      COMPARE_GREATER_THAN_ARROW_SCALER(arrow::DoubleScalar);
    }
    case arrow::Type::STRING: {
      COMPARE_GREATER_THAN_ARROW_SCALER(arrow::StringScalar);
    }

    default: return absl::InternalError("Unsupported left/right operand type for GreaterThan expression.");
  }

#undef COMPARE_GREATER_THAN_ARROW_SCALER
}

absl::StatusOr<bool> GreaterThanEqualsExpression::EvaluateBooleanExpression(
    const std::shared_ptr<arrow::Scalar> left,
    const std::shared_ptr<arrow::Scalar> right) {
#define COMPARE_GREATER_THAN_EQUALS_ARROW_SCALER(tp)       \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto lv, tp, left);  \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto rv, tp, right); \
  return lv >= rv;

  switch (left->type->id()) {
    case arrow::Type::BOOL: {
      COMPARE_GREATER_THAN_EQUALS_ARROW_SCALER(arrow::BooleanScalar);
    }
    case arrow::Type::INT64: {
      COMPARE_GREATER_THAN_EQUALS_ARROW_SCALER(arrow::Int64Scalar);
    }
    case arrow::Type::DOUBLE: {
      COMPARE_GREATER_THAN_EQUALS_ARROW_SCALER(arrow::DoubleScalar);
    }
    case arrow::Type::STRING: {
      COMPARE_GREATER_THAN_EQUALS_ARROW_SCALER(arrow::StringScalar);
    }

    default: return absl::InternalError("Unsupported left/right operand type for GreaterThanEquals expression.");
  }

#undef COMPARE_GREATER_THAN_EQUALS_ARROW_SCALER
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

absl::StatusOr<std::shared_ptr<arrow::Array>> MathExpression::EvaluateBinaryExpression(
    const std::shared_ptr<arrow::Array> left,
    const std::shared_ptr<arrow::Array> right) {
#define EVALUATE_BINARY_EXPRESSION(builder_type, scaler_type)                                           \
  builder_type builder;                                                                                 \
  builder.Reserve(left->length());                                                                      \
                                                                                                        \
  for (int i = 0; i < left->length(); i++) {                                                            \
    auto ls = left->GetScalar(i);                                                                       \
    auto rs = right->GetScalar(i);                                                                      \
                                                                                                        \
    if (!(ls.ok() && rs.ok())) { return absl::InternalError(GetMessageFromResultLeftOrRight(ls, rs)); } \
                                                                                                        \
    auto val = EvaluateMathExpression(*ls, *rs);                                                        \
    if (!val.ok()) { return absl::InternalError(GetMessageFromStatus(val.status())); }                  \
                                                                                                        \
    builder.UnsafeAppend(std::static_pointer_cast<scaler_type>(*val)->value);                           \
  }                                                                                                     \
                                                                                                        \
  auto array = builder.Finish();                                                                        \
  if (!array.ok()) { return absl::InternalError(GetMessageFromResult(array)); }                         \
  return *array;

  switch (left->type()->id()) {
    case arrow::Type::INT64: {
      EVALUATE_BINARY_EXPRESSION(arrow::Int64Builder, arrow::Int64Scalar);
    }
    case arrow::Type::DOUBLE: {
      EVALUATE_BINARY_EXPRESSION(arrow::DoubleBuilder, arrow::DoubleScalar);
    }
    default: return absl::InternalError("Unsupported type in math expression.");
  }
#undef EVALUATE_BINARY_EXPRESSION
}

absl::StatusOr<std::shared_ptr<arrow::Scalar>> AddExpression::EvaluateMathExpression(
    const std::shared_ptr<arrow::Scalar> left,
    const std::shared_ptr<arrow::Scalar> right) {
#define ADD_ARROW_SCALER(tp)                                                \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto lv, tp, left);                   \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto rv, tp, right);                  \
  auto res = arrow::MakeScalar(left->type, lv + rv);                        \
  if (!res.ok()) { return absl::InternalError(GetMessageFromResult(res)); } \
  return *res;

  switch (left->type->id()) {
    case arrow::Type::INT64: {
      ADD_ARROW_SCALER(arrow::Int64Scalar);
    }
    case arrow::Type::DOUBLE: {
      ADD_ARROW_SCALER(arrow::DoubleScalar);
    }
    default: return absl::InternalError("Unsupported type in addition expression.");
  }

#undef ADD_ARROW_SCALER
}

absl::StatusOr<std::shared_ptr<arrow::Scalar>> SubtractExpression::EvaluateMathExpression(
    const std::shared_ptr<arrow::Scalar> left,
    const std::shared_ptr<arrow::Scalar> right) {
#define SUBTRACT_ARROW_SCALER(tp)                                           \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto lv, tp, left);                   \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto rv, tp, right);                  \
  auto res = arrow::MakeScalar(left->type, lv - rv);                        \
  if (!res.ok()) { return absl::InternalError(GetMessageFromResult(res)); } \
  return *res;

  switch (left->type->id()) {
    case arrow::Type::INT64: {
      SUBTRACT_ARROW_SCALER(arrow::Int64Scalar);
    }
    case arrow::Type::DOUBLE: {
      SUBTRACT_ARROW_SCALER(arrow::DoubleScalar);
    }
    default: return absl::InternalError("Unsupported type in subtraction expression.");
  }

#undef SUBTRACT_ARROW_SCALER
}

absl::StatusOr<std::shared_ptr<arrow::Scalar>> MultiplyExpression::EvaluateMathExpression(
    const std::shared_ptr<arrow::Scalar> left,
    const std::shared_ptr<arrow::Scalar> right) {
#define MULTIPLY_ARROW_SCALER(tp)                                           \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto lv, tp, left);                   \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto rv, tp, right);                  \
  auto res = arrow::MakeScalar(left->type, lv * rv);                        \
  if (!res.ok()) { return absl::InternalError(GetMessageFromResult(res)); } \
  return *res;

  switch (left->type->id()) {
    case arrow::Type::INT64: {
      MULTIPLY_ARROW_SCALER(arrow::Int64Scalar);
    }
    case arrow::Type::DOUBLE: {
      MULTIPLY_ARROW_SCALER(arrow::DoubleScalar);
    }
    default: return absl::InternalError("Unsupported type in multiplication expression.");
  }

#undef MULTIPLY_ARROW_SCALER
}

absl::StatusOr<std::shared_ptr<arrow::Scalar>> DivideExpression::EvaluateMathExpression(
    const std::shared_ptr<arrow::Scalar> left,
    const std::shared_ptr<arrow::Scalar> right) {
#define DIVIDE_ARROW_SCALER(tp)                                             \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto lv, tp, left);                   \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto rv, tp, right);                  \
  auto res = arrow::MakeScalar(left->type, lv / rv);                        \
  if (!res.ok()) { return absl::InternalError(GetMessageFromResult(res)); } \
  return *res;

  switch (left->type->id()) {
    case arrow::Type::INT64: {
      DIVIDE_ARROW_SCALER(arrow::Int64Scalar);
    }
    case arrow::Type::DOUBLE: {
      DIVIDE_ARROW_SCALER(arrow::DoubleScalar);
    }
    default: return absl::InternalError("Unsupported type in division expression.");
  }

#undef DIVIDE_ARROW_SCALER
}

absl::StatusOr<std::shared_ptr<arrow::Array>> Cast::Evaluate(const std::shared_ptr<arrow::RecordBatch> input) { }

std::string Cast::ToString() { return "todo"; }

}  // namespace physicalplan
}  // namespace toyquery
