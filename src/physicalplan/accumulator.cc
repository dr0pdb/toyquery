#include "physicalplan/accumulator.h"

namespace toyquery {
namespace physicalplan {

absl::Status MaxAccumulator::Accumulate(std::shared_ptr<arrow::Scalar> value) {
#define COMPUTE_MAX_ARROW_SCALER(tp)                                  \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto val, tp, value);           \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(auto existing_val, tp, value_); \
  if (val > existing_val) { value_ = value; }

  if (value_) {
    switch (value->type->id()) {
      case arrow::Type::BOOL: {
        COMPUTE_MAX_ARROW_SCALER(arrow::BooleanScalar);
        break;
      }
      case arrow::Type::INT64: {
        COMPUTE_MAX_ARROW_SCALER(arrow::Int64Scalar);
        break;
      }
      case arrow::Type::DOUBLE: {
        COMPUTE_MAX_ARROW_SCALER(arrow::DoubleScalar);
        break;
      }
      case arrow::Type::STRING: {
        COMPUTE_MAX_ARROW_SCALER(arrow::StringScalar);
        break;
      }
      default: return absl::InternalError("Unsupported value type for Max accumulator.");
    }
  } else {
    value_ = value;
  }

#undef COMPUTE_MAX_ARROW_SCALER
}

absl::StatusOr<std::shared_ptr<arrow::Scalar>> MaxAccumulator::FinalValue() { return value_; }

}  // namespace physicalplan
}  // namespace toyquery
