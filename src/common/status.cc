#include "common/status.h"

namespace toyquery {
namespace common {

absl::string_view GetMessageFromStatus(absl::Status status) { return status.message(); }

absl::string_view GetMessageFromStatus(arrow::Status status) { return status.message(); }

template<typename T>
absl::string_view GetMessageFromResult(arrow::Result<T> result) {
  return result.status().detail()->ToString();
}

template<typename T>
inline absl::string_view GetMessageFromResultLeftOrRight(arrow::Result<T> left, arrow::Result<T> right) {
  if (!left.ok()) { return GetMessageFromResult(left); }

  return GetMessageFromResult(right);
}

}  // namespace common
}  // namespace toyquery
