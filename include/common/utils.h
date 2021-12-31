#ifndef COMMON_UTILS_H
#define COMMON_UTILS_H

#include <charconv>
#include <optional>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace toyquery {
namespace common {

absl::StatusOr<long> ToLong(const absl::string_view& input) {
  long out;
  const std::from_chars_result result = std::from_chars(input.data(), input.data() + input.size(), out);
  if (result.ec == std::errc::invalid_argument) {
    return absl::InvalidArgumentError(absl::StrCat(input, " is an invalid long"));
  }
  if (result.ec == std::errc::result_out_of_range) {
    return absl::OutOfRangeError(absl::StrCat(input, " is out of range for long"));
  }

  return out;
}

absl::StatusOr<double> ToDouble(const absl::string_view& input) {
  double out;
  const std::from_chars_result result = std::from_chars(input.data(), input.data() + input.size(), out);
  if (result.ec == std::errc::invalid_argument) {
    return absl::InvalidArgumentError(absl::StrCat(input, " is an invalid long"));
  }
  if (result.ec == std::errc::result_out_of_range) {
    return absl::OutOfRangeError(absl::StrCat(input, " is out of range for long"));
  }

  return out;
}

}  // namespace common
}  // namespace toyquery

#endif  // COMMON_UTILS_H