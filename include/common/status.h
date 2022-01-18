#ifndef COMMON_STATUS_H
#define COMMON_STATUS_H

#include <string>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "arrow/api.h"

namespace toyquery {
namespace common {

/**
 * @brief Get the Message From Status object
 *
 * @param status The absl::Status object
 * @return absl::string_view: status message
 */
absl::string_view GetMessageFromStatus(absl::Status status);

/**
 * @brief Get the Message From Status object
 *
 * @param status The arrow::Status object
 * @return absl::string_view: status message
 */
absl::string_view GetMessageFromStatus(arrow::Status status);

#define GetMessageFromResult(result) result.status().detail()->ToString()

#define GetMessageFromResultLeftOrRight(left, right) \
  ((!left.ok()) ? GetMessageFromResult(left) : GetMessageFromResult(right))

}  // namespace common
}  // namespace toyquery

#endif  // COMMON_STATUS_H