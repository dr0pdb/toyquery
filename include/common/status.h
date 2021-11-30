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
inline absl::string_view GetMessageFromStatus(absl::Status status);

/**
 * @brief Get the Message From Status object
 *
 * @param status The arrow::Status object
 * @return absl::string_view: status message
 */
inline absl::string_view GetMessageFromStatus(arrow::Status status);

/**
 * @brief Get the Message From Result object
 *
 * @tparam T : the type param of arrow::Result
 * @param result : the arrow::Result
 * @return absl::string_view : the status message
 */
template<typename T>
inline absl::string_view GetMessageFromResult(arrow::Result<T> result);

/**
 * @brief Get the Message from the left or right result object depending on whichever is not ok.
 *
 * @note Assumes that one of the operands is not ok.
 * @tparam T : the type param of arrow::Result
 * @param left : the left param arrow::Result
 * @param right: the right param arrow::Result
 * @return absl::string_view : the status message
 */
template<typename T>
inline absl::string_view GetMessageFromResultLeftOrRight(arrow::Result<T> left, arrow::Result<T> right);

}  // namespace common
}  // namespace toyquery

#endif  // COMMON_STATUS_H