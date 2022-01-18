#include "common/status.h"

namespace toyquery {
namespace common {

absl::string_view GetMessageFromStatus(absl::Status status) { return status.message(); }

absl::string_view GetMessageFromStatus(arrow::Status status) { return status.message(); }

}  // namespace common
}  // namespace toyquery
