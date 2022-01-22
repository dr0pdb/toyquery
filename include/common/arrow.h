#ifndef COMMON_ARROW_H
#define COMMON_ARROW_H

/**
 * @brief This file contains useful functions defined for easier use of Apache Arrow.
 */

#include "absl/status/statusor.h"
#include "arrow/api.h"

namespace toyquery {

/**
 * @brief The acceptable error margin for double precision calculations.
 */
static constexpr double DOUBLE_ACCEPTED_MARGIN = 0.000001;

/**
 * @brief Filter an arrow::Schema using the given projection
 *
 * @param schema: the original schema
 * @param projection: the name of the columns to include in the result
 * @return absl::StatusOr<std::shared_ptr<arrow::Schema>>: the resulting schema
 */
absl::StatusOr<std::shared_ptr<arrow::Schema>> FilterSchema(
    std::shared_ptr<arrow::Schema> schema,
    std::vector<std::string> projection);

}  // namespace toyquery

#endif  // COMMON_ARROW_H