/**
 * @brief This file contains useful macros for dealing with common C++ boilerplate.
 *
 */

#ifndef COMMON_MACROS_H
#define COMMON_MACROS_H

#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace toyquery {

#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  void operator=(const TypeName&)

#define RETURN_IF_NULL(expr) \
  if (expr == nullptr) { return nullptr; }

#define CHECK_OK_OR_RETURN(rhs) CHECK_OK_OR_RETURN_IMPL(STATUS_MACROS_IMPL_CONCAT_(tmp, __LINE__), rhs)

#define CHECK_OK_OR_RETURN_IMPL(tmp, rhs) \
  auto tmp = rhs;                         \
  if (!tmp.ok()) { return tmp; }

/// Evaluates an expression `rexpr` that returns a `StatusOr`-like
/// object with `.ok()`, `.status()`, and `.value()` methods.  If
/// the result is OK, moves its value into the variable defined by
/// `lhs`, otherwise returns the result of the `.status()` from the
/// current function. The error result of `.status` is returned
/// unchanged. If there is an error, `lhs` is not evaluated: thus any
/// side effects of evaluating `lhs` will only occur if `rexpr.ok()`
/// is true.
///
/// Interface:
/// ```
///   ASSIGN_OR_RETURN(lhs, rexpr)
/// ```
///
/// Example: Assigning to an existing variable:
/// ```
///   ValueType value;
///   ASSIGN_OR_RETURN(value, MaybeGetValue(arg));
/// ```
///
/// Example: Assigning to an expression with side effects:
/// ```
///   MyProto data;
///   ASSIGN_OR_RETURN(*data.mutable_str(), MaybeGetValue(arg));
///   // No field "str" is added on error.
/// ```
///
/// Example: Assigning to a `std::unique_ptr`.
/// ```
///   std::unique_ptr<T> ptr;
///   ASSIGN_OR_RETURN(ptr, MaybeGetPtr(arg));
/// ```
///
/// Example: Defining and assigning a value to a new variable:
/// ```
///   ASSIGN_OR_RETURN(ValueType value, MaybeGetValue(arg));
/// ```
#define ASSIGN_OR_RETURN(lhs, rexpr) \
  ASSIGN_OR_RETURN_IMPL_(STATUS_MACROS_IMPL_CONCAT_(status_or_value, __LINE__), lhs, rexpr)

#define ASSIGN_OR_RETURN2(lhs, diff, rexpr) \
  ASSIGN_OR_RETURN_IMPL_(STATUS_MACROS_IMPL_CONCAT_(STATUS_MACROS_IMPL_CONCAT_(status_or_value, __LINE__), diff), lhs, rexpr)

// Internal helper.
#define ASSIGN_OR_RETURN_IMPL_(statusor, lhs, rexpr)                               \
  auto statusor = (rexpr);                                                         \
  if (ABSL_PREDICT_FALSE(!statusor.ok())) { return std::move(statusor).status(); } \
  lhs = std::move(statusor).value()

// Internal helper for concatenating macro values.
#define STATUS_MACROS_IMPL_CONCAT_INNER_(x, y) x##y
#define STATUS_MACROS_IMPL_CONCAT_(x, y)       STATUS_MACROS_IMPL_CONCAT_INNER_(x, y)

#define CAST_ARROW_SCALER_TO_TYPE_OR_RETURN(lhs, tp, scaler)                               \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN_IMPL_(                                               \
      CAST_MACROS_IMPL_CONCAT_(CAST_MACROS_IMPL_CONCAT_(casted_scaler, scaler), __LINE__), \
      lhs,                                                                                 \
      tp,                                                                                  \
      scaler,                                                                              \
      "arrow::Scaler type casting error.")

#define CAST_ARROW_SCALER_TO_TYPE_OR_RETURN_WITH_MESSAGE(lhs, tp, scaler, msg) \
  CAST_ARROW_SCALER_TO_TYPE_OR_RETURN_IMPL_(CAST_MACROS_IMPL_CONCAT_(casted_scaler, __LINE__), lhs, tp, scaler, msg)

#define CAST_ARROW_SCALER_TO_TYPE_OR_RETURN_IMPL_(casted_scaler_name, lhs, tp, scaler, msg) \
  auto casted_scaler_name = std::static_pointer_cast<tp>(scaler);                           \
  if (!casted_scaler_name->is_valid) { return absl::InternalError(msg); }                   \
  lhs = casted_scaler_name->value;

#define CAST_MACROS_IMPL_CONCAT_INNER_(x, y) x##y
#define CAST_MACROS_IMPL_CONCAT_(x, y)       CAST_MACROS_IMPL_CONCAT_INNER_(x, y)

}  // namespace toyquery

#endif  // COMMON_MACROS_H