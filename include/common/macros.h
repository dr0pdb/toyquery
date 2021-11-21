// Copyright 2019 The MediaPipe Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Helper macros and methods to return and propagate errors with
// `absl::Status`.
//
// The owners of mediapipe do not endorse use of these macros as a good
// programming practice, and would prefer that you write the equivalent C++
// directly. The macros are provided and supported for those that disagree,
// with the goal of having a single, consistent, and robust implementation.

/**
 * @brief This file contains useful macros for dealing with common C++ boilerplate.
 *
 * A lot of these macros have been taken from google/mediapipe project and hence this file is licensed to the authors of the
 * project.
 *
 * https://github.com/google/mediapipe/blob/master/mediapipe/framework/deps/status_macros.h
 */

#ifndef COMMON_MACROS_H
#define COMMON_MACROS_H

namespace toyquery {

#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  void operator=(const TypeName&)

// Executes an expression `rexpr` that returns a `absl::StatusOr<T>`. On
// OK, extracts its value into the variable defined by `lhs`, otherwise returns
// from the current function. By default the error status is returned
// unchanged, but it may be modified by an `error_expression`. If there is an
// error, `lhs` is not evaluated; thus any side effects that `lhs` may have
// only occur in the success case.
//
// Interface:
//
//   ASSIGN_OR_RETURN(lhs, rexpr)
//   ASSIGN_OR_RETURN(lhs, rexpr, error_expression);
//
// WARNING: expands into multiple statements; it cannot be used in a single
// statement (e.g. as the body of an if statement without {})!
//
// Example: Declaring and initializing a new variable (ValueType can be anything
//          that can be initialized with assignment, including references):
//   ASSIGN_OR_RETURN(ValueType value, MaybeGetValue(arg));
//
// Example: Assigning to an existing variable:
//   ValueType value;
//   ASSIGN_OR_RETURN(value, MaybeGetValue(arg));
//
// Example: Assigning to an expression with side effects:
//   MyProto data;
//   ASSIGN_OR_RETURN(*data.mutable_str(), MaybeGetValue(arg));
//   // No field "str" is added on error.
//
// Example: Assigning to a std::unique_ptr.
//   ASSIGN_OR_RETURN(std::unique_ptr<T> ptr, MaybeGetPtr(arg));
//
// If passed, the `error_expression` is evaluated to produce the return
// value. The expression may reference any variable visible in scope, as
// well as a `mediapipe::StatusBuilder` object populated with the error and
// named by a single underscore `_`. The expression typically uses the
// builder to modify the status and is returned directly in manner similar
// to MP_RETURN_IF_ERROR. The expression may, however, evaluate to any type
// returnable by the function, including (void). For example:
//
// Example: Adjusting the error message.
//   ASSIGN_OR_RETURN(ValueType value, MaybeGetValue(query),
//                    _ << "while processing query " << query.DebugString());
//
// Example: Logging the error on failure.
//   ASSIGN_OR_RETURN(ValueType value, MaybeGetValue(query), _.LogError());
//

#define ASSIGN_OR_RETURN(...)                                                                        \
  STATUS_MACROS_IMPL_GET_VARIADIC_(                                                                  \
      (__VA_ARGS__, STATUS_MACROS_IMPL_ASSIGN_OR_RETURN_3_, STATUS_MACROS_IMPL_ASSIGN_OR_RETURN_2_)) \
  (__VA_ARGS__)

#define STATUS_MACROS_IMPL_GET_VARIADIC_HELPER_(_1, _2, _3, NAME, ...) NAME
#define STATUS_MACROS_IMPL_GET_VARIADIC_(args)                         STATUS_MACROS_IMPL_GET_VARIADIC_HELPER_ args

#define STATUS_MACROS_IMPL_ASSIGN_OR_RETURN_2_(lhs, rexpr) STATUS_MACROS_IMPL_ASSIGN_OR_RETURN_3_(lhs, rexpr, std::move(_))
#define STATUS_MACROS_IMPL_ASSIGN_OR_RETURN_3_(lhs, rexpr, error_expression) \
  STATUS_MACROS_IMPL_ASSIGN_OR_RETURN_(STATUS_MACROS_IMPL_CONCAT_(_status_or_value, __LINE__), lhs, rexpr, error_expression)
#define STATUS_MACROS_IMPL_ASSIGN_OR_RETURN_(statusor, lhs, rexpr, error_expression) \
  auto statusor = (rexpr);                                                           \
  if (ABSL_PREDICT_FALSE(!statusor.ok())) {                                          \
    mediapipe::StatusBuilder _(std::move(statusor).status(), __FILE__, __LINE__);    \
    (void)_; /* error_expression is allowed to not use this variable */              \
    return (error_expression);                                                       \
  }                                                                                  \
  lhs = std::move(statusor).value()

// Internal helper for concatenating macro values.
#define STATUS_MACROS_IMPL_CONCAT_INNER_(x, y) x##y
#define STATUS_MACROS_IMPL_CONCAT_(x, y)       STATUS_MACROS_IMPL_CONCAT_INNER_(x, y)

// The GNU compiler emits a warning for code like:
//
//   if (foo)
//     if (bar) { } else baz;
//
// because it thinks you might want the else to bind to the first if.  This
// leads to problems with code like:
//
//   if (do_expr) MP_RETURN_IF_ERROR(expr) << "Some message";
//
// The "switch (0) case 0:" idiom is used to suppress this.
#define STATUS_MACROS_IMPL_ELSE_BLOCKER_ \
  switch (0)                             \
  case 0:                                \
  default:  // NOLINT

}  // namespace toyquery

#endif  // COMMON_MACROS_H