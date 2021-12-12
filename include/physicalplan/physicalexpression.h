#ifndef PHYSICALPLAN_PHYSICALEXPRESSION_H
#define PHYSICALPLAN_PHYSICALEXPRESSION_H

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "accumulator.h"
#include "arrow/api.h"
#include "common/macros.h"
#include "common/status.h"

namespace toyquery {
namespace physicalplan {

class PhysicalPlan;

/**
 * @brief Base class for all physical expressions.
 *
 * The physical expression the code for evaluating it against record batches and produce columns.
 */
class PhysicalExpression {
 public:
  PhysicalExpression() = default;
  virtual ~PhysicalExpression() = 0;

  /**
   * @brief Evaluate the expression on the record batch to generate output column.
   *
   * @param input: the input record batch
   * @return absl::StatusOr<std::shared_ptr<arrow::Array>>: the output column
   */
  virtual absl::StatusOr<std::shared_ptr<arrow::Array>> Evaluate(const std::shared_ptr<arrow::RecordBatch> input) = 0;

  /**
   * @brief Get string representation to print for debugging.
   *
   * @return std::string: the string representation of the expression.
   */
  virtual std::string ToString() = 0;

 private:
};

/**
 * @brief A reference to a column of a table by name.
 */
class Column : public PhysicalExpression {
 public:
  Column(int idx) : idx_{ idx } { }
  ~Column() override;

  /**
   * @copydoc PhysicalExpression::Evaluate()
   */
  absl::StatusOr<std::shared_ptr<arrow::Array>> Evaluate(const std::shared_ptr<arrow::RecordBatch> input) override;

  /**
   * @copydoc PhysicalExpression::ToString()
   */
  std::string ToString() override;

 private:
  int idx_;
};

/**
 * @brief An expression which always evaluates to a literal long (INT64) value.
 *
 */
class LiteralLong : public PhysicalExpression {
 public:
  LiteralLong(long val) : val_{ val } { }
  ~LiteralLong() override;

  /**
   * @copydoc PhysicalExpression::Evaluate()
   */
  absl::StatusOr<std::shared_ptr<arrow::Array>> Evaluate(const std::shared_ptr<arrow::RecordBatch> input) override;

  /**
   * @copydoc PhysicalExpression::ToString()
   */
  std::string ToString() override;

 private:
  long val_;
};

/**
 * @brief An expression which always evaluates to a literal double (DOUBLE) value.
 *
 */
class LiteralDouble : public PhysicalExpression {
 public:
  LiteralDouble(double val) : val_{ val } { }
  ~LiteralDouble() override;

  /**
   * @copydoc PhysicalExpression::Evaluate()
   */
  absl::StatusOr<std::shared_ptr<arrow::Array>> Evaluate(const std::shared_ptr<arrow::RecordBatch> input) override;

  /**
   * @copydoc PhysicalExpression::ToString()
   */
  std::string ToString() override;

 private:
  double val_;
};

/**
 * @brief An expression which always evaluates to a literal string (STRING) value.
 *
 */
class LiteralString : public PhysicalExpression {
 public:
  LiteralString(std::string val) : val_{ val } { }
  ~LiteralString() override;

  /**
   * @copydoc PhysicalExpression::Evaluate()
   */
  absl::StatusOr<std::shared_ptr<arrow::Array>> Evaluate(const std::shared_ptr<arrow::RecordBatch> input) override;

  /**
   * @copydoc PhysicalExpression::ToString()
   */
  std::string ToString() override;

 private:
  std::string val_;
};

/**
 * @brief A common class for boolean expressions.
 *
 */
class BooleanExpression : public PhysicalExpression {
 public:
  BooleanExpression(std::shared_ptr<PhysicalExpression> left, std::shared_ptr<PhysicalExpression> right)
      : left_{ left },
        right_{ right } { }

  /**
   * @copydoc PhysicalExpression::Evaluate()
   */
  absl::StatusOr<std::shared_ptr<arrow::Array>> Evaluate(const std::shared_ptr<arrow::RecordBatch> input) override;

  /**
   * @brief Compare two arrow::Array using the boolean expression.
   *
   * @param left: the left operand
   * @param right: the right operand
   * @return absl::StatusOr<std::shared_ptr<arrow::Array>>: the resulting expression arrow::Array
   */
  absl::StatusOr<std::shared_ptr<arrow::Array>> Compare(
      const std::shared_ptr<arrow::Array> left,
      const std::shared_ptr<arrow::Array> right);

  /**
   * @brief Evaluate the expression on two scalers.
   *
   * @param left the left operand
   * @param right the right operand
   * @return absl::StatusOr<bool>: the resulting boolean
   */
  virtual absl::StatusOr<bool> EvaluateBooleanExpression(
      const std::shared_ptr<arrow::Scalar> left,
      const std::shared_ptr<arrow::Scalar> right) = 0;

 private:
  std::shared_ptr<PhysicalExpression> left_;
  std::shared_ptr<PhysicalExpression> right_;
};

/**
 * @brief The equality expression.
 *
 */
class EqExpression : public BooleanExpression {
 public:
  EqExpression(std::shared_ptr<PhysicalExpression> left, std::shared_ptr<PhysicalExpression> right)
      : BooleanExpression(left, right) { }

  /**
   * @copydoc BooleanExpression::EvaluateBooleanExpression
   */
  absl::StatusOr<bool> EvaluateBooleanExpression(
      const std::shared_ptr<arrow::Scalar> left,
      const std::shared_ptr<arrow::Scalar> right) override;

 private:
};

/**
 * @brief The inequality expression.
 *
 */
class NeqExpression : public BooleanExpression {
 public:
  NeqExpression(std::shared_ptr<PhysicalExpression> left, std::shared_ptr<PhysicalExpression> right)
      : BooleanExpression(left, right) { }

  /**
   * @copydoc BooleanExpression::EvaluateBooleanExpression
   */
  absl::StatusOr<bool> EvaluateBooleanExpression(
      const std::shared_ptr<arrow::Scalar> left,
      const std::shared_ptr<arrow::Scalar> right) override;

 private:
};

/**
 * @brief The AND expression.
 *
 */
class AndExpression : public BooleanExpression {
 public:
  AndExpression(std::shared_ptr<PhysicalExpression> left, std::shared_ptr<PhysicalExpression> right)
      : BooleanExpression(left, right) { }

  /**
   * @copydoc BooleanExpression::EvaluateBooleanExpression
   */
  absl::StatusOr<bool> EvaluateBooleanExpression(
      const std::shared_ptr<arrow::Scalar> left,
      const std::shared_ptr<arrow::Scalar> right) override;

 private:
};

/**
 * @brief The OR expression.
 *
 */
class OrExpression : public BooleanExpression {
 public:
  OrExpression(std::shared_ptr<PhysicalExpression> left, std::shared_ptr<PhysicalExpression> right)
      : BooleanExpression(left, right) { }

  /**
   * @copydoc BooleanExpression::EvaluateBooleanExpression
   */
  absl::StatusOr<bool> EvaluateBooleanExpression(
      const std::shared_ptr<arrow::Scalar> left,
      const std::shared_ptr<arrow::Scalar> right) override;

 private:
};

/**
 * @brief The LessThan expression.
 *
 */
class LessThanExpression : public BooleanExpression {
 public:
  LessThanExpression(std::shared_ptr<PhysicalExpression> left, std::shared_ptr<PhysicalExpression> right)
      : BooleanExpression(left, right) { }

  /**
   * @copydoc BooleanExpression::EvaluateBooleanExpression
   */
  absl::StatusOr<bool> EvaluateBooleanExpression(
      const std::shared_ptr<arrow::Scalar> left,
      const std::shared_ptr<arrow::Scalar> right) override;

 private:
};

/**
 * @brief The LessThanEquals expression.
 *
 */
class LessThanEqualsExpression : public BooleanExpression {
 public:
  LessThanEqualsExpression(std::shared_ptr<PhysicalExpression> left, std::shared_ptr<PhysicalExpression> right)
      : BooleanExpression(left, right) { }

  /**
   * @copydoc BooleanExpression::EvaluateBooleanExpression
   */
  absl::StatusOr<bool> EvaluateBooleanExpression(
      const std::shared_ptr<arrow::Scalar> left,
      const std::shared_ptr<arrow::Scalar> right) override;

 private:
};

/**
 * @brief The GreaterThan expression.
 *
 */
class GreaterThanExpression : public BooleanExpression {
 public:
  GreaterThanExpression(std::shared_ptr<PhysicalExpression> left, std::shared_ptr<PhysicalExpression> right)
      : BooleanExpression(left, right) { }

  /**
   * @copydoc BooleanExpression::EvaluateBooleanExpression
   */
  absl::StatusOr<bool> EvaluateBooleanExpression(
      const std::shared_ptr<arrow::Scalar> left,
      const std::shared_ptr<arrow::Scalar> right) override;

 private:
};

/**
 * @brief The GreaterThanEquals expression.
 *
 */
class GreaterThanEqualsExpression : public BooleanExpression {
 public:
  GreaterThanEqualsExpression(std::shared_ptr<PhysicalExpression> left, std::shared_ptr<PhysicalExpression> right)
      : BooleanExpression(left, right) { }

  /**
   * @copydoc BooleanExpression::EvaluateBooleanExpression
   */
  absl::StatusOr<bool> EvaluateBooleanExpression(
      const std::shared_ptr<arrow::Scalar> left,
      const std::shared_ptr<arrow::Scalar> right) override;

 private:
};

/**
 * @brief A common class for binary expressions.
 *
 */
class BinaryExpression : public PhysicalExpression {
 public:
  BinaryExpression(std::shared_ptr<PhysicalExpression> left, std::shared_ptr<PhysicalExpression> right)
      : left_{ left },
        right_{ right } { }

  /**
   * @copydoc PhysicalExpression::Evaluate()
   */
  absl::StatusOr<std::shared_ptr<arrow::Array>> Evaluate(const std::shared_ptr<arrow::RecordBatch> input) override;

  /**
   * @brief Evaluate the expression on two arrow::Array.
   *
   * @param left the left operand
   * @param right the right operand
   * @return absl::StatusOr<std::shared_ptr<arrow::Array>>: the resulting arrow::Array
   */
  virtual absl::StatusOr<std::shared_ptr<arrow::Array>> EvaluateBinaryExpression(
      const std::shared_ptr<arrow::Array> left,
      const std::shared_ptr<arrow::Array> right) = 0;

 private:
  std::shared_ptr<PhysicalExpression> left_;
  std::shared_ptr<PhysicalExpression> right_;
};

/**
 * @brief A common class for binary expressions.
 *
 */
class MathExpression : public BinaryExpression {
 public:
  MathExpression(std::shared_ptr<PhysicalExpression> left, std::shared_ptr<PhysicalExpression> right)
      : BinaryExpression(left, right) { }

  /**
   * @copydoc BinaryExpression::EvaluateBinaryExpression()
   */
  absl::StatusOr<std::shared_ptr<arrow::Array>> EvaluateBinaryExpression(
      const std::shared_ptr<arrow::Array> left,
      const std::shared_ptr<arrow::Array> right) override;

  /**
   * @brief Evaluate the math expression on the two scalars
   *
   * @param left: the left operand
   * @param right: the right operand
   * @return absl::StatusOr<std::shared_ptr<arrow::Scalar>>: the resulting scalar or status.
   */
  virtual absl::StatusOr<std::shared_ptr<arrow::Scalar>> EvaluateMathExpression(
      const std::shared_ptr<arrow::Scalar> left,
      const std::shared_ptr<arrow::Scalar> right) = 0;

 private:
};

/**
 * @brief The Addition expression.
 *
 */
class AddExpression : public MathExpression {
 public:
  AddExpression(std::shared_ptr<PhysicalExpression> left, std::shared_ptr<PhysicalExpression> right)
      : MathExpression(left, right) { }

  /**
   * @copydoc MathExpression::EvaluateMathExpression
   */
  absl::StatusOr<std::shared_ptr<arrow::Scalar>> EvaluateMathExpression(
      const std::shared_ptr<arrow::Scalar> left,
      const std::shared_ptr<arrow::Scalar> right) override;

 private:
};

/**
 * @brief The Subtraction expression.
 *
 */
class SubtractExpression : public MathExpression {
 public:
  SubtractExpression(std::shared_ptr<PhysicalExpression> left, std::shared_ptr<PhysicalExpression> right)
      : MathExpression(left, right) { }

  /**
   * @copydoc MathExpression::EvaluateMathExpression
   */
  absl::StatusOr<std::shared_ptr<arrow::Scalar>> EvaluateMathExpression(
      const std::shared_ptr<arrow::Scalar> left,
      const std::shared_ptr<arrow::Scalar> right) override;

 private:
};

/**
 * @brief The multiplication expression.
 *
 */
class MultiplyExpression : public MathExpression {
 public:
  MultiplyExpression(std::shared_ptr<PhysicalExpression> left, std::shared_ptr<PhysicalExpression> right)
      : MathExpression(left, right) { }

  /**
   * @copydoc MathExpression::EvaluateMathExpression
   */
  absl::StatusOr<std::shared_ptr<arrow::Scalar>> EvaluateMathExpression(
      const std::shared_ptr<arrow::Scalar> left,
      const std::shared_ptr<arrow::Scalar> right) override;

 private:
};

/**
 * @brief The Division expression.
 *
 */
class DivideExpression : public MathExpression {
 public:
  DivideExpression(std::shared_ptr<PhysicalExpression> left, std::shared_ptr<PhysicalExpression> right)
      : MathExpression(left, right) { }

  /**
   * @copydoc MathExpression::EvaluateMathExpression
   */
  absl::StatusOr<std::shared_ptr<arrow::Scalar>> EvaluateMathExpression(
      const std::shared_ptr<arrow::Scalar> left,
      const std::shared_ptr<arrow::Scalar> right) override;

 private:
};

}  // namespace physicalplan
}  // namespace toyquery

#endif  // PHYSICALPLAN_PHYSICALEXPRESSION_H