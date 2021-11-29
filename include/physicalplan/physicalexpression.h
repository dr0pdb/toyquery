#ifndef PHYSICALPLAN_PHYSICALEXPRESSION_H
#define PHYSICALPLAN_PHYSICALEXPRESSION_H

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "arrow/api.h"
#include "common/macros.h"

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
 * @brief An expression which always evaluates to a literal long value.
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
 * @brief An expression which always evaluates to a literal double value.
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
 * @brief An expression which always evaluates to a literal long value.
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

}  // namespace physicalplan
}  // namespace toyquery

#endif  // PHYSICALPLAN_PHYSICALEXPRESSION_H