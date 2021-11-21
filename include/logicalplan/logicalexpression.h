#ifndef LOGICALPLAN_LOGICALEXPRESSION_H
#define LOGICALPLAN_LOGICALEXPRESSION_H

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "arrow/api.h"
#include "common/macros.h"
#include "logicalplan.h"

namespace toyquery {
namespace logicalplan {

/**
 * @brief Base class for all logical expressions.
 *
 * The logical expression provides information needed during the planning phase such as the name and data type of the
 * expression.
 */
class LogicalExpression {
 public:
  LogicalExpression() = default;
  ~LogicalExpression();

  /**
   * @brief Get arrow::Field of value when evaluating this expression against the input.
   *
   * Get the metadata(arrow::Field) info of the value that will be produced when this expression is evaluated on the provided
   * logical plan.
   *
   * Eg: Consider the expression a + b,
   * - For a table with column a and b as int64, the resulting field would have type int64
   * - For a table with column a and b as string, the resulting field would have type string
   * - For a table with column a as int64 and b as string, the result will be a type error
   *
   * @param input: the logical plan on which this expression will be evaluated.
   * @return absl::StatusOr<std::shared_ptr<arrow::Field>>: the metadata information of the produced value or error.
   */
  virtual absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) = 0;

  /**
   * @brief Get string representation to print for debugging.
   *
   * @return std::string: the string representation of the expression.
   */
  virtual std::string ToString();

 private:
};

/**
 * @brief A reference to a column of a table by name.
 */
class Column : public LogicalExpression {
 public:
  Column(std::string name);
  ~Column();

  /**
   * @copydoc LogicalExpression::ToField()
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override;

  /**
   * @copydoc LogicalExpression::ToString()
   */
  std::string ToString() override;

 private:
  std::string name_;
};

/**
 * @brief A reference to a column of a table by index.
 */
class ColumnIndex : public LogicalExpression {
 public:
  ColumnIndex(int index);
  ~ColumnIndex();

  /**
   * @copydoc LogicalExpression::ToField()
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override;

  /**
   * @copydoc LogicalExpression::ToString()
   */
  std::string ToString() override;

 private:
  int index_;
};

/**
 * @brief A literal string expression.
 */
class LiteralString : public LogicalExpression {
 public:
  LiteralString(std::string value);
  ~LiteralString();

  /**
   * @copydoc LogicalExpression::ToField()
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override;

  /**
   * @copydoc LogicalExpression::ToString()
   */
  std::string ToString() override;

 private:
  std::string value_;
};

/**
 * @brief A literal int64 expression.
 */
class LiteralLong : public LogicalExpression {
 public:
  LiteralLong(int64_t value);
  ~LiteralLong();

  /**
   * @copydoc LogicalExpression::ToField()
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override;

  /**
   * @copydoc LogicalExpression::ToString()
   */
  std::string ToString() override;

 private:
  int64_t value_;
};

/**
 * @brief A literal double expression.
 */
class LiteralDouble : public LogicalExpression {
 public:
  LiteralDouble(double value);
  ~LiteralDouble();

  /**
   * @copydoc LogicalExpression::ToField()
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override;

  /**
   * @copydoc LogicalExpression::ToString()
   */
  std::string ToString() override;

 private:
  double value_;
};

/**
 * @brief A cast expression.
 */
class Cast : public LogicalExpression {
 public:
  Cast(std::shared_ptr<LogicalExpression> expr, std::shared_ptr<arrow::DataType> type);
  ~Cast();

  /**
   * @copydoc LogicalExpression::ToField()
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override;

  /**
   * @copydoc LogicalExpression::ToString()
   */
  std::string ToString() override;

 protected:
  std::shared_ptr<LogicalExpression> expr_;
  std::shared_ptr<arrow::DataType> type_;
};

/**
 * @brief An alias logical expression.
 *
 * Format: expr AS alias
 */
class Alias : public LogicalExpression {
 public:
  Alias(std::shared_ptr<LogicalExpression> expr, std::string alias);
  ~Alias();

  /**
   * @copydoc LogicalExpression::ToField()
   *
   * The return type of the alias expression is that same as that of the input expression.
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override {
    ASSIGN_OR_RETURN(std::shared_ptr<arrow::Field> field, left_->ToField(input));
    return std::make_shared<arrow::Field>(alias_, field->type());
  }

  /**
   * @copydoc LogicalExpression::ToString()
   */
  std::string ToString() override { return "todo"; }

 protected:
  std::shared_ptr<LogicalExpression> expr_;
  std::string alias_;
};

/**
 * @brief The base class for a unary expression.
 */
class UnaryExpression : public LogicalExpression {
 public:
  UnaryExpression(std::string name, std::string op, std::shared_ptr<LogicalExpression> expr);
  ~UnaryExpression();

  /**
   * @copydoc LogicalExpression::ToString()
   */
  std::string ToString() override;

 protected:
  std::string name_;
  std::string op_;
  std::shared_ptr<LogicalExpression> expr_;
};

/**
 * @brief The logical NOT expression.
 */
class Not : public UnaryExpression {
 public:
  Not(std::shared_ptr<LogicalExpression> expr) : UnaryExpression("not", "NOT", expr) { }
  ~Not();

  /**
   * @copydoc LogicalExpression::ToField()
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override {
    std::shared_ptr<arrow::DataType> boolean_type = std::make_shared<arrow::DataType>(arrow::Type::BOOL);
    return std::make_shared<arrow::Field>(this->name_, std::move(boolean_type));
  }
};

/**
 * @brief The base class for a binary expression.
 */
class BinaryExpression : public LogicalExpression {
 public:
  BinaryExpression(
      std::string name,
      std::string op,
      std::shared_ptr<LogicalExpression> left,
      std::shared_ptr<LogicalExpression> right);
  ~BinaryExpression();

  /**
   * @copydoc LogicalExpression::ToString()
   */
  std::string ToString() override { return "todo"; }

 protected:
  std::string name_;
  std::string op_;
  std::shared_ptr<LogicalExpression> left_;
  std::shared_ptr<LogicalExpression> right_;
};

/**
 * @brief The base class for a binary expression that outputs boolean.
 */
class BooleanBinaryExpression : public BinaryExpression {
 public:
  BooleanBinaryExpression(
      std::string name,
      std::string op,
      std::shared_ptr<LogicalExpression> left,
      std::shared_ptr<LogicalExpression> right)
      : BinaryExpression(name, op, left, right) { }

  ~BooleanBinaryExpression();

  /**
   * @copydoc LogicalExpression::ToField()
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override {
    std::shared_ptr<arrow::DataType> boolean_type = std::make_shared<arrow::DataType>(arrow::Type::BOOL);
    return std::make_shared<arrow::Field>(this->name_, std::move(boolean_type));
  }
};

/**
 * @brief The AND logical expression.
 */
class And : public BooleanBinaryExpression {
 public:
  And(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : BooleanBinaryExpression("and", "AND", left, right) { }

  ~And();
};

/**
 * @brief The OR logical expression.
 */
class Or : public BooleanBinaryExpression {
 public:
  Or(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : BooleanBinaryExpression("or", "OR", left, right) { }

  ~Or();
};

/**
 * @brief The equality logical expression.
 */
class Eq : public BooleanBinaryExpression {
 public:
  Eq(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : BooleanBinaryExpression("eq", "=", left, right) { }

  ~Eq();
};

/**
 * @brief The inequality logical expression.
 */
class Neq : public BooleanBinaryExpression {
 public:
  Neq(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : BooleanBinaryExpression("neq", "!=", left, right) { }

  ~Neq();
};

/**
 * @brief The greater than logical expression.
 */
class Gt : public BooleanBinaryExpression {
 public:
  Gt(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : BooleanBinaryExpression("ge", ">", left, right) { }

  ~Gt();
};

/**
 * @brief The greater than equals logical expression.
 */
class GtEq : public BooleanBinaryExpression {
 public:
  GtEq(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : BooleanBinaryExpression("gteq", ">=", left, right) { }

  ~GtEq();
};

/**
 * @brief The less than logical expression.
 */
class Lt : public BooleanBinaryExpression {
 public:
  Lt(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : BooleanBinaryExpression("lt", "<", left, right) { }

  ~Lt();
};

/**
 * @brief The less than equals logical expression.
 */
class LtEq : public BooleanBinaryExpression {
 public:
  LtEq(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : BooleanBinaryExpression("lteq", "<=", left, right) { }

  ~LtEq();
};

/**
 * @brief The base class for a binary expression that is the result of a mathematical operation.
 */
class MathBinaryExpression : public BinaryExpression {
 public:
  MathBinaryExpression(
      std::string name,
      std::string op,
      std::shared_ptr<LogicalExpression> left,
      std::shared_ptr<LogicalExpression> right)
      : BinaryExpression(name, op, left, right) { }

  ~MathBinaryExpression();

  /**
   * @copydoc LogicalExpression::ToField()
   *
   * The type of the result of a math expression is the same as that of it's left and right parameters.
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override {
    ASSIGN_OR_RETURN(std::shared_ptr<arrow::Field> field, left_->ToField(input));
    return std::make_shared<arrow::Field>(this->name_, field->type());
  }
};

/**
 * @brief The addition logical expression.
 */
class Add : public MathBinaryExpression {
 public:
  Add(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : MathBinaryExpression("add", "+", left, right) { }
};

/**
 * @brief The subtraction logical expression.
 */
class Subtract : public MathBinaryExpression {
 public:
  Subtract(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : MathBinaryExpression("subtract", "-", left, right) { }
};

/**
 * @brief The multiplication logical expression.
 */
class Multiply : public MathBinaryExpression {
 public:
  Multiply(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : MathBinaryExpression("multiply", "*", left, right) { }
};

/**
 * @brief The division logical expression.
 */
class Divide : public MathBinaryExpression {
 public:
  Divide(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : MathBinaryExpression("divide", "/", left, right) { }
};

/**
 * @brief The modulus logical expression.
 */
class Modulus : public MathBinaryExpression {
 public:
  Modulus(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : MathBinaryExpression("modulus", "%", left, right) { }
};

}  // namespace logicalplan
}  // namespace toyquery

#endif  // LOGICALPLAN_LOGICALEXPRESSION_H