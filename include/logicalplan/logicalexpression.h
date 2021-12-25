#ifndef LOGICALPLAN_LOGICALEXPRESSION_H
#define LOGICALPLAN_LOGICALEXPRESSION_H

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "arrow/api.h"
#include "common/macros.h"

namespace toyquery {
namespace logicalplan {

class LogicalPlan;

enum class LogicalExpressionType {
  // columns
  Column,
  ColumnIndex,

  // literals
  LiteralString,
  LiteralLong,
  LiteralDouble,

  // boolean
  Not,
  And,
  Or,

  // comparison
  Eq,
  Neq,
  Gt,
  GtEq,
  Lt,
  LtEq,

  // math
  Add,
  Subtract,
  Multiply,
  Divide,
  Modulus,

  // Aggregation
  Sum,
  Min,
  Max,
  Avg,
  Count,

  // misc.
  Cast,
  Alias,
};

/**
 * @brief Base class for all logical expressions.
 *
 * The logical expression provides information needed during the planning phase such as the name and data type of the
 * expression.
 */
struct LogicalExpression {
  LogicalExpression() = default;
  virtual ~LogicalExpression() = 0;

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
   * @brief Get the type of the logical expression.
   *
   * Can be used to dispatch to specific implementations based of this type.
   *
   * @return LogicalExpressionType: the type
   */
  virtual LogicalExpressionType type() = 0;

  /**
   * @brief Get string representation to print for debugging.
   *
   * @return std::string: the string representation of the expression.
   */
  virtual std::string ToString();
};

/**
 * @brief A reference to a column of a table by name.
 */
struct Column : public LogicalExpression {
  Column(std::string name);
  ~Column();

  /**
   * @copydoc LogicalExpression::ToField()
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override;

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Column; }

  /**
   * @copydoc LogicalExpression::ToString()
   */
  std::string ToString() override;

  std::string name_;
};

/**
 * @brief A reference to a column of a table by index.
 */
struct ColumnIndex : public LogicalExpression {
  ColumnIndex(int index);
  ~ColumnIndex();

  /**
   * @copydoc LogicalExpression::ToField()
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override;

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::ColumnIndex; }

  /**
   * @copydoc LogicalExpression::ToString()
   */
  std::string ToString() override;

  int index_;
};

/**
 * @brief A literal string expression.
 */
struct LiteralString : public LogicalExpression {
  LiteralString(std::string value);
  ~LiteralString();

  /**
   * @copydoc LogicalExpression::ToField()
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override;

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::LiteralString; }

  /**
   * @copydoc LogicalExpression::ToString()
   */
  std::string ToString() override;

  std::string value_;
};

/**
 * @brief A literal int64 expression.
 */
struct LiteralLong : public LogicalExpression {
  LiteralLong(int64_t value);
  ~LiteralLong();

  /**
   * @copydoc LogicalExpression::ToField()
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override;

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::LiteralLong; }

  /**
   * @copydoc LogicalExpression::ToString()
   */
  std::string ToString() override;

  int64_t value_;
};

/**
 * @brief A literal double expression.
 */
struct LiteralDouble : public LogicalExpression {
  LiteralDouble(double value);
  ~LiteralDouble();

  /**
   * @copydoc LogicalExpression::ToField()
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override;

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::LiteralDouble; }

  /**
   * @copydoc LogicalExpression::ToString()
   */
  std::string ToString() override;

  double value_;
};

/**
 * @brief A cast expression.
 */
struct Cast : public LogicalExpression {
  Cast(std::shared_ptr<LogicalExpression> expr, std::shared_ptr<arrow::DataType> type) : expr_{ expr }, type_{ type } {};
  ~Cast();

  /**
   * @copydoc LogicalExpression::ToField()
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override;

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Cast; }

  /**
   * @copydoc LogicalExpression::ToString()
   */
  std::string ToString() override;

  std::shared_ptr<LogicalExpression> expr_;
  std::shared_ptr<arrow::DataType> type_;
};

/**
 * @brief An alias logical expression.
 *
 * Format: expr AS alias
 */
struct Alias : public LogicalExpression {
  Alias(std::shared_ptr<LogicalExpression> expr, std::string alias)
      : expr_{ std::move(expr) },
        alias_{ std::move(alias) } { }
  ~Alias();

  /**
   * @copydoc LogicalExpression::ToField()
   *
   * The return type of the alias expression is that same as that of the input expression.
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override {
    ASSIGN_OR_RETURN(std::shared_ptr<arrow::Field> field, expr_->ToField(input));
    return std::make_shared<arrow::Field>(alias_, field->type());
  }

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Alias; }

  /**
   * @copydoc LogicalExpression::ToString()
   */
  std::string ToString() override { return "todo"; }

  std::shared_ptr<LogicalExpression> expr_;
  std::string alias_;
};

/**
 * @brief The base struct for a unary expression.
 */
struct UnaryExpression : public LogicalExpression {
  UnaryExpression(std::string name, std::string op, std::shared_ptr<LogicalExpression> expr)
      : name_{ std::move(name) },
        op_{ std::move(op) },
        expr_{ std::move(expr) } {};

  ~UnaryExpression();

  /**
   * @copydoc LogicalExpression::ToString()
   */
  std::string ToString() override;

  std::string name_;
  std::string op_;
  std::shared_ptr<LogicalExpression> expr_;
};

/**
 * @brief The logical NOT expression.
 */
struct Not : public UnaryExpression {
  Not(std::shared_ptr<LogicalExpression> expr) : UnaryExpression("not", "NOT", expr) { }
  ~Not();

  /**
   * @copydoc LogicalExpression::ToField()
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override {
    return std::make_shared<arrow::Field>(this->name_, arrow::boolean());
  }

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Not; }
};

/**
 * @brief The base struct for a binary expression.
 */
struct BinaryExpression : public LogicalExpression {
  BinaryExpression(
      std::string name,
      std::string op,
      std::shared_ptr<LogicalExpression> left,
      std::shared_ptr<LogicalExpression> right)
      : name_{ std::move(name) },
        op_{ std::move(op) },
        left_{ std::move(left) },
        right_{ std::move(right) } {};
  ~BinaryExpression();

  /**
   * @copydoc LogicalExpression::ToString()
   */
  std::string ToString() override { return "todo"; }

  std::string name_;
  std::string op_;
  std::shared_ptr<LogicalExpression> left_;
  std::shared_ptr<LogicalExpression> right_;
};

/**
 * @brief The base struct for a binary expression that outputs boolean.
 */
struct BooleanBinaryExpression : public BinaryExpression {
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
    return std::make_shared<arrow::Field>(this->name_, arrow::boolean());
  }
};

/**
 * @brief The AND logical expression.
 */
struct And : public BooleanBinaryExpression {
  And(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : BooleanBinaryExpression("and", "AND", left, right) { }

  ~And();

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Add; }
};

/**
 * @brief The OR logical expression.
 */
struct Or : public BooleanBinaryExpression {
  Or(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : BooleanBinaryExpression("or", "OR", left, right) { }

  ~Or();

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Or; }
};

/**
 * @brief The equality logical expression.
 */
struct Eq : public BooleanBinaryExpression {
  Eq(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : BooleanBinaryExpression("eq", "=", left, right) { }

  ~Eq();

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Eq; }
};

/**
 * @brief The inequality logical expression.
 */
struct Neq : public BooleanBinaryExpression {
  Neq(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : BooleanBinaryExpression("neq", "!=", left, right) { }

  ~Neq();

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Neq; }
};

/**
 * @brief The greater than logical expression.
 */
struct Gt : public BooleanBinaryExpression {
  Gt(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : BooleanBinaryExpression("ge", ">", left, right) { }

  ~Gt();

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Gt; }
};

/**
 * @brief The greater than equals logical expression.
 */
struct GtEq : public BooleanBinaryExpression {
  GtEq(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : BooleanBinaryExpression("gteq", ">=", left, right) { }

  ~GtEq();

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::GtEq; }
};

/**
 * @brief The less than logical expression.
 */
struct Lt : public BooleanBinaryExpression {
  Lt(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : BooleanBinaryExpression("lt", "<", left, right) { }

  ~Lt();

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Lt; }
};

/**
 * @brief The less than equals logical expression.
 */
struct LtEq : public BooleanBinaryExpression {
  LtEq(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : BooleanBinaryExpression("lteq", "<=", left, right) { }

  ~LtEq();

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::LtEq; }
};

/**
 * @brief The base struct for a binary expression that is the result of a mathematical operation.
 */
struct MathBinaryExpression : public BinaryExpression {
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
struct Add : public MathBinaryExpression {
  Add(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : MathBinaryExpression("add", "+", left, right) { }

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Add; }
};

/**
 * @brief The subtraction logical expression.
 */
struct Subtract : public MathBinaryExpression {
  Subtract(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : MathBinaryExpression("subtract", "-", left, right) { }

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Subtract; }
};

/**
 * @brief The multiplication logical expression.
 */
struct Multiply : public MathBinaryExpression {
  Multiply(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : MathBinaryExpression("multiply", "*", left, right) { }

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Multiply; }
};

/**
 * @brief The division logical expression.
 */
struct Divide : public MathBinaryExpression {
  Divide(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : MathBinaryExpression("divide", "/", left, right) { }

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Divide; }
};

/**
 * @brief The modulus logical expression.
 */
struct Modulus : public MathBinaryExpression {
  Modulus(std::shared_ptr<LogicalExpression> left, std::shared_ptr<LogicalExpression> right)
      : MathBinaryExpression("modulus", "%", left, right) { }

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Modulus; }
};

/**
 * @brief The base struct for an aggregate expression.
 */
struct AggregateExpression : public LogicalExpression {
  AggregateExpression(std::string name, std::shared_ptr<LogicalExpression> expr)
      : name_{ std::move(name) },
        expr_{ std::move(expr) } { }

  ~AggregateExpression();

  /**
   * @copydoc LogicalExpression::ToField()
   *
   * The type of the result of an aggregate expression is the type of expr on the given input.
   */
  absl::StatusOr<std::shared_ptr<arrow::Field>> ToField(std::shared_ptr<LogicalPlan> input) override {
    ASSIGN_OR_RETURN(std::shared_ptr<arrow::Field> field, expr_->ToField(input));
    return std::make_shared<arrow::Field>(this->name_, field->type());
  }

  std::string name_;
  std::shared_ptr<LogicalExpression> expr_;
};

/**
 * @brief The multiplication logical expression.
 */
struct Sum : public AggregateExpression {
  Sum(std::shared_ptr<LogicalExpression> input) : AggregateExpression("sum", input) { }

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Sum; }
};

/**
 * @brief The MIN aggregate logical expression.
 */
struct Min : public AggregateExpression {
  Min(std::shared_ptr<LogicalExpression> input) : AggregateExpression("min", input) { }

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Min; }
};

/**
 * @brief The MAX aggregate logical expression.
 */
struct Max : public AggregateExpression {
  Max(std::shared_ptr<LogicalExpression> input) : AggregateExpression("max", input) { }

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Max; }
};

/**
 * @brief The AVG aggregate logical expression.
 */
struct Avg : public AggregateExpression {
  Avg(std::shared_ptr<LogicalExpression> input) : AggregateExpression("avg", input) { }

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Avg; }
};

/**
 * @brief The COUNT aggregate logical expression.
 */
struct Count : public AggregateExpression {
  Count(std::shared_ptr<LogicalExpression> input) : AggregateExpression("count", input) { }

  /**
   * @copydoc LogicalExpression::type()
   */
  LogicalExpressionType type() override { return LogicalExpressionType::Count; }
};

}  // namespace logicalplan
}  // namespace toyquery

#endif  // LOGICALPLAN_LOGICALEXPRESSION_H