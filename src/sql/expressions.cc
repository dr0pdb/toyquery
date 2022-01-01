#include "sql/expressions.h"

namespace toyquery {
namespace sql {

SqlIdentifier::SqlIdentifier(absl::string_view id) : id_{ id } { }

SqlExpressionType SqlIdentifier::GetType() { return SqlExpressionType::SqlIdentifier; }

std::string SqlIdentifier::ToString() { return "todo"; }

SqlBinaryExpression::SqlBinaryExpression(
    std::shared_ptr<SqlExpression> left,
    std::string op,
    std::shared_ptr<SqlExpression> right)
    : left_{ left },
      op_{ op },
      right_{ right } { }

SqlExpressionType SqlBinaryExpression::GetType() { return SqlExpressionType::SqlBinaryExpression; }

std::string SqlBinaryExpression::ToString() { return "todo"; }

SqlLong::SqlLong(long value) : value_{ value } { }

SqlExpressionType SqlLong::GetType() { return SqlExpressionType::SqlLong; }

std::string SqlLong::ToString() { return "todo"; }

SqlString::SqlString(absl::string_view value) : value_{ value } { }

SqlExpressionType SqlString::GetType() { return SqlExpressionType::SqlString; }

std::string SqlString::ToString() { return "todo"; }

SqlDouble::SqlDouble(double value) : value_{ value } { }

SqlExpressionType SqlDouble::GetType() { return SqlExpressionType::SqlDouble; }

std::string SqlDouble::ToString() { return "todo"; }

}  // namespace sql
}  // namespace toyquery
