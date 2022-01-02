#include "sql/expressions.h"

namespace toyquery {
namespace sql {

SqlIdentifier::SqlIdentifier(absl::string_view id) : id_{ id } { }

SqlExpressionType SqlIdentifier::GetType() { return SqlExpressionType::SqlIdentifier; }

std::string SqlIdentifier::ToString() { return "todo"; }

SqlBinaryExpression::SqlBinaryExpression(
    std::shared_ptr<SqlExpression> left,
    absl::string_view op,
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

SqlFunction::SqlFunction(absl::string_view id, std::vector<std::shared_ptr<SqlExpression>> args)
    : id_{ id },
      args_{ args } { }

SqlExpressionType SqlFunction::GetType() { return SqlExpressionType::SqlFunction; }

std::string SqlFunction::ToString() { return "todo"; }

SqlAlias::SqlAlias(std::shared_ptr<SqlExpression> expr, std::shared_ptr<SqlIdentifier> alias)
    : expr_{ expr },
      alias_{ alias } { }

SqlExpressionType SqlAlias::GetType() { return SqlExpressionType::SqlAlias; }

std::string SqlAlias::ToString() { return "todo"; }

SqlCast::SqlCast(std::shared_ptr<SqlExpression> expr, std::shared_ptr<SqlIdentifier> data_type)
    : expr_{ expr },
      data_type_{ data_type } { }

SqlExpressionType SqlCast::GetType() { return SqlExpressionType::SqlCast; }

std::string SqlCast::ToString() { return "todo"; }

SqlSort::SqlSort(std::shared_ptr<SqlExpression> expr, bool asc) : expr_{ expr }, asc_{ asc } { }

SqlExpressionType SqlSort::GetType() { return SqlExpressionType::SqlSort; }

std::string SqlSort::ToString() { return "todo"; }

SqlSelect::SqlSelect(
    std::vector<std::shared_ptr<SqlExpression>> projection,
    std::shared_ptr<SqlExpression> selection,
    std::vector<std::shared_ptr<SqlExpression>> group_by,
    std::vector<std::shared_ptr<SqlSort>> order_by,
    std::shared_ptr<SqlExpression> having,
    absl::string_view table_name)
    : projection_{ projection },
      selection_{ selection },
      group_by_{ group_by },
      order_by_{ order_by },
      having_{ having },
      table_name_{ table_name } { }

SqlExpressionType SqlSelect::GetType() { return SqlExpressionType::SqlSelect; }

std::string SqlSelect::ToString() { return "todo"; }

}  // namespace sql
}  // namespace toyquery
