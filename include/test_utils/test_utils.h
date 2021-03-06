#ifndef TESTUTILS_TESTUTILS_H
#define TESTUTILS_TESTUTILS_H

/**
 * Contains utilities to be used by unit tests. The implementations are simple and may not be most efficient.
 */

#include <memory>

#include "arrow/api.h"
#include "physicalplan/physicalexpression.h"

namespace toyquery {
namespace testutils {

static constexpr int ID_COLUMN = 0;
static constexpr int NAME_COLUMN = 1;
static constexpr int AGE_COLUMN = 2;
static constexpr int FREQUENCY_COLUMN = 3;

std::shared_ptr<arrow::Schema> GetTestSchema() {
  std::vector<std::shared_ptr<arrow::Field>> expected_fields = { std::make_shared<arrow::Field>("id", arrow::int64()),
                                                                 std::make_shared<arrow::Field>("name", arrow::utf8()),
                                                                 std::make_shared<arrow::Field>("age", arrow::int64()),
                                                                 std::make_shared<arrow::Field>(
                                                                     "frequency", arrow::float64()) };
  return std::make_shared<arrow::Schema>(expected_fields);
}

std::shared_ptr<arrow::Schema> GetTestSchemaWithIdAndNameColumns() {
  std::vector<std::shared_ptr<arrow::Field>> expected_fields = { std::make_shared<arrow::Field>("id", arrow::int64()),
                                                                 std::make_shared<arrow::Field>("name", arrow::utf8()) };
  return std::make_shared<arrow::Schema>(expected_fields);
}

std::shared_ptr<arrow::Table> GetTestData() {
  // id
  arrow::Int64Builder id_builder;
  id_builder.Append(1);
  id_builder.Append(2);
  id_builder.Append(3);
  id_builder.Append(4);
  id_builder.Append(5);
  id_builder.Append(6);
  id_builder.Append(7);
  auto maybe_id_array = id_builder.Finish();
  if (!maybe_id_array.ok()) { return nullptr; }

  // name
  arrow::StringBuilder name_builder;
  name_builder.Append("random1");
  name_builder.Append("random2");
  name_builder.Append("random3");
  name_builder.Append("random4");
  name_builder.Append("random5");
  name_builder.Append("random6");
  name_builder.Append("random7");
  auto maybe_name_array = name_builder.Finish();
  if (!maybe_name_array.ok()) { return nullptr; }

  // age
  arrow::Int64Builder age_builder;
  age_builder.Append(1);
  age_builder.Append(2);
  age_builder.Append(3);
  age_builder.Append(44);
  age_builder.Append(55);
  age_builder.Append(66);
  age_builder.Append(77);
  auto maybe_age_array = age_builder.Finish();
  if (!maybe_age_array.ok()) { return nullptr; }

  // frequency
  arrow::DoubleBuilder frequency_builder;
  frequency_builder.Append(1.1);
  frequency_builder.Append(2.2);
  frequency_builder.Append(3.3);
  frequency_builder.Append(4.4);
  frequency_builder.Append(5.5);
  frequency_builder.Append(6.6);
  frequency_builder.Append(7.7);
  auto maybe_frequency_array = frequency_builder.Finish();
  if (!maybe_frequency_array.ok()) { return nullptr; }

  return arrow::Table::Make(
      GetTestSchema(), { *maybe_id_array, *maybe_name_array, *maybe_age_array, *maybe_frequency_array });
}

std::shared_ptr<toyquery::physicalplan::PhysicalExpression> GetIdColumnExpression() {
  return std::make_shared<toyquery::physicalplan::Column>(ID_COLUMN);
}

std::shared_ptr<arrow::ChunkedArray> GetIdColumn() { return GetTestData()->column(ID_COLUMN); }

std::shared_ptr<toyquery::physicalplan::PhysicalExpression> GetNameColumnExpression() {
  return std::make_shared<toyquery::physicalplan::Column>(NAME_COLUMN);
}

std::shared_ptr<arrow::ChunkedArray> GetNameColumn() { return GetTestData()->column(NAME_COLUMN); }

int GetMinAge() { return 1; }
int GetMaxAge() { return 77; }
int GetAgeSum() { return 248; }

std::shared_ptr<toyquery::physicalplan::PhysicalExpression> GetAgeColumnExpression() {
  return std::make_shared<toyquery::physicalplan::Column>(AGE_COLUMN);
}

std::shared_ptr<arrow::ChunkedArray> GetAgeColumn() { return GetTestData()->column(AGE_COLUMN); }

std::shared_ptr<toyquery::physicalplan::PhysicalExpression> GetFrequencyColumnExpression() {
  return std::make_shared<toyquery::physicalplan::Column>(FREQUENCY_COLUMN);
}

std::shared_ptr<arrow::ChunkedArray> GetFrequencyColumn() { return GetTestData()->column(FREQUENCY_COLUMN); }

bool CompareArrowTable(std::shared_ptr<arrow::Table> expected_table, std::shared_ptr<arrow::Table> table) {
  if (expected_table->num_rows() != table->num_rows()) return false;
  if (expected_table->num_columns() != table->num_columns()) return false;

  for (int col_idx = 0; col_idx < expected_table->num_columns(); col_idx++) {
    if (!expected_table->column(col_idx)->Equals(table->column(col_idx))) return false;
  }

  return true;
}

/**
 * @brief Prints the debug information if the two tables don't match.
 */
bool CompareArrowTableAndPrintDebugInfo(std::shared_ptr<arrow::Table> expected_table, std::shared_ptr<arrow::Table> table) {
  bool result = CompareArrowTable(expected_table, table);
  if (!result) {
    std::cout << "Expected table\n" << expected_table->ToString() << std::endl;
    std::cout << "Actual table\n" << table->ToString() << std::endl;
  }
  return result;
}

std::shared_ptr<arrow::RecordBatch> GetDummyRecordBatch() {
  return std::make_shared<arrow::TableBatchReader>(GetTestData().operator*())->Next().ValueOrDie();
}

bool CompareArrowArrayWithChunkArray(std::shared_ptr<arrow::Array> arr, std::shared_ptr<arrow::ChunkedArray> chunk_arr) {
  if (arr->length() != chunk_arr->length()) return false;
  int64_t rows = arr->length();

  for (int64_t row = 0; row < rows; row++) {
    if (!arr->GetScalar(row).ValueOrDie()->Equals(chunk_arr->GetScalar(row).ValueOrDie())) {
      std::cout << "Found diff at idx: " << row << ", arr[idx]: " << arr->GetScalar(row).ValueOrDie()->ToString()
                << " chunk_arr[idx]: " << chunk_arr->GetScalar(row).ValueOrDie()->ToString() << std::endl;
      return false;
    }
  }

  return true;
}

std::shared_ptr<arrow::Array> CompareIdAndAgeColumn(bool eq_expected) {
  arrow::BooleanBuilder builder;
  builder.Append(eq_expected);
  builder.Append(eq_expected);
  builder.Append(eq_expected);
  builder.Append(!eq_expected);
  builder.Append(!eq_expected);
  builder.Append(!eq_expected);
  builder.Append(!eq_expected);
  auto maybe_id_array = builder.Finish();
  if (!maybe_id_array.ok()) { return nullptr; }
  return *maybe_id_array;
}

}  // namespace testutils
}  // namespace toyquery

#endif  // TESTUTILS_TESTUTILS_H