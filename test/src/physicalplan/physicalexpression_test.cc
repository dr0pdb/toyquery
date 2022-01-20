#include "physicalplan/physicalexpression.h"

#include <gtest/gtest.h>

#include <memory>

#include "absl/strings/string_view.h"
#include "test_utils/test_utils.h"

namespace toyquery {
namespace physicalplan {

using ::toyquery::testutils::AGE_COLUMN;
using ::toyquery::testutils::CompareArrowArrayWithChunkArray;
using ::toyquery::testutils::CompareArrowTableAndPrintDebugInfo;
using ::toyquery::testutils::CompareIdAndAgeColumn;
using ::toyquery::testutils::GetAgeColumn;
using ::toyquery::testutils::GetAgeColumnExpression;
using ::toyquery::testutils::GetAgeSum;
using ::toyquery::testutils::GetDummyRecordBatch;
using ::toyquery::testutils::GetIdColumnExpression;
using ::toyquery::testutils::GetMaxAge;
using ::toyquery::testutils::GetMinAge;
using ::toyquery::testutils::GetTestData;
using ::toyquery::testutils::GetTestSchema;

TEST(ColumnTest, WorksCorrectly) {
  auto record_batch = GetDummyRecordBatch();
  auto age_column_expr = std::make_shared<Column>(AGE_COLUMN);
  auto expected_age_column = GetAgeColumn();

  auto age_column_or = age_column_expr->Evaluate(record_batch);

  EXPECT_TRUE(age_column_or.ok());
  EXPECT_TRUE(CompareArrowArrayWithChunkArray(*age_column_or, expected_age_column));
}

TEST(LiteralLongTest, WorksCorrectly) {
  auto record_batch = GetDummyRecordBatch();
  auto literal_long_expr = std::make_shared<LiteralLong>(119382);

  auto literal_column_or = literal_long_expr->Evaluate(record_batch);

  EXPECT_TRUE(literal_column_or.ok());
  EXPECT_EQ((*literal_column_or)->length(), record_batch->num_rows());
  for (int idx = 0; idx < record_batch->num_rows(); idx++) {
    EXPECT_TRUE((*literal_column_or)->GetScalar(idx).ValueOrDie()->Equals(std::make_shared<arrow::Int64Scalar>(119382)));
  }
}

TEST(LiteralDoubleTest, WorksCorrectly) {
  auto record_batch = GetDummyRecordBatch();
  auto literal_double_expr = std::make_shared<LiteralDouble>(119.382);

  auto literal_column_or = literal_double_expr->Evaluate(record_batch);

  EXPECT_TRUE(literal_column_or.ok());
  EXPECT_EQ((*literal_column_or)->length(), record_batch->num_rows());
  for (int idx = 0; idx < record_batch->num_rows(); idx++) {
    EXPECT_TRUE((*literal_column_or)->GetScalar(idx).ValueOrDie()->Equals(std::make_shared<arrow::DoubleScalar>(119.382)));
  }
}

TEST(LiteralStringTest, WorksCorrectly) {
  auto record_batch = GetDummyRecordBatch();
  auto literal_string_expr = std::make_shared<LiteralString>("test");

  auto literal_column_or = literal_string_expr->Evaluate(record_batch);

  EXPECT_TRUE(literal_column_or.ok());
  EXPECT_EQ((*literal_column_or)->length(), record_batch->num_rows());
  for (int idx = 0; idx < record_batch->num_rows(); idx++) {
    EXPECT_TRUE((*literal_column_or)->GetScalar(idx).ValueOrDie()->Equals(std::make_shared<arrow::StringScalar>("test")));
  }
}

template<typename T>
void compare_all_rows(
    std::shared_ptr<PhysicalExpression> left,
    std::shared_ptr<PhysicalExpression> right,
    std::shared_ptr<arrow::RecordBatch> record_batch,
    bool expected_result) {
  auto expr = std::make_unique<T>(left, right);
  auto result_or = expr->Evaluate(record_batch);

  EXPECT_TRUE(result_or.ok());
  EXPECT_EQ((*result_or)->length(), record_batch->num_rows());
  for (int idx = 0; idx < record_batch->num_rows(); idx++) {
    EXPECT_TRUE((*result_or)->GetScalar(idx).ValueOrDie()->Equals(std::make_shared<arrow::BooleanScalar>(expected_result)));
  }
}

TEST(EqExpressionTest, WorksCorrectly) {
  auto record_batch = GetDummyRecordBatch();

  // ints
  compare_all_rows<EqExpression>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(111), record_batch, /*expected_result=*/false);
  compare_all_rows<EqExpression>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(101), record_batch, /*expected_result=*/true);

  // strings
  compare_all_rows<EqExpression>(
      std::make_shared<LiteralString>("hello"),
      std::make_shared<LiteralString>("hello2"),
      record_batch,
      /*expected_result=*/false);
  compare_all_rows<EqExpression>(
      std::make_shared<LiteralString>("hello"),
      std::make_shared<LiteralString>("hello"),
      record_batch,
      /*expected_result=*/true);

  // double
  compare_all_rows<EqExpression>(
      std::make_shared<LiteralDouble>(1.11), std::make_shared<LiteralDouble>(1.12), record_batch, /*expected_result=*/false);
  compare_all_rows<EqExpression>(
      std::make_shared<LiteralDouble>(1.11), std::make_shared<LiteralDouble>(1.11), record_batch, /*expected_result=*/true);

  // columns - id and age
  auto id_col_expr = GetIdColumnExpression();
  auto age_col_expr = GetAgeColumnExpression();
  auto eq_expr = std::make_unique<EqExpression>(id_col_expr, age_col_expr);
  auto result_or = eq_expr->Evaluate(record_batch);
  EXPECT_TRUE(result_or.ok());
  EXPECT_EQ((*result_or)->length(), record_batch->num_rows());
  EXPECT_TRUE((*result_or)->Equals(CompareIdAndAgeColumn(true)));

  // different types
  auto eq_expr_1 =
      std::make_unique<EqExpression>(std::make_shared<LiteralLong>(1), std::make_shared<LiteralString>("hello2"));
  auto result_or_1 = eq_expr_1->Evaluate(record_batch);
  EXPECT_FALSE(result_or_1.ok());
  EXPECT_EQ(result_or_1.status().message(), "Boolean expression operands do not have the same type");
}

TEST(NeqExpressionTest, WorksCorrectly) {
  auto record_batch = GetDummyRecordBatch();

  // ints
  compare_all_rows<NeqExpression>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(111), record_batch, /*expected_result=*/true);
  compare_all_rows<NeqExpression>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(101), record_batch, /*expected_result=*/false);

  // strings
  compare_all_rows<NeqExpression>(
      std::make_shared<LiteralString>("hello"),
      std::make_shared<LiteralString>("hello2"),
      record_batch,
      /*expected_result=*/true);
  compare_all_rows<NeqExpression>(
      std::make_shared<LiteralString>("hello"),
      std::make_shared<LiteralString>("hello"),
      record_batch,
      /*expected_result=*/false);

  // double
  compare_all_rows<NeqExpression>(
      std::make_shared<LiteralDouble>(1.11), std::make_shared<LiteralDouble>(1.12), record_batch, /*expected_result=*/true);
  compare_all_rows<NeqExpression>(
      std::make_shared<LiteralDouble>(1.11), std::make_shared<LiteralDouble>(1.11), record_batch, /*expected_result=*/false);

  // columns - id and age
  auto id_col_expr = GetIdColumnExpression();
  auto age_col_expr = GetAgeColumnExpression();
  auto eq_expr = std::make_unique<NeqExpression>(id_col_expr, age_col_expr);
  auto result_or = eq_expr->Evaluate(record_batch);
  EXPECT_TRUE(result_or.ok());
  EXPECT_EQ((*result_or)->length(), record_batch->num_rows());
  EXPECT_TRUE((*result_or)->Equals(CompareIdAndAgeColumn(false)));

  // different types
  auto eq_expr_1 =
      std::make_unique<NeqExpression>(std::make_shared<LiteralLong>(1), std::make_shared<LiteralString>("hello2"));
  auto result_or_1 = eq_expr_1->Evaluate(record_batch);
  EXPECT_FALSE(result_or_1.ok());
  EXPECT_EQ(result_or_1.status().message(), "Boolean expression operands do not have the same type");
}

}  // namespace physicalplan
}  // namespace toyquery

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
