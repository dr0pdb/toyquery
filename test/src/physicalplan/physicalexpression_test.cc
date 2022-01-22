#include "physicalplan/physicalexpression.h"

#include <gtest/gtest.h>

#include <memory>

#include "absl/strings/string_view.h"
#include "common/arrow.h"
#include "fmt/core.h"
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

TEST(LiteralBooleanTest, WorksCorrectly) {
  auto record_batch = GetDummyRecordBatch();
  auto literal_string_expr = std::make_shared<LiteralBoolean>(true);

  auto literal_column_or = literal_string_expr->Evaluate(record_batch);

  EXPECT_TRUE(literal_column_or.ok());
  EXPECT_EQ((*literal_column_or)->length(), record_batch->num_rows());
  for (int idx = 0; idx < record_batch->num_rows(); idx++) {
    EXPECT_TRUE((*literal_column_or)->GetScalar(idx).ValueOrDie()->Equals(std::make_shared<arrow::BooleanScalar>(true)));
  }
}

template<typename T, typename V, typename X>
void compare_all_rows(
    std::shared_ptr<PhysicalExpression> left,
    std::shared_ptr<PhysicalExpression> right,
    std::shared_ptr<arrow::RecordBatch> record_batch,
    X expected_result) {
  auto expr = std::make_unique<T>(left, right);
  auto result_or = expr->Evaluate(record_batch);

  EXPECT_TRUE(result_or.ok()) << "failed with message: " << result_or.status() << std::endl;
  EXPECT_EQ((*result_or)->length(), record_batch->num_rows());
  for (int idx = 0; idx < record_batch->num_rows(); idx++) {
    auto value = (*result_or)->GetScalar(idx).ValueOrDie();
    EXPECT_TRUE(value->Equals(std::make_shared<V>(expected_result))) << fmt::format(
        "Mismatch between expected and actual value. expected {}, actual {}", expected_result, value->ToString());
  }
}

template<typename T>
void compare_all_rows_double(
    std::shared_ptr<PhysicalExpression> left,
    std::shared_ptr<PhysicalExpression> right,
    std::shared_ptr<arrow::RecordBatch> record_batch,
    double expected_result) {
  auto expr = std::make_unique<T>(left, right);
  auto result_or = expr->Evaluate(record_batch);

  EXPECT_TRUE(result_or.ok()) << "failed with message: " << result_or.status() << std::endl;
  EXPECT_EQ((*result_or)->length(), record_batch->num_rows());
  for (int idx = 0; idx < record_batch->num_rows(); idx++) {
    auto value = std::static_pointer_cast<arrow::DoubleScalar>((*result_or)->GetScalar(idx).ValueOrDie())->value;
    EXPECT_TRUE(abs(value - expected_result) <= DOUBLE_ACCEPTED_MARGIN)
        << fmt::format("Mismatch between expected and actual value. expected {}, actual {}", expected_result, value);
  }
}

TEST(EqExpressionTest, WorksCorrectly) {
  auto record_batch = GetDummyRecordBatch();

  // ints
  compare_all_rows<EqExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(111), record_batch, /*expected_result=*/false);
  compare_all_rows<EqExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(101), record_batch, /*expected_result=*/true);

  // strings
  compare_all_rows<EqExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralString>("hello"),
      std::make_shared<LiteralString>("hello2"),
      record_batch,
      /*expected_result=*/false);
  compare_all_rows<EqExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralString>("hello"),
      std::make_shared<LiteralString>("hello"),
      record_batch,
      /*expected_result=*/true);

  // double
  compare_all_rows<EqExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralDouble>(1.11), std::make_shared<LiteralDouble>(1.12), record_batch, /*expected_result=*/false);
  compare_all_rows<EqExpression, arrow::BooleanScalar, bool>(
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
  compare_all_rows<NeqExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(111), record_batch, /*expected_result=*/true);
  compare_all_rows<NeqExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(101), record_batch, /*expected_result=*/false);

  // strings
  compare_all_rows<NeqExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralString>("hello"),
      std::make_shared<LiteralString>("hello2"),
      record_batch,
      /*expected_result=*/true);
  compare_all_rows<NeqExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralString>("hello"),
      std::make_shared<LiteralString>("hello"),
      record_batch,
      /*expected_result=*/false);

  // double
  compare_all_rows<NeqExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralDouble>(1.11), std::make_shared<LiteralDouble>(1.12), record_batch, /*expected_result=*/true);
  compare_all_rows<NeqExpression, arrow::BooleanScalar, bool>(
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

TEST(AndExpressionTest, WorksCorrectly) {
  auto record_batch = GetDummyRecordBatch();

  compare_all_rows<AndExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralBoolean>(true), std::make_shared<LiteralBoolean>(true), record_batch, true);
  compare_all_rows<AndExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralBoolean>(true), std::make_shared<LiteralBoolean>(false), record_batch, false);
  compare_all_rows<AndExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralBoolean>(false), std::make_shared<LiteralBoolean>(true), record_batch, false);
  compare_all_rows<AndExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralBoolean>(false), std::make_shared<LiteralBoolean>(false), record_batch, false);
}

TEST(OrExpressionTest, WorksCorrectly) {
  auto record_batch = GetDummyRecordBatch();

  compare_all_rows<OrExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralBoolean>(true), std::make_shared<LiteralBoolean>(true), record_batch, true);
  compare_all_rows<OrExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralBoolean>(true), std::make_shared<LiteralBoolean>(false), record_batch, true);
  compare_all_rows<OrExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralBoolean>(false), std::make_shared<LiteralBoolean>(true), record_batch, true);
  compare_all_rows<OrExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralBoolean>(false), std::make_shared<LiteralBoolean>(false), record_batch, false);
}

TEST(LessThanExpressionTest, WorksCorrectly) {
  auto record_batch = GetDummyRecordBatch();

  // ints
  compare_all_rows<LessThanExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(111), record_batch, /*expected_result=*/true);
  compare_all_rows<LessThanExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(90), record_batch, /*expected_result=*/false);
  compare_all_rows<LessThanExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(101), record_batch, /*expected_result=*/false);

  // strings
  compare_all_rows<LessThanExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralString>("hello"),
      std::make_shared<LiteralString>("hello2"),
      record_batch,
      /*expected_result=*/true);
  compare_all_rows<LessThanExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralString>("hello"),
      std::make_shared<LiteralString>("hello"),
      record_batch,
      /*expected_result=*/false);
  compare_all_rows<LessThanExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralString>("hello2"),
      std::make_shared<LiteralString>("hello"),
      record_batch,
      /*expected_result=*/false);

  // double
  compare_all_rows<LessThanExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralDouble>(1.11),
      std::make_shared<LiteralDouble>(1.12),
      record_batch,
      /*expected_result=*/true);
  compare_all_rows<LessThanExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralDouble>(1.11),
      std::make_shared<LiteralDouble>(1.11),
      record_batch,
      /*expected_result=*/false);
}

TEST(LessThanEqualsExpressionTest, WorksCorrectly) {
  auto record_batch = GetDummyRecordBatch();

  // ints
  compare_all_rows<LessThanEqualsExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(111), record_batch, /*expected_result=*/true);
  compare_all_rows<LessThanEqualsExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(90), record_batch, /*expected_result=*/false);
  compare_all_rows<LessThanEqualsExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(101), record_batch, /*expected_result=*/true);

  // strings
  compare_all_rows<LessThanEqualsExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralString>("hello"),
      std::make_shared<LiteralString>("hello2"),
      record_batch,
      /*expected_result=*/true);
  compare_all_rows<LessThanEqualsExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralString>("hello"),
      std::make_shared<LiteralString>("hello"),
      record_batch,
      /*expected_result=*/true);
  compare_all_rows<LessThanEqualsExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralString>("hello2"),
      std::make_shared<LiteralString>("hello"),
      record_batch,
      /*expected_result=*/false);

  // double
  compare_all_rows<LessThanEqualsExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralDouble>(1.11),
      std::make_shared<LiteralDouble>(1.12),
      record_batch,
      /*expected_result=*/true);
  compare_all_rows<LessThanEqualsExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralDouble>(1.11),
      std::make_shared<LiteralDouble>(1.11),
      record_batch,
      /*expected_result=*/true);
  compare_all_rows<LessThanEqualsExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralDouble>(1.11),
      std::make_shared<LiteralDouble>(1.10),
      record_batch,
      /*expected_result=*/false);
}

TEST(GreaterThanExpressionTest, WorksCorrectly) {
  auto record_batch = GetDummyRecordBatch();

  // ints
  compare_all_rows<GreaterThanExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(111), record_batch, /*expected_result=*/false);
  compare_all_rows<GreaterThanExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(90), record_batch, /*expected_result=*/true);
  compare_all_rows<GreaterThanExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(101), record_batch, /*expected_result=*/false);

  // strings
  compare_all_rows<GreaterThanExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralString>("hello"),
      std::make_shared<LiteralString>("hello2"),
      record_batch,
      /*expected_result=*/false);
  compare_all_rows<GreaterThanExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralString>("hello"),
      std::make_shared<LiteralString>("hello"),
      record_batch,
      /*expected_result=*/false);
  compare_all_rows<GreaterThanExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralString>("hello2"),
      std::make_shared<LiteralString>("hello"),
      record_batch,
      /*expected_result=*/true);

  // double
  compare_all_rows<GreaterThanExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralDouble>(1.11),
      std::make_shared<LiteralDouble>(1.12),
      record_batch,
      /*expected_result=*/false);
  compare_all_rows<GreaterThanExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralDouble>(1.11),
      std::make_shared<LiteralDouble>(1.11),
      record_batch,
      /*expected_result=*/false);
  compare_all_rows<GreaterThanExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralDouble>(1.11),
      std::make_shared<LiteralDouble>(1.10),
      record_batch,
      /*expected_result=*/true);
}

TEST(GreaterThanEqualsExpressionTest, WorksCorrectly) {
  auto record_batch = GetDummyRecordBatch();

  // ints
  compare_all_rows<GreaterThanEqualsExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(111), record_batch, /*expected_result=*/false);
  compare_all_rows<GreaterThanEqualsExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(90), record_batch, /*expected_result=*/true);
  compare_all_rows<GreaterThanEqualsExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(101), record_batch, /*expected_result=*/true);

  // strings
  compare_all_rows<GreaterThanEqualsExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralString>("hello"),
      std::make_shared<LiteralString>("hello2"),
      record_batch,
      /*expected_result=*/false);
  compare_all_rows<GreaterThanEqualsExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralString>("hello"),
      std::make_shared<LiteralString>("hello"),
      record_batch,
      /*expected_result=*/true);
  compare_all_rows<GreaterThanEqualsExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralString>("hello2"),
      std::make_shared<LiteralString>("hello"),
      record_batch,
      /*expected_result=*/true);

  // double
  compare_all_rows<GreaterThanEqualsExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralDouble>(1.11),
      std::make_shared<LiteralDouble>(1.12),
      record_batch,
      /*expected_result=*/false);
  compare_all_rows<GreaterThanEqualsExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralDouble>(1.11),
      std::make_shared<LiteralDouble>(1.11),
      record_batch,
      /*expected_result=*/true);
  compare_all_rows<GreaterThanEqualsExpression, arrow::BooleanScalar, bool>(
      std::make_shared<LiteralDouble>(1.11),
      std::make_shared<LiteralDouble>(1.10),
      record_batch,
      /*expected_result=*/true);
}

TEST(AddExpressionTest, WorksCorrectly) {
  auto record_batch = GetDummyRecordBatch();

  // int64
  compare_all_rows<AddExpression, arrow::Int64Scalar, int64_t>(
      std::make_shared<LiteralLong>(101), std::make_shared<LiteralLong>(111), record_batch, /*expected_result=*/212);

  // double
  compare_all_rows_double<AddExpression>(
      std::make_shared<LiteralDouble>(1.11),
      std::make_shared<LiteralDouble>(1.12),
      record_batch,
      /*expected_result=*/2.23);
}

TEST(SubtractExpressionTest, WorksCorrectly) {
  auto record_batch = GetDummyRecordBatch();

  // int64
  compare_all_rows<SubtractExpression, arrow::Int64Scalar, int64_t>(
      std::make_shared<LiteralLong>(151000), std::make_shared<LiteralLong>(100000), record_batch, /*expected_result=*/51000);
  compare_all_rows<SubtractExpression, arrow::Int64Scalar, int64_t>(
      std::make_shared<LiteralLong>(151000),
      std::make_shared<LiteralLong>(200000),
      record_batch,
      /*expected_result=*/-49000);

  // double
  compare_all_rows_double<SubtractExpression>(
      std::make_shared<LiteralDouble>(1.11),
      std::make_shared<LiteralDouble>(1.12),
      record_batch,
      /*expected_result=*/-0.01);
}

TEST(MultiplyExpressionTest, WorksCorrectly) {
  auto record_batch = GetDummyRecordBatch();

  // int64
  compare_all_rows<MultiplyExpression, arrow::Int64Scalar, int64_t>(
      std::make_shared<LiteralLong>(5), std::make_shared<LiteralLong>(91), record_batch, /*expected_result=*/455);

  // double
  compare_all_rows_double<MultiplyExpression>(
      std::make_shared<LiteralDouble>(1.11),
      std::make_shared<LiteralDouble>(1.12),
      record_batch,
      /*expected_result=*/1.2432);
}

TEST(DivideExpressionTest, WorksCorrectly) {
  auto record_batch = GetDummyRecordBatch();

  // int64
  compare_all_rows<DivideExpression, arrow::Int64Scalar, int64_t>(
      std::make_shared<LiteralLong>(100), std::make_shared<LiteralLong>(4), record_batch, /*expected_result=*/25);

  // double
  compare_all_rows_double<DivideExpression>(
      std::make_shared<LiteralDouble>(1.11),
      std::make_shared<LiteralDouble>(1.91),
      record_batch,
      /*expected_result=*/0.58115183246);
}

}  // namespace physicalplan
}  // namespace toyquery

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
