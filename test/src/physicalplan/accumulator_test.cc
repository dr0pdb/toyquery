#include "physicalplan/accumulator.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>

#include "absl/strings/string_view.h"
#include "test_utils/test_utils.h"

namespace toyquery {
namespace physicalplan {

using ::toyquery::testutils::CompareArrowTableAndPrintDebugInfo;
using ::toyquery::testutils::GetAgeColumn;
using ::toyquery::testutils::GetAgeSum;
using ::toyquery::testutils::GetMaxAge;
using ::toyquery::testutils::GetMinAge;
using ::toyquery::testutils::GetTestData;
using ::toyquery::testutils::GetTestSchema;

TEST(MaxAccumulatorTest, WorksCorrectly) {
  auto max_accumulator = std::make_unique<MaxAccumulator>();
  auto age_column = GetAgeColumn();

  for (int row_idx = 0; row_idx < age_column->length(); row_idx++) {
    EXPECT_TRUE(max_accumulator->Accumulate(age_column->GetScalar(row_idx).ValueOrDie()).ok());
  }

  auto max_value_or = max_accumulator->FinalValue();
  EXPECT_TRUE(max_value_or.ok());
  EXPECT_EQ(GetMaxAge(), std::static_pointer_cast<arrow::Int64Scalar>(*max_value_or)->value);
}

TEST(MinAccumulatorTest, WorksCorrectly) {
  auto min_accumulator = std::make_unique<MinAccumulator>();
  auto age_column = GetAgeColumn();

  for (int row_idx = 0; row_idx < age_column->length(); row_idx++) {
    EXPECT_TRUE(min_accumulator->Accumulate(age_column->GetScalar(row_idx).ValueOrDie()).ok());
  }

  auto min_value_or = min_accumulator->FinalValue();
  EXPECT_TRUE(min_value_or.ok());
  EXPECT_EQ(GetMinAge(), std::static_pointer_cast<arrow::Int64Scalar>(*min_value_or)->value);
}

TEST(SumAccumulatorTest, WorksCorrectly) {
  auto sum_accumulator = std::make_unique<SumAccumulator>();
  auto age_column = GetAgeColumn();

  for (int row_idx = 0; row_idx < age_column->length(); row_idx++) {
    EXPECT_TRUE(sum_accumulator->Accumulate(age_column->GetScalar(row_idx).ValueOrDie()).ok());
  }

  auto sum_value_or = sum_accumulator->FinalValue();
  EXPECT_TRUE(sum_value_or.ok());
  EXPECT_EQ(GetAgeSum(), std::static_pointer_cast<arrow::Int64Scalar>(*sum_value_or)->value);
}

// TODO: add more tests for other data types: float, string

}  // namespace physicalplan
}  // namespace toyquery

int main(int argc, char **argv) {
  // Initialize logging library.
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
