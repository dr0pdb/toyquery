#include "physicalplan/physicalexpression.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>

#include "absl/strings/string_view.h"
#include "test_utils/test_utils.h"

namespace toyquery {
namespace physicalplan {

using ::toyquery::testutils::AGE_COLUMN;
using ::toyquery::testutils::CompareArrowArrayWithChunkArray;
using ::toyquery::testutils::CompareArrowTableAndPrintDebugInfo;
using ::toyquery::testutils::GetAgeColumn;
using ::toyquery::testutils::GetAgeSum;
using ::toyquery::testutils::GetDummyRecordBatch;
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

}  // namespace physicalplan
}  // namespace toyquery

int main(int argc, char **argv) {
  // Initialize logging library.
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
