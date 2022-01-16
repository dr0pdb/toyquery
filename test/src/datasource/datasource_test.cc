#include "datasource/datasource.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>

#include "absl/strings/string_view.h"
#include "test_utils/test_utils.h"

namespace toyquery {
namespace datasource {

using ::toyquery::testutils::CompareArrowTableAndPrintDebugInfo;
using ::toyquery::testutils::GetTestData;
using ::toyquery::testutils::GetTestSchema;

class CsvDataSourceTest : public ::testing::Test {
 protected:
  CsvDataSourceTest() { csv_data_source_ = std::make_unique<CsvDataSource>("/tmp/test.csv", 10); }

  std::unique_ptr<CsvDataSource> csv_data_source_;
};

TEST_F(CsvDataSourceTest, ReadsDataWithCorrectSchema) {
  auto expected_schema = GetTestSchema();

  auto schema_or = csv_data_source_->Schema();

  EXPECT_TRUE(schema_or.ok());
  EXPECT_TRUE(expected_schema->Equals(*schema_or));
}

TEST_F(CsvDataSourceTest, ReadsDataWithCorrectBatches) {
  auto expected_table = GetTestData();

  auto table_or = csv_data_source_->ReadFile({});

  EXPECT_TRUE(table_or.ok());
  EXPECT_TRUE(CompareArrowTableAndPrintDebugInfo(expected_table, *table_or));
}

}  // namespace datasource
}  // namespace toyquery

int main(int argc, char **argv) {
  // Initialize logging library.
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
