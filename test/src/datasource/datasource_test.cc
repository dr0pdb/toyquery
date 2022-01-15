#include "datasource/datasource.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>

#include "absl/strings/string_view.h"

namespace toyquery {
namespace datasource {

class CsvDataSourceTest : public ::testing::Test {
 protected:
  CsvDataSourceTest() { csv_data_source_ = std::make_unique<CsvDataSource>("/tmp/test.csv", 10); }

  std::unique_ptr<CsvDataSource> csv_data_source_;
};

TEST_F(CsvDataSourceTest, ReadsDataCorrectlyWithCorrectSchema) {
  auto schema_or = csv_data_source_->Schema();
  EXPECT_TRUE(schema_or.ok());
  std::cout << (*schema_or)->ToString() << std::endl;
}

}  // namespace datasource
}  // namespace toyquery

int main(int argc, char **argv) {
  // Initialize logging library.
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
