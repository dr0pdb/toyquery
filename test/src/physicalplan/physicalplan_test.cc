#include "physicalplan/physicalplan.h"

#include <gtest/gtest.h>

#include <memory>

#include "absl/strings/string_view.h"
#include "datasource/datasource.h"
#include "fmt/core.h"
#include "test_utils/test_utils.h"

namespace toyquery {
namespace physicalplan {

using ::toyquery::datasource::CsvDataSource;
using ::toyquery::testutils::CompareArrowTableAndPrintDebugInfo;
using ::toyquery::testutils::GetTestData;
using ::toyquery::testutils::GetTestSchema;
using ::toyquery::testutils::GetTestSchemaWithIdAndNameColumns;
using ::toyquery::testutils::ID_COLUMN;
using ::toyquery::testutils::NAME_COLUMN;

class PhysicalPlanTest : public ::testing::Test {
 protected:
  PhysicalPlanTest() { data_source_ = std::make_shared<CsvDataSource>("/tmp/test.csv", 10); }

  std::shared_ptr<Scan> getScanPlan() {
    std::vector<std::string> projection;
    return std::make_shared<Scan>(data_source_, projection);
  }

  std::shared_ptr<Scan> getScanPlan(std::vector<std::string> projection) {
    return std::make_shared<Scan>(data_source_, projection);
  }

  std::shared_ptr<Projection> getProjectionPlan() {
    auto scan = getScanPlan();
    std::vector<std::shared_ptr<PhysicalExpression>> projection = { std::make_shared<Column>(ID_COLUMN),
                                                                    std::make_shared<Column>(NAME_COLUMN) };
    return std::make_shared<Projection>(scan, GetTestSchemaWithIdAndNameColumns(), projection);
  }

  std::shared_ptr<CsvDataSource> data_source_;
};

//
// Scan tests
//

TEST_F(PhysicalPlanTest, ScanHasCorrectSchema) {
  auto scan = getScanPlan();
  auto expected_schema = GetTestSchema();

  auto schema_or = scan->Schema();

  EXPECT_TRUE(schema_or.ok()) << fmt::format("getting schema failed with {}", schema_or.status().message());
  EXPECT_TRUE(expected_schema->Equals(*schema_or));
}

TEST_F(PhysicalPlanTest, ScanHasCorrectSchemaWithProjection) {
  std::vector<std::string> projection = { "id", "name" };
  auto scan = getScanPlan(projection);
  auto expected_schema = GetTestSchemaWithIdAndNameColumns();

  auto schema_or = scan->Schema();

  EXPECT_TRUE(schema_or.ok()) << fmt::format("getting schema failed with {}", schema_or.status().message());
  EXPECT_TRUE(expected_schema->Equals(*schema_or));
}

TEST_F(PhysicalPlanTest, ScanReturnsErrorWithInvalidColumnName) {
  std::vector<std::string> projection = { "id", "INVALID_NAME" };
  auto scan = getScanPlan(projection);

  auto schema_or = scan->Schema();

  EXPECT_FALSE(schema_or.ok()) << fmt::format("getting schema failed with {}", schema_or.status().message());
  EXPECT_EQ(schema_or.status().message(), "The projection field with name INVALID_NAME wasn't found in the schema.");
}

void compareRecordBatchStreamWithExpectedTable(
    std::shared_ptr<toyquery::physicalplan::PhysicalPlan> plan,
    std::shared_ptr<arrow::Table> expected_data) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> result_data_batches;
  auto batch = plan->Next();
  EXPECT_TRUE(batch.ok());

  std::cout << "before while loop" << std::endl;

  while (batch.ok() && (*batch) != nullptr) {
    result_data_batches.push_back(*batch);

    std::cout << "batch value: " << (*batch)->ToString() << std::endl;

    batch = plan->Next();
    EXPECT_TRUE(batch.ok());
  }

  std::cout << "out of the while loop" << std::endl;

  auto result_data = arrow::Table::FromRecordBatches(result_data_batches);
  EXPECT_TRUE(result_data.ok()) << fmt::format("creating result data failed with error {}", result_data.status().message());
  EXPECT_TRUE(CompareArrowTableAndPrintDebugInfo(expected_data, *result_data));
}

TEST_F(PhysicalPlanTest, ScanReturnsCorrectData) {
  auto scan = getScanPlan();
  auto expected_data = GetTestData();

  auto prepare_status = scan->Prepare();
  EXPECT_TRUE(prepare_status.ok()) << fmt::format(
      "unexpected error in the prepare call for scan with message {}", prepare_status.message());
  compareRecordBatchStreamWithExpectedTable(scan, expected_data);
}

//
// Projection tests
//

}  // namespace physicalplan
}  // namespace toyquery

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
