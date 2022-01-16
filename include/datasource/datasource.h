#ifndef DATASOURCE_DATASOURCE_H
#define DATASOURCE_DATASOURCE_H

#include <string>
#include <vector>

#include "arrow/api.h"
#include "common/macros.h"

namespace toyquery {
namespace datasource {

/**
 * @brief Base class for all data sources.
 *
 */
class DataSource {
 public:
  DataSource();

  virtual ~DataSource();

  /**
   * @brief Get the schema of the data source.
   *
   * @return std::shared_ptr<arrow::Schema> Schema of the data source.
   */
  virtual absl::StatusOr<std::shared_ptr<arrow::Schema>> Schema() = 0;

  /**
   * @brief Scan the data source, selecting the specified columns by name.
   *
   * @param projection: the columns to select.
   * @return absl::StatusOr<std::shared_ptr<arrow::TableBatchReader>>: the iterator to iterate over the record batches.
   */
  virtual absl::StatusOr<std::shared_ptr<arrow::TableBatchReader>> Scan(std::vector<std::string> projection) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(DataSource);
};

class CsvDataSource : public DataSource {
 public:
  CsvDataSource(std::string filename, int batch_size);
  CsvDataSource(std::string filename, int batch_size, std::shared_ptr<arrow::Schema> schema);

  ~CsvDataSource() override;

  /**
   * @copydoc DataSource::Schema
   *
   * @note Expensive operation: The csv file is read in order to obtain the schema if not already provided.
   */
  absl::StatusOr<std::shared_ptr<arrow::Schema>> Schema() override;

  /**
   * @copydoc DataSource::Scan
   */
  absl::StatusOr<std::shared_ptr<arrow::TableBatchReader>> Scan(std::vector<std::string> projection) override;

  /**
   * @brief Read a file into an arrow::Table
   *
   * @param projection: the projection to apply on the csv file.
   * @return absl::StatusOr<std::shared_ptr<arrow::Table>>: the arrow table
   */
  absl::StatusOr<std::shared_ptr<arrow::Table>> ReadFile(std::vector<std::string> projection);

 private:
  std::string filename_;
  int batch_size_;
  std::shared_ptr<arrow::Schema> schema_;
};

}  // namespace datasource
}  // namespace toyquery

#endif  // DATASOURCE_DATASOURCE_H