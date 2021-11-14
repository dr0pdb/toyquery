#ifndef DATASOURCE_DATASOURCE_H
#define DATASOURCE_DATASOURCE_H

#include <string>
#include <vector>

#include "arrow/api.h"
#include "common/macros.h"
#include "record_batch_iterator.h"

namespace toyquery {
namespace datasource {

/**
 * @brief Base class for all data sources.
 *
 */
class DataSource {
 public:
  DataSource() = default;

  virtual ~DataSource();

  /**
   * @brief Get the schema of the data source.
   *
   * @return std::shared_ptr<arrow::Schema> Schema of the data source.
   */
  virtual std::shared_ptr<arrow::Schema> Schema() = 0;

  /**
   * @brief Scan the data source, selecting the specified columns by name.
   *
   * @param projection: the columns to select.
   * @return RecordBatchIterator: the iterator to iterate over the record batches.
   */
  virtual RecordBatchIterator Scan(std::vector<std::string> projection) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(DataSource);
};

}  // namespace datasource
}  // namespace toyquery

#endif  // DATASOURCE_DATASOURCE_H