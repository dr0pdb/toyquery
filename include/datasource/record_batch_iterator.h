#ifndef DATASOURCE_RECORDBATCH_H
#define DATASOURCE_RECORDBATCH_H

#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "common/iterator.h"
#include "common/macros.h"

namespace toyquery {
namespace datasource {

/**
 * @brief Iterator over Arrow RecordBatch.
 *
 */
class RecordBatchIterator : public common::Iterator<std::shared_ptr<arrow::RecordBatch>> {
 public:
  RecordBatchIterator() = default;

  ~RecordBatchIterator();

  /**
   * @brief Get the next RecordBatch from the iterator.
   *
   * @return std::shared_ptr<arrow::RecordBatch>: the next batch or null.
   */

  std::shared_ptr<arrow::RecordBatch> Next() override;

 private:
  // TODO(dr0pdb): needs to store state (index, data source) to be able to return the next batch.

  DISALLOW_COPY_AND_ASSIGN(RecordBatchIterator);
};

}  // namespace datasource
}  // namespace toyquery

#endif  // DATASOURCE_RECORDBATCH_H