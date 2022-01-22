#ifndef PHYSICALPLAN_PHYSICALPLAN_H
#define PHYSICALPLAN_PHYSICALPLAN_H

#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arrow/api.h"
#include "common/macros.h"
#include "datasource/datasource.h"
#include "logicalplan/logicalexpression.h"
#include "physicalplan/aggregationexpression.h"
#include "physicalplan/physicalexpression.h"

namespace toyquery {
namespace physicalplan {

namespace {

using ::toyquery::datasource::DataSource;

}

/**
 * @brief Base class for all physical plans.
 *
 */
class PhysicalPlan {
 public:
  PhysicalPlan() = default;

  virtual ~PhysicalPlan() = 0;

  /**
   * @brief Get the schema of the physical plan.
   *
   * @return std::shared_ptr<arrow::Schema> Schema of the plan.
   */
  virtual absl::StatusOr<std::shared_ptr<arrow::Schema>> Schema() = 0;

  /**
   * @brief Get the children of the physical plan.
   *
   * @return std::vector<PhysicalPlan>: the children of the physical plan.
   */
  virtual std::vector<std::shared_ptr<PhysicalPlan>> Children() = 0;

  /**
   * @brief Prepare the physical plan.
   *
   * @note Can be an expensive operation if it requires IO operations.
   * @return absl::Status: indicates the status of preparation
   */
  virtual absl::Status Prepare() = 0;

  /**
   * @brief Get the next record batch.
   *
   * @return absl::StatusOr<std::shared_ptr<arrow::RecordBatch>>: the next record batch if successful. Error status
   * otherwise.
   * @note The absl::NotFoundError indicates that all batches have been emitted. TODO: rethink this.
   */
  virtual absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> Next() = 0;

  /**
   * @brief Get string representation to print for debugging.
   *
   * @return std::string: the string representation of the expression.
   */
  virtual std::string ToString();

 private:
  DISALLOW_COPY_AND_ASSIGN(PhysicalPlan);
};

/**
 * @brief The scan execution
 *
 */
class Scan : public PhysicalPlan {
 public:
  Scan(std::shared_ptr<DataSource> data_source, std::vector<std::string> projection);
  ~Scan() override;

  /**
   * @copydoc PhysicalPlan::Schema
   */
  absl::StatusOr<std::shared_ptr<arrow::Schema>> Schema() override;

  /**
   * @copydoc PhysicalPlan::Children
   */
  std::vector<std::shared_ptr<PhysicalPlan>> Children() override;

  /**
   * @copydoc PhysicalPlan::Prepare
   */
  absl::Status Prepare() override;

  /**
   * @copydoc PhysicalPlan::Next
   */
  absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> Next() override;

  /**
   * @copydoc PhysicalPlan::ToString
   */
  std::string ToString() override;

 private:
  DISALLOW_COPY_AND_ASSIGN(Scan);

  std::shared_ptr<DataSource> data_source_;
  std::vector<std::string> projection_;
  std::shared_ptr<arrow::TableBatchReader> batch_reader_{ nullptr };
};

/**
 * @brief The projection execution
 *
 */
class Projection : public PhysicalPlan {
 public:
  Projection(
      std::shared_ptr<PhysicalPlan> input,
      std::shared_ptr<arrow::Schema> schema,
      std::vector<std::shared_ptr<PhysicalExpression>> projection);

  ~Projection() override;

  /**
   * @copydoc PhysicalPlan::Schema
   */
  absl::StatusOr<std::shared_ptr<arrow::Schema>> Schema() override;

  /**
   * @copydoc PhysicalPlan::Children
   */
  std::vector<std::shared_ptr<PhysicalPlan>> Children() override;

  /**
   * @copydoc PhysicalPlan::Prepare
   */
  absl::Status Prepare() override;

  /**
   * @copydoc PhysicalPlan::Next
   */
  absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> Next() override;

  /**
   * @copydoc PhysicalPlan::ToString
   */
  std::string ToString() override;

 private:
  DISALLOW_COPY_AND_ASSIGN(Projection);

  std::shared_ptr<PhysicalPlan> input_;
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<std::shared_ptr<PhysicalExpression>> projection_;
};

/**
 * @brief The selection execution
 *
 */
class Selection : public PhysicalPlan {
 public:
  Selection(std::shared_ptr<PhysicalPlan> input, std::shared_ptr<PhysicalExpression> predicate);
  ~Selection() override;

  /**
   * @copydoc PhysicalPlan::Schema
   */
  absl::StatusOr<std::shared_ptr<arrow::Schema>> Schema() override;

  /**
   * @copydoc PhysicalPlan::Children
   */
  std::vector<std::shared_ptr<PhysicalPlan>> Children() override;

  /**
   * @copydoc PhysicalPlan::Prepare
   */
  absl::Status Prepare() override;

  /**
   * @copydoc PhysicalPlan::Next
   */
  absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> Next() override;

  /**
   * @copydoc PhysicalPlan::ToString
   */
  std::string ToString() override;

 private:
  absl::StatusOr<std::shared_ptr<arrow::Array>> filterColumn(
      std::shared_ptr<arrow::Array> data,
      std::shared_ptr<arrow::BooleanArray> predicate);

  std::shared_ptr<PhysicalPlan> input_;
  std::shared_ptr<PhysicalExpression> predicate_;

  DISALLOW_COPY_AND_ASSIGN(Selection);
};

/**
 * @brief The hash aggregation execution
 *
 */
class HashAggregation : public PhysicalPlan {
 public:
  HashAggregation(
      std::shared_ptr<PhysicalPlan> input,
      std::shared_ptr<arrow::Schema> schema,
      std::vector<std::shared_ptr<PhysicalExpression>> grouping_expressions,
      std::vector<std::shared_ptr<AggregationExpression>> aggregation_expressions);
  ~HashAggregation() override;

  /**
   * @copydoc PhysicalPlan::Schema
   */
  absl::StatusOr<std::shared_ptr<arrow::Schema>> Schema() override;

  /**
   * @copydoc PhysicalPlan::Children
   */
  std::vector<std::shared_ptr<PhysicalPlan>> Children() override;

  /**
   * @copydoc PhysicalPlan::Prepare
   */
  absl::Status Prepare() override;

  /**
   * @copydoc PhysicalPlan::Next
   */
  absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> Next() override;

  /**
   * @copydoc PhysicalPlan::ToString
   */
  std::string ToString() override;

 private:
  absl::StatusOr<std::vector<std::shared_ptr<arrow::RecordBatch>>> getAllInputBatches();

  std::shared_ptr<PhysicalPlan> input_;
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<std::shared_ptr<PhysicalExpression>> grouping_expressions_;
  std::vector<std::shared_ptr<AggregationExpression>> aggregation_expressions_;

  std::unique_ptr<arrow::TableBatchReader> batch_reader_;
  std::shared_ptr<arrow::Table> processed_table_;

  DISALLOW_COPY_AND_ASSIGN(HashAggregation);
};

}  // namespace physicalplan
}  // namespace toyquery

#endif  // PHYSICALPLAN_PHYSICALPLAN_H