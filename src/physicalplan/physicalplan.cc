#include "physicalplan/physicalplan.h"

#include <unordered_map>

#include "common/arrow.h"
#include "common/key.h"
#include "common/status.h"

namespace toyquery {
namespace physicalplan {

using toyquery::common::GetMessageFromStatus;

absl::StatusOr<std::shared_ptr<arrow::Schema>> Scan::Schema() {
  ASSIGN_OR_RETURN(auto schema, data_source_->Schema());
  if (projection_.empty()) return schema;
  return FilterSchema(schema, projection_);
}

std::vector<std::shared_ptr<PhysicalPlan>> Scan::Children() { return {}; }

absl::Status Scan::Prepare() {
  ASSIGN_OR_RETURN(batch_reader_, data_source_->Scan(projection_));
  return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> Scan::Next() {
  auto maybe_batch = batch_reader_->Next();
  if (!maybe_batch.ok()) { return absl::InternalError(GetMessageFromResult(maybe_batch)); }
  return *maybe_batch;
}

std::string Scan::ToString() { return "todo"; }

absl::StatusOr<std::shared_ptr<arrow::Schema>> Projection::Schema() { return schema_; }

std::vector<std::shared_ptr<PhysicalPlan>> Projection::Children() { return { input_ }; }

absl::Status Projection::Prepare() { return input_->Prepare(); }

absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> Projection::Next() {
  ASSIGN_OR_RETURN(auto batch, input_->Next());

  std::vector<std::shared_ptr<arrow::Array>> columns;
  for (auto& expr : projection_) {
    ASSIGN_OR_RETURN(auto col, expr->Evaluate(batch));
    columns.push_back(col);
  }

  return arrow::RecordBatch::Make(schema_, static_cast<int64_t>(projection_.size()), columns);
}

std::string Projection::ToString() { return "todo"; }

absl::StatusOr<std::shared_ptr<arrow::Schema>> Selection::Schema() { return input_->Schema(); }

std::vector<std::shared_ptr<PhysicalPlan>> Selection::Children() { return { input_ }; }

absl::Status Selection::Prepare() { return input_->Prepare(); }

absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> Selection::Next() {
  ASSIGN_OR_RETURN(auto batch, input_->Next());
  ASSIGN_OR_RETURN(auto schema, input_->Schema());

  ASSIGN_OR_RETURN(auto filtering_result, predicate_->Evaluate(batch));
  auto filter = std::static_pointer_cast<arrow::BooleanArray>(filtering_result);

  std::vector<std::shared_ptr<arrow::Array>> columns_post_filtering;
  for (auto& column : batch->columns()) {
    ASSIGN_OR_RETURN(auto filtered_column, filterColumn(column, filter));
    columns_post_filtering.push_back(filtered_column);
  }

  return arrow::RecordBatch::Make(schema, filter->true_count(), columns_post_filtering);
}

absl::StatusOr<std::shared_ptr<arrow::Array>> Selection::filterColumn(
    std::shared_ptr<arrow::Array> data,
    std::shared_ptr<arrow::BooleanArray> predicate) {
#define FILTER_ARROW_ARRAY_WITH_PREDICATE(array_tp, builder_tp)                      \
  auto typed_data = std::static_pointer_cast<array_tp>(data);                        \
  builder_tp builder;                                                                \
  builder.Reserve(predicate->true_count());                                          \
                                                                                     \
  for (int idx = 0; idx < typed_data->length(); idx++) {                             \
    if (predicate->GetView(idx)) { builder.UnsafeAppend(typed_data->GetView(idx)); } \
  }                                                                                  \
                                                                                     \
  auto array = builder.Finish();                                                     \
  if (!array.ok()) { return absl::InternalError(GetMessageFromResult(array)); }      \
  return *array;

  switch (data->type_id()) {
    case arrow::Type::BOOL: {
      FILTER_ARROW_ARRAY_WITH_PREDICATE(arrow::BooleanArray, arrow::BooleanBuilder);
    }
    case arrow::Type::INT64: {
      FILTER_ARROW_ARRAY_WITH_PREDICATE(arrow::Int64Array, arrow::Int64Builder);
    }
    case arrow::Type::DOUBLE: {
      FILTER_ARROW_ARRAY_WITH_PREDICATE(arrow::DoubleArray, arrow::DoubleBuilder);
    }
    case arrow::Type::STRING: {
      FILTER_ARROW_ARRAY_WITH_PREDICATE(arrow::StringArray, arrow::StringBuilder);
    }

    default: return absl::InternalError("Unsupported type.");
  }

#undef FILTER_ARROW_ARRAY_WITH_PREDICATE
}

std::string Selection::ToString() { return "todo"; }

absl::StatusOr<std::shared_ptr<arrow::Schema>> HashAggregation::Schema() { return schema_; }

std::vector<std::shared_ptr<PhysicalPlan>> HashAggregation::Children() { return { input_ }; }

absl::Status HashAggregation::Prepare() { return input_->Prepare(); }

absl::StatusOr<std::shared_ptr<arrow::RecordBatch>> HashAggregation::Next() {
  if (batch_reader_ != nullptr) {
    auto next_batch_or = batch_reader_->Next();
    if (!next_batch_or.ok()) { return absl::InternalError(GetMessageFromResult(next_batch_or)); }
    return *next_batch_or;
  }

  ASSIGN_OR_RETURN(auto all_batches, getAllInputBatches());

  // map from the tuple of grouping keys to list of accumulators
  // <gk1, gk2, gk3 .. gkx> -> [ac1, ac2, .. acy]
  std::unordered_map<toyquery::Key, std::vector<std::shared_ptr<Accumulator>>> m;

  for (auto& batch : all_batches) {
    // calculate the grouping keys for this batch
    std::vector<std::shared_ptr<arrow::Array>> grouping_keys;
    for (auto& gk : grouping_expressions_) {
      ASSIGN_OR_RETURN(auto gki, gk->Evaluate(batch));
      grouping_keys.push_back(gki);
    }

    // calculate the input to the aggregate expressions.
    // Eg: SUM (4 * Col_1) => 4 * Col_1 is the input.
    std::vector<std::shared_ptr<arrow::Array>> aggregation_inputs;
    for (auto& ai : aggregation_expressions_) {
      ASSIGN_OR_RETURN(auto aii, ai->GetInputExpression()->Evaluate(batch));
      aggregation_inputs.push_back(aii);
    }

    // process each row of the batch
    for (int row_idx = 0; row_idx < batch->num_rows(); row_idx++) {
      // get the row key for the hash map
      std::vector<std::shared_ptr<arrow::Scalar>> row_key_vector;
      for (auto& gk : grouping_keys) {
        auto row_key_or = gk->GetScalar(row_idx);
        if (!row_key_or.ok()) { return absl::InternalError(GetMessageFromResult(row_key_or)); }
        row_key_vector.push_back(*row_key_or);
      }
      toyquery::Key row_key(row_key_vector);

      // create accumulator for this row key if it doesn't exist.
      if (m.find(row_key) == m.end()) {
        std::vector<std::shared_ptr<Accumulator>> row_accumulators;
        for (auto& ai : aggregation_expressions_) {
          ASSIGN_OR_RETURN(auto accum, ai->CreateAccumulator());
          row_accumulators.push_back(accum);
        }

        m[row_key] = row_accumulators;
      }

      // perform the accumulation.
      auto row_accumulators = m[row_key];
      for (int accumulator_index = 0; accumulator_index < row_accumulators.size(); accumulator_index++) {
        auto accumulator_input_or = aggregation_inputs.at(accumulator_index)->GetScalar(row_idx);
        if (!accumulator_input_or.ok()) { return absl::InternalError(GetMessageFromResult(accumulator_input_or)); }

        CHECK_OK_OR_RETURN(row_accumulators.at(accumulator_index)->Accumulate(*accumulator_input_or));
      }
    }
  }

  std::vector<std::shared_ptr<arrow::Array>> aggregated_data(schema_->num_fields());
  int num_rows = m.size();

  std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders(schema_->num_fields());
  for (auto& b : builders) { b->Reserve(num_rows); }

  for (auto& it : m) {
    auto& gk = it.first;
    auto& accum = it.second;

    // insert the row keys.
    for (int col_idx = 0; col_idx < gk.scalars_.size(); col_idx++) {
#define APPEND_ACCUMULATED_SCALAR(builder_type, scaler_type)                         \
  auto typed_builder = std::static_pointer_cast<builder_type>(builders.at(col_idx)); \
  typed_builder->UnsafeAppend(std::static_pointer_cast<scaler_type>(gk.scalars_[col_idx])->value);

      switch (schema_->field(col_idx)->type()->id()) {
        case arrow::Type::BOOL: {
          APPEND_ACCUMULATED_SCALAR(arrow::BooleanBuilder, arrow::BooleanScalar);
          break;
        }
        case arrow::Type::INT64: {
          APPEND_ACCUMULATED_SCALAR(arrow::Int64Builder, arrow::Int64Scalar);
          break;
        }
        case arrow::Type::DOUBLE: {
          APPEND_ACCUMULATED_SCALAR(arrow::DoubleBuilder, arrow::DoubleScalar);
          break;
        }
        case arrow::Type::STRING: {
          // TODO
          break;
        }
        default: return absl::InternalError("Unsupported type");
      }
#undef APPEND_ACCUMULATED_SCALAR
    }

    // insert the accumulated values for the row key.
    for (int col_idx = gk.scalars_.size(); col_idx < gk.scalars_.size() + accum.size(); col_idx++) {
#define APPEND_ACCUMULATED_SCALAR(builder_type, scaler_type)                         \
  auto typed_builder = std::static_pointer_cast<builder_type>(builders.at(col_idx)); \
  ASSIGN_OR_RETURN(auto accum_value, accum[accum_idx]->FinalValue());                \
  typed_builder->UnsafeAppend(std::static_pointer_cast<scaler_type>(accum_value)->value);

      auto accum_idx = col_idx - gk.scalars_.size();
      switch (schema_->field(col_idx)->type()->id()) {
        case arrow::Type::BOOL: {
          APPEND_ACCUMULATED_SCALAR(arrow::BooleanBuilder, arrow::BooleanScalar);
          break;
        }
        case arrow::Type::INT64: {
          APPEND_ACCUMULATED_SCALAR(arrow::Int64Builder, arrow::Int64Scalar);
          break;
        }
        case arrow::Type::DOUBLE: {
          APPEND_ACCUMULATED_SCALAR(arrow::DoubleBuilder, arrow::DoubleScalar);
          break;
        }
        case arrow::Type::STRING: {
          // TODO
          break;
        }
        default: return absl::InternalError("Unsupported type");
      }

#undef APPEND_ACCUMULATED_SCALAR
    }
  }

  // finish the builders and populate them in aggregated_data.
  for (int i = 0; i < builders.size(); i++) {
    auto finish_status = builders[i]->Finish(&aggregated_data[i]);
    if (!finish_status.ok()) { return absl::InternalError(GetMessageFromStatus(finish_status)); }
  }

  processed_table_ = arrow::Table::Make(schema_, aggregated_data);
  batch_reader_ = std::make_unique<arrow::TableBatchReader>(processed_table_.operator*());

  auto first_batch_or = batch_reader_->Next();
  if (!first_batch_or.ok()) { return absl::InternalError(GetMessageFromResult(first_batch_or)); }
  return *first_batch_or;
}

absl::StatusOr<std::vector<std::shared_ptr<arrow::RecordBatch>>> HashAggregation::getAllInputBatches() {
  std::vector<std::shared_ptr<arrow::RecordBatch>> all_batches;

  while (true) {
    auto maybe_batch = input_->Next();
    if (!maybe_batch.ok()) {
      if (absl::IsNotFound(maybe_batch.status())) {
        return all_batches;
      } else {
        return maybe_batch.status();
      }
    }

    all_batches.push_back(*maybe_batch);
  }

  return all_batches;
}

std::string HashAggregation::ToString() { return "todo"; }

}  // namespace physicalplan
}  // namespace toyquery
