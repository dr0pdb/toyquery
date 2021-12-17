#include "physicalplan/physicalplan.h"

#include "common/arrow.h"
#include "common/status.h"

namespace toyquery {
namespace physicalplan {

using toyquery::common::GetMessageFromResult;

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

}  // namespace physicalplan
}  // namespace toyquery
