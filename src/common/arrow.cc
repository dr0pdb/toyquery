#include "common/arrow.h"

#include "fmt/core.h"

namespace toyquery {

absl::StatusOr<std::shared_ptr<arrow::Schema>> FilterSchema(
    std::shared_ptr<arrow::Schema> schema,
    std::vector<std::string> projection) {
  auto fields = schema->fields();
  arrow::FieldVector projected_fields;

  for (const auto& name : projection) {
    auto field = schema->GetFieldByName(name);
    if (field == nullptr) {
      return absl::InvalidArgumentError(fmt::format("The projection field with name {} wasn't found in the schema.", name));
    }

    projected_fields.push_back(field);
  }

  return std::make_shared<arrow::Schema>(projected_fields);
}

}  // namespace toyquery
