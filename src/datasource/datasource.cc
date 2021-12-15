#include "datasource/datasource.h"

#include "arrow/csv/api.h"
#include "arrow/io/api.h"
#include "common/status.h"

namespace toyquery {
namespace datasource {

using ::toyquery::common::GetMessageFromResult;

absl::StatusOr<std::shared_ptr<arrow::Schema>> CsvDataSource::Schema() {
  if (schema_ != nullptr) { return schema_; }

  ASSIGN_OR_RETURN(auto table, readFile({}));
  schema_ = table->schema();
  return schema_;
}

absl::StatusOr<std::shared_ptr<arrow::TableBatchReader>> CsvDataSource::Scan(std::vector<std::string> projection) {
  ASSIGN_OR_RETURN(auto table, readFile(projection));
  return std::make_shared<arrow::TableBatchReader>(table.operator*());
}

absl::StatusOr<std::shared_ptr<arrow::Table>> CsvDataSource::readFile(std::vector<std::string> projection) {
  arrow::io::IOContext io_context = arrow::io::default_io_context();
  auto maybe_input = arrow::io::ReadableFile::Open(filename_);
  if (!maybe_input.ok()) { return absl::InternalError(GetMessageFromResult(maybe_input)); }

  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto parse_options = arrow::csv::ParseOptions::Defaults();

  // Apply projection if required.
  auto convert_options = arrow::csv::ConvertOptions::Defaults();
  if (!projection.empty()) { convert_options.include_columns = projection; }

  // Instantiate TableReader from input stream and options
  auto maybe_reader = arrow::csv::TableReader::Make(io_context, *maybe_input, read_options, parse_options, convert_options);
  if (!maybe_reader.ok()) { return absl::InternalError(GetMessageFromResult(maybe_reader)); }

  // Read table from CSV file
  std::shared_ptr<arrow::csv::TableReader> reader = *maybe_reader;
  auto maybe_table = reader->Read();
  if (!maybe_table.ok()) { return absl::InternalError(GetMessageFromResult(maybe_table)); }
  std::shared_ptr<arrow::Table> table = *maybe_table;

  return table;
}

}  // namespace datasource
}  // namespace toyquery
