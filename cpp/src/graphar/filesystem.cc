/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <memory>
#include "graphar/writer_util.h"
#ifdef ARROW_ORC
#include "arrow/adapters/orc/adapter.h"
#endif
#include "arrow/api.h"
#include "arrow/csv/api.h"
#include "arrow/dataset/api.h"
#if defined(ARROW_VERSION) && ARROW_VERSION <= 12000000
#include "arrow/dataset/file_json.h"
#endif
#include "arrow/filesystem/api.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/ipc/writer.h"
#include "parquet/arrow/writer.h"
#include "simple-uri-parser/uri_parser.h"

#include "graphar/expression.h"
#include "graphar/filesystem.h"
#include "graphar/fwd.h"
#include "graphar/general_params.h"

namespace graphar::detail {
template <typename U, typename T>
static Status CastToLargeOffsetArray(
    const std::shared_ptr<arrow::Array>& in,
    const std::shared_ptr<arrow::DataType>& to_type,
    std::shared_ptr<arrow::Array>& out) {  // NOLINT(runtime/references)
  auto array_data = in->data()->Copy();
  auto offset = array_data->buffers[1];
  using from_offset_type = typename U::offset_type;
  using to_string_offset_type = typename T::offset_type;
  auto raw_value_offsets_ =
      offset == NULLPTR
          ? NULLPTR
          : reinterpret_cast<const from_offset_type*>(offset->data());
  std::vector<to_string_offset_type> to_offset(offset->size() /
                                               sizeof(from_offset_type));
  for (size_t i = 0; i < to_offset.size(); ++i) {
    to_offset[i] = raw_value_offsets_[i];
  }
  std::shared_ptr<arrow::Buffer> buffer;
  arrow::TypedBufferBuilder<to_string_offset_type> buffer_builder;
  RETURN_NOT_ARROW_OK(
      buffer_builder.Append(to_offset.data(), to_offset.size()));
  RETURN_NOT_ARROW_OK(buffer_builder.Finish(&buffer));
  array_data->type = to_type;
  array_data->buffers[1] = buffer;
  out = arrow::MakeArray(array_data);
  RETURN_NOT_ARROW_OK(out->ValidateFull());
  return Status::OK();
}

template <typename U, typename T>
static Status CastToLargeOffsetArray(
    const std::shared_ptr<arrow::ChunkedArray>& in,
    const std::shared_ptr<arrow::DataType>& to_type,
    std::shared_ptr<arrow::ChunkedArray>& out) {  // NOLINT(runtime/references)
  std::vector<std::shared_ptr<arrow::Array>> chunks;
  for (auto const& chunk : in->chunks()) {
    std::shared_ptr<arrow::Array> array;
    auto status = CastToLargeOffsetArray<U, T>(chunk, to_type, array);
    GAR_RETURN_NOT_OK(status);
    chunks.emplace_back(array);
  }
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(out, arrow::ChunkedArray::Make(chunks));
  return Status::OK();
}
}  // namespace graphar::detail

namespace graphar {
namespace ds = arrow::dataset;

std::shared_ptr<ds::FileFormat> FileSystem::GetFileFormat(
    const FileType type) const {
  switch (type) {
  case CSV:
    return std::make_shared<ds::CsvFileFormat>();
  case PARQUET:
    return std::make_shared<ds::ParquetFileFormat>();
  case JSON:
    return std::make_shared<ds::JsonFileFormat>();
#ifdef ARROW_ORC
  case ORC:
    return std::make_shared<ds::OrcFileFormat>();
#endif
  default:
    return nullptr;
  }
}

void buildOrcWriteOptionsWithWriterOptions(
    arrow::adapters::orc::WriteOptions& writeOptions,
    const std::shared_ptr<WriterOptions>& options);
void buildCsvWriteOptionsWithWriterOptions(
    arrow::csv::WriteOptions& writeOptions,
    const std::shared_ptr<WriterOptions>& options);
void buildParquetWriteOptionsWithWriterOptions(
    parquet::WriterProperties::Builder& builder,
    parquet::ArrowWriterProperties::Builder& arrow_writer_porpertices_builder,
    const std::shared_ptr<WriterOptions>& options);

Result<std::shared_ptr<arrow::Table>> FileSystem::ReadFileToTable(
    const std::string& path, FileType file_type,
    const util::FilterOptions& options) const noexcept {
  std::shared_ptr<ds::FileFormat> format = GetFileFormat(file_type);
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(
      auto factory, arrow::dataset::FileSystemDatasetFactory::Make(
                        arrow_fs_, {path}, format,
                        arrow::dataset::FileSystemFactoryOptions()));
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto dataset, factory->Finish());
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto scan_builder, dataset->NewScan());

  // Apply the row filter and select the specified columns
  if (options.filter) {
    GAR_ASSIGN_OR_RAISE(auto filter, options.filter->Evaluate());
    RETURN_NOT_ARROW_OK(scan_builder->Filter(filter));
  }
  if (options.columns) {
    RETURN_NOT_ARROW_OK(scan_builder->Project(*options.columns));
  }

  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto scanner, scan_builder->Finish());
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto table, scanner->ToTable());
  // cast string array to large string array as we need concatenate chunks in
  // some places, e.g., in vineyard
  for (int i = 0; i < table->num_columns(); ++i) {
    std::shared_ptr<arrow::DataType> type = table->column(i)->type();
    if (type->id() == arrow::Type::STRING) {
      type = arrow::large_utf8();
    } else if (type->id() == arrow::Type::BINARY) {
      type = arrow::large_binary();
    }
    if (type->Equals(table->column(i)->type())) {
      continue;
    }
    // do casting
    auto field = table->field(i)->WithType(type);
    std::shared_ptr<arrow::ChunkedArray> chunked_array;

    if (table->num_rows() == 0) {
      GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(
          chunked_array, arrow::ChunkedArray::MakeEmpty(type));
    } else if (type->Equals(arrow::large_utf8())) {
      auto status = detail::CastToLargeOffsetArray<arrow::StringArray,
                                                   arrow::LargeStringArray>(
          table->column(i), type, chunked_array);
      GAR_RETURN_NOT_OK(status);
    } else if (type->Equals(arrow::large_binary())) {
      auto status = detail::CastToLargeOffsetArray<arrow::BinaryArray,
                                                   arrow::LargeBinaryArray>(
          table->column(i), type, chunked_array);
      GAR_RETURN_NOT_OK(status);
    } else {
      // noop
      chunked_array = table->column(i);
    }
    GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(table, table->RemoveColumn(i));
    GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(
        table, table->AddColumn(i, field, chunked_array));
  }
  return table;
}

template <typename T>
Result<T> FileSystem::ReadFileToValue(const std::string& path) const noexcept {
  T ret;
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto input,
                                       arrow_fs_->OpenInputStream(path));
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto bytes,
                                       input->Read(sizeof(T), &ret));
  ARROW_UNUSED(bytes);
  return ret;
}

template <>
Result<std::string> FileSystem::ReadFileToValue(
    const std::string& path) const noexcept {
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto access_file,
                                       arrow_fs_->OpenInputFile(path));
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto bytes, access_file->GetSize());
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto buffer,
                                       access_file->ReadAt(0, bytes));
  return buffer->ToString();
}

template <typename T>
Status FileSystem::WriteValueToFile(const T& value,
                                    const std::string& path) const noexcept {
  // try to create the directory, oss filesystem may not support this, ignore
  ARROW_UNUSED(arrow_fs_->CreateDir(path.substr(0, path.find_last_of("/"))));
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto ofstream,
                                       arrow_fs_->OpenOutputStream(path));
  RETURN_NOT_ARROW_OK(ofstream->Write(&value, sizeof(T)));
  RETURN_NOT_ARROW_OK(ofstream->Close());
  return Status::OK();
}

template <>
Status FileSystem::WriteValueToFile(const std::string& value,
                                    const std::string& path) const noexcept {
  // try to create the directory, oss filesystem may not support this, ignore
  ARROW_UNUSED(arrow_fs_->CreateDir(path.substr(0, path.find_last_of("/"))));
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto ofstream,
                                       arrow_fs_->OpenOutputStream(path));
  RETURN_NOT_ARROW_OK(ofstream->Write(value.c_str(), value.size()));
  RETURN_NOT_ARROW_OK(ofstream->Close());
  return Status::OK();
}

Status FileSystem::WriteTableToFile(
    const std::shared_ptr<arrow::Table>& table, FileType file_type,
    const std::string& path,
    const std::shared_ptr<WriterOptions>& options) const noexcept {
  // try to create the directory, oss filesystem may not support this, ignore
  ARROW_UNUSED(arrow_fs_->CreateDir(path.substr(0, path.find_last_of("/"))));
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto output_stream,
                                       arrow_fs_->OpenOutputStream(path));
  switch (file_type) {
  case FileType::CSV: {
    auto write_options = arrow::csv::WriteOptions::Defaults();
    write_options.include_header = true;
    write_options.quoting_style = arrow::csv::QuotingStyle::Needed;
    buildCsvWriteOptionsWithWriterOptions(write_options, options);
    GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(
        auto writer, arrow::csv::MakeCSVWriter(output_stream.get(),
                                               table->schema(), write_options));
    RETURN_NOT_ARROW_OK(writer->WriteTable(*table));
    RETURN_NOT_ARROW_OK(writer->Close());
    break;
  }
  case FileType::PARQUET: {
    auto schema = table->schema();
    auto column_num = schema->num_fields();
    parquet::WriterProperties::Builder writer_porpertices_builder;
    writer_porpertices_builder.compression(
        arrow::Compression::type::ZSTD);  // default = ZSTD
    parquet::ArrowWriterProperties::Builder arrow_writer_porpertices_builder;
    buildParquetWriteOptionsWithWriterOptions(
        writer_porpertices_builder, arrow_writer_porpertices_builder, options);
    RETURN_NOT_ARROW_OK(parquet::arrow::WriteTable(
        *table, arrow::default_memory_pool(), output_stream, 64 * 1024 * 1024,
        writer_porpertices_builder.build(),
        arrow_writer_porpertices_builder.build()));
    break;
  }
#ifdef ARROW_ORC
  case FileType::ORC: {
    auto writer_options = arrow::adapters::orc::WriteOptions();
    buildOrcWriteOptionsWithWriterOptions(writer_options, options);
    GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(
        auto writer, arrow::adapters::orc::ORCFileWriter::Open(
                         output_stream.get(), writer_options));
    RETURN_NOT_ARROW_OK(writer->Write(*table));
    RETURN_NOT_ARROW_OK(writer->Close());
    break;
  }
#endif
  default:
    return Status::Invalid(
        "Unsupported file type: ", FileTypeToString(file_type), " for wrting.");
  }
  return Status::OK();
}
void buildCsvWriteOptionsWithWriterOptions(
    arrow::csv::WriteOptions& writeOptions,
    const std::shared_ptr<WriterOptions>& options) {
  if (!options) {
    return;
  }
  auto csvOption = options->csvOption;
  if (csvOption) {
    writeOptions.include_header = csvOption->include_header;
    writeOptions.batch_size = csvOption->batch_size;
    writeOptions.delimiter = csvOption->delimiter;
    writeOptions.null_string = csvOption->null_string;
    writeOptions.io_context = csvOption->io_context;
    writeOptions.eol = csvOption->eol;
    writeOptions.quoting_style = csvOption->quoting_style;
  }
}
void buildParquetWriteOptionsWithWriterOptions(
    parquet::WriterProperties::Builder& writer_porpertices_builder,
    parquet::ArrowWriterProperties::Builder& arrow_writer_porpertices_builder,
    const std::shared_ptr<WriterOptions>& options) {
  if (!options) {
    return;
  }
  auto parquetOption = options->parquetOption;
  if (!parquetOption) {
    return;
  }
  if (parquetOption->enable_dictionary) {
    writer_porpertices_builder.enable_dictionary();
  }
  writer_porpertices_builder.dictionary_pagesize_limit(
      parquetOption->dictionary_pagesize_limit);
  writer_porpertices_builder.write_batch_size(parquetOption->write_batch_size);
  writer_porpertices_builder.max_row_group_length(
      parquetOption->max_row_group_length);
  writer_porpertices_builder.data_pagesize(parquetOption->data_pagesize);
  writer_porpertices_builder.data_page_version(
      parquetOption->data_page_version);
  writer_porpertices_builder.version(parquetOption->version);
  writer_porpertices_builder.encoding(parquetOption->encoding);
  for (const auto& kv : parquetOption->column_encoding) {
    writer_porpertices_builder.encoding(kv.first, kv.second);
  }
  writer_porpertices_builder.compression(parquetOption->compression);
  for (const auto& kv : parquetOption->column_compression) {
    writer_porpertices_builder.compression(kv.first, kv.second);
  }
  writer_porpertices_builder.compression_level(
      parquetOption->compression_level);
  for (const auto& kv : parquetOption->column_compression_level) {
    writer_porpertices_builder.compression_level(kv.first, kv.second);
  }
  writer_porpertices_builder.max_statistics_size(
      parquetOption->max_statistics_size);
  if (parquetOption->encryption_properties) {
    writer_porpertices_builder.encryption(parquetOption->encryption_properties);
  }
  if (parquetOption->enable_statistics) {
    writer_porpertices_builder.enable_statistics();
  }
  for (const auto& path_st : parquetOption->column_statistics) {
    writer_porpertices_builder.enable_statistics(path_st.first);
  }
  if (!parquetOption->sorting_columns.empty()) {
    writer_porpertices_builder.set_sorting_columns(
        parquetOption->sorting_columns);
  }
  if (parquetOption->enable_store_decimal_as_integer) {
    writer_porpertices_builder.enable_store_decimal_as_integer();
  }
  if (parquetOption->enable_write_page_index) {
    writer_porpertices_builder.enable_write_page_index();
  }
  for (const auto& column_pi : parquetOption->column_write_page_index) {
    writer_porpertices_builder.enable_write_page_index(column_pi.first);
  }
  if (parquetOption->compliant_nested_types) {
    arrow_writer_porpertices_builder.enable_compliant_nested_types();
  }
  arrow_writer_porpertices_builder.set_use_threads(parquetOption->use_threads);
  if (parquetOption->enable_deprecated_int96_timestamps) {
    arrow_writer_porpertices_builder.enable_deprecated_int96_timestamps();
  }
  arrow_writer_porpertices_builder.coerce_timestamps(
      parquetOption->coerce_timestamps);
  if (parquetOption->allow_truncated_timestamps) {
    arrow_writer_porpertices_builder.allow_truncated_timestamps();
  }
  if (parquetOption->store_schema) {
    arrow_writer_porpertices_builder.store_schema();
  }
  if (parquetOption->executor) {
    arrow_writer_porpertices_builder.set_executor(parquetOption->executor);
  }
}

void buildOrcWriteOptionsWithWriterOptions(
    arrow::adapters::orc::WriteOptions& write_options,
    const std::shared_ptr<WriterOptions>& options) {
  if (!options) {
    return;
  }
  auto orcOption = options->orcOption;
  if (!orcOption) {
    return;
  }
  write_options.batch_size = orcOption->batch_size;
  write_options.compression = orcOption->compression;
  write_options.stripe_size = orcOption->stripe_size;
  write_options.file_version = orcOption->file_version;
  write_options.bloom_filter_columns = orcOption->bloom_filter_columns;
  write_options.bloom_filter_fpp = orcOption->bloom_filter_fpp;
}

Status FileSystem::WriteLabelTableToFile(
    const std::shared_ptr<arrow::Table>& table,
    const std::string& path) const noexcept {
  // try to create the directory, oss filesystem may not support this, ignore
  ARROW_UNUSED(arrow_fs_->CreateDir(path.substr(0, path.find_last_of("/"))));
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto output_stream,
                                       arrow_fs_->OpenOutputStream(path));
  auto schema = table->schema();
  auto column_num = schema->num_fields();
  parquet::WriterProperties::Builder builder;
  builder.compression(arrow::Compression::type::ZSTD);  // enable compression
  builder.encoding(parquet::Encoding::RLE);
  RETURN_NOT_ARROW_OK(parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), output_stream, 64 * 1024 * 1024,
      builder.build(), parquet::default_arrow_writer_properties()));
  return Status::OK();
}

Status FileSystem::CopyFile(const std::string& src_path,
                            const std::string& dst_path) const noexcept {
  // try to create the directory, oss filesystem may not support this, ignore
  ARROW_UNUSED(
      arrow_fs_->CreateDir(dst_path.substr(0, dst_path.find_last_of("/"))));
  RETURN_NOT_ARROW_OK(arrow_fs_->CopyFile(src_path, dst_path));
  return Status::OK();
}

Result<IdType> FileSystem::GetFileNumOfDir(const std::string& dir_path,
                                           bool recursive) const noexcept {
  arrow::fs::FileSelector file_selector;
  file_selector.base_dir = dir_path;
  file_selector.allow_not_found = false;  // if dir_path not exist, return error
  file_selector.recursive = recursive;
  arrow::fs::FileInfoVector file_infos;
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(file_infos,
                                       arrow_fs_->GetFileInfo(file_selector));
  return static_cast<IdType>(file_infos.size());
}

FileSystem::~FileSystem() {}

Result<std::shared_ptr<FileSystem>> FileSystemFromUriOrPath(
    const std::string& uri_string, std::string* out_path) {
  if (uri_string.length() >= 1 && uri_string[0] == '/') {
    // if the uri_string is an absolute path, we need to create a local file
    GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(
        auto arrow_fs,
        arrow::fs::FileSystemFromUriOrPath(uri_string, out_path));
    // arrow would delete the last slash, so use uri string
    if (out_path != nullptr) {
      *out_path = uri_string;
    }
    return std::make_shared<FileSystem>(arrow_fs);
  }

  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(
      auto arrow_fs, arrow::fs::FileSystemFromUriOrPath(uri_string));
  auto uri = uri::parse_uri(uri_string);
  if (uri.error != uri::Error::None) {
    return Status::Invalid("Failed to parse URI: ", uri_string);
  }
  if (out_path != nullptr) {
    if (uri.scheme == "file" || uri.scheme == "hdfs" || uri.scheme.empty()) {
      *out_path = uri.path;
    } else if (uri.scheme == "s3" || uri.scheme == "gs") {
      // bucket name is the host, path is the path
      *out_path = uri.authority.host + uri.path;
    } else {
      return Status::Invalid("Unrecognized filesystem type in URI: ",
                             uri_string);
    }
  }
  return std::make_shared<FileSystem>(arrow_fs);
}

// arrow::fs::InitializeS3 and arrow::fs::FinalizeS3 need arrow_version >= 15
Status InitializeS3() {
#if defined(ARROW_VERSION) && ARROW_VERSION > 14000000
  auto options = arrow::fs::S3GlobalOptions::Defaults();
#else
  arrow::fs::S3GlobalOptions options;
  options.log_level = arrow::fs::S3LogLevel::Fatal;
#endif
#if defined(ARROW_VERSION) && ARROW_VERSION >= 15000000
  RETURN_NOT_ARROW_OK(arrow::fs::InitializeS3(options));
#endif
  return Status::OK();
}

Status FinalizeS3() {
#if defined(ARROW_VERSION) && ARROW_VERSION >= 15000000
  RETURN_NOT_ARROW_OK(arrow::fs::FinalizeS3());
#endif
  return Status::OK();
}

/// template specialization for std::string
template Result<IdType> FileSystem::ReadFileToValue<IdType>(
    const std::string&) const noexcept;
/// template specialization for std::string
template Status FileSystem::WriteValueToFile<IdType>(
    const IdType&, const std::string&) const noexcept;
}  // namespace graphar
