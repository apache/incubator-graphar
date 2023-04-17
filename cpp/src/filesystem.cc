/** Copyright 2022 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include "arrow/adapters/orc/adapter.h"
#include "arrow/api.h"
#include "arrow/csv/api.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/api.h"
#include "arrow/ipc/writer.h"
#include "arrow/util/uri.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

#include "gar/utils/filesystem.h"

namespace GAR_NAMESPACE_INTERNAL {

namespace detail {
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

Result<arrow::internal::Uri> ParseFileSystemUri(const std::string& uri_string) {
  arrow::internal::Uri uri;
  RETURN_NOT_ARROW_OK(uri.Parse(uri_string));
  return std::move(uri);
}
}  // namespace detail

Result<std::shared_ptr<arrow::Table>> FileSystem::ReadFileToTable(
    const std::string& path, FileType file_type) const noexcept {
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::shared_ptr<arrow::Table> table;
  switch (file_type) {
  case FileType::CSV: {
    GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto is,
                                         arrow_fs_->OpenInputStream(path));
    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();
    GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(
        auto reader, arrow::csv::TableReader::Make(
                         arrow::io::IOContext(pool), is, read_options,
                         parse_options, convert_options));
    GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(table, reader->Read());
    break;
  }
  case FileType::PARQUET: {
    GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto input,
                                         arrow_fs_->OpenInputFile(path));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    RETURN_NOT_ARROW_OK(parquet::arrow::OpenFile(input, pool, &reader));
    RETURN_NOT_ARROW_OK(reader->ReadTable(&table));
    break;
  }
  case FileType::ORC: {
    GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto input,
                                         arrow_fs_->OpenInputFile(path));
    GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(
        auto reader, arrow::adapters::orc::ORCFileReader::Open(input, pool));
    GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(table, reader->Read());
    break;
  }
  default:
    return Status::Invalid("File type is invalid.");
  }
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
    if (type->Equals(arrow::large_utf8())) {
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
Result<std::string> FileSystem::ReadFileToValue(const std::string& path) const
    noexcept {
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
  RETURN_NOT_ARROW_OK(
      arrow_fs_->CreateDir(path.substr(0, path.find_last_of("/"))));
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto ofstream,
                                       arrow_fs_->OpenOutputStream(path));
  RETURN_NOT_ARROW_OK(ofstream->Write(&value, sizeof(T)));
  RETURN_NOT_ARROW_OK(ofstream->Close());
  return Status::OK();
}

template <>
Status FileSystem::WriteValueToFile(const std::string& value,
                                    const std::string& path) const noexcept {
  RETURN_NOT_ARROW_OK(
      arrow_fs_->CreateDir(path.substr(0, path.find_last_of("/"))));
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto ofstream,
                                       arrow_fs_->OpenOutputStream(path));
  RETURN_NOT_ARROW_OK(ofstream->Write(value.c_str(), value.size()));
  RETURN_NOT_ARROW_OK(ofstream->Close());
  return Status::OK();
}

Status FileSystem::WriteTableToFile(const std::shared_ptr<arrow::Table>& table,
                                    FileType file_type,
                                    const std::string& path) const noexcept {
  RETURN_NOT_ARROW_OK(
      arrow_fs_->CreateDir(path.substr(0, path.find_last_of("/"))));
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto output_stream,
                                       arrow_fs_->OpenOutputStream(path));
  switch (file_type) {
  case FileType::CSV: {
    auto write_options = arrow::csv::WriteOptions::Defaults();
    write_options.include_header = true;
    write_options.quoting_style = arrow::csv::QuotingStyle::Needed;
    GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(
        auto writer, arrow::csv::MakeCSVWriter(output_stream.get(),
                                               table->schema(), write_options));
    RETURN_NOT_ARROW_OK(writer->WriteTable(*table));
    RETURN_NOT_ARROW_OK(writer->Close());
    break;
  }
  case FileType::PARQUET: {
    parquet::WriterProperties::Builder builder;
    builder.compression(arrow::Compression::type::ZSTD);  // enable compression
    RETURN_NOT_ARROW_OK(parquet::arrow::WriteTable(
        *table, arrow::default_memory_pool(), output_stream, 64 * 1024 * 1024,
        builder.build(), parquet::default_arrow_writer_properties()));
    break;
  }
  case FileType::ORC: {
    auto writer_options = arrow::adapters::orc::WriteOptions();
    writer_options.compression = arrow::Compression::type::ZSTD;
    GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(
        auto writer, arrow::adapters::orc::ORCFileWriter::Open(
                         output_stream.get(), writer_options));
    RETURN_NOT_ARROW_OK(writer->Write(*table));
    RETURN_NOT_ARROW_OK(writer->Close());
    break;
  }
  default:
    std::string message =
        "Invalid file type: " + std::string(FileTypeToString(file_type)) +
        " for writing.";
    return Status::Invalid(message);
  }
  return Status::OK();
}

Status FileSystem::CopyFile(const std::string& src_path,
                            const std::string& dst_path) const noexcept {
  RETURN_NOT_ARROW_OK(
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

Result<std::shared_ptr<FileSystem>> FileSystemFromUriOrPath(
    const std::string& uri_string, std::string* out_path) {
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(
      auto arrow_fs, arrow::fs::FileSystemFromUriOrPath(uri_string));

  GAR_ASSIGN_OR_RAISE(auto uri, detail::ParseFileSystemUri(uri_string));
  if (out_path != nullptr) {
    if (uri.scheme() == "file" || uri.scheme() == "hdfs" ||
        uri.scheme().empty()) {
      *out_path = uri.path();
    } else if (uri.scheme() == "s3" || uri.scheme() == "gs") {
      // bucket name is the host, path is the path
      // the arrow parser would delete the trailing slash which we don't want to
      *out_path = uri.host() + uri.path();
    } else {
      return Status::Invalid("Unrecognized filesystem type in URI: " +
                             uri_string);
    }
  }
  return std::make_shared<FileSystem>(arrow_fs);
}

/// template specialization for std::string
template Result<IdType> FileSystem::ReadFileToValue<IdType>(
    const std::string&) const noexcept;
/// template specialization for std::string
template Status FileSystem::WriteValueToFile<IdType>(const IdType&,
                                                     const std::string&) const
    noexcept;
}  // namespace GAR_NAMESPACE_INTERNAL
