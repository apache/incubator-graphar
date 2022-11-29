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
  return table;
}

template <typename T>
Result<T> FileSystem::ReadFileToValue(const std::string& path) const noexcept {
  T ret;
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto input,
                                       arrow_fs_->OpenInputStream(path));
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto bytes,
                                       input->Read(sizeof(T), &ret));
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
    write_options.include_header = false;
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

Result<size_t> FileSystem::GetFileNumOfDir(const std::string& dir_path,
                                           bool recursive) const noexcept {
  arrow::fs::FileSelector file_selector;
  file_selector.base_dir = dir_path;
  file_selector.allow_not_found = false;  // if dir_path not exist, return error
  file_selector.recursive = recursive;
  arrow::fs::FileInfoVector file_infos;
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(file_infos,
                                       arrow_fs_->GetFileInfo(file_selector));
  return file_infos.size();
}

Result<std::shared_ptr<FileSystem>> FileSystemFromUriOrPath(
    const std::string& uri, std::string* out_path) {
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(
      auto arrow_fs, arrow::fs::FileSystemFromUriOrPath(uri, out_path));
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
