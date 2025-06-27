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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "graphar/result.h"
#include "graphar/status.h"
#include "graphar/types.h"
#include "graphar/util.h"

#include "graphar/reader_util.h"
#include "graphar/writer_util.h"

// forward declarations
namespace arrow {
class Buffer;
class Table;
}  // namespace arrow

namespace arrow::fs {
class FileSystem;
}  // namespace arrow::fs

namespace arrow::io {
class RandomAccessFile;
}  // namespace arrow::io

namespace arrow::dataset {
class FileFormat;
}  // namespace arrow::dataset

namespace graphar {

/**
 * This class wraps an arrow::fs::FileSystem and provides methods for
 * reading and writing arrow::Table objects from and to files, as well as
 * performing other file system operations such as copying and counting files.
 */
class FileSystem {
 public:
  /**
   * @brief Create a FileSystem instance.
   * @param arrow_fs The arrow::fs::FileSystem to wrap.
   */
  explicit FileSystem(std::shared_ptr<arrow::fs::FileSystem> arrow_fs)
      : arrow_fs_(arrow_fs) {}

  ~FileSystem();

  /**
   * @brief Read and filter a file as an arrow::Table.
   *
   * @param path The path of the file to read.
   * @param file_type The type of the file to read.
   * @param options Row filter and columns to be selected
   * @return A Result containing a std::shared_ptr to an arrow::Table if
   * successful, or an error Status if unsuccessful.
   */
  Result<std::shared_ptr<arrow::Table>> ReadFileToTable(
      const std::string& path, FileType file_type,
      const util::FilterOptions& options = {}) const noexcept;

  Result<std::shared_ptr<arrow::Table>> ReadFileToTable(
      const std::string& path, FileType file_type,
      const std::vector<int>& column_indices) const noexcept;

  /**
   * @brief Read a file and convert its bytes to a value of type T.
   *
   * @tparam T The type to convert the file bytes to.
   * @param path The path of the file to read.
   * @return A Result containing the value if successful, or an error Status if
   * unsuccessful.
   */
  template <typename T>
  Result<T> ReadFileToValue(const std::string& path) const noexcept;

  /**
   * @brief Write a value of type T to a file.
   *
   * @tparam T The type of the value to be written.
   * @param value The value to be written.
   * @param path The path of the file to be written
   * @return A Status indicating OK if successful, or an error if unsuccessful.
   */
  template <typename T>
  Status WriteValueToFile(const T& value, const std::string& path) const
      noexcept;

  /**
   * @brief Write a table to a file with a specific type.
   * @param input_table The table to write.
   * @param file_type The type of the output file.
   * @param path The path of the output file.
   * @param options Options for writing the table, such as compression.
   * @return A Status indicating OK if successful, or an error if unsuccessful.
   */
  Status WriteTableToFile(const std::shared_ptr<arrow::Table>& table,
                          FileType file_type, const std::string& path,
                          const std::shared_ptr<WriterOptions>& options) const
      noexcept;

  /**
   * @brief Write a label table to a file with parquet type.
   * @param input_table The label table to write.
   * @param path The path of the output file.
   * @return A Status indicating OK if successful, or an error if unsuccessful.
   */
  Status WriteLabelTableToFile(const std::shared_ptr<arrow::Table>& table,
                               const std::string& path) const noexcept;

  /**
   * Copy a file.
   *
   * If the destination exists and is a directory, an Status::ArrowError is
   * returned. Otherwise, it is replaced.
   */
  Status CopyFile(const std::string& src_path,
                  const std::string& dst_path) const noexcept;

  /**
   * Get the number of file of a directory.
   *
   * the file is not pure file, it can be a directory or other type of file.
   */
  Result<IdType> GetFileNumOfDir(const std::string& dir_path,
                                 bool recursive = false) const noexcept;

 private:
  std::shared_ptr<arrow::dataset::FileFormat> GetFileFormat(
      const FileType file_type) const;

 private:
  std::shared_ptr<arrow::fs::FileSystem> arrow_fs_;
};

/**
 * @brief Create a new FileSystem by URI
 *
 * wrapper of arrow::fs::FileSystemFromUriOrPath
 *
 * Recognized schemes are "file", "mock", "hdfs", "viewfs", "s3",
 * "gs" and "gcs".
 *
 * in addition also recognize non-URIs, and treat them as local filesystem
 * paths. Only absolute local filesystem paths are allowed.
 */
Result<std::shared_ptr<FileSystem>> FileSystemFromUriOrPath(
    const std::string& uri, std::string* out_path = nullptr);

/**
 * @brief Initialize the S3 APIs.
 *
 * It is required to call this function at least once before using S3
 * FileSystem. Once this function is called you MUST call FinalizeS3 before the
 * end of the application in order to avoid a segmentation fault at shutdown.
 *
 * This function calls arrow:fs::Initialize() internally.
 *
 */
Status InitializeS3();

/**
 * @brief Shutdown the S3 APIs.
 *
 * This function should be called before the program exits to ensure that
 * all S3 resources are properly released.
 *
 * This function calls arrow:fs::FinalizeS3() internally.
 *
 */
Status FinalizeS3();

}  // namespace graphar
