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

#ifndef GAR_UTILS_FILESYSTEM_H_
#define GAR_UTILS_FILESYSTEM_H_

#include <memory>
#include <string>

#include "gar/utils/file_type.h"
#include "gar/utils/result.h"
#include "gar/utils/status.h"
#include "gar/utils/utils.h"

// forward declarations
namespace arrow {
class Buffer;
class Table;
namespace fs {
class FileSystem;
}
namespace io {
class RandomAccessFile;
}
}  // namespace arrow

namespace GAR_NAMESPACE_INTERNAL {

/// A wrapper of arrow::FileSystem to provide read/write arrow::Table
///    from/to file and other necessary file operations.
class FileSystem {
 public:
  /// \brief Create a FileSystem instance.
  explicit FileSystem(std::shared_ptr<arrow::fs::FileSystem> arrow_fs)
      : arrow_fs_(arrow_fs) {}

  ~FileSystem() = default;

  /// Read a file as an arrow::Table
  Result<std::shared_ptr<arrow::Table>> ReadFileToTable(
      const std::string& path, FileType file_type) const noexcept;

  /// Read a file to value
  ///   if the file bytes can not be converted to value, return
  ///   Status::ArrowError
  template <typename T>
  Result<T> ReadFileToValue(const std::string& path) const noexcept;

  /// Write a value to a file
  template <typename T>
  Status WriteValueToFile(const T& value, const std::string& path) const
      noexcept;

  /// \brief Write a table to a file with a specific type.
  ///
  /// \param input_table The table to write.
  /// \param file_type The type of the output file.
  /// \param path The path of the output file.
  Status WriteTableToFile(const std::shared_ptr<arrow::Table>& table,
                          FileType file_type, const std::string& path) const
      noexcept;

  /// Copy a file.
  ///
  /// If the destination exists and is a directory, an Status::ArrowError is
  /// returned. Otherwise, it is replaced.
  Status CopyFile(const std::string& src_path,
                  const std::string& dst_path) const noexcept;

  /// Get the number of file of a directory.
  ///
  /// the file is not pure file, it can be a directory or other type of file.
  Result<size_t> GetFileNumOfDir(const std::string& dir_path,
                                 bool recursive = false) const noexcept;

 private:
  std::shared_ptr<arrow::fs::FileSystem> arrow_fs_;
};

/// \brief Create a new FileSystem by URI
///
/// wrapper of arrow::fs::FileSystemFromUri
///
/// Recognized schemes are "file", "mock", "hdfs", "viewfs", "s3",
/// "gs" and "gcs".
///
/// in addition also recognize non-URIs, and treat them as local filesystem
/// paths. Only absolute local filesystem paths are allowed.
Result<std::shared_ptr<FileSystem>> FileSystemFromUriOrPath(
    const std::string& uri, std::string* out_path = nullptr);

}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_UTILS_FILESYSTEM_H_
