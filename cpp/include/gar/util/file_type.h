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

#include <map>
#include <stdexcept>
#include <string>

#include "gar/util/macros.h"

#ifndef GAR_UTIL_FILE_TYPE_H_
#define GAR_UTIL_FILE_TYPE_H_

namespace GAR_NAMESPACE_INTERNAL {

/** Type of file format */
enum FileType { CSV = 0, PARQUET = 1, ORC = 2 };

static inline FileType StringToFileType(const std::string& str) {
  static const std::map<std::string, FileType> str2file_type{
      {"csv", FileType::CSV},
      {"parquet", FileType::PARQUET},
      {"orc", FileType::ORC}};
  try {
    return str2file_type.at(str.c_str());
  } catch (const std::exception& e) {
    throw std::runtime_error("KeyError: " + str);
  }
}

static inline const char* FileTypeToString(FileType file_type) {
  static const std::map<FileType, const char*> file_type2string{
      {FileType::CSV, "csv"},
      {FileType::PARQUET, "parquet"},
      {FileType::ORC, "orc"}};
  return file_type2string.at(file_type);
}

}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_UTIL_FILE_TYPE_H_
