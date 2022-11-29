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

#ifndef GAR_UTILS_DATA_TYPE_H_
#define GAR_UTILS_DATA_TYPE_H_

#include <map>
#include <memory>
#include <string>

#include "gar/utils/macros.h"

// forward declaration
namespace arrow {
class DataType;
}

namespace GAR_NAMESPACE_INTERNAL {

/// \brief The DataType struct to provide enum type for data type and functions
///   to parse data type.
struct DataType {
  /// \brief Main data type enumeration
  enum type {
    /// Boolean as 1 bit, LSB bit-packed ordering
    BOOL = 0,

    /// Signed 32-bit little-endian integer
    INT32 = 1,

    /// Signed 64-bit little-endian integer
    INT64 = 2,

    /// 4-byte floating point value
    FLOAT = 3,

    /// 8-byte floating point value
    DOUBLE = 4,

    /// UTF8 variable-length string as List<Char>
    STRING = 5,

    // Leave this at the end
    MAX_ID = 6,
  };

  static std::shared_ptr<arrow::DataType> DataTypeToArrowDataType(
      DataType::type type_id);

  static DataType::type ArrowDataTypeToDataType(
      std::shared_ptr<arrow::DataType> type);

  static DataType::type StringToDataType(const std::string& str) {
    static const std::map<std::string, DataType::type> str2type{
        {"bool", DataType::type::BOOL},     {"int32", DataType::type::INT32},
        {"int64", DataType::type::INT64},   {"float", DataType::type::FLOAT},
        {"double", DataType::type::DOUBLE}, {"string", DataType::type::STRING}};
    try {
      return str2type.at(str.c_str());
    } catch (const std::exception& e) {
      throw std::runtime_error("KeyError: " + str);
    }
  }
  static const char* DataTypeToString(DataType::type type) {
    static const std::map<DataType::type, const char*> type2str{
        {DataType::type::BOOL, "bool"},     {DataType::type::INT32, "int32"},
        {DataType::type::INT64, "int64"},   {DataType::type::FLOAT, "float"},
        {DataType::type::DOUBLE, "double"}, {DataType::type::STRING, "string"}};
    return type2str.at(type);
  }
};  // struct Type
}  // namespace GAR_NAMESPACE_INTERNAL

#endif  // GAR_UTILS_DATA_TYPE_H_
