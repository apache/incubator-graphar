/*
 * Copyright 2022-2023 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef GAR_UTIL_DATA_TYPE_H_
#define GAR_UTIL_DATA_TYPE_H_

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "gar/util/macros.h"

// forward declaration
namespace arrow {
class DataType;
}

namespace GAR_NAMESPACE_INTERNAL {

/** @brief Main data type enumeration. */
enum class Type {
  /** Boolean */
  BOOL = 0,

  /** Signed 32-bit integer */
  INT32,

  /** Signed 64-bit integer */
  INT64,

  /** 4-byte floating point value */
  FLOAT,

  /** 8-byte floating point value */
  DOUBLE,

  /** UTF8 variable-length string */
  STRING,

  /** List of some logical data type */
  LIST,

  /** Date */
  Date,

  /** Time */
  TimeStamp,

  /** User-defined data type */
  USER_DEFINED,

  // Leave this at the end
  MAX_ID,
};

/**
 * @brief The DataType struct to provide enum type for data type and functions
 *   to parse data type.
 */
class DataType {
 public:
  DataType() : id_(Type::BOOL) {}

  explicit DataType(Type id, const std::string& user_defined_type_name = "")
      : id_(id),
        child_(nullptr),
        user_defined_type_name_(user_defined_type_name) {}

  explicit DataType(Type id, const std::shared_ptr<DataType>& child)
      : id_(id), child_(std::move(child)), user_defined_type_name_("") {}

  DataType(const DataType& other)
      : id_(other.id_),
        child_(other.child_),
        user_defined_type_name_(other.user_defined_type_name_) {}

  explicit DataType(DataType&& other)
      : id_(other.id_),
        child_(std::move(other.child_)),
        user_defined_type_name_(std::move(other.user_defined_type_name_)) {}

  inline DataType& operator=(const DataType& other) = default;

  bool Equals(const DataType& other) const {
    return id_ == other.id_ &&
           user_defined_type_name_ == other.user_defined_type_name_;
  }

  bool Equals(const std::shared_ptr<DataType>& other) const {
    if (!other) {
      return false;
    }
    return Equals(*other.get());
  }

  const std::shared_ptr<DataType>& value_type() const { return child_; }

  bool operator==(const DataType& other) const { return Equals(other); }

  bool operator!=(const DataType& other) const { return !Equals(other); }

  static std::shared_ptr<arrow::DataType> DataTypeToArrowDataType(
      const std::shared_ptr<DataType>& type);

  static std::shared_ptr<DataType> ArrowDataTypeToDataType(
      const std::shared_ptr<arrow::DataType>& type);

  static std::shared_ptr<DataType> TypeNameToDataType(const std::string& str);

  /** Return the type category of the DataType. */
  Type id() const { return id_; }

  std::string ToTypeName() const;

 private:
  Type id_;
  std::shared_ptr<DataType> child_;
  std::string user_defined_type_name_;
};  // struct DataType
}  // namespace GAR_NAMESPACE_INTERNAL

#endif  // GAR_UTIL_DATA_TYPE_H_
