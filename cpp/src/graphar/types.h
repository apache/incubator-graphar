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

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "graphar/fwd.h"
#include "graphar/macros.h"

// forward declaration
namespace arrow {
class DataType;
}

namespace graphar {

/** @brief Main data type enumeration. */
enum class Type : int32_t {
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

  /** int32_t days since the UNIX epoch */
  DATE,

  /** Exact timestamp encoded with int64 since UNIX epoch in milliseconds */
  TIMESTAMP,

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
    if (id_ != other.id_ ||
        user_defined_type_name_ != other.user_defined_type_name_) {
      return false;
    }
    if (child_ == nullptr && other.child_ == nullptr) {
      return true;
    }
    if (child_ != nullptr && other.child_ != nullptr) {
      return child_->Equals(other.child_);
    }
    return false;
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

// Define a Timestamp class to represent timestamp data type value
class Timestamp {
 public:
  using c_type = int64_t;
  explicit Timestamp(c_type value) : value_(value) {}

  c_type value() const { return value_; }

 private:
  c_type value_;
};

// Define a Date class to represent date data type value
class Date {
 public:
  using c_type = int32_t;
  explicit Date(c_type value) : value_(value) {}

  c_type value() const { return value_; }

 private:
  c_type value_;
};

/** Adj list type enumeration for adjacency list of graph. */
enum class AdjListType : std::int32_t {
  /// collection of edges by source, but unordered, can represent COO format
  unordered_by_source = 0b00000001,
  /// collection of edges by destination, but unordered, can represent COO
  /// format
  unordered_by_dest = 0b00000010,
  /// collection of edges by source, ordered by source, can represent CSR format
  ordered_by_source = 0b00000100,
  /// collection of edges by destination, ordered by destination, can represent
  /// CSC format
  ordered_by_dest = 0b00001000,
};

constexpr AdjListType operator|(AdjListType lhs, AdjListType rhs) {
  return static_cast<AdjListType>(
      static_cast<std::underlying_type_t<AdjListType>>(lhs) |
      static_cast<std::underlying_type_t<AdjListType>>(rhs));
}

constexpr AdjListType operator&(AdjListType lhs, AdjListType rhs) {
  return static_cast<AdjListType>(
      static_cast<std::underlying_type_t<AdjListType>>(lhs) &
      static_cast<std::underlying_type_t<AdjListType>>(rhs));
}

static inline const char* AdjListTypeToString(AdjListType adj_list_type) {
  static const std::map<AdjListType, const char*> adj_list2string{
      {AdjListType::unordered_by_source, "unordered_by_source"},
      {AdjListType::unordered_by_dest, "unordered_by_dest"},
      {AdjListType::ordered_by_source, "ordered_by_source"},
      {AdjListType::ordered_by_dest, "ordered_by_dest"}};
  return adj_list2string.at(adj_list_type);
}

static inline AdjListType OrderedAlignedToAdjListType(
    bool ordered, const std::string& aligned) {
  if (ordered) {
    return aligned == "src" ? AdjListType::ordered_by_source
                            : AdjListType::ordered_by_dest;
  }
  return aligned == "src" ? AdjListType::unordered_by_source
                          : AdjListType::unordered_by_dest;
}

static inline std::pair<bool, std::string> AdjListTypeToOrderedAligned(
    AdjListType adj_list_type) {
  switch (adj_list_type) {
  case AdjListType::unordered_by_source:
    return std::make_pair(false, "src");
  case AdjListType::unordered_by_dest:
    return std::make_pair(false, "dst");
  case AdjListType::ordered_by_source:
    return std::make_pair(true, "src");
  case AdjListType::ordered_by_dest:
    return std::make_pair(true, "dst");
  default:
    return std::make_pair(false, "dst");
  }
}

static inline FileType StringToFileType(const std::string& str) {
  static const std::map<std::string, FileType> str2file_type{
      {"csv", FileType::CSV},
      {"json", FileType::JSON},
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
      {FileType::JSON, "json"},
      {FileType::PARQUET, "parquet"},
      {FileType::ORC, "orc"}};
  return file_type2string.at(file_type);
}

static inline Cardinality StringToCardinality(const std::string& str) {
  static const std::map<std::string, Cardinality> str2cardinality{
      {"single", Cardinality::SINGLE},
      {"list", Cardinality::LIST},
      {"set", Cardinality::SET},
  };
  try {
    return str2cardinality.at(str.c_str());
  } catch (const std::exception& e) {
    throw std::runtime_error("KeyError: " + str);
  }
}

static inline const char* CardinalityToString(Cardinality cardinality) {
  static const std::map<Cardinality, const char*> cardinality2string{
      {Cardinality::SINGLE, "single"},
      {Cardinality::LIST, "list"},
      {Cardinality::SET, "set"},
  };
  try {
    return cardinality2string.at(cardinality);
  } catch (const std::exception& e) {
    throw std::runtime_error("KeyError: " +
                             std::to_string(static_cast<int>(cardinality)));
  }
}

// Helper function to split a string by a delimiter
inline std::vector<std::string> SplitString(const std::string& str,
                                            char delimiter) {
  std::vector<std::string> tokens;
  std::string token;
  std::istringstream tokenStream(str);
  while (std::getline(tokenStream, token, delimiter)) {
    tokens.push_back(token);
  }
  return tokens;
}
}  // namespace graphar
