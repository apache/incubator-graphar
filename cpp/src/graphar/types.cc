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

#include <algorithm>
#include <memory>

#include "arrow/api.h"
#include "arrow/type.h"

#include "graphar/fwd.h"
#include "graphar/types.h"

namespace graphar {

std::shared_ptr<arrow::DataType> DataType::DataTypeToArrowDataType(
    const std::shared_ptr<DataType>& type) {
  switch (type->id()) {
  case Type::BOOL:
    return arrow::boolean();
  case Type::INT32:
    return arrow::int32();
  case Type::INT64:
    return arrow::int64();
  case Type::FLOAT:
    return arrow::float32();
  case Type::DOUBLE:
    return arrow::float64();
  case Type::STRING:
    return arrow::large_utf8();
  case Type::DATE:
    return arrow::date32();
  case Type::TIMESTAMP:
    return arrow::timestamp(arrow::TimeUnit::MILLI);
  case Type::LIST:
    return arrow::list(DataTypeToArrowDataType(type->child_));
  default:
    std::string msg = "The data type " + type->ToTypeName() +
                      " is not supported yet in GraphAr";
    throw std::runtime_error(msg);
  }
}

std::shared_ptr<DataType> DataType::ArrowDataTypeToDataType(
    const std::shared_ptr<arrow::DataType>& type) {
  switch (type->id()) {
  case arrow::Type::BOOL:
    return boolean();
  case arrow::Type::INT32:
    return int32();
  case arrow::Type::INT64:
    return int64();
  case arrow::Type::FLOAT:
    return float32();
  case arrow::Type::DOUBLE:
    return float64();
  case arrow::Type::STRING:
    return string();
  case arrow::Type::LARGE_STRING:
    return string();
  case arrow::Type::DATE32:
    return date();
  case arrow::Type::TIMESTAMP:
  case arrow::Type::DATE64:  // Date64 of Arrow is used to represent timestamp
                             // milliseconds
    return timestamp();
  case arrow::Type::LIST:
    return list(ArrowDataTypeToDataType(type->field(0)->type()));
  default:
    std::string msg = "The arrow data type " + type->name() +
                      " is not supported yet in GraphAr";
    throw std::runtime_error(msg);
  }
}

std::string DataType::ToTypeName() const {
  switch (id_) {
#define TO_STRING_CASE(_id)                                            \
  case Type::_id: {                                                    \
    std::string name(GAR_STRINGIFY(_id));                              \
    std::transform(name.begin(), name.end(), name.begin(), ::tolower); \
    return name;                                                       \
  }

    TO_STRING_CASE(BOOL)
    TO_STRING_CASE(INT32)
    TO_STRING_CASE(INT64)
    TO_STRING_CASE(FLOAT)
    TO_STRING_CASE(DOUBLE)
    TO_STRING_CASE(STRING)
    TO_STRING_CASE(DATE)
    TO_STRING_CASE(TIMESTAMP)

#undef TO_STRING_CASE
  case Type::USER_DEFINED:
    return user_defined_type_name_;
  case Type::LIST:
    return "list<" + child_->ToTypeName() + ">";
  default:
    return "unknown";
  }
}

std::shared_ptr<DataType> DataType::TypeNameToDataType(const std::string& str) {
  if (str == "bool") {
    return boolean();
  } else if (str == "int32") {
    return int32();
  } else if (str == "int64") {
    return int64();
  } else if (str == "float") {
    return float32();
  } else if (str == "double") {
    return float64();
  } else if (str == "string") {
    return string();
  } else if (str == "date") {
    return date();
  } else if (str == "timestamp") {
    return timestamp();
  } else if (str == "list<int32>") {
    return list(int32());
  } else if (str == "list<int64>") {
    return list(int64());
  } else if (str == "list<float>") {
    return list(float32());
  } else if (str == "list<double>") {
    return list(float64());
  } else if (str == "list<string>") {
    return list(string());
  } else {
    throw std::runtime_error("Unsupported data type " + str);
  }
}

#define TYPE_FACTORY(NAME, TYPE)              \
  const std::shared_ptr<DataType>& NAME() {   \
    static std::shared_ptr<DataType> result = \
        std::make_shared<DataType>(TYPE);     \
    return result;                            \
  }

TYPE_FACTORY(boolean, Type::BOOL)
TYPE_FACTORY(int32, Type::INT32)
TYPE_FACTORY(int64, Type::INT64)
TYPE_FACTORY(float32, Type::FLOAT)
TYPE_FACTORY(float64, Type::DOUBLE)
TYPE_FACTORY(string, Type::STRING)
TYPE_FACTORY(date, Type::DATE)
TYPE_FACTORY(timestamp, Type::TIMESTAMP)

std::shared_ptr<DataType> list(const std::shared_ptr<DataType>& value_type) {
  return std::make_shared<DataType>(Type::LIST, value_type);
}
}  // namespace graphar
