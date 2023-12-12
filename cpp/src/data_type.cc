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

#include <memory>

#include "arrow/api.h"
#include "arrow/type.h"

#include "gar/util/data_type.h"

namespace GAR_NAMESPACE_INTERNAL {

std::shared_ptr<arrow::DataType> DataType::DataTypeToArrowDataType(
    DataType type) {
  switch (type.id()) {
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
  default:
    throw std::runtime_error("Unsupported data type");
  }
}

DataType DataType::ArrowDataTypeToDataType(
    std::shared_ptr<arrow::DataType> type) {
  switch (type->id()) {
  case arrow::Type::BOOL:
    return DataType(Type::BOOL);
  case arrow::Type::INT32:
    return DataType(Type::INT32);
  case arrow::Type::INT64:
    return DataType(Type::INT64);
  case arrow::Type::FLOAT:
    return DataType(Type::FLOAT);
  case arrow::Type::DOUBLE:
    return DataType(Type::DOUBLE);
  case arrow::Type::STRING:
    return DataType(Type::STRING);
  case arrow::Type::LARGE_STRING:
    return DataType(Type::STRING);
  default:
    throw std::runtime_error("Unsupported data type");
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

#undef TO_STRING_CASE
  case Type::USER_DEFINED:
    return user_defined_type_name_;
  default:
    return "unknown";
  }
}

}  // namespace GAR_NAMESPACE_INTERNAL
