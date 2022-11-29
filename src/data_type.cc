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

#include <memory>

#include "arrow/api.h"
#include "arrow/type.h"

#include "gar/utils/data_type.h"

namespace GAR_NAMESPACE_INTERNAL {

std::shared_ptr<arrow::DataType> DataType::DataTypeToArrowDataType(
    DataType::type type_id) {
  switch (type_id) {
  case DataType::type::BOOL:
    return arrow::boolean();
  case DataType::type::INT32:
    return arrow::int32();
  case DataType::type::INT64:
    return arrow::int64();
  case DataType::type::FLOAT:
    return arrow::float32();
  case DataType::type::DOUBLE:
    return arrow::float64();
  case DataType::type::STRING:
    return arrow::utf8();
  default:
    throw std::runtime_error("Unsupported data type");
  }
}

DataType::type DataType::ArrowDataTypeToDataType(
    std::shared_ptr<arrow::DataType> type) {
  switch (type->id()) {
  case arrow::Type::BOOL:
    return DataType::type::BOOL;
  case arrow::Type::INT32:
    return DataType::type::INT32;
  case arrow::Type::INT64:
    return DataType::type::INT64;
  case arrow::Type::FLOAT:
    return DataType::type::FLOAT;
  case arrow::Type::DOUBLE:
    return DataType::type::DOUBLE;
  case arrow::Type::STRING:
    return DataType::type::STRING;
  default:
    throw std::runtime_error("Unsupported data type");
  }
}

}  // namespace GAR_NAMESPACE_INTERNAL
