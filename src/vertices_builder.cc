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

#include "gar/writer/vertices_builder.h"
#include "gar/utils/convert_to_arrow_type.h"

namespace GAR_NAMESPACE_INTERNAL {
namespace builder {

Status VerticesBuilder::appendToArray(
    const DataType& type, const std::string& property_name,
    std::shared_ptr<arrow::Array>& array) {  // NOLINT
  switch (type.id()) {
  case Type::BOOL:
    return tryToAppend<Type::BOOL>(property_name, array);
  case Type::INT32:
    return tryToAppend<Type::INT32>(property_name, array);
  case Type::INT64:
    return tryToAppend<Type::INT64>(property_name, array);
  case Type::FLOAT:
    return tryToAppend<Type::FLOAT>(property_name, array);
  case Type::DOUBLE:
    return tryToAppend<Type::DOUBLE>(property_name, array);
  case Type::STRING:
    return tryToAppend<Type::STRING>(property_name, array);
  default:
    return Status::TypeError();
  }
  return Status::TypeError();
}

template <Type type>
Status VerticesBuilder::tryToAppend(
    const std::string& property_name,
    std::shared_ptr<arrow::Array>& array) {  // NOLINT
  using CType = typename ConvertToArrowType<type>::CType;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  typename ConvertToArrowType<type>::BuilderType builder(pool);
  for (auto& v : vertices_) {
    if (v.Empty() || !v.ContainProperty(property_name)) {
      auto status = builder.AppendNull();
      if (!status.ok())
        return Status::ArrowError(status.ToString());
    } else {
      auto status =
          builder.Append(std::any_cast<CType>(v.GetProperty(property_name)));
      if (!status.ok())
        return Status::ArrowError(status.ToString());
    }
  }
  array = builder.Finish().ValueOrDie();
  return Status::OK();
}

Result<std::shared_ptr<arrow::Table>> VerticesBuilder::convertToTable() {
  auto property_groups = vertex_info_.GetPropertyGroups();
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  for (auto& property_group : property_groups) {
    for (auto& property : property_group.GetProperties()) {
      // add a column to schema
      schema_vector.push_back(
          arrow::field(property.name, DataType::DataTypeToArrowDataType(property.type)));
      // add a column to data
      std::shared_ptr<arrow::Array> array;
      appendToArray(property.type, property.name, array);
      arrays.push_back(array);
    }
  }
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  return arrow::Table::Make(schema, arrays);
}

}  // namespace builder
}  // namespace GAR_NAMESPACE_INTERNAL
