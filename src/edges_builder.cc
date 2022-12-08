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

#include "gar/writer/edges_builder.h"
#include "gar/utils/convert_to_arrow_type.h"
#include "gar/utils/general_params.h"

namespace GAR_NAMESPACE_INTERNAL {
namespace builder {

Status EdgesBuilder::appendToArray(
    const DataType& type, const std::string& property_name,
    std::shared_ptr<arrow::Array>& array,  // NOLINT
    const std::vector<Edge>& edges) {
  switch (type.id()) {
  case Type::BOOL:
    return tryToAppend<Type::BOOL>(property_name, array, edges);
  case Type::INT32:
    return tryToAppend<Type::INT32>(property_name, array, edges);
  case Type::INT64:
    return tryToAppend<Type::INT64>(property_name, array, edges);
  case Type::FLOAT:
    return tryToAppend<Type::FLOAT>(property_name, array, edges);
  case Type::DOUBLE:
    return tryToAppend<Type::DOUBLE>(property_name, array, edges);
  case Type::STRING:
    return tryToAppend<Type::STRING>(property_name, array, edges);
  default:
    return Status::TypeError();
  }
  return Status::TypeError();
}

template <Type type>
Status EdgesBuilder::tryToAppend(
    const std::string& property_name,
    std::shared_ptr<arrow::Array>& array,  // NOLINT
    const std::vector<Edge>& edges) {
  using CType = typename ConvertToArrowType<type>::CType;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  typename ConvertToArrowType<type>::BuilderType builder(pool);
  for (const auto& e : edges) {
    if (e.Empty() || (!e.ContainProperty(property_name))) {
      auto status = builder.AppendNull();
      if (!status.ok())
        return Status::ArrowError(status.ToString());
    } else {
      auto status =
          builder.Append(std::any_cast<CType>(e.GetProperty(property_name)));
      if (!status.ok())
        return Status::ArrowError(status.ToString());
    }
  }
  array = builder.Finish().ValueOrDie();
  return Status::OK();
}

Status EdgesBuilder::tryToAppend(
    int src_or_dest,
    std::shared_ptr<arrow::Array>& array,  // NOLINT
    const std::vector<Edge>& edges) {
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  typename arrow::TypeTraits<arrow::Int64Type>::BuilderType builder(pool);
  for (const auto& e : edges) {
    auto status = builder.Append(std::any_cast<int64_t>(
        src_or_dest == 1 ? e.GetSource() : e.GetDestination()));
    if (!status.ok())
      return Status::ArrowError(status.ToString());
  }
  array = builder.Finish().ValueOrDie();
  return Status::OK();
}

Result<std::shared_ptr<arrow::Table>> EdgesBuilder::convertToTable(
    const std::vector<Edge>& edges) {
  GAR_ASSIGN_OR_RAISE(auto& property_groups,
                      edge_info_.GetPropertyGroups(adj_list_type_));
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  // add src
  std::shared_ptr<arrow::Array> array;
  schema_vector.push_back(
      arrow::field(GeneralParams::kSrcIndexCol,
                   DataType::DataTypeToArrowDataType(DataType(Type::INT64))));
  GAR_RETURN_NOT_OK(tryToAppend(1, array, edges));
  arrays.push_back(array);
  // add dst
  schema_vector.push_back(
      arrow::field(GeneralParams::kDstIndexCol,
                   DataType::DataTypeToArrowDataType(DataType(Type::INT64))));
  GAR_RETURN_NOT_OK(tryToAppend(0, array, edges));
  arrays.push_back(array);
  // add properties
  for (auto& property_group : property_groups) {
    for (auto& property : property_group.GetProperties()) {
      // add a column to schema
      schema_vector.push_back(
          arrow::field(property.name, DataType::DataTypeToArrowDataType(property.type)));
      // add a column to data
      std::shared_ptr<arrow::Array> array;
      GAR_RETURN_NOT_OK(appendToArray(property.type, property.name, array, edges));
      arrays.push_back(array);
    }
  }
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  return arrow::Table::Make(schema, arrays);
}

Result<std::shared_ptr<arrow::Table>> EdgesBuilder::getOffsetTable(
    IdType vertex_chunk_index, const std::vector<Edge>& edges) {
  arrow::Int64Builder builder;
  IdType begin_index = vertex_chunk_index * vertex_chunk_size_,
         end_index = begin_index + vertex_chunk_size_;
  RETURN_NOT_ARROW_OK(builder.Append(0));

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  schema_vector.push_back(
      arrow::field(GeneralParams::kOffsetCol,
                   DataType::DataTypeToArrowDataType(DataType(Type::INT64))));

  size_t index = 0;
  for (IdType i = begin_index; i < end_index; i++) {
    while (index < edges.size()) {
      int64_t x = (adj_list_type_ == AdjListType::ordered_by_source
                       ? edges[index].GetSource()
                       : edges[index].GetDestination());
      if (x <= i)
        index++;
      else
        break;
    }
    RETURN_NOT_ARROW_OK(builder.Append(index));
  }
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto array, builder.Finish());
  arrays.push_back(array);
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  return arrow::Table::Make(schema, arrays);
}

}  // namespace builder
}  // namespace GAR_NAMESPACE_INTERNAL
