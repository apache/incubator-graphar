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

#include "arrow/api.h"

#include "graphar/convert_to_arrow_type.h"
#include "graphar/general_params.h"
#include "graphar/high-level/edges_builder.h"
#include "graphar/result.h"

namespace graphar::builder {

Status EdgesBuilder::Dump() {
  // construct the writer
  EdgeChunkWriter writer(edge_info_, prefix_, adj_list_type_, writer_options_,
                         validate_level_);
  // construct empty edge collections for vertex chunks without edges
  IdType num_vertex_chunks =
      (num_vertices_ + vertex_chunk_size_ - 1) / vertex_chunk_size_;
  for (IdType i = 0; i < num_vertex_chunks; i++)
    if (edges_.find(i) == edges_.end()) {
      std::vector<Edge> empty_chunk_edges;
      edges_[i] = empty_chunk_edges;
    }
  // dump the offsets
  if (adj_list_type_ == AdjListType::ordered_by_source ||
      adj_list_type_ == AdjListType::ordered_by_dest) {
    for (auto& chunk_edges : edges_) {
      IdType vertex_chunk_index = chunk_edges.first;
      // sort the edges
      if (adj_list_type_ == AdjListType::ordered_by_source)
        sort(chunk_edges.second.begin(), chunk_edges.second.end(), cmp_src);
      if (adj_list_type_ == AdjListType::ordered_by_dest)
        sort(chunk_edges.second.begin(), chunk_edges.second.end(), cmp_dst);
      // construct and write offset chunk
      GAR_ASSIGN_OR_RAISE(
          auto offset_table,
          getOffsetTable(vertex_chunk_index, chunk_edges.second));
      GAR_RETURN_NOT_OK(
          writer.WriteOffsetChunk(offset_table, vertex_chunk_index));
    }
  }
  // dump the vertex num
  GAR_RETURN_NOT_OK(writer.WriteVerticesNum(num_vertices_));
  // dump the edge nums
  IdType vertex_chunk_num =
      (num_vertices_ + vertex_chunk_size_ - 1) / vertex_chunk_size_;
  for (IdType vertex_chunk_index = 0; vertex_chunk_index < vertex_chunk_num;
       vertex_chunk_index++) {
    if (edges_.find(vertex_chunk_index) == edges_.end()) {
      GAR_RETURN_NOT_OK(writer.WriteEdgesNum(vertex_chunk_index, 0));
    } else {
      GAR_RETURN_NOT_OK(writer.WriteEdgesNum(
          vertex_chunk_index, edges_[vertex_chunk_index].size()));
    }
  }
  // dump the edges
  for (auto& chunk_edges : edges_) {
    IdType vertex_chunk_index = chunk_edges.first;
    // convert to table
    GAR_ASSIGN_OR_RAISE(auto input_table, convertToTable(chunk_edges.second));
    // write table
    GAR_RETURN_NOT_OK(writer.WriteTable(input_table, vertex_chunk_index, 0));
    chunk_edges.second.clear();
  }
  is_saved_ = true;
  return Status::OK();
}

Status EdgesBuilder::validate(const Edge& e,
                              ValidateLevel validate_level) const {
  // use the builder's validate level
  if (validate_level == ValidateLevel::default_validate)
    validate_level = validate_level_;
  // no validate
  if (validate_level == ValidateLevel::no_validate)
    return Status::OK();

  // weak validate
  // can not add new edges after dumping
  if (is_saved_) {
    return Status::Invalid(
        "The edge builder has been saved, can not add "
        "new edges any more");
  }
  // adj list type not exits in edge info
  if (!edge_info_->HasAdjacentListType(adj_list_type_)) {
    return Status::KeyError(
        "Adj list type ", AdjListTypeToString(adj_list_type_),
        " does not exist in the ", edge_info_->GetEdgeType(), " edge info.");
  }

  // strong validate
  if (validate_level == ValidateLevel::strong_validate) {
    for (auto& property : e.GetProperties()) {
      // check if the property is contained
      if (!edge_info_->HasProperty(property.first)) {
        return Status::KeyError("Property with name ", property.first,
                                " is not contained in the ",
                                edge_info_->GetEdgeType(), " edge info.");
      }
      // check if the property type is correct
      auto type = edge_info_->GetPropertyType(property.first).value();
      bool invalid_type = false;
      switch (type->id()) {
      case Type::BOOL:
        if (property.second.type() !=
            typeid(typename TypeToArrowType<Type::BOOL>::CType)) {
          invalid_type = true;
        }
        break;
      case Type::INT32:
        if (property.second.type() !=
            typeid(typename TypeToArrowType<Type::INT32>::CType)) {
          invalid_type = true;
        }
        break;
      case Type::INT64:
        if (property.second.type() !=
            typeid(typename TypeToArrowType<Type::INT64>::CType)) {
          invalid_type = true;
        }
        break;
      case Type::FLOAT:
        if (property.second.type() !=
            typeid(typename TypeToArrowType<Type::FLOAT>::CType)) {
          invalid_type = true;
        }
        break;
      case Type::DOUBLE:
        if (property.second.type() !=
            typeid(typename TypeToArrowType<Type::DOUBLE>::CType)) {
          invalid_type = true;
        }
        break;
      case Type::STRING:
        if (property.second.type() !=
            typeid(typename TypeToArrowType<Type::STRING>::CType)) {
          invalid_type = true;
        }
        break;
      case Type::DATE:
        // date is stored as int32_t
        if (property.second.type() !=
            typeid(typename TypeToArrowType<Type::DATE>::CType::c_type)) {
          invalid_type = true;
        }
        break;
      case Type::TIMESTAMP:
        // timestamp is stored as int64_t
        if (property.second.type() !=
            typeid(typename TypeToArrowType<Type::TIMESTAMP>::CType::c_type)) {
          invalid_type = true;
        }
        break;
      default:
        return Status::TypeError("Unsupported property type.");
      }
      if (invalid_type) {
        return Status::TypeError(
            "Invalid data type for property ", property.first + ", defined as ",
            type->ToTypeName(), ", but got ", property.second.type().name());
      }
    }
  }
  return Status::OK();
}

template <Type type>
Status EdgesBuilder::tryToAppend(
    const std::string& property_name,
    std::shared_ptr<arrow::Array>& array,  // NOLINT
    const std::vector<Edge>& edges) {
  using CType = typename TypeToArrowType<type>::CType;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  typename TypeToArrowType<type>::BuilderType builder(pool);
  for (const auto& e : edges) {
    if (e.Empty() || (!e.ContainProperty(property_name))) {
      RETURN_NOT_ARROW_OK(builder.AppendNull());
    } else {
      RETURN_NOT_ARROW_OK(
          builder.Append(std::any_cast<CType>(e.GetProperty(property_name))));
    }
  }
  array = builder.Finish().ValueOrDie();
  return Status::OK();
}

template <>
Status EdgesBuilder::tryToAppend<Type::TIMESTAMP>(
    const std::string& property_name,
    std::shared_ptr<arrow::Array>& array,  // NOLINT
    const std::vector<Edge>& edges) {
  using CType = typename TypeToArrowType<Type::TIMESTAMP>::CType::c_type;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  typename TypeToArrowType<Type::TIMESTAMP>::BuilderType builder(
      arrow::timestamp(arrow::TimeUnit::MILLI), pool);
  for (const auto& e : edges) {
    if (e.Empty() || (!e.ContainProperty(property_name))) {
      RETURN_NOT_ARROW_OK(builder.AppendNull());
    } else {
      RETURN_NOT_ARROW_OK(
          builder.Append(std::any_cast<CType>(e.GetProperty(property_name))));
    }
  }
  array = builder.Finish().ValueOrDie();
  return Status::OK();
}

template <>
Status EdgesBuilder::tryToAppend<Type::DATE>(
    const std::string& property_name,
    std::shared_ptr<arrow::Array>& array,  // NOLINT
    const std::vector<Edge>& edges) {
  using CType = typename TypeToArrowType<Type::DATE>::CType::c_type;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  typename TypeToArrowType<Type::DATE>::BuilderType builder(pool);
  for (const auto& e : edges) {
    if (e.Empty() || (!e.ContainProperty(property_name))) {
      RETURN_NOT_ARROW_OK(builder.AppendNull());
    } else {
      RETURN_NOT_ARROW_OK(
          builder.Append(std::any_cast<CType>(e.GetProperty(property_name))));
    }
  }
  array = builder.Finish().ValueOrDie();
  return Status::OK();
}

Status EdgesBuilder::appendToArray(
    const std::shared_ptr<DataType>& type, const std::string& property_name,
    std::shared_ptr<arrow::Array>& array,  // NOLINT
    const std::vector<Edge>& edges) {
  switch (type->id()) {
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
  case Type::DATE:
    return tryToAppend<Type::DATE>(property_name, array, edges);
  case Type::TIMESTAMP:
    return tryToAppend<Type::TIMESTAMP>(property_name, array, edges);
  default:
    return Status::TypeError("Unsupported property type.");
  }
  return Status::OK();
}

Status EdgesBuilder::tryToAppend(
    int src_or_dest,
    std::shared_ptr<arrow::Array>& array,  // NOLINT
    const std::vector<Edge>& edges) {
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  typename arrow::TypeTraits<arrow::Int64Type>::BuilderType builder(pool);
  for (const auto& e : edges) {
    RETURN_NOT_ARROW_OK(builder.Append(std::any_cast<int64_t>(
        src_or_dest == 1 ? e.GetSource() : e.GetDestination())));
  }
  array = builder.Finish().ValueOrDie();
  return Status::OK();
}

Result<std::shared_ptr<arrow::Table>> EdgesBuilder::convertToTable(
    const std::vector<Edge>& edges) {
  const auto& property_groups = edge_info_->GetPropertyGroups();
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  // add src
  std::shared_ptr<arrow::Array> array;
  schema_vector.push_back(arrow::field(
      GeneralParams::kSrcIndexCol, DataType::DataTypeToArrowDataType(int64())));
  GAR_RETURN_NOT_OK(tryToAppend(1, array, edges));
  arrays.push_back(array);
  // add dst
  schema_vector.push_back(arrow::field(
      GeneralParams::kDstIndexCol, DataType::DataTypeToArrowDataType(int64())));
  GAR_RETURN_NOT_OK(tryToAppend(0, array, edges));
  arrays.push_back(array);
  // add properties
  for (auto& property_group : property_groups) {
    for (auto& property : property_group->GetProperties()) {
      // add a column to schema
      schema_vector.push_back(arrow::field(
          property.name, DataType::DataTypeToArrowDataType(property.type)));
      // add a column to data
      std::shared_ptr<arrow::Array> array;
      GAR_RETURN_NOT_OK(
          appendToArray(property.type, property.name, array, edges));
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
  schema_vector.push_back(arrow::field(
      GeneralParams::kOffsetCol, DataType::DataTypeToArrowDataType(int64())));

  size_t index = 0;
  for (IdType i = begin_index; i < end_index; i++) {
    while (index < edges.size()) {
      int64_t x = (adj_list_type_ == AdjListType::ordered_by_source
                       ? edges[index].GetSource()
                       : edges[index].GetDestination());
      if (x <= i) {
        index++;
      } else {
        break;
      }
    }
    RETURN_NOT_ARROW_OK(builder.Append(index));
  }
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto array, builder.Finish());
  arrays.push_back(array);
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  return arrow::Table::Make(schema, arrays);
}

}  // namespace graphar::builder
