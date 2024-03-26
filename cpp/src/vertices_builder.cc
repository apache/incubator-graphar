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

#include "gar/writer/vertices_builder.h"
#include "gar/graph_info.h"
#include "gar/util/convert_to_arrow_type.h"

namespace graphar {
namespace builder {

Status VerticesBuilder::validate(const Vertex& v, IdType index,
                                 ValidateLevel validate_level) const {
  // use the builder's validate level
  if (validate_level == ValidateLevel::default_validate)
    validate_level = validate_level_;
  // no validate
  if (validate_level == ValidateLevel::no_validate)
    return Status::OK();

  // weak validate
  // can not add new vertices after dumping
  if (is_saved_) {
    return Status::Invalid(
        "The vertices builder has been saved, can not add "
        "new vertices any more");
  }
  // the start vertex index must be aligned with the chunk size
  if (start_vertex_index_ % vertex_info_->GetChunkSize() != 0) {
    return Status::IndexError("The start vertex index ", start_vertex_index_,
                              " is not aligned with the chunk size ",
                              vertex_info_->GetChunkSize());
  }
  // the vertex index must larger than start index
  if (index != -1 && index < start_vertex_index_) {
    return Status::IndexError("The vertex index ", index,
                              " is smaller than the start index ",
                              start_vertex_index_);
  }

  // strong validate
  if (validate_level == ValidateLevel::strong_validate) {
    for (auto& property : v.GetProperties()) {
      // check if the property is contained
      if (!vertex_info_->HasProperty(property.first)) {
        return Status::KeyError("Property with name ", property.first,
                                " is not contained in the ",
                                vertex_info_->GetLabel(), " vertex info.");
      }
      // check if the property type is correct
      auto type = vertex_info_->GetPropertyType(property.first).value();
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
Status VerticesBuilder::tryToAppend(
    const std::string& property_name,
    std::shared_ptr<arrow::Array>& array) {  // NOLINT
  using CType = typename TypeToArrowType<type>::CType;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  typename TypeToArrowType<type>::BuilderType builder(pool);
  for (auto& v : vertices_) {
    if (v.Empty() || !v.ContainProperty(property_name)) {
      RETURN_NOT_ARROW_OK(builder.AppendNull());
    } else {
      RETURN_NOT_ARROW_OK(
          builder.Append(std::any_cast<CType>(v.GetProperty(property_name))));
    }
  }
  array = builder.Finish().ValueOrDie();
  return Status::OK();
}

template <>
Status VerticesBuilder::tryToAppend<Type::TIMESTAMP>(
    const std::string& property_name,
    std::shared_ptr<arrow::Array>& array) {  // NOLINT
  using CType = typename TypeToArrowType<Type::TIMESTAMP>::CType::c_type;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  typename TypeToArrowType<Type::TIMESTAMP>::BuilderType builder(
      arrow::timestamp(arrow::TimeUnit::MILLI), pool);
  for (auto& v : vertices_) {
    if (v.Empty() || !v.ContainProperty(property_name)) {
      RETURN_NOT_ARROW_OK(builder.AppendNull());
    } else {
      RETURN_NOT_ARROW_OK(
          builder.Append(std::any_cast<CType>(v.GetProperty(property_name))));
    }
  }
  array = builder.Finish().ValueOrDie();
  return Status::OK();
}

template <>
Status VerticesBuilder::tryToAppend<Type::DATE>(
    const std::string& property_name,
    std::shared_ptr<arrow::Array>& array) {  // NOLINT
  using CType = typename TypeToArrowType<Type::DATE>::CType::c_type;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  typename TypeToArrowType<Type::DATE>::BuilderType builder(pool);
  for (auto& v : vertices_) {
    if (v.Empty() || !v.ContainProperty(property_name)) {
      RETURN_NOT_ARROW_OK(builder.AppendNull());
    } else {
      RETURN_NOT_ARROW_OK(
          builder.Append(std::any_cast<CType>(v.GetProperty(property_name))));
    }
  }
  array = builder.Finish().ValueOrDie();
  return Status::OK();
}

Status VerticesBuilder::appendToArray(
    const std::shared_ptr<DataType>& type, const std::string& property_name,
    std::shared_ptr<arrow::Array>& array) {  // NOLINT
  switch (type->id()) {
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
  case Type::DATE:
    return tryToAppend<Type::DATE>(property_name, array);
  case Type::TIMESTAMP:
    return tryToAppend<Type::TIMESTAMP>(property_name, array);
  default:
    return Status::TypeError("Unsupported property type.");
  }
  return Status::OK();
}

Result<std::shared_ptr<arrow::Table>> VerticesBuilder::convertToTable() {
  const auto& property_groups = vertex_info_->GetPropertyGroups();
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  for (auto& property_group : property_groups) {
    for (auto& property : property_group->GetProperties()) {
      // add a column to schema
      schema_vector.push_back(arrow::field(
          property.name, DataType::DataTypeToArrowDataType(property.type)));
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
}  // namespace graphar
