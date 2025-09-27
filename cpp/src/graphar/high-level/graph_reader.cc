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

#include "graphar/high-level/graph_reader.h"
#include <algorithm>
#include <unordered_set>
#include "arrow/array.h"
#include "graphar/api/arrow_reader.h"
#include "graphar/convert_to_arrow_type.h"
#include "graphar/label.h"
#include "graphar/types.h"

namespace graphar {

template <Type type>
Status CastToAny(std::shared_ptr<arrow::Array> array,
                 std::any& any) {  // NOLINT
  if (array->IsNull(0)) {
    any = std::any();
    return Status::OK();
  }
  using ArrayType = typename TypeToArrowType<type>::ArrayType;
  auto column = std::dynamic_pointer_cast<ArrayType>(array);
  any = column->GetView(0);
  return Status::OK();
}

template <>
Status CastToAny<Type::STRING>(std::shared_ptr<arrow::Array> array,
                               std::any& any) {  // NOLINT
  using ArrayType = typename TypeToArrowType<Type::STRING>::ArrayType;
  auto column = std::dynamic_pointer_cast<ArrayType>(array);
  any = column->GetString(0);
  return Status::OK();
}

Status TryToCastToAny(const std::shared_ptr<DataType>& type,
                      std::shared_ptr<arrow::Array> array,
                      std::any& any) {  // NOLINT
  switch (type->id()) {
  case Type::BOOL:
    return CastToAny<Type::BOOL>(array, any);
  case Type::INT32:
    return CastToAny<Type::INT32>(array, any);
  case Type::INT64:
    return CastToAny<Type::INT64>(array, any);
  case Type::FLOAT:
    return CastToAny<Type::FLOAT>(array, any);
  case Type::DOUBLE:
    return CastToAny<Type::DOUBLE>(array, any);
  case Type::STRING:
    return CastToAny<Type::STRING>(array, any);
  case Type::DATE:
    return CastToAny<Type::DATE>(array, any);
  case Type::TIMESTAMP:
    return CastToAny<Type::TIMESTAMP>(array, any);
  default:
    return Status::TypeError("Unsupported type.");
  }
  return Status::OK();
}

Vertex::Vertex(IdType id,
               std::vector<VertexPropertyArrowChunkReader>& readers)  // NOLINT
    : id_(id) {
  // get the first row of table
  for (auto& reader : readers) {
    GAR_ASSIGN_OR_RAISE_ERROR(auto chunk_table,
                              reader.GetChunk(graphar::GetChunkVersion::V1));
    auto schema = chunk_table->schema();
    for (int i = 0; i < schema->num_fields(); ++i) {
      auto field = chunk_table->field(i);
      if (field->type()->id() == arrow::Type::LIST) {
        auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(
            chunk_table->column(i)->chunk(0));
        list_properties_[field->name()] = list_array->value_slice(0);
      } else {
        auto type = DataType::ArrowDataTypeToDataType(field->type());
        GAR_RAISE_ERROR_NOT_OK(TryToCastToAny(type,
                                              chunk_table->column(i)->chunk(0),
                                              properties_[field->name()]));
      }
    }
  }
}

Result<bool> VertexIter::hasLabel(const std::string& label) noexcept {
  std::shared_ptr<arrow::ChunkedArray> column(nullptr);
  label_reader_.seek(cur_offset_);
  GAR_ASSIGN_OR_RAISE(auto chunk_table, label_reader_.GetLabelChunk());
  column = util::GetArrowColumnByName(chunk_table, label);
  if (column != nullptr) {
    auto array = util::GetArrowArrayByChunkIndex(column, 0);
    auto bool_array = std::dynamic_pointer_cast<arrow::BooleanArray>(array);
    return bool_array->Value(0);
  }
  return Status::KeyError("label with name ", label,
                          " does not exist in the vertex.");
}

Result<std::vector<std::string>> VertexIter::label() noexcept {
  std::shared_ptr<arrow::ChunkedArray> column(nullptr);
  std::vector<std::string> vertex_label;
  if (is_filtered_)
    label_reader_.seek(filtered_ids_[cur_offset_]);
  else
    label_reader_.seek(cur_offset_);
  GAR_ASSIGN_OR_RAISE(auto chunk_table, label_reader_.GetLabelChunk());
  for (auto label : labels_) {
    column = util::GetArrowColumnByName(chunk_table, label);
    if (column != nullptr) {
      auto array = util::GetArrowArrayByChunkIndex(column, 0);
      auto bool_array = std::dynamic_pointer_cast<arrow::BooleanArray>(array);
      if (bool_array->Value(0)) {
        vertex_label.push_back(label);
      }
    }
  }
  return vertex_label;
}

static inline bool IsValid(bool* state, int column_number) {
  for (int i = 0; i < column_number; ++i) {
    // AND case
    if (!state[i])
      return false;
    // OR case
    // if (state[i]) return true;
  }
  // AND case
  return true;
  // OR case
  // return false;
}

Result<std::vector<IdType>> VerticesCollection::filter(
    const std::vector<std::string>& filter_labels,
    std::vector<IdType>* new_valid_chunk) {
  std::vector<int> indices;
  const int TOT_ROWS_NUM = vertex_num_;
  const int CHUNK_SIZE = vertex_info_->GetChunkSize();
  const int TOT_LABEL_NUM = labels_.size();
  const int TESTED_LABEL_NUM = filter_labels.size();
  std::vector<int> tested_label_ids;

  for (const auto& filter_label : filter_labels) {
    auto it = std::find(labels_.begin(), labels_.end(), filter_label);
    if (it != labels_.end()) {
      tested_label_ids.push_back(std::distance(labels_.begin(), it));
    }
  }
  if (tested_label_ids.empty())
    return Status::KeyError(
        "query label"
        " does not exist in the vertex.");

  uint64_t* bitmap = new uint64_t[TOT_ROWS_NUM / 64 + 1];
  memset(bitmap, 0, sizeof(uint64_t) * (TOT_ROWS_NUM / 64 + 1));
  int total_count = 0;
  int row_num;

  if (is_filtered_) {
    for (int chunk_idx : valid_chunk_) {
      row_num = std::min(CHUNK_SIZE, TOT_ROWS_NUM - chunk_idx * CHUNK_SIZE);
      std::string new_filename =
          prefix_ + vertex_info_->GetPrefix() + "labels/chunk";
      int count = read_parquet_file_and_get_valid_indices(
          new_filename.c_str(), row_num, TOT_LABEL_NUM, TESTED_LABEL_NUM,
          tested_label_ids, IsValid, chunk_idx, CHUNK_SIZE, &indices, bitmap,
          QUERY_TYPE::INDEX);
      if (count != 0 && new_valid_chunk != nullptr)
        new_valid_chunk->emplace_back(static_cast<IdType>(chunk_idx));
    }
  } else {
    for (int chunk_idx = 0; chunk_idx * CHUNK_SIZE < TOT_ROWS_NUM;
         ++chunk_idx) {
      row_num = std::min(CHUNK_SIZE, TOT_ROWS_NUM - chunk_idx * CHUNK_SIZE);
      std::string new_filename =
          prefix_ + vertex_info_->GetPrefix() + "labels/chunk";
      int count = read_parquet_file_and_get_valid_indices(
          new_filename.c_str(), row_num, TOT_LABEL_NUM, TESTED_LABEL_NUM,
          tested_label_ids, IsValid, chunk_idx, CHUNK_SIZE, &indices, bitmap,
          QUERY_TYPE::INDEX);
      if (count != 0)
        valid_chunk_.emplace_back(static_cast<IdType>(chunk_idx));
    }
  }
  // std::cout << "Total valid count: " << total_count << std::endl;
  std::vector<int64_t> indices64;

  for (int value : indices) {
    indices64.push_back(static_cast<int64_t>(value));
  }

  delete[] bitmap;

  return indices64;
}

Result<std::vector<IdType>> VerticesCollection::filter_by_acero(
    const std::vector<std::string>& filter_labels) const {
  std::vector<int> indices;
  const int TOT_ROWS_NUM = vertex_num_;
  const int CHUNK_SIZE = vertex_info_->GetChunkSize();

  std::vector<int> tested_label_ids;
  for (const auto& filter_label : filter_labels) {
    auto it = std::find(labels_.begin(), labels_.end(), filter_label);
    if (it != labels_.end()) {
      tested_label_ids.push_back(std::distance(labels_.begin(), it));
    }
  }
  int total_count = 0;
  int row_num;
  std::vector<std::shared_ptr<Expression>> filters;
  std::shared_ptr<Expression> combined_filter = nullptr;

  for (const auto& label : filter_labels) {
    filters.emplace_back(
        graphar::_Equal(graphar::_Property(label), graphar::_Literal(true)));
  }

  for (const auto& filter : filters) {
    if (!combined_filter) {
      combined_filter = graphar::_And(filter, filter);
    } else {
      combined_filter = graphar::_And(combined_filter, filter);
    }
  }

  auto maybe_filter_reader = graphar::VertexPropertyArrowChunkReader::Make(
      vertex_info_, labels_, prefix_, {});
  auto filter_reader = maybe_filter_reader.value();
  filter_reader->Filter(combined_filter);
  for (int chunk_idx = 0; chunk_idx * CHUNK_SIZE < TOT_ROWS_NUM; ++chunk_idx) {
    auto filter_result = filter_reader->GetLabelChunk();
    auto filter_table = filter_result.value();
    total_count += filter_table->num_rows();
    filter_reader->next_chunk();
  }
  // std::cout << "Total valid count: " << total_count << std::endl;
  std::vector<int64_t> indices64;

  for (int value : indices) {
    indices64.push_back(static_cast<int64_t>(value));
  }

  return indices64;
}

Result<std::vector<IdType>> VerticesCollection::filter(
    const std::string& property_name,
    std::shared_ptr<Expression> filter_expression,
    std::vector<IdType>* new_valid_chunk) {
  std::vector<int> indices;
  const int TOT_ROWS_NUM = vertex_num_;
  const int CHUNK_SIZE = vertex_info_->GetChunkSize();
  int total_count = 0;
  auto property_group = vertex_info_->GetPropertyGroup(property_name);
  auto maybe_filter_reader = graphar::VertexPropertyArrowChunkReader::Make(
      vertex_info_, property_group, prefix_, {});
  auto filter_reader = maybe_filter_reader.value();
  filter_reader->Filter(filter_expression);
  std::vector<int64_t> indices64;
  if (is_filtered_) {
    for (int chunk_idx : valid_chunk_) {
      // how to itetate valid_chunk_?
      filter_reader->seek(chunk_idx * CHUNK_SIZE);
      auto filter_result =
          filter_reader->GetChunk(graphar::GetChunkVersion::V1);
      auto filter_table = filter_result.value();
      int count = filter_table->num_rows();
      if (count != 0 && new_valid_chunk != nullptr) {
        new_valid_chunk->emplace_back(static_cast<IdType>(chunk_idx));
        // TODO(elssky): record indices
        int kVertexIndexCol = filter_table->schema()->GetFieldIndex(
            GeneralParams::kVertexIndexCol);
        auto column_array = filter_table->column(kVertexIndexCol)->chunk(0);
        auto int64_array =
            std::static_pointer_cast<arrow::Int64Array>(column_array);
        for (int64_t i = 0; i < int64_array->length(); ++i) {
          if (!int64_array->IsNull(i)) {
            indices64.push_back(int64_array->Value(i));
          }
        }
      }
    }
  } else {
    for (int chunk_idx = 0; chunk_idx * CHUNK_SIZE < TOT_ROWS_NUM;
         ++chunk_idx) {
      auto filter_result =
          filter_reader->GetChunk(graphar::GetChunkVersion::V1);
      auto filter_table = filter_result.value();
      int count = filter_table->num_rows();
      filter_reader->next_chunk();
      total_count += count;
      if (count != 0) {
        valid_chunk_.emplace_back(static_cast<IdType>(chunk_idx));
        // TODO(elssky): record indices
        int kVertexIndexCol = filter_table->schema()->GetFieldIndex(
            GeneralParams::kVertexIndexCol);
        auto column_array = filter_table->column(kVertexIndexCol)->chunk(0);
        auto int64_array =
            std::static_pointer_cast<arrow::Int64Array>(column_array);
        for (int64_t i = 0; i < int64_array->length(); ++i) {
          if (!int64_array->IsNull(i)) {
            indices64.push_back(int64_array->Value(i));
          }
        }
      }
    }
  }
  // std::cout << "Total valid count: " << total_count << std::endl;
  return indices64;
}

Result<std::shared_ptr<VerticesCollection>>
VerticesCollection::verticesWithLabel(
    const std::string& filter_label,
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& type) {
  auto prefix = graph_info->GetPrefix();
  auto vertex_info = graph_info->GetVertexInfo(type);
  auto labels = vertex_info->GetLabels();
  auto vertices_collection =
      std::make_shared<VerticesCollection>(vertex_info, prefix);
  vertices_collection->filtered_ids_ =
      vertices_collection->filter({filter_label}).value();
  vertices_collection->is_filtered_ = true;
  return vertices_collection;
}

Result<std::shared_ptr<VerticesCollection>>
VerticesCollection::verticesWithLabelbyAcero(
    const std::string& filter_label,
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& type) {
  auto prefix = graph_info->GetPrefix();
  auto vertex_info = graph_info->GetVertexInfo(type);
  auto labels = vertex_info->GetLabels();
  auto vertices_collection =
      std::make_shared<VerticesCollection>(vertex_info, prefix);
  vertices_collection->filtered_ids_ =
      vertices_collection->filter_by_acero({filter_label}).value();
  vertices_collection->is_filtered_ = true;
  return vertices_collection;
}

Result<std::shared_ptr<VerticesCollection>>
VerticesCollection::verticesWithLabel(
    const std::string& filter_label,
    const std::shared_ptr<VerticesCollection>& vertices_collection) {
  auto new_vertices_collection = std::make_shared<VerticesCollection>(
      vertices_collection->vertex_info_, vertices_collection->prefix_);
  auto filtered_ids =
      new_vertices_collection
          ->filter({filter_label}, &new_vertices_collection->valid_chunk_)
          .value();
  if (vertices_collection->is_filtered_) {
    std::unordered_set<IdType> origin_set(
        vertices_collection->filtered_ids_.begin(),
        vertices_collection->filtered_ids_.end());
    std::unordered_set<int> intersection;
    for (int num : filtered_ids) {
      if (origin_set.count(num)) {
        intersection.insert(num);
      }
    }
    filtered_ids =
        std::vector<IdType>(intersection.begin(), intersection.end());

    new_vertices_collection->is_filtered_ = true;
  }
  new_vertices_collection->filtered_ids_ = filtered_ids;

  return new_vertices_collection;
}

Result<std::shared_ptr<VerticesCollection>>
VerticesCollection::verticesWithMultipleLabels(
    const std::vector<std::string>& filter_labels,
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& type) {
  auto prefix = graph_info->GetPrefix();
  auto vertex_info = graph_info->GetVertexInfo(type);
  auto labels = vertex_info->GetLabels();
  auto vertices_collection =
      std::make_shared<VerticesCollection>(vertex_info, prefix);
  vertices_collection->filtered_ids_ =
      vertices_collection->filter(filter_labels).value();
  vertices_collection->is_filtered_ = true;
  return vertices_collection;
}

Result<std::shared_ptr<VerticesCollection>>
VerticesCollection::verticesWithMultipleLabelsbyAcero(
    const std::vector<std::string>& filter_labels,
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& type) {
  auto prefix = graph_info->GetPrefix();
  auto vertex_info = graph_info->GetVertexInfo(type);
  auto labels = vertex_info->GetLabels();
  auto vertices_collection =
      std::make_shared<VerticesCollection>(vertex_info, prefix);
  vertices_collection->filtered_ids_ =
      vertices_collection->filter_by_acero(filter_labels).value();
  vertices_collection->is_filtered_ = true;
  return vertices_collection;
}

Result<std::shared_ptr<VerticesCollection>>
VerticesCollection::verticesWithMultipleLabels(
    const std::vector<std::string>& filter_labels,
    const std::shared_ptr<VerticesCollection>& vertices_collection) {
  auto new_vertices_collection = std::make_shared<VerticesCollection>(
      vertices_collection->vertex_info_, vertices_collection->prefix_);
  auto filtered_ids =
      vertices_collection
          ->filter(filter_labels, &new_vertices_collection->valid_chunk_)
          .value();
  if (vertices_collection->is_filtered_) {
    std::unordered_set<IdType> origin_set(
        vertices_collection->filtered_ids_.begin(),
        vertices_collection->filtered_ids_.end());
    std::unordered_set<int> intersection;
    for (int num : filtered_ids) {
      if (origin_set.count(num)) {
        intersection.insert(num);
      }
    }
    filtered_ids =
        std::vector<IdType>(intersection.begin(), intersection.end());

    new_vertices_collection->is_filtered_ = true;
  }
  new_vertices_collection->filtered_ids_ = filtered_ids;

  return new_vertices_collection;
}

Result<std::shared_ptr<VerticesCollection>>
VerticesCollection::verticesWithProperty(
    const std::string property_name, const graphar::util::Filter filter,
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& type) {
  auto prefix = graph_info->GetPrefix();
  auto vertex_info = graph_info->GetVertexInfo(type);
  auto vertices_collection =
      std::make_shared<VerticesCollection>(vertex_info, prefix);
  vertices_collection->filtered_ids_ =
      vertices_collection->filter(property_name, filter).value();
  vertices_collection->is_filtered_ = true;
  return vertices_collection;
}

Result<std::shared_ptr<VerticesCollection>>
VerticesCollection::verticesWithProperty(
    const std::string property_name, const graphar::util::Filter filter,
    const std::shared_ptr<VerticesCollection>& vertices_collection) {
  auto new_vertices_collection = std::make_shared<VerticesCollection>(
      vertices_collection->vertex_info_, vertices_collection->prefix_);
  auto filtered_ids = vertices_collection
                          ->filter(property_name, filter,
                                   &new_vertices_collection->valid_chunk_)
                          .value();
  if (vertices_collection->is_filtered_) {
    std::unordered_set<IdType> origin_set(
        vertices_collection->filtered_ids_.begin(),
        vertices_collection->filtered_ids_.end());
    std::unordered_set<int> intersection;
    for (int num : filtered_ids) {
      if (origin_set.count(num)) {
        intersection.insert(num);
      }
    }
    filtered_ids =
        std::vector<IdType>(intersection.begin(), intersection.end());
    new_vertices_collection->is_filtered_ = true;
  }
  new_vertices_collection->filtered_ids_ = filtered_ids;
  return new_vertices_collection;
}

template <typename T>
Result<T> Vertex::property(const std::string& property) const {
  if constexpr (std::is_final<T>::value) {
    auto it = list_properties_.find(property);
    if (it == list_properties_.end()) {
      return Status::KeyError("The list property ", property,
                              " doesn't exist.");
    }
    auto array = std::dynamic_pointer_cast<
        typename CTypeToArrowType<typename T::ValueType>::ArrayType>(
        it->second);
    const typename T::ValueType* values = array->raw_values();
    return T(values, array->length());
  } else {
    if (properties_.find(property) == properties_.end()) {
      return Status::KeyError("Property with name ", property,
                              " does not exist in the vertex.");
    }
    try {
      if (!properties_.at(property).has_value())
        return Status::TypeError("The value of the ", property, " is null.");
      T ret = std::any_cast<T>(properties_.at(property));
      return ret;
    } catch (const std::bad_any_cast& e) {
      return Status::TypeError("Any cast failed, the property type of ",
                               property, " is not matched ", e.what());
    }
  }
}

template <>
Result<Date> Vertex::property(const std::string& property) const {
  if (properties_.find(property) == properties_.end()) {
    return Status::KeyError("Property with name ", property,
                            " does not exist in the vertex.");
  }
  try {
    if (!properties_.at(property).has_value())
      return Status::TypeError("The value of the ", property, " is null.");
    Date ret(std::any_cast<Date::c_type>(properties_.at(property)));
    return ret;
  } catch (const std::bad_any_cast& e) {
    return Status::TypeError("Any cast failed, the property type of ", property,
                             " is not matched ", e.what());
  }
}

template <>
Result<Timestamp> Vertex::property(const std::string& property) const {
  if (properties_.find(property) == properties_.end()) {
    return Status::KeyError("Property with name ", property,
                            " does not exist in the vertex.");
  }
  try {
    if (!properties_.at(property).has_value())
      return Status::TypeError("The value of the ", property, " is null.");
    Timestamp ret(std::any_cast<Timestamp::c_type>(properties_.at(property)));
    return ret;
  } catch (const std::bad_any_cast& e) {
    return Status::TypeError("Any cast failed, the property type of ", property,
                             " is not matched ", e.what());
  }
}

template <>
Result<StringArray> Vertex::property(const std::string& property) const {
  auto it = list_properties_.find(property);
  if (it == list_properties_.end()) {
    return Status::KeyError("The list property ", property, " doesn't exist.");
  }
  auto array = std::dynamic_pointer_cast<arrow::StringArray>(it->second);
  return StringArray(array->raw_value_offsets(), array->raw_data(),
                     array->length());
}

Edge::Edge(
    AdjListArrowChunkReader& adj_list_reader,                          // NOLINT
    std::vector<AdjListPropertyArrowChunkReader>& property_readers) {  // NOLINT
  // get the first row of table
  GAR_ASSIGN_OR_RAISE_ERROR(auto adj_list_chunk_table,
                            adj_list_reader.GetChunk());
  src_id_ = std::dynamic_pointer_cast<arrow::Int64Array>(
                adj_list_chunk_table->column(0)->chunk(0))
                ->GetView(0);
  dst_id_ = std::dynamic_pointer_cast<arrow::Int64Array>(
                adj_list_chunk_table->column(1)->chunk(0))
                ->GetView(0);
  for (auto& reader : property_readers) {
    // get the first row of table
    GAR_ASSIGN_OR_RAISE_ERROR(auto chunk_table, reader.GetChunk());
    auto schema = chunk_table->schema();
    for (int i = 0; i < schema->num_fields(); ++i) {
      auto field = chunk_table->field(i);
      if (field->type()->id() == arrow::Type::LIST) {
        auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(
            chunk_table->column(i)->chunk(0));
        list_properties_[field->name()] = list_array->value_slice(0);
      } else {
        auto type = DataType::ArrowDataTypeToDataType(field->type());
        GAR_RAISE_ERROR_NOT_OK(TryToCastToAny(type,
                                              chunk_table->column(i)->chunk(0),
                                              properties_[field->name()]));
      }
    }
  }
}

template <typename T>
Result<T> Edge::property(const std::string& property) const {
  if constexpr (std::is_final<T>::value) {
    auto it = list_properties_.find(property);
    if (it == list_properties_.end()) {
      return Status::KeyError("The list property ", property,
                              " doesn't exist.");
    }
    auto array = std::dynamic_pointer_cast<
        typename CTypeToArrowType<typename T::ValueType>::ArrayType>(
        it->second);
    const typename T::ValueType* values = array->raw_values();
    return T(values, array->length());
  } else {
    if (properties_.find(property) == properties_.end()) {
      return Status::KeyError("Property with name ", property,
                              " does not exist in the edge.");
    }
    try {
      if (!properties_.at(property).has_value())
        return Status::TypeError("The value of the ", property, " is null.");
      T ret = std::any_cast<T>(properties_.at(property));
      return ret;
    } catch (const std::bad_any_cast& e) {
      return Status::TypeError("Any cast failed, the property type of ",
                               property, " is not matched ", e.what());
    }
  }
}

template <>
Result<Date> Edge::property(const std::string& property) const {
  if (properties_.find(property) == properties_.end()) {
    return Status::KeyError("Property with name ", property,
                            " does not exist in the edge.");
  }
  try {
    if (!properties_.at(property).has_value())
      return Status::TypeError("The value of the ", property, " is null.");
    Date ret(std::any_cast<Date::c_type>(properties_.at(property)));
    return ret;
  } catch (const std::bad_any_cast& e) {
    return Status::TypeError("Any cast failed, the property type of ", property,
                             " is not matched ", e.what());
  }
}

template <>
Result<Timestamp> Edge::property(const std::string& property) const {
  if (properties_.find(property) == properties_.end()) {
    return Status::KeyError("Property with name ", property,
                            " does not exist in the edge.");
  }
  try {
    if (!properties_.at(property).has_value())
      return Status::TypeError("The value of the ", property, " is null.");
    Timestamp ret(std::any_cast<Timestamp::c_type>(properties_.at(property)));
    return ret;
  } catch (const std::bad_any_cast& e) {
    return Status::TypeError("Any cast failed, the property type of ", property,
                             " is not matched ", e.what());
  }
}

template <>
Result<StringArray> Edge::property(const std::string& property) const {
  auto it = list_properties_.find(property);
  if (it == list_properties_.end()) {
    return Status::KeyError("The list property ", property, " doesn't exist.");
  }
  auto array = std::dynamic_pointer_cast<arrow::StringArray>(it->second);
  return StringArray(array->raw_value_offsets(), array->raw_data(),
                     array->length());
}

#define INSTANTIATE_PROPERTY(T)                                          \
  template Result<T> Vertex::property<T>(const std::string& name) const; \
  template Result<T> Edge::property<T>(const std::string& name) const;

INSTANTIATE_PROPERTY(bool)
INSTANTIATE_PROPERTY(const bool&)
INSTANTIATE_PROPERTY(int32_t)
INSTANTIATE_PROPERTY(const int32_t&)
INSTANTIATE_PROPERTY(Int32Array)
INSTANTIATE_PROPERTY(int64_t)
INSTANTIATE_PROPERTY(const int64_t&)
INSTANTIATE_PROPERTY(Int64Array)
INSTANTIATE_PROPERTY(float)
INSTANTIATE_PROPERTY(const float&)
INSTANTIATE_PROPERTY(FloatArray)
INSTANTIATE_PROPERTY(double)
INSTANTIATE_PROPERTY(const double&)
INSTANTIATE_PROPERTY(DoubleArray)
INSTANTIATE_PROPERTY(std::string)
INSTANTIATE_PROPERTY(const std::string&)

IdType EdgeIter::source() {
  adj_list_reader_.seek(cur_offset_);
  GAR_ASSIGN_OR_RAISE_ERROR(auto chunk, adj_list_reader_.GetChunk());
  auto src_column = chunk->column(0);
  return std::dynamic_pointer_cast<arrow::Int64Array>(src_column->chunk(0))
      ->GetView(0);
}

IdType EdgeIter::destination() {
  adj_list_reader_.seek(cur_offset_);
  GAR_ASSIGN_OR_RAISE_ERROR(auto chunk, adj_list_reader_.GetChunk());
  auto src_column = chunk->column(1);
  return std::dynamic_pointer_cast<arrow::Int64Array>(src_column->chunk(0))
      ->GetView(0);
}

bool EdgeIter::first_src(const EdgeIter& from, IdType id) {
  if (from.is_end())
    return false;

  // ordered_by_dest or unordered_by_dest
  if (adj_list_type_ == AdjListType::ordered_by_dest ||
      adj_list_type_ == AdjListType::unordered_by_dest) {
    if (from.global_chunk_index_ >= chunk_end_) {
      return false;
    }
    if (from.global_chunk_index_ == global_chunk_index_) {
      cur_offset_ = from.cur_offset_;
    } else if (from.global_chunk_index_ < chunk_begin_) {
      this->to_begin();
    } else {
      global_chunk_index_ = from.global_chunk_index_;
      cur_offset_ = from.cur_offset_;
      vertex_chunk_index_ = from.vertex_chunk_index_;
      this->refresh();
    }
    while (!this->is_end()) {
      if (this->source() == id)
        return true;
      this->operator++();
    }
    return false;
  }

  // unordered_by_source
  if (adj_list_type_ == AdjListType::unordered_by_source) {
    IdType expect_chunk_index =
        index_converter_->IndexPairToGlobalChunkIndex(id / src_chunk_size_, 0);
    if (expect_chunk_index > chunk_end_)
      return false;
    if (from.global_chunk_index_ >= chunk_end_) {
      return false;
    }
    bool need_refresh = false;
    if (from.global_chunk_index_ == global_chunk_index_) {
      cur_offset_ = from.cur_offset_;
    } else if (from.global_chunk_index_ < chunk_begin_) {
      this->to_begin();
    } else {
      global_chunk_index_ = from.global_chunk_index_;
      cur_offset_ = from.cur_offset_;
      vertex_chunk_index_ = from.vertex_chunk_index_;
      need_refresh = true;
    }
    if (global_chunk_index_ < expect_chunk_index) {
      global_chunk_index_ = expect_chunk_index;
      cur_offset_ = 0;
      vertex_chunk_index_ = id / src_chunk_size_;
      need_refresh = true;
    }
    if (need_refresh)
      this->refresh();
    while (!this->is_end()) {
      if (this->source() == id)
        return true;
      if (vertex_chunk_index_ > id / src_chunk_size_)
        return false;
      this->operator++();
    }
    return false;
  }

  // ordered_by_source
  auto st = offset_reader_->seek(id);
  if (!st.ok()) {
    return false;
  }
  auto maybe_offset_chunk = offset_reader_->GetChunk();
  if (!maybe_offset_chunk.status().ok()) {
    return false;
  }
  auto offset_array =
      std::dynamic_pointer_cast<arrow::Int64Array>(maybe_offset_chunk.value());
  auto begin_offset = static_cast<IdType>(offset_array->Value(0));
  auto end_offset = static_cast<IdType>(offset_array->Value(1));
  if (begin_offset >= end_offset) {
    return false;
  }
  auto vertex_chunk_index_of_id = offset_reader_->GetChunkIndex();
  auto begin_global_index = index_converter_->IndexPairToGlobalChunkIndex(
      vertex_chunk_index_of_id, begin_offset / chunk_size_);
  auto end_global_index = index_converter_->IndexPairToGlobalChunkIndex(
      vertex_chunk_index_of_id, end_offset / chunk_size_);
  if (begin_global_index <= from.global_chunk_index_ &&
      from.global_chunk_index_ <= end_global_index) {
    if (begin_offset < from.cur_offset_ && from.cur_offset_ < end_offset) {
      global_chunk_index_ = from.global_chunk_index_;
      cur_offset_ = from.cur_offset_;
      vertex_chunk_index_ = from.vertex_chunk_index_;
      refresh();
      return true;
    } else if (from.cur_offset_ <= begin_offset) {
      global_chunk_index_ = begin_global_index;
      cur_offset_ = begin_offset;
      vertex_chunk_index_ = vertex_chunk_index_of_id;
      refresh();
      return true;
    } else {
      return false;
    }
  } else if (from.global_chunk_index_ < begin_global_index) {
    global_chunk_index_ = begin_global_index;
    cur_offset_ = begin_offset;
    vertex_chunk_index_ = vertex_chunk_index_of_id;
    refresh();
    return true;
  } else {
    return false;
  }
}

bool EdgeIter::first_dst(const EdgeIter& from, IdType id) {
  if (from.is_end())
    return false;

  // ordered_by_source or unordered_by_source
  if (adj_list_type_ == AdjListType::ordered_by_source ||
      adj_list_type_ == AdjListType::unordered_by_source) {
    if (from.global_chunk_index_ >= chunk_end_) {
      return false;
    }
    if (from.global_chunk_index_ == global_chunk_index_) {
      cur_offset_ = from.cur_offset_;
    } else if (from.global_chunk_index_ < chunk_begin_) {
      this->to_begin();
    } else {
      global_chunk_index_ = from.global_chunk_index_;
      cur_offset_ = from.cur_offset_;
      vertex_chunk_index_ = from.vertex_chunk_index_;
      this->refresh();
    }
    while (!this->is_end()) {
      if (this->destination() == id)
        return true;
      this->operator++();
    }
    return false;
  }

  // unordered_by_dest
  if (adj_list_type_ == AdjListType::unordered_by_dest) {
    IdType expect_chunk_index =
        index_converter_->IndexPairToGlobalChunkIndex(id / dst_chunk_size_, 0);
    if (expect_chunk_index > chunk_end_)
      return false;
    if (from.global_chunk_index_ >= chunk_end_) {
      return false;
    }
    bool need_refresh = false;
    if (from.global_chunk_index_ == global_chunk_index_) {
      cur_offset_ = from.cur_offset_;
    } else if (from.global_chunk_index_ < chunk_begin_) {
      this->to_begin();
    } else {
      global_chunk_index_ = from.global_chunk_index_;
      cur_offset_ = from.cur_offset_;
      vertex_chunk_index_ = from.vertex_chunk_index_;
      need_refresh = true;
    }
    if (global_chunk_index_ < expect_chunk_index) {
      global_chunk_index_ = expect_chunk_index;
      cur_offset_ = 0;
      vertex_chunk_index_ = id / dst_chunk_size_;
      need_refresh = true;
    }
    if (need_refresh)
      this->refresh();
    while (!this->is_end()) {
      if (this->destination() == id)
        return true;
      if (vertex_chunk_index_ > id / dst_chunk_size_)
        return false;
      this->operator++();
    }
    return false;
  }

  // ordered_by_dest
  auto st = offset_reader_->seek(id);
  if (!st.ok()) {
    return false;
  }
  auto maybe_offset_chunk = offset_reader_->GetChunk();
  if (!maybe_offset_chunk.status().ok()) {
    return false;
  }
  auto offset_array =
      std::dynamic_pointer_cast<arrow::Int64Array>(maybe_offset_chunk.value());
  auto begin_offset = static_cast<IdType>(offset_array->Value(0));
  auto end_offset = static_cast<IdType>(offset_array->Value(1));
  if (begin_offset >= end_offset) {
    return false;
  }
  auto vertex_chunk_index_of_id = offset_reader_->GetChunkIndex();
  auto begin_global_index = index_converter_->IndexPairToGlobalChunkIndex(
      vertex_chunk_index_of_id, begin_offset / chunk_size_);
  auto end_global_index = index_converter_->IndexPairToGlobalChunkIndex(
      vertex_chunk_index_of_id, end_offset / chunk_size_);
  if (begin_global_index <= from.global_chunk_index_ &&
      from.global_chunk_index_ <= end_global_index) {
    if (begin_offset < from.cur_offset_ && from.cur_offset_ < end_offset) {
      global_chunk_index_ = from.global_chunk_index_;
      cur_offset_ = from.cur_offset_;
      vertex_chunk_index_ = from.vertex_chunk_index_;
      refresh();
      return true;
    } else if (from.cur_offset_ <= begin_offset) {
      global_chunk_index_ = begin_global_index;
      cur_offset_ = begin_offset;
      vertex_chunk_index_ = vertex_chunk_index_of_id;
      refresh();
      return true;
    } else {
      return false;
    }
  } else if (from.global_chunk_index_ < begin_global_index) {
    global_chunk_index_ = begin_global_index;
    cur_offset_ = begin_offset;
    vertex_chunk_index_ = vertex_chunk_index_of_id;
    refresh();
    return true;
  } else {
    return false;
  }
}

Result<std::shared_ptr<EdgesCollection>> EdgesCollection::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
    const std::string& edge_type, const std::string& dst_type,
    AdjListType adj_list_type, const IdType vertex_chunk_begin,
    const IdType vertex_chunk_end) noexcept {
  auto edge_info = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
  if (!edge_info) {
    return Status::KeyError("The edge ", src_type, " ", edge_type, " ",
                            dst_type, " doesn't exist.");
  }
  if (!edge_info->HasAdjacentListType(adj_list_type)) {
    return Status::Invalid("The edge ", edge_type, " of adj list type ",
                           AdjListTypeToString(adj_list_type),
                           " doesn't exist.");
  }
  switch (adj_list_type) {
  case AdjListType::ordered_by_source:
    return std::make_shared<OBSEdgeCollection>(
        edge_info, graph_info->GetPrefix(), vertex_chunk_begin,
        vertex_chunk_end);
  case AdjListType::ordered_by_dest:
    return std::make_shared<OBDEdgesCollection>(
        edge_info, graph_info->GetPrefix(), vertex_chunk_begin,
        vertex_chunk_end);
  case AdjListType::unordered_by_source:
    return std::make_shared<UBSEdgesCollection>(
        edge_info, graph_info->GetPrefix(), vertex_chunk_begin,
        vertex_chunk_end);
  case AdjListType::unordered_by_dest:
    return std::make_shared<UBDEdgesCollection>(
        edge_info, graph_info->GetPrefix(), vertex_chunk_begin,
        vertex_chunk_end);
  default:
    return Status::Invalid("Unknown adj list type.");
  }
  return Status::OK();
}

}  // namespace graphar
