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
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/compute/api.h"

#include "graphar/arrow/chunk_reader.h"
#include "graphar/filesystem.h"
#include "graphar/fwd.h"
#include "graphar/general_params.h"
#include "graphar/graph_info.h"
#include "graphar/reader_util.h"
#include "graphar/result.h"
#include "graphar/status.h"
#include "graphar/types.h"
#include "graphar/util.h"

namespace graphar {

namespace {

Result<std::shared_ptr<arrow::Schema>> PropertyGroupToSchema(
    const std::shared_ptr<PropertyGroup> pg,
    bool contain_index_column = false) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  if (contain_index_column) {
    fields.push_back(std::make_shared<arrow::Field>(
        GeneralParams::kVertexIndexCol, arrow::int64()));
  }
  for (const auto& prop : pg->GetProperties()) {
    auto dataType = DataType::DataTypeToArrowDataType(prop.type);
    if (prop.cardinality != Cardinality::SINGLE) {
      dataType = arrow::list(dataType);
    }
    fields.push_back(std::make_shared<arrow::Field>(prop.name, dataType));
  }
  return arrow::schema(fields);
}

Result<std::shared_ptr<arrow::Schema>> LabelToSchema(
    std::vector<std::string> labels, bool contain_index_column = false) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  if (contain_index_column) {
    fields.push_back(std::make_shared<arrow::Field>(
        GeneralParams::kVertexIndexCol, arrow::int64()));
  }
  for (const auto& lab : labels) {
    fields.push_back(std::make_shared<arrow::Field>(lab, arrow::boolean()));
  }
  return arrow::schema(fields);
}
Status GeneralCast(const std::shared_ptr<arrow::Array>& in,
                   const std::shared_ptr<arrow::DataType>& to_type,
                   std::shared_ptr<arrow::Array>* out) {
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(*out,
                                       arrow::compute::Cast(*in, to_type));
  return Status::OK();
}

Status CastStringToLargeString(const std::shared_ptr<arrow::Array>& in,
                               const std::shared_ptr<arrow::DataType>& to_type,
                               std::shared_ptr<arrow::Array>* out) {
  auto array_data = in->data()->Copy();
  auto offset = array_data->buffers[1];
  using from_offset_type = typename arrow::StringArray::offset_type;
  using to_string_offset_type = typename arrow::LargeStringArray::offset_type;
  auto raw_value_offsets_ =
      offset == NULLPTR
          ? NULLPTR
          : reinterpret_cast<const from_offset_type*>(offset->data());
  std::vector<to_string_offset_type> to_offset(offset->size() /
                                               sizeof(from_offset_type));
  for (size_t i = 0; i < to_offset.size(); ++i) {
    to_offset[i] = raw_value_offsets_[i];
  }
  std::shared_ptr<arrow::Buffer> buffer;
  arrow::TypedBufferBuilder<to_string_offset_type> buffer_builder;
  RETURN_NOT_ARROW_OK(
      buffer_builder.Append(to_offset.data(), to_offset.size()));
  RETURN_NOT_ARROW_OK(buffer_builder.Finish(&buffer));
  array_data->type = to_type;
  array_data->buffers[1] = buffer;
  *out = arrow::MakeArray(array_data);
  RETURN_NOT_ARROW_OK((*out)->ValidateFull());
  return Status::OK();
}

// helper function to cast arrow::Table with a schema
Status CastTableWithSchema(const std::shared_ptr<arrow::Table>& table,
                           const std::shared_ptr<arrow::Schema>& schema,
                           std::shared_ptr<arrow::Table>* out_table) {
  if (table->schema()->Equals(*schema)) {
    *out_table = table;
  }
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
  for (int64_t i = 0; i < table->num_columns(); ++i) {
    auto column = table->column(i);
    auto table_field = table->field(i);
    auto field_name = table_field->name();

    auto schema_field = schema->GetFieldByName(field_name);
    if (table_field->type()->Equals(schema_field->type())) {
      columns.push_back(column);
      fields.push_back(table_field);
      continue;
    }
    auto from_t = table_field->type();
    auto to_t = schema_field->type();
    std::vector<std::shared_ptr<arrow::Array>> chunks;
    // process cast for each chunk
    for (int64_t j = 0; j < column->num_chunks(); ++j) {
      auto chunk = column->chunk(j);
      std::shared_ptr<arrow::Array> out;
      if (arrow::compute::CanCast(*from_t, *to_t)) {
        GAR_RETURN_NOT_OK(GeneralCast(chunk, to_t, &out));
        chunks.push_back(out);
      } else if (from_t->Equals(arrow::utf8()) &&
                 to_t->Equals(arrow::large_utf8())) {
        GAR_RETURN_NOT_OK(CastStringToLargeString(chunk, to_t, &out));
        chunks.push_back(out);
      }
    }
    fields.push_back(arrow::field(field_name, to_t));
    columns.push_back(std::make_shared<arrow::ChunkedArray>(chunks, to_t));
  }
  auto new_schema = std::make_shared<arrow::Schema>(fields);
  *out_table = arrow::Table::Make(new_schema, columns);
  return Status::OK();
}
}  // namespace

VertexPropertyArrowChunkReader::VertexPropertyArrowChunkReader(
    const std::shared_ptr<VertexInfo>& vertex_info,
    const std::shared_ptr<PropertyGroup>& property_group,
    const std::string& prefix, const util::FilterOptions& options)
    : VertexPropertyArrowChunkReader(vertex_info, property_group, {}, prefix,
                                     options) {}

VertexPropertyArrowChunkReader::VertexPropertyArrowChunkReader(
    const std::shared_ptr<VertexInfo>& vertex_info,
    const std::shared_ptr<PropertyGroup>& property_group,
    const std::vector<std::string>& property_names, const std::string& prefix,
    const util::FilterOptions& options)
    : vertex_info_(std::move(vertex_info)),
      property_group_(std::move(property_group)),
      property_names_(std::move(property_names)),
      chunk_index_(0),
      seek_id_(0),
      schema_(nullptr),
      chunk_table_(nullptr),
      filter_options_(options) {
  GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &prefix_));
  GAR_ASSIGN_OR_RAISE_ERROR(auto pg_path_prefix,
                            vertex_info->GetPathPrefix(property_group));
  std::string base_dir = prefix_ + pg_path_prefix;
  GAR_ASSIGN_OR_RAISE_ERROR(chunk_num_,
                            util::GetVertexChunkNum(prefix_, vertex_info));
  GAR_ASSIGN_OR_RAISE_ERROR(vertex_num_,
                            util::GetVertexNum(prefix_, vertex_info_));
  GAR_ASSIGN_OR_RAISE_ERROR(schema_,
                            PropertyGroupToSchema(property_group_, true));
}

// initialize for labels
VertexPropertyArrowChunkReader::VertexPropertyArrowChunkReader(
    const std::shared_ptr<VertexInfo>& vertex_info,
    const std::vector<std::string>& labels, const std::string& prefix,
    const util::FilterOptions& options)
    : vertex_info_(std::move(vertex_info)),
      labels_(labels),
      chunk_index_(0),
      seek_id_(0),
      schema_(nullptr),
      chunk_table_(nullptr),
      filter_options_(options) {
  GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &prefix_));

  std::string base_dir = prefix_ + vertex_info_->GetPrefix() + "labels/chunk" +
                         std::to_string(chunk_index_);
  GAR_ASSIGN_OR_RAISE_ERROR(chunk_num_,
                            util::GetVertexChunkNum(prefix_, vertex_info));
  GAR_ASSIGN_OR_RAISE_ERROR(vertex_num_,
                            util::GetVertexNum(prefix_, vertex_info_));
  GAR_ASSIGN_OR_RAISE_ERROR(schema_, LabelToSchema(labels));
}

Status VertexPropertyArrowChunkReader::seek(IdType id) {
  seek_id_ = id;
  IdType pre_chunk_index = chunk_index_;
  chunk_index_ = id / vertex_info_->GetChunkSize();
  if (chunk_index_ != pre_chunk_index) {
    // TODO(@acezen): use a cache to avoid reloading the same chunk, could use
    //  a LRU cache.
    chunk_table_.reset();
  }
  if (chunk_index_ >= chunk_num_) {
    return Status::IndexError("Internal vertex id ", id, " is out of range [0,",
                              chunk_num_ * vertex_info_->GetChunkSize(),
                              ") of vertex ", vertex_info_->GetType());
  }
  return Status::OK();
}

Result<std::shared_ptr<arrow::Table>>
VertexPropertyArrowChunkReader::GetChunkV2() {
  if (chunk_table_ == nullptr) {
    GAR_ASSIGN_OR_RAISE(
        auto chunk_file_path,
        vertex_info_->GetFilePath(property_group_, chunk_index_));
    std::vector<int> column_indices = {};
    std::vector<std::string> property_names;
    if (!filter_options_.columns && !property_names_.empty()) {
      property_names = property_names_;
    } else {
      if (!property_names_.empty()) {
        for (const auto& col : filter_options_.columns.value().get()) {
          if (std::find(property_names_.begin(), property_names_.end(), col) ==
              property_names_.end()) {
            return Status::Invalid("Column ", col,
                                   " is not in select properties.");
          }
          property_names.push_back(col);
        }
      }
    }
    for (const auto& col : property_names) {
      auto field_index = schema_->GetFieldIndex(col);
      if (field_index == -1) {
        return Status::Invalid("Column ", col, " is not in select properties.");
      }
      column_indices.push_back(field_index);
    }
    std::string path = prefix_ + chunk_file_path;
    GAR_ASSIGN_OR_RAISE(
        chunk_table_, fs_->ReadFileToTable(path, property_group_->GetFileType(),
                                           column_indices));
    if (schema_ != nullptr && filter_options_.filter == nullptr) {
      GAR_RETURN_NOT_OK(
          CastTableWithSchema(chunk_table_, schema_, &chunk_table_));
    }
  }
  IdType row_offset = seek_id_ - chunk_index_ * vertex_info_->GetChunkSize();
  return chunk_table_->Slice(row_offset);
}

Result<std::shared_ptr<arrow::Table>>
VertexPropertyArrowChunkReader::GetChunkV1() {
  GAR_RETURN_NOT_OK(util::CheckFilterOptions(filter_options_, property_group_));
  if (chunk_table_ == nullptr) {
    GAR_ASSIGN_OR_RAISE(
        auto chunk_file_path,
        vertex_info_->GetFilePath(property_group_, chunk_index_));
    std::string path = prefix_ + chunk_file_path;
    if (property_names_.empty()) {
      GAR_ASSIGN_OR_RAISE(
          chunk_table_,
          fs_->ReadFileToTable(path, property_group_->GetFileType(),
                               filter_options_));
    } else {
      util::FilterOptions temp_filter_options;
      temp_filter_options.filter = filter_options_.filter;
      std::vector<std::string> intersection_columns;
      if (!filter_options_.columns) {
        temp_filter_options.columns = std::ref(property_names_);
      } else {
        for (const auto& col : filter_options_.columns.value().get()) {
          if (std::find(property_names_.begin(), property_names_.end(), col) ==
              property_names_.end()) {
            return Status::Invalid("Column ", col,
                                   " is not in select properties.");
          }
        }
        temp_filter_options.columns = filter_options_.columns;
      }
      GAR_ASSIGN_OR_RAISE(
          chunk_table_,
          fs_->ReadFileToTable(path, property_group_->GetFileType(),
                               temp_filter_options));
    }
    // TODO(acezen): filter pushdown doesn't support cast schema now
    if (schema_ != nullptr && filter_options_.filter == nullptr) {
      GAR_RETURN_NOT_OK(
          CastTableWithSchema(chunk_table_, schema_, &chunk_table_));
    }
  }
  IdType row_offset = seek_id_ - chunk_index_ * vertex_info_->GetChunkSize();
  return chunk_table_->Slice(row_offset);
}

Result<std::shared_ptr<arrow::Table>> VertexPropertyArrowChunkReader::GetChunk(
    GetChunkVersion version) {
  switch (version) {
  case GetChunkVersion::V1:
    return GetChunkV1();
  case GetChunkVersion::V2:
    return GetChunkV2();
  case GetChunkVersion::AUTO:
    if (filter_options_.filter != nullptr) {
      return GetChunkV1();
    } else {
      return GetChunkV2();
    }
  default:
    return Status::Invalid("unsupport GetChunkVersion ", version);
  }
}

Result<std::shared_ptr<arrow::Table>>
VertexPropertyArrowChunkReader::GetLabelChunk() {
  FileType filetype = FileType::PARQUET;
  if (chunk_table_ == nullptr) {
    std::string path = prefix_ + vertex_info_->GetPrefix() + "labels/chunk" +
                       std::to_string(chunk_index_);
    GAR_ASSIGN_OR_RAISE(chunk_table_,
                        fs_->ReadFileToTable(path, filetype, filter_options_));
    // TODO(acezen): filter pushdown doesn't support cast schema now
    // if (schema_ != nullptr && filter_options_.filter == nullptr) {
    //   GAR_RETURN_NOT_OK(
    //       CastTableWithSchema(chunk_table_, schema_, &chunk_table_));
    // }
  }
  IdType row_offset = seek_id_ - chunk_index_ * vertex_info_->GetChunkSize();
  return chunk_table_->Slice(row_offset);
}

Status VertexPropertyArrowChunkReader::next_chunk() {
  if (++chunk_index_ >= chunk_num_) {
    return Status::IndexError(
        "vertex chunk index ", chunk_index_, " is out-of-bounds for vertex ",
        vertex_info_->GetType(), " chunk num ", chunk_num_);
  }
  seek_id_ = chunk_index_ * vertex_info_->GetChunkSize();
  chunk_table_.reset();

  return Status::OK();
}

void VertexPropertyArrowChunkReader::Filter(util::Filter filter) {
  filter_options_.filter = filter;
}

void VertexPropertyArrowChunkReader::Select(util::ColumnNames column_names) {
  filter_options_.columns = column_names;
}

Result<std::shared_ptr<VertexPropertyArrowChunkReader>>
VertexPropertyArrowChunkReader::Make(
    const std::shared_ptr<VertexInfo>& vertex_info,
    const std::shared_ptr<PropertyGroup>& property_group,
    const std::string& prefix, const util::FilterOptions& options) {
  return std::make_shared<VertexPropertyArrowChunkReader>(
      vertex_info, property_group, prefix, options);
}

Result<std::shared_ptr<VertexPropertyArrowChunkReader>>
VertexPropertyArrowChunkReader::Make(
    const std::shared_ptr<VertexInfo>& vertex_info,
    const std::shared_ptr<PropertyGroup>& property_group,
    const std::vector<std::string>& property_names, const std::string& prefix,
    const util::FilterOptions& options) {
  return std::make_shared<VertexPropertyArrowChunkReader>(
      vertex_info, property_group, property_names, prefix, options);
}

Result<std::shared_ptr<VertexPropertyArrowChunkReader>>
VertexPropertyArrowChunkReader::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
    const std::shared_ptr<PropertyGroup>& property_group,
    const util::FilterOptions& options) {
  auto vertex_info = graph_info->GetVertexInfo(type);
  if (!vertex_info) {
    return Status::KeyError("The vertex type ", type,
                            " doesn't exist in graph ", graph_info->GetName(),
                            ".");
  }
  return Make(vertex_info, property_group, graph_info->GetPrefix(), options);
}

Result<std::shared_ptr<VertexPropertyArrowChunkReader>>
VertexPropertyArrowChunkReader::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
    const std::string& property_name, const util::FilterOptions& options) {
  auto vertex_info = graph_info->GetVertexInfo(type);
  if (!vertex_info) {
    return Status::KeyError("The vertex type ", type,
                            " doesn't exist in graph ", graph_info->GetName(),
                            ".");
  }
  auto property_group = vertex_info->GetPropertyGroup(property_name);
  if (!property_group) {
    return Status::KeyError("The property ", property_name,
                            " doesn't exist in vertex type ", type, ".");
  }
  std::vector<std::string> property_names = {property_name};
  if (property_name != graphar::GeneralParams::kVertexIndexCol) {
    property_names.insert(property_names.begin(),
                          graphar::GeneralParams::kVertexIndexCol);
  }
  return Make(vertex_info, property_group, property_names,
              graph_info->GetPrefix(), options);
}

Result<std::shared_ptr<VertexPropertyArrowChunkReader>>
VertexPropertyArrowChunkReader::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
    const std::vector<std::string>& property_names_or_labels,
    const SelectType select_type, const util::FilterOptions& options) {
  switch (select_type) {
  case SelectType::LABELS:
    return MakeForLabels(graph_info, type, property_names_or_labels, options);
  case SelectType::PROPERTIES:
    return MakeForProperties(graph_info, type, property_names_or_labels,
                             options);
  }
}

Result<std::shared_ptr<VertexPropertyArrowChunkReader>>
VertexPropertyArrowChunkReader::MakeForProperties(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
    const std::vector<std::string>& property_names,
    const util::FilterOptions& options) {
  auto vertex_info = graph_info->GetVertexInfo(type);
  if (!vertex_info) {
    return Status::KeyError("The vertex type ", type,
                            " doesn't exist in graph ", graph_info->GetName(),
                            ".");
  }
  if (property_names.empty()) {
    return Status::Invalid("The property names cannot be empty.");
  }
  bool hasIndexCol = false;
  std::vector<std::string> property_names_mutable = property_names;
  if (property_names_mutable[property_names_mutable.size() - 1] ==
      graphar::GeneralParams::kVertexIndexCol) {
    hasIndexCol = true;
    std::iter_swap(property_names_mutable.begin(),
                   property_names_mutable.end() - 1);
  }
  auto property_group = vertex_info->GetPropertyGroup(
      property_names_mutable[property_names_mutable.size() - 1]);
  if (!property_group) {
    return Status::KeyError(
        "The property ",
        property_names_mutable[property_names_mutable.size() - 1],
        " doesn't exist in vertex type ", type, ".");
  }
  for (int i = 0; i < property_names_mutable.size() - 1; i++) {
    if (property_names_mutable[i] == graphar::GeneralParams::kVertexIndexCol) {
      hasIndexCol = true;
    }
    auto pg = vertex_info->GetPropertyGroup(property_names_mutable[i]);
    if (!pg) {
      return Status::KeyError("The property ", property_names_mutable[i],
                              " doesn't exist in vertex type ", type, ".");
    }
    if (pg != property_group) {
      return Status::Invalid(
          "The properties ", property_names_mutable[i], " and ",
          property_names_mutable[property_names_mutable.size() - 1],
          " are not in the same property group, please use Make with "
          "property_group instead.");
    }
  }
  if (!hasIndexCol) {
    property_names_mutable.insert(property_names_mutable.begin(),
                                  graphar::GeneralParams::kVertexIndexCol);
  }
  return Make(vertex_info, property_group, property_names_mutable,
              graph_info->GetPrefix(), options);
}
Result<std::shared_ptr<VertexPropertyArrowChunkReader>>
VertexPropertyArrowChunkReader::Make(
    const std::shared_ptr<VertexInfo>& vertex_info,
    const std::vector<std::string>& labels, const std::string& prefix,
    const util::FilterOptions& options) {
  return std::make_shared<VertexPropertyArrowChunkReader>(vertex_info, labels,
                                                          prefix, options);
}

Result<std::shared_ptr<VertexPropertyArrowChunkReader>>
VertexPropertyArrowChunkReader::MakeForLabels(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
    const std::vector<std::string>& labels,
    const util::FilterOptions& options) {
  auto vertex_info = graph_info->GetVertexInfo(type);
  if (!vertex_info) {
    return Status::KeyError("The vertex type ", type,
                            " doesn't exist in graph ", graph_info->GetName(),
                            ".");
  }
  return Make(vertex_info, labels, graph_info->GetPrefix(), options);
}

AdjListArrowChunkReader::AdjListArrowChunkReader(
    const std::shared_ptr<EdgeInfo>& edge_info, AdjListType adj_list_type,
    const std::string& prefix)
    : edge_info_(edge_info),
      adj_list_type_(adj_list_type),
      prefix_(prefix),
      vertex_chunk_index_(0),
      chunk_index_(0),
      seek_offset_(0),
      chunk_table_(nullptr),
      chunk_num_(-1) /* -1 means uninitialized */ {
  GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &prefix_));
  GAR_ASSIGN_OR_RAISE_ERROR(auto adj_list_path_prefix,
                            edge_info->GetAdjListPathPrefix(adj_list_type));
  base_dir_ = prefix_ + adj_list_path_prefix;
  GAR_ASSIGN_OR_RAISE_ERROR(
      vertex_chunk_num_,
      util::GetVertexChunkNum(prefix_, edge_info_, adj_list_type_));
}

AdjListArrowChunkReader::AdjListArrowChunkReader(
    const AdjListArrowChunkReader& other)
    : edge_info_(other.edge_info_),
      adj_list_type_(other.adj_list_type_),
      prefix_(other.prefix_),
      vertex_chunk_index_(other.vertex_chunk_index_),
      chunk_index_(other.chunk_index_),
      seek_offset_(other.seek_offset_),
      chunk_table_(nullptr),
      vertex_chunk_num_(other.vertex_chunk_num_),
      chunk_num_(other.chunk_num_),
      base_dir_(other.base_dir_),
      fs_(other.fs_) {}

Status AdjListArrowChunkReader::seek_src(IdType id) {
  if (adj_list_type_ != AdjListType::unordered_by_source &&
      adj_list_type_ != AdjListType::ordered_by_source) {
    return Status::Invalid("The seek_src operation is invalid in edge ",
                           edge_info_->GetEdgeType(), " reader with ",
                           AdjListTypeToString(adj_list_type_), " type.");
  }

  IdType new_vertex_chunk_index = id / edge_info_->GetSrcChunkSize();
  if (new_vertex_chunk_index >= vertex_chunk_num_) {
    return Status::IndexError(
        "The source internal id ", id, " is out of range [0,",
        edge_info_->GetSrcChunkSize() * vertex_chunk_num_, ") of edge ",
        edge_info_->GetEdgeType(), " reader.");
  }
  if (chunk_num_ < 0 || vertex_chunk_index_ != new_vertex_chunk_index) {
    // initialize or update chunk_num_
    vertex_chunk_index_ = new_vertex_chunk_index;
    GAR_RETURN_NOT_OK(initOrUpdateEdgeChunkNum());
    chunk_table_.reset();
  }

  if (adj_list_type_ == AdjListType::unordered_by_source) {
    return seek(0);  // start from first chunk
  } else {
    GAR_ASSIGN_OR_RAISE(auto range,
                        util::GetAdjListOffsetOfVertex(edge_info_, prefix_,
                                                       adj_list_type_, id));
    return seek(range.first);
  }
  return Status::OK();
}

Status AdjListArrowChunkReader::seek_dst(IdType id) {
  if (adj_list_type_ != AdjListType::unordered_by_dest &&
      adj_list_type_ != AdjListType::ordered_by_dest) {
    return Status::Invalid("The seek_dst operation is invalid in edge ",
                           edge_info_->GetEdgeType(), " reader with ",
                           AdjListTypeToString(adj_list_type_), " type.");
  }

  IdType new_vertex_chunk_index = id / edge_info_->GetDstChunkSize();
  if (new_vertex_chunk_index >= vertex_chunk_num_) {
    return Status::IndexError(
        "The destination internal id ", id, " is out of range [0,",
        edge_info_->GetDstChunkSize() * vertex_chunk_num_, ") of edge ",
        edge_info_->GetEdgeType(), " reader.");
  }
  if (chunk_num_ < 0 || vertex_chunk_index_ != new_vertex_chunk_index) {
    // initialize or update chunk_num_
    vertex_chunk_index_ = new_vertex_chunk_index;
    GAR_RETURN_NOT_OK(initOrUpdateEdgeChunkNum());
    chunk_table_.reset();
  }

  if (adj_list_type_ == AdjListType::unordered_by_dest) {
    return seek(0);  // start from the first chunk
  } else {
    GAR_ASSIGN_OR_RAISE(auto range,
                        util::GetAdjListOffsetOfVertex(edge_info_, prefix_,
                                                       adj_list_type_, id));
    return seek(range.first);
  }
}

Status AdjListArrowChunkReader::seek(IdType offset) {
  seek_offset_ = offset;
  IdType pre_chunk_index = chunk_index_;
  chunk_index_ = offset / edge_info_->GetChunkSize();
  if (chunk_index_ != pre_chunk_index) {
    chunk_table_.reset();
  }
  if (chunk_num_ < 0) {
    // initialize chunk_num_
    GAR_RETURN_NOT_OK(initOrUpdateEdgeChunkNum());
  }
  if (chunk_index_ >= chunk_num_) {
    return Status::IndexError("The edge offset ", offset,
                              " is out of range [0,",
                              edge_info_->GetChunkSize() * chunk_num_,
                              "), edge type: ", edge_info_->GetEdgeType());
  }
  return Status::OK();
}

Result<std::shared_ptr<arrow::Table>> AdjListArrowChunkReader::GetChunk() {
  if (chunk_table_ == nullptr) {
    // check if the edge num of the current vertex chunk is 0
    GAR_ASSIGN_OR_RAISE(auto edge_num,
                        util::GetEdgeNum(prefix_, edge_info_, adj_list_type_,
                                         vertex_chunk_index_));
    if (edge_num == 0) {
      return nullptr;
    }
    GAR_ASSIGN_OR_RAISE(auto chunk_file_path,
                        edge_info_->GetAdjListFilePath(
                            vertex_chunk_index_, chunk_index_, adj_list_type_));
    std::string path = prefix_ + chunk_file_path;
    auto file_type = edge_info_->GetAdjacentList(adj_list_type_)->GetFileType();
    GAR_ASSIGN_OR_RAISE(chunk_table_, fs_->ReadFileToTable(path, file_type));
  }
  IdType row_offset = seek_offset_ - chunk_index_ * edge_info_->GetChunkSize();
  return chunk_table_->Slice(row_offset);
}

Status AdjListArrowChunkReader::next_chunk() {
  ++chunk_index_;
  if (chunk_num_ < 0) {
    // initialize chunk_num_
    GAR_RETURN_NOT_OK(initOrUpdateEdgeChunkNum());
  }
  while (chunk_index_ >= chunk_num_) {
    ++vertex_chunk_index_;
    if (vertex_chunk_index_ >= vertex_chunk_num_) {
      return Status::IndexError("vertex chunk index ", vertex_chunk_index_,
                                " is out-of-bounds for vertex chunk num ",
                                vertex_chunk_num_);
    }
    chunk_index_ = 0;
    GAR_RETURN_NOT_OK(initOrUpdateEdgeChunkNum());
  }
  seek_offset_ = chunk_index_ * edge_info_->GetChunkSize();
  chunk_table_.reset();
  return Status::OK();
}

Status AdjListArrowChunkReader::seek_chunk_index(IdType vertex_chunk_index,
                                                 IdType chunk_index) {
  if (chunk_num_ < 0 || vertex_chunk_index_ != vertex_chunk_index) {
    vertex_chunk_index_ = vertex_chunk_index;
    GAR_RETURN_NOT_OK(initOrUpdateEdgeChunkNum());
    chunk_table_.reset();
  }
  if (chunk_index_ != chunk_index) {
    chunk_index_ = chunk_index;
    seek_offset_ = chunk_index * edge_info_->GetChunkSize();
    chunk_table_.reset();
  }
  return Status::OK();
}

Result<IdType> AdjListArrowChunkReader::GetRowNumOfChunk() {
  if (chunk_table_ == nullptr) {
    GAR_ASSIGN_OR_RAISE(auto chunk_file_path,
                        edge_info_->GetAdjListFilePath(
                            vertex_chunk_index_, chunk_index_, adj_list_type_));
    std::string path = prefix_ + chunk_file_path;
    auto file_type = edge_info_->GetAdjacentList(adj_list_type_)->GetFileType();
    GAR_ASSIGN_OR_RAISE(chunk_table_, fs_->ReadFileToTable(path, file_type));
  }
  return chunk_table_->num_rows();
}

Result<std::shared_ptr<AdjListArrowChunkReader>> AdjListArrowChunkReader::Make(
    const std::shared_ptr<EdgeInfo>& edge_info, AdjListType adj_list_type,
    const std::string& prefix) {
  if (!edge_info->HasAdjacentListType(adj_list_type)) {
    return Status::KeyError(
        "The adjacent list type ", AdjListTypeToString(adj_list_type),
        " doesn't exist in edge ", edge_info->GetEdgeType(), ".");
  }
  return std::make_shared<AdjListArrowChunkReader>(edge_info, adj_list_type,
                                                   prefix);
}

Result<std::shared_ptr<AdjListArrowChunkReader>> AdjListArrowChunkReader::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
    const std::string& edge_type, const std::string& dst_type,
    AdjListType adj_list_type) {
  auto edge_info = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
  if (!edge_info) {
    return Status::KeyError("The edge ", src_type, " ", edge_type, " ",
                            dst_type, " doesn't exist.");
  }
  return Make(edge_info, adj_list_type, graph_info->GetPrefix());
}

Status AdjListArrowChunkReader::initOrUpdateEdgeChunkNum() {
  GAR_ASSIGN_OR_RAISE(chunk_num_,
                      util::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                            vertex_chunk_index_));
  return Status::OK();
}

AdjListOffsetArrowChunkReader::AdjListOffsetArrowChunkReader(
    const std::shared_ptr<EdgeInfo>& edge_info, AdjListType adj_list_type,
    const std::string& prefix)
    : edge_info_(std::move(edge_info)),
      adj_list_type_(adj_list_type),
      prefix_(prefix),
      chunk_index_(0),
      seek_id_(0),
      chunk_table_(nullptr) {
  std::string base_dir;
  GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &prefix_));
  GAR_ASSIGN_OR_RAISE_ERROR(auto dir_path,
                            edge_info->GetOffsetPathPrefix(adj_list_type));
  base_dir_ = prefix_ + dir_path;
  if (adj_list_type == AdjListType::ordered_by_source ||
      adj_list_type == AdjListType::ordered_by_dest) {
    GAR_ASSIGN_OR_RAISE_ERROR(
        vertex_chunk_num_,
        util::GetVertexChunkNum(prefix_, edge_info_, adj_list_type_));
    vertex_chunk_size_ = adj_list_type == AdjListType::ordered_by_source
                             ? edge_info_->GetSrcChunkSize()
                             : edge_info_->GetDstChunkSize();
  } else {
    std::string err_msg = "Invalid adj list type " +
                          std::string(AdjListTypeToString(adj_list_type)) +
                          " to construct AdjListOffsetReader.";
    throw std::runtime_error(err_msg);
  }
}

Status AdjListOffsetArrowChunkReader::seek(IdType id) {
  seek_id_ = id;
  IdType pre_chunk_index = chunk_index_;
  chunk_index_ = id / vertex_chunk_size_;
  if (chunk_index_ != pre_chunk_index) {
    chunk_table_.reset();
  }
  if (chunk_index_ >= vertex_chunk_num_) {
    return Status::IndexError("Internal vertex id ", id, "is out of range [0,",
                              vertex_chunk_num_ * vertex_chunk_size_,
                              "), of edge ", edge_info_->GetEdgeType(),
                              " of adj list type ",
                              AdjListTypeToString(adj_list_type_), ".");
  }
  return Status::OK();
}

Result<std::shared_ptr<arrow::Array>>
AdjListOffsetArrowChunkReader::GetChunk() {
  if (chunk_table_ == nullptr) {
    GAR_ASSIGN_OR_RAISE(
        auto chunk_file_path,
        edge_info_->GetAdjListOffsetFilePath(chunk_index_, adj_list_type_));
    std::string path = prefix_ + chunk_file_path;
    auto file_type = edge_info_->GetAdjacentList(adj_list_type_)->GetFileType();
    GAR_ASSIGN_OR_RAISE(chunk_table_, fs_->ReadFileToTable(path, file_type));
  }
  IdType row_offset = seek_id_ - chunk_index_ * vertex_chunk_size_;
  return chunk_table_->Slice(row_offset)->column(0)->chunk(0);
}

Status AdjListOffsetArrowChunkReader::next_chunk() {
  if (++chunk_index_ >= vertex_chunk_num_) {
    return Status::IndexError("vertex chunk index ", chunk_index_,
                              " is out-of-bounds for vertex chunk num ",
                              vertex_chunk_num_, " of edge ",
                              edge_info_->GetEdgeType(), " of adj list type ",
                              AdjListTypeToString(adj_list_type_), ".");
  }
  seek_id_ = chunk_index_ * vertex_chunk_size_;
  chunk_table_.reset();

  return Status::OK();
}

Result<std::shared_ptr<AdjListOffsetArrowChunkReader>>
AdjListOffsetArrowChunkReader::Make(const std::shared_ptr<EdgeInfo>& edge_info,
                                    AdjListType adj_list_type,
                                    const std::string& prefix) {
  if (!edge_info->HasAdjacentListType(adj_list_type)) {
    return Status::KeyError(
        "The adjacent list type ", AdjListTypeToString(adj_list_type),
        " doesn't exist in edge ", edge_info->GetEdgeType(), ".");
  }
  return std::make_shared<AdjListOffsetArrowChunkReader>(edge_info,
                                                         adj_list_type, prefix);
}

Result<std::shared_ptr<AdjListOffsetArrowChunkReader>>
AdjListOffsetArrowChunkReader::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
    const std::string& edge_type, const std::string& dst_type,
    AdjListType adj_list_type) {
  auto edge_info = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
  if (!edge_info) {
    return Status::KeyError("The edge ", src_type, " ", edge_type, " ",
                            dst_type, " doesn't exist.");
  }
  return Make(edge_info, adj_list_type, graph_info->GetPrefix());
}

AdjListPropertyArrowChunkReader::AdjListPropertyArrowChunkReader(
    const std::shared_ptr<EdgeInfo>& edge_info,
    const std::shared_ptr<PropertyGroup>& property_group,
    AdjListType adj_list_type, const std::string prefix,
    const util::FilterOptions& options)
    : edge_info_(std::move(edge_info)),
      property_group_(std::move(property_group)),
      adj_list_type_(adj_list_type),
      prefix_(prefix),
      vertex_chunk_index_(0),
      chunk_index_(0),
      seek_offset_(0),
      schema_(nullptr),
      chunk_table_(nullptr),
      filter_options_(options),
      chunk_num_(-1) /* -1 means uninitialized */ {
  GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &prefix_));
  GAR_ASSIGN_OR_RAISE_ERROR(
      auto pg_path_prefix,
      edge_info->GetPropertyGroupPathPrefix(property_group, adj_list_type));
  base_dir_ = prefix_ + pg_path_prefix;
  GAR_ASSIGN_OR_RAISE_ERROR(
      vertex_chunk_num_,
      util::GetVertexChunkNum(prefix_, edge_info_, adj_list_type_));
  GAR_ASSIGN_OR_RAISE_ERROR(schema_,
                            PropertyGroupToSchema(property_group, false));
}

AdjListPropertyArrowChunkReader::AdjListPropertyArrowChunkReader(
    const AdjListPropertyArrowChunkReader& other)
    : edge_info_(other.edge_info_),
      property_group_(other.property_group_),
      adj_list_type_(other.adj_list_type_),
      prefix_(other.prefix_),
      vertex_chunk_index_(other.vertex_chunk_index_),
      chunk_index_(other.chunk_index_),
      seek_offset_(other.seek_offset_),
      schema_(other.schema_),
      chunk_table_(nullptr),
      filter_options_(other.filter_options_),
      vertex_chunk_num_(other.vertex_chunk_num_),
      chunk_num_(other.chunk_num_),
      base_dir_(other.base_dir_),
      fs_(other.fs_) {}

Status AdjListPropertyArrowChunkReader::seek_src(IdType id) {
  if (adj_list_type_ != AdjListType::unordered_by_source &&
      adj_list_type_ != AdjListType::ordered_by_source) {
    return Status::Invalid("The seek_src operation is invalid in edge ",
                           edge_info_->GetEdgeType(), " reader with ",
                           AdjListTypeToString(adj_list_type_), " type.");
  }

  IdType new_vertex_chunk_index = id / edge_info_->GetSrcChunkSize();
  if (new_vertex_chunk_index >= vertex_chunk_num_) {
    return Status::IndexError(
        "The source internal id ", id, " is out of range [0,",
        edge_info_->GetSrcChunkSize() * vertex_chunk_num_, ") of edge ",
        edge_info_->GetEdgeType(), " reader.");
  }
  if (chunk_num_ < 0 || vertex_chunk_index_ != new_vertex_chunk_index) {
    vertex_chunk_index_ = new_vertex_chunk_index;
    GAR_RETURN_NOT_OK(initOrUpdateEdgeChunkNum());
    chunk_table_.reset();
  }

  if (adj_list_type_ == AdjListType::unordered_by_source) {
    return seek(0);  // start from first chunk
  } else {
    GAR_ASSIGN_OR_RAISE(auto range,
                        util::GetAdjListOffsetOfVertex(edge_info_, prefix_,
                                                       adj_list_type_, id));
    return seek(range.first);
  }
  return Status::OK();
}

Status AdjListPropertyArrowChunkReader::seek_dst(IdType id) {
  if (adj_list_type_ != AdjListType::unordered_by_dest &&
      adj_list_type_ != AdjListType::ordered_by_dest) {
    return Status::Invalid("The seek_dst operation is invalid in edge ",
                           edge_info_->GetEdgeType(), " reader with ",
                           AdjListTypeToString(adj_list_type_), " type.");
  }

  IdType new_vertex_chunk_index = id / edge_info_->GetDstChunkSize();
  if (new_vertex_chunk_index >= vertex_chunk_num_) {
    return Status::IndexError(
        "The destination internal id ", id, " is out of range [0,",
        edge_info_->GetDstChunkSize() * vertex_chunk_num_, ") of edge ",
        edge_info_->GetEdgeType(), " reader.");
  }
  if (chunk_num_ < 0 || vertex_chunk_index_ != new_vertex_chunk_index) {
    vertex_chunk_index_ = new_vertex_chunk_index;
    GAR_RETURN_NOT_OK(initOrUpdateEdgeChunkNum());
    chunk_table_.reset();
  }

  if (adj_list_type_ == AdjListType::unordered_by_dest) {
    return seek(0);  // start from the first chunk
  } else {
    GAR_ASSIGN_OR_RAISE(auto range,
                        util::GetAdjListOffsetOfVertex(edge_info_, prefix_,
                                                       adj_list_type_, id));
    return seek(range.first);
  }
}

Status AdjListPropertyArrowChunkReader::seek(IdType offset) {
  IdType pre_chunk_index = chunk_index_;
  seek_offset_ = offset;
  chunk_index_ = offset / edge_info_->GetChunkSize();
  if (chunk_index_ != pre_chunk_index) {
    chunk_table_.reset();
  }
  if (chunk_num_ < 0) {
    // initialize chunk_num_
    GAR_RETURN_NOT_OK(initOrUpdateEdgeChunkNum());
  }
  if (chunk_index_ >= chunk_num_) {
    return Status::IndexError("The edge offset ", offset,
                              " is out of range [0,",
                              edge_info_->GetChunkSize() * chunk_num_,
                              "), edge type: ", edge_info_->GetEdgeType());
  }
  return Status::OK();
}

Result<std::shared_ptr<arrow::Table>>
AdjListPropertyArrowChunkReader::GetChunk() {
  GAR_RETURN_NOT_OK(util::CheckFilterOptions(filter_options_, property_group_));
  if (chunk_table_ == nullptr) {
    // check if the edge num of the current vertex chunk is 0
    GAR_ASSIGN_OR_RAISE(auto edge_num,
                        util::GetEdgeNum(prefix_, edge_info_, adj_list_type_,
                                         vertex_chunk_index_));
    if (edge_num == 0) {
      return nullptr;
    }
    GAR_ASSIGN_OR_RAISE(
        auto chunk_file_path,
        edge_info_->GetPropertyFilePath(property_group_, adj_list_type_,
                                        vertex_chunk_index_, chunk_index_));
    std::string path = prefix_ + chunk_file_path;
    GAR_ASSIGN_OR_RAISE(
        chunk_table_, fs_->ReadFileToTable(path, property_group_->GetFileType(),
                                           filter_options_));
    // TODO(acezen): filter pushdown doesn't support cast schema now
    if (schema_ != nullptr && filter_options_.filter == nullptr) {
      GAR_RETURN_NOT_OK(
          CastTableWithSchema(chunk_table_, schema_, &chunk_table_));
    }
  }
  IdType row_offset = seek_offset_ - chunk_index_ * edge_info_->GetChunkSize();
  return chunk_table_->Slice(row_offset);
}

Status AdjListPropertyArrowChunkReader::next_chunk() {
  ++chunk_index_;
  if (chunk_num_ < 0) {
    // initialize chunk_num_
    GAR_RETURN_NOT_OK(initOrUpdateEdgeChunkNum());
  }
  while (chunk_index_ >= chunk_num_) {
    ++vertex_chunk_index_;
    if (vertex_chunk_index_ >= vertex_chunk_num_) {
      return Status::IndexError("vertex chunk index ", vertex_chunk_index_,
                                " is out-of-bounds for vertex chunk num ",
                                vertex_chunk_num_, " of edge ",
                                edge_info_->GetEdgeType(), " of adj list type ",
                                AdjListTypeToString(adj_list_type_),
                                ", property group ", property_group_, ".");
    }
    chunk_index_ = 0;
    GAR_RETURN_NOT_OK(initOrUpdateEdgeChunkNum());
  }
  seek_offset_ = chunk_index_ * edge_info_->GetChunkSize();
  chunk_table_.reset();
  return Status::OK();
}

Status AdjListPropertyArrowChunkReader::seek_chunk_index(
    IdType vertex_chunk_index, IdType chunk_index) {
  if (chunk_num_ < 0 || vertex_chunk_index_ != vertex_chunk_index) {
    vertex_chunk_index_ = vertex_chunk_index;
    GAR_RETURN_NOT_OK(initOrUpdateEdgeChunkNum());
    chunk_table_.reset();
  }
  if (chunk_index_ != chunk_index) {
    chunk_index_ = chunk_index;
    seek_offset_ = chunk_index * edge_info_->GetChunkSize();
    chunk_table_.reset();
  }
  return Status::OK();
}

void AdjListPropertyArrowChunkReader::Filter(util::Filter filter) {
  filter_options_.filter = filter;
}

void AdjListPropertyArrowChunkReader::Select(util::ColumnNames column_names) {
  filter_options_.columns = column_names;
}

Result<std::shared_ptr<AdjListPropertyArrowChunkReader>>
AdjListPropertyArrowChunkReader::Make(
    const std::shared_ptr<EdgeInfo>& edge_info,
    const std::shared_ptr<PropertyGroup>& property_group,
    AdjListType adj_list_type, const std::string& prefix,
    const util::FilterOptions& options) {
  if (!edge_info->HasAdjacentListType(adj_list_type)) {
    return Status::KeyError(
        "The adjacent list type ", AdjListTypeToString(adj_list_type),
        " doesn't exist in edge ", edge_info->GetEdgeType(), ".");
  }
  return std::make_shared<AdjListPropertyArrowChunkReader>(
      edge_info, property_group, adj_list_type, prefix, options);
}

Result<std::shared_ptr<AdjListPropertyArrowChunkReader>>
AdjListPropertyArrowChunkReader::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
    const std::string& edge_type, const std::string& dst_type,
    const std::shared_ptr<PropertyGroup>& property_group,
    AdjListType adj_list_type, const util::FilterOptions& options) {
  auto edge_info = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
  if (!edge_info) {
    return Status::KeyError("The edge ", src_type, " ", edge_type, " ",
                            dst_type, " doesn't exist.");
  }
  return Make(edge_info, property_group, adj_list_type, graph_info->GetPrefix(),
              options);
}

Result<std::shared_ptr<AdjListPropertyArrowChunkReader>>
AdjListPropertyArrowChunkReader::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
    const std::string& edge_type, const std::string& dst_type,
    const std::string& property_name, AdjListType adj_list_type,
    const util::FilterOptions& options) {
  auto edge_info = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
  if (!edge_info) {
    return Status::KeyError("The edge ", src_type, " ", edge_type, " ",
                            dst_type, " doesn't exist.");
  }
  auto property_group = edge_info->GetPropertyGroup(property_name);
  if (!property_group) {
    return Status::KeyError("The property ", property_name,
                            " doesn't exist in edge ", src_type, " ", edge_type,
                            " ", dst_type, ".");
  }
  return Make(edge_info, property_group, adj_list_type, graph_info->GetPrefix(),
              options);
}

Status AdjListPropertyArrowChunkReader::initOrUpdateEdgeChunkNum() {
  GAR_ASSIGN_OR_RAISE(chunk_num_,
                      util::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                            vertex_chunk_index_));
  return Status::OK();
}

}  // namespace graphar
