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

#include "arrow/api.h"

#include "gar/graph_info.h"
#include "gar/reader/arrow_chunk_reader.h"
#include "gar/util/adj_list_type.h"
#include "gar/util/data_type.h"
#include "gar/util/filesystem.h"
#include "gar/util/reader_util.h"
#include "gar/util/result.h"
#include "gar/util/status.h"
#include "gar/util/util.h"

namespace GAR_NAMESPACE_INTERNAL {

VertexPropertyArrowChunkReader::VertexPropertyArrowChunkReader(
    const std::shared_ptr<VertexInfo>& vertex_info,
    const std::shared_ptr<PropertyGroup>& property_group,
    const std::string& prefix, const util::FilterOptions& options)
    : vertex_info_(std::move(vertex_info)),
      property_group_(std::move(property_group)),
      chunk_index_(0),
      seek_id_(0),
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
                              ") of vertex ", vertex_info_->GetLabel());
  }
  return Status::OK();
}

Result<std::shared_ptr<arrow::Table>>
VertexPropertyArrowChunkReader::GetChunk() {
  GAR_RETURN_NOT_OK(util::CheckFilterOptions(filter_options_, property_group_));
  if (chunk_table_ == nullptr) {
    GAR_ASSIGN_OR_RAISE(
        auto chunk_file_path,
        vertex_info_->GetFilePath(property_group_, chunk_index_));
    std::string path = prefix_ + chunk_file_path;
    GAR_ASSIGN_OR_RAISE(
        chunk_table_, fs_->ReadFileToTable(path, property_group_->GetFileType(),
                                           filter_options_));
  }
  IdType row_offset = seek_id_ - chunk_index_ * vertex_info_->GetChunkSize();
  return chunk_table_->Slice(row_offset);
}

Status VertexPropertyArrowChunkReader::next_chunk() {
  if (++chunk_index_ >= chunk_num_) {
    return Status::IndexError(
        "vertex chunk index ", chunk_index_, " is out-of-bounds for vertex ",
        vertex_info_->GetLabel(), " chunk num ", chunk_num_);
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
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& label,
    const std::shared_ptr<PropertyGroup>& property_group,
    const util::FilterOptions& options) {
  auto vertex_info = graph_info->GetVertexInfo(label);
  if (!vertex_info) {
    return Status::KeyError("The vertex type ", label,
                            " doesn't exist in graph ", graph_info->GetName(),
                            ".");
  }
  return Make(vertex_info, property_group, graph_info->GetPrefix(), options);
}

Result<std::shared_ptr<VertexPropertyArrowChunkReader>>
VertexPropertyArrowChunkReader::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& label,
    const std::string& property_name, const util::FilterOptions& options) {
  auto vertex_info = graph_info->GetVertexInfo(label);
  if (!vertex_info) {
    return Status::KeyError("The vertex type ", label,
                            " doesn't exist in graph ", graph_info->GetName(),
                            ".");
  }
  auto property_group = vertex_info->GetPropertyGroup(property_name);
  if (!property_group) {
    return Status::KeyError("The property ", property_name,
                            " doesn't exist in vertex type ", label, ".");
  }
  return Make(vertex_info, property_group, graph_info->GetPrefix(), options);
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
                           edge_info_->GetEdgeLabel(), " reader with ",
                           AdjListTypeToString(adj_list_type_), " type.");
  }

  IdType new_vertex_chunk_index = id / edge_info_->GetSrcChunkSize();
  if (new_vertex_chunk_index >= vertex_chunk_num_) {
    return Status::IndexError(
        "The source internal id ", id, " is out of range [0,",
        edge_info_->GetSrcChunkSize() * vertex_chunk_num_, ") of edge ",
        edge_info_->GetEdgeLabel(), " reader.");
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
                           edge_info_->GetEdgeLabel(), " reader with ",
                           AdjListTypeToString(adj_list_type_), " type.");
  }

  IdType new_vertex_chunk_index = id / edge_info_->GetDstChunkSize();
  if (new_vertex_chunk_index >= vertex_chunk_num_) {
    return Status::IndexError(
        "The destination internal id ", id, " is out of range [0,",
        edge_info_->GetDstChunkSize() * vertex_chunk_num_, ") of edge ",
        edge_info_->GetEdgeLabel(), " reader.");
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
                              "), edge label: ", edge_info_->GetEdgeLabel());
  }
  return Status::OK();
}

Result<std::shared_ptr<arrow::Table>> AdjListArrowChunkReader::GetChunk() {
  if (chunk_table_ == nullptr) {
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
        " doesn't exist in edge ", edge_info->GetEdgeLabel(), ".");
  }
  return std::make_shared<AdjListArrowChunkReader>(edge_info, adj_list_type,
                                                   prefix);
}

Result<std::shared_ptr<AdjListArrowChunkReader>> AdjListArrowChunkReader::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_label,
    const std::string& edge_label, const std::string& dst_label,
    AdjListType adj_list_type) {
  auto edge_info = graph_info->GetEdgeInfo(src_label, edge_label, dst_label);
  if (!edge_info) {
    return Status::KeyError("The edge ", src_label, " ", edge_label, " ",
                            dst_label, " doesn't exist.");
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
                              "), of edge ", edge_info_->GetEdgeLabel(),
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
                              edge_info_->GetEdgeLabel(), " of adj list type ",
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
        " doesn't exist in edge ", edge_info->GetEdgeLabel(), ".");
  }
  return std::make_shared<AdjListOffsetArrowChunkReader>(edge_info,
                                                         adj_list_type, prefix);
}

Result<std::shared_ptr<AdjListOffsetArrowChunkReader>>
AdjListOffsetArrowChunkReader::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_label,
    const std::string& edge_label, const std::string& dst_label,
    AdjListType adj_list_type) {
  auto edge_info = graph_info->GetEdgeInfo(src_label, edge_label, dst_label);
  if (!edge_info) {
    return Status::KeyError("The edge ", src_label, " ", edge_label, " ",
                            dst_label, " doesn't exist.");
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
                           edge_info_->GetEdgeLabel(), " reader with ",
                           AdjListTypeToString(adj_list_type_), " type.");
  }

  IdType new_vertex_chunk_index = id / edge_info_->GetSrcChunkSize();
  if (new_vertex_chunk_index >= vertex_chunk_num_) {
    return Status::IndexError(
        "The source internal id ", id, " is out of range [0,",
        edge_info_->GetSrcChunkSize() * vertex_chunk_num_, ") of edge ",
        edge_info_->GetEdgeLabel(), " reader.");
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
                           edge_info_->GetEdgeLabel(), " reader with ",
                           AdjListTypeToString(adj_list_type_), " type.");
  }

  IdType new_vertex_chunk_index = id / edge_info_->GetDstChunkSize();
  if (new_vertex_chunk_index >= vertex_chunk_num_) {
    return Status::IndexError(
        "The destination internal id ", id, " is out of range [0,",
        edge_info_->GetDstChunkSize() * vertex_chunk_num_, ") of edge ",
        edge_info_->GetEdgeLabel(), " reader.");
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
                              "), edge label: ", edge_info_->GetEdgeLabel());
  }
  return Status::OK();
}

Result<std::shared_ptr<arrow::Table>>
AdjListPropertyArrowChunkReader::GetChunk() {
  GAR_RETURN_NOT_OK(util::CheckFilterOptions(filter_options_, property_group_));
  if (chunk_table_ == nullptr) {
    GAR_ASSIGN_OR_RAISE(
        auto chunk_file_path,
        edge_info_->GetPropertyFilePath(property_group_, adj_list_type_,
                                        vertex_chunk_index_, chunk_index_));
    std::string path = prefix_ + chunk_file_path;
    GAR_ASSIGN_OR_RAISE(
        chunk_table_, fs_->ReadFileToTable(path, property_group_->GetFileType(),
                                           filter_options_));
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
      return Status::IndexError(
          "vertex chunk index ", vertex_chunk_index_,
          " is out-of-bounds for vertex chunk num ", vertex_chunk_num_,
          " of edge ", edge_info_->GetEdgeLabel(), " of adj list type ",
          AdjListTypeToString(adj_list_type_), ", property group ",
          property_group_, ".");
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
        " doesn't exist in edge ", edge_info->GetEdgeLabel(), ".");
  }
  return std::make_shared<AdjListPropertyArrowChunkReader>(
      edge_info, property_group, adj_list_type, prefix, options);
}

Result<std::shared_ptr<AdjListPropertyArrowChunkReader>>
AdjListPropertyArrowChunkReader::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_label,
    const std::string& edge_label, const std::string& dst_label,
    const std::shared_ptr<PropertyGroup>& property_group,
    AdjListType adj_list_type, const util::FilterOptions& options) {
  auto edge_info = graph_info->GetEdgeInfo(src_label, edge_label, dst_label);
  if (!edge_info) {
    return Status::KeyError("The edge ", src_label, " ", edge_label, " ",
                            dst_label, " doesn't exist.");
  }
  return Make(edge_info, property_group, adj_list_type, graph_info->GetPrefix(),
              options);
}

Result<std::shared_ptr<AdjListPropertyArrowChunkReader>>
AdjListPropertyArrowChunkReader::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_label,
    const std::string& edge_label, const std::string& dst_label,
    const std::string& property_name, AdjListType adj_list_type,
    const util::FilterOptions& options) {
  auto edge_info = graph_info->GetEdgeInfo(src_label, edge_label, dst_label);
  if (!edge_info) {
    return Status::KeyError("The edge ", src_label, " ", edge_label, " ",
                            dst_label, " doesn't exist.");
  }
  auto property_group = edge_info->GetPropertyGroup(property_name);
  if (!property_group) {
    return Status::KeyError("The property ", property_name,
                            " doesn't exist in edge ", src_label, " ",
                            edge_label, " ", dst_label, ".");
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

}  // namespace GAR_NAMESPACE_INTERNAL
