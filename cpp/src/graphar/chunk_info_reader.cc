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

#include <iostream>
#include <utility>

#include "graphar/chunk_info_reader.h"
#include "graphar/filesystem.h"
#include "graphar/graph_info.h"
#include "graphar/reader_util.h"
#include "graphar/result.h"
#include "graphar/types.h"
#include "graphar/util.h"

namespace graphar {

VertexPropertyChunkInfoReader::VertexPropertyChunkInfoReader(
    const std::shared_ptr<VertexInfo>& vertex_info,
    const std::shared_ptr<PropertyGroup>& property_group,
    const std::string& prefix)
    : vertex_info_(std::move(vertex_info)),
      property_group_(std::move(property_group)),
      prefix_(prefix),
      chunk_index_(0) {
  // init vertex chunk num
  std::string base_dir;
  GAR_ASSIGN_OR_RAISE_ERROR(auto fs,
                            FileSystemFromUriOrPath(prefix, &base_dir));
  GAR_ASSIGN_OR_RAISE_ERROR(auto pg_path_prefix,
                            vertex_info->GetPathPrefix(property_group));
  base_dir += pg_path_prefix;
  GAR_ASSIGN_OR_RAISE_ERROR(chunk_num_,
                            util::GetVertexChunkNum(prefix_, vertex_info_));
}

Status VertexPropertyChunkInfoReader::seek(IdType id) {
  chunk_index_ = id / vertex_info_->GetChunkSize();
  if (chunk_index_ >= chunk_num_) {
    return Status::IndexError("Internal vertex id ", id, " is out of range [0,",
                              chunk_num_ * vertex_info_->GetChunkSize(),
                              ") of vertex ", vertex_info_->GetType());
  }
  return Status::OK();
}

Result<std::string> VertexPropertyChunkInfoReader::GetChunk() const {
  GAR_ASSIGN_OR_RAISE(auto chunk_file_path,
                      vertex_info_->GetFilePath(property_group_, chunk_index_));
  return prefix_ + chunk_file_path;
}

Status VertexPropertyChunkInfoReader::next_chunk() {
  if (++chunk_index_ >= chunk_num_) {
    return Status::IndexError(
        "vertex chunk index ", chunk_index_, " is out-of-bounds for vertex ",
        vertex_info_->GetType(), " chunk num ", chunk_num_);
  }
  return Status::OK();
}

Result<std::shared_ptr<VertexPropertyChunkInfoReader>>
VertexPropertyChunkInfoReader::Make(
    const std::shared_ptr<VertexInfo>& vertex_info,
    const std::shared_ptr<PropertyGroup>& property_group,
    const std::string& prefix) {
  return std::make_shared<VertexPropertyChunkInfoReader>(
      vertex_info, property_group, prefix);
}

Result<std::shared_ptr<VertexPropertyChunkInfoReader>>
VertexPropertyChunkInfoReader::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
    const std::shared_ptr<PropertyGroup>& property_group) {
  auto vertex_info = graph_info->GetVertexInfo(type);
  if (!vertex_info) {
    return Status::KeyError("The vertex ", type, " doesn't exist.");
  }
  return Make(vertex_info, property_group, graph_info->GetPrefix());
}

Result<std::shared_ptr<VertexPropertyChunkInfoReader>>
VertexPropertyChunkInfoReader::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
    const std::string& property_name) {
  auto vertex_info = graph_info->GetVertexInfo(type);
  if (!vertex_info) {
    return Status::KeyError("The vertex ", type, " doesn't exist.");
  }
  auto property_group = vertex_info->GetPropertyGroup(property_name);
  if (!property_group) {
    return Status::KeyError("The property ", property_name,
                            " doesn't exist in vertex ", type, ".");
  }
  return Make(vertex_info, property_group, graph_info->GetPrefix());
}

AdjListChunkInfoReader::AdjListChunkInfoReader(
    const std::shared_ptr<EdgeInfo>& edge_info, AdjListType adj_list_type,
    const std::string& prefix)
    : edge_info_(std::move(edge_info)),
      adj_list_type_(adj_list_type),
      prefix_(prefix),
      vertex_chunk_index_(0),
      chunk_index_(0) {
  GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &base_dir_));
  GAR_ASSIGN_OR_RAISE_ERROR(auto adj_list_path_prefix,
                            edge_info->GetAdjListPathPrefix(adj_list_type));
  base_dir_ = prefix_ + adj_list_path_prefix;
  GAR_ASSIGN_OR_RAISE_ERROR(
      vertex_chunk_num_,
      util::GetVertexChunkNum(prefix_, edge_info_, adj_list_type_));
  GAR_ASSIGN_OR_RAISE_ERROR(
      chunk_num_, util::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                        vertex_chunk_index_));
}

Status AdjListChunkInfoReader::seek_src(IdType id) {
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
  if (vertex_chunk_index_ != new_vertex_chunk_index) {
    vertex_chunk_index_ = new_vertex_chunk_index;
    GAR_ASSIGN_OR_RAISE(
        chunk_num_, util::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                          vertex_chunk_index_));
  }

  if (adj_list_type_ == AdjListType::unordered_by_source) {
    return seek(0);  // start from first chunk
  } else {
    GAR_ASSIGN_OR_RAISE(auto offset_pair,
                        util::GetAdjListOffsetOfVertex(edge_info_, prefix_,
                                                       adj_list_type_, id));
    return seek(offset_pair.first);
  }
  return Status::OK();
}

Status AdjListChunkInfoReader::seek_dst(IdType id) {
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
  if (vertex_chunk_index_ != new_vertex_chunk_index) {
    vertex_chunk_index_ = new_vertex_chunk_index;
    GAR_ASSIGN_OR_RAISE(
        chunk_num_, util::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                          vertex_chunk_index_));
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

Status AdjListChunkInfoReader::seek(IdType index) {
  chunk_index_ = index / edge_info_->GetChunkSize();
  if (chunk_index_ >= chunk_num_) {
    return Status::IndexError("The edge offset ", index, " is out of range [0,",
                              edge_info_->GetChunkSize() * chunk_num_,
                              "), edge type: ", edge_info_->GetEdgeType());
  }
  return Status::OK();
}

Result<std::string> AdjListChunkInfoReader::GetChunk() {
  GAR_ASSIGN_OR_RAISE(auto chunk_file_path,
                      edge_info_->GetAdjListFilePath(
                          vertex_chunk_index_, chunk_index_, adj_list_type_));
  return prefix_ + chunk_file_path;
}

Status AdjListChunkInfoReader::next_chunk() {
  ++chunk_index_;
  while (chunk_index_ >= chunk_num_) {
    ++vertex_chunk_index_;
    if (vertex_chunk_index_ >= vertex_chunk_num_) {
      return Status::IndexError("vertex chunk index ", vertex_chunk_index_,
                                " is out-of-bounds for vertex chunk num ",
                                vertex_chunk_num_);
    }
    chunk_index_ = 0;
    GAR_ASSIGN_OR_RAISE_ERROR(
        chunk_num_, util::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                          vertex_chunk_index_));
  }
  return Status::OK();
}

Result<std::shared_ptr<AdjListChunkInfoReader>> AdjListChunkInfoReader::Make(
    const std::shared_ptr<EdgeInfo>& edge_info, AdjListType adj_list_type,
    const std::string& prefix) {
  if (!edge_info->HasAdjacentListType(adj_list_type)) {
    return Status::KeyError(
        "The adjacent list type ", AdjListTypeToString(adj_list_type),
        " doesn't exist in edge ", edge_info->GetEdgeType(), ".");
  }
  return std::make_shared<AdjListChunkInfoReader>(edge_info, adj_list_type,
                                                  prefix);
}

Result<std::shared_ptr<AdjListChunkInfoReader>> AdjListChunkInfoReader::Make(
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

AdjListOffsetChunkInfoReader::AdjListOffsetChunkInfoReader(
    const std::shared_ptr<EdgeInfo>& edge_info, AdjListType adj_list_type,
    const std::string& prefix)
    : edge_info_(std::move(edge_info)),
      adj_list_type_(adj_list_type),
      prefix_(prefix),
      chunk_index_(0) {
  std::string base_dir;
  GAR_ASSIGN_OR_RAISE_ERROR(auto fs,
                            FileSystemFromUriOrPath(prefix, &base_dir));
  GAR_ASSIGN_OR_RAISE_ERROR(auto dir_path,
                            edge_info->GetOffsetPathPrefix(adj_list_type));
  base_dir = prefix_ + dir_path;
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

Status AdjListOffsetChunkInfoReader::seek(IdType id) {
  chunk_index_ = id / vertex_chunk_size_;
  if (chunk_index_ >= vertex_chunk_num_) {
    return Status::IndexError("Internal vertex id ", id, " is out of range [0,",
                              vertex_chunk_num_ * vertex_chunk_size_,
                              ") of vertex.");
  }
  return Status::OK();
}

Result<std::string> AdjListOffsetChunkInfoReader::GetChunk() const {
  GAR_ASSIGN_OR_RAISE(
      auto chunk_file_path,
      edge_info_->GetAdjListOffsetFilePath(chunk_index_, adj_list_type_));
  return prefix_ + chunk_file_path;
}

Status AdjListOffsetChunkInfoReader::next_chunk() {
  if (++chunk_index_ >= vertex_chunk_num_) {
    return Status::IndexError("vertex chunk index ", chunk_index_,
                              " is out-of-bounds for vertex, chunk_num ",
                              vertex_chunk_num_);
  }
  return Status::OK();
}

Result<std::shared_ptr<AdjListOffsetChunkInfoReader>>
AdjListOffsetChunkInfoReader::Make(const std::shared_ptr<EdgeInfo>& edge_info,
                                   AdjListType adj_list_type,
                                   const std::string& prefix) {
  if (!edge_info->HasAdjacentListType(adj_list_type)) {
    return Status::KeyError(
        "The adjacent list type ", AdjListTypeToString(adj_list_type),
        " doesn't exist in edge ", edge_info->GetEdgeType(), ".");
  }
  return std::make_shared<AdjListOffsetChunkInfoReader>(edge_info,
                                                        adj_list_type, prefix);
}

Result<std::shared_ptr<AdjListOffsetChunkInfoReader>>
AdjListOffsetChunkInfoReader::Make(const std::shared_ptr<GraphInfo>& graph_info,
                                   const std::string& src_type,
                                   const std::string& edge_type,
                                   const std::string& dst_type,
                                   AdjListType adj_list_type) {
  auto edge_info = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
  if (!edge_info) {
    return Status::KeyError("The edge ", src_type, " ", edge_type, " ",
                            dst_type, " doesn't exist.");
  }
  return Make(edge_info, adj_list_type, graph_info->GetPrefix());
}

AdjListPropertyChunkInfoReader::AdjListPropertyChunkInfoReader(
    const std::shared_ptr<EdgeInfo>& edge_info,
    const std::shared_ptr<PropertyGroup>& property_group,
    AdjListType adj_list_type, const std::string prefix)
    : edge_info_(std::move(edge_info)),
      property_group_(std::move(property_group)),
      adj_list_type_(adj_list_type),
      prefix_(prefix),
      vertex_chunk_index_(0),
      chunk_index_(0) {
  GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &base_dir_));
  GAR_ASSIGN_OR_RAISE_ERROR(
      auto pg_path_prefix,
      edge_info->GetPropertyGroupPathPrefix(property_group, adj_list_type));
  base_dir_ = prefix_ + pg_path_prefix;
  GAR_ASSIGN_OR_RAISE_ERROR(
      vertex_chunk_num_,
      util::GetVertexChunkNum(prefix_, edge_info_, adj_list_type_));
  GAR_ASSIGN_OR_RAISE_ERROR(
      chunk_num_, util::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                        vertex_chunk_index_));
}

Status AdjListPropertyChunkInfoReader::seek_src(IdType id) {
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
  if (vertex_chunk_index_ != new_vertex_chunk_index) {
    vertex_chunk_index_ = new_vertex_chunk_index;
    GAR_ASSIGN_OR_RAISE(
        chunk_num_, util::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                          vertex_chunk_index_));
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

Status AdjListPropertyChunkInfoReader::seek_dst(IdType id) {
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
  if (vertex_chunk_index_ != new_vertex_chunk_index) {
    vertex_chunk_index_ = new_vertex_chunk_index;
    GAR_ASSIGN_OR_RAISE(
        chunk_num_, util::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                          vertex_chunk_index_));
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

Status AdjListPropertyChunkInfoReader::seek(IdType offset) {
  chunk_index_ = offset / edge_info_->GetChunkSize();
  if (chunk_index_ >= chunk_num_) {
    return Status::IndexError("The edge offset ", offset,
                              " is out of range [0,",
                              edge_info_->GetChunkSize() * chunk_num_,
                              "), edge type: ", edge_info_->GetEdgeType());
  }
  return Status::OK();
}

Result<std::string> AdjListPropertyChunkInfoReader::GetChunk() const {
  GAR_ASSIGN_OR_RAISE(
      auto chunk_file_path,
      edge_info_->GetPropertyFilePath(property_group_, adj_list_type_,
                                      vertex_chunk_index_, chunk_index_));
  return prefix_ + chunk_file_path;
}

Status AdjListPropertyChunkInfoReader::next_chunk() {
  ++chunk_index_;
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
    GAR_ASSIGN_OR_RAISE_ERROR(
        chunk_num_, util::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                          vertex_chunk_index_));
  }
  return Status::OK();
}

Result<std::shared_ptr<AdjListPropertyChunkInfoReader>>
AdjListPropertyChunkInfoReader::Make(
    const std::shared_ptr<EdgeInfo>& edge_info,
    const std::shared_ptr<PropertyGroup>& property_group,
    AdjListType adj_list_type, const std::string& prefix) {
  if (!edge_info->HasAdjacentListType(adj_list_type)) {
    return Status::KeyError(
        "The adjacent list type ", AdjListTypeToString(adj_list_type),
        " doesn't exist in edge ", edge_info->GetEdgeType(), ".");
  }
  return std::make_shared<AdjListPropertyChunkInfoReader>(
      edge_info, property_group, adj_list_type, prefix);
}

Result<std::shared_ptr<AdjListPropertyChunkInfoReader>>
AdjListPropertyChunkInfoReader::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
    const std::string& edge_type, const std::string& dst_type,
    const std::shared_ptr<PropertyGroup>& property_group,
    AdjListType adj_list_type) {
  auto edge_info = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
  if (!edge_info) {
    return Status::KeyError("The edge ", src_type, " ", edge_type, " ",
                            dst_type, " doesn't exist.");
  }
  return Make(edge_info, property_group, adj_list_type,
              graph_info->GetPrefix());
}

Result<std::shared_ptr<AdjListPropertyChunkInfoReader>>
AdjListPropertyChunkInfoReader::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
    const std::string& edge_type, const std::string& dst_type,
    const std::string& property_name, AdjListType adj_list_type) {
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
  return Make(edge_info, property_group, adj_list_type,
              graph_info->GetPrefix());
}

}  // namespace graphar
