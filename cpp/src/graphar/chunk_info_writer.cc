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

#include "graphar/chunk_info_writer.h"
#include "graphar/filesystem.h"
#include "graphar/graph_info.h"
#include "graphar/result.h"
#include "graphar/types.h"
#include "graphar/util.h"

namespace graphar {

VertexChunkInfoWriter::VertexChunkInfoWriter(
    const std::shared_ptr<VertexInfo>& vertex_info, const std::string& prefix,
    const ValidateLevel& validate_level)
    : vertex_info_(vertex_info),
      prefix_(prefix),
      validate_level_(validate_level) {
  if (validate_level_ == ValidateLevel::default_validate) {
    throw std::runtime_error(
        "default_validate is not allowed to be set as the global validate "
        "level for VertexPropertyWriter");
  }
  GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &prefix_));
}

// Check if the operation of copying a file as a chunk is allowed.
Status VertexChunkInfoWriter::validate(
    const std::shared_ptr<PropertyGroup>& property_group, IdType chunk_index,
    ValidateLevel validate_level) const {
  // use the writer's validate level
  if (validate_level == ValidateLevel::default_validate)
    validate_level = validate_level_;
  // no validate
  if (validate_level == ValidateLevel::no_validate)
    return Status::OK();
  // weak & strong validate
  if (!vertex_info_->HasPropertyGroup(property_group)) {
    return Status::KeyError("The property group", " does not exist in ",
                            vertex_info_->GetType(), " vertex info.");
  }
  if (chunk_index < 0) {
    return Status::IndexError("Negative chunk index ", chunk_index, ".");
  }
  return Status::OK();
}

Status VertexChunkInfoWriter::WriteChunk(
    const std::string& file_name,
    const std::shared_ptr<PropertyGroup>& property_group, IdType chunk_index,
    ValidateLevel validate_level) const {
  GAR_RETURN_NOT_OK(validate(property_group, chunk_index, validate_level));
  GAR_ASSIGN_OR_RAISE(auto suffix,
                      vertex_info_->GetFilePath(property_group, chunk_index));
  std::string path = prefix_ + suffix;
  return fs_->CopyFile(file_name, path);
}

EdgeChunkInfoWriter::EdgeChunkInfoWriter(
    const std::shared_ptr<EdgeInfo>& edge_info, const std::string& prefix,
    AdjListType adj_list_type, const ValidateLevel& validate_level)
    : edge_info_(edge_info),
      adj_list_type_(adj_list_type),
      validate_level_(validate_level) {
  if (validate_level_ == ValidateLevel::default_validate) {
    throw std::runtime_error(
        "default_validate is not allowed to be set as the global validate "
        "level for EdgeChunkWriter");
  }
  GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &prefix_));
  chunk_size_ = edge_info_->GetChunkSize();
  switch (adj_list_type) {
  case AdjListType::unordered_by_source:
    vertex_chunk_size_ = edge_info_->GetSrcChunkSize();
    break;
  case AdjListType::ordered_by_source:
    vertex_chunk_size_ = edge_info_->GetSrcChunkSize();
    break;
  case AdjListType::unordered_by_dest:
    vertex_chunk_size_ = edge_info_->GetDstChunkSize();
    break;
  case AdjListType::ordered_by_dest:
    vertex_chunk_size_ = edge_info_->GetDstChunkSize();
    break;
  default:
    vertex_chunk_size_ = edge_info_->GetSrcChunkSize();
  }
}

// Check if the operation of writing number or copying a file is allowed.
Status EdgeChunkInfoWriter::validate(IdType count_or_index1,
                                     IdType count_or_index2,
                                     ValidateLevel validate_level) const {
  // use the writer's validate level
  if (validate_level == ValidateLevel::default_validate)
    validate_level = validate_level_;
  // no validate
  if (validate_level == ValidateLevel::no_validate)
    return Status::OK();
  // weak & strong validate for adj list type
  if (!edge_info_->HasAdjacentListType(adj_list_type_)) {
    return Status::KeyError(
        "Adj list type ", AdjListTypeToString(adj_list_type_),
        " does not exist in the ", edge_info_->GetEdgeType(), " edge info.");
  }
  // weak & strong validate for count or index
  if (count_or_index1 < 0 || count_or_index2 < 0) {
    return Status::IndexError(
        "The count or index must be non-negative, but got ", count_or_index1,
        " and ", count_or_index2, ".");
  }
  return Status::OK();
}

// Check if the operation of copying a file as a property chunk is allowed.
Status EdgeChunkInfoWriter::validate(
    const std::shared_ptr<PropertyGroup>& property_group,
    IdType vertex_chunk_index, IdType chunk_index,
    ValidateLevel validate_level) const {
  // use the writer's validate level
  if (validate_level == ValidateLevel::default_validate)
    validate_level = validate_level_;
  // no validate
  if (validate_level == ValidateLevel::no_validate)
    return Status::OK();
  // validate for adj list type & index
  GAR_RETURN_NOT_OK(validate(vertex_chunk_index, chunk_index, validate_level));
  // weak & strong validate for property group
  if (!edge_info_->HasPropertyGroup(property_group)) {
    return Status::KeyError("Property group", " does not exist in the ",
                            edge_info_->GetEdgeType(), " edge info.");
  }
  return Status::OK();
}

Status EdgeChunkInfoWriter::WriteAdjListChunk(
    const std::string& file_name, IdType vertex_chunk_index, IdType chunk_index,
    ValidateLevel validate_level) const {
  GAR_RETURN_NOT_OK(validate(vertex_chunk_index, chunk_index, validate_level));
  GAR_ASSIGN_OR_RAISE(
      auto suffix, edge_info_->GetAdjListFilePath(vertex_chunk_index,
                                                  chunk_index, adj_list_type_));
  std::string path = prefix_ + suffix;
  return fs_->CopyFile(file_name, path);
}

Status EdgeChunkInfoWriter::WriteOffsetChunk(
    const std::string& file_name, IdType vertex_chunk_index,
    ValidateLevel validate_level) const {
  GAR_RETURN_NOT_OK(validate(vertex_chunk_index, 0, validate_level));
  GAR_ASSIGN_OR_RAISE(auto suffix, edge_info_->GetAdjListOffsetFilePath(
                                       vertex_chunk_index, adj_list_type_));
  std::string path = prefix_ + suffix;
  return fs_->CopyFile(file_name, path);
}

Status EdgeChunkInfoWriter::WritePropertyChunk(
    const std::string& file_name,
    const std::shared_ptr<PropertyGroup>& property_group,
    IdType vertex_chunk_index, IdType chunk_index,
    ValidateLevel validate_level) const {
  GAR_RETURN_NOT_OK(validate(property_group, vertex_chunk_index, chunk_index,
                             validate_level));
  GAR_ASSIGN_OR_RAISE(auto suffix, edge_info_->GetPropertyFilePath(
                                       property_group, adj_list_type_,
                                       vertex_chunk_index, chunk_index));
  std::string path = prefix_ + suffix;
  return fs_->CopyFile(file_name, path);
}
}  // namespace graphar
