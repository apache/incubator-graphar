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

#include <iostream>

#include "gar/reader/chunk_info_reader.h"
#include "gar/utils/reader_utils.h"

namespace GAR_NAMESPACE_INTERNAL {

Status AdjListChunkInfoReader::seek_src(IdType id) noexcept {
  if (adj_list_type_ != AdjListType::unordered_by_source &&
      adj_list_type_ != AdjListType::ordered_by_source) {
    return Status::Invalid("The seek_src operation is invalid in edge ",
                           edge_info_.GetEdgeLabel(), " reader with ",
                           AdjListTypeToString(adj_list_type_), " type.");
  }

  IdType new_vertex_chunk_index = id / edge_info_.GetSrcChunkSize();
  if (new_vertex_chunk_index >= vertex_chunk_num_) {
    return Status::KeyError("No vertex with internal id ", id,
                            " exists in edge ", edge_info_.GetEdgeLabel(),
                            " reader.");
  }
  if (vertex_chunk_index_ != new_vertex_chunk_index) {
    vertex_chunk_index_ = new_vertex_chunk_index;
    GAR_ASSIGN_OR_RAISE(
        chunk_num_, utils::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                           vertex_chunk_index_));
  }

  if (adj_list_type_ == AdjListType::unordered_by_source) {
    return seek(0);  // start from first chunk
  } else {
    GAR_ASSIGN_OR_RAISE(auto offset_pair,
                        utils::GetAdjListOffsetOfVertex(edge_info_, prefix_,
                                                        adj_list_type_, id));
    return seek(offset_pair.first);
  }
  return Status::OK();
}

Status AdjListChunkInfoReader::seek_dst(IdType id) noexcept {
  if (adj_list_type_ != AdjListType::unordered_by_dest &&
      adj_list_type_ != AdjListType::ordered_by_dest) {
    return Status::Invalid("The seek_dst operation is invalid in edge ",
                           edge_info_.GetEdgeLabel(), " reader with ",
                           AdjListTypeToString(adj_list_type_), " type.");
  }

  IdType new_vertex_chunk_index = id / edge_info_.GetDstChunkSize();
  if (new_vertex_chunk_index >= vertex_chunk_num_) {
    return Status::KeyError("No vertex with internal id ", id,
                            " exists in edge ", edge_info_.GetEdgeLabel(),
                            " reader.");
  }
  if (vertex_chunk_index_ != new_vertex_chunk_index) {
    vertex_chunk_index_ = new_vertex_chunk_index;
    GAR_ASSIGN_OR_RAISE(
        chunk_num_, utils::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                           vertex_chunk_index_));
  }

  if (adj_list_type_ == AdjListType::unordered_by_dest) {
    return seek(0);  // start from the first chunk
  } else {
    GAR_ASSIGN_OR_RAISE(auto range,
                        utils::GetAdjListOffsetOfVertex(edge_info_, prefix_,
                                                        adj_list_type_, id));
    return seek(range.first);
  }
}

Status AdjListPropertyChunkInfoReader::seek_src(IdType id) noexcept {
  if (adj_list_type_ != AdjListType::unordered_by_source &&
      adj_list_type_ != AdjListType::ordered_by_source) {
    return Status::Invalid("The seek_src operation is invalid in edge ",
                           edge_info_.GetEdgeLabel(), " reader with ",
                           AdjListTypeToString(adj_list_type_), " type.");
  }

  IdType new_vertex_chunk_index = id / edge_info_.GetSrcChunkSize();
  if (new_vertex_chunk_index >= vertex_chunk_num_) {
    return Status::KeyError("No vertex with internal id ", id,
                            " exists in edge ", edge_info_.GetEdgeLabel(),
                            " reader.");
  }
  if (vertex_chunk_index_ != new_vertex_chunk_index) {
    vertex_chunk_index_ = new_vertex_chunk_index;
    GAR_ASSIGN_OR_RAISE(
        chunk_num_, utils::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                           vertex_chunk_index_));
  }
  if (adj_list_type_ == AdjListType::unordered_by_source) {
    return seek(0);  // start from first chunk
  } else {
    GAR_ASSIGN_OR_RAISE(auto range,
                        utils::GetAdjListOffsetOfVertex(edge_info_, prefix_,
                                                        adj_list_type_, id));
    return seek(range.first);
  }
  return Status::OK();
}

Status AdjListPropertyChunkInfoReader::seek_dst(IdType id) noexcept {
  if (adj_list_type_ != AdjListType::unordered_by_dest &&
      adj_list_type_ != AdjListType::ordered_by_dest) {
    return Status::Invalid("The seek_dst operation is invalid in edge ",
                           edge_info_.GetEdgeLabel(), " reader with ",
                           AdjListTypeToString(adj_list_type_), " type.");
  }

  IdType new_vertex_chunk_index = id / edge_info_.GetDstChunkSize();
  if (new_vertex_chunk_index >= vertex_chunk_num_) {
    return Status::KeyError("No vertex with internal id ", id,
                            " exists in edge ", edge_info_.GetEdgeLabel(),
                            " reader.");
  }
  if (vertex_chunk_index_ != new_vertex_chunk_index) {
    vertex_chunk_index_ = new_vertex_chunk_index;
    GAR_ASSIGN_OR_RAISE(
        chunk_num_, utils::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                           vertex_chunk_index_));
  }

  if (adj_list_type_ == AdjListType::unordered_by_dest) {
    return seek(0);  // start from the first chunk
  } else {
    GAR_ASSIGN_OR_RAISE(auto range,
                        utils::GetAdjListOffsetOfVertex(edge_info_, prefix_,
                                                        adj_list_type_, id));
    return seek(range.first);
  }
}

}  // namespace GAR_NAMESPACE_INTERNAL
