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

#include "arrow/api.h"

#include "gar/reader/arrow_chunk_reader.h"
#include "gar/utils/reader_utils.h"

namespace GAR_NAMESPACE_INTERNAL {

Result<std::shared_ptr<arrow::Table>>
VertexPropertyArrowChunkReader::GetChunk() noexcept {
  if (chunk_table_ == nullptr) {
    GAR_ASSIGN_OR_RAISE(
        auto chunk_file_path,
        vertex_info_.GetFilePath(property_group_, chunk_index_));
    std::string path = prefix_ + chunk_file_path;
    GAR_ASSIGN_OR_RAISE(
        chunk_table_, fs_->ReadFileToTable(path, property_group_.GetFileType(),
                                           filter_options_));
  }
  IdType row_offset = seek_id_ - chunk_index_ * vertex_info_.GetChunkSize();
  return chunk_table_->Slice(row_offset);
}

Result<std::pair<IdType, IdType>>
VertexPropertyArrowChunkReader::GetRange() noexcept {
  if (chunk_table_ == nullptr) {
    return Status::Invalid(
        "The chunk table is not initialized, please call "
        "GetChunk() first.");
  }
  IdType row_offset = seek_id_ - chunk_index_ * vertex_info_.GetChunkSize();
  return std::make_pair(seek_id_,
                        seek_id_ + chunk_table_->num_rows() - row_offset);
}

void VertexPropertyArrowChunkReader::Filter(utils::ExpressionPtr filter) {
  filter_options_.filter = filter;
}

void VertexPropertyArrowChunkReader::ClearFilter() {
  filter_options_.filter = nullptr;
}

void VertexPropertyArrowChunkReader::Project(utils::VectorPtr columns) {
  filter_options_.columns = columns;
}

void VertexPropertyArrowChunkReader::Project(const std::string& column) {
  Project({column});
}

void VertexPropertyArrowChunkReader::ClearProjection() {
  filter_options_.columns = nullptr;
}

Status AdjListArrowChunkReader::seek_src(IdType id) noexcept {
  if (adj_list_type_ != AdjListType::unordered_by_source &&
      adj_list_type_ != AdjListType::ordered_by_source) {
    return Status::Invalid("The seek_src operation is invalid in edge ",
                           edge_info_.GetEdgeLabel(), " reader with ",
                           AdjListTypeToString(adj_list_type_), " type.");
  }

  IdType new_vertex_chunk_index = id / edge_info_.GetSrcChunkSize();
  if (new_vertex_chunk_index >= vertex_chunk_num_) {
    return Status::IndexError(
        "The source internal id ", id, " is out of range [0,",
        edge_info_.GetSrcChunkSize() * vertex_chunk_num_, ") of edge ",
        edge_info_.GetEdgeLabel(), " reader.");
  }
  if (vertex_chunk_index_ != new_vertex_chunk_index) {
    vertex_chunk_index_ = new_vertex_chunk_index;
    GAR_ASSIGN_OR_RAISE(
        chunk_num_, utils::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                           vertex_chunk_index_));
    chunk_table_.reset();
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

Status AdjListArrowChunkReader::seek_dst(IdType id) noexcept {
  if (adj_list_type_ != AdjListType::unordered_by_dest &&
      adj_list_type_ != AdjListType::ordered_by_dest) {
    return Status::Invalid("The seek_dst operation is invalid in edge ",
                           edge_info_.GetEdgeLabel(), " reader with ",
                           AdjListTypeToString(adj_list_type_), " type.");
  }

  IdType new_vertex_chunk_index = id / edge_info_.GetDstChunkSize();
  if (new_vertex_chunk_index >= vertex_chunk_num_) {
    return Status::IndexError(
        "The destination internal id ", id, " is out of range [0,",
        edge_info_.GetDstChunkSize() * vertex_chunk_num_, ") of edge ",
        edge_info_.GetEdgeLabel(), " reader.");
  }
  if (vertex_chunk_index_ != new_vertex_chunk_index) {
    vertex_chunk_index_ = new_vertex_chunk_index;
    GAR_ASSIGN_OR_RAISE(
        chunk_num_, utils::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                           vertex_chunk_index_));
    chunk_table_.reset();
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

Result<std::shared_ptr<arrow::Table>>
AdjListArrowChunkReader::GetChunk() noexcept {
  if (chunk_table_ == nullptr) {
    GAR_ASSIGN_OR_RAISE(auto chunk_file_path,
                        edge_info_.GetAdjListFilePath(
                            vertex_chunk_index_, chunk_index_, adj_list_type_));
    std::string path = prefix_ + chunk_file_path;
    GAR_ASSIGN_OR_RAISE(auto file_type, edge_info_.GetFileType(adj_list_type_));
    GAR_ASSIGN_OR_RAISE(chunk_table_, fs_->ReadFileToTable(path, file_type));
  }
  IdType row_offset = seek_offset_ - chunk_index_ * edge_info_.GetChunkSize();
  return chunk_table_->Slice(row_offset);
}

Result<IdType> AdjListArrowChunkReader::GetRowNumOfChunk() noexcept {
  if (chunk_table_ == nullptr) {
    GAR_ASSIGN_OR_RAISE(auto chunk_file_path,
                        edge_info_.GetAdjListFilePath(
                            vertex_chunk_index_, chunk_index_, adj_list_type_));
    std::string path = prefix_ + chunk_file_path;
    GAR_ASSIGN_OR_RAISE(auto file_type, edge_info_.GetFileType(adj_list_type_));
    GAR_ASSIGN_OR_RAISE(chunk_table_, fs_->ReadFileToTable(path, file_type));
  }
  return chunk_table_->num_rows();
}

Status AdjListPropertyArrowChunkReader::seek_src(IdType id) noexcept {
  if (adj_list_type_ != AdjListType::unordered_by_source &&
      adj_list_type_ != AdjListType::ordered_by_source) {
    return Status::Invalid("The seek_src operation is invalid in edge ",
                           edge_info_.GetEdgeLabel(), " reader with ",
                           AdjListTypeToString(adj_list_type_), " type.");
  }

  IdType new_vertex_chunk_index = id / edge_info_.GetSrcChunkSize();
  if (new_vertex_chunk_index >= vertex_chunk_num_) {
    return Status::IndexError(
        "The source internal id ", id, " is out of range [0,",
        edge_info_.GetSrcChunkSize() * vertex_chunk_num_, ") of edge ",
        edge_info_.GetEdgeLabel(), " reader.");
  }
  if (vertex_chunk_index_ != new_vertex_chunk_index) {
    vertex_chunk_index_ = new_vertex_chunk_index;
    GAR_ASSIGN_OR_RAISE(
        chunk_num_, utils::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                           vertex_chunk_index_));
    chunk_table_.reset();
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

Status AdjListPropertyArrowChunkReader::seek_dst(IdType id) noexcept {
  if (adj_list_type_ != AdjListType::unordered_by_dest &&
      adj_list_type_ != AdjListType::ordered_by_dest) {
    return Status::Invalid("The seek_dst operation is invalid in edge ",
                           edge_info_.GetEdgeLabel(), " reader with ",
                           AdjListTypeToString(adj_list_type_), " type.");
  }

  IdType new_vertex_chunk_index = id / edge_info_.GetDstChunkSize();
  if (new_vertex_chunk_index >= vertex_chunk_num_) {
    return Status::IndexError(
        "The destination internal id ", id, " is out of range [0,",
        edge_info_.GetDstChunkSize() * vertex_chunk_num_, ") of edge ",
        edge_info_.GetEdgeLabel(), " reader.");
  }
  if (vertex_chunk_index_ != new_vertex_chunk_index) {
    vertex_chunk_index_ = new_vertex_chunk_index;
    GAR_ASSIGN_OR_RAISE(
        chunk_num_, utils::GetEdgeChunkNum(prefix_, edge_info_, adj_list_type_,
                                           vertex_chunk_index_));
    chunk_table_.reset();
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

Result<std::shared_ptr<arrow::Array>>
AdjListOffsetArrowChunkReader::GetChunk() noexcept {
  if (chunk_table_ == nullptr) {
    GAR_ASSIGN_OR_RAISE(
        auto chunk_file_path,
        edge_info_.GetAdjListOffsetFilePath(chunk_index_, adj_list_type_));
    std::string path = prefix_ + chunk_file_path;
    GAR_ASSIGN_OR_RAISE(auto file_type, edge_info_.GetFileType(adj_list_type_));
    GAR_ASSIGN_OR_RAISE(chunk_table_, fs_->ReadFileToTable(path, file_type));
  }
  IdType row_offset = seek_id_ - chunk_index_ * vertex_chunk_size_;
  return chunk_table_->Slice(row_offset)->column(0)->chunk(0);
}

Result<std::shared_ptr<arrow::Table>>
AdjListPropertyArrowChunkReader::GetChunk() noexcept {
  if (chunk_table_ == nullptr) {
    GAR_ASSIGN_OR_RAISE(
        auto chunk_file_path,
        edge_info_.GetPropertyFilePath(property_group_, adj_list_type_,
                                       vertex_chunk_index_, chunk_index_));
    std::string path = prefix_ + chunk_file_path;
    GAR_ASSIGN_OR_RAISE(
        chunk_table_, fs_->ReadFileToTable(path, property_group_.GetFileType(),
                                           filter_options_));
  }
  IdType row_offset = seek_offset_ - chunk_index_ * edge_info_.GetChunkSize();
  return chunk_table_->Slice(row_offset);
}

void AdjListPropertyArrowChunkReader::Filter(utils::ExpressionPtr filter) {
  filter_options_.filter = filter;
}

void AdjListPropertyArrowChunkReader::ClearFilter() {
  filter_options_.filter = nullptr;
}

void AdjListPropertyArrowChunkReader::Project(utils::VectorPtr columns) {
  filter_options_.columns = columns;
}

void AdjListPropertyArrowChunkReader::Project(const std::string& column) {
  Project({column});
}

void AdjListPropertyArrowChunkReader::ClearProjection() {
  filter_options_.columns = nullptr;
}

}  // namespace GAR_NAMESPACE_INTERNAL
