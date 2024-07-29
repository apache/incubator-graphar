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

#ifdef ARROW_ORC
#include "arrow/adapters/orc/adapter.h"
#endif
#include "arrow/api.h"
#include "arrow/csv/api.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/api.h"
#include "parquet/arrow/reader.h"

#include "graphar/expression.h"
#include "graphar/filesystem.h"
#include "graphar/graph_info.h"
#include "graphar/reader_util.h"
#include "graphar/types.h"

namespace graphar::util {

/**
 * @brief Checks whether the property names in the FilterOptions match the
 * properties in the property group
 *
 * @param filter_options filter options
 * @param property_group property group
 * @return Status error if the property names in the FilterOptions do not match
 */
Status CheckFilterOptions(
    const FilterOptions& filter_options,
    const std::shared_ptr<PropertyGroup>& property_group) noexcept {
  if (filter_options.filter) {
    GAR_ASSIGN_OR_RAISE(auto filter, filter_options.filter->Evaluate());
    for (const auto& field : arrow::compute::FieldsInExpression(filter)) {
      auto property_name = *field.name();
      if (!property_group->HasProperty(property_name)) {
        return Status::Invalid(
            property_name, " in the filter does not match the property group: ",
            property_group);
      }
    }
  }
  if (filter_options.columns.has_value()) {
    for (const auto& col : filter_options.columns.value().get()) {
      if (!property_group->HasProperty(col)) {
        return Status::Invalid(
            col, " in the columns does not match the property group: ",
            property_group);
      }
    }
  }
  return Status::OK();
}

/**
 * @brief parse the vertex id to related adj list offset
 *
 * @param edge_info edge info
 * @param vertex_chunk_size vertex chunk size
 * @param prefix prefix of the payload files
 * @param adj_list_type adj list type to find the offset
 * @param vid vertex id
 *
 * @return tuple of <begin offset, end offset>
 */
Result<std::pair<IdType, IdType>> GetAdjListOffsetOfVertex(
    const std::shared_ptr<EdgeInfo>& edge_info, const std::string& prefix,
    AdjListType adj_list_type, IdType vid) noexcept {
  // get the adj list offset of id
  IdType vertex_chunk_size;
  if (adj_list_type == AdjListType::ordered_by_source) {
    vertex_chunk_size = edge_info->GetSrcChunkSize();
  } else if (adj_list_type == AdjListType::ordered_by_dest) {
    vertex_chunk_size = edge_info->GetDstChunkSize();
  } else {
    return Status::Invalid(
        "The adj list type has to be ordered_by_source or ordered_by_dest, but "
        "got ",
        std::string(AdjListTypeToString(adj_list_type)));
  }

  IdType offset_chunk_index = vid / vertex_chunk_size;
  IdType offset_in_file = vid % vertex_chunk_size;
  GAR_ASSIGN_OR_RAISE(
      auto offset_file_path,
      edge_info->GetAdjListOffsetFilePath(offset_chunk_index, adj_list_type));
  std::string out_prefix;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(prefix, &out_prefix));
  auto adjacent_list = edge_info->GetAdjacentList(adj_list_type);
  if (adjacent_list == nullptr) {
    return Status::Invalid("The adjacent list is not set for adj list type ",
                           std::string(AdjListTypeToString(adj_list_type)));
  }
  auto file_type = adjacent_list->GetFileType();
  std::string path = out_prefix + offset_file_path;
  GAR_ASSIGN_OR_RAISE(auto table, fs->ReadFileToTable(path, file_type));
  auto array = std::static_pointer_cast<arrow::Int64Array>(
      table->column(0)->Slice(offset_in_file, 2)->chunk(0));
  return std::make_pair(static_cast<IdType>(array->Value(0)),
                        static_cast<IdType>(array->Value(1)));
}

Result<IdType> GetVertexChunkNum(
    const std::string& prefix,
    const std::shared_ptr<VertexInfo>& vertex_info) noexcept {
  std::string out_prefix;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(prefix, &out_prefix));
  GAR_ASSIGN_OR_RAISE(auto vertex_num_file_suffix,
                      vertex_info->GetVerticesNumFilePath());
  std::string vertex_num_file_path = out_prefix + vertex_num_file_suffix;
  GAR_ASSIGN_OR_RAISE(auto vertex_num,
                      fs->ReadFileToValue<IdType>(vertex_num_file_path));
  return (vertex_num + vertex_info->GetChunkSize() - 1) /
         vertex_info->GetChunkSize();
}

Result<IdType> GetVertexNum(
    const std::string& prefix,
    const std::shared_ptr<VertexInfo>& vertex_info) noexcept {
  std::string out_prefix;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(prefix, &out_prefix));
  GAR_ASSIGN_OR_RAISE(auto vertex_num_file_suffix,
                      vertex_info->GetVerticesNumFilePath());
  std::string vertex_num_file_path = out_prefix + vertex_num_file_suffix;
  GAR_ASSIGN_OR_RAISE(auto vertex_num,
                      fs->ReadFileToValue<IdType>(vertex_num_file_path));
  return vertex_num;
}

Result<IdType> GetVertexChunkNum(const std::string& prefix,
                                 const std::shared_ptr<EdgeInfo>& edge_info,
                                 AdjListType adj_list_type) noexcept {
  std::string out_prefix;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(prefix, &out_prefix));
  GAR_ASSIGN_OR_RAISE(auto vertex_num_file_suffix,
                      edge_info->GetVerticesNumFilePath(adj_list_type));
  std::string vertex_num_file_path = out_prefix + vertex_num_file_suffix;
  GAR_ASSIGN_OR_RAISE(auto vertex_num,
                      fs->ReadFileToValue<IdType>(vertex_num_file_path));
  IdType chunk_size;
  if (adj_list_type == AdjListType::ordered_by_source ||
      adj_list_type == AdjListType::unordered_by_source) {
    chunk_size = edge_info->GetSrcChunkSize();
  } else {
    chunk_size = edge_info->GetDstChunkSize();
  }
  return (vertex_num + chunk_size - 1) / chunk_size;
}

Result<IdType> GetVertexNum(const std::string& prefix,
                            const std::shared_ptr<EdgeInfo>& edge_info,
                            AdjListType adj_list_type) noexcept {
  std::string out_prefix;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(prefix, &out_prefix));
  GAR_ASSIGN_OR_RAISE(auto vertex_num_file_suffix,
                      edge_info->GetVerticesNumFilePath(adj_list_type));
  std::string vertex_num_file_path = out_prefix + vertex_num_file_suffix;
  GAR_ASSIGN_OR_RAISE(auto vertex_num,
                      fs->ReadFileToValue<IdType>(vertex_num_file_path));
  return vertex_num;
}

Result<IdType> GetEdgeChunkNum(const std::string& prefix,
                               const std::shared_ptr<EdgeInfo>& edge_info,
                               AdjListType adj_list_type,
                               IdType vertex_chunk_index) noexcept {
  std::string out_prefix;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(prefix, &out_prefix));
  GAR_ASSIGN_OR_RAISE(
      auto edge_num_file_suffix,
      edge_info->GetEdgesNumFilePath(vertex_chunk_index, adj_list_type));
  std::string edge_num_file_path = out_prefix + edge_num_file_suffix;
  GAR_ASSIGN_OR_RAISE(auto edge_num,
                      fs->ReadFileToValue<IdType>(edge_num_file_path));
  return (edge_num + edge_info->GetChunkSize() - 1) / edge_info->GetChunkSize();
}

Result<IdType> GetEdgeNum(const std::string& prefix,
                          const std::shared_ptr<EdgeInfo>& edge_info,
                          AdjListType adj_list_type,
                          IdType vertex_chunk_index) noexcept {
  std::string out_prefix;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(prefix, &out_prefix));
  GAR_ASSIGN_OR_RAISE(
      auto edge_num_file_suffix,
      edge_info->GetEdgesNumFilePath(vertex_chunk_index, adj_list_type));
  std::string edge_num_file_path = out_prefix + edge_num_file_suffix;
  GAR_ASSIGN_OR_RAISE(auto edge_num,
                      fs->ReadFileToValue<IdType>(edge_num_file_path));
  return edge_num;
}

}  // namespace graphar::util
