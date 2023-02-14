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

#include "arrow/adapters/orc/adapter.h"
#include "arrow/api.h"
#include "arrow/csv/api.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/api.h"
#include "parquet/arrow/reader.h"

#include "gar/graph_info.h"
#include "gar/utils/filesystem.h"
#include "gar/utils/reader_utils.h"

namespace GAR_NAMESPACE_INTERNAL {

namespace utils {
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
    const EdgeInfo& edge_info, const std::string& prefix,
    AdjListType adj_list_type, IdType vid) noexcept {
  // get the adj list offset of id
  IdType vertex_chunk_size;
  if (adj_list_type == AdjListType::ordered_by_source) {
    vertex_chunk_size = edge_info.GetSrcChunkSize();
  } else if (adj_list_type == AdjListType::ordered_by_dest) {
    vertex_chunk_size = edge_info.GetDstChunkSize();
  } else {
    return Status::Invalid("The adj list type is invalid.");
  }

  IdType offset_chunk_index = vid / vertex_chunk_size;
  IdType offset_in_file = vid % vertex_chunk_size;
  GAR_ASSIGN_OR_RAISE(
      auto offset_file_path,
      edge_info.GetAdjListOffsetFilePath(offset_chunk_index, adj_list_type));
  std::string out_prefix;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(prefix, &out_prefix));
  GAR_ASSIGN_OR_RAISE(auto file_type, edge_info.GetFileType(adj_list_type));
  std::string path = out_prefix + offset_file_path;
  GAR_ASSIGN_OR_RAISE(auto table, fs->ReadFileToTable(path, file_type));
  auto array = std::static_pointer_cast<arrow::Int64Array>(
      table->column(0)->Slice(offset_in_file, 2)->chunk(0));
  return std::make_pair(static_cast<IdType>(array->Value(0)),
                        static_cast<IdType>(array->Value(1)));
}

Result<IdType> GetVertexChunkNum(const std::string& prefix,
                                 const VertexInfo& vertex_info) noexcept {
  std::string out_prefix;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(prefix, &out_prefix));
  GAR_ASSIGN_OR_RAISE(auto vertex_num_file_suffix,
      vertex_info.GetFiGetVerticesNumFilePath());
  std::string vertex_num_file_path = out_prefix + vertex_num_file_suffix;
  IdType vertex_num = fs->ReadFileToValue<IdType>(vertex_num_file_path);
  return vertex_num + vertex_info.GetChunkSize() - 1 /
      vertex_info.GetChunkSize();
}

Result<IdType> GetEdgeChunkNum(const std::string& prefix,
                               const EdgeInfo& edge_info,
                               AdjListType adj_list_type,
                               IdType vertex_chunk_index) noexcept {
  std::string out_prefix;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(prefix, &out_prefix));
  GAR_ASSIGN_OR_RAISE(auto adj_prefix,
                      edge_info.GetAdjListPathPrefix(adj_list_type));
  std::string chunk_dir =
      out_prefix + adj_prefix + "part" + std::to_string(vertex_chunk_index);
  return fs->GetFileNumOfDir(chunk_dir);
}

}  // namespace utils

}  // namespace GAR_NAMESPACE_INTERNAL
