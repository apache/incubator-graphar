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

#ifndef GAR_UTILS_READER_UTILS_H_
#define GAR_UTILS_READER_UTILS_H_

#include <string>
#include <utility>
#include <vector>

#include "gar/graph_info.h"
#include "gar/utils/expression.h"

namespace GAR_NAMESPACE_INTERNAL {

namespace utils {

using FilterPtr = Expression*;
using VectorPtr = std::vector<std::string>*;

struct FilterOptions {
  // The row filter to apply to the table.
  FilterPtr filter = nullptr;
  // The columns to include in the table. Select all columns by default.
  VectorPtr columns = nullptr;

  FilterOptions() {}
  FilterOptions(FilterPtr filter, VectorPtr columns)
      : filter(filter), columns(columns) {}
};

Result<std::pair<IdType, IdType>> GetAdjListOffsetOfVertex(
    const EdgeInfo& edge_info, const std::string& prefix,
    AdjListType adj_list_type, IdType vid) noexcept;

Result<IdType> GetVertexChunkNum(const std::string& prefix,
                                 const VertexInfo& vertex_info) noexcept;

Result<IdType> GetVertexNum(const std::string& prefix,
                            const VertexInfo& vertex_info) noexcept;

Result<IdType> GetVertexChunkNum(const std::string& prefix,
                                 const EdgeInfo& edge_info,
                                 AdjListType adj_list_type) noexcept;

Result<IdType> GetVertexNum(const std::string& prefix,
                            const EdgeInfo& edge_info,
                            AdjListType adj_list_type) noexcept;

Result<IdType> GetEdgeChunkNum(const std::string& prefix,
                               const EdgeInfo& edge_info,
                               AdjListType adj_list_type,
                               IdType vertex_chunk_index) noexcept;

Result<IdType> GetEdgeNum(const std::string& prefix, const EdgeInfo& edge_info,
                          AdjListType adj_list_type,
                          IdType vertex_chunk_index) noexcept;

}  // namespace utils
}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_UTILS_READER_UTILS_H_
