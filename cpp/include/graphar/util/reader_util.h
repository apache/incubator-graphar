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

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "graphar/fwd.h"

namespace graphar::util {
struct FilterOptions {
  // The row filter to apply to the table.
  Filter filter = nullptr;
  // The columns to include in the table. Select all columns by default.
  ColumnNames columns = std::nullopt;

  FilterOptions() {}
  FilterOptions(Filter filter, ColumnNames columns)
      : filter(filter), columns(columns) {}
};

Status CheckFilterOptions(
    const FilterOptions& filter_options,
    const std::shared_ptr<PropertyGroup>& property_group) noexcept;

Result<std::pair<IdType, IdType>> GetAdjListOffsetOfVertex(
    const std::shared_ptr<EdgeInfo>& edge_info, const std::string& prefix,
    AdjListType adj_list_type, IdType vid) noexcept;

Result<IdType> GetVertexChunkNum(
    const std::string& prefix,
    const std::shared_ptr<VertexInfo>& vertex_info) noexcept;

Result<IdType> GetVertexNum(
    const std::string& prefix,
    const std::shared_ptr<VertexInfo>& vertex_info) noexcept;

Result<IdType> GetVertexChunkNum(const std::string& prefix,
                                 const std::shared_ptr<EdgeInfo>& edge_info,
                                 AdjListType adj_list_type) noexcept;

Result<IdType> GetVertexNum(const std::string& prefix,
                            const std::shared_ptr<EdgeInfo>& edge_info,
                            AdjListType adj_list_type) noexcept;

Result<IdType> GetEdgeChunkNum(const std::string& prefix,
                               const std::shared_ptr<EdgeInfo>& edge_info,
                               AdjListType adj_list_type,
                               IdType vertex_chunk_index) noexcept;

Result<IdType> GetEdgeNum(const std::string& prefix,
                          const std::shared_ptr<EdgeInfo>& edge_info,
                          AdjListType adj_list_type,
                          IdType vertex_chunk_index) noexcept;

}  // namespace graphar::util
