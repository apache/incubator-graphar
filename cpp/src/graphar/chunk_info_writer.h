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
#include <vector>

#include "graphar/fwd.h"
#include "graphar/writer_util.h"

namespace graphar {

class VertexChunkInfoWriter {
 public:
  /**
   * @brief Copy a file as a vertex property group chunk.
   *
   * @param file_name The file to copy.
   * @param property_group The property group.
   * @param chunk_index The index of the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */

  /**
   * @brief Initialize the VertexChunkWriter.
   *
   * @param vertex_info The vertex info that describes the vertex type.
   * @param prefix The absolute prefix.
   */
  explicit VertexChunkInfoWriter(
      const std::shared_ptr<VertexInfo>& vertex_info, const std::string& prefix,
      const ValidateLevel& validate_level = ValidateLevel::no_validate);

  Status WriteChunk(
      const std::string& file_name,
      const std::shared_ptr<PropertyGroup>& property_group, IdType chunk_index,
      ValidateLevel validate_level = ValidateLevel::no_validate) const;

 private:
  /**
   * @brief Check if the operation of copying a file as a chunk is allowed.
   *
   * @param property_group The property group to write.
   * @param chunk_index The index of the vertex chunk.
   * @param validate_level The validate level for this operation.
   * @return Status: ok or error.
   */
  Status validate(const std::shared_ptr<PropertyGroup>& property_group,
                  IdType chunk_index, ValidateLevel validate_level) const;

 private:
  std::shared_ptr<VertexInfo> vertex_info_;
  std::string prefix_;
  std::shared_ptr<FileSystem> fs_;
  ValidateLevel validate_level_;
};

class EdgeChunkInfoWriter {
  /**
   * @brief Initialize the EdgeChunkWriter.
   *
   * @param edge_info The edge info that describes the edge type.
   * @param prefix The absolute prefix.
   * @param adj_list_type The adj list type for the edges.
   * @param validate_level The global validate level for the writer, with no
   * validate by default. It could be ValidateLevel::no_validate,
   * ValidateLevel::weak_validate or ValidateLevel::strong_validate, but could
   * not be ValidateLevel::default_validate.
   */
  explicit EdgeChunkInfoWriter(
      const std::shared_ptr<EdgeInfo>& edge_info, const std::string& prefix,
      AdjListType adj_list_type,
      const ValidateLevel& validate_level = ValidateLevel::no_validate);

  /**
   * @brief Copy a file as an adj list chunk.
   *
   * @param file_name The file to copy.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param chunk_index The index of the edge chunk inside the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WriteAdjListChunk(
      const std::string& file_name, IdType vertex_chunk_index,
      IdType chunk_index,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Copy a file as a offset chunk.
   *
   * @param file_name The file to copy.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WriteOffsetChunk(
      const std::string& file_name, IdType vertex_chunk_index,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Copy a file as an edge property group chunk.
   *
   * @param file_name The file to copy.
   * @param property_group The property group to write.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param chunk_index The index of the edge chunk inside the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WritePropertyChunk(
      const std::string& file_name,
      const std::shared_ptr<PropertyGroup>& property_group,
      IdType vertex_chunk_index, IdType chunk_index,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

 private:
  /**
   * @brief Check if the operation of writing number or copying a file is
   * allowed.
   *
   * @param count_or_index1 The first count or index used by the operation.
   * @param count_or_index2 The second count or index used by the operation.
   * @param validate_level The validate level for this operation.
   * @return Status: ok or error.
   */
  Status validate(IdType count_or_index1, IdType count_or_index2,
                  ValidateLevel validate_level) const;

  /**
   * @brief Check if the operation of copying a file as a property chunk is
   * allowed.
   *
   * @param property_group The property group to write.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param chunk_index The index of the edge chunk inside the vertex chunk.
   * @param validate_level The validate level for this operation.
   * @return Status: ok or error.
   */
  Status validate(const std::shared_ptr<PropertyGroup>& property_group,
                  IdType vertex_chunk_index, IdType chunk_index,
                  ValidateLevel validate_level) const;

 private:
  std::shared_ptr<EdgeInfo> edge_info_;
  IdType vertex_chunk_size_;
  IdType chunk_size_;
  AdjListType adj_list_type_;
  std::string prefix_;
  std::shared_ptr<FileSystem> fs_;
  ValidateLevel validate_level_;
};
}  // namespace graphar
