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

// forward declaration
namespace arrow {
class Table;
}

namespace graphar {

/**
 * @brief The writer for vertex property group chunks.
 *
 * Notes: For each writing operation, a validate_level could be set, which will
 * be used to validate the data before writing. The validate_level could be:
 *
 * ValidateLevel::default_validate: to use the validate_level of the writer,
 * which set through the constructor or the SetValidateLevel method;
 *
 * ValidateLevel::no_validate: without validation;
 *
 * ValidateLevel::weak_validate: to validate if the vertex count or vertex chunk
 * index is non-negative, the property group exists and the size of input_table
 * is not larger than the vertex chunk size;
 *
 * ValidateLevel::strong_validate: besides weak_validate, also validate the
 * schema of input_table is consistent with that of property group; for writing
 * operations without input_table, such as writing vertices number or copying
 * file, the strong_validate is same as weak_validate.
 *
 */
class VertexPropertyWriter {
 public:
  /**
   * @brief Initialize the VertexPropertyWriter.
   *
   * @param vertex_info The vertex info that describes the vertex type.
   * @param prefix The absolute prefix.
   * @param validate_level The global validate level for the writer, with no
   * validate by default. It could be ValidateLevel::no_validate,
   * ValidateLevel::weak_validate or ValidateLevel::strong_validate, but could
   * not be ValidateLevel::default_validate.
   */
  explicit VertexPropertyWriter(
      const std::shared_ptr<VertexInfo>& vertex_info, const std::string& prefix,
      const std::shared_ptr<WriterOptions>& options =
          WriterOptions::DefaultWriterOption(),
      const ValidateLevel& validate_level = ValidateLevel::no_validate);

  /**
   * @brief Set the validate level.
   *
   * @param validate_level The validate level to set.
   */
  inline void SetValidateLevel(const ValidateLevel& validate_level) {
    if (validate_level == ValidateLevel::default_validate) {
      return;
    }
    validate_level_ = validate_level;
  }

  /**
   * @brief Get the validate level.
   *
   * @return The validate level of this writer.
   */
  inline ValidateLevel GetValidateLevel() const { return validate_level_; }

  /**
   * @brief Write the number of vertices into the file.
   *
   * @param count The number of vertices.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WriteVerticesNum(
      const IdType& count,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Validate and write a single property group for a
   * single vertex chunk.
   *
   * @param input_table The table containing data.
   * @param property_group The property group.
   * @param chunk_index The index of the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WriteChunk(
      const std::shared_ptr<arrow::Table>& input_table,
      const std::shared_ptr<PropertyGroup>& property_group, IdType chunk_index,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Write all labels of a single vertex chunk
   * to corresponding files.
   *
   * @param input_table The table containing data.
   * @param chunk_index The index of the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WriteLabelChunk(
      const std::shared_ptr<arrow::Table>& input_table, IdType chunk_index,
      FileType file_type,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Write all property groups of a single vertex chunk
   * to corresponding files.
   *
   * @param input_table The table containing data.
   * @param chunk_index The index of the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WriteChunk(
      const std::shared_ptr<arrow::Table>& input_table, IdType chunk_index,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Write a single property group for multiple vertex chunks
   * to corresponding files.
   *
   * @param input_table The table containing data.
   * @param property_group The property group.
   * @param start_chunk_index The start index of the vertex chunks.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WriteTable(
      const std::shared_ptr<arrow::Table>& input_table,
      const std::shared_ptr<PropertyGroup>& property_group,
      IdType start_chunk_index,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Write all property groups for multiple vertex chunks
   * to corresponding files.
   *
   * @param input_table The table containing data.
   * @param start_chunk_index The start index of the vertex chunks.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WriteTable(
      const std::shared_ptr<arrow::Table>& input_table,
      IdType start_chunk_index,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Write all labels for multiple vertex chunks
   * to corresponding files.
   *
   * @param input_table The table containing data.
   * @param start_chunk_index The start index of the vertex chunks.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WriteLabelTable(
      const std::shared_ptr<arrow::Table>& input_table,
      IdType start_chunk_index, FileType file_type,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Get label column from table to formulate label table
   * @param input_table The table containing data.
   * @param labels The labels.
   * @return The table only containing label columns
   * */
  Result<std::shared_ptr<arrow::Table>> GetLabelTable(
      const std::shared_ptr<arrow::Table>& input_table,
      const std::vector<std::string>& labels) const;

  Result<std::shared_ptr<arrow::Table>> GetLabelTableAndRandomlyAddLabels(
      const std::shared_ptr<arrow::Table>& input_table,
      const std::vector<std::string>& labels) const;

  /**
   * @brief Construct a VertexPropertyWriter from vertex info.
   *
   * @param vertex_info The vertex info that describes the vertex type.
   * @param prefix The absolute prefix.
   * @param validate_level The global validate level for the writer, default is
   * no_validate.
   * @param options Options for writing the table, such as compression.
   */
  static Result<std::shared_ptr<VertexPropertyWriter>> Make(
      const std::shared_ptr<VertexInfo>& vertex_info, const std::string& prefix,
      const std::shared_ptr<WriterOptions>& options,
      const ValidateLevel& validate_level = ValidateLevel::no_validate);

  static Result<std::shared_ptr<VertexPropertyWriter>> Make(
      const std::shared_ptr<VertexInfo>& vertex_info, const std::string& prefix,
      const ValidateLevel& validate_level = ValidateLevel::no_validate);

  /**
   * @brief Construct a VertexPropertyWriter from graph info and vertex type.
   *
   * @param graph_info The graph info that describes the graph.
   * @param type The vertex type.
   * @param validate_level The global validate level for the writer, default is
   * no_validate.
   * @param options Options for writing the table, such as compression.
   */
  static Result<std::shared_ptr<VertexPropertyWriter>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
      const std::shared_ptr<WriterOptions>& options,
      const ValidateLevel& validate_level = ValidateLevel::no_validate);

  static Result<std::shared_ptr<VertexPropertyWriter>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
      const ValidateLevel& validate_level = ValidateLevel::no_validate);

  void setWriterOptions(const std::shared_ptr<WriterOptions>& options) {
    options_ = options;
  }

  Result<std::shared_ptr<arrow::Table>> AddIndexColumn(
      const std::shared_ptr<arrow::Table>& table, IdType chunk_index,
      IdType chunk_size) const;

 private:
  /**
   * @brief Check if the operation of writing vertices number is allowed.
   *
   * @param count The number of vertices.
   * @param validate_level The validate level for this operation.
   * @return Status: ok or error.
   */
  Status validate(const IdType& count, ValidateLevel validate_level) const;

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

  /**
   * @brief Check if the operation of writing a table as a chunk is allowed.
   *
   * @param input_table The input table containing data.
   * @param property_group The property group to write.
   * @param chunk_index The index of the vertex chunk.
   * @param validate_level The validate level for this operation.
   * @return Status: ok or error.
   */
  Status validate(const std::shared_ptr<arrow::Table>& input_table,
                  const std::shared_ptr<PropertyGroup>& property_group,
                  IdType chunk_index, ValidateLevel validate_level) const;

 private:
  std::shared_ptr<VertexInfo> vertex_info_;
  std::string prefix_;
  std::shared_ptr<FileSystem> fs_;
  ValidateLevel validate_level_;
  std::shared_ptr<WriterOptions> options_;
};

/**
 * @brief The writer for edge (adj list, offset and property group) chunks.
 *
 * Notes: For each writing operation, a validate_level could be set, which will
 * be used to validate the data before writing. The validate_level could be:
 *
 * ValidateLevel::default_validate: to use the validate_level of the writer,
 * which set through the constructor or the SetValidateLevel method;
 *
 * ValidateLevel::no_validate: without validation;
 *
 * ValidateLevel::weak_validate: to validate if the vertex/edge count or
 * vertex/edge chunk index is non-negative, the adj_list type is valid, the
 * property group exists and the size of input_table is not larger than the
 * chunk size;
 *
 * ValidateLevel::strong_validate: besides weak_validate, also validate the
 * schema of input_table is consistent with that of property group; for writing
 * operations without input_table, such as writing vertices/edges number or
 * copying file, the strong_validate is same as weak_validate.
 *
 */
class EdgeChunkWriter {
 public:
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
  explicit EdgeChunkWriter(
      const std::shared_ptr<EdgeInfo>& edge_info, const std::string& prefix,
      AdjListType adj_list_type,
      const std::shared_ptr<WriterOptions>& options =
          WriterOptions::DefaultWriterOption(),
      const ValidateLevel& validate_level = ValidateLevel::no_validate);

  /**
   * @brief Set the validate level.
   *
   * @param validate_level The validate level to set.
   */
  void SetValidateLevel(const ValidateLevel& validate_level) {
    if (validate_level == ValidateLevel::default_validate) {
      return;
    }
    validate_level_ = validate_level;
  }

  /**
   * @brief Get the validate level.
   *
   * @return The validate level of this writer.
   */
  inline ValidateLevel GetValidateLevel() const { return validate_level_; }

  /**
   * @brief Write the number of edges into the file.
   *
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param count The number of edges.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WriteEdgesNum(
      IdType vertex_chunk_index, const IdType& count,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Write the number of vertices into the file.
   *
   * @param count The number of vertices.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WriteVerticesNum(
      const IdType& count,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Validate and write the offset chunk for a vertex chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WriteOffsetChunk(
      const std::shared_ptr<arrow::Table>& input_table,
      IdType vertex_chunk_index,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Validate and write the adj list chunk for an edge chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param chunk_index The index of the edge chunk inside the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WriteAdjListChunk(
      const std::shared_ptr<arrow::Table>& input_table,
      IdType vertex_chunk_index, IdType chunk_index,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Validate and write a single edge property group for an edge chunk.
   *
   * @param input_table The table containing data.
   * @param property_group The property group to write.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param chunk_index The index of the edge chunk inside the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WritePropertyChunk(
      const std::shared_ptr<arrow::Table>& input_table,
      const std::shared_ptr<PropertyGroup>& property_group,
      IdType vertex_chunk_index, IdType chunk_index,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Write all edge property groups for an edge chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param chunk_index The index of the edge chunk inside the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WritePropertyChunk(
      const std::shared_ptr<arrow::Table>& input_table,
      IdType vertex_chunk_index, IdType chunk_index,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Write the adj list and all property groups for an edge chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param chunk_index The index of the edge chunk inside the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WriteChunk(
      const std::shared_ptr<arrow::Table>& input_table,
      IdType vertex_chunk_index, IdType chunk_index,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Write the adj list chunks for the edges of a vertex chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param start_chunk_index The start index of the edge chunks inside
   * the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WriteAdjListTable(
      const std::shared_ptr<arrow::Table>& input_table,
      IdType vertex_chunk_index, IdType start_chunk_index = 0,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Write chunks of a single property group for the edges of a
   * vertex chunk.
   *
   * @param input_table The table containing data.
   * @param property_group The property group to write.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param start_chunk_index The start index of the edge chunks inside
   * the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WritePropertyTable(
      const std::shared_ptr<arrow::Table>& input_table,
      const std::shared_ptr<PropertyGroup>& property_group,
      IdType vertex_chunk_index, IdType start_chunk_index = 0,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Write chunks of all property groups for the edges of a vertex
   * chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param start_chunk_index The start index of the edge chunks inside
   * the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WritePropertyTable(
      const std::shared_ptr<arrow::Table>& input_table,
      IdType vertex_chunk_index, IdType start_chunk_index = 0,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Write chunks of the adj list and all property groups for the
   * edges of a vertex chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param start_chunk_index The start index of the edge chunks inside
   * the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status WriteTable(
      const std::shared_ptr<arrow::Table>& input_table,
      IdType vertex_chunk_index, IdType start_chunk_index = 0,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Sort the edges, and write the adj list chunks for the edges of a
   * vertex chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param start_chunk_index The start index of the edge chunks inside
   * the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status SortAndWriteAdjListTable(
      const std::shared_ptr<arrow::Table>& input_table,
      IdType vertex_chunk_index, IdType start_chunk_index = 0,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Sort the edges, and write chunks of a single property group for the
   * edges of a vertex chunk.
   *
   * @param input_table The table containing data.
   * @param property_group The property group to write.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param start_chunk_index The start index of the edge chunks inside
   * the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status SortAndWritePropertyTable(
      const std::shared_ptr<arrow::Table>& input_table,
      const std::shared_ptr<PropertyGroup>& property_group,
      IdType vertex_chunk_index, IdType start_chunk_index = 0,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Sort the edges, and write chunks of all property groups for the
   * edges of a vertex chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param start_chunk_index The start index of the edge chunks inside
   * the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status SortAndWritePropertyTable(
      const std::shared_ptr<arrow::Table>& input_table,
      IdType vertex_chunk_index, IdType start_chunk_index = 0,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Sort the edges, and write chunks of the adj list and all property
   * groups for the edges of a vertex chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param start_chunk_index The start index of the edge chunks inside
   * the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status SortAndWriteTable(
      const std::shared_ptr<arrow::Table>& input_table,
      IdType vertex_chunk_index, IdType start_chunk_index = 0,
      ValidateLevel validate_level = ValidateLevel::default_validate) const;

  /**
   * @brief Construct an EdgeChunkWriter from edge info.
   *
   * @param edge_info The edge info that describes the edge type.
   * @param prefix The absolute prefix.
   * @param adj_list_type The adj list type for the edges.
   * @param validate_level The global validate level for the writer, default is
   * no_validate.
   */
  static Result<std::shared_ptr<EdgeChunkWriter>> Make(
      const std::shared_ptr<EdgeInfo>& edge_info, const std::string& prefix,
      AdjListType adj_list_type, const std::shared_ptr<WriterOptions>& options,
      const ValidateLevel& validate_level = ValidateLevel::no_validate);

  static Result<std::shared_ptr<EdgeChunkWriter>> Make(
      const std::shared_ptr<EdgeInfo>& edge_info, const std::string& prefix,
      AdjListType adj_list_type,
      const ValidateLevel& validate_level = ValidateLevel::no_validate);

  /**
   * @brief Construct an EdgeChunkWriter from graph info and edge type.
   *
   * @param graph_info The graph info that describes the graph.
   * @param src_type The source vertex type.
   * @param edge_type The edge type.
   * @param dst_type The destination vertex type.
   * @param adj_list_type The adj list type for the edges.
   * @param validate_level The global validate level for the writer, default is
   * no_validate.
   */
  static Result<std::shared_ptr<EdgeChunkWriter>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
      const std::string& edge_type, const std::string& dst_type,
      AdjListType adj_list_type, const std::shared_ptr<WriterOptions>& options,
      const ValidateLevel& validate_level = ValidateLevel::no_validate);

  static Result<std::shared_ptr<EdgeChunkWriter>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
      const std::string& edge_type, const std::string& dst_type,
      AdjListType adj_list_type,
      const ValidateLevel& validate_level = ValidateLevel::no_validate);

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

  /**
   * @brief Check if the operation of writing a table as an offset chunk is
   * allowed.
   *
   * @param input_table The input table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param validate_level The validate level for this operation.
   * @return Status: ok or error.
   */
  Status validate(const std::shared_ptr<arrow::Table>& input_table,
                  IdType vertex_chunk_index,
                  ValidateLevel validate_level) const;

  /**
   * @brief Check if the operation of writing a table as an adj list chunk is
   * allowed.
   *
   * @param input_table The input table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param chunk_index The index of the edge chunk inside the vertex chunk.
   * @param validate_level The validate level for this operation.
   * @return Status: ok or error.
   */
  Status validate(const std::shared_ptr<arrow::Table>& input_table,
                  IdType vertex_chunk_index, IdType chunk_index,
                  ValidateLevel validate_level) const;

  /**
   * @brief Check if the operation of writing a table as a property chunk is
   * allowed.
   *
   * @param input_table The input table containing data.
   * @param property_group The property group to write.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param chunk_index The index of the edge chunk inside the vertex chunk.
   * @param validate_level The validate level for this operation.
   * @return Status: ok or error.
   */
  Status validate(const std::shared_ptr<arrow::Table>& input_table,
                  const std::shared_ptr<PropertyGroup>& property_group,
                  IdType vertex_chunk_index, IdType chunk_index,
                  ValidateLevel validate_level) const;

  /**
   * @brief Construct the offset table.
   *
   * @param input_table The table containing the edge data.
   * @param column_name The column name that records the offset data.
   * @param vertex_chunk_index The index of the vertex chunk.
   */
  Result<std::shared_ptr<arrow::Table>> getOffsetTable(
      const std::shared_ptr<arrow::Table>& input_table,
      const std::string& column_name, IdType vertex_chunk_index) const;

  /**
   * @brief Get the column name which is used to sort the edges.
   *
   * @param adj_list_type The adj list type of the edges.
   * @return The column name, which is GeneralParams::kSrcIndexCol by default.
   */
  static std::string getSortColumnName(AdjListType adj_list_type);

  /**
   * @brief Sort a table according to a specific column.
   *
   * @param input_table The table to sort.
   * @param column_name The column that is used to sort.
   * @return The sorted table.
   */
  static Result<std::shared_ptr<arrow::Table>> sortTable(
      const std::shared_ptr<arrow::Table>& input_table,
      const std::string& column_name);

 private:
  std::shared_ptr<EdgeInfo> edge_info_;
  IdType vertex_chunk_size_;
  IdType chunk_size_;
  AdjListType adj_list_type_;
  std::string prefix_;
  std::shared_ptr<FileSystem> fs_;
  ValidateLevel validate_level_;
  std::shared_ptr<WriterOptions> options_;
};

}  // namespace graphar
