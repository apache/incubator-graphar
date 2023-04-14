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

#ifndef GAR_WRITER_ARROW_CHUNK_WRITER_H_
#define GAR_WRITER_ARROW_CHUNK_WRITER_H_

#include <memory>
#include <string>
#include <vector>

#include "gar/graph_info.h"
#include "gar/utils/data_type.h"
#include "gar/utils/filesystem.h"
#include "gar/utils/general_params.h"
#include "gar/utils/result.h"
#include "gar/utils/status.h"
#include "gar/utils/utils.h"

// forward declaration
namespace arrow {
class Table;
}

namespace GAR_NAMESPACE_INTERNAL {

/**
 * @brief The level for validating writing operations.
 */
enum class ValidateLevel : char {
  default_validate = -1,
  no_validate = 0,
  weak_validate = 1,
  strong_validate = 2
};

/**
 * @brief The writer for vertex property group chunks.
 *
 */
class VertexPropertyWriter {
 public:
  /**
   * @brief Initialize the VertexPropertyWriter.
   *
   * @param vertex_info The vertex info that describes the vertex type.
   * @param prefix The absolute prefix.
   * @param validate_level The validate level, with no validate by default.
   */
  VertexPropertyWriter(
      const VertexInfo& vertex_info, const std::string& prefix,
      const ValidateLevel& validate_level = ValidateLevel::no_validate)
      : vertex_info_(vertex_info),
        prefix_(prefix),
        validate_level_(validate_level) {
    GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &prefix_));
  }

  /**
   * @brief Set the validate level.
   *
   * @param validate_level The validate level to set.
   */
  inline void SetValidateLevel(const ValidateLevel& validate_level) {
    validate_level_ = validate_level;
  }

  /**
   * @brief Get the validate level.
   *
   * @return The validate level of this writer.
   */
  inline ValidateLevel GetValidateLevel() const { return validate_level_; }

  /**
   * @brief Check if the write opeartion is allowed.
   *
   * @param input_table The input table containing data.
   * @param property_group The property group to write.
   * @param chunk_index The index of the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status Validate(const std::shared_ptr<arrow::Table>& input_table,
                  const PropertyGroup& property_group, IdType chunk_index,
                  ValidateLevel validate_level =
                      ValidateLevel::default_validate) const noexcept;

  /**
   * @brief Write the number of vertices into the file.
   *
   * @param count The number of vertices.
   * @return Status: ok or error.
   */
  Status WriteVerticesNum(const IdType& count) const noexcept;

  /**
   * @brief Copy a file as a vertex property group chunk.
   *
   * @param file_name The file to copy.
   * @param property_group The property group.
   * @param chunk_index The index of the vertex chunk.
   * @return Status: ok or error.
   */
  Status WriteChunk(const std::string& file_name,
                    const PropertyGroup& property_group,
                    IdType chunk_index) const noexcept;
  /**
   * @brief Validate and write a single property group for a
   * single vertex chunk.
   *
   * @param input_table The table containing data.
   * @param property_group The property group.
   * @param chunk_index The index of the vertex chunk.
   * @return Status: ok or error.
   */
  Status WriteChunk(const std::shared_ptr<arrow::Table>& input_table,
                    const PropertyGroup& property_group,
                    IdType chunk_index) const noexcept;

  /**
   * @brief Write all property groups of a single vertex chunk
   * to corresponding files.
   *
   * @param input_table The table containing data.
   * @param chunk_index The index of the vertex chunk.
   * @return Status: ok or error.
   */
  Status WriteChunk(const std::shared_ptr<arrow::Table>& input_table,
                    IdType chunk_index) const noexcept;

  /**
   * @brief Write a single property group for multiple vertex chunks
   * to corresponding files.
   *
   * @param input_table The table containing data.
   * @param property_group The property group.
   * @param start_chunk_index The start index of the vertex chunks.
   * @return Status: ok or error.
   */
  Status WriteTable(const std::shared_ptr<arrow::Table>& input_table,
                    const PropertyGroup& property_group,
                    IdType start_chunk_index) const noexcept;

  /**
   * @brief Write all property groups for multiple vertex chunks
   * to corresponding files.
   *
   * @param input_table The table containing data.
   * @param start_chunk_index The start index of the vertex chunks.
   * @return Status: ok or error.
   */
  Status WriteTable(const std::shared_ptr<arrow::Table>& input_table,
                    IdType start_chunk_index) const noexcept;

 private:
  VertexInfo vertex_info_;
  std::string prefix_;
  std::shared_ptr<FileSystem> fs_;
  ValidateLevel validate_level_;
};

/**
 * @brief The writer for edge (adj list, offset and property group) chunks.
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
   * @param validate_level The validate level, with no validate by default.
   */
  EdgeChunkWriter(
      const EdgeInfo& edge_info, const std::string& prefix,
      const AdjListType adj_list_type = AdjListType::unordered_by_source,
      const ValidateLevel& validate_level = ValidateLevel::no_validate)
      : edge_info_(edge_info),
        adj_list_type_(adj_list_type),
        validate_level_(validate_level) {
    GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &prefix_));
    chunk_size_ = edge_info_.GetChunkSize();
    switch (adj_list_type) {
    case AdjListType::unordered_by_source:
      vertex_chunk_size_ = edge_info_.GetSrcChunkSize();
      break;
    case AdjListType::ordered_by_source:
      vertex_chunk_size_ = edge_info_.GetSrcChunkSize();
      break;
    case AdjListType::unordered_by_dest:
      vertex_chunk_size_ = edge_info_.GetDstChunkSize();
      break;
    case AdjListType::ordered_by_dest:
      vertex_chunk_size_ = edge_info_.GetDstChunkSize();
      break;
    default:
      vertex_chunk_size_ = edge_info_.GetSrcChunkSize();
    }
  }

  /**
   * @brief Set the validate level.
   *
   * @param validate_level The validate level to set.
   */
  void SetValidateLevel(const ValidateLevel& validate_level) {
    validate_level_ = validate_level;
  }

  /**
   * @brief Check if the writer operation for offset is allowed.
   *
   * @param input_table The input table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status Validate(const std::shared_ptr<arrow::Table>& input_table,
                  IdType vertex_chunk_index,
                  ValidateLevel validate_level =
                      ValidateLevel::default_validate) const noexcept;

  /**
   * @brief Check if the writer operation for adj list is allowed.
   *
   * @param input_table The input table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param chunk_index The index of the edge chunk inside the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status Validate(const std::shared_ptr<arrow::Table>& input_table,
                  IdType vertex_chunk_index, IdType chunk_index,
                  ValidateLevel validate_level =
                      ValidateLevel::default_validate) const noexcept;

  /**
   * @brief Check if the writer operation (for property group) is allowed.
   *
   * @param input_table The input table containing data.
   * @param property_group The property group to write.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param chunk_index The index of the edge chunk inside the vertex chunk.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or error.
   */
  Status Validate(const std::shared_ptr<arrow::Table>& input_table,
                  const PropertyGroup& property_group,
                  IdType vertex_chunk_index, IdType chunk_index,
                  ValidateLevel validate_level =
                      ValidateLevel::default_validate) const noexcept;

  /**
   * @brief Write the number of edges into the file.
   *
   * @param count The number of edges.
   * @return Status: ok or error.
   */
  Status WriteEdgesNum(IdType vertex_chunk_index, const IdType& count) const
      noexcept;

  /**
   * @brief Copy a file as a offset chunk.
   *
   * @param file_name The file to copy.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @return Status: ok or error.
   */
  Status WriteOffsetChunk(const std::string& file_name,
                          IdType vertex_chunk_index) const noexcept;

  /**
   * @brief Copy a file as an adj list chunk.
   *
   * @param file_name The file to copy.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param chunk_index The index of the edge chunk inside the vertex chunk.
   * @return Status: ok or error.
   */
  Status WriteAdjListChunk(const std::string& file_name,
                           IdType vertex_chunk_index, IdType chunk_index) const
      noexcept;

  /**
   * @brief Copy a file as an edge property group chunk.
   *
   * @param file_name The file to copy.
   * @param property_group The property group to write.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param chunk_index The index of the edge chunk inside the vertex chunk.
   * @return Status: ok or error.
   */
  Status WritePropertyChunk(const std::string& file_name,
                            const PropertyGroup& property_group,
                            IdType vertex_chunk_index, IdType chunk_index) const
      noexcept;

  /**
   * @brief Validate and write the offset chunk for a vertex chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @return Status: ok or error.
   */
  Status WriteOffsetChunk(const std::shared_ptr<arrow::Table>& input_table,
                          IdType vertex_chunk_index) const noexcept;

  /**
   * @brief Validate and write the adj list chunk for an edge chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param chunk_index The index of the edge chunk inside the vertex chunk.
   * @return Status: ok or error.
   */
  Status WriteAdjListChunk(const std::shared_ptr<arrow::Table>& input_table,
                           IdType vertex_chunk_index, IdType chunk_index) const
      noexcept;

  /**
   * @brief Validate and write a single edge property group for an edge chunk.
   *
   * @param input_table The table containing data.
   * @param property_group The property group to write.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param chunk_index The index of the edge chunk inside the vertex chunk.
   * @return Status: ok or error.
   */
  Status WritePropertyChunk(const std::shared_ptr<arrow::Table>& input_table,
                            const PropertyGroup& property_group,
                            IdType vertex_chunk_index, IdType chunk_index) const
      noexcept;

  /**
   * @brief Write all edge property groups for an edge chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param chunk_index The index of the edge chunk inside the vertex chunk.
   * @return Status: ok or error.
   */
  Status WritePropertyChunk(const std::shared_ptr<arrow::Table>& input_table,
                            IdType vertex_chunk_index, IdType chunk_index) const
      noexcept;

  /**
   * @brief Write the adj list and all property groups for an edge chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param chunk_index The index of the edge chunk inside the vertex chunk.
   * @return Status: ok or error.
   */
  Status WriteChunk(const std::shared_ptr<arrow::Table>& input_table,
                    IdType vertex_chunk_index, IdType chunk_index) const
      noexcept;

  /**
   * @brief Write the adj list chunks for the edges of a vertex chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param start_chunk_index The start index of the edge chunks inside
   * the vertex chunk.
   * @return Status: ok or error.
   */
  Status WriteAdjListTable(const std::shared_ptr<arrow::Table>& input_table,
                           IdType vertex_chunk_index,
                           IdType start_chunk_index = 0) const noexcept;

  /**
   * @brief Write chunks of a single property group for the edges of a
   * vertex chunk.
   *
   * @param input_table The table containing data.
   * @param property_group The property group to write.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param start_chunk_index The start index of the edge chunks inside
   * the vertex chunk.
   * @return Status: ok or error.
   */
  Status WritePropertyTable(const std::shared_ptr<arrow::Table>& input_table,
                            const PropertyGroup& property_group,
                            IdType vertex_chunk_index,
                            IdType start_chunk_index = 0) const noexcept;

  /**
   * @brief Write chunks of all property groups for the edges of a vertex
   * chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param start_chunk_index The start index of the edge chunks inside
   * the vertex chunk.
   * @return Status: ok or error.
   */
  Status WritePropertyTable(const std::shared_ptr<arrow::Table>& input_table,
                            IdType vertex_chunk_index,
                            IdType start_chunk_index = 0) const noexcept;

  /**
   * @brief Write chunks of the adj list and all property groups for the
   * edges of a vertex chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param start_chunk_index The start index of the edge chunks inside
   * the vertex chunk.
   * @return Status: ok or error.
   */
  Status WriteTable(const std::shared_ptr<arrow::Table>& input_table,
                    IdType vertex_chunk_index,
                    IdType start_chunk_index = 0) const noexcept;

  /**
   * @brief Sort the edges, and write the adj list chunks for the edges of a
   * vertex chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param start_chunk_index The start index of the edge chunks inside
   * the vertex chunk.
   * @return Status: ok or error.
   */
  Status SortAndWriteAdjListTable(
      const std::shared_ptr<arrow::Table>& input_table,
      IdType vertex_chunk_index, IdType start_chunk_index = 0) const noexcept;

  /**
   * @brief Sort the edges, and write chunks of a single property group for the
   * edges of a vertex chunk.
   *
   * @param input_table The table containing data.
   * @param property_group The property group to write.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param start_chunk_index The start index of the edge chunks inside
   * the vertex chunk.
   * @return Status: ok or error.
   */
  Status SortAndWritePropertyTable(
      const std::shared_ptr<arrow::Table>& input_table,
      const PropertyGroup& property_group, IdType vertex_chunk_index,
      IdType start_chunk_index = 0) const noexcept;

  /**
   * @brief Sort the edges, and write chunks of all property groups for the
   * edges of a vertex chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param start_chunk_index The start index of the edge chunks inside
   * the vertex chunk.
   * @return Status: ok or error.
   */
  Status SortAndWritePropertyTable(
      const std::shared_ptr<arrow::Table>& input_table,
      IdType vertex_chunk_index, IdType start_chunk_index = 0) const noexcept;

  /**
   * @brief Sort the edges, and write chunks of the adj list and all property
   * groups for the edges of a vertex chunk.
   *
   * @param input_table The table containing data.
   * @param vertex_chunk_index The index of the vertex chunk.
   * @param start_chunk_index The start index of the edge chunks inside
   * the vertex chunk.
   * @return Status: ok or error.
   */
  Status SortAndWriteTable(const std::shared_ptr<arrow::Table>& input_table,
                           IdType vertex_chunk_index,
                           IdType start_chunk_index = 0) const noexcept;

 private:
  /**
   * @brief Construct the offset table.
   *
   * @param input_table The table containing the edge data.
   * @param column_name The column name that records the offset data.
   * @param vertex_chunk_index The index of the vertex chunk.
   */
  Result<std::shared_ptr<arrow::Table>> getOffsetTable(
      const std::shared_ptr<arrow::Table>& input_table,
      const std::string& column_name, IdType vertex_chunk_index) const noexcept;

  /**
   * @brief Get the column name which is used to sort the edges.
   *
   * @param adj_list_type The adj list type of the edges.
   * @return The column name, which is GeneralParams::kSrcIndexCol by default.
   */
  static std::string getSortColumnName(AdjListType adj_list_type) {
    switch (adj_list_type) {
    case AdjListType::unordered_by_source:
      return GeneralParams::kSrcIndexCol;
    case AdjListType::ordered_by_source:
      return GeneralParams::kSrcIndexCol;
    case AdjListType::unordered_by_dest:
      return GeneralParams::kDstIndexCol;
    case AdjListType::ordered_by_dest:
      return GeneralParams::kDstIndexCol;
    default:
      return GeneralParams::kSrcIndexCol;
    }
    return GeneralParams::kSrcIndexCol;
  }

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
  EdgeInfo edge_info_;
  IdType vertex_chunk_size_;
  IdType chunk_size_;
  AdjListType adj_list_type_;
  std::string prefix_;
  std::shared_ptr<FileSystem> fs_;
  ValidateLevel validate_level_;
};

}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_WRITER_ARROW_CHUNK_WRITER_H_
