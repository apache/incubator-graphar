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

#ifndef GAR_WRITER_VERTICES_BUILDER_H_
#define GAR_WRITER_VERTICES_BUILDER_H_

#include <any>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "gar/writer/arrow_chunk_writer.h"

// forward declaration
namespace arrow {
class Array;
class Table;
}  // namespace arrow

namespace GAR_NAMESPACE_INTERNAL {
namespace builder {

/**
 * @brief Vertex is designed for constucting vertices builder.
 *
 */
class Vertex {
 public:
  Vertex() : empty_(true) {}

  /**
   * @brief Initialize the vertex with a given id.
   *
   * @param id The id of the vertex.
   */
  explicit Vertex(IdType id) : id_(id), empty_(false) {}

  /**
   * @brief Get id of the vertex.
   *
   * @return The id of the vertex.
   */
  inline IdType GetId() const noexcept { return id_; }

  /**
   * @brief Set id of the vertex.
   *
   * @param id The id of the vertex.
   */
  inline void SetId(IdType id) { id_ = id; }

  /**
   * @brief Check if the vertex is empty.
   *
   * @return true/false.
   */
  inline bool Empty() const noexcept { return empty_; }

  /**
   * @brief Add a property to the vertex.
   *
   * @param name The name of the property.
   * @param val The value of the property.
   */
  inline void AddProperty(const std::string& name, const std::any& val) {
    empty_ = false;
    properties_[name] = val;
  }

  /**
   * @brief Get a property of the vertex.
   *
   * @param property The name of the property.
   * @return The value of the property.
   */
  inline const std::any& GetProperty(const std::string& property) const {
    return properties_.at(property);
  }

  /**
   * @brief Get all properties of the vertex.
   *
   * @return The map containing all properties of the vertex.
   */
  inline const std::unordered_map<std::string, std::any>& GetProperties()
      const {
    return properties_;
  }

  /**
   * @brief Check if the vertex contains a property.
   *
   * @param property The name of the property.
   * @return true/false.
   */
  inline bool ContainProperty(const std::string& property) {
    return (properties_.find(property) != properties_.end());
  }

 private:
  IdType id_;
  bool empty_;
  std::unordered_map<std::string, std::any> properties_;
};

/**
 * @brief VertexBuilder is designed for building and writing a collection of
 * vertices.
 *
 */
class VerticesBuilder {
 public:
  /**
   * @brief Initialize the VerciesBuilder.
   *
   * @param vertex_info The vertex info that describes the vertex type.
   * @param prefix The absolute prefix.
   * @param start_vertex_index The start index of the vertices collection.
   * @param validate_level The validate level, with no validate by default.
   */
  explicit VerticesBuilder(
      const VertexInfo& vertex_info, const std::string& prefix,
      IdType start_vertex_index = 0,
      const ValidateLevel& validate_level = ValidateLevel::no_validate)
      : vertex_info_(vertex_info),
        prefix_(prefix),
        start_vertex_index_(start_vertex_index),
        validate_level_(validate_level) {
    vertices_.clear();
    num_vertices_ = 0;
    is_saved_ = false;
  }

  /**
   * @brief Clear the vertices in this VerciesBuilder.
   */
  inline void Clear() {
    vertices_.clear();
    num_vertices_ = 0;
    is_saved_ = false;
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
   * @brief Add a vertex with the given index.
   *
   * @param v The vertex to add.
   * @param index The given index, -1 means the next unused index.
   * @param validate_level The validate level for this operation,
   * which is the writer's validate level by default.
   * @return Status: ok or Status::InvalidOperation error.
   */
  Status AddVertex(
      Vertex& v, IdType index = -1,  // NOLINT
      ValidateLevel validate_level = ValidateLevel::default_validate) {
    // validate
    GAR_RETURN_NOT_OK(validate(v, index, validate_level));
    // add a vertex
    if (index == -1) {
      v.SetId(vertices_.size());
      vertices_.push_back(v);
    } else {
      v.SetId(index);
      if (index >= static_cast<IdType>(vertices_.size()))
        vertices_.resize(index + 1);
      vertices_[index] = v;
    }
    num_vertices_++;
    return Status::OK();
  }

  /**
   * @brief Get the current number of vertices in the collection.
   *
   * @return The current number of vertices in the collection.
   */
  IdType GetNum() const { return num_vertices_; }

  /**
   * @brief Dump the collection into files.
   *
   * @return Status: ok or error.
   */
  Status Dump() {
    // construct the writer
    VertexPropertyWriter writer(vertex_info_, prefix_, validate_level_);
    IdType start_chunk_index =
        start_vertex_index_ / vertex_info_.GetChunkSize();
    // convert to table
    GAR_ASSIGN_OR_RAISE(auto input_table, convertToTable());
    // write table
    GAR_RETURN_NOT_OK(writer.WriteTable(input_table, start_chunk_index));
    GAR_RETURN_NOT_OK(
        writer.WriteVerticesNum(num_vertices_ + start_vertex_index_));
    is_saved_ = true;
    vertices_.clear();
    return Status::OK();
  }

 private:
  /**
   * @brief Check if adding a vertex with the given index is allowed.
   *
   * @param v The vertex to add.
   * @param index The given index, -1 means the next unused index.
   * @param validate_level The validate level for this operation.
   * @return Status: ok or Status::InvalidOperation error.
   */
  Status validate(const Vertex& v, IdType index,
                  ValidateLevel validate_level) const;

  /**
   * @brief Construct an array for a given property.
   *
   * @param type The type of the property.
   * @param property_name The name of the property.
   * @param array The constructed array.
   * @return Status: ok or Status::TypeError error.
   */
  Status appendToArray(const DataType& type, const std::string& property_name,
                       std::shared_ptr<arrow::Array>& array);  // NOLINT

  /**
   * @brief Append values for a property into the given array.
   *
   * @tparam type The data type.
   * @param property_name The name of the property.
   * @param array The array to append.
   * @return Status: ok or Status::ArrowError error.
   */
  template <Type type>
  Status tryToAppend(const std::string& property_name,
                     std::shared_ptr<arrow::Array>& array);  // NOLINT

  /**
   * @brief Convert the vertices collection into an Arrow Table.
   */
  Result<std::shared_ptr<arrow::Table>> convertToTable();

 private:
  VertexInfo vertex_info_;
  std::string prefix_;
  std::vector<Vertex> vertices_;
  IdType start_vertex_index_;
  IdType num_vertices_;
  bool is_saved_;
  ValidateLevel validate_level_;
};

}  // namespace builder
}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_WRITER_VERTICES_BUILDER_H_
