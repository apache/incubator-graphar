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

#include <any>
#include <cassert>
#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "graphar/arrow/chunk_writer.h"
#include "graphar/fwd.h"
#include "graphar/graph_info.h"
#include "graphar/result.h"
#include "graphar/status.h"
#include "graphar/types.h"
#include "graphar/writer_util.h"

// forward declaration
namespace arrow {
class Array;
class Table;
}  // namespace arrow

namespace graphar::builder {

/**
 * @brief Vertex is designed for constructing vertices builder.
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
  // TODO(@acezen): Enable the property to be a vector(list).
  inline void AddProperty(const std::string& name, const std::any& val) {
    empty_ = false;
    properties_[name] = val;
  }

  inline void AddProperty(const Cardinality cardinality,
                          const std::string& name, const std::any& val) {
    if (cardinality == Cardinality::SINGLE) {
      cardinalities_[name] = Cardinality::SINGLE;
      AddProperty(name, val);
      return;
    }
    empty_ = false;
    if (cardinalities_.find(name) != cardinalities_.end()) {
      if (cardinalities_[name] != cardinality) {
        throw std::runtime_error("Cardinality mismatch for property: " + name);
      }
      auto property_value_list =
          std::any_cast<std::vector<std::any>>(properties_[name]);
      property_value_list.push_back(val);
      properties_[name] = property_value_list;
    } else {
      auto property_value_list = std::vector<std::any>();
      property_value_list.push_back(val);
      properties_[name] = property_value_list;
    }
    cardinalities_[name] = cardinality;
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

  inline bool IsMultiProperty(const std::string& property) const {
    return (cardinalities_.find(property) != cardinalities_.end() &&
            cardinalities_.at(property) != Cardinality::SINGLE);
  }

  template <typename T>
  Status ValidatePropertyType(const std::string& property,
                              const Cardinality cardinality) const {
    if (cardinality == Cardinality::SINGLE && IsMultiProperty(property)) {
      return Status::TypeError(
          "Invalid data cardinality for property ", property,
          ", defined as SINGLE but got ",
          cardinalities_.at(property) == Cardinality::LIST ? "LIST" : "SET");
    }
    if (IsMultiProperty(property) &&
        (cardinality == Cardinality::SET ||
         cardinalities_.at(property) == Cardinality::SET)) {
      GAR_RETURN_NOT_OK(ValidateMultiPropertySet<T>(property));
    }
    if (IsMultiProperty(property)) {
      auto value_list =
          std::any_cast<std::vector<std::any>>(properties_.at(property));
      for (auto value : value_list) {
        auto& value_type = value.type();
        if (value_type != typeid(T)) {
          return Status::TypeError("Invalid data type for property ", property,
                                   ", defined as ", typeid(T).name(),
                                   ", but got ", value_type.name());
        }
      }
    } else {
      auto& value_type = properties_.at(property).type();
      if (value_type != typeid(T)) {
        return Status::TypeError("Invalid data type for property ", property,
                                 ", defined as ", typeid(T).name(),
                                 ", but got ", value_type.name());
      }
    }
    return Status::OK();
  }

  template <typename T>
  Status ValidateMultiProperty(const std::string& property) const {
    if (IsMultiProperty(property) &&
        cardinalities_.at(property) == Cardinality::SET) {
      GAR_RETURN_NOT_OK(ValidateMultiPropertySet<T>(property));
    }
    return Status::OK();
  }

  template <typename T>
  Status ValidateMultiPropertySet(const std::string& property) const {
    auto vec = std::any_cast<std::vector<std::any>>(properties_.at(property));
    std::unordered_set<T> seen;
    for (const auto& item : vec) {
      if (!seen.insert(std::any_cast<T>(item)).second) {
        return Status::KeyError(
            "Duplicate values exist in set type multi-property key: ", property,
            " value: ", std::any_cast<T>(item));
      }
    }
    return Status::OK();
  }

 private:
  IdType id_;
  bool empty_;
  std::unordered_map<std::string, std::any> properties_;
  std::unordered_map<std::string, Cardinality> cardinalities_;
};

/**
 * @brief VertexBuilder is designed for building and writing a collection of
 * vertices.
 *
 */
class VerticesBuilder {
 public:
  /**
   * @brief Initialize the VerticesBuilder.
   *
   * @param vertex_info The vertex info that describes the vertex type.
   * @param prefix The absolute prefix.
   * @param start_vertex_index The start index of the vertices collection.
   * @param writerOptions The writerOptions provides configuration options for
   * different file format writers.
   * @param validate_level The global validate level for the writer, with no
   * validate by default. It could be ValidateLevel::no_validate,
   * ValidateLevel::weak_validate or ValidateLevel::strong_validate, but could
   * not be ValidateLevel::default_validate.
   */
  explicit VerticesBuilder(
      const std::shared_ptr<VertexInfo>& vertex_info, const std::string& prefix,
      IdType start_vertex_index = 0,
      std::shared_ptr<WriterOptions> writerOptions = nullptr,
      const ValidateLevel& validate_level = ValidateLevel::no_validate)
      : vertex_info_(std::move(vertex_info)),
        prefix_(prefix),
        start_vertex_index_(start_vertex_index),
        writer_options_(writerOptions),
        validate_level_(validate_level) {
    if (validate_level_ == ValidateLevel::default_validate) {
      throw std::runtime_error(
          "default_validate is not allowed to be set as the global validate "
          "level for VerticesBuilder");
    }
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
   * @brief Set the writerOptions.
   *
   * @return The writerOptions provides configuration options for different file
   * format writers.
   */
  inline void SetWriterOptions(std::shared_ptr<WriterOptions> writer_options) {
    this->writer_options_ = writer_options;
  }

  /**
   * @brief Set the writerOptions.
   *
   * @param writerOptions The writerOptions provides configuration options for
   * different file format writers.
   */
  inline std::shared_ptr<WriterOptions> GetWriterOptions() {
    return this->writer_options_;
  }

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
   * @brief Add a vertex with the given index.
   *
   * The validate_level for this operation could be:
   *
   * ValidateLevel::default_validate: to use the validate_level of the builder,
   * which set through the constructor or the SetValidateLevel method;
   *
   * ValidateLevel::no_validate: without validation;
   *
   * ValidateLevel::weak_validate: to validate if the start index and the vertex
   * index is valid, and the data in builder is not saved;
   *
   * ValidateLevel::strong_validate: besides weak_validate, also validate the
   * schema of the vertex is consistent with the info defined.
   *
   * @param v The vertex to add.
   * @param index The given index, -1 means the next unused index.
   * @param validate_level The validate level for this operation,
   * which is the builder's validate level by default.
   * @return Status: ok or Status::Invalid error.
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
    VertexPropertyWriter writer(vertex_info_, prefix_, writer_options_,
                                validate_level_);
    IdType start_chunk_index =
        start_vertex_index_ / vertex_info_->GetChunkSize();
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

  /**
   * @brief Construct a VertexBuilder from vertex info.
   *
   * @param vertex_info The vertex info that describes the vertex type.
   * @param prefix The absolute prefix.
   * @param start_vertex_index The start index of the vertices collection.
   * @param writerOptions The writerOptions provides configuration options for
   * different file format writers.
   * @param validate_level The global validate level for the builder, default is
   * no_validate.
   */
  static Result<std::shared_ptr<VerticesBuilder>> Make(
      const std::shared_ptr<VertexInfo>& vertex_info, const std::string& prefix,
      std::shared_ptr<WriterOptions> writer_options,
      IdType start_vertex_index = 0,
      const ValidateLevel& validate_level = ValidateLevel::no_validate) {
    return std::make_shared<VerticesBuilder>(vertex_info, prefix,
                                             start_vertex_index, writer_options,
                                             validate_level);
  }

  static Result<std::shared_ptr<VerticesBuilder>> Make(
      const std::shared_ptr<VertexInfo>& vertex_info, const std::string& prefix,
      IdType start_vertex_index = 0,
      const ValidateLevel& validate_level = ValidateLevel::no_validate) {
    return std::make_shared<VerticesBuilder>(
        vertex_info, prefix, start_vertex_index, nullptr, validate_level);
  }

  /**
   * @brief Construct a VertexBuilder from graph info and vertex type.
   *
   * @param graph_info The graph info that describes the graph.
   * @param type The type of the vertex.
   * @param start_vertex_index The start index of the vertices collection.
   * @param writerOptions The writerOptions provides configuration options for
   * different file format writers.
   * @param validate_level The global validate level for the builder, default is
   * no_validate.
   */
  static Result<std::shared_ptr<VerticesBuilder>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
      std::shared_ptr<WriterOptions> writer_options,
      IdType start_vertex_index = 0,
      const ValidateLevel& validate_level = ValidateLevel::no_validate) {
    const auto vertex_info = graph_info->GetVertexInfo(type);
    if (!vertex_info) {
      return Status::KeyError("The vertex type ", type,
                              " doesn't exist in graph ", graph_info->GetName(),
                              ".");
    }
    return Make(vertex_info, graph_info->GetPrefix(), writer_options,
                start_vertex_index, validate_level);
  }

  static Result<std::shared_ptr<VerticesBuilder>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
      IdType start_vertex_index = 0,
      const ValidateLevel& validate_level = ValidateLevel::no_validate) {
    const auto vertex_info = graph_info->GetVertexInfo(type);
    if (!vertex_info) {
      return Status::KeyError("The vertex type ", type,
                              " doesn't exist in graph ", graph_info->GetName(),
                              ".");
    }
    return Make(vertex_info, graph_info->GetPrefix(), nullptr,
                start_vertex_index, validate_level);
  }

 private:
  /**
   * @brief Check if adding a vertex with the given index is allowed.
   *
   * @param v The vertex to add.
   * @param index The given index, -1 means the next unused index.
   * @param validate_level The validate level for this operation.
   * @return Status: ok or Status::Invalid error.
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
  Status appendToArray(const std::shared_ptr<DataType>& type,
                       const std::string& property_name,
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
  std::shared_ptr<VertexInfo> vertex_info_;
  std::string prefix_;
  std::vector<Vertex> vertices_;
  IdType start_vertex_index_;
  IdType num_vertices_;
  bool is_saved_;
  std::shared_ptr<WriterOptions> writer_options_;
  ValidateLevel validate_level_;
};

}  // namespace graphar::builder
