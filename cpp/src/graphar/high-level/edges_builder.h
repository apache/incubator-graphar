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

#include <algorithm>
#include <any>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "graphar/arrow/chunk_writer.h"
#include "graphar/fwd.h"
#include "graphar/graph_info.h"
#include "graphar/types.h"

namespace arrow {
class Array;
}

namespace graphar::builder {

/**
 * @brief Edge is designed for constructing edges builder.
 *
 */
class Edge {
 public:
  /**
   * @brief Initialize the edge with its source and destination.
   *
   * @param src_id The id of the source vertex.
   * @param dst_id The id of the destination vertex.
   */
  explicit Edge(IdType src_id, IdType dst_id)
      : src_id_(src_id), dst_id_(dst_id), empty_(true) {}

  /**
   * @brief Check if the edge is empty.
   *
   * @return true/false.
   */
  inline bool Empty() const noexcept { return empty_; }

  /**
   * @brief Get source id of the edge.
   *
   * @return The id of the source vertex.
   */
  inline IdType GetSource() const noexcept { return src_id_; }

  /**
   * @brief Get destination id of the edge.
   *
   * @return The id of the destination vertex.
   */
  inline IdType GetDestination() const noexcept { return dst_id_; }

  /**
   * @brief Add a property to the edge.
   *
   * @param name The name of the property.
   * @param val The value of the property.
   */
  // TODO(@acezen): Enable the property to be a vector(list).
  inline void AddProperty(const std::string& name, const std::any& val) {
    empty_ = false;
    properties_[name] = val;
  }

  /**
   * @brief Get a property of the edge.
   *
   * @param property The name of the property.
   * @return The value of the property.
   */
  inline const std::any& GetProperty(const std::string& property) const {
    return properties_.at(property);
  }

  /**
   * @brief Get all properties of the edge.
   *
   * @return The map containing all properties of the edge.
   */
  inline const std::unordered_map<std::string, std::any>& GetProperties()
      const {
    return properties_;
  }

  /**
   * @brief Check if the edge contains a property.
   *
   * @param property The name of the property.
   * @return true/false.
   */
  inline bool ContainProperty(const std::string& property) const {
    return (properties_.find(property) != properties_.end());
  }

 private:
  IdType src_id_, dst_id_;
  bool empty_;
  std::unordered_map<std::string, std::any> properties_;
};

/**
 * @brief The compare function for sorting edges by source id.
 *
 * @param a The first edge to compare.
 * @param b The second edge to compare.
 * @return If a is less than b: true/false.
 */
inline bool cmp_src(const Edge& a, const Edge& b) {
  return a.GetSource() < b.GetSource();
}

/**
 * @brief The compare function for sorting edges by destination id.
 *
 * @param a The first edge to compare.
 * @param b The second edge to compare.
 * @return If a is less than b: true/false.
 */
inline bool cmp_dst(const Edge& a, const Edge& b) {
  return a.GetDestination() < b.GetDestination();
}

/**
 * @brief EdgeBuilder is designed for building and writing a collection of
 * edges.
 *
 */
class EdgesBuilder {
 public:
  /**
   * @brief Initialize the EdgesBuilder.
   *
   * @param edge_info The edge info that describes the vertex type.
   * @param prefix The absolute prefix.
   * @param adj_list_type The adj list type of the edges.
   * @param num_vertices The total number of vertices for source or destination.
   * @param writerOptions The writerOptions provides configuration options for
   * different file format writers.
   * @param validate_level The global validate level for the writer, with no
   * validate by default. It could be ValidateLevel::no_validate,
   * ValidateLevel::weak_validate or ValidateLevel::strong_validate, but could
   * not be ValidateLevel::default_validate.
   */
  explicit EdgesBuilder(
      const std::shared_ptr<EdgeInfo>& edge_info, const std::string& prefix,
      AdjListType adj_list_type, IdType num_vertices,
      std::shared_ptr<WriterOptions> writerOptions = nullptr,
      const ValidateLevel& validate_level = ValidateLevel::no_validate)
      : edge_info_(std::move(edge_info)),
        prefix_(prefix),
        adj_list_type_(adj_list_type),
        num_vertices_(num_vertices),
        writer_options_(writerOptions),
        validate_level_(validate_level) {
    if (validate_level_ == ValidateLevel::default_validate) {
      throw std::runtime_error(
          "default_validate is not allowed to be set as the global validate "
          "level for EdgesBuilder");
    }
    edges_.clear();
    num_edges_ = 0;
    is_saved_ = false;
    switch (adj_list_type) {
    case AdjListType::unordered_by_source:
      vertex_chunk_size_ = edge_info_->GetSrcChunkSize();
      break;
    case AdjListType::ordered_by_source:
      vertex_chunk_size_ = edge_info_->GetSrcChunkSize();
      break;
    case AdjListType::unordered_by_dest:
      vertex_chunk_size_ = edge_info_->GetDstChunkSize();
      break;
    case AdjListType::ordered_by_dest:
      vertex_chunk_size_ = edge_info_->GetDstChunkSize();
      break;
    default:
      vertex_chunk_size_ = edge_info_->GetSrcChunkSize();
    }
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
   * @brief Get the validate level.
   *
   * @return The validate level of this writer.
   */
  inline ValidateLevel GetValidateLevel() const { return validate_level_; }

  /**
   * @brief Clear the edges in this EdgesBuilder.
   */
  inline void Clear() {
    edges_.clear();
    num_edges_ = 0;
    is_saved_ = false;
  }

  /**
   * @brief Add an edge to the collection.
   *
   * The validate_level for this operation could be:
   *
   * ValidateLevel::default_validate: to use the validate_level of the builder,
   * which set through the constructor or the SetValidateLevel method;
   *
   * ValidateLevel::no_validate: without validation;
   *
   * ValidateLevel::weak_validate: to validate if the adj_list type is valid,
   * and the data in builder is not saved;
   *
   * ValidateLevel::strong_validate: besides weak_validate, also validate the
   * schema of the edge is consistent with the info defined.
   *
   * @param e The edge to add.
   * @param validate_level The validate level for this operation,
   * which is the builder's validate level by default.
   * @return Status: ok or Status::Invalid error.
   */
  Status AddEdge(const Edge& e, const ValidateLevel& validate_level =
                                    ValidateLevel::default_validate) {
    // validate
    GAR_RETURN_NOT_OK(validate(e, validate_level));
    // add an edge
    IdType vertex_chunk_index = getVertexChunkIndex(e);
    edges_[vertex_chunk_index].push_back(e);
    num_edges_++;
    return Status::OK();
  }

  /**
   * @brief Get the current number of edges in the collection.
   *
   * @return The current number of edges in the collection.
   */
  IdType GetNum() const { return num_edges_; }

  /**
   * @brief Dump the collection into files.
   *
   * @return Status: ok or error.
   */
  Status Dump();

  /**
   * @brief Construct an EdgesBuilder from edge info.
   *
   * @param edge_info The edge info that describes the edge type.
   * @param prefix The absolute prefix.
   * @param adj_list_type The adj list type of the edges.
   * @param num_vertices The total number of vertices for source or destination.
   * @param writerOptions The writerOptions provides configuration options for
   * different file format writers.
   * @param validate_level The global validate level for the builder, default is
   * no_validate.
   */
  static Result<std::shared_ptr<EdgesBuilder>> Make(
      const std::shared_ptr<EdgeInfo>& edge_info, const std::string& prefix,
      AdjListType adj_list_type, IdType num_vertices,
      std::shared_ptr<WriterOptions> writer_options,
      const ValidateLevel& validate_level = ValidateLevel::no_validate) {
    if (!edge_info->HasAdjacentListType(adj_list_type)) {
      return Status::KeyError(
          "The adjacent list type ", AdjListTypeToString(adj_list_type),
          " doesn't exist in edge ", edge_info->GetEdgeType(), ".");
    }
    return std::make_shared<EdgesBuilder>(edge_info, prefix, adj_list_type,
                                          num_vertices, writer_options,
                                          validate_level);
  }

  static Result<std::shared_ptr<EdgesBuilder>> Make(
      const std::shared_ptr<EdgeInfo>& edge_info, const std::string& prefix,
      AdjListType adj_list_type, IdType num_vertices,
      const ValidateLevel& validate_level = ValidateLevel::no_validate) {
    if (!edge_info->HasAdjacentListType(adj_list_type)) {
      return Status::KeyError(
          "The adjacent list type ", AdjListTypeToString(adj_list_type),
          " doesn't exist in edge ", edge_info->GetEdgeType(), ".");
    }
    return std::make_shared<EdgesBuilder>(edge_info, prefix, adj_list_type,
                                          num_vertices, nullptr,
                                          validate_level);
  }

  /**
   * @brief Construct an EdgesBuilder from graph info.
   *
   * @param graph_info The graph info that describes the graph.
   * @param src_type The type of the source vertex type.
   * @param edge_type The type of the edge type.
   * @param dst_type The type of the destination vertex type.
   * @param adj_list_type The adj list type of the edges.
   * @param num_vertices The total number of vertices for source or destination.
   * @param validate_level The global validate level for the builder, default is
   * no_validate.
   */
  static Result<std::shared_ptr<EdgesBuilder>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
      const std::string& edge_type, const std::string& dst_type,
      const AdjListType& adj_list_type, IdType num_vertices,
      std::shared_ptr<WriterOptions> writer_options,
      const ValidateLevel& validate_level = ValidateLevel::no_validate) {
    auto edge_info = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
    if (!edge_info) {
      return Status::KeyError("The edge ", src_type, " ", edge_type, " ",
                              dst_type, " doesn't exist.");
    }
    return Make(edge_info, graph_info->GetPrefix(), adj_list_type, num_vertices,
                writer_options, validate_level);
  }

  static Result<std::shared_ptr<EdgesBuilder>> Make(
      const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
      const std::string& edge_type, const std::string& dst_type,
      const AdjListType& adj_list_type, IdType num_vertices,
      const ValidateLevel& validate_level = ValidateLevel::no_validate) {
    auto edge_info = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
    if (!edge_info) {
      return Status::KeyError("The edge ", src_type, " ", edge_type, " ",
                              dst_type, " doesn't exist.");
    }
    return Make(edge_info, graph_info->GetPrefix(), adj_list_type, num_vertices,
                nullptr, validate_level);
  }

 private:
  /**
   * @brief Get the vertex chunk index of a given edge.
   *
   * @param e The edge to add.
   * @return The vertex chunk index of the edge.
   */
  IdType getVertexChunkIndex(const Edge& e) {
    switch (adj_list_type_) {
    case AdjListType::unordered_by_source:
      return e.GetSource() / vertex_chunk_size_;
    case AdjListType::ordered_by_source:
      return e.GetSource() / vertex_chunk_size_;
    case AdjListType::unordered_by_dest:
      return e.GetDestination() / vertex_chunk_size_;
    case AdjListType::ordered_by_dest:
      return e.GetDestination() / vertex_chunk_size_;
    default:
      return e.GetSource() / vertex_chunk_size_;
    }
  }

  /**
   * @brief Check if adding an edge is allowed.
   *
   * @param e The edge to add.
   * @param validate_level The validate level for this operation.
   * @return Status: ok or status::InvalidOperation error.
   */
  Status validate(const Edge& e, ValidateLevel validate_level) const;

  /**
   * @brief Construct an array for a given property.
   *
   * @param type The type of the property.
   * @param property_name The name of the property.
   * @param array The constructed array.
   * @param edges The edges of a specific vertex chunk.
   * @return Status: ok or Status::TypeError error.
   */
  Status appendToArray(const std::shared_ptr<DataType>& type,
                       const std::string& property_name,
                       std::shared_ptr<arrow::Array>& array,  // NOLINT
                       const std::vector<Edge>& edges);

  /**
   * @brief Append the values for a property for edges in a specific vertex
   * chunk into the given array.
   *
   * @tparam type The data type.
   * @param property_name The name of the property.
   * @param array The array to append.
   * @param edges The edges of a specific vertex chunk.
   * @return Status: ok or Status::ArrowError error.
   */
  template <Type type>
  Status tryToAppend(const std::string& property_name,
                     std::shared_ptr<arrow::Array>& array,  // NOLINT
                     const std::vector<Edge>& edges);

  /**
   * @brief Append the adj list for edges in a specific vertex chunk
   * into the given array.
   *
   * @param src_or_dest Choose to append sources or destinations.
   * @param array The array to append.
   * @param edges The edges of a specific vertex chunk.
   * @return Status: ok or Status::ArrowError error.
   */
  Status tryToAppend(int src_or_dest,
                     std::shared_ptr<arrow::Array>& array,  // NOLINT
                     const std::vector<Edge>& edges);

  /**
   * @brief Convert the edges in a specific vertex chunk into
   * an Arrow Table.
   *
   * @param edges The edges of a specific vertex chunk.
   */
  Result<std::shared_ptr<arrow::Table>> convertToTable(
      const std::vector<Edge>& edges);

  /**
   * @brief Construct the offset table if the adj list type is ordered.
   *
   * @param vertex_chunk_index The corresponding vertex chunk index.
   * @param edges The edges of a specific vertex chunk.
   */
  Result<std::shared_ptr<arrow::Table>> getOffsetTable(
      IdType vertex_chunk_index, const std::vector<Edge>& edges);

 private:
  std::shared_ptr<EdgeInfo> edge_info_;
  std::string prefix_;
  AdjListType adj_list_type_;
  std::unordered_map<IdType, std::vector<Edge>> edges_;
  IdType vertex_chunk_size_;
  IdType num_vertices_;
  IdType num_edges_;
  bool is_saved_;
  std::shared_ptr<WriterOptions> writer_options_;
  ValidateLevel validate_level_;
};

}  // namespace graphar::builder
