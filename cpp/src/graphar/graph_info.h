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
#include <unordered_map>
#include <vector>

#include "graphar/fwd.h"

namespace graphar {

/**
 * Property is a struct to store the property information.
 */
class Property {
 public:
  std::string name;                // property name
  std::shared_ptr<DataType> type;  // property data type
  bool is_primary;                 // primary key tag
  bool is_nullable;                // nullable tag for non-primary key
  Cardinality
      cardinality;  // cardinality of the property, only use in vertex info

  Property() = default;

  explicit Property(const std::string& name,
                    const std::shared_ptr<DataType>& type = nullptr,
                    bool is_primary = false, bool is_nullable = true,
                    Cardinality cardinality = Cardinality::SINGLE)
      : name(name),
        type(type),
        is_primary(is_primary),
        is_nullable(!is_primary && is_nullable),
        cardinality(cardinality) {}
};

bool operator==(const Property& lhs, const Property& rhs);

/**
 * PropertyGroup is a class to store the property group information.
 *
 * A property group is a collection of properties with a file type and prefix
 * used for chunk files. The prefix is optional and is the concatenation of
 * property names with '_' as separator by default.
 */
class PropertyGroup {
 public:
  /**
   * Initialize the PropertyGroup with a list of properties, file type, and
   * optional prefix.
   *
   * @param properties Property list of the group
   * @param file_type File type of property group chunk file
   * @param prefix prefix of property group chunk file. The default
   *        prefix is the concatenation of property names with '_' as separator
   */
  explicit PropertyGroup(const std::vector<Property>& properties,
                         FileType file_type, const std::string& prefix = "");

  /**
   * Get the property list of group.
   *
   * @return The property list of group.
   */
  const std::vector<Property>& GetProperties() const;

  bool HasProperty(const std::string& property_name) const;

  /** Get the file type of property group chunk file.
   *
   * @return The file type of group.
   */
  inline FileType GetFileType() const { return file_type_; }

  /** Get the prefix of property group chunk file.
   *
   * @return The path prefix of group.
   */
  inline const std::string& GetPrefix() const { return prefix_; }

  /**
   * Check if the property group is validated.
   */
  bool IsValidated() const;

  friend std::ostream& operator<<(std::ostream& stream,
                                  const PropertyGroup& pg) {
    for (size_t i = 0; i < pg.properties_.size(); ++i) {
      stream << pg.properties_[i].name;
      if (i != pg.properties_.size() - 1) {
        stream << "_";
      }
    }
    return stream;
  }

 private:
  std::vector<Property> properties_;
  FileType file_type_;
  std::string prefix_;
};

bool operator==(const PropertyGroup& lhs, const PropertyGroup& rhs);

/**
 * AdjacentList is a class to store the adjacency list information.
 */
class AdjacentList {
 public:
  /**
   * Initialize the AdjacentList with the given type, file type, and optional
   * prefix
   *
   * @param type Type of adjacent list
   * @param file_type File type of adjacent list chunk file
   * @param prefix The prefix of the adjacent list. If left empty, the default
   *        prefix will be set the type name of adjacent list
   */
  explicit AdjacentList(AdjListType type, FileType file_type,
                        const std::string& prefix = "");

  /**
   * @brief Get the type of adjacent list
   *
   * @return The type of adjacent list
   */
  inline AdjListType GetType() const { return type_; }

  /**
   * @brief Get the file type of adjacent list
   *
   * @return The file type of adjacent list
   */
  inline FileType GetFileType() const { return file_type_; }

  /**
   * @brief Get the prefix of adjacent list
   *
   * @return The path prefix of adjacent list
   */
  inline const std::string& GetPrefix() const { return prefix_; }

  /**
   * Returns whether the adjacent list is validated.
   *
   * @return True if the adjacent list is valid, False otherwise.
   */
  bool IsValidated() const;

 private:
  AdjListType type_;
  FileType file_type_;
  std::string prefix_;
};

/**
 * \class VertexInfo
 * \brief VertexInfo is a class to describe the vertex information, including
 * the vertex type, chunk size, property groups, and prefix.
 */
class VertexInfo {
 public:
  /**
   * Construct a VertexInfo object with the given information and property
   * group.
   *
   * @param type The type of the vertex.
   * @param chunk_size The number of vertices in each vertex chunk.
   * @param property_groups The property group vector of the vertex.
   * @param labels The labels of the vertex.
   * @param prefix The prefix of the vertex info. If left empty, the default
   *        prefix will be set to the type of the vertex.
   * @param version The format version of the vertex info.
   */
  explicit VertexInfo(const std::string& type, IdType chunk_size,
                      const PropertyGroupVector& property_groups,
                      const std::vector<std::string>& labels = {},
                      const std::string& prefix = "",
                      std::shared_ptr<const InfoVersion> version = nullptr);

  ~VertexInfo();

  /**
   * Adds a property group to the vertex info and returns a new VertexInfo
   *
   * @param property_group The PropertyGroup object to add.
   */
  Result<std::shared_ptr<VertexInfo>> AddPropertyGroup(
      std::shared_ptr<PropertyGroup> property_group) const;

  /**
   * @brief Removes a property group from the VertexInfo instance and returns a
   * new VertexInfo.
   * @param property_group The property group to remove.
   * @return A Status object indicating the success or failure of the
   * operation. Returns InvalidOperation if the property group is not contained.
   */
  Result<std::shared_ptr<VertexInfo>> RemovePropertyGroup(
      std::shared_ptr<PropertyGroup> property_group) const;

  /**
   * Get the type of the vertex.
   *
   * @return The type of the vertex.
   */
  const std::string& GetType() const;

  /**
   * Get the chunk size of the vertex.
   *
   * @return The chunk size of the vertex.
   */
  IdType GetChunkSize() const;

  /**
   * Get the path prefix of the vertex.
   *
   * @return The path prefix of the vertex.
   */
  const std::string& GetPrefix() const;

  /**
   * Get the version info of the vertex.
   *
   * @return The version info of the vertex.
   */
  const std::shared_ptr<const InfoVersion>& version() const;

  /**
   * Get the labels of the vertex.
   * @return The labels of the vertex.
   */
  const std::vector<std::string>& GetLabels() const;

  /**
   * Get the number of property groups of the vertex.
   *
   * @return The number of property groups of the vertex.
   */
  int PropertyGroupNum() const;

  /**
   * Get the property groups of the vertex.
   */
  const PropertyGroupVector& GetPropertyGroups() const;

  /**
   * Get the property group that contains the specified property.
   *
   * @param property_name The name of the property.
   * @return property group may be nullptr if the property is not found.
   */
  std::shared_ptr<PropertyGroup> GetPropertyGroup(
      const std::string& property_name) const;

  /**
   * Get the property group at the specified index.
   *
   * @param index The index of the property group.
   * @return property group may be nullptr if the index is out of range.
   */
  std::shared_ptr<PropertyGroup> GetPropertyGroupByIndex(int index) const;

  /**
   * Get the data type of the specified property.
   *
   * @param property_name The name of the property.
   * @return A Result object containing the data type of the property, or a
   * KeyError Status object if the property is not found.
   */
  Result<std::shared_ptr<DataType>> GetPropertyType(
      const std::string& property_name) const;

  Result<Cardinality> GetPropertyCardinality(
      const std::string& property_name) const;
  /**
   * Get whether the vertex info contains the specified property.
   *
   * @param property_name The name of the property.
   * @return True if the property exists in the vertex info, False otherwise.
   */
  bool HasProperty(const std::string& property_name) const;

  /**
   * Saves the vertex info to a YAML file.
   *
   * @param file_name The name of the file to save to.
   * @return A Status object indicating success or failure.
   */
  Status Save(const std::string& file_name) const;

  /**
   * Returns the vertex info as a YAML formatted string.
   *
   * @return A Result object containing the YAML string, or a Status object
   * indicating an error.
   */
  Result<std::string> Dump() const noexcept;

  /**
   * Returns whether the specified property is a primary key.
   *
   * @param property_name The name of the property.
   * @return True if the property is a primary key, False otherwise.
   */
  bool IsPrimaryKey(const std::string& property_name) const;

  /**
   * Returns whether the specified property is a nullable key.
   *
   * @param property_name The name of the property.
   * @return True if the property is a nullable key, False otherwise.
   */
  bool IsNullableKey(const std::string& property_name) const;

  /**
   * Returns whether the vertex info contains the specified property group.
   *
   * @param property_group The PropertyGroup object to check for.
   * @return True if the property group exists in the vertex info, False
   * otherwise.
   */
  bool HasPropertyGroup(
      const std::shared_ptr<PropertyGroup>& property_group) const;

  /**
   * Get the file path for the specified property group and chunk index.
   *
   * @param property_group The PropertyGroup object to get the file path for.
   * @param chunk_index The chunk index.
   * @return A Result object containing the file path, or a KeyError Status
   * object if the property group is not found in the vertex info.
   */
  Result<std::string> GetFilePath(std::shared_ptr<PropertyGroup> property_group,
                                  IdType chunk_index) const;

  /**
   * Get the path prefix for the specified property group.
   *
   * @param property_group The PropertyGroup object to get the path prefix for.
   * @return A Result object containing the path prefix, or a KeyError Status
   * object if the property group is not found in the vertex info.
   */
  Result<std::string> GetPathPrefix(
      std::shared_ptr<PropertyGroup> property_group) const;

  /**
   * Get the file path for the number of vertices.
   *
   * @return The file path for the number of vertices.
   */
  Result<std::string> GetVerticesNumFilePath() const;

  /**
   * Returns whether the vertex info is validated.
   *
   * @return True if the vertex info is valid, False otherwise.
   */
  bool IsValidated() const;

  /**
   * Loads vertex info from a YAML object.
   *
   * @param yaml A shared pointer to a Yaml object containing the YAML string.
   * @return A Result object containing the VertexInfo object, or a Status
   * object indicating an error.
   */
  static Result<std::shared_ptr<VertexInfo>> Load(std::shared_ptr<Yaml> yaml);

  /**
   * Loads vertex info from a YAML string.
   *
   * @param input The YAML content string.
   * @return A Result object containing the VertexInfo object, or a Status.
   */
  static Result<std::shared_ptr<VertexInfo>> Load(const std::string& input);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

/**
 * \class EdgeInfo
 * \brief EdgeInfo is a class to describe the edge information, including the
 * source vertex type, edge type, destination vertex type, chunk size,
 * adjacent list property groups, and prefix.
 */
class EdgeInfo {
 public:
  /**
   * @brief Construct an EdgeInfo object with the given information and property
   * groups.
   *
   * @param src_type The type of the source vertex.
   * @param edge_type The type of the edge.
   * @param dst_type The type of the destination vertex.
   * @param chunk_size The number of edges in each edge chunk.
   * @param src_chunk_size The number of source vertices in each vertex chunk.
   * @param dst_chunk_size The number of destination vertices in each vertex
   * chunk.
   * @param directed Whether the edge is directed.
   * @param adjacent_lists The adjacency list vector of the edge.
   * @param property_groups The property group vector of the edge.
   * @param prefix The path prefix of the edge info.
   * @param version The version of the edge info.
   */
  explicit EdgeInfo(const std::string& src_type, const std::string& edge_type,
                    const std::string& dst_type, IdType chunk_size,
                    IdType src_chunk_size, IdType dst_chunk_size, bool directed,
                    const AdjacentListVector& adjacent_lists,
                    const PropertyGroupVector& property_groups,
                    const std::string& prefix = "",
                    std::shared_ptr<const InfoVersion> version = nullptr);

  ~EdgeInfo();

  /**
   * Add an adjacency list information to the edge info and returns a new
   * EdgeInfo. The adjacency list information indicating the adjacency list
   * stored with CSR, CSC, or COO format.
   *
   * @param adj_list The adjacency list to add.
   */
  Result<std::shared_ptr<EdgeInfo>> AddAdjacentList(
      std::shared_ptr<AdjacentList> adj_list) const;

  /**
   * @brief Removes an adjacency list from the EdgeInfo instance and returns a
   * new EdgeInfo.
   * @param adj_list The adjacency list to remove.
   * @return A Status object indicating the success or failure of the
   * operation. Returns InvalidOperation if the adjacency list is not contained.
   */
  Result<std::shared_ptr<EdgeInfo>> RemoveAdjacentList(
      std::shared_ptr<AdjacentList> adj_list) const;

  /**
   * Add a property group to edge info and returns a new EdgeInfo.
   *
   * @param property_group Property group to add.
   */
  Result<std::shared_ptr<EdgeInfo>> AddPropertyGroup(
      std::shared_ptr<PropertyGroup> property_group) const;

  /**
   * @brief Removes a property group from the EdgeInfo instance and returns a
   * new EdgeInfo.
   * @param property_group The property group to remove.
   * @return A Status object indicating the success or failure of the
   * operation. Returns InvalidOperation if the property group is not contained.
   */
  Result<std::shared_ptr<EdgeInfo>> RemovePropertyGroup(
      std::shared_ptr<PropertyGroup> property_group) const;

  /**
   * Get the type of the source vertex.
   * @return The type of the source vertex.
   */
  const std::string& GetSrcType() const;

  /**
   * Get the type of the edge.
   * @return The type of the edge.
   */
  const std::string& GetEdgeType() const;

  /**
   * Get the type of the destination vertex.
   * @return The type of the destination vertex.
   */
  const std::string& GetDstType() const;

  /**
   * Get the number of edges in each edge chunk.
   * @return The number of edges in each edge chunk.
   */
  IdType GetChunkSize() const;

  /**
   * Get the number of source vertices in each vertex chunk.
   * @return The number of source vertices in each vertex chunk.
   */
  IdType GetSrcChunkSize() const;

  /**
   * Get the number of destination vertices in each vertex chunk.
   * @return The number of destination vertices in each vertex chunk.
   */
  IdType GetDstChunkSize() const;

  /**
   * Get the path prefix of the edge.
   * @return The path prefix of the edge.
   */
  const std::string& GetPrefix() const;

  /**
   * Returns whether the edge is directed.
   * @return True if the edge is directed, false otherwise.
   */
  bool IsDirected() const;

  /**
   * Get the version info of the edge.
   * @return The version info of the edge.
   */
  const std::shared_ptr<const InfoVersion>& version() const;

  /**
   * Return whether the edge info contains the adjacency list information.
   *
   * @param adj_list_type The adjacency list type.
   * @return True if the edge info contains the adjacency list information,
   * false otherwise.
   */
  bool HasAdjacentListType(AdjListType adj_list_type) const;

  /**
   * @brief Returns whether the edge info contains the given property
   *
   * @param property Property name to check.
   * @return True if the edge info contains the property, false otherwise.
   */
  bool HasProperty(const std::string& property_name) const;

  /**
   * @brief Returns whether the edge info contains the given property group
   */
  bool HasPropertyGroup(
      const std::shared_ptr<PropertyGroup>& property_group) const;

  std::shared_ptr<AdjacentList> GetAdjacentList(
      AdjListType adj_list_type) const;

  /**
   * @brief Get the number of property groups.
   */
  int PropertyGroupNum() const;

  /**
   * @brief Get the property groups.
   *
   */
  const PropertyGroupVector& GetPropertyGroups() const;

  /**
   * @brief Get the property group containing the given property.
   *
   * @param property Property name.
   * @return Property group may be nullptr if the property is not found.
   */
  std::shared_ptr<PropertyGroup> GetPropertyGroup(
      const std::string& property) const;

  /**
   * @brief Get the property group at the specified index.
   *
   * @param index The index of the property group.
   * @return Property group may be nullptr if the index is out of range.
   */
  std::shared_ptr<PropertyGroup> GetPropertyGroupByIndex(int index) const;

  /**
   * @brief Get the file path for the number of vertices.
   *
   * @param adj_list_type The adjacency list type.
   * @return A Result object containing the file path for the number of edges,
   * or a Status object indicating an error.
   */
  Result<std::string> GetVerticesNumFilePath(AdjListType adj_list_type) const;

  /**
   * Get the file path for the number of edges.
   *
   * @param vertex_chunk_index the vertex chunk index
   * @param adj_list_type The adjacency list type.
   * @return A Result object containing the file path for the number of edges,
   * or a Status object indicating an error.
   */
  Result<std::string> GetEdgesNumFilePath(IdType vertex_chunk_index,
                                          AdjListType adj_list_type) const;

  /**
   * @brief Get the file path of adj list topology chunk
   *
   * @param vertex_chunk_index the vertex chunk index
   * @param edge_chunk_index index of edge adj list chunk of the vertex chunk
   * @param adj_list_type The adjacency list type.
   */
  Result<std::string> GetAdjListFilePath(IdType vertex_chunk_index,
                                         IdType edge_chunk_index,
                                         AdjListType adj_list_type) const;

  /**
   * @brief Get the path prefix of the adjacency list topology chunk for the
   * given adjacency list type.
   * @param adj_list_type The adjacency list type.
   * @return A Result object containing the directory, or a Status object
   * indicating an error.
   */
  Result<std::string> GetAdjListPathPrefix(AdjListType adj_list_type) const;

  /**
   * @brief Get the adjacency list offset chunk file path of vertex chunk
   *    the offset chunks is aligned with the vertex chunks
   *
   * @param vertex_chunk_index index of vertex chunk
   * @param adj_list_type The adjacency list type.
   */
  Result<std::string> GetAdjListOffsetFilePath(IdType vertex_chunk_index,
                                               AdjListType adj_list_type) const;

  /**
   * Get the path prefix of the adjacency list offset chunk for the given
   * adjacency list type.
   * @param adj_list_type The adjacency list type.
   * @return A Result object containing the path prefix, or a Status object
   * indicating an error.
   */
  Result<std::string> GetOffsetPathPrefix(AdjListType adj_list_type) const;

  /**
   * @brief Get the chunk file path of adj list property group
   *    the property group chunks is aligned with the adj list topology chunks
   *
   * @param property_group property group
   * @param adj_list_type adj list type that the property group belongs to
   * @param vertex_chunk_index the vertex chunk index
   * @param edge_chunk_index index of edge property group chunk of the vertex
   * chunk
   */
  Result<std::string> GetPropertyFilePath(
      const std::shared_ptr<PropertyGroup>& property_group,
      AdjListType adj_list_type, IdType vertex_chunk_index,
      IdType edge_chunk_index) const;

  /**
   * Get the path prefix of the property group chunk for the given
   * adjacency list type.
   * @param property_group property group.
   * @param adj_list_type The adjacency list type.
   * @return A Result object containing the path prefix, or a Status object
   * indicating an error.
   */
  Result<std::string> GetPropertyGroupPathPrefix(
      const std::shared_ptr<PropertyGroup>& property_group,
      AdjListType adj_list_type) const;

  /**
   * Get the data type of the specified property.
   *
   * @param property_name The name of the property.
   * @return A Result object containing the data type of the property, or a
  KeyError Status object if the property is not found.
   */
  Result<std::shared_ptr<DataType>> GetPropertyType(
      const std::string& property_name) const;
  /**
   * Returns whether the specified property is a primary key.
   *
   * @param property_name The name of the property.
   * @return True if the property is a primary key, False otherwise.
   */
  bool IsPrimaryKey(const std::string& property_name) const;

  /**
   * Returns whether the specified property is a nullable key.
   *
   * @param property_name The name of the property.
   * @return True if the property is a nullable key, False otherwise.
   */
  bool IsNullableKey(const std::string& property_name) const;

  /**
   * Saves the edge info to a YAML file.
   *
   * @param file_name The name of the file to save to.
   * @return A Status object indicating success or failure.
   */
  Status Save(const std::string& file_name) const;

  /**
   * Returns the edge info as a YAML formatted string.
   *
   * @return A Result object containing the YAML string, or a Status object
   * indicating an error.
   */
  Result<std::string> Dump() const noexcept;

  /**
   * Returns whether the edge info is validated.
   *
   * @return True if the edge info is valid, False otherwise.
   */
  bool IsValidated() const;

  /** Loads the yaml as an EdgeInfo instance. */
  static Result<std::shared_ptr<EdgeInfo>> Load(std::shared_ptr<Yaml> yaml);

  /**
   * Loads edge info from a YAML string.
   *
   * @param input The YAML content string.
   * @return A Result object containing the EdgeInfo object, or a Status.
   */
  static Result<std::shared_ptr<EdgeInfo>> Load(const std::string& input);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

/**
 * GraphInfo is a class to store the graph meta information.
 */
class GraphInfo {
 public:
  /**
   * @brief Constructs a GraphInfo instance.
   * @param graph_name The name of the graph.
   * @param vertex_infos The vertex info vector of the graph.
   * @param edge_infos The edge info vector of the graph.
   * @param labels The vertex labels of the graph.
   * @param prefix The absolute path prefix to store chunk files of the graph.
   *               Defaults to "./".
   * @param version The version of the graph info.
   * @param extra_info The extra metadata of the graph info.
   */
  explicit GraphInfo(
      const std::string& graph_name, VertexInfoVector vertex_infos,
      EdgeInfoVector edge_infos, const std::vector<std::string>& labels = {},
      const std::string& prefix = "./",
      std::shared_ptr<const InfoVersion> version = nullptr,
      const std::unordered_map<std::string, std::string>& extra_info = {});

  ~GraphInfo();

  /**
   * @brief Loads the input file as a `GraphInfo` instance.
   * @param path The path of the YAML file.
   * @return A Result object containing the GraphInfo instance, or a Status
   * object indicating an error.
   */
  static Result<std::shared_ptr<GraphInfo>> Load(const std::string& path);

  /**
   * @brief Loads the input string as a `GraphInfo` instance.
   * @param input The YAML content string.
   * @param relative_path The relative path to access vertex/edge YAML.
   * @return A Result object containing the GraphInfo instance, or a `Status`
   * object indicating an error.
   */
  static Result<std::shared_ptr<GraphInfo>> Load(
      const std::string& input, const std::string& relative_path);

  /**
   * @brief Adds a vertex info to the GraphInfo instance and returns a new
   * GraphInfo.
   * @param vertex_info The vertex info to add.
   * @return A Status object indicating the success or failure of the
   * operation. Returns InvalidOperation if the vertex info is already
   * contained.
   */
  Result<std::shared_ptr<GraphInfo>> AddVertex(
      std::shared_ptr<VertexInfo> vertex_info) const;

  /**
   * @brief Removes a vertex info from the GraphInfo instance and returns a new
   * GraphInfo.
   * @param vertex_info The vertex info to remove.
   * @return A Status object indicating the success or failure of the
   * operation. Returns InvalidOperation if the vertex info is not contained.
   */
  Result<std::shared_ptr<GraphInfo>> RemoveVertex(
      std::shared_ptr<VertexInfo> vertex_info) const;

  /**
   * @brief Adds an edge info to the GraphInfo instance and returns a new
   * GraphInfo.
   * @param edge_info The edge info to add.
   * @return A Status object indicating the success or failure of the
   * operation. Returns `InvalidOperation` if the edge info is already
   * contained.
   */
  Result<std::shared_ptr<GraphInfo>> AddEdge(
      std::shared_ptr<EdgeInfo> edge_info) const;

  /**
   * @brief Removes an edge info from the GraphInfo instance and returns a new
   * GraphInfo.
   * @param edge_info The edge info to remove.
   * @return A Status object indicating the success or failure of the
   * operation. Returns InvalidOperation if the edge info is not contained.
   */
  Result<std::shared_ptr<GraphInfo>> RemoveEdge(
      std::shared_ptr<EdgeInfo> edge_info) const;

  /**
   * @brief Get the name of the graph.
   * @return The name of the graph.
   */
  const std::string& GetName() const;

  /**
   * @brief Get the vertex labels of the graph.
   * @return The vertex labels of the graph.
   */
  const std::vector<std::string>& GetLabels() const;

  /**
   * @brief Get the absolute path prefix of the chunk files.
   * @return The absolute path prefix of the chunk files.
   */
  const std::string& GetPrefix() const;

  /**
   * @brief Get the version info of the graph info object.
   *
   * @return The version info of the graph info object.
   */
  const std::shared_ptr<const InfoVersion>& version() const;

  /**
   * @brief Get the extra metadata of the graph info object.
   *
   * @return The extra metadata of the graph info object.
   */
  const std::unordered_map<std::string, std::string>& GetExtraInfo() const;

  /**
   * @brief Get the vertex info with the given type.
   * @param type The type of the vertex.
   * @return vertex info may be nullptr if the type is not found.
   */
  std::shared_ptr<VertexInfo> GetVertexInfo(const std::string& type) const;

  /**
   * @brief Get the edge info with the given source vertex type, edge type,
   * and destination vertex type.
   * @param src_type The type of the source vertex.
   * @param edge_type The type of the edge.
   * @param dst_type The type of the destination vertex.
   * @return edge info may be nullptr if the type is not found.
   */
  std::shared_ptr<EdgeInfo> GetEdgeInfo(const std::string& src_type,
                                        const std::string& edge_type,
                                        const std::string& dst_type) const;

  /**
   * @brief Get the vertex info index with the given type.
   */
  int GetVertexInfoIndex(const std::string& type) const;

  /**
   * @brief Get the edge info index with the given source vertex type, edge
   * type, and destination type.
   */
  int GetEdgeInfoIndex(const std::string& src_type,
                       const std::string& edge_type,
                       const std::string& dst_type) const;

  /**
   * @brief Get the number of vertex infos.
   */
  int VertexInfoNum() const;

  /**
   * @brief Get the number of edge infos.
   */
  int EdgeInfoNum() const;

  /**
   * @brief Get the vertex info at the specified index.
   *
   * @param index The index of the vertex info.
   * @return vertex info may be nullptr if the index is out of range.
   */
  const std::shared_ptr<VertexInfo> GetVertexInfoByIndex(int index) const;

  /**
   * @brief Get the edge info at the specified index.
   *
   * @param index The index of the edge info.
   * @return edge info may be nullptr if the index is out of range.
   */
  const std::shared_ptr<EdgeInfo> GetEdgeInfoByIndex(int index) const;

  /**
   * @brief Get the vertex infos of graph info
   *
   * @return vertex infos of graph info
   */
  const VertexInfoVector& GetVertexInfos() const;

  /**
   * @brief Get the edge infos of graph info
   *
   * @return edge infos of graph info
   */
  const EdgeInfoVector& GetEdgeInfos() const;

  /**
   * Saves the graph info to a YAML file.
   *
   * @param path The path of the file to save to.
   * @return A Status object indicating success or failure.
   */
  Status Save(const std::string& path) const;

  /**
   * Returns the graph info as a YAML formatted string.
   *
   * @return A Result object containing the YAML string, or a Status object
   * indicating an error.
   */
  Result<std::string> Dump() const;

  /**
   * Returns whether the graph info is validated.
   *
   * @return True if the graph info is valid, False otherwise.
   */
  bool IsValidated() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace graphar
