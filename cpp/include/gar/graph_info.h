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

#ifndef GAR_GRAPH_INFO_H_
#define GAR_GRAPH_INFO_H_

#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "util/adj_list_type.h"
#include "util/data_type.h"
#include "util/file_type.h"
#include "util/result.h"
#include "util/status.h"
#include "util/util.h"
#include "util/version_parser.h"
#include "util/yaml.h"

namespace GAR_NAMESPACE_INTERNAL {

class Yaml;

class FileSystem;

/**
 * Property is a struct to store the property information for a group.
 */
struct Property {
  std::string name;  // property name
  DataType type;     // property data type
  bool is_primary;   // primary key tag

  Property() {}
  explicit Property(const std::string& name) : name(name) {}
  Property(const std::string& name, const DataType& type, bool is_primary)
      : name(name), type(type), is_primary(is_primary) {}
};

static bool operator==(const Property& lhs, const Property& rhs) {
  return (lhs.name == rhs.name) && (lhs.type.Equals(rhs.type)) &&
         (lhs.is_primary == rhs.is_primary);
}

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
   * Default constructor.
   *
   * Creates an empty property group.
   */
  PropertyGroup() = default;

  /**
   * Destructor.
   */
  ~PropertyGroup() {}

  /**
   * Initialize the PropertyGroup with a list of properties, file type, and
   * optional prefix.
   *
   * @param properties Property list of group
   * @param file_type File type of property group chunk file
   * @param prefix prefix of property group chunk file. The default
   *        prefix is the concatenation of property names with '_' as separator
   */
  explicit PropertyGroup(std::vector<Property> properties, FileType file_type,
                         const std::string& prefix = "")
      : properties_(properties), file_type_(file_type), prefix_(prefix) {
    if (prefix_.empty()) {
      std::vector<std::string> names;
      for (auto& property : properties_) {
        names.push_back(property.name);
        property_names_.insert(property.name);
      }
      prefix_ = util::ConcatStringWithDelimiter(names, REGULAR_SEPERATOR) + "/";
    }
  }

  /**
   * Copy constructor.
   */
  PropertyGroup(const PropertyGroup& other) = default;

  /**
   * Move constructor.
   */
  PropertyGroup(PropertyGroup&& other) = default;

  /**
   * Copy assignment operator.
   */
  inline PropertyGroup& operator=(const PropertyGroup& other) = default;

  /**
   * Move assignment operator.
   */
  inline PropertyGroup& operator=(PropertyGroup&& other) = default;

  /**
   * Get the property list of group.
   *
   * @return The property list of group.
   */
  inline const std::vector<Property>& GetProperties() const {
    return properties_;
  }

  inline bool ContainProperty(const std::string& property_name) const {
    return property_names_.find(property_name) != property_names_.end();
  }

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
  std::unordered_set<std::string> property_names_;
  FileType file_type_;
  std::string prefix_;
};

static bool operator==(const PropertyGroup& lhs, const PropertyGroup& rhs) {
  return (lhs.GetPrefix() == rhs.GetPrefix()) &&
         (lhs.GetFileType() == rhs.GetFileType()) &&
         (lhs.GetProperties() == rhs.GetProperties());
}

/**
 * VertexInfo is a class that stores metadata information about a vertex.
 */
class VertexInfo {
 public:
  /**
   * Default constructor.
   */
  VertexInfo() = default;

  /**
   * Construct a VertexInfo object with the given metadata information.
   *
   * @param label The label of the vertex.
   * @param chunk_size The number of vertices in each vertex chunk.
   * @param version The version of the vertex info.
   * @param prefix The prefix of the vertex info. If left empty, the default
   *        prefix will be set to the label of the vertex.
   */
  explicit VertexInfo(const std::string& label, IdType chunk_size,
                      const InfoVersion& version,
                      const std::string& prefix = "")
      : label_(label),
        chunk_size_(chunk_size),
        version_(version),
        prefix_(prefix) {
    if (prefix_.empty()) {
      prefix_ = label_ + "/";  // default prefix
    }
  }

  /**
   * Destructor.
   */
  ~VertexInfo() {}

  /**
   * Copy constructor.
   */
  VertexInfo(const VertexInfo& vertex_info) = default;

  /**
   * Move constructor.
   */
  explicit VertexInfo(VertexInfo&& vertex_info) = default;

  /**
   * Copy assignment operator.
   */
  inline VertexInfo& operator=(const VertexInfo& other) = default;

  /**
   * Move assignment operator.
   */
  inline VertexInfo& operator=(VertexInfo&& other) = default;

  /**
   * Adds a property group to the vertex info.
   *
   * @param property_group The PropertyGroup object to add.
   * @return A Status object indicating success or failure.
   */
  inline Status AddPropertyGroup(const PropertyGroup& property_group) {
    if (ContainPropertyGroup(property_group)) {
      return Status::Invalid("The property group ", property_group,
                             " has already existed, can't not be added again.");
    }
    for (const auto& property : property_group.GetProperties()) {
      if (ContainProperty(property.name)) {
        return Status::Invalid("The property ", property.name,
                               " has already existed in the ", label_,
                               " vertex info, can't not be added again.");
      }
    }

    property_groups_.push_back(property_group);
    for (const auto& p : property_group.GetProperties()) {
      if (!version_.CheckType(p.type.ToTypeName())) {
        return Status::Invalid("The property type ", p.type.ToTypeName(),
                               " is not supported by the version.");
      }
      p2type_[p.name] = p.type;
      p2primary_[p.name] = p.is_primary;
      p2group_index_[p.name] = property_groups_.size() - 1;
    }
    return Status::OK();
  }

  /**
   * Get the label of the vertex.
   *
   * @return The label of the vertex.
   */
  inline std::string GetLabel() const { return label_; }

  /**
   * Get the chunk size of the vertex.
   *
   * @return The chunk size of the vertex.
   */
  inline IdType GetChunkSize() const { return chunk_size_; }

  /**
   * Get the path prefix of the vertex.
   *
   * @return The path prefix of the vertex.
   */
  inline std::string GetPrefix() const { return prefix_; }

  /**
   * Get the version info of the vertex.
   *
   * @return The version info of the vertex.
   */
  inline const InfoVersion& GetVersion() const { return version_; }

  /**
   * Get the property groups of the vertex.
   *
   *@return A vector of PropertyGroup objects for the vertex.
   */
  inline const std::vector<PropertyGroup>& GetPropertyGroups() const {
    return property_groups_;
  }

  /**
   * Get the property group that contains the specified property.
   *
   * @param property_name The name of the property.
   * @return A Result object containing the PropertyGroup object, or a KeyError
   * Status object if the property is not found.
   */
  Result<const PropertyGroup&> GetPropertyGroup(
      const std::string& property_name) const noexcept {
    if (!ContainProperty(property_name)) {
      return Status::KeyError("No property with name ", property_name,
                              " found in ", label_, " vertex.");
    }
    return property_groups_[p2group_index_.at(property_name)];
  }

  /**
   * Get the data type of the specified property.
   *
   * @param property_name The name of the property.
   * @return A Result object containing the data type of the property, or a
   * KeyError Status object if the property is not found.
   */
  inline Result<DataType> GetPropertyType(
      const std::string& property_name) const noexcept {
    if (p2type_.find(property_name) == p2type_.end()) {
      return Status::KeyError("No property with name ", property_name,
                              " found in ", label_, " vertex.");
    }
    return p2type_.at(property_name);
  }

  /**
   * Get whether the vertex info contains the specified property.
   *
   * @param property_name The name of the property.
   * @return True if the property exists in the vertex info, False otherwise.
   */
  bool ContainProperty(const std::string& property_name) const {
    return p2type_.find(property_name) != p2type_.end();
  }

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
   * @return A Result object containing a bool indicating whether the property
   * is a primary key, or a KeyError Status object if the property is not found.
   */
  inline Result<bool> IsPrimaryKey(const std::string& property_name) const
      noexcept {
    if (p2primary_.find(property_name) == p2primary_.end()) {
      return Status::KeyError("No property with name ", property_name,
                              " found in ", label_, " vertex.");
    }
    return p2primary_.at(property_name);
  }

  /**
   * Returns whether the vertex info contains the specified property group.
   *
   * @param property_group The PropertyGroup object to check for.
   * @return True if the property group exists in the vertex info, False
   * otherwise.
   */
  bool ContainPropertyGroup(const PropertyGroup& property_group) const {
    for (const auto& pg : property_groups_) {
      if (pg == property_group) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns a new VertexInfo object with the specified property group added to
   * it.
   *
   * @param property_group The PropertyGroup object to add.
   * @return A Result object containing the new VertexInfo object, or a Status
   * object indicating an error.
   */
  const Result<VertexInfo> Extend(const PropertyGroup& property_group) const
      noexcept {
    VertexInfo new_info(*this);
    GAR_RETURN_NOT_OK(new_info.AddPropertyGroup(property_group));
    return new_info;
  }

  /**
   * Get the file path for the specified property group and chunk index.
   *
   * @param property_group The PropertyGroup object to get the file path for.
   * @param chunk_index The chunk index.
   * @return A Result object containing the file path, or a KeyError Status
   * object if the property group is not found in the vertex info.
   */
  inline Result<std::string> GetFilePath(const PropertyGroup& property_group,
                                         IdType chunk_index) const noexcept {
    if (!ContainPropertyGroup(property_group)) {
      return Status::KeyError("The property group: ", property_group,
                              " is not found in the ", label_, " vertex.");
    }
    return prefix_ + property_group.GetPrefix() + "chunk" +
           std::to_string(chunk_index);
  }

  /**
   * Get the path prefix for the specified property group.
   *
   * @param property_group The PropertyGroup object to get the path prefix for.
   * @return A Result object containing the path prefix, or a KeyError Status
   * object if the property group is not found in the vertex info.
   */
  inline Result<std::string> GetPathPrefix(
      const PropertyGroup& property_group) const noexcept {
    if (!ContainPropertyGroup(property_group)) {
      return Status::KeyError("The property group: ", property_group,
                              " is not found in the ", label_, " vertex.");
    }
    return prefix_ + property_group.GetPrefix();
  }

  /**
   * Get the file path for the number of vertices.
   *
   * @return The file path for the number of vertices.
   */
  inline Result<std::string> GetVerticesNumFilePath() const noexcept {
    return prefix_ + "vertex_count";
  }

  /**
   * Returns whether the vertex info is validated.
   *
   * @return True if the vertex info is valid, False otherwise.
   */
  bool IsValidated() const noexcept {
    if (label_.empty() || chunk_size_ <= 0 || prefix_.empty()) {
      return false;
    }
    for (const auto& pg : property_groups_) {
      if (pg.GetProperties().empty()) {
        return false;
      }
      auto file_type = pg.GetFileType();
      if (file_type != FileType::CSV && file_type != FileType::PARQUET &&
          file_type != FileType::ORC) {
        return false;
      }
    }
    return true;
  }

  /**
   * Loads vertex info from a YAML object.
   *
   * @param yaml A shared pointer to a Yaml object containing the YAML string.
   * @return A Result object containing the VertexInfo object, or a Status
   * object indicating an error.
   */
  static Result<VertexInfo> Load(std::shared_ptr<Yaml> yaml);

 private:
  std::string label_;
  IdType chunk_size_;
  InfoVersion version_;
  std::string prefix_;
  std::vector<PropertyGroup> property_groups_;
  std::map<std::string, DataType> p2type_;
  std::map<std::string, bool> p2primary_;
  std::map<std::string, size_t> p2group_index_;
};

/**
 * EdgeInfo is a class that stores metadata information about an edge.
 */
class EdgeInfo {
 public:
  /**
   * Default constructor.
   */
  EdgeInfo() = default;

  /**
   * Destructor
   */
  ~EdgeInfo() {}

  /**
   * @brief Construct an EdgeInfo object with the given metadata information.
   *
   * @param src_label The label of the source vertex.
   * @param edge_label The label of the edge.
   * @param dst_label The label of the destination vertex.
   * @param chunk_size The number of edges in each edge chunk.
   * @param src_chunk_size The number of source vertices in each vertex chunk.
   * @param dst_chunk_size The number of destination vertices in each vertex
   * chunk.
   * @param directed Whether the edge is directed.
   * @param version The version of the edge info.
   * @param prefix The path prefix of the edge info.
   */
  explicit EdgeInfo(const std::string& src_label, const std::string& edge_label,
                    const std::string& dst_label, IdType chunk_size,
                    IdType src_chunk_size, IdType dst_chunk_size, bool directed,
                    const InfoVersion& version, const std::string& prefix = "")
      : src_label_(src_label),
        edge_label_(edge_label),
        dst_label_(dst_label),
        chunk_size_(chunk_size),
        src_chunk_size_(src_chunk_size),
        dst_chunk_size_(dst_chunk_size),
        directed_(directed),
        version_(version),
        prefix_(prefix) {
    if (prefix_.empty()) {
      prefix_ = src_label_ + REGULAR_SEPERATOR + edge_label_ +
                REGULAR_SEPERATOR + dst_label_ + "/";  // default prefix
    }
  }

  /**
   * Copy constructor.
   */
  EdgeInfo(const EdgeInfo& info) = default;

  /**
   * Move constructor.
   */
  explicit EdgeInfo(EdgeInfo&& info) = default;

  /**
   * Copy assignment operator.
   */
  inline EdgeInfo& operator=(const EdgeInfo& other) = default;

  /**
   * Move assignment operator.
   */
  inline EdgeInfo& operator=(EdgeInfo&& other) = default;

  /**
   * Add an adjacency list information to the edge info.
   * The adjacency list information indicating the adjacency list stored with
   * CSR, CSC, or COO format.
   *
   * @param adj_list_type The type of the adjacency list to add.
   * @param file_type The file type of the adjacency list topology and offset
   * chunk file.
   * @param prefix The prefix of the adjacency list topology chunk (optional,
   * default is empty).
   * @return A Status object indicating success or an error if the adjacency
   * list type has already been added.
   */
  Status AddAdjList(const AdjListType& adj_list_type, FileType file_type,
                    const std::string& prefix = "") {
    if (ContainAdjList(adj_list_type)) {
      return Status::KeyError(
          "Adjacency list type: ", AdjListTypeToString(adj_list_type),
          " already exists in edge info.");
    }
    if (prefix.empty()) {
      // default prefix
      adj_list2prefix_[adj_list_type] =
          std::string(AdjListTypeToString(adj_list_type)) + "/";
    } else {
      adj_list2prefix_[adj_list_type] = prefix;
    }
    adj_list2file_type_[adj_list_type] = file_type;
    adj_list2property_groups_[adj_list_type];  // init an empty property groups
    return Status::OK();
  }

  /**
   * Add a property group to edge info for the given adjacency list type.
   *
   * @param property_group Property group to add.
   * @param adj_list_type Adjacency list type to add property group to.
   * @return A Status object indicating success or an error if adj_list_type is
   * not supported by edge info or if the property group is already added to the
   * adjacency list type.
   */
  Status AddPropertyGroup(const PropertyGroup& property_group,
                          AdjListType adj_list_type) noexcept {
    if (!ContainAdjList(adj_list_type)) {
      return Status::KeyError(
          "Adjacency list type: ", AdjListTypeToString(adj_list_type),
          " is not found in ", edge_label_, " edgeinfo.");
    }
    if (ContainPropertyGroup(property_group, adj_list_type)) {
      return Status::KeyError("Property group: ", property_group,
                              " already exists in adjacency list.");
    }
    adj_list2property_groups_[adj_list_type].push_back(property_group);
    for (auto& p : property_group.GetProperties()) {
      if (!version_.CheckType(p.type.ToTypeName())) {
        return Status::Invalid("Invalid property type: ", p.type.ToTypeName());
      }
      p2type_[p.name] = p.type;
      p2primary_[p.name] = p.is_primary;
      p2group_index_[p.name][adj_list_type] =
          adj_list2property_groups_.at(adj_list_type).size() - 1;
    }
    return Status::OK();
  }

  /**
   * Get the label of the source vertex.
   * @return The label of the source vertex.
   */
  inline std::string GetSrcLabel() const { return src_label_; }

  /**
   * Get the label of the edge.
   * @return The label of the edge.
   */
  inline std::string GetEdgeLabel() const { return edge_label_; }

  /**
   * Get the label of the destination vertex.
   * @return The label of the destination vertex.
   */
  inline std::string GetDstLabel() const { return dst_label_; }

  /**
   * Get the number of edges in each edge chunk.
   * @return The number of edges in each edge chunk.
   */
  inline IdType GetChunkSize() const { return chunk_size_; }

  /**
   * Get the number of source vertices in each vertex chunk.
   * @return The number of source vertices in each vertex chunk.
   */
  inline IdType GetSrcChunkSize() const { return src_chunk_size_; }

  /**
   * Get the number of destination vertices in each vertex chunk.
   * @return The number of destination vertices in each vertex chunk.
   */
  inline IdType GetDstChunkSize() const { return dst_chunk_size_; }

  /**
   * Get the path prefix of the edge.
   * @return The path prefix of the edge.
   */
  inline std::string GetPrefix() const { return prefix_; }

  /**
   * Returns whether the edge is directed.
   * @return True if the edge is directed, false otherwise.
   */
  inline bool IsDirected() const noexcept { return directed_; }

  /**
   * Get the version info of the edge.
   * @return The version info of the edge.
   */
  inline const InfoVersion& GetVersion() const { return version_; }

  /**
   * Return whether the edge info contains the adjacency list information.
   *
   * @param adj_list_type The adjacency list type.
   * @return True if the edge info contains the adjacency list information,
   * false otherwise.
   */
  inline bool ContainAdjList(AdjListType adj_list_type) const noexcept {
    return adj_list2prefix_.find(adj_list_type) != adj_list2prefix_.end();
  }

  /**
   * Returns whether the edge info contains the given property group for the
   * specified adjacency list type.
   *
   * @param property_group Property group to check.
   * @param adj_list_type Adjacency list type the property group belongs to.
   * @return True if the edge info contains the property group, false otherwise.
   */
  inline bool ContainPropertyGroup(const PropertyGroup& property_group,
                                   AdjListType adj_list_type) const {
    if (!ContainAdjList(adj_list_type)) {
      return false;
    }
    for (auto& pg : adj_list2property_groups_.at(adj_list_type)) {
      if (pg == property_group) {
        return true;
      }
    }
    return false;
  }

  /**
   * @brief Returns whether the edge info contains the given property for any
   * adjacency list type.
   *
   * @param property Property name to check.
   * @return True if the edge info contains the property, false otherwise.
   */
  bool ContainProperty(const std::string& property) const {
    return p2type_.find(property) != p2type_.end();
  }

  /**
   * Get the file type of the adjacency list topology and offset chunk file for
   * the given adjacency list type.
   *
   * @param adj_list_type The adjacency list type.
   * @return A Result object containing the file type, or a Status object
   * indicating an KeyError if the adjacency list type is not found in the edge
   * info.
   */
  inline Result<FileType> GetFileType(AdjListType adj_list_type) const
      noexcept {
    if (!ContainAdjList(adj_list_type)) {
      return Status::KeyError(
          "Adjacency list type: ", AdjListTypeToString(adj_list_type),
          " is not found in edge info.");
    }
    return adj_list2file_type_.at(adj_list_type);
  }

  /**
   * @brief Get the property groups for the given adjacency list type.
   *
   * @param adj_list_type Adjacency list type.
   * @return A Result object containing reference to the property groups for the
   * given adjacency list type, or a Status object indicating an KeyError if the
   * adjacency list type is not found in the edge info.
   */
  inline Result<const std::vector<PropertyGroup>&> GetPropertyGroups(
      AdjListType adj_list_type) const noexcept {
    if (!ContainAdjList(adj_list_type)) {
      return Status::KeyError(
          "Adjacency list type: ", AdjListTypeToString(adj_list_type),
          " is not found in edge info.");
    }
    return adj_list2property_groups_.at(adj_list_type);
  }

  /**
   * @brief Get the property group containing the given property and for the
   * specified adjacency list type.
   *
   * @param property Property name.
   * @param adj_list_type Adjacency list type.
   * @return A Result object containing reference to the property group, or a
   * Status object indicating an KeyError if the adjacency list type is not
   * found in the edge info.
   */
  inline Result<const PropertyGroup&> GetPropertyGroup(
      const std::string& property, AdjListType adj_list_type) const noexcept {
    if (p2group_index_.find(property) == p2group_index_.end() ||
        p2group_index_.at(property).find(adj_list_type) ==
            p2group_index_.at(property).end()) {
      return Status::KeyError("No property with name: ", property,
                              " is found in edge info for adj list type: ",
                              AdjListTypeToString(adj_list_type));
    }
    return adj_list2property_groups_.at(
        adj_list_type)[p2group_index_.at(property).at(adj_list_type)];
  }

  /**
   * Get the file path for the number of vertices.
   *
   * @param adj_list_type The adjacency list type.
   * @return A Result object containing the file path for the number of edges,
   * or a Status object indicating an error.
   */
  inline Result<std::string> GetVerticesNumFilePath(
      AdjListType adj_list_type) const noexcept {
    if (!ContainAdjList(adj_list_type)) {
      return Status::KeyError(
          "Adjacency list type: ", AdjListTypeToString(adj_list_type),
          " is not found in edge info.");
    }
    return prefix_ + adj_list2prefix_.at(adj_list_type) + "vertex_count";
  }

  /**
   * Get the file path for the number of edges.
   *
   * @param vertex_chunk_index the vertex chunk index
   * @param adj_list_type The adjacency list type.
   * @return A Result object containing the file path for the number of edges,
   * or a Status object indicating an error.
   */
  inline Result<std::string> GetEdgesNumFilePath(
      IdType vertex_chunk_index, AdjListType adj_list_type) const noexcept {
    if (!ContainAdjList(adj_list_type)) {
      return Status::KeyError(
          "Adjacency list type: ", AdjListTypeToString(adj_list_type),
          " is not found in edge info.");
    }
    return prefix_ + adj_list2prefix_.at(adj_list_type) + "edge_count" +
           std::to_string(vertex_chunk_index);
  }

  /**
   * @brief Get the file path of adj list topology chunk
   *
   * @param vertex_chunk_index the vertex chunk index
   * @param edge_chunk_index index of edge adj list chunk of the vertex chunk
   * @param adj_list_type The adjacency list type.
   */
  inline Result<std::string> GetAdjListFilePath(IdType vertex_chunk_index,
                                                IdType edge_chunk_index,
                                                AdjListType adj_list_type) const
      noexcept {
    if (!ContainAdjList(adj_list_type)) {
      return Status::KeyError(
          "Adjacency list type: ", AdjListTypeToString(adj_list_type),
          " is not found in edge info.");
    }
    return prefix_ + adj_list2prefix_.at(adj_list_type) + "adj_list/part" +
           std::to_string(vertex_chunk_index) + "/" + "chunk" +
           std::to_string(edge_chunk_index);
  }

  /**
   * Get the path prefix of the adjacency list topology chunk for the given
   * adjacency list type.
   * @param adj_list_type The adjacency list type.
   * @return A Result object containing the directory, or a Status object
   * indicating an error.
   */
  inline Result<std::string> GetAdjListPathPrefix(
      const AdjListType& adj_list_type) const noexcept {
    if (!ContainAdjList(adj_list_type)) {
      return Status::KeyError(
          "Adjacency list type: ", AdjListTypeToString(adj_list_type),
          " is not found in edge info.");
    }
    return prefix_ + adj_list2prefix_.at(adj_list_type) + "adj_list/";
  }

  /**
   * @brief Get the adjacency list offset chunk file path of vertex chunk
   *    the offset chunks is aligned with the vertex chunks
   *
   * @param vertex_chunk_index index of vertex chunk
   * @param adj_list_type The adjacency list type.
   */
  inline Result<std::string> GetAdjListOffsetFilePath(
      IdType vertex_chunk_index, AdjListType adj_list_type) const noexcept {
    if (!ContainAdjList(adj_list_type)) {
      return Status::KeyError(
          "Adjacency list type: ", AdjListTypeToString(adj_list_type),
          " is not found in edge info.");
    }
    return prefix_ + adj_list2prefix_.at(adj_list_type) + "offset/chunk" +
           std::to_string(vertex_chunk_index);
  }

  /**
   * Get the path prefix of the adjacency list offset chunk for the given
   * adjacency list type.
   * @param adj_list_type The adjacency list type.
   * @return A Result object containing the path prefix, or a Status object
   * indicating an error.
   */
  inline Result<std::string> GetOffsetPathPrefix(
      AdjListType adj_list_type) const noexcept {
    if (!ContainAdjList(adj_list_type)) {
      return Status::KeyError(
          "Adjacency list type: ", AdjListTypeToString(adj_list_type),
          " is not found in edge info.");
    }
    return prefix_ + adj_list2prefix_.at(adj_list_type) + "offset/";
  }

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
  inline Result<std::string> GetPropertyFilePath(
      const PropertyGroup& property_group, AdjListType adj_list_type,
      IdType vertex_chunk_index, IdType edge_chunk_index) const {
    if (!ContainPropertyGroup(property_group, adj_list_type)) {
      return Status::KeyError("The property group: ", property_group,
                              " is not found in the edge info.");
    }
    return prefix_ + adj_list2prefix_.at(adj_list_type) +
           property_group.GetPrefix() + "part" +
           std::to_string(vertex_chunk_index) + "/chunk" +
           std::to_string(edge_chunk_index);
  }

  /**
   * Get the path prefix of the property group chunk for the given
   * adjacency list type.
   * @param property_group property group.
   * @param adj_list_type The adjacency list type.
   * @return A Result object containing the path prefix, or a Status object
   * indicating an error.
   */
  inline Result<std::string> GetPropertyGroupPathPrefix(
      const PropertyGroup& property_group, AdjListType adj_list_type) const
      noexcept {
    if (!ContainPropertyGroup(property_group, adj_list_type)) {
      return Status::KeyError("The property group: ", property_group,
                              " is not found in the edge info");
    }
    return prefix_ + adj_list2prefix_.at(adj_list_type) +
           property_group.GetPrefix();
  }

  /**
   * Get the data type of the specified property.
   *
   * @param property_name The name of the property.
   * @return A Result object containing the data type of the property, or a
  KeyError Status object if the property is not found.
   */
  Result<DataType> GetPropertyType(const std::string& property) const noexcept {
    if (p2type_.find(property) == p2type_.end()) {
      return Status::KeyError("No property with name ", property,
                              " is not found in the edge info");
    }
    return p2type_.at(property);
  }

  /**
   * Returns whether the specified property is a primary key.
   *
   * @param property_name The name of the property.
   * @return A Result object containing a bool indicating whether the property
   * is a primary key, or a KeyError Status object if the property is not found.
   */
  Result<bool> IsPrimaryKey(const std::string& property) const noexcept {
    if (p2primary_.find(property) == p2primary_.end()) {
      return Status::KeyError("No property with name ", property,
                              " is not found in the edge info");
    }
    return p2primary_.at(property);
  }

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
   * Returns a new EdgeInfo object with the specified adjacency list type
   * added to with given metadata.
   *
   * @param adj_list_type The type of the adjacency list to add.
   * @param file_type The file type of the adjacency list topology and offset
   * chunk file.
   * @param prefix The prefix of the adjacency list topology chunk (optional,
   * default is empty).
   * @return A Result object containing the new EdgeInfo object, or a Status
   * object indicating an error.
   */
  const Result<EdgeInfo> ExtendAdjList(AdjListType adj_list_type,
                                       FileType file_type,
                                       const std::string& prefix = "") const
      noexcept {
    EdgeInfo new_info(*this);
    GAR_RETURN_NOT_OK(new_info.AddAdjList(adj_list_type, file_type, prefix));
    return new_info;
  }

  /**
   * Returns a new EdgeInfo object with the specified property group added to
   * given adjacency list type.
   *
   * @param property_group The PropertyGroup object to add.
   * @param adj_list_type The adjacency list type to add the property group to.
   * @return A Result object containing the new EdgeInfo object, or a Status
   * object indicating an error.
   */
  const Result<EdgeInfo> ExtendPropertyGroup(
      const PropertyGroup& property_group, AdjListType adj_list_type) const
      noexcept {
    EdgeInfo new_info(*this);
    GAR_RETURN_NOT_OK(new_info.AddPropertyGroup(property_group, adj_list_type));
    return new_info;
  }

  /**
   * Returns whether the edge info is validated.
   *
   * @return True if the edge info is valid, False otherwise.
   */
  bool IsValidated() const noexcept {
    if (src_label_.empty() || edge_label_.empty() || dst_label_.empty()) {
      return false;
    }
    if (chunk_size_ <= 0 || src_chunk_size_ <= 0 || dst_chunk_size_ <= 0) {
      return false;
    }
    for (const auto& item : adj_list2prefix_) {
      if (item.second.empty()) {
        return false;
      }
    }
    for (const auto& item : adj_list2file_type_) {
      if (item.second != FileType::CSV && item.second != FileType::PARQUET &&
          item.second != FileType::ORC) {
        return false;
      }
    }
    for (const auto& item : adj_list2property_groups_) {
      for (const auto& pg : item.second) {
        if (pg.GetProperties().empty()) {
          return false;
        }
        auto file_type = pg.GetFileType();
        if (file_type != FileType::CSV && file_type != FileType::PARQUET &&
            file_type != FileType::ORC) {
          // file type not validated
          return false;
        }
      }
    }
    return true;
  }

  /** Loads the yaml as an EdgeInfo instance. */
  static Result<EdgeInfo> Load(std::shared_ptr<Yaml> yaml);

 private:
  std::string src_label_;
  std::string edge_label_;
  std::string dst_label_;
  IdType chunk_size_, src_chunk_size_, dst_chunk_size_;
  bool directed_;
  InfoVersion version_;
  std::string prefix_;
  std::map<std::string, DataType> p2type_;
  std::map<std::string, bool> p2primary_;
  std::map<std::string, std::map<AdjListType, size_t>> p2group_index_;
  std::map<AdjListType, std::string> adj_list2prefix_;
  std::map<AdjListType, FileType> adj_list2file_type_;
  std::map<AdjListType, std::vector<PropertyGroup>> adj_list2property_groups_;
};

/**
 * GraphInfo is a class to store the graph meta information.
 */
class GraphInfo {
 public:
  /**
   * @brief Constructs a GraphInfo instance.
   * @param graph_name The name of the graph.
   * @param version The version of the graph info.
   * @param prefix The absolute path prefix to store chunk files of the graph.
   *               Defaults to "./".
   */
  explicit GraphInfo(const std::string& graph_name, const InfoVersion& version,
                     const std::string& prefix = "./")
      : name_(graph_name), version_(version), prefix_(prefix) {}

  /**
   * @brief Loads the input file as a `GraphInfo` instance.
   * @param path The path of the YAML file.
   * @return A Result object containing the GraphInfo instance, or a Status
   * object indicating an error.
   */
  static Result<GraphInfo> Load(const std::string& path);

  /**
   * @brief Loads the input string as a `GraphInfo` instance.
   * @param input The YAML content string.
   * @param relative_path The relative path to access vertex/edge YAML.
   * @return A Result object containing the GraphInfo instance, or a `Status`
   * object indicating an error.
   */
  static Result<GraphInfo> Load(const std::string& input,
                                const std::string& relative_path);

  /**
   * @brief Adds a vertex info to the GraphInfo instance.
   * @param vertex_info The vertex info to add.
   * @return A Status object indicating the success or failure of the
   * operation. Returns InvalidOperation if the vertex info is already
   * contained.
   */
  Status AddVertex(const VertexInfo& vertex_info) noexcept {
    std::string label = vertex_info.GetLabel();
    if (vertex2info_.find(label) != vertex2info_.end()) {
      return Status::Invalid("The vertex info of ", label,
                             " is already existed.");
    }
    vertex2info_.emplace(label, vertex_info);
    return Status::OK();
  }

  /**
   * @brief Adds an edge info to the GraphInfo instance.
   * @param edge_info The edge info to add.
   * @return A Status object indicating the success or failure of the
   * operation. Returns `InvalidOperation` if the edge info is already
   * contained.
   */
  Status AddEdge(const EdgeInfo& edge_info) noexcept {
    std::string key = edge_info.GetSrcLabel() + REGULAR_SEPERATOR +
                      edge_info.GetEdgeLabel() + REGULAR_SEPERATOR +
                      edge_info.GetDstLabel();
    if (edge2info_.find(key) != edge2info_.end()) {
      return Status::Invalid("The edge info of ", key, " is already existed.");
    }
    edge2info_.emplace(key, edge_info);
    return Status::OK();
  }

  /**
   *@brief Add a vertex info path to graph info instance.
   *
   *@param path The vertex info path to add
   */
  void AddVertexInfoPath(const std::string& path) noexcept {
    vertex_paths_.push_back(path);
  }

  /**
   *@brief Add an edge info path to graph info instance.
   *
   *@param path The edge info path to add
   */
  void AddEdgeInfoPath(const std::string& path) noexcept {
    edge_paths_.push_back(path);
  }

  /**
   * @brief Get the name of the graph.
   * @return The name of the graph.
   */
  inline std::string GetName() const noexcept { return name_; }

  /**
   * @brief Get the absolute path prefix of the chunk files.
   * @return The absolute path prefix of the chunk files.
   */
  inline std::string GetPrefix() const noexcept { return prefix_; }

  /**
   * Get the version info of the graph info object.
   *
   * @return The version info of the graph info object.
   */
  inline const InfoVersion& GetVersion() const { return version_; }

  /**
   * Get the vertex info with the given label.
   * @param label The label of the vertex.
   * @return A Result object containing the vertex info, or a Status object
   * indicating an error.
   */
  inline Result<const VertexInfo&> GetVertexInfo(const std::string& label) const
      noexcept {
    if (vertex2info_.find(label) == vertex2info_.end()) {
      return Status::KeyError("The vertex info of ", label,
                              " is not found in graph info.");
    }
    return vertex2info_.at(label);
  }

  /**
   * Get the edge info with the given source vertex label, edge label, and
   * destination vertex label.
   * @param src_label The label of the source vertex.
   * @param edge_label The label of the edge.
   * @param dst_label The label of the destination vertex.
   * @return A Result object containing the edge info, or a Status object
   * indicating an error.
   */
  inline Result<const EdgeInfo&> GetEdgeInfo(const std::string& src_label,
                                             const std::string& edge_label,
                                             const std::string& dst_label) const
      noexcept {
    std::string key = src_label + REGULAR_SEPERATOR + edge_label +
                      REGULAR_SEPERATOR + dst_label;
    if (edge2info_.find(key) == edge2info_.end()) {
      return Status::KeyError("The edge info of ", key,
                              " is not found in graph info.");
    }
    return edge2info_.at(key);
  }

  /**
   *@brief Get the property group of vertex by label and property
   *
   *@param label vertex label
   *@param property vertex property that belongs to the group
   */
  inline Result<const PropertyGroup&> GetVertexPropertyGroup(
      const std::string& label, const std::string& property) const noexcept {
    if (vertex2info_.find(label) == vertex2info_.end()) {
      return Status::KeyError("The vertex info of ", label,
                              " is not found in graph info.");
    }
    return vertex2info_.at(label).GetPropertyGroup(property);
  }

  /**
   *@brief Get the property group of edge by label, property and adj list type
   *
   *@param src_label source vertex label
   *@param edge_label edge label
   *@param dst_label destination vertex label
   *@param property edge property that belongs to the group
   *@param adj_list_type adj list type of edge
   */
  inline Result<const PropertyGroup&> GetEdgePropertyGroup(
      const std::string& src_label, const std::string& edge_label,
      const std::string& dst_label, const std::string& property,
      AdjListType adj_list_type) const noexcept {
    std::string key = src_label + REGULAR_SEPERATOR + edge_label +
                      REGULAR_SEPERATOR + dst_label;
    if (edge2info_.find(key) == edge2info_.end()) {
      return Status::KeyError("The edge info of ", key,
                              " is not found in graph info.");
    }
    return edge2info_.at(key).GetPropertyGroup(property, adj_list_type);
  }

  /**
   * @brief Get the vertex infos of graph info
   *
   * @return vertex infos of graph info
   */
  inline const std::map<std::string, VertexInfo>& GetVertexInfos() const
      noexcept {
    return vertex2info_;
  }

  /**
   * @brief Get the edge infos of graph info
   *
   * @return edge infos of graph info
   */
  inline const std::map<std::string, EdgeInfo>& GetEdgeInfos() const noexcept {
    return edge2info_;
  }

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
  Result<std::string> Dump() const noexcept;

  /**
   * Returns whether the graph info is validated.
   *
   * @return True if the graph info is valid, False otherwise.
   */
  inline bool IsValidated() const noexcept {
    if (name_.empty() || prefix_.empty()) {
      return false;
    }
    for (const auto& vertex_info : vertex2info_) {
      if (!vertex_info.second.IsValidated()) {
        return false;
      }
    }
    for (const auto& edge_info : edge2info_) {
      if (!edge_info.second.IsValidated()) {
        return false;
      }
    }
    return true;
  }

 private:
  std::string name_;
  InfoVersion version_;
  std::string prefix_;
  std::map<std::string, VertexInfo> vertex2info_;  // label -> info
  std::map<std::string, EdgeInfo>
      edge2info_;  // "person_knows_person" ->  EdgeInfo of (person, knows,
                   // person)
  std::vector<std::string> vertex_paths_, edge_paths_;
};

}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_GRAPH_INFO_H_
