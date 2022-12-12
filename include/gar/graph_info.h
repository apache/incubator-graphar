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
#include <vector>

#include "utils/adj_list_type.h"
#include "utils/data_type.h"
#include "utils/file_type.h"
#include "utils/result.h"
#include "utils/status.h"
#include "utils/utils.h"
#include "utils/version_parser.h"
#include "utils/yaml.h"

namespace GAR_NAMESPACE_INTERNAL {

class Yaml;

/// Property is a struct to store the property information.
struct Property {
  std::string name;  // property name
  DataType type;     // property data type
  bool is_primary;   // primary key tag
};

static bool operator==(const Property& lhs, const Property& rhs) {
  return (lhs.name == rhs.name) && (lhs.type.Equals(rhs.type)) &&
         (lhs.is_primary == rhs.is_primary);
}

/// PropertyGroup is a class to store the property group information.
class PropertyGroup {
 public:
  /// Default constructor
  PropertyGroup() = default;

  ~PropertyGroup() {}

  /**
   * Initialize the PropertyGroup
   *
   * @param properties Property list of group
   * @param file_type File type of property group chunk file
   * @param prefix prefix of property group chunk file [Option]. the default
   *        prefix is the concatenation of property names with '_' as separator
   */
  explicit PropertyGroup(std::vector<Property> properties, FileType file_type,
                         const std::string& prefix = "")
      : properties_(properties), file_type_(file_type), prefix_(prefix) {
    if (prefix_.empty()) {
      std::vector<std::string> names;
      for (auto& property : properties_) {
        names.push_back(property.name);
      }
      prefix_ = util::ConcatStringWithDelimiter(names, REGULAR_SEPERATOR) + "/";
    }
  }

  /// Copy constructor
  PropertyGroup(const PropertyGroup& other) = default;
  /// Move constructor
  PropertyGroup(PropertyGroup&& other) = default;

  /// Copy assignment operator
  inline PropertyGroup& operator=(const PropertyGroup& other) = default;
  /// Move assignment operator
  inline PropertyGroup& operator=(PropertyGroup&& other) = default;

  /// Get the property list of group
  inline const std::vector<Property>& GetProperties() const {
    return properties_;
  }

  /// Get the file type of property group chunk file
  inline FileType GetFileType() const { return file_type_; }

  /// Get the prefix of property group chunk file
  inline const std::string& GetPrefix() const { return prefix_; }

 private:
  std::vector<Property> properties_;
  FileType file_type_;
  std::string prefix_;
};

static bool operator==(const PropertyGroup& lhs, const PropertyGroup& rhs) {
  return (lhs.GetPrefix() == rhs.GetPrefix()) &&
         (lhs.GetFileType() == rhs.GetFileType()) &&
         (lhs.GetProperties() == rhs.GetProperties());
}

/// VertexInfo is a class to store the vertex meta information.
class VertexInfo {
 public:
  /// Default constructor
  VertexInfo() = default;

  ~VertexInfo() {}

  /**
   * @brief Initialize the vertex info.
   *
   * @param label The label of the vertex.
   * @param chunk_size number of vertex in each vertex chunk.
   * @param version version of the vertex info.
   * @param prefix prefix of the vertex info.
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

  /// Copy constructor
  VertexInfo(const VertexInfo& vertex_info) = default;

  /// Move constructor
  explicit VertexInfo(VertexInfo&& vertex_info) = default;

  /// Copy assignment operator
  inline VertexInfo& operator=(const VertexInfo& other) = default;

  /// Move assignment operator
  inline VertexInfo& operator=(VertexInfo&& other) = default;

  /// Add a property group to vertex info
  inline Status AddPropertyGroup(const PropertyGroup& property_group) {
    if (ContainPropertyGroup(property_group)) {
      return Status::InvalidOperation(
          "The property group has already existed, can't not be added again.");
    }
    for (const auto& property : property_group.GetProperties()) {
      if (ContainProperty(property.name)) {
        std::string err_msg = "The property " + property.name +
                              " has already existed in the vertex info.";
        return Status::InvalidOperation(err_msg);
      }
    }

    property_groups_.push_back(property_group);
    for (const auto& p : property_group.GetProperties()) {
      if (!version_.CheckType(p.type.ToTypeName())) {
        return Status::Invalid(
            "The property type is not supported by the version.");
      }
      p2type_[p.name] = p.type;
      p2primary_[p.name] = p.is_primary;
      p2group_index_[p.name] = property_groups_.size() - 1;
    }
    return Status::OK();
  }

  /// Get the label of the vertex.
  inline std::string GetLabel() const { return label_; }

  /// Get the chunk size of the vertex.
  inline IdType GetChunkSize() const { return chunk_size_; }

  /// Get the path prefix of the vertex.
  inline std::string GetPrefix() const { return prefix_; }

  /// Get the property groups of the vertex.
  inline const std::vector<PropertyGroup>& GetPropertyGroups() const {
    return property_groups_;
  }

  /// Get the property group that contains property
  Result<const PropertyGroup&> GetPropertyGroup(
      const std::string& property_name) const noexcept {
    if (!ContainProperty(property_name)) {
      return Status::KeyError("The property is not found.");
    }
    return property_groups_[p2group_index_.at(property_name)];
  }

  /// Get the data type of property
  inline Result<DataType> GetPropertyType(
      const std::string& property_name) const noexcept {
    if (p2type_.find(property_name) == p2type_.end()) {
      return Status::KeyError("The property is not found.");
    }
    return p2type_.at(property_name);
  }

  /// Check if the vertex info contains certain property.
  bool ContainProperty(const std::string& property_name) const {
    return p2type_.find(property_name) != p2type_.end();
  }

  /// Save the vertex info to yaml file
  Status Save(const std::string& path) const;

  /// Dump the vertex info to yaml format string
  Result<std::string> Dump() const noexcept;

  /// Check if the property is primary key or not
  inline Result<bool> IsPrimaryKey(const std::string& property_name) const
      noexcept {
    if (p2primary_.find(property_name) == p2primary_.end()) {
      return Status::KeyError("The property is not found.");
    }
    return p2primary_.at(property_name);
  }

  /// Check if the vertex info contains the property group.
  bool ContainPropertyGroup(const PropertyGroup& property_group) const {
    for (const auto& pg : property_groups_) {
      if (pg == property_group) {
        return true;
      }
    }
    return false;
  }

  /// Extending the property groups of vertex and return a new vertex info
  const Result<VertexInfo> Extend(const PropertyGroup& property_group) const
      noexcept {
    VertexInfo new_info(*this);
    GAR_RETURN_NOT_OK(new_info.AddPropertyGroup(property_group));
    return new_info;
  }

  /// Get the chunk file path of property group of vertex chunk
  inline Result<std::string> GetFilePath(const PropertyGroup& property_group,
                                         IdType chunk_index) const noexcept {
    if (!ContainPropertyGroup(property_group)) {
      return Status::KeyError(
          "Vertex info does not contain the property group.");
    }
    return prefix_ + property_group.GetPrefix() + "part" +
           std::to_string(chunk_index) + "/" + "chunk0";
  }

  /// Get the chunk files directory path of property group
  inline Result<std::string> GetDirPath(
      const PropertyGroup& property_group) const noexcept {
    if (!ContainPropertyGroup(property_group)) {
      return Status::KeyError(
          "Vertex info does not contain the property group.");
    }

    return prefix_ + property_group.GetPrefix();
  }

  /// Get the chunk file path of the number of vertices
  inline Result<std::string> GetVerticesNumFilePath() const noexcept {
    return prefix_ + "vertex_count";
  }

  /// Check if the vertex info is validated
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

  /// Load the input yaml as a VertexInfo instance.
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

/// Edge info is a class to store the edge meta information.
class EdgeInfo {
 public:
  /// Default constructor
  EdgeInfo() = default;

  ~EdgeInfo() {}

  /**
   * @brief Initialize the EdgeInfo.
   *
   * @param src_label source vertex label
   * @param edge_label edge label
   * @param dst_label destination vertex label
   * @param chunk_size number of edges in each edge chunk
   * @param src_chunk_size number of source vertices in each vertex chunk
   * @param dst_chunk_size number of destination vertices in each vertex chunk
   * @param directed whether the edge is directed
   * @param version version of the edge info
   * @param prefix prefix of the edge info
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

  /// Copy constructor
  EdgeInfo(const EdgeInfo& info) = default;

  /// Move constructor
  explicit EdgeInfo(EdgeInfo&& info) = default;

  /// Copy assignment operator
  inline EdgeInfo& operator=(const EdgeInfo& other) = default;

  /// Move assignment operator
  inline EdgeInfo& operator=(EdgeInfo&& other) = default;

  /**
   * @brief Add adj list information to edge info
   *      The adj list information is used to store the edge list by CSR, CSC
   *      or COO format.
   *
   * @param adj_list_type adj list type to add
   * @param file_type the file type of adj list topology and offset chunk file
   * @param prefix prefix of adj list topology chunk, optional, default is empty
   * @return InvalidOperation if the adj list type is already added
   */
  Status AddAdjList(const AdjListType& adj_list_type, FileType file_type,
                    const std::string& prefix = "") {
    if (ContainAdjList(adj_list_type)) {
      return Status::InvalidOperation(
          "The adj list type has already existed in edge info.");
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
   * @brief Add a property group to edge info
   *    Each adj list type has its own property groups.
   *
   * @param property_group property group to add
   * @param adj_list_type adj list type to add property group to
   * @return InvalidOperation if adj_list_type not support or
   *    the property group is already added to the adj list type
   */
  Status AddPropertyGroup(const PropertyGroup& property_group,
                          AdjListType adj_list_type) noexcept {
    if (!ContainAdjList(adj_list_type)) {
      return Status::InvalidOperation(
          "The adj list type not supported by edge info.");
    }
    if (ContainPropertyGroup(property_group, adj_list_type)) {
      return Status::InvalidOperation(
          "The property group has already existed.");
    }
    adj_list2property_groups_[adj_list_type].push_back(property_group);
    for (auto& p : property_group.GetProperties()) {
      if (!version_.CheckType(p.type.ToTypeName())) {
        return Status::Invalid(
            "The property type is not supported by the version.");
      }
      p2type_[p.name] = p.type;
      p2primary_[p.name] = p.is_primary;
      p2group_index_[p.name][adj_list_type] =
          adj_list2property_groups_.at(adj_list_type).size() - 1;
    }
    return Status::OK();
  }

  /// Get source vertex label of edge.
  inline std::string GetSrcLabel() const { return src_label_; }

  /// Get edge label of edge.
  inline std::string GetEdgeLabel() const { return edge_label_; }

  /// Get destination vertex label of edge.
  inline std::string GetDstLabel() const { return dst_label_; }

  /// Get chunk size of edge.
  inline IdType GetChunkSize() const { return chunk_size_; }

  /// Get chunk size of source vertex.
  inline IdType GetSrcChunkSize() const { return src_chunk_size_; }

  /// Get chunk size of destination vertex.
  inline IdType GetDstChunkSize() const { return dst_chunk_size_; }

  /// Get path prefix of edge.
  inline std::string GetPrefix() const { return prefix_; }

  /// Check if edge is directed.
  inline bool IsDirected() const noexcept { return directed_; }

  /// Get path prefix of adj list type.
  inline Result<std::string> GetAdjListPrefix(AdjListType adj_list_type) const {
    if (!ContainAdjList(adj_list_type)) {
      return Status::KeyError("The adj list type is not found in edge info.");
    }
    return adj_list2prefix_.at(adj_list_type);
  }

  /// Check if the edge info contains the adj list type
  inline bool ContainAdjList(AdjListType adj_list_type) const noexcept {
    return adj_list2prefix_.find(adj_list_type) != adj_list2prefix_.end();
  }

  /**
   * @brief Check if the edge info contains the property group
   *     if adj_list_type is not supported by edge info, return false
   *
   * @param property_group property group to check
   * @param adj_list_type the adj list type property group belongs to
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

  /// Check if the edge info contains the property
  bool ContainProperty(const std::string& property) const {
    return p2type_.find(property) != p2type_.end();
  }

  /// Get the adj list topology chunk file type of adj list type
  inline Result<FileType> GetAdjListFileType(AdjListType adj_list_type) const
      noexcept {
    if (!ContainAdjList(adj_list_type)) {
      return Status::KeyError("The adj list type is not found in edge info.");
    }
    return adj_list2file_type_.at(adj_list_type);
  }

  /// Get the property groups of adj list type
  ///   if adj_list_type is not supported by edge info, return error.
  inline Result<const std::vector<PropertyGroup>&> GetPropertyGroups(
      AdjListType adj_list_type) const noexcept {
    if (!ContainAdjList(adj_list_type)) {
      return Status::KeyError("The adj list type is not found in edge info.");
    }
    return adj_list2property_groups_.at(adj_list_type);
  }

  /**
   * @brief Return property group that contains certain property and with adj
   * list type
   *
   * @param property property name
   * @param adj_list_type adj list type of the property group
   */
  inline Result<const PropertyGroup&> GetPropertyGroup(
      const std::string& property, AdjListType adj_list_type) const noexcept {
    if (p2group_index_.find(property) == p2group_index_.end()) {
      return Status::KeyError("The property is not found.");
    }
    if (p2group_index_.at(property).find(adj_list_type) ==
        p2group_index_.at(property).end()) {
      return Status::KeyError("The property is not contained in the adj list.");
    }
    return adj_list2property_groups_.at(
        adj_list_type)[p2group_index_.at(property).at(adj_list_type)];
  }

  /**
   * @brief Get the file path of adj list topology chunk
   *
   * @param vertex_chunk_index the vertex chunk index
   * @param edge_chunk_index index of edge adj list chunk of the vertex chunk
   */
  inline Result<std::string> GetAdjListFilePath(IdType vertex_chunk_index,
                                                IdType edge_chunk_index,
                                                AdjListType adj_list_type) const
      noexcept {
    if (!ContainAdjList(adj_list_type)) {
      return Status::KeyError("The adj list type is not found in edge info.");
    }
    return prefix_ + adj_list2prefix_.at(adj_list_type) + "adj_list/part" +
           std::to_string(vertex_chunk_index) + "/" + "chunk" +
           std::to_string(edge_chunk_index);
  }

  /// Get the adj list topology chunk file directory path of adj list type
  inline Result<std::string> GetAdjListDirPath(AdjListType adj_list_type) const
      noexcept {
    if (!ContainAdjList(adj_list_type)) {
      return Status::KeyError("The adj list type is not found in edge info.");
    }
    return prefix_ + adj_list2prefix_.at(adj_list_type) + "adj_list/";
  }

  /**
   * @brief Get the adj list offset chunk file path of vertex chunk
   *    the offset chunks is aligned with the vertex chunks
   *
   * @param vertex_chunk_index index of vertex chunk
   */
  inline Result<std::string> GetAdjListOffsetFilePath(
      IdType vertex_chunk_index, AdjListType adj_list_type) const noexcept {
    if (!ContainAdjList(adj_list_type)) {
      return Status::KeyError("The adj list type is not found in edge info.");
    }
    return prefix_ + adj_list2prefix_.at(adj_list_type) + "offset/part" +
           std::to_string(vertex_chunk_index) + "/" + "chunk0";
  }

  /// Get the adj list offset chunk file directory path of adj list type
  inline Result<std::string> GetAdjListOffsetDirPath(
      AdjListType adj_list_type) const noexcept {
    if (!ContainAdjList(adj_list_type)) {
      return Status::KeyError("The adj list type is not found in edge info.");
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
      return Status::KeyError(
          "The edge info does not contain the property group.");
    }
    return prefix_ + adj_list2prefix_.at(adj_list_type) +
           property_group.GetPrefix() + "part" +
           std::to_string(vertex_chunk_index) + "/chunk" +
           std::to_string(edge_chunk_index);
  }

  /// Get the property group chunk file directory path of adj list type
  inline Result<std::string> GetPropertyDirPath(
      const PropertyGroup& property_group, AdjListType adj_list_type) const
      noexcept {
    if (!ContainPropertyGroup(property_group, adj_list_type)) {
      return Status::KeyError(
          "The edge info does not contain the property group.");
    }
    return prefix_ + adj_list2prefix_.at(adj_list_type) +
           property_group.GetPrefix();
  }

  /// Get the data type of property
  Result<DataType> GetPropertyType(const std::string& property) const noexcept {
    if (p2type_.find(property) == p2type_.end()) {
      return Status::KeyError("The property is not found.");
    }
    return p2type_.at(property);
  }

  /// Check if the property is primary key
  Result<bool> IsPrimaryKey(const std::string& property) const noexcept {
    if (p2primary_.find(property) == p2primary_.end()) {
      return Status::KeyError("The property is not found.");
    }
    return p2primary_.at(property);
  }

  /// Save the edge info to yaml file
  Status Save(const std::string& path) const;

  /// Dump the vertex info to yaml format string
  Result<std::string> Dump() const noexcept;

  /**
   * @brief Extend the adj list type of edge info and return a new edge info
   *     return error if the adj list type is already contained
   *
   * @param adj_list_type adj list type to extend
   * @param prefix path prefix of adj list type
   * @param file_type file type of adj list topology and offset chunks
   * @return new edge info
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
   * @brief Extend the property groups of adj list type and return a new edge
   * info return error if the property group is already contained or the adj
   * list type is not contained
   * @param property_group property group to extend.
   * @param adj_list_type the adj list type of property group
   * @return new edge info
   */
  const Result<EdgeInfo> ExtendPropertyGroup(
      const PropertyGroup& property_group, AdjListType adj_list_type) const
      noexcept {
    EdgeInfo new_info(*this);
    GAR_RETURN_NOT_OK(new_info.AddPropertyGroup(property_group, adj_list_type));
    return new_info;
  }

  /// Check if the edge info is validated
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

  /// Loads the yaml as a EdgeInfo instance.
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

/// GraphInfo is is a class to store the graph meta information.
class GraphInfo {
 public:
  /**
   * @brief Initialize the GraphInfo.
   *      the prefix of graph would be ./ by default.
   *
   * @param[in] graph_name name of graph
   * @param[in] version version of graph info
   * @param[in] prefix absolute path prefix to store chunk files of graph.
   */
  explicit GraphInfo(const std::string& graph_name, const InfoVersion& version,
                     const std::string& prefix = "./")
      : name_(graph_name), version_(version), prefix_(prefix) {}

  /**
   * @brief Loads the input file as a GraphInfo instance.
   *
   * @param[in] path path of yaml file.
   */
  static Result<GraphInfo> Load(const std::string& path);

  /**
   * @brief Loads the input string as a GraphInfo instance.
   *
   * @param[in] content yaml content string.
   * @param[in] relative_path relative path to access vertex/edge yaml.
   */
  static Result<GraphInfo> Load(const std::string& input,
                                const std::string& relative_path);

  /// Add a vertex info to graph info
  Status AddVertex(const VertexInfo& vertex_info) noexcept {
    std::string label = vertex_info.GetLabel();
    if (vertex2info_.find(label) != vertex2info_.end()) {
      return Status::InvalidOperation("The vertex info is already contained.");
    }
    vertex2info_.emplace(label, vertex_info);
    return Status::OK();
  }

  /// Add an edge info to graph info
  Status AddEdge(const EdgeInfo& edge_info) noexcept {
    std::string key = edge_info.GetSrcLabel() + REGULAR_SEPERATOR +
                      edge_info.GetEdgeLabel() + REGULAR_SEPERATOR +
                      edge_info.GetDstLabel();
    if (edge2info_.find(key) != edge2info_.end()) {
      return Status::InvalidOperation("The edge info is already contained.");
    }
    edge2info_.emplace(key, edge_info);
    return Status::OK();
  }

  /**
   *@brief Add a vertex info path to graph info
   *
   *@param path vertex info path to add
   */
  void AddVertexInfoPath(const std::string& path) noexcept {
    vertex_paths_.push_back(path);
  }

  /**
   *@brief Add a edge info path to graph info
   *
   *@param path edge info path to add
   */
  void AddEdgeInfoPath(const std::string& path) noexcept {
    edge_paths_.push_back(path);
  }

  /// Get the name of graph
  inline std::string GetName() const noexcept { return name_; }

  /// Get the absolute path prefix of chunk files.
  inline std::string GetPrefix() const noexcept { return prefix_; }

  /// Get the vertex info by vertex label
  inline Result<const VertexInfo&> GetVertexInfo(const std::string& label) const
      noexcept {
    if (vertex2info_.find(label) == vertex2info_.end()) {
      return Status::KeyError("The vertex info is not found in graph info.");
    }
    return vertex2info_.at(label);
  }

  /**
   *@brief Get the edge info by src label, edge label and dst label
   *
   *@param src_label source vertex label
   *@param edge_label edge label
   *@param dst_label destination vertex label
   */
  inline Result<const EdgeInfo&> GetEdgeInfo(const std::string& src_label,
                                             const std::string& edge_label,
                                             const std::string& dst_label) const
      noexcept {
    std::string key = src_label + REGULAR_SEPERATOR + edge_label +
                      REGULAR_SEPERATOR + dst_label;
    if (edge2info_.find(key) == edge2info_.end()) {
      return Status::KeyError("The edge info is not found in graph info.");
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
      return Status::KeyError("The vertex info is not found in graph info.");
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
      return Status::KeyError("The edge info is not found in graph info.");
    }
    return edge2info_.at(key).GetPropertyGroup(property, adj_list_type);
  }

  /// Get all vertex info of graph.
  inline const std::map<std::string, VertexInfo>& GetAllVertexInfo() const
      noexcept {
    return vertex2info_;
  }

  /// Get all edge info of graph.
  inline const std::map<std::string, EdgeInfo>& GetAllEdgeInfo() const
      noexcept {
    return edge2info_;
  }

  /// Save the graph info to yaml file
  Status Save(const std::string& path) const;

  /// Dump the graph info to yaml string
  Result<std::string> Dump() const noexcept;

  /// Check if the graph info is validated
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
