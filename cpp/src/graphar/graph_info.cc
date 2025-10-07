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

#include <unordered_set>
#include <utility>

#include "graphar/status.h"
#include "mini-yaml/yaml/Yaml.hpp"

#include "graphar/filesystem.h"
#include "graphar/graph_info.h"
#include "graphar/result.h"
#include "graphar/types.h"
#include "graphar/version_parser.h"
#include "graphar/yaml.h"

namespace graphar {

#define CHECK_HAS_ADJ_LIST_TYPE(adj_list_type)                         \
  do {                                                                 \
    if (!HasAdjacentListType(adj_list_type)) {                         \
      return Status::KeyError(                                         \
          "Adjacency list type: ", AdjListTypeToString(adj_list_type), \
          " is not found in edge info.");                              \
    }                                                                  \
  } while (false)

namespace {

std::string ConcatEdgeTriple(const std::string& src_type,
                             const std::string& edge_type,
                             const std::string& dst_type) {
  return src_type + REGULAR_SEPARATOR + edge_type + REGULAR_SEPARATOR +
         dst_type;
}

template <int NotFoundValue = -1>
int LookupKeyIndex(const std::unordered_map<std::string, int>& key_to_index,
                   const std::string& type) {
  auto it = key_to_index.find(type);
  if (it == key_to_index.end()) {
    return NotFoundValue;
  }
  return it->second;
}

template <typename T>
std::vector<T> AddVectorElement(const std::vector<T>& values, T new_element) {
  std::vector<T> out;
  out.reserve(values.size() + 1);
  for (size_t i = 0; i < values.size(); ++i) {
    out.push_back(values[i]);
  }
  out.emplace_back(std::move(new_element));
  return out;
}

template <typename T>
std::vector<T> RemoveVectorElement(const std::vector<T>& values, size_t index) {
  if (index >= values.size()) {
    return values;
  }
  std::vector<T> out;
  out.reserve(values.size() - 1);
  for (size_t i = 0; i < values.size(); ++i) {
    if (i != index) {
      out.push_back(values[i]);
    }
  }
  return out;
}

std::string BuildPath(const std::vector<std::string>& paths) {
  std::string path;
  for (const auto& p : paths) {
    if (p.back() == '/') {
      path += p;
    } else {
      path += p + "/";
    }
  }
  return path;
}
}  // namespace

bool operator==(const Property& lhs, const Property& rhs) {
  return (lhs.name == rhs.name) && (lhs.type == rhs.type) &&
         (lhs.is_primary == rhs.is_primary) &&
         (lhs.is_nullable == rhs.is_nullable) &&
         (lhs.cardinality == rhs.cardinality);
}

PropertyGroup::PropertyGroup(const std::vector<Property>& properties,
                             FileType file_type, const std::string& prefix)
    : properties_(properties), file_type_(file_type), prefix_(prefix) {
  if (prefix_.empty() && !properties_.empty()) {
    for (const auto& p : properties_) {
      prefix_ += p.name + REGULAR_SEPARATOR;
    }
    prefix_.back() = '/';
  }
}

const std::vector<Property>& PropertyGroup::GetProperties() const {
  return properties_;
}

bool PropertyGroup::HasProperty(const std::string& property_name) const {
  for (const auto& p : properties_) {
    if (p.name == property_name) {
      return true;
    }
  }
  return false;
}

bool PropertyGroup::IsValidated() const {
  if (prefix_.empty() ||
      (file_type_ != FileType::CSV && file_type_ != FileType::PARQUET &&
       file_type_ != FileType::ORC)) {
    return false;
  }
  if (properties_.empty()) {
    return false;
  }
  std::unordered_set<std::string> check_property_unique_set;
  for (const auto& p : properties_) {
    if (p.name.empty() || p.type == nullptr) {
      return false;
    }
    if (check_property_unique_set.find(p.name) !=
        check_property_unique_set.end()) {
      return false;
    } else {
      check_property_unique_set.insert(p.name);
    }
    // TODO(@acezen): support list type in csv file
    if (p.type->id() == Type::LIST && file_type_ == FileType::CSV) {
      // list type is not supported in csv file
      return false;
    }
    // TODO(@yangxk): support cardinality in csv file
    if (p.cardinality != Cardinality::SINGLE && file_type_ == FileType::CSV) {
      // list cardinality is not supported in csv file
      return false;
    }
  }
  return true;
}

std::shared_ptr<PropertyGroup> CreatePropertyGroup(
    const std::vector<Property>& properties, FileType file_type,
    const std::string& prefix) {
  if (properties.empty()) {
    // empty property group is not allowed
    return nullptr;
  }
  return std::make_shared<PropertyGroup>(properties, file_type, prefix);
}

bool operator==(const PropertyGroup& lhs, const PropertyGroup& rhs) {
  return (lhs.GetPrefix() == rhs.GetPrefix()) &&
         (lhs.GetFileType() == rhs.GetFileType()) &&
         (lhs.GetProperties() == rhs.GetProperties());
}

AdjacentList::AdjacentList(AdjListType type, FileType file_type,
                           const std::string& prefix)
    : type_(type), file_type_(file_type), prefix_(prefix) {
  if (prefix_.empty()) {
    prefix_ = std::string(AdjListTypeToString(type_)) + "/";
  }
}

bool AdjacentList::IsValidated() const {
  if (type_ != AdjListType::unordered_by_source &&
      type_ != AdjListType::ordered_by_source &&
      type_ != AdjListType::unordered_by_dest &&
      type_ != AdjListType::ordered_by_dest) {
    return false;
  }
  if (prefix_.empty() ||
      (file_type_ != FileType::CSV && file_type_ != FileType::PARQUET &&
       file_type_ != FileType::ORC)) {
    return false;
  }
  return true;
}

std::shared_ptr<AdjacentList> CreateAdjacentList(AdjListType type,
                                                 FileType file_type,
                                                 const std::string& prefix) {
  return std::make_shared<AdjacentList>(type, file_type, prefix);
}

class VertexInfo::Impl {
 public:
  Impl(const std::string& type, IdType chunk_size, const std::string& prefix,
       const PropertyGroupVector& property_groups,
       const std::vector<std::string>& labels,
       std::shared_ptr<const InfoVersion> version)
      : type_(type),
        chunk_size_(chunk_size),
        property_groups_(std::move(property_groups)),
        labels_(labels),
        prefix_(prefix),
        version_(std::move(version)) {
    if (prefix_.empty()) {
      prefix_ = type_ + "/";  // default prefix
    }
    for (size_t i = 0; i < property_groups_.size(); i++) {
      const auto& pg = property_groups_[i];
      if (!pg) {
        continue;
      }
      for (const auto& p : pg->GetProperties()) {
        property_name_to_index_.emplace(p.name, i);
        property_name_to_primary_.emplace(p.name, p.is_primary);
        property_name_to_nullable_.emplace(p.name, p.is_nullable);
        property_name_to_type_.emplace(p.name, p.type);
        property_name_to_cardinality_.emplace(p.name, p.cardinality);
      }
    }
  }

  bool is_validated() const noexcept {
    if (type_.empty() || chunk_size_ <= 0 || prefix_.empty()) {
      return false;
    }
    std::unordered_set<std::string> check_property_unique_set;
    for (const auto& pg : property_groups_) {
      // check if property group is validated
      if (!pg || !pg->IsValidated()) {
        return false;
      }
      // check if property name is unique in all property groups
      for (const auto& p : pg->GetProperties()) {
        if (check_property_unique_set.find(p.name) !=
            check_property_unique_set.end()) {
          return false;
        } else {
          check_property_unique_set.insert(p.name);
        }
      }
    }

    return true;
  }

  std::string type_;
  IdType chunk_size_;
  PropertyGroupVector property_groups_;
  std::vector<std::string> labels_;
  std::string prefix_;
  std::shared_ptr<const InfoVersion> version_;
  std::unordered_map<std::string, int> property_name_to_index_;
  std::unordered_map<std::string, bool> property_name_to_primary_;
  std::unordered_map<std::string, bool> property_name_to_nullable_;
  std::unordered_map<std::string, std::shared_ptr<DataType>>
      property_name_to_type_;
  std::unordered_map<std::string, Cardinality> property_name_to_cardinality_;
};

VertexInfo::VertexInfo(const std::string& type, IdType chunk_size,
                       const PropertyGroupVector& property_groups,
                       const std::vector<std::string>& labels,
                       const std::string& prefix,
                       std::shared_ptr<const InfoVersion> version)
    : impl_(new Impl(type, chunk_size, prefix, property_groups, labels,
                     version)) {}

VertexInfo::~VertexInfo() = default;

const std::string& VertexInfo::GetType() const { return impl_->type_; }

IdType VertexInfo::GetChunkSize() const { return impl_->chunk_size_; }

const std::string& VertexInfo::GetPrefix() const { return impl_->prefix_; }

const std::vector<std::string>& VertexInfo::GetLabels() const {
  return impl_->labels_;
}

const std::shared_ptr<const InfoVersion>& VertexInfo::version() const {
  return impl_->version_;
}

Result<std::string> VertexInfo::GetFilePath(
    std::shared_ptr<PropertyGroup> property_group, IdType chunk_index) const {
  if (property_group == nullptr) {
    return Status::Invalid("property group is nullptr");
  }
  return BuildPath({impl_->prefix_, property_group->GetPrefix()}) + "chunk" +
         std::to_string(chunk_index);
}

Result<std::string> VertexInfo::GetPathPrefix(
    std::shared_ptr<PropertyGroup> property_group) const {
  if (property_group == nullptr) {
    return Status::Invalid("property group is nullptr");
  }
  return BuildPath({impl_->prefix_, property_group->GetPrefix()});
}

Result<std::string> VertexInfo::GetVerticesNumFilePath() const {
  return BuildPath({impl_->prefix_}) + "vertex_count";
}

int VertexInfo::PropertyGroupNum() const {
  return static_cast<int>(impl_->property_groups_.size());
}

std::shared_ptr<PropertyGroup> VertexInfo::GetPropertyGroup(
    const std::string& property_name) const {
  int i = LookupKeyIndex(impl_->property_name_to_index_, property_name);
  return i == -1 ? nullptr : impl_->property_groups_[i];
}

std::shared_ptr<PropertyGroup> VertexInfo::GetPropertyGroupByIndex(
    int index) const {
  if (index < 0 || index >= static_cast<int>(impl_->property_groups_.size())) {
    return nullptr;
  }
  return impl_->property_groups_[index];
}

const PropertyGroupVector& VertexInfo::GetPropertyGroups() const {
  return impl_->property_groups_;
}

bool VertexInfo::IsPrimaryKey(const std::string& property_name) const {
  auto it = impl_->property_name_to_primary_.find(property_name);
  if (it == impl_->property_name_to_primary_.end()) {
    return false;
  }
  return it->second;
}

bool VertexInfo::IsNullableKey(const std::string& property_name) const {
  auto it = impl_->property_name_to_nullable_.find(property_name);
  if (it == impl_->property_name_to_nullable_.end()) {
    return false;
  }
  return it->second;
}

bool VertexInfo::HasProperty(const std::string& property_name) const {
  return impl_->property_name_to_index_.find(property_name) !=
         impl_->property_name_to_index_.end();
}

bool VertexInfo::HasPropertyGroup(
    const std::shared_ptr<PropertyGroup>& property_group) const {
  if (property_group == nullptr) {
    return false;
  }
  for (const auto& pg : impl_->property_groups_) {
    if (*pg == *property_group) {
      return true;
    }
  }
  return false;
}

Result<std::shared_ptr<DataType>> VertexInfo::GetPropertyType(
    const std::string& property_name) const {
  auto it = impl_->property_name_to_type_.find(property_name);
  if (it == impl_->property_name_to_type_.end()) {
    return Status::Invalid("property name not found: ", property_name);
  }
  return it->second;
}

Result<Cardinality> VertexInfo::GetPropertyCardinality(
    const std::string& property_name) const {
  auto it = impl_->property_name_to_cardinality_.find(property_name);
  if (it == impl_->property_name_to_cardinality_.end()) {
    return Status::Invalid("property name not found: ", property_name);
  }
  return it->second;
}

Result<std::shared_ptr<VertexInfo>> VertexInfo::AddPropertyGroup(
    std::shared_ptr<PropertyGroup> property_group) const {
  if (property_group == nullptr) {
    return Status::Invalid("property group is nullptr");
  }
  for (const auto& property : property_group->GetProperties()) {
    if (HasProperty(property.name)) {
      return Status::Invalid("property in the property group already exists: ",
                             property.name);
    }
  }
  return std::make_shared<VertexInfo>(
      impl_->type_, impl_->chunk_size_,
      AddVectorElement(impl_->property_groups_, property_group), impl_->labels_,
      impl_->prefix_, impl_->version_);
}

Result<std::shared_ptr<VertexInfo>> VertexInfo::RemovePropertyGroup(
    std::shared_ptr<PropertyGroup> property_group) const {
  if (property_group == nullptr) {
    return Status::Invalid("property group is nullptr");
  }
  int idx = -1;
  for (size_t i = 0; i < impl_->property_groups_.size(); i++) {
    if (*(impl_->property_groups_[i]) == *property_group) {
      idx = i;
      break;
    }
  }
  if (idx == -1) {
    return Status::Invalid("property group not found");
  }
  return std::make_shared<VertexInfo>(
      impl_->type_, impl_->chunk_size_,
      RemoveVectorElement(impl_->property_groups_, static_cast<size_t>(idx)),
      impl_->labels_, impl_->prefix_, impl_->version_);
}

bool VertexInfo::IsValidated() const { return impl_->is_validated(); }

std::shared_ptr<VertexInfo> CreateVertexInfo(
    const std::string& type, IdType chunk_size,
    const PropertyGroupVector& property_groups,
    const std::vector<std::string>& labels, const std::string& prefix,
    std::shared_ptr<const InfoVersion> version) {
  if (type.empty() || chunk_size <= 0) {
    return nullptr;
  }
  return std::make_shared<VertexInfo>(type, chunk_size, property_groups, labels,
                                      prefix, version);
}

Result<std::shared_ptr<VertexInfo>> VertexInfo::Load(
    std::shared_ptr<Yaml> yaml) {
  if (yaml == nullptr) {
    return Status::Invalid("yaml shared pointer is nullptr");
  }
  std::string type = yaml->operator[]("type").As<std::string>();
  IdType chunk_size =
      static_cast<IdType>(yaml->operator[]("chunk_size").As<int64_t>());
  std::string prefix;
  if (!yaml->operator[]("prefix").IsNone()) {
    prefix = yaml->operator[]("prefix").As<std::string>();
  }
  std::vector<std::string> labels;
  const auto& labels_node = yaml->operator[]("labels");
  if (labels_node.IsSequence()) {
    for (auto it = labels_node.Begin(); it != labels_node.End(); it++) {
      labels.push_back((*it).second.As<std::string>());
    }
  }
  std::shared_ptr<const InfoVersion> version = nullptr;
  if (!yaml->operator[]("version").IsNone()) {
    GAR_ASSIGN_OR_RAISE(
        version,
        InfoVersion::Parse(yaml->operator[]("version").As<std::string>()));
  }
  PropertyGroupVector property_groups;
  auto property_groups_node = yaml->operator[]("property_groups");
  if (!property_groups_node.IsNone()) {  // property_groups exist
    for (auto it = property_groups_node.Begin();
         it != property_groups_node.End(); it++) {
      std::string pg_prefix;
      auto& node = (*it).second;
      if (!node["prefix"].IsNone()) {
        pg_prefix = node["prefix"].As<std::string>();
      }
      auto file_type = StringToFileType(node["file_type"].As<std::string>());
      std::vector<Property> property_vec;
      auto& properties = node["properties"];
      for (auto iit = properties.Begin(); iit != properties.End(); iit++) {
        auto& p_node = (*iit).second;
        auto property_name = p_node["name"].As<std::string>();
        auto property_type =
            DataType::TypeNameToDataType(p_node["data_type"].As<std::string>());
        bool is_primary = p_node["is_primary"].As<bool>();
        bool is_nullable =
            p_node["is_nullable"].IsNone() || p_node["is_nullable"].As<bool>();
        Cardinality cardinality = Cardinality::SINGLE;
        if (!p_node["cardinality"].IsNone()) {
          cardinality =
              StringToCardinality(p_node["cardinality"].As<std::string>());
        }
        property_vec.emplace_back(property_name, property_type, is_primary,
                                  is_nullable, cardinality);
      }
      property_groups.push_back(
          std::make_shared<PropertyGroup>(property_vec, file_type, pg_prefix));
    }
  }
  return std::make_shared<VertexInfo>(type, chunk_size, property_groups, labels,
                                      prefix, version);
}

Result<std::shared_ptr<VertexInfo>> VertexInfo::Load(const std::string& input) {
  GAR_ASSIGN_OR_RAISE(auto yaml, Yaml::Load(input));
  return VertexInfo::Load(yaml);
}

Result<std::string> VertexInfo::Dump() const noexcept {
  if (!IsValidated()) {
    return Status::Invalid("The vertex info is not validated");
  }
  std::string dump_string;
  ::Yaml::Node node;
  try {
    node["type"] = impl_->type_;
    node["chunk_size"] = std::to_string(impl_->chunk_size_);
    node["prefix"] = impl_->prefix_;
    if (impl_->labels_.size() > 0) {
      node["labels"];
      for (const auto& label : impl_->labels_) {
        node["labels"].PushBack();
        node["labels"][node["labels"].Size() - 1] = label;
      }
    }
    for (const auto& pg : impl_->property_groups_) {
      ::Yaml::Node pg_node;
      if (!pg->GetPrefix().empty()) {
        pg_node["prefix"] = pg->GetPrefix();
      }
      pg_node["file_type"] = FileTypeToString(pg->GetFileType());
      for (const auto& p : pg->GetProperties()) {
        ::Yaml::Node p_node;
        p_node["name"] = p.name;
        p_node["data_type"] = p.type->ToTypeName();
        p_node["is_primary"] = p.is_primary ? "true" : "false";
        p_node["is_nullable"] = p.is_nullable ? "true" : "false";
        if (p.cardinality != Cardinality::SINGLE) {
          p_node["cardinality"] = CardinalityToString(p.cardinality);
        }
        pg_node["properties"].PushBack();
        pg_node["properties"][pg_node["properties"].Size() - 1] = p_node;
      }
      node["property_groups"].PushBack();
      node["property_groups"][node["property_groups"].Size() - 1] = pg_node;
    }
    if (impl_->version_ != nullptr) {
      node["version"] = impl_->version_->ToString();
    }
    ::Yaml::Serialize(node, dump_string);
  } catch (const std::exception& e) {
    return Status::Invalid("Failed to dump vertex info: ", e.what());
  }
  return dump_string;
}

Status VertexInfo::Save(const std::string& path) const {
  std::string no_url_path;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(path, &no_url_path));
  GAR_ASSIGN_OR_RAISE(auto yaml_content, this->Dump());
  return fs->WriteValueToFile(yaml_content, no_url_path);
}

class EdgeInfo::Impl {
 public:
  Impl(const std::string& src_type, const std::string& edge_type,
       const std::string& dst_type, IdType chunk_size, IdType src_chunk_size,
       IdType dst_chunk_size, bool directed, const std::string& prefix,
       const AdjacentListVector& adjacent_lists,
       const PropertyGroupVector& property_groups,
       std::shared_ptr<const InfoVersion> version)
      : src_type_(src_type),
        edge_type_(edge_type),
        dst_type_(dst_type),
        chunk_size_(chunk_size),
        src_chunk_size_(src_chunk_size),
        dst_chunk_size_(dst_chunk_size),
        directed_(directed),
        prefix_(prefix),
        adjacent_lists_(std::move(adjacent_lists)),
        property_groups_(std::move(property_groups)),
        version_(std::move(version)) {
    if (prefix_.empty()) {
      prefix_ = src_type_ + REGULAR_SEPARATOR + edge_type_ + REGULAR_SEPARATOR +
                dst_type_ + "/";  // default prefix
    }
    for (size_t i = 0; i < adjacent_lists_.size(); i++) {
      if (!adjacent_lists_[i]) {
        continue;
      }

      auto adj_list_type = adjacent_lists_[i]->GetType();
      adjacent_list_type_to_index_[adj_list_type] = i;
    }
    for (size_t i = 0; i < property_groups_.size(); i++) {
      const auto& pg = property_groups_[i];
      if (!pg) {
        continue;
      }
      for (const auto& p : pg->GetProperties()) {
        property_name_to_index_.emplace(p.name, i);
        property_name_to_primary_.emplace(p.name, p.is_primary);
        property_name_to_nullable_.emplace(p.name, p.is_nullable);
        property_name_to_type_.emplace(p.name, p.type);
      }
    }
  }

  bool is_validated() const noexcept {
    if (src_type_.empty() || edge_type_.empty() || dst_type_.empty() ||
        chunk_size_ <= 0 || src_chunk_size_ <= 0 || dst_chunk_size_ <= 0 ||
        prefix_.empty() || adjacent_lists_.empty()) {
      return false;
    }

    for (const auto& al : adjacent_lists_) {
      if (!al || !al->IsValidated()) {
        return false;
      }
    }

    std::unordered_set<std::string> check_property_unique_set;
    for (const auto& pg : property_groups_) {
      // check if property group is validated
      if (!pg || !pg->IsValidated()) {
        return false;
      }
      // check if property name is unique in all property groups
      for (const auto& p : pg->GetProperties()) {
        if (p.cardinality != Cardinality::SINGLE) {
          // edge property only supports single cardinality
          std::cout
              << "Edge property only supports single cardinality, but got: "
              << CardinalityToString(p.cardinality) << std::endl;
          return false;
        }
        if (check_property_unique_set.find(p.name) !=
            check_property_unique_set.end()) {
          return false;
        } else {
          check_property_unique_set.insert(p.name);
        }
      }
    }
    if (adjacent_lists_.size() != adjacent_list_type_to_index_.size()) {
      return false;
    }
    return true;
  }

  std::string src_type_;
  std::string edge_type_;
  std::string dst_type_;
  IdType chunk_size_;
  IdType src_chunk_size_;
  IdType dst_chunk_size_;
  bool directed_;
  std::string prefix_;
  AdjacentListVector adjacent_lists_;
  PropertyGroupVector property_groups_;
  std::unordered_map<AdjListType, int> adjacent_list_type_to_index_;
  std::unordered_map<std::string, int> property_name_to_index_;
  std::unordered_map<std::string, bool> property_name_to_primary_;
  std::unordered_map<std::string, bool> property_name_to_nullable_;
  std::unordered_map<std::string, std::shared_ptr<DataType>>
      property_name_to_type_;
  std::shared_ptr<const InfoVersion> version_;
};

EdgeInfo::EdgeInfo(const std::string& src_type, const std::string& edge_type,
                   const std::string& dst_type, IdType chunk_size,
                   IdType src_chunk_size, IdType dst_chunk_size, bool directed,
                   const AdjacentListVector& adjacent_lists,
                   const PropertyGroupVector& property_groups,
                   const std::string& prefix,
                   std::shared_ptr<const InfoVersion> version)
    : impl_(new Impl(src_type, edge_type, dst_type, chunk_size, src_chunk_size,
                     dst_chunk_size, directed, prefix, adjacent_lists,
                     property_groups, version)) {}

EdgeInfo::~EdgeInfo() = default;

const std::string& EdgeInfo::GetSrcType() const { return impl_->src_type_; }

const std::string& EdgeInfo::GetEdgeType() const { return impl_->edge_type_; }

const std::string& EdgeInfo::GetDstType() const { return impl_->dst_type_; }

IdType EdgeInfo::GetChunkSize() const { return impl_->chunk_size_; }

IdType EdgeInfo::GetSrcChunkSize() const { return impl_->src_chunk_size_; }

IdType EdgeInfo::GetDstChunkSize() const { return impl_->dst_chunk_size_; }

const std::string& EdgeInfo::GetPrefix() const { return impl_->prefix_; }

bool EdgeInfo::IsDirected() const { return impl_->directed_; }

const std::shared_ptr<const InfoVersion>& EdgeInfo::version() const {
  return impl_->version_;
}

bool EdgeInfo::HasAdjacentListType(AdjListType adj_list_type) const {
  return impl_->adjacent_list_type_to_index_.find(adj_list_type) !=
         impl_->adjacent_list_type_to_index_.end();
}

bool EdgeInfo::HasProperty(const std::string& property_name) const {
  return impl_->property_name_to_index_.find(property_name) !=
         impl_->property_name_to_index_.end();
}

bool EdgeInfo::HasPropertyGroup(
    const std::shared_ptr<PropertyGroup>& property_group) const {
  if (property_group == nullptr) {
    return false;
  }
  for (const auto& pg : impl_->property_groups_) {
    if (*pg == *property_group) {
      return true;
    }
  }
  return false;
}

std::shared_ptr<AdjacentList> EdgeInfo::GetAdjacentList(
    AdjListType adj_list_type) const {
  auto it = impl_->adjacent_list_type_to_index_.find(adj_list_type);
  if (it == impl_->adjacent_list_type_to_index_.end()) {
    return nullptr;
  }
  return impl_->adjacent_lists_[it->second];
}

int EdgeInfo::PropertyGroupNum() const {
  return static_cast<int>(impl_->property_groups_.size());
}

const PropertyGroupVector& EdgeInfo::GetPropertyGroups() const {
  return impl_->property_groups_;
}

std::shared_ptr<PropertyGroup> EdgeInfo::GetPropertyGroup(
    const std::string& property_name) const {
  int i = LookupKeyIndex(impl_->property_name_to_index_, property_name);
  return i == -1 ? nullptr : impl_->property_groups_[i];
}

std::shared_ptr<PropertyGroup> EdgeInfo::GetPropertyGroupByIndex(
    int index) const {
  if (index < 0 || index >= static_cast<int>(impl_->property_groups_.size())) {
    return nullptr;
  }
  return impl_->property_groups_[index];
}

Result<std::string> EdgeInfo::GetVerticesNumFilePath(
    AdjListType adj_list_type) const {
  CHECK_HAS_ADJ_LIST_TYPE(adj_list_type);
  int i = impl_->adjacent_list_type_to_index_.at(adj_list_type);
  return BuildPath({impl_->prefix_, impl_->adjacent_lists_[i]->GetPrefix()}) +
         "vertex_count";
}

Result<std::string> EdgeInfo::GetEdgesNumFilePath(
    IdType vertex_chunk_index, AdjListType adj_list_type) const {
  CHECK_HAS_ADJ_LIST_TYPE(adj_list_type);
  int i = impl_->adjacent_list_type_to_index_.at(adj_list_type);
  return BuildPath({impl_->prefix_, impl_->adjacent_lists_[i]->GetPrefix()}) +
         "edge_count" + std::to_string(vertex_chunk_index);
}

Result<std::string> EdgeInfo::GetAdjListFilePath(
    IdType vertex_chunk_index, IdType edge_chunk_index,
    AdjListType adj_list_type) const {
  CHECK_HAS_ADJ_LIST_TYPE(adj_list_type);
  int i = impl_->adjacent_list_type_to_index_.at(adj_list_type);
  return BuildPath({impl_->prefix_, impl_->adjacent_lists_[i]->GetPrefix()}) +
         "adj_list/part" + std::to_string(vertex_chunk_index) + "/chunk" +
         std::to_string(edge_chunk_index);
}

Result<std::string> EdgeInfo::GetAdjListPathPrefix(
    AdjListType adj_list_type) const {
  CHECK_HAS_ADJ_LIST_TYPE(adj_list_type);
  int i = impl_->adjacent_list_type_to_index_.at(adj_list_type);
  return BuildPath({impl_->prefix_, impl_->adjacent_lists_[i]->GetPrefix()}) +
         "adj_list/";
}

Result<std::string> EdgeInfo::GetAdjListOffsetFilePath(
    IdType vertex_chunk_index, AdjListType adj_list_type) const {
  CHECK_HAS_ADJ_LIST_TYPE(adj_list_type);
  int i = impl_->adjacent_list_type_to_index_.at(adj_list_type);
  return BuildPath({impl_->prefix_, impl_->adjacent_lists_[i]->GetPrefix()}) +
         "offset/chunk" + std::to_string(vertex_chunk_index);
}

Result<std::string> EdgeInfo::GetOffsetPathPrefix(
    AdjListType adj_list_type) const {
  CHECK_HAS_ADJ_LIST_TYPE(adj_list_type);
  int i = impl_->adjacent_list_type_to_index_.at(adj_list_type);
  return BuildPath({impl_->prefix_, impl_->adjacent_lists_[i]->GetPrefix()}) +
         "offset/";
}

Result<std::string> EdgeInfo::GetPropertyFilePath(
    const std::shared_ptr<PropertyGroup>& property_group,
    AdjListType adj_list_type, IdType vertex_chunk_index,
    IdType edge_chunk_index) const {
  if (property_group == nullptr) {
    return Status::Invalid("property group is nullptr");
  }
  CHECK_HAS_ADJ_LIST_TYPE(adj_list_type);
  int i = impl_->adjacent_list_type_to_index_.at(adj_list_type);
  return BuildPath({impl_->prefix_, impl_->adjacent_lists_[i]->GetPrefix(),
                    property_group->GetPrefix()}) +
         "part" + std::to_string(vertex_chunk_index) + "/chunk" +
         std::to_string(edge_chunk_index);
}

Result<std::string> EdgeInfo::GetPropertyGroupPathPrefix(
    const std::shared_ptr<PropertyGroup>& property_group,
    AdjListType adj_list_type) const {
  if (property_group == nullptr) {
    return Status::Invalid("property group is nullptr");
  }
  CHECK_HAS_ADJ_LIST_TYPE(adj_list_type);
  int i = impl_->adjacent_list_type_to_index_.at(adj_list_type);
  return BuildPath({impl_->prefix_, impl_->adjacent_lists_[i]->GetPrefix(),
                    property_group->GetPrefix()});
}

Result<std::shared_ptr<DataType>> EdgeInfo::GetPropertyType(
    const std::string& property_name) const {
  auto it = impl_->property_name_to_type_.find(property_name);
  if (it == impl_->property_name_to_type_.end()) {
    return Status::Invalid("property name not found: ", property_name);
  }
  return it->second;
}

bool EdgeInfo::IsPrimaryKey(const std::string& property_name) const {
  auto it = impl_->property_name_to_primary_.find(property_name);
  if (it == impl_->property_name_to_primary_.end()) {
    return false;
  }
  return it->second;
}

bool EdgeInfo::IsNullableKey(const std::string& property_name) const {
  auto it = impl_->property_name_to_nullable_.find(property_name);
  if (it == impl_->property_name_to_nullable_.end()) {
    return false;
  }
  return it->second;
}

Result<std::shared_ptr<EdgeInfo>> EdgeInfo::AddAdjacentList(
    std::shared_ptr<AdjacentList> adj_list) const {
  if (adj_list == nullptr) {
    return Status::Invalid("adj list is nullptr");
  }
  if (HasAdjacentListType(adj_list->GetType())) {
    return Status::Invalid("adj list type already exists: ",
                           AdjListTypeToString(adj_list->GetType()));
  }
  return std::make_shared<EdgeInfo>(
      impl_->src_type_, impl_->edge_type_, impl_->dst_type_, impl_->chunk_size_,
      impl_->src_chunk_size_, impl_->dst_chunk_size_, impl_->directed_,
      AddVectorElement(impl_->adjacent_lists_, adj_list),
      impl_->property_groups_, impl_->prefix_, impl_->version_);
}

Result<std::shared_ptr<EdgeInfo>> EdgeInfo::RemoveAdjacentList(
    std::shared_ptr<AdjacentList> adj_list) const {
  if (adj_list == nullptr) {
    return Status::Invalid("adj list is nullptr");
  }
  int idx = -1;
  for (size_t i = 0; i < impl_->adjacent_lists_.size(); i++) {
    if (impl_->adjacent_lists_[i]->GetType() == adj_list->GetType()) {
      idx = i;
      break;
    }
  }
  if (idx == -1) {
    return Status::Invalid("adj list not found");
  }
  return std::make_shared<EdgeInfo>(
      impl_->src_type_, impl_->edge_type_, impl_->dst_type_, impl_->chunk_size_,
      impl_->src_chunk_size_, impl_->dst_chunk_size_, impl_->directed_,
      RemoveVectorElement(impl_->adjacent_lists_, static_cast<size_t>(idx)),
      impl_->property_groups_, impl_->prefix_, impl_->version_);
}

Result<std::shared_ptr<EdgeInfo>> EdgeInfo::AddPropertyGroup(
    std::shared_ptr<PropertyGroup> property_group) const {
  if (property_group == nullptr) {
    return Status::Invalid("property group is nullptr");
  }
  for (const auto& property : property_group->GetProperties()) {
    if (HasProperty(property.name)) {
      return Status::Invalid("property in property group already exists: ",
                             property.name);
    }
  }
  return std::make_shared<EdgeInfo>(
      impl_->src_type_, impl_->edge_type_, impl_->dst_type_, impl_->chunk_size_,
      impl_->src_chunk_size_, impl_->dst_chunk_size_, impl_->directed_,
      impl_->adjacent_lists_,
      AddVectorElement(impl_->property_groups_, property_group), impl_->prefix_,
      impl_->version_);
}

Result<std::shared_ptr<EdgeInfo>> EdgeInfo::RemovePropertyGroup(
    std::shared_ptr<PropertyGroup> property_group) const {
  if (property_group == nullptr) {
    return Status::Invalid("property group is nullptr");
  }
  int idx = -1;
  for (size_t i = 0; i < impl_->property_groups_.size(); i++) {
    if (*(impl_->property_groups_[i]) == *property_group) {
      idx = i;
    }
  }
  if (idx == -1) {
    return Status::Invalid("property group not found");
  }
  return std::make_shared<EdgeInfo>(
      impl_->src_type_, impl_->edge_type_, impl_->dst_type_, impl_->chunk_size_,
      impl_->src_chunk_size_, impl_->dst_chunk_size_, impl_->directed_,
      impl_->adjacent_lists_,
      RemoveVectorElement(impl_->property_groups_, static_cast<size_t>(idx)),
      impl_->prefix_, impl_->version_);
}

bool EdgeInfo::IsValidated() const { return impl_->is_validated(); }

std::shared_ptr<EdgeInfo> CreateEdgeInfo(
    const std::string& src_type, const std::string& edge_type,
    const std::string& dst_type, IdType chunk_size, IdType src_chunk_size,
    IdType dst_chunk_size, bool directed,
    const AdjacentListVector& adjacent_lists,
    const PropertyGroupVector& property_groups, const std::string& prefix,
    std::shared_ptr<const InfoVersion> version) {
  if (src_type.empty() || edge_type.empty() || dst_type.empty() ||
      chunk_size <= 0 || src_chunk_size <= 0 || dst_chunk_size <= 0 ||
      adjacent_lists.empty()) {
    return nullptr;
  }
  return std::make_shared<EdgeInfo>(
      src_type, edge_type, dst_type, chunk_size, src_chunk_size, dst_chunk_size,
      directed, adjacent_lists, property_groups, prefix, version);
}

Result<std::shared_ptr<EdgeInfo>> EdgeInfo::Load(std::shared_ptr<Yaml> yaml) {
  if (yaml == nullptr) {
    return Status::Invalid("yaml shared pointer is nullptr.");
  }
  std::string src_type = yaml->operator[]("src_type").As<std::string>();
  std::string edge_type = yaml->operator[]("edge_type").As<std::string>();
  std::string dst_type = yaml->operator[]("dst_type").As<std::string>();
  IdType chunk_size =
      static_cast<IdType>(yaml->operator[]("chunk_size").As<int64_t>());
  IdType src_chunk_size =
      static_cast<IdType>(yaml->operator[]("src_chunk_size").As<int64_t>());
  IdType dst_chunk_size =
      static_cast<IdType>(yaml->operator[]("dst_chunk_size").As<int64_t>());
  bool directed = yaml->operator[]("directed").As<bool>();
  std::string prefix;
  if (!yaml->operator[]("prefix").IsNone()) {
    prefix = yaml->operator[]("prefix").As<std::string>();
  }
  std::shared_ptr<const InfoVersion> version = nullptr;
  if (!yaml->operator[]("version").IsNone()) {
    GAR_ASSIGN_OR_RAISE(
        version,
        InfoVersion::Parse(yaml->operator[]("version").As<std::string>()));
  }

  AdjacentListVector adjacent_lists;
  PropertyGroupVector property_groups;
  auto adj_lists_node = yaml->operator[]("adj_lists");
  if (adj_lists_node.IsSequence()) {
    for (auto it = adj_lists_node.Begin(); it != adj_lists_node.End(); it++) {
      auto& node = (*it).second;
      auto ordered = node["ordered"].As<bool>();
      auto aligned = node["aligned_by"].As<std::string>();
      auto adj_list_type = OrderedAlignedToAdjListType(ordered, aligned);
      auto file_type = StringToFileType(node["file_type"].As<std::string>());
      std::string adj_list_prefix;
      if (!node["prefix"].IsNone()) {
        adj_list_prefix = node["prefix"].As<std::string>();
      }
      adjacent_lists.push_back(std::make_shared<AdjacentList>(
          adj_list_type, file_type, adj_list_prefix));
    }
  }
  auto property_groups_node = yaml->operator[]("property_groups");
  if (!property_groups_node.IsNone()) {  // property_groups exist
    for (auto pg_it = property_groups_node.Begin();
         pg_it != property_groups_node.End(); pg_it++) {
      auto& pg_node = (*pg_it).second;
      std::string pg_prefix;
      if (!pg_node["prefix"].IsNone()) {
        pg_prefix = pg_node["prefix"].As<std::string>();
      }
      auto file_type = StringToFileType(pg_node["file_type"].As<std::string>());
      auto properties = pg_node["properties"];
      std::vector<Property> property_vec;
      for (auto p_it = properties.Begin(); p_it != properties.End(); p_it++) {
        auto& p_node = (*p_it).second;
        auto property_name = p_node["name"].As<std::string>();
        auto property_type =
            DataType::TypeNameToDataType(p_node["data_type"].As<std::string>());
        if (!p_node["cardinality"].IsNone() &&
            StringToCardinality(p_node["cardinality"].As<std::string>()) !=
                Cardinality::SINGLE) {
          return Status::YamlError(
              "Unsupported set cardinality for edge property.");
        }
        bool is_primary = p_node["is_primary"].As<bool>();
        bool is_nullable =
            p_node["is_nullable"].IsNone() || p_node["is_nullable"].As<bool>();
        property_vec.emplace_back(property_name, property_type, is_primary,
                                  is_nullable);
      }
      property_groups.push_back(
          std::make_shared<PropertyGroup>(property_vec, file_type, pg_prefix));
    }
  }
  return std::make_shared<EdgeInfo>(
      src_type, edge_type, dst_type, chunk_size, src_chunk_size, dst_chunk_size,
      directed, adjacent_lists, property_groups, prefix, version);
}

Result<std::shared_ptr<EdgeInfo>> EdgeInfo::Load(const std::string& input) {
  GAR_ASSIGN_OR_RAISE(auto yaml, Yaml::Load(input));
  return EdgeInfo::Load(yaml);
}

Result<std::string> EdgeInfo::Dump() const noexcept {
  if (!IsValidated()) {
    return Status::Invalid("The edge info is not validated.");
  }
  std::string dump_string;
  ::Yaml::Node node;
  try {
    node["src_type"] = impl_->src_type_;
    node["edge_type"] = impl_->edge_type_;
    node["dst_type"] = impl_->dst_type_;
    node["chunk_size"] = std::to_string(impl_->chunk_size_);
    node["src_chunk_size"] = std::to_string(impl_->src_chunk_size_);
    node["dst_chunk_size"] = std::to_string(impl_->dst_chunk_size_);
    node["prefix"] = impl_->prefix_;
    node["directed"] = impl_->directed_ ? "true" : "false";
    for (const auto& adjacent_list : impl_->adjacent_lists_) {
      ::Yaml::Node adj_list_node;
      auto adj_list_type = adjacent_list->GetType();
      auto pair = AdjListTypeToOrderedAligned(adj_list_type);
      adj_list_node["ordered"] = pair.first ? "true" : "false";
      adj_list_node["aligned_by"] = pair.second;
      adj_list_node["prefix"] = adjacent_list->GetPrefix();
      adj_list_node["file_type"] =
          FileTypeToString(adjacent_list->GetFileType());
      node["adj_lists"].PushBack();
      node["adj_lists"][node["adj_lists"].Size() - 1] = adj_list_node;
    }
    for (const auto& pg : impl_->property_groups_) {
      ::Yaml::Node pg_node;
      if (!pg->GetPrefix().empty()) {
        pg_node["prefix"] = pg->GetPrefix();
      }
      pg_node["file_type"] = FileTypeToString(pg->GetFileType());
      for (const auto& p : pg->GetProperties()) {
        ::Yaml::Node p_node;
        p_node["name"] = p.name;
        p_node["data_type"] = p.type->ToTypeName();
        p_node["is_primary"] = p.is_primary ? "true" : "false";
        p_node["is_nullable"] = p.is_nullable ? "true" : "false";
        pg_node["properties"].PushBack();
        pg_node["properties"][pg_node["properties"].Size() - 1] = p_node;
      }
      node["property_groups"].PushBack();
      node["property_groups"][node["property_groups"].Size() - 1] = pg_node;
    }
    if (impl_->version_ != nullptr) {
      node["version"] = impl_->version_->ToString();
    }
    ::Yaml::Serialize(node, dump_string);
  } catch (const std::exception& e) {
    return Status::Invalid("Failed to dump edge info: ", e.what());
  }
  return dump_string;
}

Status EdgeInfo::Save(const std::string& path) const {
  std::string no_url_path;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(path, &no_url_path));
  GAR_ASSIGN_OR_RAISE(auto yaml_content, this->Dump());
  return fs->WriteValueToFile(yaml_content, no_url_path);
}

namespace {

static std::string PathToDirectory(const std::string& path) {
  if (path.rfind("s3://", 0) == 0) {
    int t = path.find_last_of('?');
    std::string prefix = path.substr(0, t);
    std::string suffix = path.substr(t);
    const size_t last_slash_idx = prefix.rfind('/');
    if (std::string::npos != last_slash_idx) {
      return prefix.substr(0, last_slash_idx + 1) + suffix;
    }
  } else {
    const size_t last_slash_idx = path.rfind('/');
    if (std::string::npos != last_slash_idx) {
      return path.substr(0, last_slash_idx + 1);  // +1 to include the slash
    }
  }
  return path;
}

static Result<std::shared_ptr<GraphInfo>> ConstructGraphInfo(
    std::shared_ptr<Yaml> graph_meta, const std::string& default_name,
    const std::string& default_prefix, const std::shared_ptr<FileSystem> fs,
    const std::string& no_url_path) {
  std::string name = default_name;
  std::string prefix = default_prefix;
  if (!graph_meta->operator[]("name").IsNone()) {
    name = graph_meta->operator[]("name").As<std::string>();
  }
  if (!graph_meta->operator[]("prefix").IsNone()) {
    prefix = graph_meta->operator[]("prefix").As<std::string>();
  }
  std::shared_ptr<const InfoVersion> version = nullptr;
  if (!graph_meta->operator[]("version").IsNone()) {
    GAR_ASSIGN_OR_RAISE(
        version, InfoVersion::Parse(
                     graph_meta->operator[]("version").As<std::string>()));
  }
  std::unordered_map<std::string, std::string> extra_info;
  if (!graph_meta->operator[]("extra_info").IsNone()) {
    auto& extra_info_node = graph_meta->operator[]("extra_info");
    for (auto it = extra_info_node.Begin(); it != extra_info_node.End(); it++) {
      auto node = (*it).second;
      auto key = node["key"].As<std::string>();
      auto value = node["value"].As<std::string>();
      extra_info.emplace(key, value);
    }
  }

  VertexInfoVector vertex_infos;
  EdgeInfoVector edge_infos;
  const auto& vertices = graph_meta->operator[]("vertices");
  if (vertices.IsSequence()) {
    for (auto it = vertices.Begin(); it != vertices.End(); it++) {
      std::string vertex_meta_file =
          no_url_path + (*it).second.As<std::string>();
      GAR_ASSIGN_OR_RAISE(auto input,
                          fs->ReadFileToValue<std::string>(vertex_meta_file));
      GAR_ASSIGN_OR_RAISE(auto vertex_meta, Yaml::Load(input));
      GAR_ASSIGN_OR_RAISE(auto vertex_info, VertexInfo::Load(vertex_meta));
      vertex_infos.push_back(vertex_info);
    }
  }
  const auto& edges = graph_meta->operator[]("edges");
  if (edges.IsSequence()) {
    for (auto it = edges.Begin(); it != edges.End(); it++) {
      std::string edge_meta_file = no_url_path + (*it).second.As<std::string>();
      GAR_ASSIGN_OR_RAISE(auto input,
                          fs->ReadFileToValue<std::string>(edge_meta_file));
      GAR_ASSIGN_OR_RAISE(auto edge_meta, Yaml::Load(input));
      GAR_ASSIGN_OR_RAISE(auto edge_info, EdgeInfo::Load(edge_meta));
      edge_infos.push_back(edge_info);
    }
  }

  std::vector<std::string> labels;
  if (!graph_meta->operator[]("labels").IsNone()) {
    const auto& labels_node = graph_meta->operator[]("labels");
    if (labels_node.IsSequence()) {
      for (auto it = labels_node.Begin(); it != labels_node.End(); it++) {
        labels.push_back((*it).second.As<std::string>());
      }
    }
  }
  return std::make_shared<GraphInfo>(name, vertex_infos, edge_infos, labels,
                                     prefix, version, extra_info);
}

}  // namespace

class GraphInfo::Impl {
 public:
  Impl(const std::string& graph_name, VertexInfoVector vertex_infos,
       EdgeInfoVector edge_infos, const std::vector<std::string>& labels,
       const std::string& prefix, std::shared_ptr<const InfoVersion> version,
       const std::unordered_map<std::string, std::string>& extra_info)
      : name_(graph_name),
        vertex_infos_(std::move(vertex_infos)),
        edge_infos_(std::move(edge_infos)),
        labels_(labels),
        prefix_(prefix),
        version_(std::move(version)),
        extra_info_(extra_info) {
    for (size_t i = 0; i < vertex_infos_.size(); i++) {
      if (vertex_infos_[i] != nullptr) {
        vtype_to_index_[vertex_infos_[i]->GetType()] = i;
      }
    }
    for (size_t i = 0; i < edge_infos_.size(); i++) {
      if (edge_infos_[i] != nullptr) {
        std::string edge_key = ConcatEdgeTriple(edge_infos_[i]->GetSrcType(),
                                                edge_infos_[i]->GetEdgeType(),
                                                edge_infos_[i]->GetDstType());
        etype_to_index_[edge_key] = i;
      }
    }
  }

  bool is_validated() const noexcept {
    if (name_.empty() || prefix_.empty()) {
      return false;
    }
    for (const auto& v : vertex_infos_) {
      if (!v || !v->IsValidated()) {
        return false;
      }
    }
    for (const auto& e : edge_infos_) {
      if (!e || !e->IsValidated()) {
        return false;
      }
    }
    if (vertex_infos_.size() != vtype_to_index_.size() ||
        edge_infos_.size() != etype_to_index_.size()) {
      return false;
    }
    return true;
  }

  std::string name_;
  VertexInfoVector vertex_infos_;
  EdgeInfoVector edge_infos_;
  std::vector<std::string> labels_;
  std::string prefix_;
  std::shared_ptr<const InfoVersion> version_;
  std::unordered_map<std::string, std::string> extra_info_;
  std::unordered_map<std::string, int> vtype_to_index_;
  std::unordered_map<std::string, int> etype_to_index_;
};

GraphInfo::GraphInfo(
    const std::string& graph_name, VertexInfoVector vertex_infos,
    EdgeInfoVector edge_infos, const std::vector<std::string>& labels,
    const std::string& prefix, std::shared_ptr<const InfoVersion> version,
    const std::unordered_map<std::string, std::string>& extra_info)
    : impl_(new Impl(graph_name, std::move(vertex_infos), std::move(edge_infos),
                     labels, prefix, version, extra_info)) {}

GraphInfo::~GraphInfo() = default;

const std::string& GraphInfo::GetName() const { return impl_->name_; }

const std::vector<std::string>& GraphInfo::GetLabels() const {
  return impl_->labels_;
}

const std::string& GraphInfo::GetPrefix() const { return impl_->prefix_; }

const std::shared_ptr<const InfoVersion>& GraphInfo::version() const {
  return impl_->version_;
}

const std::unordered_map<std::string, std::string>& GraphInfo::GetExtraInfo()
    const {
  return impl_->extra_info_;
}

std::shared_ptr<VertexInfo> GraphInfo::GetVertexInfo(
    const std::string& type) const {
  int i = GetVertexInfoIndex(type);
  return i == -1 ? nullptr : impl_->vertex_infos_[i];
}

int GraphInfo::GetVertexInfoIndex(const std::string& type) const {
  return LookupKeyIndex(impl_->vtype_to_index_, type);
}

std::shared_ptr<EdgeInfo> GraphInfo::GetEdgeInfo(
    const std::string& src_type, const std::string& edge_type,
    const std::string& dst_type) const {
  int i = GetEdgeInfoIndex(src_type, edge_type, dst_type);
  return i == -1 ? nullptr : impl_->edge_infos_[i];
}

int GraphInfo::GetEdgeInfoIndex(const std::string& src_type,
                                const std::string& edge_type,
                                const std::string& dst_type) const {
  std::string edge_key = ConcatEdgeTriple(src_type, edge_type, dst_type);
  return LookupKeyIndex(impl_->etype_to_index_, edge_key);
}

int GraphInfo::VertexInfoNum() const {
  return static_cast<int>(impl_->vertex_infos_.size());
}

int GraphInfo::EdgeInfoNum() const {
  return static_cast<int>(impl_->edge_infos_.size());
}

const std::shared_ptr<VertexInfo> GraphInfo::GetVertexInfoByIndex(
    int index) const {
  if (index < 0 || index >= static_cast<int>(impl_->vertex_infos_.size())) {
    return nullptr;
  }
  return impl_->vertex_infos_[index];
}

const std::shared_ptr<EdgeInfo> GraphInfo::GetEdgeInfoByIndex(int index) const {
  if (index < 0 || index >= static_cast<int>(impl_->edge_infos_.size())) {
    return nullptr;
  }
  return impl_->edge_infos_[index];
}

const VertexInfoVector& GraphInfo::GetVertexInfos() const {
  return impl_->vertex_infos_;
}

const EdgeInfoVector& GraphInfo::GetEdgeInfos() const {
  return impl_->edge_infos_;
}

bool GraphInfo::IsValidated() const { return impl_->is_validated(); }

Result<std::shared_ptr<GraphInfo>> GraphInfo::AddVertex(
    std::shared_ptr<VertexInfo> vertex_info) const {
  if (vertex_info == nullptr) {
    return Status::Invalid("vertex info is nullptr");
  }
  if (GetVertexInfoIndex(vertex_info->GetType()) != -1) {
    return Status::Invalid("vertex info already exists");
  }
  return std::make_shared<GraphInfo>(
      impl_->name_, AddVectorElement(impl_->vertex_infos_, vertex_info),
      impl_->edge_infos_, impl_->labels_, impl_->prefix_, impl_->version_);
}

Result<std::shared_ptr<GraphInfo>> GraphInfo::RemoveVertex(
    std::shared_ptr<VertexInfo> vertex_info) const {
  if (vertex_info == nullptr) {
    return Status::Invalid("vertex info is nullptr");
  }
  int idx = GetVertexInfoIndex(vertex_info->GetType());
  if (idx == -1) {
    return Status::Invalid("vertex info not found");
  }
  return std::make_shared<GraphInfo>(
      impl_->name_,
      RemoveVectorElement(impl_->vertex_infos_, static_cast<size_t>(idx)),
      impl_->edge_infos_, impl_->labels_, impl_->prefix_, impl_->version_,
      impl_->extra_info_);
}

Result<std::shared_ptr<GraphInfo>> GraphInfo::AddEdge(
    std::shared_ptr<EdgeInfo> edge_info) const {
  if (edge_info == nullptr) {
    return Status::Invalid("edge info is nullptr");
  }
  if (GetEdgeInfoIndex(edge_info->GetSrcType(), edge_info->GetEdgeType(),
                       edge_info->GetDstType()) != -1) {
    return Status::Invalid("edge info already exists");
  }
  return std::make_shared<GraphInfo>(
      impl_->name_, impl_->vertex_infos_,
      AddVectorElement(impl_->edge_infos_, edge_info), impl_->labels_,
      impl_->prefix_, impl_->version_);
}

Result<std::shared_ptr<GraphInfo>> GraphInfo::RemoveEdge(
    std::shared_ptr<EdgeInfo> edge_info) const {
  if (edge_info == nullptr) {
    return Status::Invalid("edge info is nullptr");
  }
  int idx = GetEdgeInfoIndex(edge_info->GetSrcType(), edge_info->GetEdgeType(),
                             edge_info->GetDstType());
  if (idx == -1) {
    return Status::Invalid("edge info not found");
  }
  return std::make_shared<GraphInfo>(
      impl_->name_, impl_->vertex_infos_,
      RemoveVectorElement(impl_->edge_infos_, static_cast<size_t>(idx)),
      impl_->labels_, impl_->prefix_, impl_->version_, impl_->extra_info_);
}

std::shared_ptr<GraphInfo> CreateGraphInfo(
    const std::string& name, const VertexInfoVector& vertex_infos,
    const EdgeInfoVector& edge_infos, const std::vector<std::string>& labels,
    const std::string& prefix, std::shared_ptr<const InfoVersion> version,
    const std::unordered_map<std::string, std::string>& extra_info) {
  if (name.empty()) {
    return nullptr;
  }
  return std::make_shared<GraphInfo>(name, vertex_infos, edge_infos, labels,
                                     prefix, version, extra_info);
}

Result<std::shared_ptr<GraphInfo>> GraphInfo::Load(const std::string& path) {
  std::string no_url_path;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(path, &no_url_path));
  GAR_ASSIGN_OR_RAISE(auto yaml_content,
                      fs->ReadFileToValue<std::string>(no_url_path));
  GAR_ASSIGN_OR_RAISE(auto graph_meta, Yaml::Load(yaml_content));
  std::string default_name = "graph";
  std::string default_prefix = PathToDirectory(path);
  no_url_path = PathToDirectory(no_url_path);
  return ConstructGraphInfo(graph_meta, default_name, default_prefix, fs,
                            no_url_path);
}

Result<std::shared_ptr<GraphInfo>> GraphInfo::Load(
    const std::string& input, const std::string& relative_location) {
  GAR_ASSIGN_OR_RAISE(auto graph_meta, Yaml::Load(input));
  std::string default_name = "graph";
  std::string default_prefix =
      relative_location;  // default chunk file prefix is relative location
  std::string no_url_path;
  GAR_ASSIGN_OR_RAISE(auto fs,
                      FileSystemFromUriOrPath(relative_location, &no_url_path));
  return ConstructGraphInfo(graph_meta, default_name, default_prefix, fs,
                            no_url_path);
}

Result<std::string> GraphInfo::Dump() const {
  if (!IsValidated()) {
    return Status::Invalid("The graph info is not validated.");
  }
  ::Yaml::Node node;
  std::string dump_string;
  try {
    node["name"] = impl_->name_;
    node["prefix"] = impl_->prefix_;
    node["vertices"];
    node["edges"];
    for (const auto& vertex : GetVertexInfos()) {
      node["vertices"].PushBack();
      node["vertices"][node["vertices"].Size() - 1] =
          vertex->GetType() + ".vertex.yaml";
    }
    for (const auto& edge : GetEdgeInfos()) {
      node["edges"].PushBack();
      node["edges"][node["edges"].Size() - 1] =
          ConcatEdgeTriple(edge->GetSrcType(), edge->GetEdgeType(),
                           edge->GetDstType()) +
          ".edge.yaml";
    }
    if (impl_->labels_.size() > 0) {
      node["labels"];
      for (const auto& label : impl_->labels_) {
        node["labels"].PushBack();
        node["labels"][node["labels"].Size() - 1] = label;
      }
    }
    if (impl_->version_ != nullptr) {
      node["version"] = impl_->version_->ToString();
    }
    if (impl_->extra_info_.size() > 0) {
      node["extra_info"];
      for (const auto& pair : impl_->extra_info_) {
        ::Yaml::Node extra_info_node;
        extra_info_node["key"] = pair.first;
        extra_info_node["value"] = pair.second;
        node["extra_info"].PushBack();
        node["extra_info"][node["extra_info"].Size() - 1] = extra_info_node;
      }
    }
    ::Yaml::Serialize(node, dump_string);
  } catch (const std::exception& e) {
    return Status::Invalid("Failed to dump graph info: ", e.what());
  }
  return dump_string;
}

Status GraphInfo::Save(const std::string& path) const {
  std::string no_url_path;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(path, &no_url_path));
  GAR_ASSIGN_OR_RAISE(auto yaml_content, this->Dump());
  return fs->WriteValueToFile(yaml_content, no_url_path);
}

}  // namespace graphar
