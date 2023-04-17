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

#include <iostream>

#include "yaml-cpp/yaml.h"

#include "gar/graph_info.h"
#include "gar/utils/filesystem.h"
#include "gar/utils/yaml.h"

namespace GAR_NAMESPACE_INTERNAL {

Result<VertexInfo> VertexInfo::Load(std::shared_ptr<Yaml> yaml) {
  if (yaml == nullptr) {
    return Status::YamlError("yaml is nullptr");
  }
  std::string label = yaml->operator[]("label").as<std::string>();
  IdType chunk_size =
      static_cast<IdType>(yaml->operator[]("chunk_size").as<int64_t>());
  std::string prefix;
  if (yaml->operator[]("prefix")) {
    prefix = yaml->operator[]("prefix").as<std::string>();
  }
  InfoVersion version;
  if (yaml->operator[]("version")) {
    GAR_ASSIGN_OR_RAISE(
        version,
        InfoVersion::Parse(yaml->operator[]("version").as<std::string>()));
  }
  VertexInfo vertex_info(label, chunk_size, version, prefix);
  auto property_groups = yaml->operator[]("property_groups");
  if (property_groups) {  // property_groups exist
    for (YAML::const_iterator it = property_groups.begin();
         it != property_groups.end(); ++it) {
      std::string pg_prefix;
      if (it->operator[]("prefix")) {
        pg_prefix = it->operator[]("prefix").as<std::string>();
      }
      auto file_type =
          StringToFileType(it->operator[]("file_type").as<std::string>());
      std::vector<Property> property_vec;
      auto& properties = it->operator[]("properties");
      for (YAML::const_iterator iit = properties.begin();
           iit != properties.end(); ++iit) {
        Property property;
        property.name = iit->operator[]("name").as<std::string>();
        property.type = DataType::TypeNameToDataType(
            iit->operator[]("data_type").as<std::string>());
        property.is_primary = iit->operator[]("is_primary").as<bool>();
        property_vec.push_back(property);
      }
      PropertyGroup pg(property_vec, file_type, pg_prefix);
      GAR_RETURN_NOT_OK(vertex_info.AddPropertyGroup(pg));
    }
  }
  return vertex_info;
}

Result<std::string> VertexInfo::Dump() const noexcept {
  if (!IsValidated()) {
    return Status::Invalid();
  }
  YAML::Node node;
  node["label"] = label_;
  node["chunk_size"] = chunk_size_;
  node["prefix"] = prefix_;
  for (const auto& pg : property_groups_) {
    YAML::Node pg_node;
    if (!pg.GetPrefix().empty()) {
      pg_node["prefix"] = pg.GetPrefix();
    }
    pg_node["file_type"] = FileTypeToString(pg.GetFileType());
    for (auto& p : pg.GetProperties()) {
      YAML::Node p_node;
      p_node["name"] = p.name;
      p_node["data_type"] = p.type.ToTypeName();
      p_node["is_primary"] = p.is_primary;
      pg_node["properties"].push_back(p_node);
    }
    node["property_groups"].push_back(pg_node);
  }
  node["version"] = version_.ToString();
  return YAML::Dump(node);
}

Status VertexInfo::Save(const std::string& path) const {
  std::string no_url_path;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(path, &no_url_path));
  GAR_ASSIGN_OR_RAISE(auto yaml_content, this->Dump());
  return fs->WriteValueToFile(yaml_content, path);
}

Result<EdgeInfo> EdgeInfo::Load(std::shared_ptr<Yaml> yaml) {
  if (yaml == nullptr) {
    return Status::YamlError("yaml is nullptr");
  }
  std::string src_label = yaml->operator[]("src_label").as<std::string>();
  std::string edge_label = yaml->operator[]("edge_label").as<std::string>();
  std::string dst_label = yaml->operator[]("dst_label").as<std::string>();
  IdType chunk_size =
      static_cast<IdType>(yaml->operator[]("chunk_size").as<int64_t>());
  IdType src_chunk_size =
      static_cast<IdType>(yaml->operator[]("src_chunk_size").as<int64_t>());
  IdType dst_chunk_size =
      static_cast<IdType>(yaml->operator[]("dst_chunk_size").as<int64_t>());
  bool directed = yaml->operator[]("directed").as<bool>();
  std::string prefix;
  if (yaml->operator[]("prefix")) {
    prefix = yaml->operator[]("prefix").as<std::string>();
  }
  InfoVersion version;
  if (yaml->operator[]("version")) {
    GAR_ASSIGN_OR_RAISE(
        version,
        InfoVersion::Parse(yaml->operator[]("version").as<std::string>()));
  }

  EdgeInfo edge_info(src_label, edge_label, dst_label, chunk_size,
                     src_chunk_size, dst_chunk_size, directed, version, prefix);

  auto adj_lists = yaml->operator[]("adj_lists");
  if (adj_lists) {
    for (YAML::const_iterator it = adj_lists.begin(); it != adj_lists.end();
         ++it) {
      auto ordered = it->operator[]("ordered").as<bool>();
      auto aligned = it->operator[]("aligned_by").as<std::string>();
      auto adj_list_type = OrderedAlignedToAdjListType(ordered, aligned);
      auto file_type =
          StringToFileType(it->operator[]("file_type").as<std::string>());
      std::string adj_list_prefix;
      if (it->operator[]("prefix")) {
        adj_list_prefix = it->operator[]("prefix").as<std::string>();
      }
      GAR_RETURN_NOT_OK(
          edge_info.AddAdjList(adj_list_type, file_type, adj_list_prefix));

      auto property_groups = it->operator[]("property_groups");
      if (property_groups) {  // property_groups exist
        for (YAML::const_iterator pg_it = property_groups.begin();
             pg_it != property_groups.end(); ++pg_it) {
          std::string pg_prefix;
          if (pg_it->operator[]("prefix")) {
            pg_prefix = pg_it->operator[]("prefix").as<std::string>();
          }
          auto file_type = StringToFileType(
              pg_it->operator[]("file_type").as<std::string>());
          auto properties = pg_it->operator[]("properties");
          std::vector<Property> property_vec;
          for (YAML::const_iterator p_it = properties.begin();
               p_it != properties.end(); ++p_it) {
            Property property;
            property.name = p_it->operator[]("name").as<std::string>();
            property.type = DataType::TypeNameToDataType(
                p_it->operator[]("data_type").as<std::string>());
            property.is_primary = p_it->operator[]("is_primary").as<bool>();
            property_vec.push_back(property);
          }
          PropertyGroup pg(property_vec, file_type, pg_prefix);
          GAR_RETURN_NOT_OK(edge_info.AddPropertyGroup(pg, adj_list_type));
        }
      }
    }
  }
  return edge_info;
}

Result<std::string> EdgeInfo::Dump() const noexcept {
  if (!IsValidated()) {
    return Status::Invalid();
  }
  YAML::Node node;
  node["src_label"] = src_label_;
  node["edge_label"] = edge_label_;
  node["dst_label"] = dst_label_;
  node["chunk_size"] = chunk_size_;
  node["src_chunk_size"] = src_chunk_size_;
  node["dst_chunk_size"] = dst_chunk_size_;
  node["prefix"] = prefix_;
  node["directed"] = directed_;
  for (const auto& item : adj_list2prefix_) {
    YAML::Node adj_list_node;
    auto adj_list_type = item.first;
    auto pair = AdjListTypeToOrderedAligned(adj_list_type);
    adj_list_node["ordered"] = pair.first;
    adj_list_node["aligned_by"] = pair.second;
    adj_list_node["prefix"] = adj_list2prefix_.at(adj_list_type);
    adj_list_node["file_type"] =
        FileTypeToString(adj_list2file_type_.at(adj_list_type));
    for (const auto& pg : adj_list2property_groups_.at(adj_list_type)) {
      YAML::Node pg_node;
      if (!pg.GetPrefix().empty()) {
        pg_node["prefix"] = pg.GetPrefix();
      }
      pg_node["file_type"] = FileTypeToString(pg.GetFileType());
      for (auto& p : pg.GetProperties()) {
        YAML::Node p_node;
        p_node["name"] = p.name;
        p_node["data_type"] = p.type.ToTypeName();
        p_node["is_primary"] = p.is_primary;
        pg_node["properties"].push_back(p_node);
      }
      adj_list_node["property_groups"].push_back(pg_node);
    }
    node["adj_lists"].push_back(adj_list_node);
  }
  node["version"] = version_.ToString();
  return YAML::Dump(node);
}

Status EdgeInfo::Save(const std::string& path) const {
  std::string no_url_path;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(path, &no_url_path));
  GAR_ASSIGN_OR_RAISE(auto yaml_content, this->Dump());
  return fs->WriteValueToFile(yaml_content, path);
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

static Result<GraphInfo> ConstructGraphInfo(
    std::shared_ptr<Yaml> graph_meta, const std::string& default_name,
    const std::string& default_prefix, const std::shared_ptr<FileSystem> fs,
    const std::string& no_url_path) {
  std::string name = default_name;
  std::string prefix = default_prefix;
  if (graph_meta->operator[]("name")) {
    name = graph_meta->operator[]("name").as<std::string>();
  }
  if (graph_meta->operator[]("prefix")) {
    prefix = graph_meta->operator[]("prefix").as<std::string>();
  }
  InfoVersion version;
  if (graph_meta->operator[]("version")) {
    GAR_ASSIGN_OR_RAISE(
        version, InfoVersion::Parse(
                     graph_meta->operator[]("version").as<std::string>()));
  }
  GraphInfo graph_info(name, version, prefix);

  const auto& vertices = graph_meta->operator[]("vertices");
  if (vertices) {
    for (YAML::const_iterator it = vertices.begin(); it != vertices.end();
         ++it) {
      std::string vertex_meta_file = no_url_path + it->as<std::string>();
      GAR_ASSIGN_OR_RAISE(auto input,
                          fs->ReadFileToValue<std::string>(vertex_meta_file));
      GAR_ASSIGN_OR_RAISE(auto vertex_meta, Yaml::Load(input));
      GAR_ASSIGN_OR_RAISE(auto vertex_info, VertexInfo::Load(vertex_meta));
      GAR_RETURN_NOT_OK(graph_info.AddVertex(vertex_info));
    }
  }
  const auto& edges = graph_meta->operator[]("edges");
  if (edges) {
    for (YAML::const_iterator it = edges.begin(); it != edges.end(); ++it) {
      std::string edge_meta_file = no_url_path + it->as<std::string>();
      GAR_ASSIGN_OR_RAISE(auto input,
                          fs->ReadFileToValue<std::string>(edge_meta_file));
      GAR_ASSIGN_OR_RAISE(auto edge_meta, Yaml::Load(input));
      GAR_ASSIGN_OR_RAISE(auto edge_info, EdgeInfo::Load(edge_meta));
      GAR_RETURN_NOT_OK(graph_info.AddEdge(edge_info));
    }
  }
  return graph_info;
}

}  // namespace

Result<GraphInfo> GraphInfo::Load(const std::string& path) {
  std::string no_url_path;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(path, &no_url_path));
  GAR_ASSIGN_OR_RAISE(auto yaml_content,
                      fs->ReadFileToValue<std::string>(no_url_path));
  GAR_ASSIGN_OR_RAISE(auto graph_meta, Yaml::Load(yaml_content));
  std::string default_name = "graph";
  std::string default_prefix = PathToDirectory(path);
  no_url_path = PathToDirectory(no_url_path);
  std::cout << "default_prefix: " << default_prefix << std::endl;
  std::cout << "no_url_path: " << no_url_path << std::endl;
  return ConstructGraphInfo(graph_meta, default_name, default_prefix, fs,
                            no_url_path);
}

Result<GraphInfo> GraphInfo::Load(const std::string& input,
                                  const std::string& relative_location) {
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

Result<std::string> GraphInfo::Dump() const noexcept {
  if (!IsValidated()) {
    return Status::Invalid();
  }
  YAML::Node node;
  node["name"] = name_;
  node["prefix"] = prefix_;
  node["vertices"];
  node["edges"];
  for (auto& path : vertex_paths_) {
    node["vertices"].push_back(path);
  }
  for (auto& path : edge_paths_) {
    node["edges"].push_back(path);
  }
  node["version"] = version_.ToString();
  return YAML::Dump(node);
}

Status GraphInfo::Save(const std::string& path) const {
  std::string no_url_path;
  GAR_ASSIGN_OR_RAISE(auto fs, FileSystemFromUriOrPath(path, &no_url_path));
  GAR_ASSIGN_OR_RAISE(auto yaml_content, this->Dump());
  return fs->WriteValueToFile(yaml_content, no_url_path);
}

}  // namespace GAR_NAMESPACE_INTERNAL
