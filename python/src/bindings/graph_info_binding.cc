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

#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "utils/pybind_util.h"

#include "graphar/graph_info.h"
#include "graphar/types.h"
#include "graphar/version_parser.h"

#define STRINGIFY(x) #x
#define MACRO_STRINGIFY(x) STRINGIFY(x)

namespace py = pybind11;

// Changed from PYBIND11_MODULE to a regular function
extern "C" void bind_graph_info(pybind11::module_& m) {
  // Minimal binding for DataType so pybind11 recognizes
  // std::shared_ptr<graphar::DataType> used in Property constructor defaults.
  py::class_<graphar::DataType, std::shared_ptr<graphar::DataType>>(m,
                                                                    "DataType")
      .def(py::init<>())
      .def(py::init<graphar::Type>())
      .def("id", &graphar::DataType::id)
      .def("to_type_name", &graphar::DataType::ToTypeName);

  // Bind InfoVersion
  py::class_<graphar::InfoVersion, std::shared_ptr<graphar::InfoVersion>>(
      m, "InfoVersion")
      .def(py::init<>())
      .def(py::init<int>(), py::arg("version"))
      .def(py::init<int, const std::vector<std::string>&>(), py::arg("version"),
           py::arg("user_define_types"))
      .def("get_version", &graphar::InfoVersion::version)
      .def("get_user_define_types", &graphar::InfoVersion::user_define_types,
           py::return_value_policy::reference_internal)
      .def("to_string", &graphar::InfoVersion::ToString)
      .def("check_type", &graphar::InfoVersion::CheckType)
      .def_static("parse",
                  [](const std::string& str) {
                    return ThrowOrReturn(graphar::InfoVersion::Parse(str));
                  })
      .def("__eq__",
           [](const graphar::InfoVersion& self,
              const graphar::InfoVersion& other) { return self == other; });

  // Bind Property
  py::class_<graphar::Property>(m, "Property")
      .def(py::init<>())
      .def(py::init<const std::string&,
                    const std::shared_ptr<graphar::DataType>&, bool, bool,
                    graphar::Cardinality>(),
           py::arg("name"), py::arg("type") = nullptr,
           py::arg("is_primary") = false, py::arg("is_nullable") = true,
           py::arg("cardinality") = graphar::Cardinality::SINGLE)
      .def_readwrite("name", &graphar::Property::name)
      .def_readwrite("type", &graphar::Property::type)
      .def_readwrite("is_primary", &graphar::Property::is_primary)
      .def_readwrite("is_nullable", &graphar::Property::is_nullable)
      .def_readwrite("cardinality", &graphar::Property::cardinality);

  // Bind PropertyGroup
  py::class_<graphar::PropertyGroup, std::shared_ptr<graphar::PropertyGroup>>(
      m, "PropertyGroup")
      .def(py::init<const std::vector<graphar::Property>&, graphar::FileType,
                    const std::string&>(),
           py::arg("properties"), py::arg("file_type"), py::arg("prefix") = "")
      .def("get_properties", &graphar::PropertyGroup::GetProperties,
           py::return_value_policy::reference_internal)
      .def("has_property", &graphar::PropertyGroup::HasProperty)
      .def("get_file_type", &graphar::PropertyGroup::GetFileType)
      .def("get_prefix", &graphar::PropertyGroup::GetPrefix)
      .def("is_validated", &graphar::PropertyGroup::IsValidated);

  // Bind AdjacentList
  py::class_<graphar::AdjacentList, std::shared_ptr<graphar::AdjacentList>>(
      m, "AdjacentList")
      .def(py::init<graphar::AdjListType, graphar::FileType,
                    const std::string&>(),
           py::arg("type"), py::arg("file_type"), py::arg("prefix") = "")
      .def("get_type", &graphar::AdjacentList::GetType)
      .def("get_file_type", &graphar::AdjacentList::GetFileType)
      .def("get_prefix", &graphar::AdjacentList::GetPrefix)
      .def("is_validated", &graphar::AdjacentList::IsValidated);

  // Bind VertexInfo
  py::class_<graphar::VertexInfo, std::shared_ptr<graphar::VertexInfo>>(
      m, "VertexInfo")
      .def(py::init<const std::string&, graphar::IdType,
                    const std::vector<std::shared_ptr<graphar::PropertyGroup>>&,
                    const std::vector<std::string>&, const std::string&,
                    std::shared_ptr<const graphar::InfoVersion>>(),
           py::arg("type"), py::arg("chunk_size"), py::arg("property_groups"),
           py::arg("labels") = std::vector<std::string>(),
           py::arg("prefix") = "", py::arg("version") = nullptr)
      .def("add_property_group",
           [](const graphar::VertexInfo& self,
              std::shared_ptr<graphar::PropertyGroup> property_group) {
             return ThrowOrReturn(self.AddPropertyGroup(property_group));
           })
      .def("remove_property_group",
           [](const graphar::VertexInfo& self,
              std::shared_ptr<graphar::PropertyGroup> property_group) {
             return ThrowOrReturn(self.RemovePropertyGroup(property_group));
           })
      .def("get_type", &graphar::VertexInfo::GetType,
           py::return_value_policy::reference_internal)
      .def("get_chunk_size", &graphar::VertexInfo::GetChunkSize)
      .def("get_prefix", &graphar::VertexInfo::GetPrefix,
           py::return_value_policy::reference_internal)
      .def("version", &graphar::VertexInfo::version)
      .def("get_labels", &graphar::VertexInfo::GetLabels,
           py::return_value_policy::reference_internal)
      .def("property_group_num", &graphar::VertexInfo::PropertyGroupNum)
      .def("get_property_groups", &graphar::VertexInfo::GetPropertyGroups,
           py::return_value_policy::reference_internal)
      .def("get_property_group",
           [](const graphar::VertexInfo& self,
              const std::string& property_name) {
             return self.GetPropertyGroup(property_name);
           })
      .def("get_property_group_by_index",
           [](const graphar::VertexInfo& self, int index) {
             return self.GetPropertyGroupByIndex(index);
           })
      .def("get_property_type",
           [](const graphar::VertexInfo& self,
              const std::string& property_name) {
             return ThrowOrReturn(self.GetPropertyType(property_name));
           })
      .def("get_property_cardinality",
           [](const graphar::VertexInfo& self,
              const std::string& property_name) {
             return ThrowOrReturn(self.GetPropertyCardinality(property_name));
           })
      .def("has_property", &graphar::VertexInfo::HasProperty)
      .def("save",
           [](const graphar::VertexInfo& self, const std::string& file_name) {
             CheckStatus(self.Save(file_name));
           })
      .def("dump",
           [](const graphar::VertexInfo& self) {
             return ThrowOrReturn(self.Dump());
           })
      .def("is_primary_key", &graphar::VertexInfo::IsPrimaryKey)
      .def("is_nullable_key", &graphar::VertexInfo::IsNullableKey)
      .def("has_property_group", &graphar::VertexInfo::HasPropertyGroup)
      .def(
          "get_file_path",
          [](const graphar::VertexInfo& self,
             std::shared_ptr<graphar::PropertyGroup> property_group,
             graphar::IdType chunk_index) {
            return ThrowOrReturn(self.GetFilePath(property_group, chunk_index));
          })
      .def("get_path_prefix",
           [](const graphar::VertexInfo& self,
              std::shared_ptr<graphar::PropertyGroup> property_group) {
             return ThrowOrReturn(self.GetPathPrefix(property_group));
           })
      .def("get_vertices_num_file_path",
           [](const graphar::VertexInfo& self) {
             return ThrowOrReturn(self.GetVerticesNumFilePath());
           })
      .def("is_validated", &graphar::VertexInfo::IsValidated);

  // Bind EdgeInfo
  py::class_<graphar::EdgeInfo, std::shared_ptr<graphar::EdgeInfo>>(m,
                                                                    "EdgeInfo")
      .def(py::init<const std::string&, const std::string&, const std::string&,
                    graphar::IdType, graphar::IdType, graphar::IdType, bool,
                    const std::vector<std::shared_ptr<graphar::AdjacentList>>&,
                    const std::vector<std::shared_ptr<graphar::PropertyGroup>>&,
                    const std::string&,
                    std::shared_ptr<const graphar::InfoVersion>>(),
           py::arg("src_type"), py::arg("edge_type"), py::arg("dst_type"),
           py::arg("chunk_size"), py::arg("src_chunk_size"),
           py::arg("dst_chunk_size"), py::arg("directed"),
           py::arg("adjacent_lists"), py::arg("property_groups"),
           py::arg("prefix") = "", py::arg("version") = nullptr)
      .def("add_adjacent_list",
           [](const graphar::EdgeInfo& self,
              std::shared_ptr<graphar::AdjacentList> adj_list) {
             return ThrowOrReturn(self.AddAdjacentList(adj_list));
           })
      .def("remove_adjacent_list",
           [](const graphar::EdgeInfo& self,
              std::shared_ptr<graphar::AdjacentList> adj_list) {
             return ThrowOrReturn(self.RemoveAdjacentList(adj_list));
           })
      .def("add_property_group",
           [](const graphar::EdgeInfo& self,
              std::shared_ptr<graphar::PropertyGroup> property_group) {
             return ThrowOrReturn(self.AddPropertyGroup(property_group));
           })
      .def("remove_property_group",
           [](const graphar::EdgeInfo& self,
              std::shared_ptr<graphar::PropertyGroup> property_group) {
             return ThrowOrReturn(self.RemovePropertyGroup(property_group));
           })
      .def("get_src_type", &graphar::EdgeInfo::GetSrcType,
           py::return_value_policy::reference_internal)
      .def("get_edge_type", &graphar::EdgeInfo::GetEdgeType,
           py::return_value_policy::reference_internal)
      .def("get_dst_type", &graphar::EdgeInfo::GetDstType,
           py::return_value_policy::reference_internal)
      .def("get_chunk_size", &graphar::EdgeInfo::GetChunkSize)
      .def("get_src_chunk_size", &graphar::EdgeInfo::GetSrcChunkSize)
      .def("get_dst_chunk_size", &graphar::EdgeInfo::GetDstChunkSize)
      .def("get_prefix", &graphar::EdgeInfo::GetPrefix,
           py::return_value_policy::reference_internal)
      .def("is_directed", &graphar::EdgeInfo::IsDirected)
      .def("version", &graphar::EdgeInfo::version)
      .def("has_adjacent_list_type", &graphar::EdgeInfo::HasAdjacentListType)
      .def("has_property", &graphar::EdgeInfo::HasProperty)
      .def("has_property_group", &graphar::EdgeInfo::HasPropertyGroup)
      .def("get_adjacent_list", &graphar::EdgeInfo::GetAdjacentList)
      .def("property_group_num", &graphar::EdgeInfo::PropertyGroupNum)
      .def("get_property_groups", &graphar::EdgeInfo::GetPropertyGroups,
           py::return_value_policy::reference_internal)
      .def("get_property_group",
           [](const graphar::EdgeInfo& self, const std::string& property) {
             return self.GetPropertyGroup(property);
           })
      .def("get_property_group_by_index",
           [](const graphar::EdgeInfo& self, int index) {
             return self.GetPropertyGroupByIndex(index);
           })
      .def("get_vertices_num_file_path",
           [](const graphar::EdgeInfo& self,
              graphar::AdjListType adj_list_type) {
             return ThrowOrReturn(self.GetVerticesNumFilePath(adj_list_type));
           })
      .def("get_edges_num_file_path",
           [](const graphar::EdgeInfo& self, graphar::IdType vertex_chunk_index,
              graphar::AdjListType adj_list_type) {
             return ThrowOrReturn(
                 self.GetEdgesNumFilePath(vertex_chunk_index, adj_list_type));
           })
      .def("get_adj_list_file_path",
           [](const graphar::EdgeInfo& self, graphar::IdType vertex_chunk_index,
              graphar::IdType edge_chunk_index,
              graphar::AdjListType adj_list_type) {
             return ThrowOrReturn(self.GetAdjListFilePath(
                 vertex_chunk_index, edge_chunk_index, adj_list_type));
           })
      .def("get_adj_list_path_prefix",
           [](const graphar::EdgeInfo& self,
              graphar::AdjListType adj_list_type) {
             return ThrowOrReturn(self.GetAdjListPathPrefix(adj_list_type));
           })
      .def("get_adj_list_offset_file_path",
           [](const graphar::EdgeInfo& self, graphar::IdType vertex_chunk_index,
              graphar::AdjListType adj_list_type) {
             return ThrowOrReturn(self.GetAdjListOffsetFilePath(
                 vertex_chunk_index, adj_list_type));
           })
      .def("get_offset_path_prefix",
           [](const graphar::EdgeInfo& self,
              graphar::AdjListType adj_list_type) {
             return ThrowOrReturn(self.GetOffsetPathPrefix(adj_list_type));
           })
      .def("get_property_file_path",
           [](const graphar::EdgeInfo& self,
              const std::shared_ptr<graphar::PropertyGroup>& property_group,
              graphar::AdjListType adj_list_type,
              graphar::IdType vertex_chunk_index,
              graphar::IdType edge_chunk_index) {
             return ThrowOrReturn(self.GetPropertyFilePath(
                 property_group, adj_list_type, vertex_chunk_index,
                 edge_chunk_index));
           })
      .def("get_property_group_path_prefix",
           [](const graphar::EdgeInfo& self,
              const std::shared_ptr<graphar::PropertyGroup>& property_group,
              graphar::AdjListType adj_list_type) {
             return ThrowOrReturn(self.GetPropertyGroupPathPrefix(
                 property_group, adj_list_type));
           })
      .def("get_property_type",
           [](const graphar::EdgeInfo& self, const std::string& property_name) {
             return ThrowOrReturn(self.GetPropertyType(property_name));
           })
      .def("is_primary_key", &graphar::EdgeInfo::IsPrimaryKey)
      .def("is_nullable_key", &graphar::EdgeInfo::IsNullableKey)
      .def("save",
           [](const graphar::EdgeInfo& self, const std::string& file_name) {
             CheckStatus(self.Save(file_name));
           })
      .def("dump",
           [](const graphar::EdgeInfo& self) {
             return ThrowOrReturn(self.Dump());
           })
      .def("is_validated", &graphar::EdgeInfo::IsValidated);

  // Bind GraphInfo
  py::class_<graphar::GraphInfo, std::shared_ptr<graphar::GraphInfo>>(
      m, "GraphInfo")
      .def(py::init<const std::string&,
                    const std::vector<std::shared_ptr<graphar::VertexInfo>>&,
                    const std::vector<std::shared_ptr<graphar::EdgeInfo>>&,
                    const std::vector<std::string>&, const std::string&,
                    std::shared_ptr<const graphar::InfoVersion>,
                    const std::unordered_map<std::string, std::string>&>(),
           py::arg("graph_name"), py::arg("vertex_infos"),
           py::arg("edge_infos"),
           py::arg("labels") = std::vector<std::string>(),
           py::arg("prefix") = "./", py::arg("version") = nullptr,
           py::arg("extra_info") =
               std::unordered_map<std::string, std::string>())
      .def_static("load",
                  [](const std::string& path) {
                    return ThrowOrReturn(graphar::GraphInfo::Load(path));
                  })
      .def_static(
          "load",
          [](const std::string& input, const std::string& relative_path) {
            return ThrowOrReturn(
                graphar::GraphInfo::Load(input, relative_path));
          })
      .def("add_vertex",
           [](const graphar::GraphInfo& self,
              std::shared_ptr<graphar::VertexInfo> vertex_info) {
             return ThrowOrReturn(self.AddVertex(vertex_info));
           })
      .def("remove_vertex",
           [](const graphar::GraphInfo& self,
              std::shared_ptr<graphar::VertexInfo> vertex_info) {
             return ThrowOrReturn(self.RemoveVertex(vertex_info));
           })
      .def("add_edge",
           [](const graphar::GraphInfo& self,
              std::shared_ptr<graphar::EdgeInfo> edge_info) {
             return ThrowOrReturn(self.AddEdge(edge_info));
           })
      .def("remove_edge",
           [](const graphar::GraphInfo& self,
              std::shared_ptr<graphar::EdgeInfo> edge_info) {
             return ThrowOrReturn(self.RemoveEdge(edge_info));
           })
      .def("get_name", &graphar::GraphInfo::GetName,
           py::return_value_policy::reference_internal)
      .def("get_labels", &graphar::GraphInfo::GetLabels,
           py::return_value_policy::reference_internal)
      .def("get_prefix", &graphar::GraphInfo::GetPrefix,
           py::return_value_policy::reference_internal)
      .def("version", &graphar::GraphInfo::version)
      .def("get_extra_info", &graphar::GraphInfo::GetExtraInfo,
           py::return_value_policy::reference_internal)
      .def("get_vertex_info",
           [](const graphar::GraphInfo& self, const std::string& type) {
             return self.GetVertexInfo(type);
           })
      .def("get_edge_info",
           [](const graphar::GraphInfo& self, const std::string& src_type,
              const std::string& edge_type, const std::string& dst_type) {
             return self.GetEdgeInfo(src_type, edge_type, dst_type);
           })
      .def("get_vertex_info_index", &graphar::GraphInfo::GetVertexInfoIndex)
      .def("get_edge_info_index", &graphar::GraphInfo::GetEdgeInfoIndex)
      .def("vertex_info_num", &graphar::GraphInfo::VertexInfoNum)
      .def("edge_info_num", &graphar::GraphInfo::EdgeInfoNum)
      .def("get_vertex_info_by_index",
           [](const graphar::GraphInfo& self, int index) {
             return self.GetVertexInfoByIndex(index);
           })
      .def("get_edge_info_by_index",
           [](const graphar::GraphInfo& self, int index) {
             return self.GetEdgeInfoByIndex(index);
           })
      .def("get_vertex_infos", &graphar::GraphInfo::GetVertexInfos,
           py::return_value_policy::reference_internal)
      .def("get_edge_infos", &graphar::GraphInfo::GetEdgeInfos,
           py::return_value_policy::reference_internal)
      .def("save",
           [](const graphar::GraphInfo& self, const std::string& path) {
             CheckStatus(self.Save(path));
           })
      .def("dump",
           [](const graphar::GraphInfo& self) {
             return ThrowOrReturn(self.Dump());
           })
      .def("is_validated", &graphar::GraphInfo::IsValidated);
}  // namespace graphar