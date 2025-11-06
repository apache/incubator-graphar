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

#include "graphar/filesystem.h"
#include "graphar/graph_info.h"
#include "graphar/reader_util.h"
#include "importer.h"

#define STRINGIFY(x) #x
#define MACRO_STRINGIFY(x) STRINGIFY(x)

std::string ShowGraph(const std::string& path) {
  // TODO: check all the result values
  auto graph_info = graphar::GraphInfo::Load(path).value();
  return graph_info->Dump().value();
}

std::string ShowVertex(const std::string& path,
                       const std::string& vertex_type) {
  auto graph_info = graphar::GraphInfo::Load(path).value();
  auto vertex_info = graph_info->GetVertexInfo(vertex_type);
  return vertex_info->Dump().value();
}

std::string ShowEdge(const std::string& path, const std::string& src_type,
                     const std::string& edge_type,
                     const std::string& dst_type) {
  auto graph_info = graphar::GraphInfo::Load(path).value();
  auto edge_info = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
  return edge_info->Dump().value();
}

bool CheckGraph(const std::string& path) {
  auto graph_info = graphar::GraphInfo::Load(path).value();
  return graph_info->IsValidated();
}

bool CheckVertex(const std::string& path, const std::string& vertex_type) {
  auto graph_info = graphar::GraphInfo::Load(path).value();
  auto vertex_info = graph_info->GetVertexInfo(vertex_type);
  return vertex_info->IsValidated();
}

bool CheckEdge(const std::string& path, const std::string& src_type,
               const std::string& edge_type, const std::string& dst_type) {
  auto graph_info = graphar::GraphInfo::Load(path).value();
  auto edge_info = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
  return edge_info->IsValidated();
}

int64_t GetVertexCount(const std::string& path,
                       const std::string& vertex_type) {
  auto graph_info = graphar::GraphInfo::Load(path).value();
  auto graph_prefix = graph_info->GetPrefix();
  auto vertex_info = graph_info->GetVertexInfo(vertex_type);
  return graphar::util::GetVertexNum(graph_prefix, vertex_info).value();
}

// TODO(ljj): Add this to graphar library

std::vector<graphar::AdjListType> _GetAdjListTypes(
    const std::shared_ptr<graphar::EdgeInfo>& edge_info) {
  std::vector<graphar::AdjListType> adj_list_types;
  if (edge_info->HasAdjacentListType(graphar::AdjListType::ordered_by_dest)) {
    adj_list_types.push_back(graphar::AdjListType::ordered_by_dest);
  }
  if (edge_info->HasAdjacentListType(graphar::AdjListType::ordered_by_source)) {
    adj_list_types.push_back(graphar::AdjListType::ordered_by_source);
  }
  if (edge_info->HasAdjacentListType(graphar::AdjListType::unordered_by_dest)) {
    adj_list_types.push_back(graphar::AdjListType::unordered_by_dest);
  }
  if (edge_info->HasAdjacentListType(
          graphar::AdjListType::unordered_by_source)) {
    adj_list_types.push_back(graphar::AdjListType::unordered_by_source);
  }
  if (adj_list_types.empty()) {
    throw std::runtime_error("No valid adj list type found");
  }
  return adj_list_types;
}

int64_t GetEdgeCount(const std::string& path, const std::string& src_type,
                     const std::string& edge_type,
                     const std::string& dst_type) {
  auto graph_info = graphar::GraphInfo::Load(path).value();
  auto graph_prefix = graph_info->GetPrefix();
  auto edge_info = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
  auto adj_list_types = _GetAdjListTypes(edge_info);
  auto adj_list_type = adj_list_types[0];
  auto vertices_num_file_path =
      edge_info->GetVerticesNumFilePath(adj_list_type).value();
  std::string base_dir;
  auto fs = graphar::FileSystemFromUriOrPath(graph_prefix, &base_dir).value();
  std::string vertices_num_path = base_dir + vertices_num_file_path;
  auto vertices_num = fs->ReadFileToValue<int64_t>(vertices_num_path).value();
  int max_chunk_index = (vertices_num + edge_info->GetSrcChunkSize() - 1) /
                        edge_info->GetSrcChunkSize();
  int64_t edge_count = 0;
  for (int i = 0; i < max_chunk_index; i++) {
    // TODO: file may not exist
    edge_count +=
        graphar::util::GetEdgeNum(graph_prefix, edge_info, adj_list_type, i)
            .value();
  }
  return edge_count;
}

std::vector<std::string> GetVertexTypes(const std::string& path) {
  auto graph_info = graphar::GraphInfo::Load(path).value();
  auto vertex_infos = graph_info->GetVertexInfos();
  // TODO: change to unordered_set
  std::vector<std::string> vertex_types;
  for (const auto& vertex_info : vertex_infos) {
    vertex_types.push_back(vertex_info->GetType());
  }
  return vertex_types;
}

std::vector<std::vector<std::string>> GetEdgeTypes(const std::string& path) {
  auto graph_info = graphar::GraphInfo::Load(path).value();
  auto edge_infos = graph_info->GetEdgeInfos();
  // TODO: change to unordered_set
  std::vector<std::vector<std::string>> edge_types;
  for (const auto& edge_info : edge_infos) {
    std::vector<std::string> edge_type;
    edge_type.push_back(edge_info->GetSrcType());
    edge_type.push_back(edge_info->GetEdgeType());
    edge_type.push_back(edge_info->GetDstType());
    edge_types.push_back(edge_type);
  }
  return edge_types;
}

namespace py = pybind11;

// Changed from PYBIND11_MODULE to a regular function
extern "C" void bind_cli(pybind11::module_& m) {
  // CLI-level convenience functions
  m.def("show_graph", &ShowGraph, "Show the graph info");
  m.def("show_vertex", &ShowVertex, "Show the vertex info");
  m.def("show_edge", &ShowEdge, "Show the edge info");
  m.def("check_graph", &CheckGraph, "Check the graph info");
  m.def("check_vertex", &CheckVertex, "Check the vertex info");
  m.def("check_edge", &CheckEdge, "Check the edge info");
  m.def("get_vertex_types", &GetVertexTypes, "Get the vertex types");
  m.def("get_edge_types", &GetEdgeTypes, "Get the edge types");
  m.def("get_vertex_count", &GetVertexCount, "Get the vertex count");
  m.def("get_edge_count", &GetEdgeCount, "Get the edge count");
  m.def("do_import", &DoImport, "Do the import");
#ifdef VERSION_INFO
  m.attr("__version__") = MACRO_STRINGIFY(VERSION_INFO);
#else
  m.attr("__version__") = "dev";
#endif
}