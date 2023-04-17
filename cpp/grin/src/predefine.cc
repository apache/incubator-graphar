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

#include <algorithm>
#include <string>
#include <vector>

#include "grin/src/predefine.h"

bool cmp(const GAR_NAMESPACE::EdgeInfo& info1,
         const GAR_NAMESPACE::EdgeInfo& info2) {
  return info1.GetEdgeLabel() < info2.GetEdgeLabel();
}

GRIN_GRAPH get_graph_by_info_path(const std::string& path) {
  auto maybe_graph_info = GAR_NAMESPACE::GraphInfo::Load(path);
  if (maybe_graph_info.has_error())
    return GRIN_NULL_GRAPH;
  auto graph_info = maybe_graph_info.value();

  auto graph = new GRIN_GRAPH_T(graph_info);
  graph->vertex_type_num = graph_info.GetVertexInfos().size();
  graph->vertex_types.clear();
  graph->vertex_offsets.clear();
  graph->vertex_offsets.push_back(0);

  for (const auto& [label, vertex_info] : graph->graph_info.GetVertexInfos()) {
    auto maybe_vertices_collection =
        GAR_NAMESPACE::ConstructVerticesCollection(graph->graph_info, label);
    auto vertices = maybe_vertices_collection.value();
    graph->vertex_types.push_back(label);
    graph->tot_vertex_num += vertices.size();
    graph->vertex_offsets.push_back(graph->tot_vertex_num);
    graph->vertices_collections.push_back(vertices);
  }

  std::vector<GAR_NAMESPACE::EdgeInfo> all_edge_infos;
  for (const auto& [label, edge_info] : graph->graph_info.GetEdgeInfos()) {
    all_edge_infos.push_back(edge_info);
  }
  std::sort(all_edge_infos.begin(), all_edge_infos.end(), cmp);

  for (const auto& edge_info : all_edge_infos) {
    auto src_label = edge_info.GetSrcLabel();
    auto dst_label = edge_info.GetDstLabel();
    auto edge_label = edge_info.GetEdgeLabel();
    if (graph->unique_edge_type_2_ids.find(edge_label) ==
        graph->unique_edge_type_2_ids.end()) {
      graph->unique_edge_types.push_back(edge_label);
      graph->unique_edge_type_2_ids.insert(
          {edge_label, graph->unique_edge_type_num});
      graph->unique_edge_type_num++;
    }

    unsigned src_type_id = std::find(graph->vertex_types.begin(),
                                     graph->vertex_types.end(), src_label) -
                           graph->vertex_types.begin();
    unsigned dst_type_id = std::find(graph->vertex_types.begin(),
                                     graph->vertex_types.end(), dst_label) -
                           graph->vertex_types.begin();
    unsigned unique_edge_type_id = graph->unique_edge_type_2_ids.at(edge_label);
    graph->src_type_ids.push_back(src_type_id);
    graph->dst_type_ids.push_back(dst_type_id);
    graph->unique_edge_type_ids.push_back(unique_edge_type_id);
    graph->edge_types.push_back(edge_label);
    graph->edge_num.push_back(-1);
    std::map<GAR_NAMESPACE::AdjListType, GAR_NAMESPACE::Edges> edge_map;
    graph->edges_collections.push_back(edge_map);

    GAR_NAMESPACE::AdjListType adj_list_type =
        GAR_NAMESPACE::AdjListType::ordered_by_source;
    if (edge_info.ContainAdjList(adj_list_type)) {
      auto maybe_edges_collection = GAR_NAMESPACE::ConstructEdgesCollection(
          graph->graph_info, src_label, edge_label, dst_label, adj_list_type);
      auto edges = maybe_edges_collection.value();
      graph->edges_collections[graph->edge_type_num].insert(
          {adj_list_type, edges});
    }

    adj_list_type = GAR_NAMESPACE::AdjListType::ordered_by_dest;
    if (edge_info.ContainAdjList(adj_list_type)) {
      auto maybe_edges_collection = GAR_NAMESPACE::ConstructEdgesCollection(
          graph->graph_info, src_label, edge_label, dst_label, adj_list_type);
      auto edges = maybe_edges_collection.value();
      graph->edges_collections[graph->edge_type_num].insert(
          {adj_list_type, edges});
    }

    adj_list_type = GAR_NAMESPACE::AdjListType::unordered_by_source;
    if (edge_info.ContainAdjList(adj_list_type)) {
      auto maybe_edges_collection = GAR_NAMESPACE::ConstructEdgesCollection(
          graph->graph_info, src_label, edge_label, dst_label, adj_list_type);
      auto edges = maybe_edges_collection.value();
      graph->edges_collections[graph->edge_type_num].insert(
          {adj_list_type, edges});
    }

    adj_list_type = GAR_NAMESPACE::AdjListType::unordered_by_dest;
    if (edge_info.ContainAdjList(adj_list_type)) {
      auto maybe_edges_collection = GAR_NAMESPACE::ConstructEdgesCollection(
          graph->graph_info, src_label, edge_label, dst_label, adj_list_type);
      auto edges = maybe_edges_collection.value();
      graph->edges_collections[graph->edge_type_num].insert(
          {adj_list_type, edges});
    }
    graph->edge_type_num++;
  }
  graph->tot_edge_num = __grin_get_edge_num(graph, 0, graph->edge_type_num);
  return graph;
}

std::string GetDataTypeName(GRIN_DATATYPE type) {
  switch (type) {
  case GRIN_DATATYPE::Int32:
    return "int32";
  case GRIN_DATATYPE::UInt32:
    return "uint32";
  case GRIN_DATATYPE::Int64:
    return "int64";
  case GRIN_DATATYPE::UInt64:
    return "uint64";
  case GRIN_DATATYPE::Float:
    return "float";
  case GRIN_DATATYPE::Double:
    return "double";
  case GRIN_DATATYPE::String:
    return "string";
  case GRIN_DATATYPE::Date32:
    return "date32";
  case GRIN_DATATYPE::Date64:
    return "date64";
  default:
    return "undefined";
  }
}

GRIN_DATATYPE GARToDataType(GAR_NAMESPACE::DataType type) {
  switch (type.id()) {
  case GAR_NAMESPACE::Type::BOOL:
    return GRIN_DATATYPE::Undefined;
  case GAR_NAMESPACE::Type::INT32:
    return GRIN_DATATYPE::Int32;
  case GAR_NAMESPACE::Type::INT64:
    return GRIN_DATATYPE::Int64;
  case GAR_NAMESPACE::Type::FLOAT:
    return GRIN_DATATYPE::Float;
  case GAR_NAMESPACE::Type::DOUBLE:
    return GRIN_DATATYPE::Double;
  case GAR_NAMESPACE::Type::STRING:
    return GRIN_DATATYPE::String;
  default:
    return GRIN_DATATYPE::Undefined;
  }
}

size_t __grin_get_edge_num(GRIN_GRAPH_T* _g, unsigned type_begin,
                           unsigned type_end) {
  size_t res = 0;
  for (auto type_id = type_begin; type_id < type_end; type_id++) {
    if (type_id >= _g->edge_type_num)
      break;
    if (_g->edge_num[type_id] != -1) {
      res += _g->edge_num[type_id];
      continue;
    }
    _g->edge_num[type_id] = 0;

    if (_g->edges_collections[type_id].find(
            GAR_NAMESPACE::AdjListType::ordered_by_source) !=
        _g->edges_collections[type_id].end()) {
      auto adj_list_type = GAR_NAMESPACE::AdjListType::ordered_by_source;
      _g->edge_num[type_id] =
          std::get<GAR_NAMESPACE::EdgesCollection<
              GAR_NAMESPACE::AdjListType::ordered_by_source>>(
              _g->edges_collections[type_id].at(adj_list_type))
              .size();
    } else if (_g->edges_collections[type_id].find(
                   GAR_NAMESPACE::AdjListType::ordered_by_dest) !=
               _g->edges_collections[type_id].end()) {
      auto adj_list_type = GAR_NAMESPACE::AdjListType::ordered_by_dest;
      _g->edge_num[type_id] =
          std::get<GAR_NAMESPACE::EdgesCollection<
              GAR_NAMESPACE::AdjListType::ordered_by_dest>>(
              _g->edges_collections[type_id].at(adj_list_type))
              .size();
    } else if (_g->edges_collections[type_id].find(
                   GAR_NAMESPACE::AdjListType::unordered_by_source) !=
               _g->edges_collections[type_id].end()) {
      auto adj_list_type = GAR_NAMESPACE::AdjListType::unordered_by_source;
      _g->edge_num[type_id] =
          std::get<GAR_NAMESPACE::EdgesCollection<
              GAR_NAMESPACE::AdjListType::unordered_by_source>>(
              _g->edges_collections[type_id].at(adj_list_type))
              .size();
    } else if (_g->edges_collections[type_id].find(
                   GAR_NAMESPACE::AdjListType::unordered_by_dest) !=
               _g->edges_collections[type_id].end()) {
      auto adj_list_type = GAR_NAMESPACE::AdjListType::unordered_by_dest;
      _g->edge_num[type_id] =
          std::get<GAR_NAMESPACE::EdgesCollection<
              GAR_NAMESPACE::AdjListType::unordered_by_dest>>(
              _g->edges_collections[type_id].at(adj_list_type))
              .size();
    }
    res += _g->edge_num[type_id];
  }
  return res;
}
