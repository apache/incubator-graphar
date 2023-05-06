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
  graph->unique_edge_type_begin_type.clear();

  for (const auto& [label, vertex_info] : graph->graph_info.GetVertexInfos()) {
    auto maybe_vertices_collection =
        GAR_NAMESPACE::ConstructVerticesCollection(graph->graph_info, label);
    auto& vertices = maybe_vertices_collection.value();
    graph->vertex_types.push_back(label);
    graph->tot_vertex_num += vertices.size();
    graph->vertex_offsets.push_back(graph->tot_vertex_num);
    graph->vertices_collections.push_back(std::move(vertices));
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
      graph->unique_edge_type_begin_type.push_back(graph->edge_type_num);
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
    graph->edge_num.push_back(0);
    std::map<GAR_NAMESPACE::AdjListType, GAR_NAMESPACE::Edges> edge_map;
    graph->edges_collections.push_back(edge_map);

    GAR_NAMESPACE::AdjListType adj_list_type = GAR_ORDERED_BY_SOURCE;
    if (edge_info.ContainAdjList(adj_list_type)) {
      auto maybe_edges_collection = GAR_NAMESPACE::ConstructEdgesCollection(
          graph->graph_info, src_label, edge_label, dst_label, adj_list_type);
      auto& edges = maybe_edges_collection.value();
      graph->edges_collections[graph->edge_type_num].insert(
          {adj_list_type, std::move(edges)});
    }

    adj_list_type = GAR_ORDERED_BY_DEST;
    if (edge_info.ContainAdjList(adj_list_type)) {
      auto maybe_edges_collection = GAR_NAMESPACE::ConstructEdgesCollection(
          graph->graph_info, src_label, edge_label, dst_label, adj_list_type);
      auto& edges = maybe_edges_collection.value();
      graph->edges_collections[graph->edge_type_num].insert(
          {adj_list_type, std::move(edges)});
    }

    adj_list_type = GAR_UNORDERED_BY_SOURCE;
    if (edge_info.ContainAdjList(adj_list_type)) {
      auto maybe_edges_collection = GAR_NAMESPACE::ConstructEdgesCollection(
          graph->graph_info, src_label, edge_label, dst_label, adj_list_type);
      auto& edges = maybe_edges_collection.value();
      graph->edges_collections[graph->edge_type_num].insert(
          {adj_list_type, std::move(edges)});
    }

    adj_list_type = GAR_UNORDERED_BY_DEST;
    if (edge_info.ContainAdjList(adj_list_type)) {
      auto maybe_edges_collection = GAR_NAMESPACE::ConstructEdgesCollection(
          graph->graph_info, src_label, edge_label, dst_label, adj_list_type);
      auto& edges = maybe_edges_collection.value();
      graph->edges_collections[graph->edge_type_num].insert(
          {adj_list_type, std::move(edges)});
    }
    graph->edge_type_num++;
  }
  graph->unique_edge_type_begin_type.push_back(graph->edge_type_num);
  graph->tot_edge_num = __grin_get_edge_num(graph, 0, graph->edge_type_num);
  __grin_init_vertex_properties(graph);
  __grin_init_edge_properties(graph);
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
    if (_g->edge_num[type_id] != 0) {
      res += _g->edge_num[type_id];
      continue;
    }

    if (_g->edges_collections[type_id].find(GAR_ORDERED_BY_SOURCE) !=
        _g->edges_collections[type_id].end()) {
      _g->edge_num[type_id] =
          std::get<GAR_NAMESPACE::EdgesCollection<GAR_ORDERED_BY_SOURCE>>(
              _g->edges_collections[type_id].at(GAR_ORDERED_BY_SOURCE))
              .size();
    } else if (_g->edges_collections[type_id].find(GAR_ORDERED_BY_DEST) !=
               _g->edges_collections[type_id].end()) {
      _g->edge_num[type_id] =
          std::get<GAR_NAMESPACE::EdgesCollection<GAR_ORDERED_BY_DEST>>(
              _g->edges_collections[type_id].at(GAR_ORDERED_BY_DEST))
              .size();
    } else if (_g->edges_collections[type_id].find(GAR_UNORDERED_BY_SOURCE) !=
               _g->edges_collections[type_id].end()) {
      _g->edge_num[type_id] =
          std::get<GAR_NAMESPACE::EdgesCollection<GAR_UNORDERED_BY_SOURCE>>(
              _g->edges_collections[type_id].at(GAR_UNORDERED_BY_SOURCE))
              .size();
    } else if (_g->edges_collections[type_id].find(GAR_UNORDERED_BY_DEST) !=
               _g->edges_collections[type_id].end()) {
      _g->edge_num[type_id] =
          std::get<GAR_NAMESPACE::EdgesCollection<GAR_UNORDERED_BY_DEST>>(
              _g->edges_collections[type_id].at(GAR_UNORDERED_BY_DEST))
              .size();
    }
    res += _g->edge_num[type_id];
  }
  return res;
}

void __grin_init_vertex_properties(GRIN_GRAPH_T* _g) {
  _g->vertex_properties.clear();
  _g->vertex_property_offsets.clear();
  _g->vertex_property_name_2_ids.clear();
  unsigned property_id = 0, vtype = 0;
  for (const auto& [label, vertex_info] : _g->graph_info.GetVertexInfos()) {
    _g->vertex_property_offsets.push_back(property_id);
    std::map<std::string, unsigned> name_2_id;
    for (auto& group : vertex_info.GetPropertyGroups()) {
      for (auto& property : group.GetProperties()) {
        GRIN_VERTEX_PROPERTY_T vp(vtype, property.name,
                                  GARToDataType(property.type),
                                  property.is_primary);
        _g->vertex_properties.push_back(vp);
        name_2_id.insert({property.name, property_id});
        property_id++;
      }
    }
    _g->vertex_property_name_2_ids.push_back(name_2_id);
    vtype++;
  }
  _g->vertex_property_offsets.push_back(property_id);
}

void __grin_init_edge_properties(GRIN_GRAPH_T* _g) {
  _g->edge_properties.clear();
  _g->edge_property_offsets.clear();
  _g->edge_property_name_2_ids.clear();
  unsigned property_id = 0;

  for (unsigned etype = 0; etype < _g->unique_edge_type_num; etype++) {
    _g->edge_property_offsets.push_back(property_id);
    std::map<std::string, unsigned> name_2_id;
    for (auto et = _g->unique_edge_type_begin_type[etype];
         et < _g->unique_edge_type_begin_type[etype + 1]; ++et) {
      auto& edge_info = _g->graph_info
                            .GetEdgeInfo(_g->vertex_types[_g->src_type_ids[et]],
                                         _g->edge_types[et],
                                         _g->vertex_types[_g->dst_type_ids[et]])
                            .value();
      auto adj_list_type = _g->edges_collections[et].begin()->first;
      for (auto& group : edge_info.GetPropertyGroups(adj_list_type).value()) {
        for (auto& property : group.GetProperties()) {
          GRIN_EDGE_PROPERTY_T ep(etype, property.name,
                                  GARToDataType(property.type));
          if (name_2_id.find(property.name) != name_2_id.end()) {
            // TODO: throw exception
            continue;
          }
          _g->edge_properties.push_back(ep);
          name_2_id.insert({property.name, property_id});
          property_id++;
        }
      }
    }
    _g->edge_property_name_2_ids.push_back(name_2_id);
  }
  _g->edge_property_offsets.push_back(property_id);
}
