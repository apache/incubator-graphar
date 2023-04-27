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
#include <set>

extern "C" {
#include "grin/include/property/propertylist.h"
}
#include "grin/src/predefine.h"

#ifdef GRIN_WITH_VERTEX_PROPERTY
GRIN_VERTEX_PROPERTY_LIST grin_get_vertex_property_list_by_type(
    GRIN_GRAPH g, GRIN_VERTEX_TYPE vtype) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vtype = static_cast<GRIN_VERTEX_TYPE_T*>(vtype);
  auto type_id = *_vtype;
  auto& type_name = _g->vertex_types[type_id];
  auto& vertex_info = _g->graph_info.GetVertexInfo(type_name).value();
  auto vpl = new GRIN_VERTEX_PROPERTY_LIST_T();
  for (auto& group : vertex_info.GetPropertyGroups()) {
    for (auto& property : group.GetProperties()) {
      GRIN_VERTEX_PROPERTY_T vp(type_id, property.name,
                                GARToDataType(property.type));
      vpl->push_back(vp);
    }
  }
  return vpl;
}

size_t grin_get_vertex_property_list_size(GRIN_GRAPH g,
                                          GRIN_VERTEX_PROPERTY_LIST vpl) {
  auto _vpl = static_cast<GRIN_VERTEX_PROPERTY_LIST_T*>(vpl);
  return _vpl->size();
}

GRIN_VERTEX_PROPERTY grin_get_vertex_property_from_list(
    GRIN_GRAPH g, GRIN_VERTEX_PROPERTY_LIST vpl, size_t idx) {
  auto _vpl = static_cast<GRIN_VERTEX_PROPERTY_LIST_T*>(vpl);
  auto vp = new GRIN_VERTEX_PROPERTY_T((*_vpl)[idx]);
  return vp;
}

GRIN_VERTEX_PROPERTY_LIST grin_create_vertex_property_list(GRIN_GRAPH g) {
  auto vpl = new GRIN_VERTEX_PROPERTY_LIST_T();
  return vpl;
}

void grin_destroy_vertex_property_list(GRIN_GRAPH g,
                                       GRIN_VERTEX_PROPERTY_LIST vpl) {
  auto _vpl = static_cast<GRIN_VERTEX_PROPERTY_LIST_T*>(vpl);
  delete _vpl;
}

bool grin_insert_vertex_property_to_list(GRIN_GRAPH g,
                                         GRIN_VERTEX_PROPERTY_LIST vpl,
                                         GRIN_VERTEX_PROPERTY vp) {
  auto _vpl = static_cast<GRIN_VERTEX_PROPERTY_LIST_T*>(vpl);
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  _vpl->push_back(*_vp);
  return true;
}
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_PROPERTY
GRIN_VERTEX_PROPERTY grin_get_vertex_property_from_id(GRIN_GRAPH,
                                                      GRIN_VERTEX_TYPE,
                                                      GRIN_VERTEX_PROPERTY_ID);

GRIN_VERTEX_PROPERTY_ID grin_get_vertex_property_id(GRIN_GRAPH,
                                                    GRIN_VERTEX_TYPE,
                                                    GRIN_VERTEX_PROPERTY);
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
GRIN_EDGE_PROPERTY_LIST grin_get_edge_property_list_by_type(
    GRIN_GRAPH g, GRIN_EDGE_TYPE etype) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _etype = static_cast<GRIN_EDGE_TYPE_T*>(etype);
  auto epl = new GRIN_EDGE_PROPERTY_LIST_T();
  std::set<GRIN_EDGE_PROPERTY_T> edge_properties;
  for (auto etype = 0; etype < _g->edge_type_num; ++etype) {
    if (_g->unique_edge_type_ids[etype] > *_etype)
      break;
    if (_g->unique_edge_type_ids[etype] < *_etype)
      continue;
    auto& edge_info =
        _g->graph_info
            .GetEdgeInfo(_g->vertex_types[_g->src_type_ids[etype]],
                         _g->edge_types[etype],
                         _g->vertex_types[_g->dst_type_ids[etype]])
            .value();
    auto adj_list_type = _g->edges_collections[etype].begin()->first;
    for (auto& group : edge_info.GetPropertyGroups(adj_list_type).value()) {
      for (auto& property : group.GetProperties()) {
        GRIN_EDGE_PROPERTY_T ep(*_etype, property.name,
                                GARToDataType(property.type));
        if (edge_properties.find(ep) != edge_properties.end())
          continue;
        edge_properties.insert(ep);
        epl->push_back(ep);
      }
    }
  }
  return epl;
}

size_t grin_get_edge_property_list_size(GRIN_GRAPH g,
                                        GRIN_EDGE_PROPERTY_LIST epl) {
  auto _epl = static_cast<GRIN_EDGE_PROPERTY_LIST_T*>(epl);
  return _epl->size();
}

GRIN_EDGE_PROPERTY grin_get_edge_property_from_list(GRIN_GRAPH g,
                                                    GRIN_EDGE_PROPERTY_LIST epl,
                                                    size_t idx) {
  auto _epl = static_cast<GRIN_EDGE_PROPERTY_LIST_T*>(epl);
  auto ep = new GRIN_EDGE_PROPERTY_T((*_epl)[idx]);
  return ep;
}

GRIN_EDGE_PROPERTY_LIST grin_create_edge_property_list(GRIN_GRAPH g) {
  auto epl = new GRIN_EDGE_PROPERTY_LIST_T();
  return epl;
}

void grin_destroy_edge_property_list(GRIN_GRAPH g,
                                     GRIN_EDGE_PROPERTY_LIST epl) {
  auto _epl = static_cast<GRIN_EDGE_PROPERTY_LIST_T*>(epl);
  delete _epl;
}

bool grin_insert_edge_property_to_list(GRIN_GRAPH g,
                                       GRIN_EDGE_PROPERTY_LIST epl,
                                       GRIN_EDGE_PROPERTY ep) {
  auto _epl = static_cast<GRIN_EDGE_PROPERTY_LIST_T*>(epl);
  auto _ep = static_cast<GRIN_EDGE_PROPERTY_T*>(ep);
  _epl->push_back(*_ep);
  return true;
}
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_EDGE_PROPERTY
GRIN_EDGE_PROPERTY grin_get_edge_property_from_id(GRIN_GRAPH, GRIN_EDGE_TYPE,
                                                  GRIN_EDGE_PROPERTY_ID);

GRIN_EDGE_PROPERTY_ID grin_get_edge_property_id(GRIN_GRAPH, GRIN_EDGE_TYPE,
                                                GRIN_EDGE_PROPERTY);
#endif
