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

#include <utility>

#include "grin/include/property/primarykey.h"
#include "grin/include/property/propertytable.h"
#include "grin/src/predefine.h"

#ifdef GRIN_ENABLE_VERTEX_PRIMARY_KEYS
GRIN_VERTEX_TYPE_LIST grin_get_vertex_types_with_primary_keys(GRIN_GRAPH g) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto vtl = new GRIN_VERTEX_TYPE_LIST_T();
  unsigned type_id = 0;
  for (const auto& [label, vertex_info] : _g->graph_info.GetVertexInfos()) {
    bool flag = false;
    for (auto& group : vertex_info.GetPropertyGroups()) {
      for (auto& property : group.GetProperties()) {
        if (property.is_primary) {
          flag = true;
          break;
        }
      }
      if (flag)
        break;
    }
    if (flag)
      vtl->push_back(type_id);
    type_id++;
  }
  return vtl;
}

GRIN_VERTEX_PROPERTY_LIST grin_get_primary_keys_by_vertex_type(
    GRIN_GRAPH g, GRIN_VERTEX_TYPE vtype) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vtype = static_cast<GRIN_VERTEX_TYPE_T*>(vtype);
  auto type_id = *_vtype;
  auto type_name = _g->vertex_types[type_id];
  auto vertex_info = _g->graph_info.GetVertexInfo(type_name).value();
  auto vpl = new GRIN_VERTEX_PROPERTY_LIST_T();
  for (auto& group : vertex_info.GetPropertyGroups()) {
    for (auto& property : group.GetProperties()) {
      if (property.is_primary) {
        GRIN_VERTEX_PROPERTY_T vp(type_id, property.name,
                                  GARToDataType(property.type));
        vpl->push_back(vp);
      }
    }
  }
  return vpl;
}

GRIN_VERTEX grin_get_vertex_by_primary_keys(GRIN_GRAPH g,
                                            GRIN_VERTEX_TYPE vtype,
                                            GRIN_ROW r) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vtype = static_cast<GRIN_VERTEX_TYPE_T*>(vtype);
  auto vpl = grin_get_primary_keys_by_vertex_type(g, vtype);
  auto _vpl = static_cast<GRIN_VERTEX_PROPERTY_LIST_T*>(vpl);
  if (_vpl->size() == 0)
    return GRIN_NULL_VERTEX;
  auto type_id = *_vtype;
  auto& vertices = _g->vertices_collections[type_id];
  auto it_end = vertices.end();
  for (auto it = vertices.begin(); it != it_end; it++) {
    bool flag = true;
    unsigned idx = 0;
    for (auto& property : *_vpl) {
      auto name = property.name;
      auto type = property.type;
      auto value = grin_get_value_from_row(g, r, type, idx);
      idx++;
      switch (type) {
      case GRIN_DATATYPE::Int32: {
        auto p1 = *static_cast<int32_t const*>(value);
        auto p2 = it.property<int32_t>(name).value();
        if (p1 != p2)
          flag = false;
        break;
      }
      case GRIN_DATATYPE::Int64: {
        auto p1 = *static_cast<int64_t const*>(value);
        auto p2 = it.property<int64_t>(name).value();
        if (p1 != p2)
          flag = false;
        break;
      }
      case GRIN_DATATYPE::Float: {
        auto p1 = *static_cast<float const*>(value);
        auto p2 = it.property<float>(name).value();
        if (p1 != p2)
          flag = false;
        break;
      }
      case GRIN_DATATYPE::Double: {
        auto p1 = *static_cast<double const*>(value);
        auto p2 = it.property<double>(name).value();
        if (p1 != p2)
          flag = false;
        break;
      }
      case GRIN_DATATYPE::String: {
        auto p1 = *static_cast<std::string const*>(value);
        auto p2 = it.property<std::string>(name).value();
        if (p1 != p2)
          flag = false;
        break;
      }
      default:
        flag = false;
      }
      if (!flag)
        break;
    }
    if (flag) {
      auto v = new GRIN_VERTEX_T(it.id(), type_id);
      return v;
    }
  }
  return GRIN_NULL_VERTEX;
}
#endif

#ifdef GRIN_WITH_EDGE_PRIMARY_KEYS
GRIN_EDGE_TYPE_LIST grin_get_edge_types_with_primary_keys(GRIN_GRAPH);

GRIN_EDGE_PROPERTY_LIST grin_get_primary_keys_by_edge_type(GRIN_GRAPH,
                                                           GRIN_EDGE_TYPE);

GRIN_EDGE grin_get_edge_by_primay_keys(GRIN_GRAPH, GRIN_EDGE_TYPE, GRIN_ROW);
#endif
