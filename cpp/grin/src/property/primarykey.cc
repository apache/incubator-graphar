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

extern "C" {
#include "grin/include/property/primarykey.h"
}
#include "grin/src/predefine.h"

#ifdef GRIN_ENABLE_VERTEX_PRIMARY_KEYS
GRIN_VERTEX_TYPE_LIST grin_get_vertex_types_with_primary_keys(GRIN_GRAPH g) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto vtl = new GRIN_VERTEX_TYPE_LIST_T();
  for (unsigned vtype = 0; vtype < _g->vertex_type_num; vtype++) {
    for (unsigned property_id = _g->vertex_property_offsets[vtype];
         property_id < _g->vertex_property_offsets[vtype + 1]; property_id++) {
      if (_g->vertex_properties[property_id].is_primary) {
        vtl->push_back(vtype);
        break;
      }
    }
  }
  return vtl;
}

GRIN_VERTEX_PROPERTY_LIST grin_get_primary_keys_by_vertex_type(
    GRIN_GRAPH g, GRIN_VERTEX_TYPE vtype) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  if (vtype >= _g->vertex_type_num)
    return GRIN_NULL_LIST;
  auto vpl = new GRIN_VERTEX_PROPERTY_LIST_T();
  for (unsigned i = _g->vertex_property_offsets[vtype];
       i < _g->vertex_property_offsets[vtype + 1]; ++i) {
    if (_g->vertex_properties[i].is_primary)
      vpl->push_back(i);
  }
  return vpl;
}

GRIN_VERTEX grin_get_vertex_by_primary_keys(GRIN_GRAPH g,
                                            GRIN_VERTEX_TYPE vtype,
                                            GRIN_ROW r) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto vpl = grin_get_primary_keys_by_vertex_type(g, vtype);
  auto _vpl = static_cast<GRIN_VERTEX_PROPERTY_LIST_T*>(vpl);
  if (_vpl->size() == 0 || r == GRIN_NULL_ROW)
    return GRIN_NULL_VERTEX;
  auto _r = static_cast<GRIN_ROW_T*>(r);

  // traverse all vertices
  auto& vertices = _g->vertices_collections[vtype];
  auto it_end = vertices.end();
  for (auto it = vertices.begin(); it != it_end; it++) {
    bool flag = true;
    unsigned idx = 0;
    for (auto& vp : *_vpl) {
      auto& property = _g->vertex_properties[vp];
      auto& name = property.name;
      auto type = property.type;
      switch (type) {
      case GRIN_DATATYPE::Int32: {
        auto p1 = std::any_cast<int32_t>((*_r)[idx++]);
        auto p2 = it.property<int32_t>(name).value();
        if (p1 != p2)
          flag = false;
        break;
      }
      case GRIN_DATATYPE::Int64: {
        auto p1 = std::any_cast<int64_t>((*_r)[idx++]);
        auto p2 = it.property<int64_t>(name).value();
        if (p1 != p2)
          flag = false;
        break;
      }
      case GRIN_DATATYPE::Float: {
        auto p1 = std::any_cast<float>((*_r)[idx++]);
        auto p2 = it.property<float>(name).value();
        if (p1 != p2)
          flag = false;
        break;
      }
      case GRIN_DATATYPE::Double: {
        auto p1 = std::any_cast<double>((*_r)[idx++]);
        auto p2 = it.property<double>(name).value();
        if (p1 != p2)
          flag = false;
        break;
      }
      case GRIN_DATATYPE::String: {
        auto&& p1 = std::any_cast<std::string>((*_r)[idx++]);
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
      auto v = new GRIN_VERTEX_T(it.id(), vtype);
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
