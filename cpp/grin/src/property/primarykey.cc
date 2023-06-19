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

#include "grin/src/predefine.h"
// GRIN headers
#include "property/primarykey.h"

#define __grin_get_gar_vertex(_v)                           \
  if (_v->vertex.has_value() == false) {                    \
    auto& vertices = _g->vertices_collections[_v->type_id]; \
    auto it = vertices.begin() + _v->id;                    \
    _v->vertex = *it;                                       \
  }

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
    return GRIN_NULL_VERTEX_PROPERTY_LIST;
  auto vpl = new GRIN_VERTEX_PROPERTY_LIST_T();
  for (unsigned i = _g->vertex_property_offsets[vtype];
       i < _g->vertex_property_offsets[vtype + 1]; ++i) {
    if (_g->vertex_properties[i].is_primary)
      vpl->push_back(i);
  }
  return vpl;
}

GRIN_ROW grin_get_vertex_primary_keys_row(GRIN_GRAPH g, GRIN_VERTEX v) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  __grin_get_gar_vertex(_v);

  auto r = new GRIN_ROW_T();
  for (auto vp = _g->vertex_property_offsets[_v->type_id];
       vp < _g->vertex_property_offsets[_v->type_id + 1]; ++vp) {
    auto& property = _g->vertex_properties[vp];
    if (!property.is_primary)
      continue;

    switch (property.type) {
    case GRIN_DATATYPE::Int32: {
      auto value = _v->vertex.value().property<int32_t>(property.name).value();
      r->push_back(value);
      break;
    }
    case GRIN_DATATYPE::Int64: {
      auto value = _v->vertex.value().property<int64_t>(property.name).value();
      r->push_back(value);
      break;
    }
    case GRIN_DATATYPE::Float: {
      auto value = _v->vertex.value().property<float>(property.name).value();
      r->push_back(value);
      break;
    }
    case GRIN_DATATYPE::Double: {
      auto value = _v->vertex.value().property<double>(property.name).value();
      r->push_back(value);
      break;
    }
    case GRIN_DATATYPE::String: {
      auto value =
          _v->vertex.value().property<std::string>(property.name).value();
      r->push_back(std::move(value));
      break;
    }
    default: {
      delete r;
      return GRIN_NULL_ROW;
    }
    }
  }
  return r;
}
#endif

#ifdef GRIN_WITH_EDGE_PRIMARY_KEYS
GRIN_EDGE_TYPE_LIST grin_get_edge_types_with_primary_keys(GRIN_GRAPH);

GRIN_EDGE_PROPERTY_LIST grin_get_primary_keys_by_edge_type(GRIN_GRAPH,
                                                           GRIN_EDGE_TYPE);

GRIN_EDGE grin_get_edge_by_primay_keys(GRIN_GRAPH, GRIN_EDGE_TYPE, GRIN_ROW);
#endif
