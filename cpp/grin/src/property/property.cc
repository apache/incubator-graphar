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

extern "C" {
#include "grin/include/property/property.h"
}
#include "grin/src/predefine.h"

#ifdef GRIN_WITH_VERTEX_PROPERTY_NAME
const char* grin_get_vertex_property_name(GRIN_GRAPH g,
                                          GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto& s = _g->vertex_properties[vp].name;
  int len = s.length() + 1;
  char* out = new char[len];
  snprintf(out, len, "%s", s.c_str());
  return out;
}

GRIN_VERTEX_PROPERTY grin_get_vertex_property_by_name(GRIN_GRAPH g,
                                                      GRIN_VERTEX_TYPE vtype,
                                                      const char* name) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto s = std::string(name);
  auto& name2id = _g->vertex_property_name_2_ids[vtype];
  if (name2id.find(s) == name2id.end())
    return GRIN_NULL_VERTEX_PROPERTY;
  else
    return name2id[s];
}

GRIN_VERTEX_PROPERTY_LIST grin_get_vertex_properties_by_name(GRIN_GRAPH g,
                                                             const char* name) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto s = std::string(name);
  auto vpl = new GRIN_VERTEX_PROPERTY_LIST_T();
  for (auto vtype = 0; vtype < _g->vertex_type_num; ++vtype) {
    auto& name2id = _g->vertex_property_name_2_ids[vtype];
    if (name2id.find(s) != name2id.end())
      vpl->push_back(name2id[s]);
  }
  if (vpl->size() == 0) {
    delete vpl;
    return GRIN_NULL_LIST;
  } else {
    return vpl;
  }
}
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY_NAME
const char* grin_get_edge_property_name(GRIN_GRAPH g, GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto& s = _g->edge_properties[ep].name;
  int len = s.length() + 1;
  char* out = new char[len];
  snprintf(out, len, "%s", s.c_str());
  return out;
}

GRIN_EDGE_PROPERTY grin_get_edge_property_by_name(GRIN_GRAPH g,
                                                  GRIN_EDGE_TYPE etype,
                                                  const char* name) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto s = std::string(name);
  auto& name2id = _g->edge_property_name_2_ids[etype];
  if (name2id.find(s) == name2id.end())
    return GRIN_NULL_EDGE_PROPERTY;
  else
    return name2id[s];
}

GRIN_EDGE_PROPERTY_LIST grin_get_edge_properties_by_name(GRIN_GRAPH g,
                                                         const char* name) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto s = std::string(name);
  auto epl = new GRIN_EDGE_PROPERTY_LIST_T();
  for (auto etype = 0; etype < _g->unique_edge_type_num; ++etype) {
    auto& name2id = _g->edge_property_name_2_ids[etype];
    if (name2id.find(s) != name2id.end())
      epl->push_back(name2id[s]);
  }
  if (epl->size() == 0) {
    delete epl;
    return GRIN_NULL_LIST;
  } else {
    return epl;
  }
}
#endif

#ifdef GRIN_WITH_VERTEX_PROPERTY
bool grin_equal_vertex_property(GRIN_GRAPH g, GRIN_VERTEX_PROPERTY vp1,
                                GRIN_VERTEX_PROPERTY vp2) {
  return vp1 == vp2;
}

void grin_destroy_vertex_property(GRIN_GRAPH g, GRIN_VERTEX_PROPERTY vp) {
  return;
}

GRIN_DATATYPE grin_get_vertex_property_data_type(GRIN_GRAPH g,
                                                 GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  return _g->vertex_properties[vp].type;
}

GRIN_VERTEX_TYPE grin_get_vertex_property_vertex_type(GRIN_GRAPH g,
                                                      GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  return _g->vertex_properties[vp].type_id;
}
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
bool grin_equal_edge_property(GRIN_GRAPH g, GRIN_EDGE_PROPERTY ep1,
                              GRIN_EDGE_PROPERTY ep2) {
  return ep1 == ep2;
}

void grin_destroy_edge_property(GRIN_GRAPH g, GRIN_EDGE_PROPERTY ep) { return; }

GRIN_DATATYPE grin_get_edge_property_data_type(GRIN_GRAPH g,
                                               GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  return _g->edge_properties[ep].type;
}

GRIN_EDGE_TYPE grin_get_edge_property_edge_type(GRIN_GRAPH g,
                                                GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  return _g->edge_properties[ep].type_id;
}
#endif
