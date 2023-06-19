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

#include "grin/src/predefine.h"
// GRIN headers
#include "property/type.h"

#ifdef GRIN_WITH_VERTEX_PROPERTY
bool grin_equal_vertex_type(GRIN_GRAPH g, GRIN_VERTEX_TYPE vt1,
                            GRIN_VERTEX_TYPE vt2) {
  return vt1 == vt2;
}

GRIN_VERTEX_TYPE grin_get_vertex_type(GRIN_GRAPH g, GRIN_VERTEX v) {
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  return _v->type_id;
}

void grin_destroy_vertex_type(GRIN_GRAPH g, GRIN_VERTEX_TYPE vt) { return; }

GRIN_VERTEX_TYPE_LIST grin_get_vertex_type_list(GRIN_GRAPH g) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto vtl = new GRIN_VERTEX_TYPE_LIST_T();
  for (auto i = 0; i < _g->vertex_type_num; ++i) {
    vtl->push_back(i);
  }
  return vtl;
}

void grin_destroy_vertex_type_list(GRIN_GRAPH g, GRIN_VERTEX_TYPE_LIST vtl) {
  auto _vtl = static_cast<GRIN_VERTEX_TYPE_LIST*>(vtl);
  delete _vtl;
}

GRIN_VERTEX_TYPE_LIST grin_create_vertex_type_list(GRIN_GRAPH g) {
  auto vtl = new GRIN_VERTEX_TYPE_LIST_T();
  return vtl;
}

bool grin_insert_vertex_type_to_list(GRIN_GRAPH g, GRIN_VERTEX_TYPE_LIST vtl,
                                     GRIN_VERTEX_TYPE vt) {
  auto _vtl = static_cast<GRIN_VERTEX_TYPE_LIST_T*>(vtl);
  _vtl->push_back(vt);
  return true;
}

size_t grin_get_vertex_type_list_size(GRIN_GRAPH g, GRIN_VERTEX_TYPE_LIST vtl) {
  auto _vtl = static_cast<GRIN_VERTEX_TYPE_LIST_T*>(vtl);
  return _vtl->size();
}

GRIN_VERTEX_TYPE grin_get_vertex_type_from_list(GRIN_GRAPH g,
                                                GRIN_VERTEX_TYPE_LIST vtl,
                                                size_t idx) {
  auto _vtl = static_cast<GRIN_VERTEX_TYPE_LIST_T*>(vtl);
  return (*_vtl)[idx];
}
#endif

#ifdef GRIN_WITH_VERTEX_TYPE_NAME
const char* grin_get_vertex_type_name(GRIN_GRAPH g, GRIN_VERTEX_TYPE vt) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto& s = _g->vertex_types[vt];
  return s.c_str();
}

GRIN_VERTEX_TYPE grin_get_vertex_type_by_name(GRIN_GRAPH g, const char* name) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto s = std::string(name);
  auto it = std::find(_g->vertex_types.begin(), _g->vertex_types.end(), s);
  if (it == _g->vertex_types.end())
    return GRIN_NULL_VERTEX_TYPE;
  else
    return it - _g->vertex_types.begin();
}
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_TYPE
GRIN_VERTEX_TYPE_ID grin_get_vertex_type_id(GRIN_GRAPH g, GRIN_VERTEX_TYPE vt) {
  return vt;
}

GRIN_VERTEX_TYPE grin_get_vertex_type_by_id(GRIN_GRAPH g,
                                            GRIN_VERTEX_TYPE_ID vti) {
  return vti;
}
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
bool grin_equal_edge_type(GRIN_GRAPH g, GRIN_EDGE_TYPE et1,
                          GRIN_EDGE_TYPE et2) {
  return (et1 == et2);
}

GRIN_EDGE_TYPE grin_get_edge_type(GRIN_GRAPH g, GRIN_EDGE e) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  return _g->unique_edge_type_ids[_e->type_id];
}

void grin_destroy_edge_type(GRIN_GRAPH g, GRIN_EDGE_TYPE et) { return; }

GRIN_EDGE_TYPE_LIST grin_get_edge_type_list(GRIN_GRAPH g) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto etl = new GRIN_EDGE_TYPE_LIST_T();
  for (auto i = 0; i < _g->unique_edge_type_num; ++i) {
    etl->push_back(i);
  }
  return etl;
}

void grin_destroy_edge_type_list(GRIN_GRAPH g, GRIN_EDGE_TYPE_LIST etl) {
  auto _etl = static_cast<GRIN_EDGE_TYPE_LIST_T*>(etl);
  delete _etl;
}

GRIN_EDGE_TYPE_LIST grin_create_edge_type_list(GRIN_GRAPH g) {
  auto etl = new GRIN_EDGE_TYPE_LIST_T();
  return etl;
}

bool grin_insert_edge_type_to_list(GRIN_GRAPH g, GRIN_EDGE_TYPE_LIST etl,
                                   GRIN_EDGE_TYPE et) {
  auto _etl = static_cast<GRIN_EDGE_TYPE_LIST_T*>(etl);
  _etl->push_back(et);
  return true;
}

size_t grin_get_edge_type_list_size(GRIN_GRAPH g, GRIN_EDGE_TYPE_LIST etl) {
  auto _etl = static_cast<GRIN_EDGE_TYPE_LIST_T*>(etl);
  return _etl->size();
}

GRIN_EDGE_TYPE grin_get_edge_type_from_list(GRIN_GRAPH g,
                                            GRIN_EDGE_TYPE_LIST etl,
                                            size_t idx) {
  auto _etl = static_cast<GRIN_EDGE_TYPE_LIST_T*>(etl);
  return (*_etl)[idx];
}
#endif

#ifdef GRIN_WITH_EDGE_TYPE_NAME
const char* grin_get_edge_type_name(GRIN_GRAPH g, GRIN_EDGE_TYPE et) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto& s = _g->unique_edge_types[et];
  return s.c_str();
}

GRIN_EDGE_TYPE grin_get_edge_type_by_name(GRIN_GRAPH g, const char* name) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto s = std::string(name);
  if (_g->unique_edge_type_2_ids.find(s) == _g->unique_edge_type_2_ids.end())
    return GRIN_NULL_EDGE_TYPE;
  return _g->unique_edge_type_2_ids.at(s);
}
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_EDGE_TYPE
GRIN_EDGE_TYPE_ID grin_get_edge_type_id(GRIN_GRAPH g, GRIN_EDGE_TYPE et) {
  return et;
}

GRIN_EDGE_TYPE grin_get_edge_type_by_id(GRIN_GRAPH g, GRIN_EDGE_TYPE_ID eti) {
  return eti;
}
#endif

#if defined(GRIN_WITH_VERTEX_PROPERTY) && defined(GRIN_WITH_EDGE_PROPERTY)
GRIN_VERTEX_TYPE_LIST grin_get_src_types_by_edge_type(GRIN_GRAPH g,
                                                      GRIN_EDGE_TYPE et) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  if (et >= _g->unique_edge_type_num)
    return GRIN_NULL_VERTEX_TYPE_LIST;
  auto vtl = new GRIN_VERTEX_TYPE_LIST_T();
  for (auto i = _g->unique_edge_type_begin_type[et];
       i < _g->unique_edge_type_begin_type[et + 1]; i++) {
    vtl->push_back(_g->src_type_ids[i]);
  }
  return vtl;
}

GRIN_VERTEX_TYPE_LIST grin_get_dst_types_by_edge_type(GRIN_GRAPH g,
                                                      GRIN_EDGE_TYPE et) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  if (et >= _g->unique_edge_type_num)
    return GRIN_NULL_VERTEX_TYPE_LIST;
  auto vtl = new GRIN_VERTEX_TYPE_LIST_T();
  for (auto i = _g->unique_edge_type_begin_type[et];
       i < _g->unique_edge_type_begin_type[et + 1]; i++) {
    vtl->push_back(_g->dst_type_ids[i]);
  }
  return vtl;
}

GRIN_EDGE_TYPE_LIST grin_get_edge_types_by_vertex_type_pair(
    GRIN_GRAPH g, GRIN_VERTEX_TYPE src_vt, GRIN_VERTEX_TYPE dst_vt) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto etl = new GRIN_EDGE_TYPE_LIST_T();
  for (auto i = 0; i < _g->edge_type_num; i++) {
    if (_g->src_type_ids[i] == src_vt && _g->dst_type_ids[i] == dst_vt) {
      etl->push_back(_g->unique_edge_type_ids[i]);
    }
  }
  return etl;
}
#endif
