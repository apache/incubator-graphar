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

extern "C" {
#include "grin/include/common/error.h"
#include "grin/include/property/property.h"
}
#include "grin/src/predefine.h"

#define __grin_get_gar_vertex(_v)                           \
  if (_v->vertex.has_value() == false) {                    \
    auto& vertices = _g->vertices_collections[_v->type_id]; \
    auto it = vertices.begin() + _v->id;                    \
    _v->vertex = *it;                                       \
  }

#define __grin_check_vertex_property(_v, x)            \
  grin_error_code = NO_ERROR;                          \
  if (_v->type_id != property.type_id) {               \
    grin_error_code = INVALID_VALUE;                   \
    return x;                                          \
  }

#define __grin_check_edge_property(_e, x)                          \
  grin_error_code = NO_ERROR;                                      \
  if (_e->type_id >= _g->edge_type_num ||                          \
      _g->unique_edge_type_ids[_e->type_id] != property.type_id) { \
    grin_error_code = INVALID_VALUE;                               \
    return x;                                                      \
  }

void grin_destroy_string_value(GRIN_GRAPH g, const char* value) {
  return;
}

#ifdef GRIN_WITH_VERTEX_PROPERTY_NAME
const char* grin_get_vertex_property_name(GRIN_GRAPH g, GRIN_VERTEX_TYPE vtype,
                                          GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto& s = _g->vertex_properties[vp].name;
  return s.c_str();
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
const char* grin_get_edge_property_name(GRIN_GRAPH g, GRIN_EDGE_TYPE etype,
                                        GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto& s = _g->edge_properties[ep].name;
  return s.c_str();
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

GRIN_DATATYPE grin_get_vertex_property_datatype(GRIN_GRAPH g,
                                                GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  return _g->vertex_properties[vp].type;
}

int grin_get_vertex_property_value_of_int32(GRIN_GRAPH g, GRIN_VERTEX v, GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, 0);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value().property<int32_t>(property.name).value();
}

unsigned int grin_get_vertex_property_value_of_uint32(GRIN_GRAPH g, GRIN_VERTEX v, GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, 0);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value().property<uint32_t>(property.name).value();
}

long long int grin_get_vertex_property_value_of_int64(GRIN_GRAPH g, // NOLINT
GRIN_VERTEX v, 
GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, 0);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value().property<int64_t>(property.name).value();
}

unsigned long long int grin_get_vertex_property_value_of_uint64(GRIN_GRAPH g, // NOLINT
GRIN_VERTEX v, GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, 0);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value().property<uint64_t>(property.name).value();
}

float grin_get_vertex_property_value_of_float(GRIN_GRAPH g, GRIN_VERTEX v, GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, 0);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value().property<float>(property.name).value();
}

double grin_get_vertex_property_value_of_double(GRIN_GRAPH g, GRIN_VERTEX v, GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, 0);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value().property<double>(property.name).value();
}

const char* grin_get_vertex_property_value_of_string(GRIN_GRAPH g, GRIN_VERTEX v, GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, NULL);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value()
      .property<const std::string&>(property.name)
      .value()
      .c_str();
}

int grin_get_vertex_property_value_of_date32(GRIN_GRAPH g, GRIN_VERTEX v, GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, 0);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value().property<int32_t>(property.name).value();
}

int grin_get_vertex_property_value_of_time32(GRIN_GRAPH g, GRIN_VERTEX v, GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, 0);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value().property<int32_t>(property.name).value();
}

long long int grin_get_vertex_property_value_of_timestamp64(GRIN_GRAPH g, // NOLINT
GRIN_VERTEX v, GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, 0);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value().property<int64_t>(property.name).value();
}

GRIN_VERTEX_TYPE grin_get_vertex_type_from_property(GRIN_GRAPH g,
                                                    GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  return _g->vertex_properties[vp].type_id;
}
#endif

#if defined(GRIN_WITH_VERTEX_PROPERTY) && defined(GRIN_TRAIT_CONST_VALUE_PTR)
const void* grin_get_vertex_property_value(GRIN_GRAPH, GRIN_VERTEX, GRIN_VERTEX_PROPERTY);
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
bool grin_equal_edge_property(GRIN_GRAPH g, GRIN_EDGE_PROPERTY ep1,
                              GRIN_EDGE_PROPERTY ep2) {
  return ep1 == ep2;
}

void grin_destroy_edge_property(GRIN_GRAPH g, GRIN_EDGE_PROPERTY ep) { return; }

GRIN_DATATYPE grin_get_edge_property_datatype(GRIN_GRAPH g,
                                              GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  return _g->edge_properties[ep].type;
}

int grin_get_edge_property_value_of_int32(GRIN_GRAPH g, GRIN_EDGE e, GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, 0);
  return _e->edge.property<int32_t>(property.name).value();
}

unsigned int grin_get_edge_property_value_of_uint32(GRIN_GRAPH g, GRIN_EDGE e, GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, 0);
  return _e->edge.property<uint32_t>(property.name).value();
}

long long int grin_get_edge_property_value_of_int64(GRIN_GRAPH g, // NOLINT
GRIN_EDGE e, GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, 0);
  return _e->edge.property<int64_t>(property.name).value();
}

unsigned long long int grin_get_edge_property_value_of_uint64(GRIN_GRAPH g, // NOLINT
 GRIN_EDGE e, GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, 0);
  return _e->edge.property<uint64_t>(property.name).value();
}

float grin_get_edge_property_value_of_float(GRIN_GRAPH g, GRIN_EDGE e, GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, 0);
  return _e->edge.property<float>(property.name).value();
}

double grin_get_edge_property_value_of_double(GRIN_GRAPH g, GRIN_EDGE e, GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, 0);
  return _e->edge.property<double>(property.name).value();
}

const char* grin_get_edge_property_value_of_string(GRIN_GRAPH g, GRIN_EDGE e, GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, NULL);
  return _e->edge.property<const std::string&>(property.name).value().c_str();
}

int grin_get_edge_property_value_of_date32(GRIN_GRAPH g, GRIN_EDGE e, GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, 0);
  return _e->edge.property<int32_t>(property.name).value();
}

int grin_get_edge_property_value_of_time32(GRIN_GRAPH g, GRIN_EDGE e, GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, 0);
  return _e->edge.property<int32_t>(property.name).value();
}

long long int grin_get_edge_property_value_of_timestamp64(GRIN_GRAPH g, // NOLINT
GRIN_EDGE e, GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, 0);
  return _e->edge.property<int64_t>(property.name).value();
}

GRIN_EDGE_TYPE grin_get_edge_type_from_property(GRIN_GRAPH g,
                                                GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  return _g->edge_properties[ep].type_id;
}
#endif

#if defined(GRIN_WITH_EDGE_PROPERTY) && defined(GRIN_TRAIT_CONST_VALUE_PTR)
const void* grin_get_edge_property_value(GRIN_GRAPH, GRIN_EDGE, GRIN_EDGE_PROPERTY);
#endif
