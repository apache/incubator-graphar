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
#include "common/error.h"
#include "property/property.h"
#include "property/value.h"

#define __grin_get_gar_vertex(_v)                           \
  if (_v->vertex.has_value() == false) {                    \
    auto& vertices = _g->vertices_collections[_v->type_id]; \
    auto it = vertices.begin() + _v->id;                    \
    _v->vertex = *it;                                       \
  }

#define __grin_check_vertex_property(_v, x) \
  grin_error_code = NO_ERROR;               \
  if (_v->type_id != property.type_id) {    \
    grin_error_code = INVALID_VALUE;        \
    return x;                               \
  }

#define __grin_check_edge_property(_e, x)                          \
  grin_error_code = NO_ERROR;                                      \
  if (_e->type_id >= _g->edge_type_num ||                          \
      _g->unique_edge_type_ids[_e->type_id] != property.type_id) { \
    grin_error_code = INVALID_VALUE;                               \
    return x;                                                      \
  }

#if defined(GRIN_WITH_VERTEX_PROPERTY) && defined(GRIN_TRAIT_PROPERTY_VALUE_OF_FLOAT_ARRAY)
// TODO
void grin_destroy_vertex_property_value_of_float_array(GRIN_GRAPH g, const float* value, size_t size) {
  delete[] value;
  return;
}

const float* grin_get_vertex_property_value_of_float_array(GRIN_GRAPH g, GRIN_VERTEX v, GRIN_VERTEX_PROPERTY vp, size_t* size_ptr) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, NULL);
  __grin_get_gar_vertex(_v);
  auto vtype = _v->type_id;
  auto size = _g->vertex_property_offsets[vtype + 1] -
              _g->vertex_property_offsets[vtype];
  *size_ptr = size;
  auto value = new float[size];
  for (auto i = 0; i < size; ++i) {
    auto vp_handle = _g->vertex_property_offsets[vtype] + i;
    auto& property = _g->vertex_properties[vp_handle];
    try {  // try float
      value[i] = _v->vertex.value().property<float>(property.name).value();
    } catch (std::exception& e) {
      try {  // try double
        value[i] = _v->vertex.value().property<double>(property.name).value();
      } catch (std::exception& e) {
        try {  // try int64_t
          value[i] = _v->vertex.value().property<int64_t>(property.name).value();
        } catch (std::exception& e) {
          delete[] value;
          return NULL;
        }
      }
    }
  }
  return value;
}
#endif

#ifdef GRIN_WITH_VERTEX_PROPERTY
void grin_destroy_vertex_property_value_of_string(GRIN_GRAPH g, const char* value) {
  return;
}

int grin_get_vertex_property_value_of_int32(GRIN_GRAPH g, GRIN_VERTEX v,
                                            GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, 0);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value().property<int32_t>(property.name).value();
}

unsigned int grin_get_vertex_property_value_of_uint32(GRIN_GRAPH g,
                                                      GRIN_VERTEX v,
                                                      GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, 0);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value().property<uint32_t>(property.name).value();
}

long long int grin_get_vertex_property_value_of_int64(GRIN_GRAPH g,  // NOLINT
                                                      GRIN_VERTEX v,
                                                      GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, 0);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value().property<int64_t>(property.name).value();
}

unsigned long long int grin_get_vertex_property_value_of_uint64(  // NOLINT
    GRIN_GRAPH g, GRIN_VERTEX v, GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, 0);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value().property<uint64_t>(property.name).value();
}

float grin_get_vertex_property_value_of_float(GRIN_GRAPH g, GRIN_VERTEX v,
                                              GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, 0);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value().property<float>(property.name).value();
}

double grin_get_vertex_property_value_of_double(GRIN_GRAPH g, GRIN_VERTEX v,
                                                GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, 0);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value().property<double>(property.name).value();
}

const char* grin_get_vertex_property_value_of_string(GRIN_GRAPH g,
                                                     GRIN_VERTEX v,
                                                     GRIN_VERTEX_PROPERTY vp) {
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

int grin_get_vertex_property_value_of_date32(GRIN_GRAPH g, GRIN_VERTEX v,
                                             GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, 0);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value().property<int32_t>(property.name).value();
}

int grin_get_vertex_property_value_of_time32(GRIN_GRAPH g, GRIN_VERTEX v,
                                             GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, 0);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value().property<int32_t>(property.name).value();
}

long long int grin_get_vertex_property_value_of_timestamp64(  // NOLINT
    GRIN_GRAPH g, GRIN_VERTEX v, GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto& property = _g->vertex_properties[vp];
  __grin_check_vertex_property(_v, 0);
  __grin_get_gar_vertex(_v);
  return _v->vertex.value().property<int64_t>(property.name).value();
}
#endif

#if defined(GRIN_WITH_VERTEX_PROPERTY) && defined(GRIN_TRAIT_CONST_VALUE_PTR)
const void* grin_get_vertex_property_value(GRIN_GRAPH, GRIN_VERTEX, GRIN_VERTEX_PROPERTY);
#endif

#if defined(GRIN_WITH_EDGE_PROPERTY) && defined(GRIN_TRAIT_PROPERTY_VALUE_OF_FLOAT_ARRAY)
void grin_destroy_edge_property_value_of_float_array(GRIN_GRAPH g, const float* value, size_t size) {
  delete [] value;
  return;
}

const float* grin_get_edge_property_value_of_float_array(GRIN_GRAPH g, GRIN_EDGE e, GRIN_EDGE_PROPERTY ep, size_t* size_ptr) {
auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, NULL);
  auto etype = grin_get_edge_type_from_property(g, ep);
  auto size =
      _g->edge_property_offsets[etype + 1] - _g->edge_property_offsets[etype];
  *size_ptr = size;
  float* value = new float[size];
  for (auto i = 0; i < size; ++i) {
    auto ep_handle = _g->edge_property_offsets[etype] + i;
    auto& property = _g->edge_properties[ep_handle];
    try {  // try float
      value[i] = _e->edge.property<float>(property.name).value();
    } catch (std::exception& e) {
      try {  // try double
        value[i] = _e->edge.property<double>(property.name).value();
      } catch (std::exception& e) {
        try {  // try int64_t
          value[i] = _e->edge.property<int64_t>(property.name).value();
        } catch (std::exception& e) {
          delete[] value;
          return NULL;
        }
      }
    }
  }
  return value;
}
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
// TODO: check
void grin_destroy_edge_property_value_of_string(GRIN_GRAPH g, const char* value) {
  return;
}

int grin_get_edge_property_value_of_int32(GRIN_GRAPH g, GRIN_EDGE e,
                                          GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, 0);
  return _e->edge.property<int32_t>(property.name).value();
}

unsigned int grin_get_edge_property_value_of_uint32(GRIN_GRAPH g, GRIN_EDGE e,
                                                    GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, 0);
  return _e->edge.property<uint32_t>(property.name).value();
}

long long int grin_get_edge_property_value_of_int64(GRIN_GRAPH g,  // NOLINT
                                                    GRIN_EDGE e,
                                                    GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, 0);
  return _e->edge.property<int64_t>(property.name).value();
}

unsigned long long int grin_get_edge_property_value_of_uint64(  // NOLINT
    GRIN_GRAPH g, GRIN_EDGE e, GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, 0);
  return _e->edge.property<uint64_t>(property.name).value();
}

float grin_get_edge_property_value_of_float(GRIN_GRAPH g, GRIN_EDGE e,
                                            GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, 0);
  return _e->edge.property<float>(property.name).value();
}

double grin_get_edge_property_value_of_double(GRIN_GRAPH g, GRIN_EDGE e,
                                              GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, 0);
  return _e->edge.property<double>(property.name).value();
}

const char* grin_get_edge_property_value_of_string(GRIN_GRAPH g, GRIN_EDGE e,
                                                   GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, NULL);
  return _e->edge.property<const std::string&>(property.name).value().c_str();
}

int grin_get_edge_property_value_of_date32(GRIN_GRAPH g, GRIN_EDGE e,
                                           GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, 0);
  return _e->edge.property<int32_t>(property.name).value();
}

int grin_get_edge_property_value_of_time32(GRIN_GRAPH g, GRIN_EDGE e,
                                           GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, 0);
  return _e->edge.property<int32_t>(property.name).value();
}

long long int grin_get_edge_property_value_of_timestamp64(  // NOLINT
    GRIN_GRAPH g, GRIN_EDGE e, GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto& property = _g->edge_properties[ep];
  __grin_check_edge_property(_e, 0);
  return _e->edge.property<int64_t>(property.name).value();
}

#endif

#if defined(GRIN_WITH_EDGE_PROPERTY) && defined(GRIN_TRAIT_CONST_VALUE_PTR)
const void* grin_get_edge_property_value(GRIN_GRAPH, GRIN_EDGE, GRIN_EDGE_PROPERTY);
#endif