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
extern "C" {
#include "common/error.h"
#include "property/row.h"
}

#define __grin_get_gar_vertex(_v)                           \
  if (_v->vertex.has_value() == false) {                    \
    auto& vertices = _g->vertices_collections[_v->type_id]; \
    auto it = vertices.begin() + _v->id;                    \
    _v->vertex = *it;                                       \
  }

#define __grin_check_row(_r, x)      \
  grin_error_code = NO_ERROR;        \
  if (idx >= _r->size()) {           \
    grin_error_code = INVALID_VALUE; \
    return x;                        \
  }

#ifdef GRIN_ENABLE_ROW
void grin_destroy_row(GRIN_GRAPH g, GRIN_ROW r) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  delete _r;
}

int grin_get_int32_from_row(GRIN_GRAPH g, GRIN_ROW r, size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  __grin_check_row(_r, 0);
  return std::any_cast<int32_t>((*_r)[idx]);
}

unsigned int grin_get_uint32_from_row(GRIN_GRAPH g, GRIN_ROW r, size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  __grin_check_row(_r, 0);
  return std::any_cast<uint32_t>((*_r)[idx]);
}

long long int grin_get_int64_from_row(GRIN_GRAPH g,  // NOLINT
                                      GRIN_ROW r, size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  __grin_check_row(_r, 0);
  return std::any_cast<int64_t>((*_r)[idx]);
}

unsigned long long int grin_get_uint64_from_row(GRIN_GRAPH g,  // NOLINT
                                                GRIN_ROW r, size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  __grin_check_row(_r, 0);
  return std::any_cast<uint64_t>((*_r)[idx]);
}

float grin_get_float_from_row(GRIN_GRAPH g, GRIN_ROW r, size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  __grin_check_row(_r, 0);
  return std::any_cast<float>((*_r)[idx]);
}

double grin_get_double_from_row(GRIN_GRAPH g, GRIN_ROW r, size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  __grin_check_row(_r, 0);
  return std::any_cast<double>((*_r)[idx]);
}

const char* grin_get_string_from_row(GRIN_GRAPH g, GRIN_ROW r, size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  __grin_check_row(_r, NULL);
  return std::any_cast<const std::string&>((*_r)[idx]).c_str();
}

int grin_get_date32_from_row(GRIN_GRAPH g, GRIN_ROW r, size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  __grin_check_row(_r, 0);
  return std::any_cast<int32_t>((*_r)[idx]);
}

int grin_get_time32_from_row(GRIN_GRAPH g, GRIN_ROW r, size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  __grin_check_row(_r, 0);
  return std::any_cast<int32_t>((*_r)[idx]);
}

long long int grin_get_timestamp64_from_row(GRIN_GRAPH g, GRIN_ROW r,  // NOLINT
                                            size_t idx) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  __grin_check_row(_r, 0);
  return std::any_cast<int64_t>((*_r)[idx]);
}

GRIN_ROW grin_create_row(GRIN_GRAPH g) {
  auto r = new GRIN_ROW_T();
  return r;
}

bool grin_insert_int32_to_row(GRIN_GRAPH g, GRIN_ROW r, int value) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(value);
  return true;
}

bool grin_insert_uint32_to_row(GRIN_GRAPH g, GRIN_ROW r, unsigned int value) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(value);
  return true;
}

bool grin_insert_int64_to_row(GRIN_GRAPH g, GRIN_ROW r,
                              long long int value) {  // NOLINT
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(value);
  return true;
}

bool grin_insert_uint64_to_row(GRIN_GRAPH g, GRIN_ROW r,
                               unsigned long long int value) {  // NOLINT
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(value);
  return true;
}

bool grin_insert_float_to_row(GRIN_GRAPH g, GRIN_ROW r, float value) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(value);
  return true;
}

bool grin_insert_double_to_row(GRIN_GRAPH g, GRIN_ROW r, double value) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(value);
  return true;
}

bool grin_insert_string_to_row(GRIN_GRAPH g, GRIN_ROW r, const char* value) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(std::string(value));
  return true;
}

bool grin_insert_date32_to_row(GRIN_GRAPH g, GRIN_ROW r, int value) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(value);
  return true;
}

bool grin_insert_time32_to_row(GRIN_GRAPH g, GRIN_ROW r, int value) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(value);
  return true;
}

bool grin_insert_timestamp64_to_row(GRIN_GRAPH g, GRIN_ROW r,
                                    long long int value) {  // NOLINT
  auto _r = static_cast<GRIN_ROW_T*>(r);
  _r->push_back(value);
  return true;
}
#endif

#if defined(GRIN_ENABLE_ROW) && defined(GRIN_TRAIT_CONST_VALUE_PTR)
const void* grin_get_value_from_row(GRIN_GRAPH, GRIN_ROW, GRIN_DATATYPE,
                                    size_t);
#endif

#if defined(GRIN_WITH_VERTEX_PROPERTY) && defined(GRIN_ENABLE_ROW)
GRIN_ROW grin_get_vertex_row(GRIN_GRAPH g, GRIN_VERTEX v) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  __grin_get_gar_vertex(_v);

  auto r = new GRIN_ROW_T();
  for (auto vp = _g->vertex_property_offsets[_v->type_id];
       vp < _g->vertex_property_offsets[_v->type_id + 1]; ++vp) {
    auto& property = _g->vertex_properties[vp];

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

#if defined(GRIN_WITH_EDGE_PROPERTY) && defined(GRIN_ENABLE_ROW)
GRIN_ROW grin_get_edge_row(GRIN_GRAPH g, GRIN_EDGE e) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto etype = _g->unique_edge_type_ids[_e->type_id];

  auto r = new GRIN_ROW_T();
  for (auto ep = _g->edge_property_offsets[etype];
       ep < _g->edge_property_offsets[etype + 1]; ++ep) {
    auto& property = _g->edge_properties[ep];

    switch (property.type) {
    case GRIN_DATATYPE::Int32: {
      auto value = _e->edge.property<int32_t>(property.name).value();
      r->push_back(value);
      break;
    }
    case GRIN_DATATYPE::Int64: {
      auto value = _e->edge.property<int64_t>(property.name).value();
      r->push_back(value);
      break;
    }
    case GRIN_DATATYPE::Float: {
      auto value = _e->edge.property<float>(property.name).value();
      r->push_back(value);
      break;
    }
    case GRIN_DATATYPE::Double: {
      auto value = _e->edge.property<double>(property.name).value();
      r->push_back(value);
      break;
    }
    case GRIN_DATATYPE::String: {
      auto value = _e->edge.property<std::string>(property.name).value();
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
