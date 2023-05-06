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
#include "grin/include/property/propertytable.h"
}
#include <iostream>
#include "grin/src/predefine.h"

#ifdef GRIN_ENABLE_ROW
void grin_destroy_row(GRIN_GRAPH g, GRIN_ROW r) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  delete _r;
}

const void* grin_get_value_from_row(GRIN_GRAPH g, GRIN_ROW r,
                                    GRIN_DATATYPE type, size_t idx) {
  if (r == GRIN_NULL_ROW)
    return NULL;
  auto _r = static_cast<GRIN_ROW_T*>(r);
  switch (type) {
  case GRIN_DATATYPE::Int32:
    return new int32_t(std::any_cast<int32_t>((*_r)[idx]));
  case GRIN_DATATYPE::UInt32:
    return new uint32_t(std::any_cast<uint32_t>((*_r)[idx]));
  case GRIN_DATATYPE::Int64:
    return new int64_t(std::any_cast<int64_t>((*_r)[idx]));
  case GRIN_DATATYPE::UInt64:
    return new uint64_t(std::any_cast<uint64_t>((*_r)[idx]));
  case GRIN_DATATYPE::Float:
    return new float(std::any_cast<float>((*_r)[idx]));
  case GRIN_DATATYPE::Double:
    return new double(std::any_cast<double>((*_r)[idx]));
  case GRIN_DATATYPE::String: {
    auto&& s = std::any_cast<std::string>((*_r)[idx]);
    int len = s.length() + 1;
    char* out = new char[len];
    snprintf(out, len, "%s", s.c_str());
    return out;
  }
  case GRIN_DATATYPE::Date32:
    return new int32_t(std::any_cast<int32_t>((*_r)[idx]));
  case GRIN_DATATYPE::Date64:
    return new int64_t(std::any_cast<int64_t>((*_r)[idx]));
  default:
    return NULL;
  }
  return NULL;
}

GRIN_ROW grin_create_row(GRIN_GRAPH g) {
  auto r = new GRIN_ROW_T();
  return r;
}

bool grin_insert_value_to_row(GRIN_GRAPH g, GRIN_ROW r, GRIN_DATATYPE type,
                              const void* value) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  switch (type) {
  case GRIN_DATATYPE::Int32:
    _r->push_back(*static_cast<const int32_t*>(value));
    return true;
  case GRIN_DATATYPE::UInt32:
    _r->push_back(*static_cast<const uint32_t*>(value));
    return true;
  case GRIN_DATATYPE::Int64:
    _r->push_back(*static_cast<const int64_t*>(value));
    return true;
  case GRIN_DATATYPE::UInt64:
    _r->push_back(*static_cast<const uint64_t*>(value));
    return true;
  case GRIN_DATATYPE::Float:
    _r->push_back(*static_cast<const float*>(value));
    return true;
  case GRIN_DATATYPE::Double:
    _r->push_back(*static_cast<const double*>(value));
    return true;
  case GRIN_DATATYPE::String:
    _r->push_back(std::string(static_cast<const char*>(value)));
    return true;
  case GRIN_DATATYPE::Date32:
    _r->push_back(*static_cast<const int32_t*>(value));
    return true;
  case GRIN_DATATYPE::Date64:
    _r->push_back(*static_cast<const int64_t*>(value));
    return true;
  default:
    return false;
  }
  return false;
}
#endif

#ifdef GRIN_ENABLE_VERTEX_PROPERTY_TABLE
void grin_destroy_vertex_property_table(GRIN_GRAPH g,
                                        GRIN_VERTEX_PROPERTY_TABLE vpt) {
  return;
}

GRIN_VERTEX_PROPERTY_TABLE grin_get_vertex_property_table_by_type(
    GRIN_GRAPH g, GRIN_VERTEX_TYPE vtype) {
  return vtype;
}

const void* grin_get_value_from_vertex_property_table(
    GRIN_GRAPH g, GRIN_VERTEX_PROPERTY_TABLE vpt, GRIN_VERTEX v,
    GRIN_VERTEX_PROPERTY vp) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto _vp = static_cast<GRIN_VERTEX_PROPERTY_T*>(vp);
  if (_vp->type_id != vpt || _v->type_id != vpt)
    return NULL;

  // properties are not stored in vertex
  if (_v->vertex.has_value() == false) {
    auto& vertices = _g->vertices_collections[_vp->type_id];
    auto it = vertices.begin() + _v->id;
    _v->vertex = *it;
  }

  // properties are stored in vertex
  switch (_vp->type) {
  case GRIN_DATATYPE::Int32: {
    auto value =
        new int32_t(_v->vertex.value().property<int32_t>(_vp->name).value());
    return value;
  }
  case GRIN_DATATYPE::Int64: {
    auto value =
        new int64_t(_v->vertex.value().property<int64_t>(_vp->name).value());
    return value;
  }
  case GRIN_DATATYPE::Float: {
    auto value =
        new float(_v->vertex.value().property<float>(_vp->name).value());
    return value;
  }
  case GRIN_DATATYPE::Double: {
    auto value =
        new double(_v->vertex.value().property<double>(_vp->name).value());
    return value;
  }
  case GRIN_DATATYPE::String: {
    auto s = _v->vertex.value().property<std::string>(_vp->name).value();
    int len = s.length() + 1;
    char* out = new char[len];
    snprintf(out, len, "%s", s.c_str());
    return out;
  }
  default:
    return NULL;
  }

  return NULL;
}

#endif

#if defined(GRIN_ENABLE_VERTEX_PROPERTY_TABLE) && defined(GRIN_ENABLE_ROW)
GRIN_ROW grin_get_row_from_vertex_property_table(
    GRIN_GRAPH g, GRIN_VERTEX_PROPERTY_TABLE vpt, GRIN_VERTEX v,
    GRIN_VERTEX_PROPERTY_LIST vpl) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto _vpl = static_cast<GRIN_VERTEX_PROPERTY_LIST_T*>(vpl);
  if (_v->type_id != vpt)
    return GRIN_NULL_ROW;
  auto r = new GRIN_ROW_T();

  // properties are not stored in vertex
  if (_v->vertex.has_value() == false) {
    auto& vertices = _g->vertices_collections[_v->type_id];
    auto it = vertices.begin() + _v->id;
    _v->vertex = *it;
  }

  // properties are stored in vertex
  for (auto& _vp : *_vpl) {
    if (_vp.type_id != _v->type_id) {
      delete r;
      return GRIN_NULL_ROW;
    }

    switch (_vp.type) {
    case GRIN_DATATYPE::Int32: {
      auto value = _v->vertex.value().property<int32_t>(_vp.name).value();
      r->push_back(value);
      break;
    }
    case GRIN_DATATYPE::Int64: {
      auto value = _v->vertex.value().property<int64_t>(_vp.name).value();
      r->push_back(value);
      break;
    }
    case GRIN_DATATYPE::Float: {
      auto value = _v->vertex.value().property<float>(_vp.name).value();
      r->push_back(value);
      break;
    }
    case GRIN_DATATYPE::Double: {
      auto value = _v->vertex.value().property<double>(_vp.name).value();
      r->push_back(value);
      break;
    }
    case GRIN_DATATYPE::String: {
      auto value = _v->vertex.value().property<std::string>(_vp.name).value();
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

#if !defined(GRIN_ASSUME_COLUMN_STORE_FOR_VERTEX_PROPERTY) && \
    defined(GRIN_ENABLE_ROW)
GRIN_ROW grin_get_vertex_row(GRIN_GRAPH, GRIN_VERTEX,
                             GRIN_VERTEX_PROPERTY_LIST);
#endif

#ifdef GRIN_ENABLE_EDGE_PROPERTY_TABLE
void grin_destroy_edge_property_table(GRIN_GRAPH g,
                                      GRIN_EDGE_PROPERTY_TABLE ept) {
  return;
}

GRIN_EDGE_PROPERTY_TABLE grin_get_edge_property_table_by_type(
    GRIN_GRAPH g, GRIN_EDGE_TYPE etype) {
  return etype;
}

const void* grin_get_value_from_edge_property_table(
    GRIN_GRAPH g, GRIN_EDGE_PROPERTY_TABLE ept, GRIN_EDGE e,
    GRIN_EDGE_PROPERTY ep) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto _ep = static_cast<GRIN_EDGE_PROPERTY_T*>(ep);
  if (_g->unique_edge_type_ids[_e->type_id] != ept)
    return NULL;
  if (_g->unique_edge_type_ids[_e->type_id] != _ep->type_id)
    return NULL;
  switch (_ep->type) {
  case GRIN_DATATYPE::Int32: {
    auto value = new int32_t(_e->edge.property<int32_t>(_ep->name).value());
    return value;
  }
  case GRIN_DATATYPE::Int64: {
    auto value = new int64_t(_e->edge.property<int64_t>(_ep->name).value());
    return value;
  }
  case GRIN_DATATYPE::Float: {
    auto value = new float(_e->edge.property<float>(_ep->name).value());
    return value;
  }
  case GRIN_DATATYPE::Double: {
    auto value = new double(_e->edge.property<double>(_ep->name).value());
    return value;
  }
  case GRIN_DATATYPE::String: {
    auto s = _e->edge.property<std::string>(_ep->name).value();
    int len = s.length() + 1;
    char* out = new char[len];
    snprintf(out, len, "%s", s.c_str());
    return out;
  }
  default:
    return NULL;
  }
  return NULL;
}

#endif

#if defined(GRIN_ENABLE_EDGE_PROPERTY_TABLE) && defined(GRIN_ENABLE_ROW)
GRIN_ROW grin_get_row_from_edge_property_table(GRIN_GRAPH g,
                                               GRIN_EDGE_PROPERTY_TABLE ept,
                                               GRIN_EDGE e,
                                               GRIN_EDGE_PROPERTY_LIST epl) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _e = static_cast<GRIN_EDGE_T*>(e);
  auto _epl = static_cast<GRIN_EDGE_PROPERTY_LIST_T*>(epl);
  if (_g->unique_edge_type_ids[_e->type_id] != ept)
    return GRIN_NULL_ROW;

  auto r = new GRIN_ROW_T();
  for (auto& _ep : *_epl) {
    if (_g->unique_edge_type_ids[_e->type_id] != _ep.type_id) {
      delete r;
      return GRIN_NULL_ROW;
    }

    switch (_ep.type) {
    case GRIN_DATATYPE::Int32: {
      auto value = _e->edge.property<int32_t>(_ep.name).value();
      r->push_back(value);
      break;
    }
    case GRIN_DATATYPE::Int64: {
      auto value = _e->edge.property<int64_t>(_ep.name).value();
      r->push_back(value);
      break;
    }
    case GRIN_DATATYPE::Float: {
      auto value = _e->edge.property<float>(_ep.name).value();
      r->push_back(value);
      break;
    }
    case GRIN_DATATYPE::Double: {
      auto value = _e->edge.property<double>(_ep.name).value();
      r->push_back(value);
      break;
    }
    case GRIN_DATATYPE::String: {
      auto value = _e->edge.property<std::string>(_ep.name).value();
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

#if !defined(GRIN_ASSUME_COLUMN_STORE_FOR_EDGE_PROPERTY) && \
    defined(GRIN_ENABLE_ROW)
GRIN_ROW grin_get_edge_row(GRIN_GRAPH, GRIN_EDGE, GRIN_EDGE_PROPERTY_LIST);
#endif
