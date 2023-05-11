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

#include <sstream>

extern "C" {
#include "grin/include/partition/reference.h"
}
#include "grin/src/predefine.h"

#ifdef GRIN_ENABLE_VERTEX_REF
GRIN_VERTEX_REF grin_get_vertex_ref_by_vertex(GRIN_GRAPH g, GRIN_VERTEX v) {
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  return new GRIN_VERTEX_REF_T(_v->id, _v->type_id);
}

void grin_destroy_vertex_ref(GRIN_GRAPH g, GRIN_VERTEX_REF vr) {
  auto _vr = static_cast<GRIN_VERTEX_REF_T*>(vr);
  delete _vr;
}

GRIN_VERTEX grin_get_vertex_from_vertex_ref(GRIN_GRAPH g, GRIN_VERTEX_REF vr) {
  auto _vr = static_cast<GRIN_VERTEX_REF_T*>(vr);
  return new GRIN_VERTEX_T(_vr->id, _vr->type_id);
}

GRIN_PARTITION grin_get_master_partition_from_vertex_ref(GRIN_GRAPH g,
                                                         GRIN_VERTEX_REF vr) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vr = static_cast<GRIN_VERTEX_REF_T*>(vr);
  return __grin_get_master_partition_id(_g, _vr->id, _vr->type_id);
}

const char* grin_serialize_vertex_ref(GRIN_GRAPH g, GRIN_VERTEX_REF vr) {
  auto _vr = static_cast<GRIN_VERTEX_REF_T*>(vr);
  std::stringstream ss;
  ss << _vr->id << ' ' << _vr->type_id;
  int len = ss.str().length() + 1;
  char* out = new char[len];
  snprintf(out, len, "%s", ss.str().c_str());
  return out;
}

void grin_destroy_serialized_vertex_ref(GRIN_GRAPH g, const char* msg) {
  delete[] msg;
}

GRIN_VERTEX_REF grin_deserialize_to_vertex_ref(GRIN_GRAPH g, const char* msg) {
  std::stringstream ss(msg);
  GAR_NAMESPACE::IdType id;
  unsigned type_id;
  ss >> id >> type_id;
  return new GRIN_VERTEX_REF_T(id, type_id);
}

bool grin_is_master_vertex(GRIN_GRAPH g, GRIN_VERTEX v) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  return __grin_get_master_partition_id(_g, _v->id, _v->type_id) ==
         _g->partition_id;
}

bool grin_is_mirror_vertex(GRIN_GRAPH g, GRIN_VERTEX v) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  return __grin_get_master_partition_id(_g, _v->id, _v->type_id) !=
         _g->partition_id;
}
#endif

#ifdef GRIN_TRAIT_FAST_VERTEX_REF
long long int grin_serialize_vertex_ref_as_int64(GRIN_GRAPH g,
                                                 GRIN_VERTEX_REF vr) {
  auto _vr = static_cast<GRIN_VERTEX_REF_T*>(vr);
  return __gin_generate_int64_from_id_and_type(_vr->id, _vr->type_id);
}

GRIN_VERTEX_REF grin_deserialize_int64_to_vertex_ref(GRIN_GRAPH g,
                                                     long long int svr) {
  auto pair = __gin_generate_id_and_type_from_int64(svr);
  return new GRIN_VERTEX_REF_T(pair.first, pair.second);
}
#endif

#ifdef GRIN_TRAIT_MASTER_VERTEX_MIRROR_PARTITION_LIST
GRIN_PARTITION_LIST grin_get_master_vertex_mirror_partition_list(
    GRIN_GRAPH g, GRIN_VERTEX v) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto partition_id = __grin_get_master_partition_id(_g, _v->id, _v->type_id);
  if (partition_id != _g->partition_id)
    return GRIN_NULL_LIST;
  auto pl = new GRIN_PARTITION_LIST_T();
  for (size_t i = 0; i < _g->partition_num; ++i) {
    if (i != _g->partition_id)
      pl->push_back(i);
  }
  return pl;
}
#endif

#ifdef GRIN_TRAIT_MIRROR_VERTEX_MIRROR_PARTITION_LIST
GRIN_PARTITION_LIST grin_get_mirror_vertex_mirror_partition_list(
    GRIN_GRAPH g, GRIN_VERTEX v) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto partition_id = __grin_get_master_partition_id(_g, _v->id, _v->type_id);
  if (partition_id == _g->partition_id)
    return GRIN_NULL_LIST;
  auto pl = new GRIN_PARTITION_LIST_T();
  for (size_t i = 0; i < _g->partition_num; ++i) {
    if (i != partition_id)
      pl->push_back(i);
  }
  return pl;
}
#endif

#ifdef GRIN_ENABLE_EDGE_REF
GRIN_EDGE_REF grin_get_edge_ref_by_edge(GRIN_GRAPH, GRIN_EDGE);

void grin_destroy_edge_ref(GRIN_GRAPH, GRIN_EDGE_REF);

GRIN_EDGE grin_get_edge_from_edge_ref(GRIN_GRAPH, GRIN_EDGE_REF);

GRIN_PARTITION grin_get_master_partition_from_edge_ref(GRIN_GRAPH,
                                                       GRIN_EDGE_REF);

const char* grin_serialize_edge_ref(GRIN_GRAPH, GRIN_EDGE_REF);

void grin_destroy_serialized_edge_ref(GRIN_GRAPH, GRIN_EDGE_REF);

GRIN_EDGE_REF grin_deserialize_to_edge_ref(GRIN_GRAPH, const char*);

bool grin_is_master_edge(GRIN_GRAPH, GRIN_EDGE);

bool grin_is_mirror_edge(GRIN_GRAPH, GRIN_EDGE);
#endif

#ifdef GRIN_TRAIT_MASTER_EDGE_MIRROR_PARTITION_LIST
GRIN_PARTITION_LIST grin_get_master_edge_mirror_partition_list(GRIN_GRAPH,
                                                               GRIN_EDGE);
#endif

#ifdef GRIN_TRAIT_MIRROR_EDGE_MIRROR_PARTITION_LIST
GRIN_PARTITION_LIST grin_get_mirror_edge_mirror_partition_list(GRIN_GRAPH,
                                                               GRIN_EDGE);
#endif
