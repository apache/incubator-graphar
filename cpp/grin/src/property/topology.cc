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
#include "grin/include/property/topology.h"
}
#include "grin/src/predefine.h"

#ifdef GRIN_WITH_VERTEX_PROPERTY
size_t grin_get_vertex_num_by_type(GRIN_GRAPH g, GRIN_VERTEX_TYPE vtype) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  return _g->vertex_offsets[vtype + 1] - _g->vertex_offsets[vtype];
}
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
size_t grin_get_edge_num_by_type(GRIN_GRAPH g, GRIN_EDGE_TYPE etype) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  unsigned type_begin = 0, type_end = 0;
  for (auto i = 0; i < _g->edge_type_num; i++) {
    if (_g->unique_edge_type_ids[i] > etype)
      break;
    else if (_g->unique_edge_type_ids[i] < etype)
      type_begin = i + 1;
    else
      type_end = i + 1;
  }
  return __grin_get_edge_num(_g, type_begin, type_end);
}
#endif

#if defined(GRIN_ENABLE_GRAPH_PARTITION) && defined(GRIN_WITH_VERTEX_PROPERTY)
size_t grin_get_total_vertex_num_by_type(GRIN_PARTITIONED_GRAPH,
                                         GRIN_VERTEX_TYPE) {}
#endif

#if defined(GRIN_ENABLE_GRAPH_PARTITION) && defined(GRIN_WITH_EDGE_PROPERTY)
size_t grin_get_total_edge_num_by_type(GRIN_PARTITIONED_GRAPH, GRIN_EDGE_TYPE);
#endif

#ifdef GRIN_ASSUME_BY_TYPE_VERTEX_ORIGINAL_ID
GRIN_VERTEX grin_get_vertex_from_original_id_by_type(
    GRIN_GRAPH g, GRIN_VERTEX_TYPE vtype, GRIN_VERTEX_ORIGINAL_ID oid) {
  auto _oid = static_cast<GRIN_VERTEX_ORIGINAL_ID_T*>(oid);
  auto v = new GRIN_VERTEX_T(*_oid, vtype);
  return v;
}
#endif

#ifdef GRIN_TRAIT_SELECT_TYPE_FOR_VERTEX_LIST
GRIN_VERTEX_LIST grin_select_type_for_vertex_list(GRIN_GRAPH g,
                                                  GRIN_VERTEX_TYPE vtype,
                                                  GRIN_VERTEX_LIST vl) {
  if (vl.type_begin > vtype || vl.type_end <= vtype)
    return GRIN_NULL_VERTEX_LIST;
  return {vtype, vtype + 1};
}
#endif

#ifdef GRIN_TRAIT_SELECT_TYPE_FOR_EDGE_LIST
GRIN_EDGE_LIST grin_select_type_for_edge_list(GRIN_GRAPH g,
                                              GRIN_EDGE_TYPE etype,
                                              GRIN_EDGE_LIST el) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  unsigned type_begin = el.type_begin, type_end = el.type_end;
  for (auto i = el.type_begin; i < el.type_end; i++) {
    if (_g->unique_edge_type_ids[i] > etype)
      break;
    else if (_g->unique_edge_type_ids[i] < etype)
      type_begin = i + 1;
    else
      type_end = i + 1;
  }
  if (type_begin >= type_end)
    return GRIN_NULL_EDGE_LIST;
  return {type_begin, type_end};
}
#endif

#ifdef GRIN_TRAIT_SELECT_NEIGHBOR_TYPE_FOR_ADJACENT_LIST
GRIN_ADJACENT_LIST grin_select_neighbor_type_for_adjacent_list(
    GRIN_GRAPH g, GRIN_VERTEX_TYPE vtype, GRIN_ADJACENT_LIST al) {
  auto _al = static_cast<GRIN_ADJACENT_LIST_T*>(al);
  auto fal = new GRIN_ADJACENT_LIST_T(_al->v, _al->dir, _al->etype_begin,
                                      _al->etype_end, vtype, vtype + 1);
  return fal;
}
#endif

#ifdef GRIN_TRAIT_SELECT_EDGE_TYPE_FOR_ADJACENT_LIST
GRIN_ADJACENT_LIST grin_select_edge_type_for_adjacent_list(
    GRIN_GRAPH g, GRIN_EDGE_TYPE etype, GRIN_ADJACENT_LIST al) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _al = static_cast<GRIN_ADJACENT_LIST_T*>(al);
  unsigned type_begin = _al->etype_begin, type_end = _al->etype_end;
  for (auto i = _al->etype_begin; i < _al->etype_end; i++) {
    if (_g->unique_edge_type_ids[i] > etype)
      break;
    else if (_g->unique_edge_type_ids[i] < etype)
      type_begin = i + 1;
    else
      type_end = i + 1;
  }
  if (type_begin >= type_end)
    return GRIN_NULL_LIST;
  auto fal = new GRIN_ADJACENT_LIST_T(_al->v, _al->dir, type_begin, type_end, 0,
                                      _g->vertex_type_num);
  return fal;
}
#endif
