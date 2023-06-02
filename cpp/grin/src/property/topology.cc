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
#include "property/topology.h"
}

#ifdef GRIN_WITH_VERTEX_PROPERTY
size_t grin_get_vertex_num_by_type(GRIN_GRAPH g, GRIN_VERTEX_TYPE vtype) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  return _g->vertex_offsets[vtype + 1] - _g->vertex_offsets[vtype];
}
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
size_t grin_get_edge_num_by_type(GRIN_GRAPH g, GRIN_EDGE_TYPE etype) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  if (etype >= _g->unique_edge_type_num)
    return GRIN_NULL_SIZE;
  unsigned type_begin = _g->unique_edge_type_begin_type[etype],
           type_end = _g->unique_edge_type_begin_type[etype + 1];
  return __grin_get_edge_num(_g, type_begin, type_end);
}
#endif

#if defined(GRIN_ENABLE_VERTEX_LIST) && defined(GRIN_WITH_VERTEX_PROPERTY)
GRIN_VERTEX_LIST grin_get_vertex_list_by_type(GRIN_GRAPH g,
                                              GRIN_VERTEX_TYPE vtype) {
  auto vl = new GRIN_VERTEX_LIST_T(vtype, vtype + 1);
  return vl;
}
#endif

#if defined(GRIN_ENABLE_EDGE_LIST) && defined(GRIN_WITH_EDGE_PROPERTY)
GRIN_EDGE_LIST grin_get_edge_list_by_type(GRIN_GRAPH g, GRIN_EDGE_TYPE etype) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  if (etype >= _g->unique_edge_type_num)
    return GRIN_NULL_LIST;
  auto el = new GRIN_EDGE_LIST_T(_g->unique_edge_type_begin_type[etype],
                                 _g->unique_edge_type_begin_type[etype + 1]);
  return el;
}
#endif

#if defined(GRIN_ENABLE_ADJACENT_LIST) && defined(GRIN_WITH_EDGE_PROPERTY)
GRIN_ADJACENT_LIST grin_get_adjacent_list_by_edge_type(GRIN_GRAPH g,
                                                       GRIN_DIRECTION d,
                                                       GRIN_VERTEX v,
                                                       GRIN_EDGE_TYPE etype) {
  if (d == GRIN_DIRECTION::BOTH)
    return GRIN_NULL_LIST;
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  if (etype >= _g->unique_edge_type_num)
    return GRIN_NULL_LIST;
  auto al = new GRIN_ADJACENT_LIST_T(
      _v->id, _v->type_id, d, _g->unique_edge_type_begin_type[etype],
      _g->unique_edge_type_begin_type[etype + 1], 0, _g->vertex_type_num);
  return al;
}
#endif

/*
#ifdef GRIN_TRAIT_SELECT_TYPE_FOR_VERTEX_LIST
GRIN_VERTEX_LIST grin_select_type_for_vertex_list(GRIN_GRAPH g,
                                                  GRIN_VERTEX_TYPE vtype,
                                                  GRIN_VERTEX_LIST vl) {
  auto _vl = static_cast<GRIN_VERTEX_LIST_T*>(vl);
  if (_vl->type_begin > vtype || _vl->type_end <= vtype)
    return GRIN_NULL_LIST;
  auto fvl = new GRIN_VERTEX_LIST_T(vtype, vtype + 1, _vl->partition_type,
                                    _vl->partition_id);
  return fvl;
}
#endif

#ifdef GRIN_TRAIT_SELECT_TYPE_FOR_EDGE_LIST
GRIN_EDGE_LIST grin_select_type_for_edge_list(GRIN_GRAPH g,
                                              GRIN_EDGE_TYPE etype,
                                              GRIN_EDGE_LIST el) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _el = static_cast<GRIN_EDGE_LIST_T*>(el);
  if (etype >= _g->unique_edge_type_num)
    return GRIN_NULL_LIST;
  unsigned type_begin =
      std::max(_g->unique_edge_type_begin_type[etype], _el->type_begin);
  unsigned type_end =
      std::min(_g->unique_edge_type_begin_type[etype + 1], _el->type_end);
  if (type_begin >= type_end)
    return GRIN_NULL_LIST;
  auto fel = new GRIN_EDGE_LIST_T(type_begin, type_end);
  return fel;
}
#endif

#ifdef GRIN_TRAIT_SELECT_NEIGHBOR_TYPE_FOR_ADJACENT_LIST
GRIN_ADJACENT_LIST grin_select_neighbor_type_for_adjacent_list(
    GRIN_GRAPH g, GRIN_VERTEX_TYPE vtype, GRIN_ADJACENT_LIST al) {
  auto _al = static_cast<GRIN_ADJACENT_LIST_T*>(al);
  auto fal = new GRIN_ADJACENT_LIST_T(_al->vid, _al->vtype_id, _al->dir,
                                      _al->etype_begin, _al->etype_end, vtype,
                                      vtype + 1);
  return fal;
}
#endif

#ifdef GRIN_TRAIT_SELECT_EDGE_TYPE_FOR_ADJACENT_LIST
GRIN_ADJACENT_LIST grin_select_edge_type_for_adjacent_list(
    GRIN_GRAPH g, GRIN_EDGE_TYPE etype, GRIN_ADJACENT_LIST al) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _al = static_cast<GRIN_ADJACENT_LIST_T*>(al);
  if (etype >= _g->unique_edge_type_num)
    return GRIN_NULL_LIST;
  unsigned type_begin =
      std::max(_g->unique_edge_type_begin_type[etype], _al->etype_begin);
  unsigned type_end =
      std::min(_g->unique_edge_type_begin_type[etype + 1], _al->etype_end);
  if (type_begin >= type_end)
    return GRIN_NULL_LIST;
  auto fal =
      new GRIN_ADJACENT_LIST_T(_al->vid, _al->vtype_id, _al->dir, type_begin,
                               type_end, 0, _g->vertex_type_num);
  return fal;
}
#endif
*/
