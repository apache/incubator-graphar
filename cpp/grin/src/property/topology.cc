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
#include "property/topology.h"

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
  auto vl = new GRIN_VERTEX_LIST_T(vtype);
  return vl;
}
#endif

#if defined(GRIN_ENABLE_EDGE_LIST) && defined(GRIN_WITH_EDGE_PROPERTY)
GRIN_EDGE_LIST grin_get_edge_list_by_type(GRIN_GRAPH g, GRIN_EDGE_TYPE etype) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  if (etype >= _g->unique_edge_type_num)
    return GRIN_NULL_EDGE_LIST;
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
    return GRIN_NULL_ADJACENT_LIST;
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  if (etype >= _g->unique_edge_type_num)
    return GRIN_NULL_ADJACENT_LIST;
  auto al = new GRIN_ADJACENT_LIST_T(
      _v->id, _v->type_id, d, _g->unique_edge_type_begin_type[etype],
      _g->unique_edge_type_begin_type[etype + 1]);
  return al;
}
#endif
