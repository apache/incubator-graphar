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
#include "index/internal_id.h"

#if defined(GRIN_ENABLE_VERTEX_INTERNAL_ID_INDEX) && \
    !defined(GRIN_ENABLE_SCHEMA)
long long int grin_get_vertex_internal_id(GRIN_GRAPH, GRIN_VERTEX);  // NOLINT

GRIN_VERTEX grin_get_vertex_by_internal_id(GRIN_GRAPH,
                                           long long int id);  // NOLINT

long long int grin_get_vertex_internal_id_upper_bound(GRIN_GRAPH);  // NOLINT

long long int grin_get_vertex_internal_id_lower_bound(GRIN_GRAPH);  // NOLINT
#endif

#if defined(GRIN_ENABLE_VERTEX_INTERNAL_ID_INDEX) && \
    defined(GRIN_ENABLE_SCHEMA)

long long int grin_get_vertex_internal_id_by_type(GRIN_GRAPH g,  // NOLINT
                                                  GRIN_VERTEX_TYPE vtype,
                                                  GRIN_VERTEX v) {
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  return _v->id;
}

GRIN_VERTEX grin_get_vertex_by_internal_id_by_type(
    GRIN_GRAPH g, GRIN_VERTEX_TYPE vtype,
    long long int id) {  // NOLINT
  auto v = new GRIN_VERTEX_T(id, vtype);
  return v;
}

long long int grin_get_vertex_internal_id_upper_bound_by_type(  // NOLINT
    GRIN_GRAPH g, GRIN_VERTEX_TYPE vtype) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  return _g->vertex_offsets[vtype + 1] - _g->vertex_offsets[vtype];
}

long long int grin_get_vertex_internal_id_lower_bound_by_type(  // NOLINT
    GRIN_GRAPH g, GRIN_VERTEX_TYPE vtype) {
  return 0;
}
#endif
