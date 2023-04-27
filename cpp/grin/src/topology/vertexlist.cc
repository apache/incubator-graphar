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
#include "grin/include/topology/vertexlist.h"
}
#include "grin/src/predefine.h"

#ifdef GRIN_ENABLE_VERTEX_LIST
GRIN_VERTEX_LIST grin_get_vertex_list(GRIN_GRAPH g) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto vl = new GRIN_VERTEX_LIST_T(0, _g->vertex_type_num);
  return vl;
}

void grin_destroy_vertex_list(GRIN_GRAPH g, GRIN_VERTEX_LIST vl) {
  auto _vl = static_cast<GRIN_VERTEX_LIST_T*>(vl);
  delete _vl;
}
#endif

#ifdef GRIN_ENABLE_VERTEX_LIST_ARRAY
size_t grin_get_vertex_list_size(GRIN_GRAPH g, GRIN_VERTEX_LIST vl) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vl = static_cast<GRIN_VERTEX_LIST_T*>(vl);
  return _g->vertex_offsets[_vl->type_end] -
         _g->vertex_offsets[_vl->type_begin];
}

GRIN_VERTEX grin_get_vertex_from_list(GRIN_GRAPH g, GRIN_VERTEX_LIST vl,
                                      size_t idx) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vl = static_cast<GRIN_VERTEX_LIST_T*>(vl);
  for (auto i = _vl->type_begin; i < _vl->type_end; i++) {
    if (idx < _g->vertex_offsets[i + 1] - _g->vertex_offsets[_vl->type_begin]) {
      auto _idx =
          idx + _g->vertex_offsets[_vl->type_begin] - _g->vertex_offsets[i];
      auto v = new GRIN_VERTEX_T(_idx, i);
      return v;
    }
  }
  return GRIN_NULL_VERTEX;
}
#endif

#ifdef GRIN_ENABLE_VERTEX_LIST_ITERATOR
GRIN_VERTEX_LIST_ITERATOR grin_get_vertex_list_begin(GRIN_GRAPH g,
                                                     GRIN_VERTEX_LIST vl) {
  auto _vl = static_cast<GRIN_VERTEX_LIST_T*>(vl);
  auto vli = new GRIN_VERTEX_LIST_ITERATOR_T(_vl->type_begin, _vl->type_end,
                                             _vl->type_begin, 0);
  return vli;
}

void grin_destroy_vertex_list_iter(GRIN_GRAPH g,
                                   GRIN_VERTEX_LIST_ITERATOR vli) {
  auto _vli = static_cast<GRIN_VERTEX_LIST_ITERATOR_T*>(vli);
  delete _vli;
}

void grin_get_next_vertex_list_iter(GRIN_GRAPH g,
                                    GRIN_VERTEX_LIST_ITERATOR vli) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vli = static_cast<GRIN_VERTEX_LIST_ITERATOR_T*>(vli);
  _vli->current_offset++;
  while (_vli->current_type < _vli->type_end &&
         _vli->current_offset >= _g->vertex_offsets[_vli->current_type + 1] -
                                     _g->vertex_offsets[_vli->current_type]) {
    _vli->current_type++;
    _vli->current_offset = 0;
  }
}

bool grin_is_vertex_list_end(GRIN_GRAPH g, GRIN_VERTEX_LIST_ITERATOR vli) {
  if (vli == GRIN_NULL_LIST_ITERATOR)
    return true;
  auto _vli = static_cast<GRIN_VERTEX_LIST_ITERATOR_T*>(vli);
  return _vli->current_type >= _vli->type_end;
}

GRIN_VERTEX grin_get_vertex_from_iter(GRIN_GRAPH g,
                                      GRIN_VERTEX_LIST_ITERATOR vli) {
  auto _vli = static_cast<GRIN_VERTEX_LIST_ITERATOR_T*>(vli);
  auto v = new GRIN_VERTEX_T(_vli->current_offset, _vli->current_type);
  return v;
}
#endif
