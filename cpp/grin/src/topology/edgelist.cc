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
#include "grin/include/topology/edgelist.h"
}
#include "grin/src/predefine.h"

#ifdef GRIN_ENABLE_EDGE_LIST
GRIN_EDGE_LIST grin_get_edge_list(GRIN_GRAPH g) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto el = new GRIN_EDGE_LIST_T(0, _g->edge_type_num);
  return el;
}

void grin_destroy_edge_list(GRIN_GRAPH g, GRIN_EDGE_LIST el) {
  auto _el = static_cast<GRIN_EDGE_LIST_T*>(el);
  delete _el;
}
#endif

#ifdef GRIN_ENABLE_EDGE_LIST_ARRAY
size_t grin_get_edge_list_size(GRIN_GRAPH, GRIN_EDGE_LIST);

GRIN_EDGE grin_get_edge_from_list(GRIN_GRAPH, GRIN_EDGE_LIST, size_t);
#endif

#ifdef GRIN_ENABLE_EDGE_LIST_ITERATOR
GRIN_EDGE_LIST_ITERATOR grin_get_edge_list_begin(GRIN_GRAPH g,
                                                 GRIN_EDGE_LIST el) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _el = static_cast<GRIN_EDGE_LIST_T*>(el);
  auto type_id = _el->type_begin;
  if (type_id >= _g->edge_type_num ||
      _g->edges_collections[type_id].size() == 0)
    return GRIN_NULL_LIST_ITERATOR;
  auto adj_list_type = _g->edges_collections[type_id].begin()->first;
  switch (adj_list_type) {
  case GAR_ORDERED_BY_SOURCE:
    return new GRIN_EDGE_LIST_ITERATOR_T(
        _el->type_end, _el->type_begin,
        std::get<GAR_NAMESPACE::EdgesCollection<GAR_ORDERED_BY_SOURCE>>(
            _g->edges_collections[type_id].at(adj_list_type))
            .begin());
  case GAR_ORDERED_BY_DEST:
    return new GRIN_EDGE_LIST_ITERATOR_T(
        _el->type_end, _el->type_begin,
        std::get<GAR_NAMESPACE::EdgesCollection<GAR_ORDERED_BY_DEST>>(
            _g->edges_collections[type_id].at(adj_list_type))
            .begin());
  case GAR_UNORDERED_BY_SOURCE:
    return new GRIN_EDGE_LIST_ITERATOR_T(
        _el->type_end, _el->type_begin,
        std::get<GAR_NAMESPACE::EdgesCollection<GAR_UNORDERED_BY_SOURCE>>(
            _g->edges_collections[type_id].at(adj_list_type))
            .begin());
  case GAR_UNORDERED_BY_DEST:
    return new GRIN_EDGE_LIST_ITERATOR_T(
        _el->type_end, _el->type_begin,
        std::get<GAR_NAMESPACE::EdgesCollection<GAR_UNORDERED_BY_DEST>>(
            _g->edges_collections[type_id].at(adj_list_type))
            .begin());
  default:
    return GRIN_NULL_LIST_ITERATOR;
  }
  return GRIN_NULL_LIST_ITERATOR;
}

void grin_destroy_edge_list_iter(GRIN_GRAPH g, GRIN_EDGE_LIST_ITERATOR eli) {
  auto _eli = static_cast<GRIN_EDGE_LIST_ITERATOR_T*>(eli);
  delete _eli;
}

void grin_get_next_edge_list_iter(GRIN_GRAPH g, GRIN_EDGE_LIST_ITERATOR eli) {
  auto _eli = static_cast<GRIN_EDGE_LIST_ITERATOR_T*>(eli);
  ++_eli->iter;
  if (_eli->iter.is_end()) {
    _eli->current_type++;
    if (_eli->current_type >= _eli->type_end)
      return;
    auto type_id = _eli->current_type;
    auto _g = static_cast<GRIN_GRAPH_T*>(g);
    if (_g->edges_collections[type_id].size() == 0) {
      _eli->current_type = _eli->type_end;
      return;
    }
    auto adj_list_type = _g->edges_collections[type_id].begin()->first;
    switch (adj_list_type) {
    case GAR_ORDERED_BY_SOURCE:
      _eli->iter =
          std::get<GAR_NAMESPACE::EdgesCollection<GAR_ORDERED_BY_SOURCE>>(
              _g->edges_collections[type_id].at(adj_list_type))
              .begin();
      break;
    case GAR_ORDERED_BY_DEST:
      _eli->iter =
          std::get<GAR_NAMESPACE::EdgesCollection<GAR_ORDERED_BY_DEST>>(
              _g->edges_collections[type_id].at(adj_list_type))
              .begin();
      break;
    case GAR_UNORDERED_BY_SOURCE:
      _eli->iter =
          std::get<GAR_NAMESPACE::EdgesCollection<GAR_UNORDERED_BY_SOURCE>>(
              _g->edges_collections[type_id].at(adj_list_type))
              .begin();
      break;
    case GAR_UNORDERED_BY_DEST:
      _eli->iter =
          std::get<GAR_NAMESPACE::EdgesCollection<GAR_UNORDERED_BY_DEST>>(
              _g->edges_collections[type_id].at(adj_list_type))
              .begin();
      break;
    default:
      _eli->current_type = _eli->type_end;
    }
  }
}

bool grin_is_edge_list_end(GRIN_GRAPH g, GRIN_EDGE_LIST_ITERATOR eli) {
  if (eli == GRIN_NULL_LIST_ITERATOR)
    return true;
  auto _eli = static_cast<GRIN_EDGE_LIST_ITERATOR_T*>(eli);
  return _eli->current_type >= _eli->type_end;
}

GRIN_EDGE grin_get_edge_from_iter(GRIN_GRAPH g, GRIN_EDGE_LIST_ITERATOR eli) {
  auto _eli = static_cast<GRIN_EDGE_LIST_ITERATOR_T*>(eli);
  auto e = new GRIN_EDGE_T(*_eli->iter, _eli->current_type);
  return e;
}
#endif
