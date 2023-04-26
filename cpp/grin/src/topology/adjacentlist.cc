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

#include "grin/include/topology/adjacentlist.h"
#include "grin/src/predefine.h"

#ifdef GRIN_ENABLE_ADJACENT_LIST
GRIN_ADJACENT_LIST grin_get_adjacent_list(GRIN_GRAPH g, GRIN_DIRECTION d,
                                          GRIN_VERTEX v) {
  if (d == GRIN_DIRECTION::BOTH)
    return GRIN_NULL_LIST;
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto al = new GRIN_ADJACENT_LIST_T(*_v, d, 0, _g->edge_type_num, 0,
                                     _g->vertex_type_num);
  return al;
}

void grin_destroy_adjacent_list(GRIN_GRAPH g, GRIN_ADJACENT_LIST al) {
  auto _al = static_cast<GRIN_ADJACENT_LIST_T*>(al);
  delete _al;
}
#endif

#ifdef GRIN_ENABLE_ADJACENT_LIST_ARRAY
size_t grin_get_adjacent_list_size(GRIN_GRAPH, GRIN_ADJACENT_LIST);

GRIN_VERTEX grin_get_neighbor_from_adjacent_list(GRIN_GRAPH, GRIN_ADJACENT_LIST,
                                                 size_t);

GRIN_EDGE grin_get_edge_from_adjacent_list(GRIN_GRAPH, GRIN_ADJACENT_LIST,
                                           size_t);
#endif

#ifdef GRIN_ENABLE_ADJACENT_LIST_ITERATOR
GRIN_ADJACENT_LIST_ITERATOR grin_get_adjacent_list_begin(
    GRIN_GRAPH g, GRIN_ADJACENT_LIST al) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _al = static_cast<GRIN_ADJACENT_LIST_T*>(al);
  for (auto i = _al->etype_begin; i < _al->etype_end; i++) {
    // IN edges
    if (_al->dir == GRIN_DIRECTION::IN) {
      if (_g->dst_type_ids[i] != _al->v.type_id)
        continue;
      if (_g->src_type_ids[i] < _al->vtype_begin ||
          _g->src_type_ids[i] >= _al->vtype_end)
        continue;

      auto adj_list_type = GAR_NAMESPACE::AdjListType::ordered_by_dest;
      if (_g->edges_collections[i].find(adj_list_type) !=
          _g->edges_collections[i].end()) {
        auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
            GAR_NAMESPACE::AdjListType::ordered_by_dest>>(
            _g->edges_collections[i].at(adj_list_type));
        auto iter = edges.find_dst(_al->v.id, edges.begin());
        if (iter.is_end()) {
          continue;
        } else {
          auto ali = new GRIN_ADJACENT_LIST_ITERATOR_T(
              _al->v, _al->dir, _al->etype_begin, _al->etype_end,
              _al->vtype_begin, _al->vtype_end, i, iter);
          return ali;
        }
      }
      adj_list_type = GAR_NAMESPACE::AdjListType::unordered_by_dest;
      if (_g->edges_collections[i].find(adj_list_type) !=
          _g->edges_collections[i].end()) {
        auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
            GAR_NAMESPACE::AdjListType::unordered_by_dest>>(
            _g->edges_collections[i].at(adj_list_type));
        auto iter = edges.find_dst(_al->v.id, edges.begin());
        if (iter.is_end()) {
          continue;
        } else {
          auto ali = new GRIN_ADJACENT_LIST_ITERATOR_T(
              _al->v, _al->dir, _al->etype_begin, _al->etype_end,
              _al->vtype_begin, _al->vtype_end, i, iter);
          return ali;
        }
      }
      adj_list_type = GAR_NAMESPACE::AdjListType::ordered_by_source;
      if (_g->edges_collections[i].find(adj_list_type) !=
          _g->edges_collections[i].end()) {
        auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
            GAR_NAMESPACE::AdjListType::ordered_by_source>>(
            _g->edges_collections[i].at(adj_list_type));
        auto iter = edges.find_dst(_al->v.id, edges.begin());
        if (iter.is_end()) {
          continue;
        } else {
          auto ali = new GRIN_ADJACENT_LIST_ITERATOR_T(
              _al->v, _al->dir, _al->etype_begin, _al->etype_end,
              _al->vtype_begin, _al->vtype_end, i, iter);
          return ali;
        }
      }
      adj_list_type = GAR_NAMESPACE::AdjListType::unordered_by_source;
      if (_g->edges_collections[i].find(adj_list_type) !=
          _g->edges_collections[i].end()) {
        auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
            GAR_NAMESPACE::AdjListType::unordered_by_source>>(
            _g->edges_collections[i].at(adj_list_type));
        auto iter = edges.find_dst(_al->v.id, edges.begin());
        if (iter.is_end()) {
          continue;
        } else {
          auto ali = new GRIN_ADJACENT_LIST_ITERATOR_T(
              _al->v, _al->dir, _al->etype_begin, _al->etype_end,
              _al->vtype_begin, _al->vtype_end, i, iter);
          return ali;
        }
      }
    }

    // OUT edges
    if (_al->dir == GRIN_DIRECTION::OUT) {
      if (_g->src_type_ids[i] != _al->v.type_id)
        continue;
      if (_g->dst_type_ids[i] < _al->vtype_begin ||
          _g->dst_type_ids[i] >= _al->vtype_end)
        continue;

      auto adj_list_type = GAR_NAMESPACE::AdjListType::ordered_by_source;
      if (_g->edges_collections[i].find(adj_list_type) !=
          _g->edges_collections[i].end()) {
        auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
            GAR_NAMESPACE::AdjListType::ordered_by_source>>(
            _g->edges_collections[i].at(adj_list_type));
        auto iter = edges.find_src(_al->v.id, edges.begin());
        if (iter.is_end()) {
          continue;
        } else {
          auto ali = new GRIN_ADJACENT_LIST_ITERATOR_T(
              _al->v, _al->dir, _al->etype_begin, _al->etype_end,
              _al->vtype_begin, _al->vtype_end, i, iter);
          return ali;
        }
      }
      adj_list_type = GAR_NAMESPACE::AdjListType::unordered_by_source;
      if (_g->edges_collections[i].find(adj_list_type) !=
          _g->edges_collections[i].end()) {
        auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
            GAR_NAMESPACE::AdjListType::unordered_by_source>>(
            _g->edges_collections[i].at(adj_list_type));
        auto iter = edges.find_src(_al->v.id, edges.begin());
        if (iter.is_end()) {
          continue;
        } else {
          auto ali = new GRIN_ADJACENT_LIST_ITERATOR_T(
              _al->v, _al->dir, _al->etype_begin, _al->etype_end,
              _al->vtype_begin, _al->vtype_end, i, iter);
          return ali;
        }
      }
      adj_list_type = GAR_NAMESPACE::AdjListType::ordered_by_dest;
      if (_g->edges_collections[i].find(adj_list_type) !=
          _g->edges_collections[i].end()) {
        auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
            GAR_NAMESPACE::AdjListType::ordered_by_dest>>(
            _g->edges_collections[i].at(adj_list_type));
        auto iter = edges.find_src(_al->v.id, edges.begin());
        if (iter.is_end()) {
          continue;
        } else {
          auto ali = new GRIN_ADJACENT_LIST_ITERATOR_T(
              _al->v, _al->dir, _al->etype_begin, _al->etype_end,
              _al->vtype_begin, _al->vtype_end, i, iter);
          return ali;
        }
      }
      adj_list_type = GAR_NAMESPACE::AdjListType::unordered_by_dest;
      if (_g->edges_collections[i].find(adj_list_type) !=
          _g->edges_collections[i].end()) {
        auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
            GAR_NAMESPACE::AdjListType::unordered_by_dest>>(
            _g->edges_collections[i].at(adj_list_type));
        auto iter = edges.find_src(_al->v.id, edges.begin());
        if (iter.is_end()) {
          continue;
        } else {
          auto ali = new GRIN_ADJACENT_LIST_ITERATOR_T(
              _al->v, _al->dir, _al->etype_begin, _al->etype_end,
              _al->vtype_begin, _al->vtype_end, i, iter);
          return ali;
        }
      }
    }
  }
  return GRIN_NULL_LIST_ITERATOR;
}

void grin_destroy_adjacent_list_iter(GRIN_GRAPH g,
                                     GRIN_ADJACENT_LIST_ITERATOR ali) {
  auto _ali = static_cast<GRIN_ADJACENT_LIST_ITERATOR_T*>(ali);
  delete _ali;
}

void grin_get_next_adjacent_list_iter(GRIN_GRAPH g,
                                      GRIN_ADJACENT_LIST_ITERATOR ali) {
  if (ali == GRIN_NULL_LIST_ITERATOR)
    return;
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _ali = static_cast<GRIN_ADJACENT_LIST_ITERATOR_T*>(ali);
  if (_ali->dir == GRIN_DIRECTION::IN) {
    if (_ali->iter.next_dst())
      return;
  } else {
    if (_ali->iter.next_src())
      return;
  }

  for (_ali->current_etype++; _ali->current_etype < _ali->etype_end;
       _ali->current_etype++) {
    auto i = _ali->current_etype;

    // IN edges
    if (_ali->dir == GRIN_DIRECTION::IN) {
      if (_g->dst_type_ids[i] != _ali->v.type_id)
        continue;
      if (_g->src_type_ids[i] < _ali->vtype_begin ||
          _g->src_type_ids[i] >= _ali->vtype_end)
        continue;
      auto adj_list_type = GAR_NAMESPACE::AdjListType::ordered_by_dest;
      if (_g->edges_collections[i].find(adj_list_type) !=
          _g->edges_collections[i].end()) {
        auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
            GAR_NAMESPACE::AdjListType::ordered_by_dest>>(
            _g->edges_collections[i].at(adj_list_type));
        auto iter = edges.find_dst(_ali->v.id, edges.begin());
        if (iter.is_end()) {
          continue;
        } else {
          _ali->iter = std::move(iter);
          return;
        }
      }
      adj_list_type = GAR_NAMESPACE::AdjListType::unordered_by_dest;
      if (_g->edges_collections[i].find(adj_list_type) !=
          _g->edges_collections[i].end()) {
        auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
            GAR_NAMESPACE::AdjListType::unordered_by_dest>>(
            _g->edges_collections[i].at(adj_list_type));
        auto iter = edges.find_dst(_ali->v.id, edges.begin());
        if (iter.is_end()) {
          continue;
        } else {
          _ali->iter = std::move(iter);
          return;
        }
      }
      adj_list_type = GAR_NAMESPACE::AdjListType::ordered_by_source;
      if (_g->edges_collections[i].find(adj_list_type) !=
          _g->edges_collections[i].end()) {
        auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
            GAR_NAMESPACE::AdjListType::ordered_by_source>>(
            _g->edges_collections[i].at(adj_list_type));
        auto iter = edges.find_dst(_ali->v.id, edges.begin());
        if (iter.is_end()) {
          continue;
        } else {
          _ali->iter = std::move(iter);
          return;
        }
      }
      adj_list_type = GAR_NAMESPACE::AdjListType::unordered_by_source;
      if (_g->edges_collections[i].find(adj_list_type) !=
          _g->edges_collections[i].end()) {
        auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
            GAR_NAMESPACE::AdjListType::unordered_by_source>>(
            _g->edges_collections[i].at(adj_list_type));
        auto iter = edges.find_dst(_ali->v.id, edges.begin());
        if (iter.is_end()) {
          continue;
        } else {
          _ali->iter = std::move(iter);
          return;
        }
      }
    }

    // OUT edges
    if (_ali->dir == GRIN_DIRECTION::OUT) {
      if (_g->src_type_ids[i] != _ali->v.type_id)
        continue;
      if (_g->dst_type_ids[i] < _ali->vtype_begin ||
          _g->dst_type_ids[i] >= _ali->vtype_end)
        continue;
      auto adj_list_type = GAR_NAMESPACE::AdjListType::ordered_by_source;
      if (_g->edges_collections[i].find(adj_list_type) !=
          _g->edges_collections[i].end()) {
        auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
            GAR_NAMESPACE::AdjListType::ordered_by_source>>(
            _g->edges_collections[i].at(adj_list_type));
        auto iter = edges.find_src(_ali->v.id, edges.begin());
        if (iter.is_end()) {
          continue;
        } else {
          _ali->iter = std::move(iter);
          return;
        }
      }
      adj_list_type = GAR_NAMESPACE::AdjListType::unordered_by_source;
      if (_g->edges_collections[i].find(adj_list_type) !=
          _g->edges_collections[i].end()) {
        auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
            GAR_NAMESPACE::AdjListType::unordered_by_source>>(
            _g->edges_collections[i].at(adj_list_type));
        auto iter = edges.find_src(_ali->v.id, edges.begin());
        if (iter.is_end()) {
          continue;
        } else {
          _ali->iter = std::move(iter);
          return;
        }
      }
      adj_list_type = GAR_NAMESPACE::AdjListType::ordered_by_dest;
      if (_g->edges_collections[i].find(adj_list_type) !=
          _g->edges_collections[i].end()) {
        auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
            GAR_NAMESPACE::AdjListType::ordered_by_dest>>(
            _g->edges_collections[i].at(adj_list_type));
        auto iter = edges.find_src(_ali->v.id, edges.begin());
        if (iter.is_end()) {
          continue;
        } else {
          _ali->iter = std::move(iter);
          return;
        }
      }
      adj_list_type = GAR_NAMESPACE::AdjListType::unordered_by_dest;
      if (_g->edges_collections[i].find(adj_list_type) !=
          _g->edges_collections[i].end()) {
        auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
            GAR_NAMESPACE::AdjListType::unordered_by_dest>>(
            _g->edges_collections[i].at(adj_list_type));
        auto iter = edges.find_src(_ali->v.id, edges.begin());
        if (iter.is_end()) {
          continue;
        } else {
          _ali->iter = std::move(iter);
          return;
        }
      }
    }
  }
}

bool grin_is_adjacent_list_end(GRIN_GRAPH g, GRIN_ADJACENT_LIST_ITERATOR ali) {
  if (ali == GRIN_NULL_LIST_ITERATOR)
    return true;
  auto _ali = static_cast<GRIN_ADJACENT_LIST_ITERATOR_T*>(ali);
  return ((_ali->current_etype >= _ali->etype_end) ||
          (_ali->current_etype == _ali->etype_end - 1 && _ali->iter.is_end()));
}

GRIN_VERTEX grin_get_neighbor_from_adjacent_list_iter(
    GRIN_GRAPH g, GRIN_ADJACENT_LIST_ITERATOR ali) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _ali = static_cast<GRIN_ADJACENT_LIST_ITERATOR_T*>(ali);
  if (_ali->dir == GRIN_DIRECTION::IN) {
    auto v = new GRIN_VERTEX_T(_ali->iter.source(),
                               _g->src_type_ids[_ali->current_etype]);
    return v;
  } else if (_ali->dir == GRIN_DIRECTION::OUT) {
    auto v = new GRIN_VERTEX_T(_ali->iter.destination(),
                               _g->dst_type_ids[_ali->current_etype]);
    return v;
  }
  return GRIN_NULL_VERTEX;
}

GRIN_EDGE grin_get_edge_from_adjacent_list_iter(
    GRIN_GRAPH g, GRIN_ADJACENT_LIST_ITERATOR ali) {
  auto _ali = static_cast<GRIN_ADJACENT_LIST_ITERATOR_T*>(ali);
  auto e = new GRIN_EDGE_T(*_ali->iter, _ali->current_etype);
  return e;
}
#endif
