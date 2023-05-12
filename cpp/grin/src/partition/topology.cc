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
#include "grin/include/partition/topology.h"
}
#include "grin/src/predefine.h"

#ifdef GRIN_TRAIT_SELECT_MASTER_FOR_VERTEX_LIST
GRIN_VERTEX_LIST grin_select_master_for_vertex_list(GRIN_GRAPH g,
                                                    GRIN_VERTEX_LIST vl) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vl = static_cast<GRIN_VERTEX_LIST_T*>(vl);
  if (_vl->partition_type == ONE_PARTITION &&
      _vl->partition_id != _g->partition_id) {
    return GRIN_NULL_LIST;
  }
  if (_vl->partition_type == ALL_BUT_ONE_PARTITION &&
      _vl->partition_id == _g->partition_id) {
    return GRIN_NULL_LIST;
  }
  return new GRIN_VERTEX_LIST_T(_vl->type_begin, _vl->type_end, ONE_PARTITION,
                                _g->partition_id);
}

GRIN_VERTEX_LIST grin_select_mirror_for_vertex_list(GRIN_GRAPH g,
                                                    GRIN_VERTEX_LIST vl) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vl = static_cast<GRIN_VERTEX_LIST_T*>(vl);
  if (_vl->partition_type == ONE_PARTITION &&
      _vl->partition_id == _g->partition_id) {
    return GRIN_NULL_LIST;
  }
  if (_vl->partition_type == ALL_BUT_ONE_PARTITION &&
      _vl->partition_id != _g->partition_id) {
    return GRIN_NULL_LIST;
  }
  return new GRIN_VERTEX_LIST_T(_vl->type_begin, _vl->type_end,
                                ALL_BUT_ONE_PARTITION, _g->partition_id);
}
#endif

#ifdef GRIN_TRAIT_SELECT_PARTITION_FOR_VERTEX_LIST
GRIN_VERTEX_LIST grin_select_partition_for_vertex_list(GRIN_GRAPH g,
                                                       GRIN_PARTITION p,
                                                       GRIN_VERTEX_LIST vl) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vl = static_cast<GRIN_VERTEX_LIST_T*>(vl);
  if (p >= _g->partition_num) {
    return GRIN_NULL_LIST;
  }
  if (_vl->partition_type == ONE_PARTITION && _vl->partition_id != p) {
    return GRIN_NULL_LIST;
  }
  if (_vl->partition_type == ALL_BUT_ONE_PARTITION && _vl->partition_id == p) {
    return GRIN_NULL_LIST;
  }
  return new GRIN_VERTEX_LIST_T(_vl->type_begin, _vl->type_end, ONE_PARTITION,
                                p);
}
#endif

#ifdef GRIN_TRAIT_SELECT_MASTER_FOR_EDGE_LIST
GRIN_EDGE_LIST grin_select_master_for_edge_list(GRIN_GRAPH, GRIN_EDGE_LIST);

GRIN_EDGE_LIST grin_select_mirror_for_edge_list(GRIN_GRAPH, GRIN_EDGE_LIST);
#endif

#ifdef GRIN_TRAIT_SELECT_PARTITION_FOR_EDGE_LIST
GRIN_EDGE_LIST grin_select_partition_for_edge_list(GRIN_GRAPH, GRIN_PARTITION,
                                                   GRIN_EDGE_LIST);
#endif

#ifdef GRIN_TRAIT_SELECT_MASTER_NEIGHBOR_FOR_ADJACENT_LIST
GRIN_ADJACENT_LIST grin_select_master_neighbor_for_adjacent_list(
    GRIN_GRAPH, GRIN_ADJACENT_LIST);

GRIN_ADJACENT_LIST grin_select_mirror_neighbor_for_adjacent_list(
    GRIN_GRAPH, GRIN_ADJACENT_LIST);
#endif

#ifdef GRIN_TRAIT_SELECT_NEIGHBOR_PARTITION_FOR_ADJACENT_LIST
GRIN_ADJACENT_LIST grin_select_neighbor_partition_for_adjacent_list(
    GRIN_GRAPH, GRIN_PARTITION, GRIN_ADJACENT_LIST);
#endif
