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
#include "grin/include/index/order.h"
}
#include "grin/src/predefine.h"

#ifdef GRIN_ASSUME_ALL_VERTEX_LIST_SORTED
bool grin_smaller_vertex(GRIN_GRAPH g, GRIN_VERTEX v1, GRIN_VERTEX v2) {
  auto _v1 = static_cast<GRIN_VERTEX_T*>(v1);
  auto _v2 = static_cast<GRIN_VERTEX_T*>(v2);
  if (_v1->type_id != _v2->type_id)  // compare type id first
    return _v1->type_id < _v2->type_id;
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  if (_g->partition_strategy != SEGMENTED_PARTITION) {  // compare partition id
    auto p1 = __grin_get_master_partition_id(_g, _v1->id, _v1->type_id);
    auto p2 = __grin_get_master_partition_id(_g, _v2->id, _v2->type_id);
    if (p1 != p2)
      return p1 < p2;
  }
  return _v1->id < _v2->id;  // compare vertex id
}
#endif

#if defined(GRIN_ASSUME_ALL_VERTEX_LIST_SORTED) && \
    defined(GRIN_ENABLE_VERTEX_LIST_ARRAY)
size_t grin_get_position_of_vertex_from_sorted_list(GRIN_GRAPH g,
                                                    GRIN_VERTEX_LIST vl,
                                                    GRIN_VERTEX v) {
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto _vl = static_cast<GRIN_VERTEX_LIST_T*>(vl);
  if (_v->type_id < _vl->type_begin || _v->type_id >= _vl->type_end)
    return GRIN_NULL_SIZE;
  auto _g = static_cast<GRIN_GRAPH_T*>(g);

  // all partition
  if (_vl->partition_type == ALL_PARTITION) {
    size_t offset = _g->vertex_offsets[_v->type_id] + _v->id;
    if (offset < _g->vertex_offsets[_v->type_id + 1]) {
      return offset - _g->vertex_offsets[_vl->type_begin];
    } else {
      return GRIN_NULL_SIZE;
    }
  }

  auto partition_id = __grin_get_master_partition_id(_g, _v->id, _v->type_id);

  // one partition
  if (_vl->partition_type == ONE_PARTITION) {
    if (partition_id != _vl->partition_id)
      return GRIN_NULL_SIZE;  // not in this vertex list

    size_t offset = 0;
    // previous vertex types
    for (auto i = _vl->type_begin; i < _v->type_id; i++) {
      offset += _g->partitioned_vertex_num[i][_vl->partition_id];
    }
    // in the same vertex type
    offset += __grin_get_partitioned_vertex_id_from_vertex_id(
        _g, _v->type_id, _vl->partition_id, _g->partition_strategy, _v->id);
    return offset;
  }

  // all but one partition
  if (_vl->partition_type == ALL_BUT_ONE_PARTITION) {
    if (partition_id == _vl->partition_id)
      return GRIN_NULL_SIZE;  // not in this vertex list

    size_t offset = 0;
    // previous vertex types
    for (auto i = _vl->type_begin; i < _v->type_id; i++) {
      offset += _g->vertex_offsets[i + 1] - _g->vertex_offsets[i];
      offset -= _g->partitioned_vertex_num[i][_vl->partition_id];
    }
    // previous partitions of the same vertex type
    for (auto j = 0; j < partition_id; j++) {
      if (j == _vl->partition_id)
        continue;  // skip the partition
      offset += _g->partitioned_vertex_num[_v->type_id][j];
    }
    // in the same partition of the same vertex type
    offset += __grin_get_partitioned_vertex_id_from_vertex_id(
        _g, _v->type_id, partition_id, _g->partition_strategy, _v->id);
    return offset;
  }

  return GRIN_NULL_SIZE;  // undefined
}
#endif
