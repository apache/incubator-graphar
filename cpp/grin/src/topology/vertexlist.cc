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
#include "topology/vertexlist.h"
}

#if defined(GRIN_ENABLE_VERTEX_LIST) && !defined(GRIN_WITH_VERTEX_PROPERTY)
GRIN_VERTEX_LIST grin_get_vertex_list(GRIN_GRAPH);
#endif

#ifdef GRIN_ENABLE_VERTEX_LIST
void grin_destroy_vertex_list(GRIN_GRAPH g, GRIN_VERTEX_LIST vl) {
  auto _vl = static_cast<GRIN_VERTEX_LIST_T*>(vl);
  delete _vl;
}
#endif

#ifdef GRIN_ENABLE_VERTEX_LIST_ARRAY
size_t grin_get_vertex_list_size(GRIN_GRAPH g, GRIN_VERTEX_LIST vl) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vl = static_cast<GRIN_VERTEX_LIST_T*>(vl);
  auto vtype = _vl->type_id;

  // all partition
  if (_vl->partition_type == ALL_PARTITION) {
    return _g->vertex_offsets[vtype + 1] - _g->vertex_offsets[vtype];
  }

  // one partition
  if (_vl->partition_type == ONE_PARTITION) {
    return _g->partitioned_vertex_num[vtype][_vl->partition_id];
  }

  // all but one partition
  if (_vl->partition_type == ALL_BUT_ONE_PARTITION) {
    auto tot_size = _g->vertex_offsets[vtype + 1] - _g->vertex_offsets[vtype];
    tot_size -= _g->partitioned_vertex_num[vtype][_vl->partition_id];
    return tot_size;
  }

  return 0;  // undefined
}

GRIN_VERTEX grin_get_vertex_from_list(GRIN_GRAPH g, GRIN_VERTEX_LIST vl,
                                      size_t idx) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vl = static_cast<GRIN_VERTEX_LIST_T*>(vl);
  auto vtype = _vl->type_id;

  // all partition
  if (_vl->partition_type == ALL_PARTITION) {
    if (idx >= _g->vertex_offsets[vtype + 1] - _g->vertex_offsets[vtype])
      return GRIN_NULL_VERTEX;
    auto v = new GRIN_VERTEX_T(idx, vtype);
    return v;
  }

  // one partition
  if (_vl->partition_type == ONE_PARTITION) {
    auto partition_id = _vl->partition_id;
    auto partitioned_vertex_num =
        _g->partitioned_vertex_num[vtype][partition_id];
    if (idx >= partitioned_vertex_num)
      return GRIN_NULL_VERTEX;
    auto _idx = __grin_get_vertex_id_from_partitioned_vertex_id(
        _g, vtype, partition_id, _g->partition_strategy, idx);
    auto v = new GRIN_VERTEX_T(_idx, vtype);
    return v;
  }

  // all but one partition
  if (_vl->partition_type == ALL_BUT_ONE_PARTITION) {
    auto partition_id = _vl->partition_id;
    auto partitioned_vertex_num =
        _g->vertex_offsets[vtype + 1] - _g->vertex_offsets[vtype];
    partitioned_vertex_num -= _g->partitioned_vertex_num[vtype][partition_id];
    if (idx >= partitioned_vertex_num)
      return GRIN_NULL_VERTEX;

    int l = 0, r = _g->partition_num - 1;
    while (l <= r) {
      int mid = (l + r) >> 1;
      int pre_num = _g->partitioned_vertex_offsets[vtype][mid];
      if (mid > partition_id)
        pre_num -= _g->partitioned_vertex_num[vtype][partition_id];
      auto cur_num =
          (mid == partition_id ? 0 : _g->partitioned_vertex_num[vtype][mid]);
      // in this partition
      if (idx >= pre_num && idx < pre_num + cur_num) {
        auto _idx = __grin_get_vertex_id_from_partitioned_vertex_id(
            _g, vtype, mid, _g->partition_strategy, idx - pre_num);
        auto v = new GRIN_VERTEX_T(_idx, vtype);
        return v;
      } else if (idx < pre_num) {
        r = mid - 1;
      } else {
        l = mid + 1;
      }
    }
    return GRIN_NULL_VERTEX;
  }

  return GRIN_NULL_VERTEX;  // undefined
}
#endif

#ifdef GRIN_ENABLE_VERTEX_LIST_ITERATOR
GRIN_VERTEX_LIST_ITERATOR grin_get_vertex_list_begin(GRIN_GRAPH g,
                                                     GRIN_VERTEX_LIST vl) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vl = static_cast<GRIN_VERTEX_LIST_T*>(vl);
  auto vtype = _vl->type_id;
  if (vtype >= _g->vertex_type_num)
    return GRIN_NULL_LIST_ITERATOR;

  // all partition
  if (_vl->partition_type == ALL_PARTITION) {
    auto& vertices = _g->vertices_collections[vtype];
    auto vli = new GRIN_VERTEX_LIST_ITERATOR_T(
        vtype, _vl->partition_type, _vl->partition_id, 0, 0, vertices.begin());
    return vli;
  }

  // one partition
  if (_vl->partition_type == ONE_PARTITION) {
    // find first non-empty valid type & partition
    auto partition_id = _vl->partition_id;
    if (_g->partitioned_vertex_num[vtype][partition_id] == 0)
      return GRIN_NULL_LIST_ITERATOR;

    // find first vertex in this partition
    auto& vertices = _g->vertices_collections[vtype];
    auto idx = __grin_get_first_vertex_id_in_partition(_g, vtype, partition_id,
                                                       _g->partition_strategy);
    auto vli = new GRIN_VERTEX_LIST_ITERATOR_T(vtype, _vl->partition_type,
                                               _vl->partition_id, partition_id,
                                               idx, vertices.begin() + idx);
    return vli;
  }

  // all but one partition
  if (_vl->partition_type == ALL_BUT_ONE_PARTITION) {
    // find first non-empty valid type & partition
    auto partition_id = 0;
    while (partition_id < _g->partition_num) {
      if (partition_id == _vl->partition_id ||
          _g->partitioned_vertex_num[vtype][partition_id] == 0) {
        partition_id++;
      } else {
        break;
      }
    }
    if (partition_id == _g->partition_num)
      return GRIN_NULL_LIST_ITERATOR;
    // find first vertex in this partition
    auto& vertices = _g->vertices_collections[vtype];
    auto idx = __grin_get_first_vertex_id_in_partition(_g, vtype, partition_id,
                                                       _g->partition_strategy);
    auto vli = new GRIN_VERTEX_LIST_ITERATOR_T(vtype, _vl->partition_type,
                                               _vl->partition_id, partition_id,
                                               idx, vertices.begin() + idx);
    return vli;
  }

  return GRIN_NULL_LIST_ITERATOR;  // undefined
}

void grin_destroy_vertex_list_iter(GRIN_GRAPH g,
                                   GRIN_VERTEX_LIST_ITERATOR vli) {
  if (vli == GRIN_NULL_LIST_ITERATOR)
    return;
  auto _vli = static_cast<GRIN_VERTEX_LIST_ITERATOR_T*>(vli);
  delete _vli;
}

void grin_get_next_vertex_list_iter(GRIN_GRAPH g,
                                    GRIN_VERTEX_LIST_ITERATOR vli) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto _vli = static_cast<GRIN_VERTEX_LIST_ITERATOR_T*>(vli);

  // all partition
  if (_vli->partition_type == ALL_PARTITION) {
    ++_vli->current_offset;
    // end of the list
    if (_vli->current_offset >= _g->vertex_offsets[_vli->type_id + 1] -
                                    _g->vertex_offsets[_vli->type_id]) {
      _vli->current_offset = -1;
    } else {
      ++_vli->iter;
    }
    return;
  }

  // one partition
  if (_vli->partition_type == ONE_PARTITION) {
    auto idx = __grin_get_next_vertex_id_in_partition(
        _g, _vli->type_id, _vli->partition_id, _g->partition_strategy,
        _vli->current_offset);
    if (idx != -1) {  // next vertex in this partition
      _vli->iter += idx - _vli->current_offset;
      _vli->current_offset = idx;
    } else {  // end of the list
      _vli->current_offset = -1;
    }
    return;
  }

  // all but one partition
  if (_vli->partition_type == ALL_BUT_ONE_PARTITION) {
    auto partition_id = _vli->current_partition_id;
    auto idx = __grin_get_next_vertex_id_in_partition(
        _g, _vli->type_id, partition_id, _g->partition_strategy,
        _vli->current_offset);
    if (idx != -1) {  // next vertex in this partition
      _vli->iter += idx - _vli->current_offset;
      _vli->current_offset = idx;
    } else {
      // find next valid parititon in this type
      while (partition_id < _g->partition_num) {
        partition_id++;
        if (partition_id == _g->partition_num)
          break;
        if (partition_id == _vli->partition_id)
          continue;  // skip invalid partition
        if (_g->partitioned_vertex_num[_vli->type_id][partition_id] == 0)
          continue;  // skip empty partition
        break;
      }
      // next valid partition in this type exists
      if (partition_id < _g->partition_num) {
        auto idx = __grin_get_first_vertex_id_in_partition(
            _g, _vli->type_id, partition_id, _g->partition_strategy);
        _vli->iter += idx - _vli->current_offset;
        _vli->current_partition_id = partition_id;
        _vli->current_offset = idx;
      } else {  // end of the list
        _vli->current_offset = -1;
      }
    }
    return;
  }
}

bool grin_is_vertex_list_end(GRIN_GRAPH g, GRIN_VERTEX_LIST_ITERATOR vli) {
  if (vli == GRIN_NULL_LIST_ITERATOR)
    return true;
  auto _vli = static_cast<GRIN_VERTEX_LIST_ITERATOR_T*>(vli);
  return _vli->current_offset == -1;
}

GRIN_VERTEX grin_get_vertex_from_iter(GRIN_GRAPH g,
                                      GRIN_VERTEX_LIST_ITERATOR vli) {
  auto _vli = static_cast<GRIN_VERTEX_LIST_ITERATOR_T*>(vli);
  auto v = new GRIN_VERTEX_T(_vli->current_offset, _vli->type_id, *_vli->iter);
  return v;
}
#endif
