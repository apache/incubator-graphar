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

#include <regex>  // NOLINT
#include <sstream>

#include "grin/src/predefine.h"
// GRIN headers
#include "partition/partition.h"

#ifdef GRIN_ENABLE_GRAPH_PARTITION
GRIN_PARTITIONED_GRAPH grin_get_partitioned_graph_from_storage(
    const char* uri) {
  if (uri == NULL)
    return GRIN_NULL_PARTITIONED_GRAPH;
  std::string input(uri);
  std::regex re(R"(graphar://([^?]+)\?partition_num=(\d+)&strategy=(\w+))");
  std::smatch match;
  if (std::regex_search(input, match, re)) {
    std::string path = match[1];
    unsigned partition_num = std::stoi(match[2]);
    std::string strategy = match[3];
    auto strategy_type = PARTITION_STRATEGY_MAX;
    if (strategy == "segmented")
      strategy_type = SEGMENTED_PARTITION;
    else if (strategy == "hash")
      strategy_type = HASH_PARTITION;
    else
      return GRIN_NULL_PARTITIONED_GRAPH;
    return new GRIN_PARTITIONED_GRAPH_T(path, partition_num, strategy_type);
  } else {
    return GRIN_NULL_PARTITIONED_GRAPH;
  }
}

void grin_destroy_partitioned_graph(GRIN_PARTITIONED_GRAPH pg) {
  auto _pg = static_cast<GRIN_PARTITIONED_GRAPH_T*>(pg);
  delete _pg;
}

size_t grin_get_total_partitions_number(GRIN_PARTITIONED_GRAPH pg) {
  auto _pg = static_cast<GRIN_PARTITIONED_GRAPH_T*>(pg);
  return _pg->partition_num;
}

GRIN_PARTITION_LIST grin_get_local_partition_list(GRIN_PARTITIONED_GRAPH pg) {
  auto _pg = static_cast<GRIN_PARTITIONED_GRAPH_T*>(pg);
  auto pl = new GRIN_PARTITION_LIST_T();
  for (size_t i = 0; i < _pg->partition_num; ++i) {
    pl->push_back(i);
  }
  return pl;
}

void grin_destroy_partition_list(GRIN_PARTITIONED_GRAPH pg,
                                 GRIN_PARTITION_LIST pl) {
  auto _pl = static_cast<GRIN_PARTITION_LIST_T*>(pl);
  delete _pl;
}

GRIN_PARTITION_LIST grin_create_partition_list(GRIN_PARTITIONED_GRAPH pg) {
  return new GRIN_PARTITION_LIST_T();
}

bool grin_insert_partition_to_list(GRIN_PARTITIONED_GRAPH pg,
                                   GRIN_PARTITION_LIST pl, GRIN_PARTITION p) {
  auto _pl = static_cast<GRIN_PARTITION_LIST_T*>(pl);
  _pl->push_back(p);
  return true;
}

size_t grin_get_partition_list_size(GRIN_PARTITIONED_GRAPH pg,
                                    GRIN_PARTITION_LIST pl) {
  auto _pl = static_cast<GRIN_PARTITION_LIST_T*>(pl);
  return _pl->size();
}

GRIN_PARTITION grin_get_partition_from_list(GRIN_PARTITIONED_GRAPH pg,
                                            GRIN_PARTITION_LIST pl,
                                            size_t index) {
  auto _pl = static_cast<GRIN_PARTITION_LIST_T*>(pl);
  return (*_pl)[index];
}

bool grin_equal_partition(GRIN_PARTITIONED_GRAPH pg, GRIN_PARTITION p1,
                          GRIN_PARTITION p2) {
  return p1 == p2;
}

void grin_destroy_partition(GRIN_PARTITIONED_GRAPH pg, GRIN_PARTITION p) {
  return;
}

const void* grin_get_partition_info(GRIN_PARTITIONED_GRAPH pg,
                                    GRIN_PARTITION p) {
  std::stringstream ss;
  auto _pg = static_cast<GRIN_PARTITIONED_GRAPH_T*>(pg);
  ss << "partition_strategy: " << _pg->partition_strategy;
  ss << "; partition: " << p;
  int len = ss.str().length() + 1;
  char* out = new char[len];
  snprintf(out, len, "%s", ss.str().c_str());
  return out;
}

GRIN_GRAPH grin_get_local_graph_by_partition(GRIN_PARTITIONED_GRAPH pg,
                                             GRIN_PARTITION p) {
  auto _pg = static_cast<GRIN_PARTITIONED_GRAPH_T*>(pg);
  if (p >= _pg->partition_num)
    return GRIN_NULL_GRAPH;
  auto graph = get_graph_by_info_path(_pg->info_path);
  if (graph != NULL) {
    __grin_init_graph_partitions(graph, _pg->partition_num, p,
                                 _pg->partition_strategy);
  }
  return graph;
}
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_PARTITION
GRIN_PARTITION grin_get_partition_by_id(GRIN_PARTITIONED_GRAPH g,
                                        GRIN_PARTITION_ID id) {
  return id;
}

GRIN_PARTITION_ID grin_get_partition_id(GRIN_PARTITIONED_GRAPH g,
                                        GRIN_PARTITION p) {
  return p;
}
#endif
