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

#include <iostream>

#include "grin/test/config.h"

extern "C" {
#include "grin/include/partition/partition.h"
#include "grin/include/topology/structure.h"
}

void test_partition_partition(GRIN_PARTITIONED_GRAPH pg, unsigned n) {
  std::cout << "\n++++ test partition: partition ++++" << std::endl;

  // check partition number
  assert(pg != GRIN_NULL_GRAPH);
  auto partition_num = grin_get_total_partitions_number(pg);
  assert(partition_num == n);

  // check partition list
  auto partition_list = grin_get_local_partition_list(pg);
  assert(partition_list != GRIN_NULL_LIST);
  auto partition_list_size = grin_get_partition_list_size(pg, partition_list);
  assert(partition_list_size == n);

  // check create new partition list
  auto new_partition_list = grin_create_partition_list(pg);
  assert(new_partition_list != GRIN_NULL_LIST);
  for (auto i = 0; i < partition_list_size; ++i) {
    // get & insert partition
    auto partition = grin_get_partition_from_list(pg, partition_list, i);
    assert(grin_insert_partition_to_list(pg, new_partition_list, partition) ==
           true);
    // check & destroy partition
    auto partition_from_new_list =
        grin_get_partition_from_list(pg, new_partition_list, i);
    assert(grin_equal_partition(pg, partition, partition_from_new_list) ==
           true);
    grin_destroy_partition(pg, partition);
    grin_destroy_partition(pg, partition_from_new_list);
  }
  assert(grin_get_partition_list_size(pg, new_partition_list) ==
         partition_list_size);
  grin_destroy_partition_list(pg, new_partition_list);

  // check partition id
  auto partition_a = grin_get_partition_from_list(pg, partition_list, 0);
  auto id = grin_get_partition_id(pg, partition_a);
  auto partition_b = grin_get_partition_by_id(pg, id);
  assert(grin_equal_partition(pg, partition_a, partition_b) == true);
  grin_destroy_partition(pg, partition_a);
  grin_destroy_partition(pg, partition_b);

  // check get local graph
  for (auto i = 0; i < partition_list_size; ++i) {
    // get local graph from partition
    auto partition = grin_get_partition_from_list(pg, partition_list, i);
    auto info = grin_get_partition_info(pg, partition);
    std::cout << "Partition " << static_cast<const char*>(info) << std::endl;
    auto graph = grin_get_local_graph_by_partition(pg, partition);
    // check information of local graph
    assert(graph != GRIN_NULL_GRAPH);
    auto vertex_num = grin_get_vertex_num(graph);
    auto edge_num = grin_get_edge_num(graph);
    std::cout << "- vertex num = " << vertex_num << std::endl;
    std::cout << "- edge num = " << edge_num << std::endl;
    // destroy
    grin_destroy_partition(pg, partition);
    grin_destroy_graph(graph);
  }

  // destroy
  grin_destroy_partition_list(pg, partition_list);
  std::cout << "---- test partition: partition completed ----" << std::endl;
}

int main(int argc, char* argv[]) {
  // partition number = 1, stragey = segmented
  std::string path = TEST_DATA_PATH;
  uint32_t partition_num = 1;
  std::cout << "GraphInfo path = " << path << std::endl;
  std::cout << "Partition strategy = segmented" << std::endl;
  std::cout << "Partition number = " << partition_num << std::endl;

  // get partitioned graph from graph info of GraphAr
  char** args = new char*[1];
  args[0] = new char[path.length() + 1];
  snprintf(args[0], path.length() + 1, "%s", path.c_str());
  GRIN_PARTITIONED_GRAPH pg = grin_get_partitioned_graph_from_storage(1, args);

  // test partitioned graph
  test_partition_partition(pg, partition_num);

  // partition number = 4, stragety = hash
  partition_num = 4;
  std::cout << std::endl;
  std::cout << "GraphInfo path = " << path << std::endl;
  std::cout << "Partition strategy = hash" << std::endl;
  std::cout << "Partition number = " << partition_num << std::endl;

  // get partitioned graph from graph info of GraphAr
  char** args2 = new char*[3];
  args2[0] = new char[path.length() + 1];
  snprintf(args2[0], path.length() + 1, "%s", path.c_str());
  args2[1] = new char[2];
  snprintf(args2[1], 2, "%d", partition_num);
  args2[2] = new char[2];
  uint32_t strategy = 1;
  snprintf(args2[2], 2, "%d", strategy);
  GRIN_PARTITIONED_GRAPH pg2 = grin_get_partitioned_graph_from_storage(3, args2);

  // test partitioned graph
  test_partition_partition(pg2, partition_num);

  // destroy partitioned graph
  grin_destroy_partitioned_graph(pg);
  grin_destroy_partitioned_graph(pg2);

  return 0;
}
