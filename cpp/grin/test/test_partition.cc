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

#include "grin/predefine.h"
#include "grin/test/config.h"

// GRIN headers
#include "partition/partition.h"
#include "topology/structure.h"

void test_partition_partition(GRIN_PARTITIONED_GRAPH pg, unsigned n) {
  std::cout << "\n++++ test partition: partition ++++" << std::endl;

  // check partition number
  ASSERT(pg != GRIN_NULL_PARTITIONED_GRAPH);
  auto partition_num = grin_get_total_partitions_number(pg);
  ASSERT(partition_num == n);

  // check partition list
  auto partition_list = grin_get_local_partition_list(pg);
  ASSERT(partition_list != GRIN_NULL_PARTITION_LIST);
  auto partition_list_size = grin_get_partition_list_size(pg, partition_list);
  ASSERT(partition_list_size == n);

  // check create new partition list
  auto new_partition_list = grin_create_partition_list(pg);
  ASSERT(new_partition_list != GRIN_NULL_PARTITION_LIST);
  for (auto i = 0; i < partition_list_size; ++i) {
    // get & insert partition
    auto partition = grin_get_partition_from_list(pg, partition_list, i);
    auto status =
        grin_insert_partition_to_list(pg, new_partition_list, partition);
    ASSERT(status == true);
    // check & destroy partition
    auto partition_from_new_list =
        grin_get_partition_from_list(pg, new_partition_list, i);
    ASSERT(grin_equal_partition(pg, partition, partition_from_new_list) ==
           true);
    grin_destroy_partition(pg, partition);
    grin_destroy_partition(pg, partition_from_new_list);
  }
  ASSERT(grin_get_partition_list_size(pg, new_partition_list) ==
         partition_list_size);
  grin_destroy_partition_list(pg, new_partition_list);

  // check partition id
  auto partition_a = grin_get_partition_from_list(pg, partition_list, 0);
  auto id = grin_get_partition_id(pg, partition_a);
  auto partition_b = grin_get_partition_by_id(pg, id);
  ASSERT(grin_equal_partition(pg, partition_a, partition_b) == true);
  grin_destroy_partition(pg, partition_a);
  grin_destroy_partition(pg, partition_b);

  // check get local graph
  for (auto i = 0; i < partition_list_size; ++i) {
    // get local graph from partition
    auto partition = grin_get_partition_from_list(pg, partition_list, i);
    auto info = grin_get_partition_info(pg, partition);
    std::cout << "Partition " << static_cast<const char*>(info) << std::endl;
    auto graph = grin_get_local_graph_by_partition(pg, partition);
    ASSERT(graph != GRIN_NULL_GRAPH);
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
  std::string partitioned_path =
      path + ":" + std::to_string(partition_num) + ":" + "segmented";
  char* id = new char[partitioned_path.length() + 1];
  snprintf(id, partitioned_path.length() + 1, "%s", partitioned_path.c_str());
  GRIN_PARTITIONED_GRAPH pg = grin_get_partitioned_graph_from_storage(id, NULL);
  delete[] id;

  // test partitioned graph
  test_partition_partition(pg, partition_num);

  // partition number = 4, stragety = hash
  partition_num = 4;
  std::cout << std::endl;
  std::cout << "GraphInfo path = " << path << std::endl;
  std::cout << "Partition strategy = hash" << std::endl;
  std::cout << "Partition number = " << partition_num << std::endl;

  // get partitioned graph from graph info of GraphAr
  std::string partitioned_path2 =
      path + ":" + std::to_string(partition_num) + ":" + "hash";
  char* id2 = new char[partitioned_path2.length() + 1];
  snprintf(id2, partitioned_path2.length() + 1, "%s",
           partitioned_path2.c_str());
  GRIN_PARTITIONED_GRAPH pg2 =
      grin_get_partitioned_graph_from_storage(id2, NULL);
  delete[] id2;

  // test partitioned graph
  test_partition_partition(pg2, partition_num);

  // destroy partitioned graph
  grin_destroy_partitioned_graph(pg);
  grin_destroy_partitioned_graph(pg2);

  return 0;
}
