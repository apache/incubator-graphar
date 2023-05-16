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
#include "grin/include/index/order.h"
#include "grin/include/index/original_id.h"
#include "grin/include/partition/partition.h"
#include "grin/include/partition/reference.h"
#include "grin/include/partition/topology.h"
#include "grin/include/property/topology.h"
#include "grin/include/property/type.h"
#include "grin/include/topology/structure.h"
#include "grin/include/topology/vertexlist.h"
}

void test_vertex_list(GRIN_GRAPH g, GRIN_VERTEX_LIST vl) {
  // check vertex list
  auto vl_size = grin_get_vertex_list_size(g, vl);
  auto vl_iter = grin_get_vertex_list_begin(g, vl);
  for (auto i = 0; i < vl_size; ++i) {
    auto v0 = grin_get_vertex_from_list(g, vl, i);
    assert(grin_is_vertex_list_end(g, vl_iter) == false);
    auto v1 = grin_get_vertex_from_iter(g, vl_iter);
    assert(grin_equal_vertex(g, v0, v1) == true);
    grin_get_next_vertex_list_iter(g, vl_iter);
    // destroy vertex
    grin_destroy_vertex(g, v0);
    grin_destroy_vertex(g, v1);
  }
  assert(grin_is_vertex_list_end(g, vl_iter) == true);
  // destroy vertex list iter
  grin_destroy_vertex_list_iter(g, vl_iter);

  // test vertex order in list
  if (vl_size > 200) {
    size_t idx0 = 100, idx1 = 200;
    auto v0 = grin_get_vertex_from_list(g, vl, idx0);
    auto v1 = grin_get_vertex_from_list(g, vl, idx1);
    assert(grin_smaller_vertex(g, v0, v1) == true);
    assert(grin_smaller_vertex(g, v1, v0) == false);
    size_t pos0 = grin_get_position_of_vertex_from_sorted_list(g, vl, v0);
    size_t pos1 = grin_get_position_of_vertex_from_sorted_list(g, vl, v1);
    assert(pos0 == idx0);
    assert(pos1 == idx1);
    // destroy vertex
    grin_destroy_vertex(g, v0);
    grin_destroy_vertex(g, v1);
  }

  std::cout << "(Correct) check vertex list succeed" << std::endl;
}

void test_select_type_for_vertex_list(GRIN_GRAPH g, GRIN_VERTEX_LIST vl) {
  // select vertex type
  auto type = grin_get_vertex_type_by_name(g, "person");
  auto select_vertex_list = grin_select_type_for_vertex_list(g, type, vl);
  auto select_vertex_list_size =
      grin_get_vertex_list_size(g, select_vertex_list);
  std::cout << "select vertex type \"person\", size = "
            << select_vertex_list_size << std::endl;

  // check vertex list
  test_vertex_list(g, select_vertex_list);

  // destroy
  grin_destroy_vertex_list(g, select_vertex_list);
  grin_destroy_vertex_type(g, type);
}

void test_partition_topology(GRIN_PARTITIONED_GRAPH pg, unsigned n) {
  std::cout << "\n++++ test partition: topology ++++" << std::endl;

  // check partition number
  assert(pg != GRIN_NULL_GRAPH);
  auto partition_num = grin_get_total_partitions_number(pg);
  assert(partition_num == n);

  for (auto partition_id = 0; partition_id < n; ++partition_id) {
    std::cout << "\n== test partition " << partition_id << " ==" << std::endl;
    // create a local graph
    auto partition = grin_get_partition_by_id(pg, partition_id);
    auto graph = grin_get_local_graph_by_partition(pg, partition);

    // get vertex list
    auto vertex_list = grin_get_vertex_list(graph);
    auto vertex_list_size = grin_get_vertex_list_size(graph, vertex_list);
    std::cout << "complete vertex list size = " << vertex_list_size
              << std::endl;

    // select master
    auto master_vertex_list =
        grin_select_master_for_vertex_list(graph, vertex_list);
    auto master_vertex_list_size =
        grin_get_vertex_list_size(graph, master_vertex_list);
    std::cout << "master vertex list size = " << master_vertex_list_size
              << std::endl;
    if (partition_id == 0) {
      test_vertex_list(graph, master_vertex_list);
    }
    test_select_type_for_vertex_list(graph, master_vertex_list);

    // select mirror
    auto mirror_vertex_list =
        grin_select_mirror_for_vertex_list(graph, vertex_list);
    auto mirror_vertex_list_size =
        grin_get_vertex_list_size(graph, mirror_vertex_list);
    std::cout << "mirror vertex list size = " << mirror_vertex_list_size
              << std::endl;
    if (partition_id == 1) {
      test_vertex_list(graph, mirror_vertex_list);
    }
    test_select_type_for_vertex_list(graph, mirror_vertex_list);

    // check vertex number
    assert(vertex_list_size ==
           master_vertex_list_size + mirror_vertex_list_size);

    // select by partition
    auto partition0 = grin_get_partition_by_id(pg, 0);
    auto vertex_list0 =
        grin_select_partition_for_vertex_list(graph, partition0, vertex_list);
    auto vertex_list0_size = grin_get_vertex_list_size(graph, vertex_list0);
    std::cout << "vertex list size of partition 0 = " << vertex_list0_size
              << std::endl;
    if (partition_id == 2) {
      test_vertex_list(graph, vertex_list0);
    }
    test_select_type_for_vertex_list(graph, vertex_list0);

    // invalid operations
    if (partition_id == 0) {
      assert(grin_select_mirror_for_vertex_list(graph, vertex_list0) ==
             GRIN_NULL_LIST);
      assert(grin_select_partition_for_vertex_list(
                 graph, partition0, mirror_vertex_list) == GRIN_NULL_LIST);
    } else {
      assert(grin_select_master_for_vertex_list(graph, vertex_list0) ==
             GRIN_NULL_LIST);
      assert(grin_select_partition_for_vertex_list(
                 graph, partition0, master_vertex_list) == GRIN_NULL_LIST);
    }

    // destroy
    grin_destroy_partition(graph, partition);
    grin_destroy_partition(graph, partition0);
    grin_destroy_graph(graph);
    grin_destroy_vertex_list(graph, vertex_list);
    grin_destroy_vertex_list(graph, master_vertex_list);
  }

  std::cout << "---- test partition: topology completed ----" << std::endl;
}

int main(int argc, char* argv[]) {
  // partition number = 4, stragey = segmented
  std::string path = TEST_DATA_PATH;
  uint32_t partition_num = 4;
  std::cout << "GraphInfo path = " << path << std::endl;
  std::cout << "Partition strategy = segmented" << std::endl;
  std::cout << "Partition number = " << partition_num << std::endl;

  // get partitioned graph from graph info of GraphAr
  char** args = new char*[2];
  args[0] = new char[path.length() + 1];
  snprintf(args[0], path.length() + 1, "%s", path.c_str());
  args[1] = new char[2];
  snprintf(args[1], sizeof(args[1]), "%d", partition_num);
  GRIN_PARTITIONED_GRAPH pg = grin_get_partitioned_graph_from_storage(2, args);

  // test partitioned graph
  test_partition_topology(pg, partition_num);

  // partition number = 2, stragety = hash
  partition_num = 2;
  std::cout << std::endl;
  std::cout << "GraphInfo path = " << path << std::endl;
  std::cout << "Partition strategy = hash" << std::endl;
  std::cout << "Partition number = " << partition_num << std::endl;

  // get partitioned graph from graph info of GraphAr
  char** args2 = new char*[3];
  args2[0] = new char[path.length() + 1];
  snprintf(args2[0], path.length() + 1, "%s", path.c_str());
  args2[1] = new char[2];
  snprintf(args2[1], sizeof(args2[1]), "%d", partition_num);
  args2[2] = new char[2];
  uint32_t strategy = 1;
  snprintf(args2[2], sizeof(args2[2]), "%d", strategy);
  GRIN_PARTITIONED_GRAPH pg2 =
      grin_get_partitioned_graph_from_storage(3, args2);

  // test partitioned graph
  test_partition_topology(pg2, partition_num);

  // destroy partitioned graph
  grin_destroy_partitioned_graph(pg);
  grin_destroy_partitioned_graph(pg2);

  return 0;
}
