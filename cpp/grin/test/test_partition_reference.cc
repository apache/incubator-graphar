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
#include "grin/include/partition/reference.h"
#include "grin/include/topology/structure.h"
#include "grin/include/topology/vertexlist.h"
}

void test_vertex_ref(GRIN_PARTITIONED_GRAPH pg, GRIN_GRAPH graph,
                     GRIN_VERTEX v) {
  std::cout << "\n== test vertex ref ==" << std::endl;

  // check vertex ref
  auto vr = grin_get_vertex_ref_by_vertex(graph, v);
  auto v_from_vr = grin_get_vertex_from_vertex_ref(graph, vr);
  assert(grin_equal_vertex(graph, v, v_from_vr) == true);

  // serialize & deserialize vertex ref
  auto msg = grin_serialize_vertex_ref(graph, vr);
  std::cout << "serialized vertex ref = " << msg << std::endl;
  auto vr_from_msg = grin_deserialize_to_vertex_ref(graph, msg);
  auto v_from_vr_from_msg = grin_get_vertex_from_vertex_ref(graph, vr_from_msg);
  assert(grin_equal_vertex(graph, v, v_from_vr_from_msg) == true);

  // serialize & deserialize vertex ref as int64
  auto int64_msg = grin_serialize_vertex_ref_as_int64(graph, vr);
  std::cout << "serialized vertex ref as int64 = " << int64_msg << std::endl;
  auto vr_from_int64_msg =
      grin_deserialize_int64_to_vertex_ref(graph, int64_msg);
  auto v_from_vr_from_int64_msg =
      grin_get_vertex_from_vertex_ref(graph, vr_from_int64_msg);
  assert(grin_equal_vertex(graph, v, v_from_vr_from_int64_msg) == true);

  // check master or mirror
  auto is_master = grin_is_master_vertex(graph, v);
  auto is_mirror = grin_is_mirror_vertex(graph, v);
  assert((is_master || is_mirror) && !(is_master && is_mirror));
  if (is_master) {
    std::cout << "vertex is master, ";
  } else {
    std::cout << "vertex is mirror, ";
  }
  // get master partition id
  auto master_partition = grin_get_master_partition_from_vertex_ref(graph, vr);
  auto master_partition_id = grin_get_partition_id(pg, master_partition);
  std::cout << "master partition id = " << master_partition_id << std::endl;
  // get mirror_partition_list
  if (is_master) {
    assert(grin_get_mirror_vertex_mirror_partition_list(graph, v) ==
           GRIN_NULL_LIST);
    auto partition_list =
        grin_get_master_vertex_mirror_partition_list(graph, v);

    std::cout << "mirror partition ids = ";
    auto partition_list_size = grin_get_partition_list_size(pg, partition_list);
    for (auto i = 0; i < partition_list_size; ++i) {
      auto partition = grin_get_partition_from_list(pg, partition_list, i);
      auto partition_id = grin_get_partition_id(pg, partition);
      std::cout << " " << partition_id;
      grin_destroy_partition(pg, partition);
    }
    std::cout << std::endl;

    grin_destroy_partition_list(pg, partition_list);

  } else {
    assert(grin_get_master_vertex_mirror_partition_list(graph, v) ==
           GRIN_NULL_LIST);
    auto partition_list =
        grin_get_mirror_vertex_mirror_partition_list(graph, v);

    std::cout << "mirror partition ids = ";
    auto partition_list_size = grin_get_partition_list_size(pg, partition_list);
    for (auto i = 0; i < partition_list_size; ++i) {
      auto partition = grin_get_partition_from_list(pg, partition_list, i);
      auto partition_id = grin_get_partition_id(pg, partition);
      std::cout << " " << partition_id;
      grin_destroy_partition(pg, partition);
    }
    std::cout << std::endl;

    grin_destroy_partition_list(pg, partition_list);
  }

  // destroy
  grin_destroy_partition(graph, master_partition);
  grin_destroy_vertex(graph, v_from_vr);
  grin_destroy_vertex(graph, v_from_vr_from_msg);
  grin_destroy_vertex(graph, v_from_vr_from_int64_msg);
  grin_destroy_vertex_ref(graph, vr);
  grin_destroy_vertex_ref(graph, vr_from_msg);
  grin_destroy_vertex_ref(graph, vr_from_int64_msg);
  grin_destroy_serialized_vertex_ref(graph, msg);
}

void test_partition_reference(GRIN_PARTITIONED_GRAPH pg, unsigned n) {
  std::cout << "\n++++ test partition: reference ++++" << std::endl;

  // check partition number
  assert(pg != GRIN_NULL_GRAPH);
  auto partition_num = grin_get_total_partitions_number(pg);
  assert(partition_num == n);

  // create a local graph
  auto partition0 = grin_get_partition_by_id(pg, 0);
  auto graph = grin_get_local_graph_by_partition(pg, partition0);

  auto vertex_list = grin_get_vertex_list(graph);
  // get vertex 0 & test
  if (grin_get_vertex_list_size(graph, vertex_list) > 0) {
    auto v = grin_get_vertex_from_list(graph, vertex_list, 0);
    test_vertex_ref(pg, graph, v);
    grin_destroy_vertex(graph, v);
  }
  // get vertex 150000 & test
  if (grin_get_vertex_list_size(graph, vertex_list) > 150000) {
    auto v = grin_get_vertex_from_list(graph, vertex_list, 150000);
    test_vertex_ref(pg, graph, v);
    grin_destroy_vertex(graph, v);
  }

  // destroy
  grin_destroy_partition(graph, partition0);
  grin_destroy_graph(graph);
  grin_destroy_vertex_list(graph, vertex_list);

  std::cout << "---- test partition: reference completed ----" << std::endl;
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
  snprintf(args[1], 2, "%d", partition_num);
  GRIN_PARTITIONED_GRAPH pg = grin_get_partitioned_graph_from_storage(2, args);

  // test partitioned graph
  test_partition_reference(pg, partition_num);

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
  snprintf(args2[1], 2, "%d", partition_num);
  args2[2] = new char[2];
  uint32_t strategy = 1;
  snprintf(args2[2], 2, "%d", strategy);
  GRIN_PARTITIONED_GRAPH pg2 =
      grin_get_partitioned_graph_from_storage(3, args2);

  // test partitioned graph
  test_partition_reference(pg2, partition_num);

  // destroy partitioned graph
  grin_destroy_partitioned_graph(pg);
  grin_destroy_partitioned_graph(pg2);

  return 0;
}
