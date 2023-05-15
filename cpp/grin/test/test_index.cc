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
#include "grin/include/topology/structure.h"
#include "grin/include/topology/vertexlist.h"
}

void test_index_order(GRIN_GRAPH graph) {
  std::cout << "\n++++ test index: order ++++" << std::endl;

  std::cout << "test vertex order" << std::endl;
  auto vertex_list = grin_get_vertex_list(graph);
  size_t idx0 = 0, idx1 = 1;
  auto v0 = grin_get_vertex_from_list(graph, vertex_list, idx0);
  auto v1 = grin_get_vertex_from_list(graph, vertex_list, idx1);
  assert(grin_smaller_vertex(graph, v0, v1) == true);
  assert(grin_smaller_vertex(graph, v1, v0) == false);

  std::cout << "test get position of vertex from sorted list" << std::endl;
  size_t pos0 =
      grin_get_position_of_vertex_from_sorted_list(graph, vertex_list, v0);
  size_t pos1 =
      grin_get_position_of_vertex_from_sorted_list(graph, vertex_list, v1);
  assert(pos0 == idx0);
  assert(pos1 == idx1);

  // destroy
  grin_destroy_vertex(graph, v0);
  grin_destroy_vertex(graph, v1);
  grin_destroy_vertex_list(graph, vertex_list);

  std::cout << "---- test index: order completed ----" << std::endl;
}

void test_original_id(GRIN_GRAPH graph) {
  std::cout << "\n++++ test index: original id ++++" << std::endl;

  std::cout << "test vertex original id" << std::endl;
  auto vertex_list = grin_get_vertex_list(graph);
  size_t idx0 = 0, idx1 = 1;
  auto v0 = grin_get_vertex_from_list(graph, vertex_list, idx0);
  auto v1 = grin_get_vertex_from_list(graph, vertex_list, idx1);

  // orignal id datatype
  auto type = grin_get_vertex_original_id_datatype(graph);
  assert(type == GRIN_DATATYPE::Int64);

  // get original id of vertex
  auto oid0 = grin_get_vertex_original_id_of_int64(graph, v0);
  auto oid1 = grin_get_vertex_original_id_of_int64(graph, v1);
  std::cout << "original id of v0 = " << oid0 << std::endl;
  std::cout << "original id of v1 = " << oid1 << std::endl;

  // get vertex by original id
  auto v0_from_oid = grin_get_vertex_by_original_id_of_int64(graph, oid0);
  auto v1_from_oid = grin_get_vertex_by_original_id_of_int64(graph, oid1);
  assert(grin_equal_vertex(graph, v0, v0_from_oid) == true);
  assert(grin_equal_vertex(graph, v1, v1_from_oid) == true);

  // destroy
  grin_destroy_vertex(graph, v0);
  grin_destroy_vertex(graph, v1);
  grin_destroy_vertex(graph, v0_from_oid);
  grin_destroy_vertex(graph, v1_from_oid);
  grin_destroy_vertex_list(graph, vertex_list);

  std::cout << "---- test index: original id completed ----" << std::endl;
}

int main(int argc, char* argv[]) {
  // get graph from graph info of GraphAr
  std::string path = TEST_DATA_PATH;
  std::cout << "GraphInfo path = " << path << std::endl;

  char** args = new char*[1];
  args[0] = new char[path.length() + 1];
  snprintf(args[0], path.length() + 1, "%s", path.c_str());
  GRIN_GRAPH graph = grin_get_graph_from_storage(1, args);

  // test index order
  test_index_order(graph);

  // test original id
  test_original_id(graph);

  // destroy graph
  grin_destroy_graph(graph);

  return 0;
}
