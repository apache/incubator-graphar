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
#include "index/internal_id.h"
#include "index/order.h"
#include "property/topology.h"
#include "property/type.h"
#include "topology/structure.h"
#include "topology/vertexlist.h"

void test_index_order(GRIN_GRAPH graph) {
  std::cout << "\n++++ test index: order ++++" << std::endl;

  std::cout << "test vertex order" << std::endl;
  auto vtype = grin_get_vertex_type_by_id(graph, 0);
  auto vertex_list = grin_get_vertex_list_by_type(graph, vtype);
  size_t idx0 = 0, idx1 = 1;
  auto v0 = grin_get_vertex_from_list(graph, vertex_list, idx0);
  auto v1 = grin_get_vertex_from_list(graph, vertex_list, idx1);
  ASSERT(grin_smaller_vertex(graph, v0, v1) == true);
  ASSERT(grin_smaller_vertex(graph, v1, v0) == false);

  std::cout << "test get position of vertex from sorted list" << std::endl;
  size_t pos0 =
      grin_get_position_of_vertex_from_sorted_list(graph, vertex_list, v0);
  size_t pos1 =
      grin_get_position_of_vertex_from_sorted_list(graph, vertex_list, v1);
  ASSERT(pos0 == idx0);
  ASSERT(pos1 == idx1);

  // destroy
  grin_destroy_vertex(graph, v0);
  grin_destroy_vertex(graph, v1);
  grin_destroy_vertex_list(graph, vertex_list);
  grin_destroy_vertex_type(graph, vtype);

  std::cout << "---- test index: order completed ----" << std::endl;
}

void test_internal_id(GRIN_GRAPH graph) {
  std::cout << "\n++++ test index: internal id ++++" << std::endl;

  std::cout << "test vertex internal id" << std::endl;
  auto vtype = grin_get_vertex_type_by_id(graph, 0);
  auto vertex_list = grin_get_vertex_list_by_type(graph, vtype);
  size_t idx0 = 0, idx1 = 1;
  auto v0 = grin_get_vertex_from_list(graph, vertex_list, idx0);
  auto v1 = grin_get_vertex_from_list(graph, vertex_list, idx1);

  // get internal id of vertex
  auto id0 = grin_get_vertex_internal_id_by_type(graph, vtype, v0);
  auto id1 = grin_get_vertex_internal_id_by_type(graph, vtype, v1);
  std::cout << "internal id of v0 = " << id0 << std::endl;
  std::cout << "internal id of v1 = " << id1 << std::endl;

  // get vertex by internal id
  auto v0_from_id = grin_get_vertex_by_internal_id_by_type(graph, vtype, id0);
  auto v1_from_id = grin_get_vertex_by_internal_id_by_type(graph, vtype, id1);
  ASSERT(grin_equal_vertex(graph, v0, v0_from_id) == true);
  ASSERT(grin_equal_vertex(graph, v1, v1_from_id) == true);

  // get upper bound and lower bound of internal id
  auto upper_bound =
      grin_get_vertex_internal_id_upper_bound_by_type(graph, vtype);
  auto lower_bound =
      grin_get_vertex_internal_id_lower_bound_by_type(graph, vtype);
  std::cout << "upper bound of internal id = " << upper_bound << std::endl;
  std::cout << "lower bound of internal id = " << lower_bound << std::endl;

  // destroy
  grin_destroy_vertex(graph, v0);
  grin_destroy_vertex(graph, v1);
  grin_destroy_vertex(graph, v0_from_id);
  grin_destroy_vertex(graph, v1_from_id);
  grin_destroy_vertex_list(graph, vertex_list);
  grin_destroy_vertex_type(graph, vtype);

  std::cout << "---- test index: internal id completed ----" << std::endl;
}

int main(int argc, char* argv[]) {
  // get graph from graph info of GraphAr
  std::string path = TEST_DATA_PATH;
  std::cout << "GraphInfo path = " << path << std::endl;

  char* id = new char[path.length() + 1];
  snprintf(id, path.length() + 1, "%s", path.c_str());
  GRIN_GRAPH graph = grin_get_graph_from_storage(id, NULL);
  delete[] id;

  // test index order
  test_index_order(graph);

  // test internal id
  test_internal_id(graph);

  // destroy graph
  grin_destroy_graph(graph);

  return 0;
}
