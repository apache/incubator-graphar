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

#include "grin/example/config.h"

#include "grin/include/property/type.h"
#include "grin/include/topology/edgelist.h"
#include "grin/include/topology/structure.h"
#include "grin/include/topology/vertexlist.h"

void test_property_type_vertex(GRIN_GRAPH graph) {
  std::cout << "\n== test vertex type ==" << std::endl;

  // get vertex type from vertex
  auto vertex_list = grin_get_vertex_list(graph);
  size_t idx0 = 0, idx1 = 1;
  auto v0 = grin_get_vertex_from_list(graph, vertex_list, idx0);
  auto v1 = grin_get_vertex_from_list(graph, vertex_list, idx1);
  auto vertex_type0 = grin_get_vertex_type(graph, v0);
  auto vertex_type1 = grin_get_vertex_type(graph, v1);
  grin_destroy_vertex(graph, v0);
  grin_destroy_vertex(graph, v1);
  grin_destroy_vertex_list(graph, vertex_list);

  // compare
  assert(grin_equal_vertex_type(graph, vertex_type0, vertex_type0) == true);

  // vertex type name
  auto name = grin_get_vertex_type_name(graph, vertex_type0);
  std::cout << "vertex type name of vertex 0: " << name << std::endl;
  auto vertex_type3 = grin_get_vertex_type_by_name(graph, name);
  assert(grin_equal_vertex_type(graph, vertex_type0, vertex_type3) == true);
  auto vertex_type4 = grin_get_vertex_type_by_name(graph, "invalid");
  assert(vertex_type4 == GRIN_NULL_VERTEX_TYPE);
  grin_destroy_name(graph, name);

  // vertex type id
  auto id = grin_get_vertex_type_id(graph, vertex_type0);
  auto vertex_type5 = grin_get_vertex_type_from_id(graph, id);
  assert(grin_equal_vertex_type(graph, vertex_type0, vertex_type5) == true);

  // destroy vertex type
  grin_destroy_vertex_type(graph, vertex_type0);
  grin_destroy_vertex_type(graph, vertex_type1);
  grin_destroy_vertex_type(graph, vertex_type3);
  grin_destroy_vertex_type(graph, vertex_type5);

  // get vertex type list
  auto vertex_type_list = grin_get_vertex_type_list(graph);
  size_t n = grin_get_vertex_type_list_size(graph, vertex_type_list);
  std::cout << "size of vertex type list = " << n << std::endl;

  // create vertex type list
  auto new_vertex_type_list = grin_create_vertex_type_list(graph);

  // get vertex type from list and add to a new list
  for (auto i = 0; i < n; i++) {
    auto vertex_type =
        grin_get_vertex_type_from_list(graph, vertex_type_list, i);
    // insert to list
    grin_insert_vertex_type_to_list(graph, new_vertex_type_list, vertex_type);
    // get name from vertex type
    auto name = grin_get_vertex_type_name(graph, vertex_type);
    std::cout << "name of vertex type " << i << ": " << name << std::endl;
    // destroy
    grin_destroy_vertex_type(graph, vertex_type);
    grin_destroy_name(graph, name);
  }

  size_t m = grin_get_vertex_type_list_size(graph, new_vertex_type_list);
  std::cout << "size of new vertex type list = " << m << std::endl;
  assert(m == n);

  // destroy vertex type list
  grin_destroy_vertex_type_list(graph, vertex_type_list);
  grin_destroy_vertex_type_list(graph, new_vertex_type_list);
}

void test_property_type_edge(GRIN_GRAPH graph) {
  std::cout << "\n== test edge type ==" << std::endl;

  auto edge_list = grin_get_edge_list(graph);
  auto it = grin_get_edge_list_begin(graph, edge_list);
  if (grin_is_edge_list_end(graph, it) == false) {
    auto e = grin_get_edge_from_iter(graph, it);

    // get edge type from edge & compare
    auto edge_type = grin_get_edge_type(graph, e);
    grin_destroy_edge(graph, e);
    assert(grin_equal_edge_type(graph, edge_type, edge_type) == true);

    // edge type name
    auto name = grin_get_edge_type_name(graph, edge_type);
    std::cout << "edge type name of edge 0: " << name << std::endl;
    auto edge_type2 = grin_get_edge_type_by_name(graph, name);
    assert(grin_equal_edge_type(graph, edge_type, edge_type2) == true);
    auto edge_type3 = grin_get_edge_type_by_name(graph, "invalid");

    assert(edge_type3 == GRIN_NULL_EDGE_TYPE);
    grin_destroy_name(graph, name);

    // edge type id
    auto id = grin_get_edge_type_id(graph, edge_type);
    auto edge_type4 = grin_get_edge_type_from_id(graph, id);
    assert(grin_equal_edge_type(graph, edge_type, edge_type4) == true);

    // destroy edge type
    grin_destroy_edge_type(graph, edge_type);
    grin_destroy_edge_type(graph, edge_type2);
    grin_destroy_edge_type(graph, edge_type4);
  }
  grin_destroy_edge_list_iter(graph, it);
  grin_destroy_edge_list(graph, edge_list);

  // get edge type list
  auto edge_type_list = grin_get_edge_type_list(graph);
  size_t n = grin_get_edge_type_list_size(graph, edge_type_list);
  std::cout << "size of edge type list = " << n << std::endl;

  // create edge type list
  auto new_edge_type_list = grin_create_edge_type_list(graph);

  // get edgetype from list and add to a new list
  for (auto i = 0; i < n; i++) {
    auto edge_type = grin_get_edge_type_from_list(graph, edge_type_list, i);
    // insert to list
    grin_insert_edge_type_to_list(graph, new_edge_type_list, edge_type);
    // get name from edge type
    auto name = grin_get_edge_type_name(graph, edge_type);
    std::cout << "name of edge type " << i << ": " << name << std::endl;
    // destroy
    grin_destroy_name(graph, name);
    grin_destroy_edge_type(graph, edge_type);
  }

  size_t m = grin_get_edge_type_list_size(graph, new_edge_type_list);
  std::cout << "size of new edge type list = " << m << std::endl;
  assert(m == n);

  // destroy edge type list
  grin_destroy_edge_type_list(graph, edge_type_list);
  grin_destroy_edge_type_list(graph, new_edge_type_list);
}

void test_property_type_vertex_and_edge(GRIN_GRAPH graph) {
  std::cout << "\n== test vertex and edge type ==" << std::endl;

  // get edge type list
  auto edge_type_list = grin_get_edge_type_list(graph);
  size_t n = grin_get_edge_type_list_size(graph, edge_type_list);

  if (n > 0) {
    // get edge type from list
    auto edge_type = grin_get_edge_type_from_list(graph, edge_type_list, 0);

    // get vertex types from edge type
    auto src_type_list = grin_get_src_types_from_edge_type(graph, edge_type);
    auto dst_type_list = grin_get_dst_types_from_edge_type(graph, edge_type);
    assert(grin_get_vertex_type_list_size(graph, src_type_list) ==
           grin_get_vertex_type_list_size(graph, dst_type_list));
    std::cout << "size of vertex type list from edge type = "
              << grin_get_vertex_type_list_size(graph, dst_type_list)
              << std::endl;

    // get edge type from vertex types
    auto src_type = grin_get_vertex_type_from_list(graph, src_type_list, 0);
    auto dst_type = grin_get_vertex_type_from_list(graph, dst_type_list, 0);
    auto edge_type_list_2 =
        grin_get_edge_types_from_vertex_type_pair(graph, src_type, dst_type);
    std::cout << "size of edge type list from vertex type pair = "
              << grin_get_edge_type_list_size(graph, edge_type_list_2)
              << std::endl;

    // destroy
    grin_destroy_edge_type(graph, edge_type);
    grin_destroy_vertex_type_list(graph, src_type_list);
    grin_destroy_vertex_type_list(graph, dst_type_list);
    grin_destroy_vertex_type(graph, src_type);
    grin_destroy_vertex_type(graph, dst_type);
    grin_destroy_edge_type_list(graph, edge_type_list_2);
  }

  // destroy edge type list
  grin_destroy_edge_type_list(graph, edge_type_list);
}

void test_property_type(GRIN_GRAPH graph) {
  std::cout << "\n++++ test property: type ++++" << std::endl;

  test_property_type_vertex(graph);

  test_property_type_edge(graph);

  test_property_type_vertex_and_edge(graph);

  std::cout << "---- test property: type completed ----" << std::endl;
}

int main(int argc, char* argv[]) {
  // get graph from graph info of GraphAr
  std::string path = TEST_DATA_PATH;
  std::cout << "GraphInfo path = " << path << std::endl;

  char** args = new char*[1];
  args[0] = new char[path.length() + 1];
  snprintf(args[0], path.length() + 1, "%s", path.c_str());
  GRIN_GRAPH graph = grin_get_graph_from_storage(1, args);

  // test property type
  test_property_type(graph);

  // destroy graph
  grin_destroy_graph(graph);

  return 0;
}
