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

#include "grin/include/proto/message.h"
#include "grin/include/topology/adjacentlist.h"
#include "grin/include/topology/edgelist.h"
#include "grin/include/topology/structure.h"
#include "grin/include/topology/vertexlist.h"

void test_protobuf() {
  std::cout << "\n++++ test protobuf ++++" << std::endl;

  std::cout << grin_get_static_storage_feature_msg() << std::endl;

  std::cout << "---- test protobuf ----" << std::endl;
}

void test_topology_structure(GRIN_GRAPH graph) {
  std::cout << "\n++++ test topology: structure ++++" << std::endl;

  assert(grin_is_multigraph(graph) == true);

  std::cout << "vertex num = " << grin_get_vertex_num(graph) << std::endl;

  std::cout << "edge num = " << grin_get_edge_num(graph) << std::endl;

  std::cout << "---- test topology: structure ----" << std::endl;
}

void test_topology_vertexlist(GRIN_GRAPH graph) {
  std::cout << "\n++++ test topology: vertexlist ++++" << std::endl;
  // get vertex list
  auto vertex_list = grin_get_vertex_list(graph);

  // test vertex list array
  auto n = grin_get_vertex_list_size(graph, vertex_list);
  std::cout << "vertex list size = " << n << std::endl;
  // methods on vertex
  auto v0 = grin_get_vertex_from_list(graph, vertex_list, 0);
  auto v1 = grin_get_vertex_from_list(graph, vertex_list, 1);
  auto v2 = grin_get_vertex_from_list(graph, vertex_list, 100000000);
  assert(grin_equal_vertex(graph, v0, v0) == true);
  assert(grin_equal_vertex(graph, v0, v1) == false);
  assert(v2 == GRIN_NULL_VERTEX);
  grin_destroy_vertex(graph, v0);
  grin_destroy_vertex(graph, v1);

  // test vertex list iterator
  auto it = grin_get_vertex_list_begin(graph, vertex_list);
  auto count = 0;
  while (grin_is_vertex_list_end(graph, it) == false) {
    auto v = grin_get_vertex_from_iter(graph, it);
    grin_destroy_vertex(graph, v);
    grin_get_next_vertex_list_iter(graph, it);
    count++;
  }
  assert(count == n);
  grin_destroy_vertex_list_iter(graph, it);

  // destroy vertex list
  grin_destroy_vertex_list(graph, vertex_list);

  std::cout << "---- test topology: vertexlist ----" << std::endl;
}

void test_topology_edgelist(GRIN_GRAPH graph) {
  std::cout << "\n++++ test topology: edgelist ++++" << std::endl;
  // get edge list
  auto edge_list = grin_get_edge_list(graph);

  // test edge list iterator
  auto it = grin_get_edge_list_begin(graph, edge_list);
  auto count = 0;
  while (grin_is_edge_list_end(graph, it) == false) {
    // methods on edge (only get 10000 times)
    if (count < 10000) {
      auto e = grin_get_edge_from_iter(graph, it);
      auto v1 = grin_get_edge_src(graph, e);
      auto v2 = grin_get_edge_dst(graph, e);
      grin_destroy_vertex(graph, v1);
      grin_destroy_vertex(graph, v2);
      grin_destroy_edge(graph, e);
    }

    grin_get_next_edge_list_iter(graph, it);
    count++;
    if (count % 50000 == 0)
      std::cout << "edge list count = " << count << std::endl;
  }
  std::cout << "edge list size = " << count << std::endl;
  grin_destroy_edge_list_iter(graph, it);

  // destroy edge list
  grin_destroy_edge_list(graph, edge_list);

  std::cout << "---- test topology: edgelist -----" << std::endl;
}

void test_topology_adjlist_in(GRIN_GRAPH graph) {
  // get adj list
  auto vertex_list = grin_get_vertex_list(graph);
  auto v = grin_get_vertex_from_list(graph, vertex_list, 201);
  auto adj_list = grin_get_adjacent_list(graph, GRIN_DIRECTION::IN, v);

  // iterate adj list
  auto it = grin_get_adjacent_list_begin(graph, adj_list);
  auto count = 0;
  while (grin_is_adjacent_list_end(graph, it) == false) {
    auto e = grin_get_edge_from_adjacent_list_iter(graph, it);
    auto v1 = grin_get_edge_src(graph, e);
    auto v2 = grin_get_edge_dst(graph, e);
    auto nbr = grin_get_neighbor_from_adjacent_list_iter(graph, it);

    // check src & dst
    assert(grin_equal_vertex(graph, v1, nbr) == true);
    assert(grin_equal_vertex(graph, v2, v) == true);

    grin_destroy_vertex(graph, v1);
    grin_destroy_vertex(graph, v2);
    grin_destroy_vertex(graph, nbr);
    grin_destroy_edge(graph, e);
    grin_get_next_adjacent_list_iter(graph, it);
    count++;
  }
  std::cout << "adj list size (IN) of vertex #201 = " << count << std::endl;
  grin_destroy_adjacent_list_iter(graph, it);

  // destory adj list
  grin_destroy_adjacent_list(graph, adj_list);
}

void test_topology_adjlist_out(GRIN_GRAPH graph) {
  // get adj list
  auto vertex_list = grin_get_vertex_list(graph);
  auto v = grin_get_vertex_from_list(graph, vertex_list, 297);
  auto adj_list = grin_get_adjacent_list(graph, GRIN_DIRECTION::OUT, v);

  // iterate adj list
  auto it = grin_get_adjacent_list_begin(graph, adj_list);
  auto count = 0;
  while (grin_is_adjacent_list_end(graph, it) == false) {
    auto e = grin_get_edge_from_adjacent_list_iter(graph, it);
    auto v1 = grin_get_edge_src(graph, e);
    auto v2 = grin_get_edge_dst(graph, e);
    auto nbr = grin_get_neighbor_from_adjacent_list_iter(graph, it);

    // check src & dst
    assert(grin_equal_vertex(graph, v1, v) == true);
    assert(grin_equal_vertex(graph, v2, nbr) == true);

    grin_destroy_vertex(graph, v1);
    grin_destroy_vertex(graph, v2);
    grin_destroy_vertex(graph, nbr);
    grin_destroy_edge(graph, e);
    grin_get_next_adjacent_list_iter(graph, it);
    count++;
  }
  std::cout << "adj list size (OUT) of vertex #297 = " << count << std::endl;
  grin_destroy_adjacent_list_iter(graph, it);

  // destory adj list
  grin_destroy_adjacent_list(graph, adj_list);
}

void test_topology_adjlist(GRIN_GRAPH graph) {
  std::cout << "\n++++ test topology: adjlist ++++" << std::endl;

  test_topology_adjlist_in(graph);

  test_topology_adjlist_out(graph);

  std::cout << "---- test topology: adjlist ----" << std::endl;
}

int main(int argc, char* argv[]) {
  // get graph from graph info of GraphAr
  std::string path = TEST_DATA_PATH;
  std::cout << "GraphInfo path = " << path << std::endl;

  char** args = new char*[1];
  args[0] = new char[path.length() + 1];
  snprintf(args[0], path.length() + 1, "%s", path.c_str());
  GRIN_GRAPH graph = grin_get_graph_from_storage(1, args);

  // test protobuf
  test_protobuf();

  // test topology structure
  test_topology_structure(graph);

  // test topology vertexlist
  test_topology_vertexlist(graph);

  // test topology edgelist
  test_topology_edgelist(graph);

  // test topology adjlist
  test_topology_adjlist(graph);

  // destroy graph
  grin_destroy_graph(graph);

  return 0;
}
