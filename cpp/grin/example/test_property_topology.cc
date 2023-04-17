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

#include "grin/include/property/topology.h"
#include "grin/include/property/type.h"
#include "grin/include/topology/adjacentlist.h"
#include "grin/include/topology/edgelist.h"
#include "grin/include/topology/structure.h"
#include "grin/include/topology/vertexlist.h"

void test_property_topology_vertex(GRIN_GRAPH graph) {
  std::cout << "\n++++ test property: topology (vertex) ++++" << std::endl;

  // get vertex type list
  auto vertex_type_list = grin_get_vertex_type_list(graph);
  size_t n = grin_get_vertex_type_list_size(graph, vertex_type_list);
  std::cout << "size of vertex type list = " << n << std::endl;

  // get vertex list
  auto vertex_list = grin_get_vertex_list(graph);
  std::cout << "all vertex list size = "
            << grin_get_vertex_list_size(graph, vertex_list) << std::endl;

  for (auto i = 0; i < n; i++) {
    std::cout << "\n== vertex type " << i << ": ==" << std::endl;

    // get vertex type from vertex type list
    auto vertex_type =
        grin_get_vertex_type_from_list(graph, vertex_type_list, i);
    auto name = grin_get_vertex_type_name(graph, vertex_type);
    size_t m = grin_get_vertex_num_by_type(graph, vertex_type);
    std::cout << "name of vertex type " << i << ": " << name << std::endl;
    std::cout << "size of vertex list of vertex type " << i << " = " << m
              << std::endl;

    // select vertex list
    auto select_vertex_list =
        grin_select_type_for_vertex_list(graph, vertex_type, vertex_list);
    std::cout << "size of select vertex list of vertex type " << i << " = "
              << grin_get_vertex_list_size(graph, select_vertex_list)
              << std::endl;
    assert(grin_get_vertex_list_size(graph, select_vertex_list) == m);

    // destroy
    grin_destroy_name(graph, name);
    grin_destroy_vertex_type(graph, vertex_type);
    grin_destroy_vertex_list(graph, select_vertex_list);
  }

  // destroy
  grin_destroy_vertex_type_list(graph, vertex_type_list);
  grin_destroy_vertex_list(graph, vertex_list);

  std::cout << "---- test property: topology (vertex) completed ----"
            << std::endl;
}

void test_property_topology_edge(GRIN_GRAPH graph) {
  std::cout << "\n++++ test property: topology (edge) ++++" << std::endl;

  // get edge type list
  auto edge_type_list = grin_get_edge_type_list(graph);
  size_t n = grin_get_edge_type_list_size(graph, edge_type_list);
  std::cout << "size of edge type list = " << n << std::endl;

  // get edge list
  auto edge_list = grin_get_edge_list(graph);

  for (auto i = 0; i < n; i++) {
    std::cout << "\n== edge type " << i << ": ==" << std::endl;

    // get edge type from edge type list
    auto edge_type = grin_get_edge_type_from_list(graph, edge_type_list, i);
    size_t m = grin_get_edge_num_by_type(graph, edge_type);
    std::cout << "edge num of edge type " << i << " = " << m << std::endl;
    auto name = grin_get_edge_type_name(graph, edge_type);
    std::cout << "name of edge type " << i << ": " << name << std::endl;

    // select edge list
    auto select_edge_list =
        grin_select_type_for_edge_list(graph, edge_type, edge_list);
    auto it = grin_get_edge_list_begin(graph, select_edge_list);
    auto count = 0;
    while (grin_is_edge_list_end(graph, it) == false) {
      grin_get_next_edge_list_iter(graph, it);
      count++;
    }
    assert(count == m);
    std::cout << "size of select edge list of edge type " << i << " = " << m
              << std::endl;

    // destroy
    grin_destroy_name(graph, name);
    grin_destroy_edge_list_iter(graph, it);
    grin_destroy_edge_list(graph, select_edge_list);
    grin_destroy_edge_type(graph, edge_type);
  }

  // destroy
  grin_destroy_edge_type_list(graph, edge_type_list);
  grin_destroy_edge_list(graph, edge_list);

  std::cout << "---- test property: topology (edge) completed ----"
            << std::endl;
}

void test_property_topology_adj_list(GRIN_GRAPH graph) {
  std::cout << "\n++++ test property: topology (adj_list) ++++" << std::endl;

  // get edge type list
  auto edge_type_list = grin_get_edge_type_list(graph);
  size_t ne = grin_get_edge_type_list_size(graph, edge_type_list);
  std::cout << "size of edge type list = " << ne << std::endl;

  // get vertex type list
  auto vertex_type_list = grin_get_vertex_type_list(graph);
  size_t nv = grin_get_vertex_type_list_size(graph, vertex_type_list);
  std::cout << "size of vertex type list = " << nv << std::endl;

  // get adj list
  auto vertex_list = grin_get_vertex_list(graph);
  auto v = grin_get_vertex_from_list(graph, vertex_list, 201);
  auto adj_list = grin_get_adjacent_list(graph, GRIN_DIRECTION::IN, v);

  // iterate adj list
  auto it = grin_get_adjacent_list_begin(graph, adj_list);
  auto adj_list_size = 0;
  while (grin_is_adjacent_list_end(graph, it) == false) {
    grin_get_next_adjacent_list_iter(graph, it);
    adj_list_size++;
  }
  std::cout << "adj list size (IN) of vertex #201 = " << adj_list_size
            << std::endl;

  // select by edge type
  for (auto i = 0; i < ne; i++) {
    std::cout << "\n== edge type " << i << ": ==" << std::endl;

    // get edge type from edge type list
    auto edge_type = grin_get_edge_type_from_list(graph, edge_type_list, i);

    // select adj list
    auto select_adj_list =
        grin_select_edge_type_for_adjacent_list(graph, edge_type, adj_list);
    auto adj_list_it = grin_get_adjacent_list_begin(graph, select_adj_list);
    auto adj_list_size = 0;
    while (grin_is_adjacent_list_end(graph, adj_list_it) == false) {
      grin_get_next_adjacent_list_iter(graph, adj_list_it);
      adj_list_size++;
    }
    std::cout << "adj list size (IN) of edge type " << i
              << " for vertex #201 = " << adj_list_size << std::endl;

    // destroy
    grin_destroy_adjacent_list_iter(graph, adj_list_it);
    grin_destroy_adjacent_list(graph, select_adj_list);
    grin_destroy_edge_type(graph, edge_type);
  }

  // select by vertex type
  for (auto i = 0; i < nv; i++) {
    std::cout << "\n== vertex type " << i << ": ==" << std::endl;

    // get vertex type from vertex type list
    auto vertex_type =
        grin_get_vertex_type_from_list(graph, vertex_type_list, i);

    // select adj list
    auto select_adj_list = grin_select_neighbor_type_for_adjacent_list(
        graph, vertex_type, adj_list);
    auto adj_list_it = grin_get_adjacent_list_begin(graph, select_adj_list);
    auto adj_list_size = 0;
    while (grin_is_adjacent_list_end(graph, adj_list_it) == false) {
      grin_get_next_adjacent_list_iter(graph, adj_list_it);
      adj_list_size++;
    }
    std::cout << "adj list size (IN) of neighbor vertex type " << i
              << " for vertex #201 = " << adj_list_size << std::endl;

    // destroy
    grin_destroy_adjacent_list_iter(graph, adj_list_it);
    grin_destroy_adjacent_list(graph, select_adj_list);
    grin_destroy_vertex_type(graph, vertex_type);
  }

  // destroy
  grin_destroy_vertex_list(graph, vertex_list);
  grin_destroy_adjacent_list(graph, adj_list);
  grin_destroy_vertex_type_list(graph, vertex_type_list);
  grin_destroy_edge_type_list(graph, edge_type_list);

  std::cout << "---- test property: topology (adj_list) completed ----"
            << std::endl;
}

int main(int argc, char* argv[]) {
  // get graph from graph info of GraphAr
  std::string path =
      TEST_DATA_DIR + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  std::cout << "GraphInfo path = " << path << std::endl;

  char** args = new char*[1];
  args[0] = new char[path.length() + 1];
  snprintf(args[0], path.length() + 1, "%s", path.c_str());
  GRIN_GRAPH graph = grin_get_graph_from_storage(1, args);

  // test property topology (vertex)
  test_property_topology_vertex(graph);

  // test property topology (edge)
  test_property_topology_edge(graph);

  // test property topology (adj_list)
  test_property_topology_adj_list(graph);

  // destroy graph
  grin_destroy_graph(graph);

  return 0;
}
