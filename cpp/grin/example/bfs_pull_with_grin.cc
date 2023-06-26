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

#include <ctime>
#include <iostream>
#include <vector>

#include "grin/example/config.h"
#include "grin/predefine.h"

// GRIN headers
#include "index/internal_id.h"
#include "property/property.h"
#include "property/topology.h"
#include "property/type.h"
#include "topology/adjacentlist.h"
#include "topology/edgelist.h"
#include "topology/structure.h"
#include "topology/vertexlist.h"

void run_bfs(GRIN_GRAPH graph, size_t root = BFS_ROOT_ID,
             bool print_result = false) {
  std::cout << "++++ Run BFS (pull) algorithm with GRIN ++++" << std::endl;

  // select vertex type
  auto vtype = grin_get_vertex_type_by_name(graph, BFS_VERTEX_TYPE.c_str());
  auto vertex_list = grin_get_vertex_list_by_type(graph, vtype);
  const size_t num_vertices = grin_get_vertex_num_by_type(graph, vtype);
  // select edge type
  auto etype = grin_get_edge_type_by_name(graph, BFS_EDGE_TYPE.c_str());

  // initialize BFS value of vertices
  std::vector<int32_t> distance(num_vertices);
  for (size_t i = 0; i < num_vertices; i++)
    distance[i] = (i == root ? 0 : -1);

  // run BFS algorithm
  for (int iter = 0;; iter++) {
    size_t count = 0;
    for (size_t i = 0; i < num_vertices; i++) {
      if (distance[i] == -1) {
        // get vertex
        auto v = grin_get_vertex_from_list(graph, vertex_list, i);

        // get incoming edges of v and update it
        auto adj_list = grin_get_adjacent_list_by_edge_type(
            graph, GRIN_DIRECTION::IN, v, etype);
        auto it = grin_get_adjacent_list_begin(graph, adj_list);
        while (grin_is_adjacent_list_end(graph, it) == false) {
          // get neighbor
          auto nbr = grin_get_neighbor_from_adjacent_list_iter(graph, it);
          auto nbr_id = grin_get_vertex_internal_id_by_type(graph, vtype, nbr);
          grin_destroy_vertex(graph, nbr);
          // update
          if (distance[nbr_id] == iter) {
            distance[i] = iter + 1;
            count++;
            break;
          }
          grin_get_next_adjacent_list_iter(graph, it);
        }

        // destroy
        grin_destroy_vertex(graph, v);
        grin_destroy_adjacent_list(graph, adj_list);
        grin_destroy_adjacent_list_iter(graph, it);
      }
    }
    std::cout << "iter " << iter << ": " << count << " vertices." << std::endl;
    if (count == 0)
      break;
  }

  // output results
  if (print_result) {
    std::cout << "num_vertices: " << num_vertices << std::endl;
    auto property =
        grin_get_vertex_property_by_name(graph, vtype, VERTEX_OID_NAME.c_str());
    auto data_type = grin_get_vertex_property_datatype(graph, property);

    auto it = grin_get_vertex_list_begin(graph, vertex_list);
    size_t i = 0;
    while (grin_is_vertex_list_end(graph, it) == false) {
      // get vertex
      auto v = grin_get_vertex_from_iter(graph, it);
      // output
      std::cout << "vertex " << i;
      if (data_type == GRIN_DATATYPE::Int64) {
        // get property "id" of vertex
        auto value =
            grin_get_vertex_property_value_of_int64(graph, v, property);
        std::cout << ", id = " << value;
      }
      std::cout << ", distance = " << distance[i] << std::endl;
      // destroy vertex
      grin_destroy_vertex(graph, v);
      grin_get_next_vertex_list_iter(graph, it);
      i++;
    }

    // destroy
    grin_destroy_vertex_property(graph, property);
  }

  grin_destroy_vertex_list(graph, vertex_list);
  grin_destroy_vertex_type(graph, vtype);
  grin_destroy_edge_type(graph, etype);

  std::cout << "---- Run BFS (pull) algorithm completed ----" << std::endl;
}

int main(int argc, char* argv[]) {
  // get graph from graph info of GraphAr
  std::string path = "graphar://" + BFS_TEST_DATA_PATH;
  std::cout << "graph uri = " << path << std::endl;

  // initialize graph
  auto init_start = clock();
  char* uri = new char[path.length() + 1];
  snprintf(uri, path.length() + 1, "%s", path.c_str());
  GRIN_GRAPH graph = grin_get_graph_from_storage(uri);
  delete[] uri;
  auto init_time = 1000.0 * (clock() - init_start) / CLOCKS_PER_SEC;

  // run bfs algorithm
  auto run_start = clock();
  run_bfs(graph, BFS_ROOT_ID);
  auto run_time = 1000.0 * (clock() - run_start) / CLOCKS_PER_SEC;

  // output execution time
  std::cout << "Init time for BFS (pull) with GRIN = " << init_time << " ms"
            << std::endl;
  std::cout << "Run time for BFS (pull) with GRIN = " << run_time << " ms"
            << std::endl;
  std::cout << "Totoal time for BFS (pull) with GRIN = " << init_time + run_time
            << " ms" << std::endl;

  return 0;
}
