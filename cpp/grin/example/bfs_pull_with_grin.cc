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

#include "grin/test/config.h"

extern "C" {
#include "grin/include/index/order.h"
#include "grin/include/property/property.h"
#include "grin/include/property/propertytable.h"
#include "grin/include/property/type.h"
#include "grin/include/topology/adjacentlist.h"
#include "grin/include/topology/edgelist.h"
#include "grin/include/topology/structure.h"
#include "grin/include/topology/vertexlist.h"
}

void run_bfs(GRIN_GRAPH graph, size_t root = 0, bool print_result = false) {
  std::cout << "++++ Run BFS (pull) algorithm with GRIN ++++" << std::endl;

  // initialize parameters and the graph
  const size_t num_vertices = grin_get_vertex_num(graph);
  auto vertex_list = grin_get_vertex_list(graph);

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
        auto adj_list = grin_get_adjacent_list(graph, GRIN_DIRECTION::IN, v);
        auto it = grin_get_adjacent_list_begin(graph, adj_list);
        while (grin_is_adjacent_list_end(graph, it) == false) {
          // get neighbor
          auto nbr = grin_get_neighbor_from_adjacent_list_iter(graph, it);
          auto nbr_id = grin_get_position_of_vertex_from_sorted_list(
              graph, vertex_list, nbr);
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
    for (size_t i = 0; i < num_vertices; i++) {
      // get vertex
      auto v = grin_get_vertex_from_list(graph, vertex_list, i);

      // get property "id" of vertex
      auto type = grin_get_vertex_type(graph, v);
      auto table = grin_get_vertex_property_table_by_type(graph, type);
      auto property = grin_get_vertex_property_by_name(graph, type, "id");
      auto data_type = grin_get_vertex_property_data_type(graph, property);
      auto value =
          grin_get_value_from_vertex_property_table(graph, table, v, property);

      // output
      std::cout << "vertex " << i;
      if (data_type == GRIN_DATATYPE::Int64) {
        std::cout << ", id = " << *static_cast<const int64_t*>(value);
      }
      std::cout << ", distance = " << distance[i] << std::endl;

      // destroy
      grin_destroy_value(graph, data_type, value);
      grin_destroy_vertex_property(graph, property);
      grin_destroy_vertex_property_table(graph, table);
      grin_destroy_vertex_type(graph, type);
      grin_destroy_vertex(graph, v);
    }
  }

  grin_destroy_vertex_list(graph, vertex_list);

  std::cout << "---- Run BFS (pull) algorithm completed ----" << std::endl;
}

int main(int argc, char* argv[]) {
  // get graph from graph info of GraphAr
  std::string path = TEST_DATA_SMALL_PATH;
  std::cout << "GraphInfo path = " << path << std::endl;

  char** args = new char*[1];
  args[0] = new char[path.length() + 1];
  snprintf(args[0], path.length() + 1, "%s", path.c_str());
  auto init_start = clock();
  GRIN_GRAPH graph = grin_get_graph_from_storage(1, args);
  auto init_time = 1000.0 * (clock() - init_start) / CLOCKS_PER_SEC;

  // run bfs algorithm
  auto run_start = clock();
  run_bfs(graph);
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
