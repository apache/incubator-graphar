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
#include <unordered_set>
#include <vector>

#include "grin/test/config.h"

extern "C" {
#include "grin/include/index/original_id.h"
#include "grin/include/property/property.h"
#include "grin/include/property/type.h"
#include "grin/include/topology/adjacentlist.h"
#include "grin/include/topology/edgelist.h"
#include "grin/include/topology/structure.h"
#include "grin/include/topology/vertexlist.h"
}

void run_cc(GRIN_GRAPH graph, bool print_result = false) {
  std::cout << "++++ Run CC (push) algorithm with GRIN ++++" << std::endl;

  // initialize parameters and the graph
  const size_t num_vertices = grin_get_vertex_num(graph);
  auto vertex_list = grin_get_vertex_list(graph);

  // initialize
  std::vector<size_t> component(num_vertices);
  std::vector<bool> active[2];
  for (size_t i = 0; i < num_vertices; i++) {
    component[i] = i;
    active[0].push_back(true);
    active[1].push_back(false);
  }

  // run CC algorithm
  int count = num_vertices;
  for (int iter = 0; count > 0; iter++) {
    std::cout << "iter " << iter << ": " << count << std::endl;
    std::fill(active[1 - iter % 2].begin(), active[1 - iter % 2].end(), 0);
    count = 0;
    for (size_t vid = 0; vid < num_vertices; vid++) {
      if (active[iter % 2][vid]) {
        // get vertex
        auto v = grin_get_vertex_from_list(graph, vertex_list, vid);
        // find outgoing edges and update neighbors
        auto adj_list_out =
            grin_get_adjacent_list(graph, GRIN_DIRECTION::OUT, v);
        auto it_out = grin_get_adjacent_list_begin(graph, adj_list_out);
        while (grin_is_adjacent_list_end(graph, it_out) == false) {
          // get neighbor
          auto nbr = grin_get_neighbor_from_adjacent_list_iter(graph, it_out);
          auto nbr_id = grin_get_vertex_original_id_of_int64(graph, nbr);
          grin_destroy_vertex(graph, nbr);
          // update
          if (component[vid] < component[nbr_id]) {
            component[nbr_id] = component[vid];
            if (!active[1 - iter % 2][nbr_id]) {
              active[1 - iter % 2][nbr_id] = true;
              count++;
            }
          }
          grin_get_next_adjacent_list_iter(graph, it_out);
        }

        // find incoming edges and update neighbors
        auto adj_list_in = grin_get_adjacent_list(graph, GRIN_DIRECTION::IN, v);
        auto it_in = grin_get_adjacent_list_begin(graph, adj_list_in);
        while (grin_is_adjacent_list_end(graph, it_in) == false) {
          // get neighbor
          auto nbr = grin_get_neighbor_from_adjacent_list_iter(graph, it_in);
          auto nbr_id = grin_get_vertex_original_id_of_int64(graph, nbr);
          grin_destroy_vertex(graph, nbr);
          // update
          if (component[vid] < component[nbr_id]) {
            component[nbr_id] = component[vid];
            if (!active[1 - iter % 2][nbr_id]) {
              active[1 - iter % 2][nbr_id] = true;
              count++;
            }
          }
          grin_get_next_adjacent_list_iter(graph, it_in);
        }

        // destroy
        grin_destroy_vertex(graph, v);
        grin_destroy_adjacent_list(graph, adj_list_out);
        grin_destroy_adjacent_list_iter(graph, it_out);
        grin_destroy_adjacent_list(graph, adj_list_in);
        grin_destroy_adjacent_list_iter(graph, it_in);
      }
    }
  }

  // output results
  if (print_result) {
    std::cout << "num_vertices: " << num_vertices << std::endl;
    // count the number of connected components
    std::unordered_set<size_t> cc_count;
    size_t cc_num = 0;
    for (int i = 0; i < num_vertices; i++) {
      std::cout << i << ", component id: " << component[i] << std::endl;
      if (cc_count.find(component[i]) == cc_count.end()) {
        cc_count.insert(component[i]);
        ++cc_num;
      }
    }
    std::cout << "Total number of components: " << cc_num << std::endl;
  }

  grin_destroy_vertex_list(graph, vertex_list);

  std::cout << "---- Run CC (push) algorithm completed ----" << std::endl;
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
  delete[] args[0];
  delete[] args;
  auto init_time = 1000.0 * (clock() - init_start) / CLOCKS_PER_SEC;

  // run cc algorithm
  auto run_start = clock();
  run_cc(graph);
  auto run_time = 1000.0 * (clock() - run_start) / CLOCKS_PER_SEC;

  // output execution time
  std::cout << "Init time for CC (push) with GRIN = " << init_time << " ms"
            << std::endl;
  std::cout << "Run time for CC (push) with GRIN = " << run_time << " ms"
            << std::endl;
  std::cout << "Totoal time for CC (push) with GRIN = " << init_time + run_time
            << " ms" << std::endl;

  return 0;
}
