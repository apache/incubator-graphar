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

#include <chrono>  // NOLINT
#include <iostream>
#include <unordered_set>
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

void run_cc(GRIN_GRAPH graph, bool print_result = false) {
  std::cout << "++++ Run CC (push) algorithm with GRIN ++++" << std::endl;

  // initialize parameters and the graph
  // select vertex type
  auto vtype = grin_get_vertex_type_by_name(graph, CC_VERTEX_TYPE.c_str());
  auto vertex_list = grin_get_vertex_list_by_type(graph, vtype);
  const size_t num_vertices = grin_get_vertex_num_by_type(graph, vtype);
  // select edge type
  auto etype = grin_get_edge_type_by_name(graph, CC_EDGE_TYPE.c_str());

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
        auto adj_list_out = grin_get_adjacent_list_by_edge_type(
            graph, GRIN_DIRECTION::OUT, v, etype);
        auto it_out = grin_get_adjacent_list_begin(graph, adj_list_out);
        while (grin_is_adjacent_list_end(graph, it_out) == false) {
          // get neighbor
          auto nbr = grin_get_neighbor_from_adjacent_list_iter(graph, it_out);
          auto nbr_id = grin_get_vertex_internal_id_by_type(graph, vtype, nbr);
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
        auto adj_list_in = grin_get_adjacent_list_by_edge_type(
            graph, GRIN_DIRECTION::IN, v, etype);
        auto it_in = grin_get_adjacent_list_begin(graph, adj_list_in);
        while (grin_is_adjacent_list_end(graph, it_in) == false) {
          // get neighbor
          auto nbr = grin_get_neighbor_from_adjacent_list_iter(graph, it_in);
          auto nbr_id = grin_get_vertex_internal_id_by_type(graph, vtype, nbr);
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
        grin_destroy_adjacent_list(graph, adj_list_in);
        grin_destroy_adjacent_list_iter(graph, it_out);
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
  grin_destroy_vertex_type(graph, vtype);
  grin_destroy_edge_type(graph, etype);

  std::cout << "---- Run CC (push) algorithm completed ----" << std::endl;
}

int main(int argc, char* argv[]) {
  // get graph from graph info of GraphAr
  std::string path = "graphar://" + CC_TEST_DATA_PATH;
  std::cout << "graph uri = " << path << std::endl;

  // initialize graph
  auto init_start = std::chrono::high_resolution_clock::now();
  char* uri = new char[path.length() + 1];
  snprintf(uri, path.length() + 1, "%s", path.c_str());
  GRIN_GRAPH graph = grin_get_graph_from_storage(uri);
  delete[] uri;
  auto init_end = std::chrono::high_resolution_clock::now();
  auto init_time = std::chrono::duration_cast<std::chrono::milliseconds>(
      init_end - init_start);

  // run cc algorithm
  auto run_start = std::chrono::high_resolution_clock::now();
  run_cc(graph);
  auto run_end = std::chrono::high_resolution_clock::now();
  auto run_time = std::chrono::duration_cast<std::chrono::milliseconds>(
      run_end - run_start);

  // output execution time
  std::cout << "Init time for CC (push) with GRIN = " << init_time.count()
            << " ms" << std::endl;
  std::cout << "Run time for CC (push) with GRIN = " << run_time.count()
            << " ms" << std::endl;
  std::cout << "Totoal time for CC (push) with GRIN = "
            << init_time.count() + run_time.count() << " ms" << std::endl;

  return 0;
}
