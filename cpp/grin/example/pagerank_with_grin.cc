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

extern "C" {
#include "grin/include/index/order.h"
#include "grin/include/index/original_id.h"
#include "grin/include/property/property.h"
#include "grin/include/property/topology.h"
#include "grin/include/property/type.h"
#include "grin/include/topology/adjacentlist.h"
#include "grin/include/topology/edgelist.h"
#include "grin/include/topology/structure.h"
#include "grin/include/topology/vertexlist.h"
}

void run_pagerank(GRIN_GRAPH graph, bool print_result = false) {
  std::cout << "++++ Run PageRank algorithm with GRIN ++++" << std::endl;

  // initialize parameters and the graph
  const double damping = 0.85;
  const int max_iters = PR_MAX_ITERS;
  // select vertex type
  auto vtype = grin_get_vertex_type_by_name(graph, PR_VERTEX_TYPE.c_str());
  const size_t num_vertices = grin_get_vertex_num_by_type(graph, vtype);
  // select edge type
  auto etype = grin_get_edge_type_by_name(graph, PR_EDGE_TYPE.c_str());
  auto all_edge_list = grin_get_edge_list(graph);
  auto edge_list = grin_select_type_for_edge_list(graph, etype, all_edge_list);

  // initialize pagerank value of vertices
  std::vector<double> pr_curr(num_vertices);
  std::vector<double> pr_next(num_vertices);
  std::vector<size_t> out_degree(num_vertices);
  for (size_t i = 0; i < num_vertices; i++) {
    pr_curr[i] = 1 / static_cast<double>(num_vertices);
    pr_next[i] = 0;
  }

  // initiliaze out degree of vertices
  auto it = grin_get_edge_list_begin(graph, edge_list);
  while (grin_is_edge_list_end(graph, it) == false) {
    auto e = grin_get_edge_from_iter(graph, it);
    auto v1 = grin_get_src_vertex_from_edge(graph, e);
    auto src = gar_get_internal_id_from_original_id(
        grin_get_vertex_original_id_of_int64(graph, v1));
    out_degree[src]++;

    grin_destroy_vertex(graph, v1);
    grin_destroy_edge(graph, e);

    grin_get_next_edge_list_iter(graph, it);
  }
  grin_destroy_edge_list_iter(graph, it);

  // run pagerank algorithm for #max_iters iterators
  for (int iter = 0; iter < max_iters; iter++) {
    std::cout << "iter " << iter << std::endl;

    // traverse edges to update vertices
    auto it = grin_get_edge_list_begin(graph, edge_list);
    while (grin_is_edge_list_end(graph, it) == false) {
      auto e = grin_get_edge_from_iter(graph, it);
      auto v1 = grin_get_src_vertex_from_edge(graph, e);
      auto v2 = grin_get_dst_vertex_from_edge(graph, e);
      auto src = gar_get_internal_id_from_original_id(
          grin_get_vertex_original_id_of_int64(graph, v1));
      auto dst = gar_get_internal_id_from_original_id(
          grin_get_vertex_original_id_of_int64(graph, v2));
      pr_next[dst] += pr_curr[src] / out_degree[src];

      grin_destroy_vertex(graph, v1);
      grin_destroy_vertex(graph, v2);
      grin_destroy_edge(graph, e);

      grin_get_next_edge_list_iter(graph, it);
    }
    grin_destroy_edge_list_iter(graph, it);

    // apply updated values
    for (size_t i = 0; i < num_vertices; i++) {
      pr_next[i] = damping * pr_next[i] +
                   (1 - damping) * (1 / static_cast<double>(num_vertices));
      if (out_degree[i] == 0)
        pr_next[i] += damping * pr_curr[i];
      pr_curr[i] = pr_next[i];
      pr_next[i] = 0;
    }
  }

  // output results
  if (print_result) {
    std::cout << "num_vertices: " << num_vertices << std::endl;
    auto all_vertex_list = grin_get_vertex_list(graph);
    auto vertex_list =
        grin_select_type_for_vertex_list(graph, vtype, all_vertex_list);
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
      std::cout << ", pagerank value = " << pr_curr[i] << std::endl;
      // destroy vertex
      grin_destroy_vertex(graph, v);
      grin_get_next_vertex_list_iter(graph, it);
      i++;
    }

    // destroy
    grin_destroy_vertex_list_iter(graph, it);
    grin_destroy_vertex_property(graph, property);
    grin_destroy_vertex_list(graph, vertex_list);
    grin_destroy_vertex_list(graph, all_vertex_list);
  }

  grin_destroy_edge_list(graph, edge_list);
  grin_destroy_edge_list(graph, all_edge_list);
  grin_destroy_vertex_type(graph, vtype);
  grin_destroy_edge_type(graph, etype);

  std::cout << "---- Run PageRank algorithm completed ----" << std::endl;
}

int main(int argc, char* argv[]) {
  // get graph from graph info of GraphAr
  std::string path = PR_TEST_DATA_PATH;
  std::cout << "GraphInfo path = " << path << std::endl;

  char** args = new char*[1];
  args[0] = new char[path.length() + 1];
  snprintf(args[0], path.length() + 1, "%s", path.c_str());
  auto init_start = clock();
  GRIN_GRAPH graph = grin_get_graph_from_storage(1, args);
  delete[] args[0];
  delete[] args;
  auto init_time = 1000.0 * (clock() - init_start) / CLOCKS_PER_SEC;

  // run pagerank algorithm
  auto run_start = clock();
  run_pagerank(graph);
  auto run_time = 1000.0 * (clock() - run_start) / CLOCKS_PER_SEC;

  // output execution time
  std::cout << "Init time for PageRank with GRIN = " << init_time << " ms"
            << std::endl;
  std::cout << "Run time for PageRank with GRIN = " << run_time << " ms"
            << std::endl;
  std::cout << "Totoal time for PageRank with GRIN = " << init_time + run_time
            << " ms" << std::endl;

  return 0;
}
