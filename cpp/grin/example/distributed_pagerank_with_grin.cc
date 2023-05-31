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

#include <mpi.h>

#include <ctime>
#include <iostream>
#include <vector>

#include "grin/example/config.h"

extern "C" {
#include "grin/include/index/order.h"
#include "grin/include/index/original_id.h"
#include "grin/include/partition/partition.h"
#include "grin/include/partition/reference.h"
#include "grin/include/partition/topology.h"
#include "grin/include/property/property.h"
#include "grin/include/property/topology.h"
#include "grin/include/property/type.h"
#include "grin/include/topology/adjacentlist.h"
#include "grin/include/topology/edgelist.h"
#include "grin/include/topology/structure.h"
#include "grin/include/topology/vertexlist.h"
}

GRIN_GRAPH init(GRIN_PARTITIONED_GRAPH partitioned_graph, int pid = 0) {
  // get local graph
  auto partition = grin_get_partition_by_id(partitioned_graph, pid);
  auto graph = grin_get_local_graph_by_partition(partitioned_graph, partition);

  // destroy partition
  grin_destroy_partition(partitioned_graph, partition);

  std::cout << "Init GRIN_GRAPH completed for partition " << pid << std::endl;
  return graph;
}

void run_pagerank(GRIN_PARTITIONED_GRAPH graph, bool print_result = false) {
  // initialize parameters
  const double damping = 0.85;
  const int max_iters = 20;
  // get vertex list & select by vertex type
  auto all_vertex_list = grin_get_vertex_list(graph);
  auto vtype = grin_get_vertex_type_by_name(graph, DIS_PR_VERTEX_TYPE.c_str());
  auto etype = grin_get_edge_type_by_name(graph, DIS_PR_EDGE_TYPE.c_str());
  auto vertex_list =
      grin_select_type_for_vertex_list(graph, vtype, all_vertex_list);
  const size_t num_vertices = grin_get_vertex_num_by_type(graph, vtype);
  std::cout << "num_vertices = " << num_vertices << std::endl;

  // initialize MPI
  int pid = 0, is_master = 0, n_procs = 0;
  MPI_Comm_size(MPI_COMM_WORLD, &n_procs);
  MPI_Comm_rank(MPI_COMM_WORLD, &pid);
  is_master = (pid == 0);

  std::cout << "++++ Run distributed PageRank algorithm start for partition "
            << pid << " ++++" << std::endl;

  // select master
  auto master_vertex_list =
      grin_select_master_for_vertex_list(graph, vertex_list);
  const size_t num_masters =
      grin_get_vertex_list_size(graph, master_vertex_list);
  std::cout << "pid = " << pid << ", num_masters = " << num_masters
            << std::endl;
  size_t total_num_masters = 0;
  MPI_Allreduce(&num_masters, &total_num_masters, 1, MPI_UNSIGNED_LONG, MPI_SUM,
                MPI_COMM_WORLD);
  assert(total_num_masters == num_vertices);

  // initialize cnt and offset for MPI_Allgatherv
  std::vector<int> cnt(n_procs), offset(n_procs);
  MPI_Allgather(&num_masters, 1, MPI_INT, cnt.data(), 1, MPI_INT,
                MPI_COMM_WORLD);
  offset[0] = 0;
  for (int i = 1; i < n_procs; ++i) {
    offset[i] = offset[i - 1] + cnt[i - 1];
  }
  MPI_Bcast(cnt.data(), n_procs, MPI_INT, 0, MPI_COMM_WORLD);
  MPI_Bcast(offset.data(), n_procs, MPI_INT, 0, MPI_COMM_WORLD);

  // initialize pagerank value
  std::vector<double> pr_curr(num_vertices);
  std::vector<double> next(num_masters);
  for (auto i = 0; i < num_vertices; ++i) {
    pr_curr[i] = 1 / static_cast<double>(num_vertices);
  }

  // initialize out degree
  std::vector<size_t> out_degree(num_vertices, 0);
  for (auto i = 0; i < num_masters; ++i) {
    // get vertex
    auto v = grin_get_vertex_from_list(graph, master_vertex_list, i);
    auto id =
        grin_get_position_of_vertex_from_sorted_list(graph, vertex_list, v);
    // get outgoing adjacent list
    auto all_adjacent_list =
        grin_get_adjacent_list(graph, GRIN_DIRECTION::OUT, v);
    auto adjacent_list = grin_select_edge_type_for_adjacent_list(
        graph, etype, all_adjacent_list);
    auto it = grin_get_adjacent_list_begin(graph, adjacent_list);
    while (grin_is_adjacent_list_end(graph, it) == false) {
      out_degree[id]++;
      grin_get_next_adjacent_list_iter(graph, it);
    }
    // destroy
    grin_destroy_adjacent_list_iter(graph, it);
    grin_destroy_adjacent_list(graph, adjacent_list);
    grin_destroy_adjacent_list(graph, all_adjacent_list);
    grin_destroy_vertex(graph, v);
  }
  // synchronize out degree
  MPI_Allreduce(MPI_IN_PLACE, out_degree.data(), num_vertices,
                MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);

  // run pagerank algorithm for #max_iters iterators
  for (int iter = 0; iter < max_iters; iter++) {
    std::cout << "pid = " << pid << ", iter = " << iter << std::endl;

    // update pagerank value
    for (auto i = 0; i < num_masters; ++i) {
      // get vertex
      auto v = grin_get_vertex_from_list(graph, master_vertex_list, i);
      // get incoming adjacent list
      auto all_adjacent_list =
          grin_get_adjacent_list(graph, GRIN_DIRECTION::IN, v);
      auto adjacent_list = grin_select_edge_type_for_adjacent_list(
          graph, etype, all_adjacent_list);
      auto it = grin_get_adjacent_list_begin(graph, adjacent_list);
      // update pagerank value
      next[i] = 0;
      while (grin_is_adjacent_list_end(graph, it) == false) {
        // get neighbor
        auto nbr = grin_get_neighbor_from_adjacent_list_iter(graph, it);
        auto nbr_id = grin_get_position_of_vertex_from_sorted_list(
            graph, vertex_list, nbr);
        next[i] += pr_curr[nbr_id] / out_degree[nbr_id];
        grin_destroy_vertex(graph, nbr);
        grin_get_next_adjacent_list_iter(graph, it);
      }
      // destroy
      grin_destroy_adjacent_list_iter(graph, it);
      grin_destroy_adjacent_list(graph, adjacent_list);
      grin_destroy_adjacent_list(graph, all_adjacent_list);
      grin_destroy_vertex(graph, v);
    }

    // apply updated values
    for (auto i = 0; i < num_masters; ++i) {
      // get vertex
      auto v = grin_get_vertex_from_list(graph, master_vertex_list, i);
      auto id =
          grin_get_position_of_vertex_from_sorted_list(graph, vertex_list, v);
      next[i] = damping * next[i] +
                (1 - damping) * (1 / static_cast<double>(num_vertices));
      if (out_degree[id] == 0)
        next[i] += damping * pr_curr[id];
      grin_destroy_vertex(graph, v);
    }

    // synchronize pagerank value
    MPI_Allgatherv(next.data(), num_masters, MPI_DOUBLE, pr_curr.data(),
                   cnt.data(), offset.data(), MPI_DOUBLE, MPI_COMM_WORLD);
  }

  // output results
  if (is_master && print_result) {
    auto type = grin_get_vertex_type_by_name(graph, "person");
    auto property = grin_get_vertex_property_by_name(graph, type, "id");
    auto data_type = grin_get_vertex_property_datatype(graph, property);

    for (size_t i = 0; i < num_vertices; i++) {
      // get vertex
      auto v = grin_get_vertex_from_list(graph, vertex_list, i);
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
    }

    // destroy
    grin_destroy_vertex_property(graph, property);
    grin_destroy_vertex_type(graph, type);
  }

  grin_destroy_vertex_list(graph, master_vertex_list);
  grin_destroy_vertex_list(graph, vertex_list);
  grin_destroy_vertex_list(graph, all_vertex_list);
  grin_destroy_vertex_type(graph, vtype);
  grin_destroy_edge_type(graph, etype);

  std::cout
      << "---- Run distributed PageRank algorithm completed for partition "
      << pid << " -----" << std::endl;
}

int main(int argc, char* argv[]) {
  auto init_start = clock();  // init start

  // MPI initialize
  int flag = 0, pid = 0, n_procs = 0, is_master = 0;
  MPI_Initialized(&flag);
  if (!flag)
    MPI_Init(NULL, NULL);
  MPI_Comm_rank(MPI_COMM_WORLD, &pid);
  MPI_Comm_size(MPI_COMM_WORLD, &n_procs);
  is_master = (pid == 0);

  // set graph info path and partition number
  std::string path = DIS_PR_TEST_DATA_PATH;
  // set partition number = n_procs, stragey = segmented
  uint32_t partition_num = n_procs;
  if (is_master) {
    std::cout << "GraphInfo path = " << path << std::endl;
    std::cout << "Partition strategy = segmented" << std::endl;
    std::cout << "Partition number = " << partition_num << std::endl;
  }

  // get partitioned graph from graph info of GraphAr
  char** args = new char*[3];
  args[0] = new char[path.length() + 1];
  snprintf(args[0], path.length() + 1, "%s", path.c_str());
  args[1] = new char[2];
  snprintf(args[1], sizeof(args[1]), "%d", partition_num);
  args[2] = new char[2];
  uint32_t strategy = 0;  // segmented
  snprintf(args[2], sizeof(args[2]), "%d", strategy);
  GRIN_PARTITIONED_GRAPH pg = grin_get_partitioned_graph_from_storage(3, args);
  // destroy args
  delete[] args[0];
  delete[] args[1];
  delete[] args[2];
  delete[] args;
  // get local graph from partitioned graph
  GRIN_GRAPH graph = init(pg, pid);

  auto init_time =
      1000.0 * (clock() - init_start) / CLOCKS_PER_SEC;  // init end

  // run pagerank algorithm
  auto run_start = clock();  // run start
  run_pagerank(graph);       // do not print result by default
  auto run_time = 1000.0 * (clock() - run_start) / CLOCKS_PER_SEC;  // run end

  // destroy
  grin_destroy_graph(graph);
  grin_destroy_partitioned_graph(pg);

  // MPI finalize
  MPI_Finalize();

  // output execution time
  if (is_master) {
    std::cout << "Init time for distributed PageRank with GRIN = " << init_time
              << " ms" << std::endl;
    std::cout << "Run time for distibuted PageRank with GRIN = " << run_time
              << " ms" << std::endl;
    std::cout << "Totoal time for distributed PageRank with GRIN = "
              << init_time + run_time << " ms" << std::endl;
  }

  return 0;
}
