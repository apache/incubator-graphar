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

#include "gar/graph.h"
#include "gar/graph_info.h"
#include "grin/test/config.h"

void run_bfs(GAR_NAMESPACE::GraphInfo graph_info,
             GAR_NAMESPACE::IdType root = 0, bool print_result = false) {
  std::cout << "++++ Run BFS (pull) algorithm without GRIN ++++" << std::endl;

  // construct vertices collection
  std::string label = "person";
  assert(graph_info.GetVertexInfo(label).status().ok());
  auto maybe_vertices =
      GAR_NAMESPACE::ConstructVerticesCollection(graph_info, label);
  assert(maybe_vertices.status().ok());
  auto& vertices = maybe_vertices.value();
  int num_vertices = vertices.size();

  // construct edges collection
  std::string src_label = "person", edge_label = "knows", dst_label = "person";
  auto maybe_edges = GAR_NAMESPACE::ConstructEdgesCollection(
      graph_info, src_label, edge_label, dst_label,
      GAR_NAMESPACE::AdjListType::ordered_by_dest);
  assert(!maybe_edges.has_error());
  auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
      GAR_NAMESPACE::AdjListType::ordered_by_dest>>(maybe_edges.value());

  // initialize distance
  std::vector<int32_t> distance(num_vertices);
  for (GAR_NAMESPACE::IdType i = 0; i < num_vertices; i++)
    distance[i] = (i == root ? 0 : -1);

  // run bfs algorithm
  for (int iter = 0;; iter++) {
    GAR_NAMESPACE::IdType count = 0;
    for (GAR_NAMESPACE::IdType vid = 0; vid < num_vertices; vid++) {
      if (distance[vid] == -1) {
        auto it = edges.find_dst(vid, edges.begin());
        if (it == edges.end())
          continue;
        do {
          GAR_NAMESPACE::IdType nbr = it.source();
          if (distance[nbr] == iter) {
            distance[vid] = iter + 1;
            count++;
            break;
          }
        } while (it.next_dst());
      }
    }
    std::cout << "iter " << iter << ": " << count << " vertices." << std::endl;
    if (count == 0)
      break;
  }

  // output results
  if (print_result) {
    std::cout << "num_vertices: " << num_vertices << std::endl;
    auto it = vertices.begin();
    for (size_t i = 0; i < num_vertices; i++) {
      std::cout << "vertex " << i
                << ", id = " << it.property<int64_t>("id").value()
                << ", distance = " << distance[i] << std::endl;
      ++it;
    }
  }

  std::cout << "---- Run BFS (pull) algorithm completed ----" << std::endl;
}

int main(int argc, char* argv[]) {
  // read file and construct graph info
  std::string path = TEST_DATA_SMALL_PATH;
  std::cout << "GraphInfo path = " << path << std::endl;
  auto graph_info = GAR_NAMESPACE::GraphInfo::Load(path).value();

  // run BFS (push) algorithm
  auto run_start = clock();
  run_bfs(graph_info);
  auto run_time = 1000.0 * (clock() - run_start) / CLOCKS_PER_SEC;

  std::cout << "Run time for BFS (pull) without GRIN = " << run_time << " ms"
            << std::endl;

  return 0;
}
