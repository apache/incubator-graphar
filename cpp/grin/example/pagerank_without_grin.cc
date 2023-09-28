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

#include <chrono>
#include <iostream>

#include "gar/graph.h"
#include "gar/graph_info.h"
#include "grin/example/config.h"

void run_pagerank(GAR_NAMESPACE::GraphInfo graph_info,
                  bool print_result = false) {
  std::cout << "++++ Run PageRank algorithm without GRIN ++++" << std::endl;

  // construct vertices collection
  std::string label = PR_VERTEX_TYPE;
  ASSERT(graph_info.GetVertexInfo(label).status().ok());
  auto maybe_vertices =
      GAR_NAMESPACE::ConstructVerticesCollection(graph_info, label);
  ASSERT(maybe_vertices.status().ok());
  auto& vertices = maybe_vertices.value();
  int num_vertices = vertices.size();

  // construct edges collection
  std::string src_label = PR_VERTEX_TYPE, edge_label = PR_EDGE_TYPE,
              dst_label = PR_VERTEX_TYPE;
  auto maybe_edges = GAR_NAMESPACE::ConstructEdgesCollection(
      graph_info, src_label, edge_label, dst_label,
      GAR_NAMESPACE::AdjListType::ordered_by_source);
  ASSERT(!maybe_edges.has_error());
  auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
      GAR_NAMESPACE::AdjListType::ordered_by_source>>(maybe_edges.value());

  // run pagerank algorithm
  const double damping = 0.85;
  const int max_iters = PR_MAX_ITERS;
  std::vector<double> pr_curr(num_vertices);
  std::vector<double> pr_next(num_vertices);
  std::vector<GAR_NAMESPACE::IdType> out_degree(num_vertices);
  for (GAR_NAMESPACE::IdType i = 0; i < num_vertices; i++) {
    pr_curr[i] = 1 / static_cast<double>(num_vertices);
    pr_next[i] = 0;
    out_degree[i] = 0;
  }
  auto it_begin = edges.begin(), it_end = edges.end();
  for (auto it = it_begin; it != it_end; ++it) {
    GAR_NAMESPACE::Edge edge = *it;
    GAR_NAMESPACE::IdType src = edge.source();
    out_degree[src]++;
  }
  for (int iter = 0; iter < max_iters; iter++) {
    std::cout << "iter " << iter << std::endl;
    for (auto it = it_begin; it != it_end; ++it) {
      GAR_NAMESPACE::Edge edge = *it;
      GAR_NAMESPACE::IdType src = edge.source();
      GAR_NAMESPACE::IdType dst = edge.destination();
      pr_next[dst] += pr_curr[src] / out_degree[src];
    }
    for (GAR_NAMESPACE::IdType i = 0; i < num_vertices; i++) {
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
    auto it = vertices.begin();
    for (size_t i = 0; i < num_vertices; i++) {
      std::cout << "vertex " << i
                << ", id = " << it.property<int64_t>(VERTEX_OID_NAME).value()
                << ", pagerank value = " << pr_curr[i] << std::endl;
      ++it;
    }
  }

  std::cout << "---- Run PageRank algorithm completed ----" << std::endl;
}

int main(int argc, char* argv[]) {
  // read file and construct graph info
  std::string path = PR_TEST_DATA_PATH;
  std::cout << "GraphInfo path = " << path << std::endl;
  auto graph_info = GAR_NAMESPACE::GraphInfo::Load(path).value();

  // run pagerank algorithm
  auto run_start = std::chrono::high_resolution_clock::now();
  run_pagerank(graph_info);
  auto run_end = std::chrono::high_resolution_clock::now();
  auto run_time = std::chrono::duration_cast<std::chrono::milliseconds>(
      run_end - run_start);

  std::cout << "Run time for PageRank without GRIN = " << run_time.count()
            << " ms" << std::endl;

  return 0;
}
