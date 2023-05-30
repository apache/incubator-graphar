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

#include "gar/graph.h"
#include "gar/graph_info.h"
#include "grin/example/config.h"

void run_cc(GAR_NAMESPACE::GraphInfo graph_info, bool print_result = false) {
  std::cout << "++++ Run CC (push) algorithm without GRIN ++++" << std::endl;

  // construct vertices collection
  std::string label = CC_VERTEX_TYPE;
  assert(graph_info.GetVertexInfo(label).status().ok());
  auto maybe_vertices =
      GAR_NAMESPACE::ConstructVerticesCollection(graph_info, label);
  assert(maybe_vertices.status().ok());
  auto& vertices = maybe_vertices.value();
  int num_vertices = vertices.size();

  // construct edges collection
  std::string src_label = CC_SRC_TYPE, edge_label = CC_EDGE_TYPE,
              dst_label = CC_DST_TYPE;
  auto expect1 = GAR_NAMESPACE::ConstructEdgesCollection(
      graph_info, src_label, edge_label, dst_label,
      GAR_NAMESPACE::AdjListType::ordered_by_source);
  assert(!expect1.has_error());
  auto& edges1 = std::get<GAR_NAMESPACE::EdgesCollection<
      GAR_NAMESPACE::AdjListType::ordered_by_source>>(expect1.value());
  auto expect2 = GAR_NAMESPACE::ConstructEdgesCollection(
      graph_info, src_label, edge_label, dst_label,
      GAR_NAMESPACE::AdjListType::ordered_by_dest);
  assert(!expect2.has_error());
  auto& edges2 = std::get<GAR_NAMESPACE::EdgesCollection<
      GAR_NAMESPACE::AdjListType::ordered_by_dest>>(expect2.value());

  // initialize
  std::vector<GAR_NAMESPACE::IdType> component(num_vertices);
  std::vector<bool> active[2];
  for (GAR_NAMESPACE::IdType i = 0; i < num_vertices; i++) {
    component[i] = i;
    active[0].push_back(true);
    active[1].push_back(false);
  }

  // run cc algorithm
  int count = num_vertices;
  for (int iter = 0; count > 0; iter++) {
    std::cout << "iter " << iter << ": " << count << std::endl;
    std::fill(active[1 - iter % 2].begin(), active[1 - iter % 2].end(), 0);
    count = 0;
    for (GAR_NAMESPACE::IdType vid = 0; vid < num_vertices; vid++) {
      if (active[iter % 2][vid]) {
        // find outgoing edges and update neighbors
        auto it1 = edges1.find_src(vid, edges1.begin());
        if (it1 != edges1.end()) {
          do {
            GAR_NAMESPACE::IdType nbr = it1.destination();
            if (component[vid] < component[nbr]) {
              component[nbr] = component[vid];
              if (!active[1 - iter % 2][nbr]) {
                active[1 - iter % 2][nbr] = true;
                count++;
              }
            }
          } while (it1.next_src());
        }

        // find incoming edges and update neighbors
        auto it2 = edges2.find_dst(vid, edges2.begin());
        if (it2 != edges2.end()) {
          do {
            GAR_NAMESPACE::IdType nbr = it2.source();
            if (component[vid] < component[nbr]) {
              component[nbr] = component[vid];
              if (!active[1 - iter % 2][nbr]) {
                active[1 - iter % 2][nbr] = true;
                count++;
              }
            }
          } while (it2.next_dst());
        }
      }
    }
  }

  // output results
  if (print_result) {
    std::cout << "num_vertices: " << num_vertices << std::endl;
    // count the number of connected components
    std::unordered_set<GAR_NAMESPACE::IdType> cc_count;
    GAR_NAMESPACE::IdType cc_num = 0;
    for (int i = 0; i < num_vertices; i++) {
      std::cout << i << ", component id: " << component[i] << std::endl;
      if (cc_count.find(component[i]) == cc_count.end()) {
        cc_count.insert(component[i]);
        ++cc_num;
      }
    }
    std::cout << "Total number of components: " << cc_num << std::endl;
  }

  std::cout << "---- Run CC (push) algorithm completed ----" << std::endl;
}

int main(int argc, char* argv[]) {
  // read file and construct graph info
  std::string path = CC_TEST_DATA_PATH;
  std::cout << "GraphInfo path = " << path << std::endl;
  auto graph_info = GAR_NAMESPACE::GraphInfo::Load(path).value();

  // run cc algorithm
  auto run_start = clock();
  run_cc(graph_info);
  auto run_time = 1000.0 * (clock() - run_start) / CLOCKS_PER_SEC;

  std::cout << "Run time for CC (push) without GRIN = " << run_time << " ms"
            << std::endl;

  return 0;
}
