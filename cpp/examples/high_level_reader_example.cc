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

#include "arrow/api.h"
#include "arrow/filesystem/api.h"

#include "./config.h"
#include "gar/graph.h"

void vertices_collection(const GAR_NAMESPACE::GraphInfo& graph_info) {
  // construct vertices collection
  std::string label = "person", property = "firstName";
  auto maybe_vertices_collection =
      GAR_NAMESPACE::ConstructVerticesCollection(graph_info, label);
  ASSERT(!maybe_vertices_collection.has_error());
  auto vertices = maybe_vertices_collection.value();

  // use vertices collection
  auto count = 0;
  for (auto it = vertices->begin(); it != vertices->end(); ++it) {
    // access data through iterator directly
    std::cout << it.id() << ", id=" << it.property<int64_t>("id").value()
              << ", firstName=" << it.property<std::string>("firstName").value()
              << "; ";
    // access data through vertex
    auto vertex = *it;
    std::cout << vertex.id()
              << ", id=" << vertex.property<int64_t>("id").value()
              << ", firstName="
              << vertex.property<std::string>("firstName").value() << std::endl;
    count++;
  }
  // add operator+ for iterator
  auto it_last = vertices->begin() + (count - 1);
  std::cout << "the last vertex: " << std::endl;
  std::cout << it_last.id()
            << ", id=" << it_last.property<int64_t>("id").value()
            << ", firstName="
            << it_last.property<std::string>("firstName").value() << std::endl;
  // count
  ASSERT(count == vertices->size());
  std::cout << "vertex_count=" << count << std::endl;
}

void edges_collection(const GAR_NAMESPACE::GraphInfo& graph_info) {
  // construct edges collection
  std::string src_label = "person", edge_label = "knows", dst_label = "person";
  auto expect = GAR_NAMESPACE::ConstructEdgesCollection(
      graph_info, src_label, edge_label, dst_label,
      GAR_NAMESPACE::AdjListType::ordered_by_source);
  ASSERT(!expect.has_error());
  auto edges = expect.value();

  // use edges collection
  auto end = edges->end();
  size_t count = 0;
  for (auto it = edges->begin(); it != end; ++it) {
    // access data through iterator directly
    std::cout << "src=" << it.source() << ", dst=" << it.destination() << "; ";
    // access data through edge
    auto edge = *it;
    std::cout << "src=" << edge.source() << ", dst=" << edge.destination()
              << ", creationDate="
              << edge.property<std::string>("creationDate").value()
              << std::endl;
    count++;
  }
  // count
  ASSERT(count == edges->size());
  std::cout << "edge_count=" << count << std::endl;
}

int main(int argc, char* argv[]) {
  // read file and construct graph info
  std::string path =
      TEST_DATA_DIR + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto graph_info = GAR_NAMESPACE::GraphInfo::Load(path).value();

  // vertices collection
  std::cout << "Vertices collection" << std::endl;
  std::cout << "-------------------" << std::endl;
  vertices_collection(graph_info);
  std::cout << std::endl;

  // edges collection
  std::cout << "Edges collection" << std::endl;
  std::cout << "----------------" << std::endl;
  edges_collection(graph_info);
}
