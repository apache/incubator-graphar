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

#include "./util.h"
#include "gar/graph.h"

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

TEST_CASE("test_vertices_collection") {
  std::string root;
  REQUIRE(GetTestResourceRoot(&root).ok());

  // read file and construct graph info
  std::string path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto maybe_graph_info = GAR_NAMESPACE::GraphInfo::Load(path);
  REQUIRE(maybe_graph_info.status().ok());
  auto graph_info = maybe_graph_info.value();

  // construct vertices collection
  std::string label = "person", property = "firstName";
  auto maybe_vertices_collection =
      GAR_NAMESPACE::ConstructVerticesCollection(graph_info, label);
  REQUIRE(!maybe_vertices_collection.has_error());
  auto& vertices = maybe_vertices_collection.value();
  for (auto it = vertices.begin(); it != vertices.end(); ++it) {
    // access data through iterator directly
    std::cout << it.id() << ", id=" << it.property<int64_t>("id").value()
              << ", firstName=" << it.property<std::string>("firstName").value()
              << std::endl;
    // access data through vertex
    auto vertex = *it;
    std::cout << vertex.id()
              << ", id=" << vertex.property<int64_t>("id").value()
              << ", firstName="
              << vertex.property<std::string>("firstName").value() << std::endl;
  }
}

TEST_CASE("test_edges_collection", "[Slow]") {
  std::string root;
  REQUIRE(GetTestResourceRoot(&root).ok());

  std::string path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  std::string src_label = "person", edge_label = "knows", dst_label = "person";
  auto graph_info = GAR_NAMESPACE::GraphInfo::Load(path).value();

  // iterate edges of vertex chunk 0
  auto expect = GAR_NAMESPACE::ConstructEdgesCollection(
      graph_info, src_label, edge_label, dst_label,
      GAR_NAMESPACE::AdjListType::ordered_by_source, 0);
  REQUIRE(!expect.has_error());
  auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
      GAR_NAMESPACE::AdjListType::ordered_by_source>>(expect.value());
  auto end = edges.end();
  for (auto it = edges.begin(); it != end; ++it) {
    // access data through iterator directly
    std::cout << "src=" << it.source() << ", dst=" << it.destination()
              << std::endl;
    auto edge = *it;
    std::cout << "src=" << edge.source() << ", dst=" << edge.destination()
              << std::endl;
  }
  // iterate all edges
  auto expect2 = GAR_NAMESPACE::ConstructEdgesCollection(
      graph_info, src_label, edge_label, dst_label,
      GAR_NAMESPACE::AdjListType::ordered_by_source);
  REQUIRE(!expect2.has_error());
  auto& edges2 = std::get<GAR_NAMESPACE::EdgesCollection<
      GAR_NAMESPACE::AdjListType::ordered_by_source>>(expect2.value());
  auto end2 = edges2.end();
  for (auto it = edges2.begin(); it != end2; ++it) {
    auto edge = *it;
    std::cout << "src=" << edge.source() << ", dst=" << edge.destination()
              << std::endl;
  }
}
