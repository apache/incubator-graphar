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

namespace GAR_NAMESPACE {
TEST_CASE("test_vertices_collection") {
  std::string root;
  REQUIRE(GetTestResourceRoot(&root).ok());

  // read file and construct graph info
  std::string path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto maybe_graph_info = GraphInfo::Load(path);
  REQUIRE(maybe_graph_info.status().ok());
  auto graph_info = maybe_graph_info.value();

  // construct vertices collection
  std::string label = "person", property = "firstName";
  auto maybe_vertices_collection = VerticesCollection::Make(graph_info, label);
  REQUIRE(!maybe_vertices_collection.has_error());
  auto vertices = maybe_vertices_collection.value();
  auto count = 0;
  for (auto it = vertices->begin(); it != vertices->end(); ++it) {
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
    // access data reference through vertex
    REQUIRE(vertex.property<int64_t>("id").value() ==
            vertex.property<const int64_t&>("id").value());
    REQUIRE(vertex.property<std::string>("firstName").value() ==
            vertex.property<const std::string&>("firstName").value());
    REQUIRE(vertex.property<const std::string&>("id").has_error());
    count++;
  }
  auto it_last = vertices->begin() + (count - 1);
  std::cout << it_last.id()
            << ", id=" << it_last.property<int64_t>("id").value()
            << ", firstName="
            << it_last.property<std::string>("firstName").value() << std::endl;
  auto it_begin = it_last + (1 - count);

  auto it = vertices->begin();
  it += (count - 1);
  REQUIRE(it.id() == it_last.id());
  REQUIRE(it.property<int64_t>("id").value() ==
          it_last.property<int64_t>("id").value());
  it += (1 - count);
  REQUIRE(it.id() == it_begin.id());
  REQUIRE(it.property<int64_t>("id").value() ==
          it_begin.property<int64_t>("id").value());
}

TEST_CASE("test_edges_collection", "[Slow]") {
  std::string root;
  REQUIRE(GetTestResourceRoot(&root).ok());

  std::string path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  std::string src_label = "person", edge_label = "knows", dst_label = "person";
  auto graph_info = GraphInfo::Load(path).value();

  // iterate edges of vertex chunk 0
  auto expect =
      EdgesCollection::Make(graph_info, src_label, edge_label, dst_label,
                            AdjListType::ordered_by_source, 0, 1);
  REQUIRE(!expect.has_error());
  auto edges = expect.value();
  auto end = edges->end();
  size_t count = 0;
  for (auto it = edges->begin(); it != end; ++it) {
    // access data through iterator directly
    std::cout << "src=" << it.source() << ", dst=" << it.destination() << " ";
    // access data through edge
    auto edge = *it;
    REQUIRE(edge.source() == it.source());
    REQUIRE(edge.destination() == it.destination());
    std::cout << "creationDate="
              << edge.property<std::string>("creationDate").value()
              << std::endl;
    // access data reference through edge
    REQUIRE(edge.property<std::string>("creationDate").value() ==
            edge.property<const std::string&>("creationDate").value());
    REQUIRE(edge.property<const int64_t&>("creationDate").has_error());
    count++;
  }
  std::cout << "edge_count=" << count << std::endl;
  REQUIRE(edges->size() == count);

  // iterate edges of vertex chunk [2, 4)
  auto expect1 =
      EdgesCollection::Make(graph_info, src_label, edge_label, dst_label,
                            AdjListType::ordered_by_dest, 2, 4);
  REQUIRE(!expect1.has_error());
  auto edges1 = expect1.value();
  auto end1 = edges1->end();
  size_t count1 = 0;
  for (auto it = edges1->begin(); it != end1; ++it) {
    count1++;
  }
  std::cout << "edge_count=" << count1 << std::endl;
  REQUIRE(edges1->size() == count1);

  // iterate all edges
  auto expect2 =
      EdgesCollection::Make(graph_info, src_label, edge_label, dst_label,
                            AdjListType::ordered_by_source);
  REQUIRE(!expect2.has_error());
  auto& edges2 = expect2.value();
  auto end2 = edges2->end();
  size_t count2 = 0;
  for (auto it = edges2->begin(); it != end2; ++it) {
    auto edge = *it;
    std::cout << "src=" << edge.source() << ", dst=" << edge.destination()
              << std::endl;
    count2++;
  }
  std::cout << "edge_count=" << count2 << std::endl;
  REQUIRE(edges2->size() == count2);

  // empty collection
  auto expect3 =
      EdgesCollection::Make(graph_info, src_label, edge_label, dst_label,
                            AdjListType::unordered_by_source, 5, 5);
  REQUIRE(!expect2.has_error());
  auto edges3 = expect3.value();
  auto end3 = edges3->end();
  size_t count3 = 0;
  for (auto it = edges3->begin(); it != end3; ++it) {
    count3++;
  }
  std::cout << "edge_count=" << count3 << std::endl;
  REQUIRE(count3 == 0);
  REQUIRE(edges3->size() == 0);

  // invalid adjlist type
  auto expect4 =
      EdgesCollection::Make(graph_info, src_label, edge_label, dst_label,
                            AdjListType::unordered_by_dest);
  REQUIRE(expect4.status().IsInvalid());
}
}  // namespace GAR_NAMESPACE
