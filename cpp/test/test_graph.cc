/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <iostream>

#include "./util.h"
#include "graphar/api/high_level_reader.h"

#include <catch2/catch_test_macros.hpp>

namespace graphar {
TEST_CASE_METHOD(GlobalFixture, "Graph") {
  // read file and construct graph info
  std::string path =
      test_data_dir + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto maybe_graph_info = GraphInfo::Load(path);
  REQUIRE(maybe_graph_info.status().ok());
  auto graph_info = maybe_graph_info.value();

  SECTION("VerticesCollection") {
    // construct vertices collection
    std::string type = "person", property = "firstName";
    auto maybe_vertices_collection = VerticesCollection::Make(graph_info, type);
    REQUIRE(!maybe_vertices_collection.has_error());
    auto vertices = maybe_vertices_collection.value();
    auto count = 0;
    for (auto it = vertices->begin(); it != vertices->end(); ++it) {
      // access data through iterator directly
      std::cout << it.id() << ", id=" << it.property<int64_t>("id").value()
                << ", firstName="
                << it.property<std::string>("firstName").value() << std::endl;
      // access data through vertex
      auto vertex = *it;
      std::cout << vertex.id()
                << ", id=" << vertex.property<int64_t>("id").value()
                << ", firstName="
                << vertex.property<std::string>("firstName").value()
                << std::endl;
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
              << it_last.property<std::string>("firstName").value()
              << std::endl;
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

  SECTION("ListProperty") {
    // read file and construct graph info
    std::string path =
        test_data_dir +
        "/ldbc_sample/parquet/ldbc_sample_with_feature.graph.yml";
    auto maybe_graph_info = GraphInfo::Load(path);
    REQUIRE(maybe_graph_info.status().ok());
    auto graph_info = maybe_graph_info.value();
    std::string type = "person", list_property = "feature";
    auto maybe_vertices_collection = VerticesCollection::Make(graph_info, type);
    REQUIRE(!maybe_vertices_collection.has_error());
    auto vertices = maybe_vertices_collection.value();
    auto count = 0;
    auto vertex_info = graph_info->GetVertexInfo(type);
    auto data_type = vertex_info->GetPropertyType(list_property).value();
    REQUIRE(data_type->id() == Type::LIST);
    REQUIRE(data_type->value_type()->id() == Type::FLOAT);
    if (data_type->id() == Type::LIST &&
        data_type->value_type()->id() == Type::FLOAT) {
      for (auto it = vertices->begin(); it != vertices->end(); ++it) {
        auto vertex = *it;
        auto float_array = vertex.property<FloatArray>(list_property).value();
        for (size_t i = 0; i < float_array.size(); i++) {
          REQUIRE(float_array[i] == static_cast<float>(vertex.id()) + i);
        }
        count++;
      }
      REQUIRE(count == 903);
    }
  }

  SECTION("EdgesCollection") {
    std::string src_type = "person", edge_type = "knows", dst_type = "person";
    // iterate edges of vertex chunk 0
    auto expect =
        EdgesCollection::Make(graph_info, src_type, edge_type, dst_type,
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
        EdgesCollection::Make(graph_info, src_type, edge_type, dst_type,
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
        EdgesCollection::Make(graph_info, src_type, edge_type, dst_type,
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
        EdgesCollection::Make(graph_info, src_type, edge_type, dst_type,
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
        EdgesCollection::Make(graph_info, src_type, edge_type, dst_type,
                              AdjListType::unordered_by_dest);
    REQUIRE(expect4.status().IsInvalid());

    // EdgeIter synchronization tests - prevent regression of property misalignment bug
    // These tests ensure property readers stay synchronized with iterator state
    INFO("Testing EdgeIter synchronization to prevent property misalignment");
    
    // Test 1: Sequential vs segmented iteration consistency
    // Original bug: segmented iteration could return different results than sequential
    std::vector<std::string> reference_dates;
    std::vector<IdType> reference_sources;
    auto it_ref = edges2->begin();
    const size_t SYNC_TEST_SIZE = std::min(static_cast<size_t>(50), edges2->size());
    
    // Collect reference data with sequential iteration
    for (size_t i = 0; i < SYNC_TEST_SIZE && it_ref != edges2->end(); ++i, ++it_ref) {
      reference_dates.push_back(it_ref.property<std::string>("creationDate").value());
      reference_sources.push_back(it_ref.source());
    }
    
    // Verify segmented iteration returns same results
    for (size_t target_pos = 0; target_pos < SYNC_TEST_SIZE; ++target_pos) {
      auto it_seg = edges2->begin();
      for (size_t pos = 0; pos < target_pos && it_seg != edges2->end(); ++pos, ++it_seg) {}
      
      if (it_seg != edges2->end()) {
        REQUIRE(it_seg.property<std::string>("creationDate").value() == reference_dates[target_pos]);
        REQUIRE(it_seg.source() == reference_sources[target_pos]);
      }
    }
    
    // Test 2: Multiple property access consistency
    // Original bug: repeated property() calls could return different values
    auto it_multi = edges2->begin();
    for (int i = 0; i < 20 && it_multi != edges2->end(); ++i, ++it_multi) {
      auto date1 = it_multi.property<std::string>("creationDate").value();
      auto date2 = it_multi.property<std::string>("creationDate").value();
      auto date3 = it_multi.property<std::string>("creationDate").value();
      
      REQUIRE(date1 == date2);
      REQUIRE(date2 == date3);
    }
    
    // Test 3: Iterator property vs Edge property consistency
    // Original bug: it.property() and (*it).property() could diverge
    auto it_consistency = edges2->begin();
    for (int i = 0; i < 20 && it_consistency != edges2->end(); ++i, ++it_consistency) {
      auto iter_date = it_consistency.property<std::string>("creationDate").value();
      auto edge = *it_consistency;
      auto edge_date = edge.property<std::string>("creationDate").value();
      
      REQUIRE(iter_date == edge_date);
      REQUIRE(it_consistency.source() == edge.source());
      REQUIRE(it_consistency.destination() == edge.destination());
    }
    
    // Test 4: Multiple iterator independence
    // Original bug: shared property readers could cause cross-contamination
    auto it1 = edges2->begin();
    auto it2 = edges2->begin();
    
    // Advance iterators to different positions
    for (int i = 0; i < 5 && it1 != edges2->end(); ++i, ++it1) {}
    for (int i = 0; i < 15 && it2 != edges2->end(); ++i, ++it2) {}
    
    if (it1 != edges2->end() && it2 != edges2->end()) {
      auto date1_before = it1.property<std::string>("creationDate").value();
      
      // Access it2's properties (this could affect it1 if readers are shared)
      volatile auto temp_date2 = it2.property<std::string>("creationDate").value();
      volatile auto temp_src2 = it2.source();
      (void)temp_date2; (void)temp_src2;
      
      // Verify it1's state hasn't changed
      auto date1_after = it1.property<std::string>("creationDate").value();
      REQUIRE(date1_before == date1_after);
    }
    
    // Test 5: Chunk boundary behavior
    // Original bug: crossing chunk boundaries could cause sync issues
    auto it_boundary = edges2->begin();
    std::string last_date;
    size_t boundary_count = 0;
    const size_t MAX_BOUNDARY_TEST = std::min(static_cast<size_t>(100), edges2->size());
    
    while (it_boundary != edges2->end() && boundary_count < MAX_BOUNDARY_TEST) {
      // Each position should have consistent property access
      auto current_date = it_boundary.property<std::string>("creationDate").value();
      auto repeat_date = it_boundary.property<std::string>("creationDate").value();
      
      REQUIRE(current_date == repeat_date);
      
      // Verify edge object consistency
      auto edge = *it_boundary;
      auto edge_date = edge.property<std::string>("creationDate").value();
      REQUIRE(current_date == edge_date);
      
      last_date = current_date;
      ++it_boundary;
      ++boundary_count;
    }
    
    REQUIRE(boundary_count > 0); // Ensure we actually tested something
  }

  SECTION("ValidateProperty") {
    // read file and construct graph info
    std::string path = test_data_dir + "/neo4j/MovieGraph.graph.yml";
    auto maybe_graph_info = GraphInfo::Load(path);
    REQUIRE(maybe_graph_info.status().ok());
    auto graph_info = maybe_graph_info.value();
    // get vertices collection
    std::string type = "Person", property = "born";
    auto maybe_vertices_collection = VerticesCollection::Make(graph_info, type);
    REQUIRE(!maybe_vertices_collection.has_error());
    auto vertices = maybe_vertices_collection.value();
    // the count of valid property value
    auto count = 0;
    for (auto it = vertices->begin(); it != vertices->end(); ++it) {
      // get a vertex and access its data
      auto vertex = *it;
      // property not exists
      REQUIRE_THROWS_AS(vertex.IsValid("bornn"), std::invalid_argument);
      if (vertex.IsValid(property)) {
        REQUIRE(vertex.property<int64_t>(property).value() != 0);
        count++;
      } else {
        std::cout << "the property is not valid" << std::endl;
      }
    }
    REQUIRE(count == 128);
    auto last_invalid_vertex = *(vertices->end() + -1);
    REQUIRE(last_invalid_vertex.property<int64_t>(property).has_error());
  }
}
}  // namespace graphar
