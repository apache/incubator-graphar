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

  SECTION("EdgeIterator") {
    std::string src_type = "person", edge_type = "knows", dst_type = "person";
    auto expect =
        EdgesCollection::Make(graph_info, src_type, edge_type, dst_type,
                              AdjListType::ordered_by_source);
    REQUIRE(!expect.has_error());
    auto edges = expect.value();

    // Test iterator functionality
    auto begin = edges->begin();
    auto end = edges->end();
    size_t count = 0;

    // Iterate through first 2000 edges
    for (auto it = begin; it != end; ++it) {
      if (count >= 2000) {
        break;
      }
      count++;
      REQUIRE(it.source() >= 0);
      REQUIRE(it.destination() >= 0);
      REQUIRE(it.property<std::string>("creationDate").has_value());
    }
    REQUIRE(count == 2000);

    // Test skipping and iterating next 2000 edges
    auto begin2 = edges->begin();
    size_t i = 0;
    for (auto it = begin2; it != end; ++it, i++) {
      if (i < 2000) {
        continue;
      }
      if (i >= 4000) {
        break;
      }
      count++;
      REQUIRE(it.source() >= 0);
      REQUIRE(it.destination() >= 0);
      REQUIRE(it.property<std::string>("creationDate").has_value());
    }
    REQUIRE(count == 4000);

    // Test skipping and iterating next 2000 edges
    auto begin3 = edges->begin();
    size_t j = 0;
    for (auto it = begin3; it != end; ++it, j++) {
      if (j < 4000) {
        continue;
      }
      if (j >= 6000) {
        break;
      }
      count++;
      REQUIRE(it.source() >= 0);
      REQUIRE(it.destination() >= 0);
      REQUIRE(it.property<std::string>("creationDate").has_value());
    }
    REQUIRE(count == 6000);

    // Test iterating remaining edges
    auto begin4 = edges->begin();
    size_t k = 0;
    for (auto it = begin4; it != end; ++it, k++) {
      if (k < 6000) {
        continue;
      }
      count++;
      REQUIRE(it.source() >= 0);
      REQUIRE(it.destination() >= 0);
      REQUIRE(it.property<std::string>("creationDate").has_value());
    }

    // Verify total count matches collection size
    REQUIRE(count == edges->size());
    std::cout << "Total edge_count=" << count << std::endl;
  }

  SECTION("DateType") {
    std::string path_date =
        test_data_dir + "/ldbc_sample/parquet/ldbc_sample_date.graph.yml";
    auto maybe_graph_info_date = GraphInfo::Load(path_date);
    REQUIRE(maybe_graph_info_date.status().ok());
    auto graph_info_date = maybe_graph_info_date.value();
    std::string src_type = "person", edge_type = "knows-date",
                dst_type = "person";
    auto expect =
        EdgesCollection::Make(graph_info_date, src_type, edge_type, dst_type,
                              AdjListType::ordered_by_source);
    REQUIRE(!expect.has_error());
    auto edges = expect.value();

    // Expected values for the first ten creationDate-date entries
    int32_t expected_dates[10] = {14820, 15442, 14909, 15182, 15141,
                                  15058, 15155, 15135, 15364, 15455};
    size_t count = 0;
    for (auto it = edges->begin(); it != edges->end() && count < 10;
         ++it, ++count) {
      auto date_val = it.property<int32_t>("creationDate-date");
      REQUIRE(date_val.has_value());
      REQUIRE(date_val.value() == expected_dates[count]);
    }
    REQUIRE(count == 10);
    std::cout << "DateType edge_count=" << count << std::endl;
  }

  SECTION("TimestampType") {
    std::string path_timestamp =
        test_data_dir + "/ldbc_sample/parquet/ldbc_sample_timestamp.graph.yml";
    auto maybe_graph_info_timestamp = GraphInfo::Load(path_timestamp);
    REQUIRE(maybe_graph_info_timestamp.status().ok());
    auto graph_info_timestamp = maybe_graph_info_timestamp.value();
    std::string src_type = "person", edge_type = "knows-timestamp",
                dst_type = "person";
    auto expect =
        EdgesCollection::Make(graph_info_timestamp, src_type, edge_type,
                              dst_type, AdjListType::ordered_by_source);
    REQUIRE(!expect.has_error());
    auto edges = expect.value();

    // Expected values for the first ten creationDate-timestamp entries
    int64_t expected_timestamps[10] = {
        1280503193298LL, 1334239018931LL, 1288146786288LL, 1311781394869LL,
        1308223719623LL, 1301064563134LL, 1309416320777LL, 1307728039432LL,
        1327492287348LL, 1335389465259LL};
    size_t count = 0;
    for (auto it = edges->begin(); it != edges->end() && count < 10;
         ++it, ++count) {
      auto ts_val = it.property<int64_t>("creationDate-timestamp");
      REQUIRE(ts_val.has_value());
      REQUIRE(ts_val.value() == expected_timestamps[count]);
    }
    REQUIRE(count == 10);
    std::cout << "TimestampType edge_count=" << count << std::endl;
  }
}
}  // namespace graphar
