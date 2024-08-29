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

#include "arrow/api.h"
#include "arrow/filesystem/api.h"

#include "./config.h"
#include "graphar/api/high_level_reader.h"

void vertices_collection(
    const std::shared_ptr<graphar::GraphInfo>& graph_info) {
  // construct vertices collection
  std::string type = "person", property = "firstName";
  auto maybe_vertices_collection =
      graphar::VerticesCollection::Make(graph_info, type);
  ASSERT(!maybe_vertices_collection.has_error());
  auto vertices = maybe_vertices_collection.value();

  // use vertices collection
  size_t count = 0;
  // iterate through vertices collection
  for (auto it = vertices->begin(); it != vertices->end(); ++it) {
    count++;
    // print the first 10 vertices
    if (count > 10) {
      continue;
    }
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
  }
  // add operator+ for iterator
  auto it_last = vertices->begin() + (count - 1);
  std::cout << "the last vertex: " << std::endl;
  std::cout << it_last.id()
            << ", id=" << it_last.property<int64_t>("id").value()
            << ", firstName="
            << it_last.property<std::string>("firstName").value() << std::endl;
  // find the vertex with internal id = 100
  auto it_find = vertices->find(100);
  std::cout << "the vertex with internal id = 100: " << std::endl;
  std::cout << it_find.id()
            << ", id=" << it_find.property<int64_t>("id").value()
            << ", firstName="
            << it_find.property<std::string>("firstName").value() << std::endl;
  // count
  ASSERT(count == vertices->size());
  std::cout << "vertex_count=" << count << std::endl;
}

void edges_collection(const std::shared_ptr<graphar::GraphInfo>& graph_info) {
  // construct edges collection
  std::string src_type = "person", edge_type = "knows", dst_type = "person";
  auto expect =
      graphar::EdgesCollection::Make(graph_info, src_type, edge_type, dst_type,
                                     graphar::AdjListType::ordered_by_source);
  ASSERT(!expect.has_error());
  auto edges = expect.value();

  // use edges collection
  auto begin = edges->begin();
  auto end = edges->end();
  size_t count = 0;
  // iterate through edges collection
  for (auto it = begin; it != end; ++it) {
    count++;
    // print the first 10 edges
    if (count > 10) {
      continue;
    }
    // access data through iterator directly
    std::cout << "src=" << it.source() << ", dst=" << it.destination() << "; ";
    // access data through edge
    auto edge = *it;
    std::cout << "src=" << edge.source() << ", dst=" << edge.destination()
              << ", creationDate="
              << edge.property<std::string>("creationDate").value()
              << std::endl;
  }
  // find the first edge with source = 100
  auto it_find = edges->find_src(100, begin);
  std::cout << "the edge with source = 100: " << std::endl;
  do {
    std::cout << "src=" << it_find.source() << ", dst=" << it_find.destination()
              << ", creationDate="
              << it_find.property<std::string>("creationDate").value()
              << std::endl;
  } while (it_find.next_src());

  // count
  ASSERT(count == edges->size());
  std::cout << "edge_count=" << count << std::endl;
}

int main(int argc, char* argv[]) {
  // read file and construct graph info
  std::string path =
      GetTestingResourceRoot() + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto graph_info = graphar::GraphInfo::Load(path).value();

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
