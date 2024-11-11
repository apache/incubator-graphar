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
#include "graphar/api/arrow_reader.h"
#include "graphar/api/high_level_reader.h"

void vertices_collection(
    const std::shared_ptr<graphar::GraphInfo>& graph_info) {
  std::string type = "organisation";
  auto vertex_info = graph_info->GetVertexInfo("organisation");
  auto labels = vertex_info->GetLabels();

  std::cout << "Query vertices with a specific label" << std::endl;
  std::cout << "--------------------------------------" << std::endl;

  auto maybe_filter_vertices_collection =
      graphar::VerticesCollection::verticesWithLabel(std::string("company"),
                                                     graph_info, type);

  ASSERT(!maybe_filter_vertices_collection.has_error());
  auto filter_vertices = maybe_filter_vertices_collection.value();
  std::cout << "valid vertices num: " << filter_vertices->size() << std::endl;

  std::cout << std::endl;
  std::cout << "Query vertices with specific label in a filtered vertices set"
            << std::endl;
  std::cout << "--------------------------------------" << std::endl;

  auto maybe_filter_vertices_collection_2 =
      graphar::VerticesCollection::verticesWithLabel(std::string("public"),
                                                     filter_vertices);
  ASSERT(!maybe_filter_vertices_collection_2.has_error());
  auto filter_vertices_2 = maybe_filter_vertices_collection_2.value();
  std::cout << "valid vertices num: " << filter_vertices_2->size() << std::endl;

  std::cout << std::endl;
  std::cout << "Test vertices with multi labels" << std::endl;
  std::cout << "--------------------------------------" << std::endl;
  auto maybe_filter_vertices_collection_3 =
      graphar::VerticesCollection::verticesWithMultipleLabels(
          {"company", "public"}, graph_info, type);
  ASSERT(!maybe_filter_vertices_collection_3.has_error());
  auto filter_vertices_3 = maybe_filter_vertices_collection_3.value();
  std::cout << "valid vertices num: " << filter_vertices_3->size() << std::endl;

  for (auto it = filter_vertices_3->begin(); it != filter_vertices_3->end();
       ++it) {
    // get a node's all labels
    auto label_result = it.label();
    std::cout << "id: " << it.id() << " ";
    if (!label_result.has_error()) {
      for (auto label : label_result.value()) {
        std::cout << label << " ";
      }
    }
    std::cout << "name: ";
    auto property = it.property<std::string>("name").value();
    std::cout << property << " ";
    std::cout << std::endl;
  }
  std::cout << std::endl;

  std::cout << "Test vertices with property in a filtered vertices set"
            << std::endl;
  std::cout << "--------------------------------------" << std::endl;
  auto filter = graphar::_Equal(graphar::_Property("name"),
                                graphar::_Literal("Safi_Airways"));
  auto maybe_filter_vertices_collection_4 =
      graphar::VerticesCollection::verticesWithProperty(
          std::string("name"), filter, graph_info, type);
  ASSERT(!maybe_filter_vertices_collection_4.has_error());
  auto filter_vertices_4 = maybe_filter_vertices_collection_4.value();
  std::cout << "valid vertices num: " << filter_vertices_4->size() << std::endl;

  for (auto it = filter_vertices_4->begin(); it != filter_vertices_4->end();
       ++it) {
    // get a node's all labels
    auto label_result = it.label();
    std::cout << "id: " << it.id() << " ";
    if (!label_result.has_error()) {
      for (auto label : label_result.value()) {
        std::cout << label << " ";
      }
    }
    std::cout << "name: ";
    auto property = it.property<std::string>("name").value();
    std::cout << property << " ";
    std::cout << std::endl;
  }

  std::cout << "Test vertices with property" << std::endl;
  std::cout << "--------------------------------------" << std::endl;
  auto filter_2 =
      graphar::_Equal(graphar::_Property("name"), graphar::_Literal("Kam_Air"));
  auto maybe_filter_vertices_collection_5 =
      graphar::VerticesCollection::verticesWithProperty(
          std::string("name"), filter_2, filter_vertices_3);
  ASSERT(!maybe_filter_vertices_collection_5.has_error());
  auto filter_vertices_5 = maybe_filter_vertices_collection_5.value();
  std::cout << "valid vertices num: " << filter_vertices_5->size() << std::endl;

  for (auto it = filter_vertices_5->begin(); it != filter_vertices_5->end();
       ++it) {
    // get a node's all labels
    auto label_result = it.label();
    std::cout << "id: " << it.id() << " ";
    if (!label_result.has_error()) {
      for (auto label : label_result.value()) {
        std::cout << label << " ";
      }
    }
    std::cout << "name: ";
    auto property = it.property<std::string>("name").value();
    std::cout << property << " ";
    std::cout << std::endl;
  }
}
int main(int argc, char* argv[]) {
  // read file and construct graph info
  std::string path = GetTestingResourceRoot() + "/ldbc/parquet/ldbc.graph.yml";
  auto graph_info = graphar::GraphInfo::Load(path).value();

  // vertices collection
  std::cout << "Vertices collection" << std::endl;
  std::cout << "-------------------" << std::endl;
  vertices_collection(graph_info);
  std::cout << std::endl;
}
