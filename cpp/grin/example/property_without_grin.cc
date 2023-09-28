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

#include <chrono>  // NOLINT
#include <iostream>

#include "gar/graph.h"
#include "gar/graph_info.h"
#include "grin/example/config.h"

void test_vertex_properties(GAR_NAMESPACE::GraphInfo graph_info,
                            bool print_result = false) {
  std::cout << "++++ Test vertex properties ++++" << std::endl;

  for (const auto& [label, vertex_info] : graph_info.GetVertexInfos()) {
    // construct vertices collection
    auto maybe_vertices_collection =
        GAR_NAMESPACE::ConstructVerticesCollection(graph_info, label);
    auto& vertices = maybe_vertices_collection.value();
    std::cout << "vertex type: " << label << ", vertex num: " << vertices.size()
              << std::endl;

    auto it_end = vertices.end();
    for (auto it = vertices.begin(); it != it_end; ++it) {
      auto vertex = *it;
      for (auto& group : vertex_info.GetPropertyGroups()) {
        for (auto& property : group.GetProperties()) {
          auto& name = property.name;
          auto& type = property.type;
          switch (type.id()) {
          case GAR_NAMESPACE::Type::INT32: {
            auto value = vertex.property<int32_t>(name).value();
            if (print_result)
              std::cout << value << "; ";
            break;
          }
          case GAR_NAMESPACE::Type::INT64: {
            auto value = vertex.property<int64_t>(name).value();
            if (print_result)
              std::cout << value << "; ";
            break;
          }
          case GAR_NAMESPACE::Type::FLOAT: {
            auto value = vertex.property<float>(name).value();
            if (print_result)
              std::cout << value << "; ";
            break;
          }
          case GAR_NAMESPACE::Type::DOUBLE: {
            auto value = vertex.property<double>(name).value();
            if (print_result)
              std::cout << value << "; ";
            break;
          }
          case GAR_NAMESPACE::Type::STRING: {
            auto s = vertex.property<std::string>(name).value();
            int len = s.length() + 1;
            char* value = new char[len];
            snprintf(value, len, "%s", s.c_str());
            if (print_result)
              std::cout << value << "; ";
            break;
          }
          default:
            std::cout << "Unsupported data type." << std::endl;
          }
        }
      }
      if (print_result) {
        std::cout << std::endl;
      }
    }
    std::cout << std::endl;
  }

  std::cout << "--- Test vertex properties ---" << std::endl;
}

void test_edge_properties(GAR_NAMESPACE::GraphInfo graph_info,
                          bool print_result = false) {
  std::cout << "\n++++ Test edge properties ++++" << std::endl;

  for (const auto& [label, edge_info] : graph_info.GetEdgeInfos()) {
    // construct edges collection
    auto src_label = edge_info.GetSrcLabel();
    auto dst_label = edge_info.GetDstLabel();
    auto edge_label = edge_info.GetEdgeLabel();
    auto adj_list_type = GAR_NAMESPACE::AdjListType::ordered_by_source;
    auto expect = GAR_NAMESPACE::ConstructEdgesCollection(
        graph_info, src_label, edge_label, dst_label, adj_list_type);
    auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
        GAR_NAMESPACE::AdjListType::ordered_by_source>>(expect.value());
    std::cout << "edge type: " << edge_label << ", edge num: " << edges.size()
              << std::endl;

    auto it_end = edges.end();
    for (auto it = edges.begin(); it != it_end; ++it) {
      auto edge = *it;
      for (auto& group : edge_info.GetPropertyGroups(adj_list_type).value()) {
        for (auto& property : group.GetProperties()) {
          auto& name = property.name;
          auto& type = property.type;
          switch (type.id()) {
          case GAR_NAMESPACE::Type::INT32: {
            auto value = edge.property<int32_t>(name).value();
            if (print_result)
              std::cout << value << "; ";
            break;
          }
          case GAR_NAMESPACE::Type::INT64: {
            auto value = edge.property<int64_t>(name).value();
            if (print_result)
              std::cout << value << "; ";
            break;
          }
          case GAR_NAMESPACE::Type::FLOAT: {
            auto value = edge.property<float>(name).value();
            if (print_result)
              std::cout << value << "; ";
            break;
          }
          case GAR_NAMESPACE::Type::DOUBLE: {
            auto value = edge.property<double>(name).value();
            if (print_result)
              std::cout << value << "; ";
            break;
          }
          case GAR_NAMESPACE::Type::STRING: {
            auto s = edge.property<std::string>(name).value();
            int len = s.length() + 1;
            char* value = new char[len];
            snprintf(value, len, "%s", s.c_str());
            if (print_result)
              std::cout << value << "; ";
            break;
          }
          default:
            std::cout << "Unsupported data type." << std::endl;
          }
        }
      }
      if (print_result)
        std::cout << std::endl;
    }
    std::cout << std::endl;
  }

  std::cout << "--- Test edge properties ---" << std::endl;
}

int main(int argc, char* argv[]) {
  // read file and construct graph info
  std::string path = PROPERTY_TEST_DATA_PATH;
  std::cout << "GraphInfo path = " << path << std::endl;
  auto graph_info = GAR_NAMESPACE::GraphInfo::Load(path).value();

  // test vertex properties
  auto run_start = std::chrono::high_resolution_clock::now();
  test_vertex_properties(graph_info);
  auto run_end = std::chrono::high_resolution_clock::now();
  auto vertex_run_time = std::chrono::duration_cast<std::chrono::milliseconds>(
      run_end - run_start);

  // test edge properties
  run_start = std::chrono::high_resolution_clock::now();
  test_edge_properties(graph_info);
  run_end = std::chrono::high_resolution_clock::now();
  auto edge_run_time = std::chrono::duration_cast<std::chrono::milliseconds>(
      run_end - run_start);

  // print run time
  std::cout << "Run time for vertex properties without GRIN = "
            << vertex_run_time.count() << " ms" << std::endl;
  std::cout << "Run time for edge properties without GRIN = "
            << edge_run_time.count() << " ms" << std::endl;

  return 0;
}
