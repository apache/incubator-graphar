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

#include "grin/example/config.h"

extern "C" {
#include "grin/include/property/property.h"
#include "grin/include/property/propertylist.h"
#include "grin/include/property/topology.h"
#include "grin/include/property/type.h"
#include "grin/include/topology/adjacentlist.h"
#include "grin/include/topology/edgelist.h"
#include "grin/include/topology/structure.h"
#include "grin/include/topology/vertexlist.h"
}

void test_vertex_properties(GRIN_GRAPH graph, bool print_result = false) {
  std::cout << "++++ Test vertex properties ++++" << std::endl;

  auto vertex_list = grin_get_vertex_list(graph);

  // get vertex type list
  auto vertex_type_list = grin_get_vertex_type_list(graph);
  size_t n = grin_get_vertex_type_list_size(graph, vertex_type_list);

  for (auto i = 0; i < n; i++) {
    // get vertex type
    auto vertex_type =
        grin_get_vertex_type_from_list(graph, vertex_type_list, i);
    auto label = grin_get_vertex_type_name(graph, vertex_type);
    size_t m = grin_get_vertex_num_by_type(graph, vertex_type);
    std::cout << "vertex type: " << label << ", vertex num: " << m << std::endl;

    // get property list by vertex type
    auto property_list =
        grin_get_vertex_property_list_by_type(graph, vertex_type);
    size_t vpn = grin_get_vertex_property_list_size(graph, property_list);

    // select type for vertex list
    auto select_vertex_list =
        grin_select_type_for_vertex_list(graph, vertex_type, vertex_list);

    auto it = grin_get_vertex_list_begin(graph, select_vertex_list);
    while (grin_is_vertex_list_end(graph, it) == false) {
      auto v = grin_get_vertex_from_iter(graph, it);
      for (auto idx = 0; idx < vpn; idx++) {
        auto property =
            grin_get_vertex_property_from_list(graph, property_list, idx);
        auto data_type = grin_get_vertex_property_datatype(graph, property);
        switch (data_type) {
        case GRIN_DATATYPE::Int32: {
          auto value =
              grin_get_vertex_property_value_of_int32(graph, v, property);
          if (print_result)
            std::cout << value << "; ";
          break;
        }
        case GRIN_DATATYPE::Int64: {
          auto value =
              grin_get_vertex_property_value_of_int64(graph, v, property);
          if (print_result)
            std::cout << value << "; ";
          break;
        }
        case GRIN_DATATYPE::Float: {
          auto value =
              grin_get_vertex_property_value_of_float(graph, v, property);
          if (print_result)
            std::cout << value << "; ";
          break;
        }
        case GRIN_DATATYPE::Double: {
          auto value =
              grin_get_vertex_property_value_of_double(graph, v, property);
          if (print_result)
            std::cout << value << "; ";
          break;
        }
        case GRIN_DATATYPE::String: {
          auto value =
              grin_get_vertex_property_value_of_string(graph, v, property);
          if (print_result)
            std::cout << value << "; ";
          grin_destroy_string_value(graph, value);
          break;
        }
        default:
          std::cout << "Unsupported data type." << std::endl;
        }
        grin_destroy_vertex_property(graph, property);
      }
      if (print_result) {
        std::cout << std::endl;
      }
      grin_destroy_vertex(graph, v);
      grin_get_next_vertex_list_iter(graph, it);
    }

    std::cout << std::endl;
    // destroy
    grin_destroy_vertex_list_iter(graph, it);
    grin_destroy_vertex_type(graph, vertex_type);
    grin_destroy_vertex_list(graph, select_vertex_list);
    grin_destroy_vertex_property_list(graph, property_list);
  }

  // destroy vertex list
  grin_destroy_vertex_list(graph, vertex_list);

  std::cout << "--- Test vertex properties ---" << std::endl;
}

void test_edge_properties(GRIN_GRAPH graph, bool print_result = false) {
  std::cout << "\n++++ Test edge properties ++++" << std::endl;

  auto edge_list = grin_get_edge_list(graph);

  // get edge type list
  auto edge_type_list = grin_get_edge_type_list(graph);
  size_t n = grin_get_edge_type_list_size(graph, edge_type_list);

  for (int i = 0; i < n; i++) {
    // get edge type
    auto edge_type = grin_get_edge_type_from_list(graph, edge_type_list, i);
    auto label = grin_get_edge_type_name(graph, edge_type);
    size_t m = grin_get_edge_num_by_type(graph, edge_type);
    std::cout << "edge type: " << label << ", edge num: " << m << std::endl;

    // get property list by edge type
    auto property_list = grin_get_edge_property_list_by_type(graph, edge_type);
    size_t epn = grin_get_edge_property_list_size(graph, property_list);

    // select type for edge list
    auto select_edge_list =
        grin_select_type_for_edge_list(graph, edge_type, edge_list);

    auto it = grin_get_edge_list_begin(graph, select_edge_list);
    while (grin_is_edge_list_end(graph, it) == false) {
      auto e = grin_get_edge_from_iter(graph, it);
      for (auto idx = 0; idx < epn; idx++) {
        auto property =
            grin_get_edge_property_from_list(graph, property_list, idx);
        auto data_type = grin_get_edge_property_datatype(graph, property);
        switch (data_type) {
        case GRIN_DATATYPE::Int32: {
          auto value =
              grin_get_edge_property_value_of_int32(graph, e, property);
          if (print_result)
            std::cout << value << "; ";
          break;
        }
        case GRIN_DATATYPE::Int64: {
          auto value =
              grin_get_edge_property_value_of_int64(graph, e, property);
          if (print_result)
            std::cout << value << "; ";
          break;
        }
        case GRIN_DATATYPE::Float: {
          auto value =
              grin_get_edge_property_value_of_float(graph, e, property);
          if (print_result)
            std::cout << value << "; ";
          break;
        }
        case GRIN_DATATYPE::Double: {
          auto value =
              grin_get_edge_property_value_of_double(graph, e, property);
          if (print_result)
            std::cout << value << "; ";
          break;
        }
        case GRIN_DATATYPE::String: {
          auto value =
              grin_get_edge_property_value_of_string(graph, e, property);
          if (print_result)
            std::cout << value << "; ";
          grin_destroy_string_value(graph, value);
          break;
        }
        default:
          std::cout << "Unsupported data type." << std::endl;
        }
        grin_destroy_edge_property(graph, property);
      }
      if (print_result) {
        std::cout << std::endl;
      }
      grin_destroy_edge(graph, e);
      grin_get_next_edge_list_iter(graph, it);
    }
    std::cout << std::endl;

    // destroy
    grin_destroy_edge_list_iter(graph, it);
    grin_destroy_edge_type(graph, edge_type);
    grin_destroy_edge_list(graph, select_edge_list);
    grin_destroy_edge_property_list(graph, property_list);
  }

  std::cout << "--- Test edge properties ---" << std::endl;
}

int main(int argc, char* argv[]) {
  // get graph from graph info of GraphAr
  std::string path = PROPERTY_TEST_DATA_PATH;
  std::cout << "GraphInfo path = " << path << std::endl;

  char** args = new char*[1];
  args[0] = new char[path.length() + 1];
  snprintf(args[0], path.length() + 1, "%s", path.c_str());
  auto init_start = clock();
  GRIN_GRAPH graph = grin_get_graph_from_storage(1, args);
  delete[] args[0];
  delete[] args;
  auto init_time = 1000.0 * (clock() - init_start) / CLOCKS_PER_SEC;

  // test vertex properties
  auto run_start = clock();
  test_vertex_properties(graph);
  auto vertex_run_time = 1000.0 * (clock() - run_start) / CLOCKS_PER_SEC;

  // test edge properties
  run_start = clock();
  test_edge_properties(graph);
  auto edge_run_time = 1000.0 * (clock() - run_start) / CLOCKS_PER_SEC;

  // print run time
  std::cout << "Init time for building graph with GRIN = " << init_time << " ms"
            << std::endl;
  std::cout << "Run time for vertex properties with GRIN = " << vertex_run_time
            << " ms" << std::endl;
  std::cout << "Run time for edge properties with GRIN = " << edge_run_time
            << " ms" << std::endl;

  return 0;
}
