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

#include "grin/predefine.h"
#include "grin/test/config.h"

// GRIN headers
#include "common/error.h"
#include "property/property.h"
#include "property/propertylist.h"
#include "property/row.h"
#include "property/topology.h"
#include "property/type.h"
#include "topology/adjacentlist.h"
#include "topology/edgelist.h"
#include "topology/structure.h"
#include "topology/vertexlist.h"

void test_property_row(GRIN_GRAPH graph) {
  std::cout << "\n++++ test property: row ++++" << std::endl;

  // create row
  auto row = grin_create_row(graph);

  // insert value to row
  int32_t value0 = 0;
  const char* value1 = "Test String";
  uint64_t value2 = 2;
  double value3 = 3.3;

  std::cout << "put value0: " << value0 << std::endl;
  std::cout << "put value1: " << value1 << std::endl;
  std::cout << "put value2: " << value2 << std::endl;
  std::cout << "put value3: " << value3 << std::endl;
  auto status = grin_insert_int32_to_row(graph, row, value0);
  ASSERT(status == true);
  status = grin_insert_string_to_row(graph, row, value1);
  ASSERT(status == true);
  status = grin_insert_uint64_to_row(graph, row, value2);
  ASSERT(status == true);
  status = grin_insert_double_to_row(graph, row, value3);
  ASSERT(status == true);

  // get value from row
  auto value0_ = grin_get_int32_from_row(graph, row, 0);
  auto value1_ = grin_get_string_from_row(graph, row, 1);
  auto invalid_value = grin_get_float_from_row(graph, row, 100);
  ASSERT(grin_get_last_error_code() == INVALID_VALUE && invalid_value == 0.0);
  auto value2_ = grin_get_uint64_from_row(graph, row, 2);
  auto value3_ = grin_get_double_from_row(graph, row, 3);
  ASSERT(grin_get_last_error_code() == NO_ERROR);

  // check value
  std::cout << "get value0: " << value0_ << std::endl;
  std::cout << "get value1: " << value1_ << std::endl;
  std::cout << "get value2: " << value2_ << std::endl;
  std::cout << "get value3: " << value3_ << std::endl;
  ASSERT(value0_ == value0);
  ASSERT(strcmp(value1_, value1) == 0);
  ASSERT(value2_ == value2);
  ASSERT(value3_ == value3);

  // destroy
  grin_destroy_string_value(graph, value1_);
  grin_destroy_row(graph, row);

  std::cout << "---- test property: row completed ----" << std::endl;
}

void test_property_vertex(GRIN_GRAPH graph) {
  std::cout << "\n++++ test property: vertex ++++" << std::endl;

  // get vertex type list
  auto vertex_type_list = grin_get_vertex_type_list(graph);
  size_t n = grin_get_vertex_type_list_size(graph, vertex_type_list);
  std::cout << "size of vertex type list = " << n << std::endl;

  for (auto i = 0; i < n; i++) {
    std::cout << "== vertex type " << i << ": ==" << std::endl;
    auto vertex_type =
        grin_get_vertex_type_from_list(graph, vertex_type_list, i);

    // select type for vertex list
    auto select_vertex_list = grin_get_vertex_list_by_type(graph, vertex_type);

    // get property list by vertex type
    auto property_list =
        grin_get_vertex_property_list_by_type(graph, vertex_type);

    if (grin_get_vertex_list_size(graph, select_vertex_list) > 0 &&
        grin_get_vertex_property_list_size(graph, property_list) > 0) {
      // get vertex from vertex list
      auto vertex = grin_get_vertex_from_list(graph, select_vertex_list, 0);

      // get property from property list
      auto property =
          grin_get_vertex_property_from_list(graph, property_list, 0);
      auto name = grin_get_vertex_property_name(graph, vertex_type, property);
      auto data_type = grin_get_vertex_property_datatype(graph, property);
      std::cout << "get value of property \"" << name << "\" for vertex 0"
                << std::endl;

      // get row
      auto r = grin_get_vertex_row(graph, vertex);

      // check value from row and from property (int64)
      if (data_type == GRIN_DATATYPE::Int64) {
        auto value1 = grin_get_int64_from_row(graph, r, 0);
        auto value2 =
            grin_get_vertex_property_value_of_int64(graph, vertex, property);
        ASSERT(value1 == value2);
        std::cout << "value of property \"" << name
                  << "\" for vertex 0 of vertex type " << i << ": " << value1
                  << std::endl;
        std::cout << "check values from row and from property are equal (int64)"
                  << std::endl;
      }

      // check value from row and from property (string)
      if (data_type == GRIN_DATATYPE::String) {
        auto value1 = grin_get_string_from_row(graph, r, 0);
        auto value2 =
            grin_get_vertex_property_value_of_string(graph, vertex, property);
        ASSERT(grin_get_last_error_code() == NO_ERROR);
        ASSERT(strcmp(value1, value2) == 0);

        std::cout << "value of property \"" << name
                  << "\" for vertex 0 of vertex type " << i << ": " << value1
                  << std::endl;
        std::cout
            << "check values from row and from property are equal (string)"
            << std::endl;

        // destroy
        grin_destroy_string_value(graph, value1);
        grin_destroy_string_value(graph, value2);
      }

      // destroy
      grin_destroy_row(graph, r);
      grin_destroy_vertex(graph, vertex);
      grin_destroy_vertex_property(graph, property);
    }

    // destroy
    grin_destroy_vertex_property_list(graph, property_list);
    grin_destroy_vertex_list(graph, select_vertex_list);
    grin_destroy_vertex_type(graph, vertex_type);
  }

  // destroy
  grin_destroy_vertex_type_list(graph, vertex_type_list);

  std::cout << "---- test property: vertex completed ----" << std::endl;
}

void test_property_edge(GRIN_GRAPH graph) {
  std::cout << "\n++++ test property: edge ++++" << std::endl;

  // get edge type list
  auto edge_type_list = grin_get_edge_type_list(graph);
  size_t n = grin_get_edge_type_list_size(graph, edge_type_list);
  std::cout << "size of edge type list = " << n << std::endl;

  for (auto i = 0; i < n; i++) {
    std::cout << "== edge type " << i << ": ==" << std::endl;
    auto edge_type = grin_get_edge_type_from_list(graph, edge_type_list, i);

    // select type for edge list
    auto select_edge_list = grin_get_edge_list_by_type(graph, edge_type);

    // get property list by edge type
    auto property_list = grin_get_edge_property_list_by_type(graph, edge_type);
    auto it = grin_get_edge_list_begin(graph, select_edge_list);

    if (grin_is_edge_list_end(graph, it) == false &&
        grin_get_edge_property_list_size(graph, property_list) > 0) {
      // get edge from edge list iter
      auto edge = grin_get_edge_from_iter(graph, it);

      // get property from property list
      auto property = grin_get_edge_property_from_list(graph, property_list, 0);

      auto name = grin_get_edge_property_name(graph, edge_type, property);
      auto data_type = grin_get_edge_property_datatype(graph, property);
      std::cout << "get value of property \"" << name << "\" for edge 0"
                << std::endl;

      // get row
      auto r = grin_get_edge_row(graph, edge);

      // check value from row and from property (int64)
      if (data_type == GRIN_DATATYPE::Int64) {
        auto value1 = grin_get_int64_from_row(graph, r, 0);
        auto value2 =
            grin_get_edge_property_value_of_int64(graph, edge, property);
        ASSERT(grin_get_last_error_code() == NO_ERROR);
        ASSERT(value1 == value2);
        std::cout << "value of property \"" << name
                  << "\" for edge 0 of edge type " << i << ": " << value1
                  << std::endl;
        std::cout << "check values from row and from property are equal (int64)"
                  << std::endl;
      }

      // check value from row and from property (string)
      if (data_type == GRIN_DATATYPE::String) {
        auto value1 = grin_get_string_from_row(graph, r, 0);
        auto value2 =
            grin_get_edge_property_value_of_string(graph, edge, property);
        ASSERT(grin_get_last_error_code() == NO_ERROR);
        ASSERT(strcmp(value1, value2) == 0);
        std::cout << "value of property \"" << name
                  << "\" for edge 0 of edge type " << i << ": " << value1
                  << std::endl;
        std::cout
            << "check values from row and from property are equal (string)"
            << std::endl;

        // destroy
        grin_destroy_string_value(graph, value1);
        grin_destroy_string_value(graph, value2);
      }

      // destroy
      grin_destroy_row(graph, r);
      grin_destroy_edge(graph, edge);
      grin_destroy_edge_property(graph, property);
    }

    // destroy
    grin_destroy_edge_property_list(graph, property_list);
    grin_destroy_edge_list_iter(graph, it);
    grin_destroy_edge_list(graph, select_edge_list);
    grin_destroy_edge_type(graph, edge_type);
  }

  // destroy
  grin_destroy_edge_type_list(graph, edge_type_list);

  std::cout << "---- test property: edge completed ----" << std::endl;
}

int main(int argc, char* argv[]) {
  // get graph from graph info of GraphAr
  std::string path = "graphar://" + TEST_DATA_PATH;
  std::cout << "graph uri = " << path << std::endl;

  char* uri = new char[path.length() + 1];
  snprintf(uri, path.length() + 1, "%s", path.c_str());
  GRIN_GRAPH graph = grin_get_graph_from_storage(uri);
  delete[] uri;

  // test property row
  test_property_row(graph);

  // test property for vertex
  test_property_vertex(graph);

  // test property for edge
  test_property_edge(graph);

  // destroy graph
  grin_destroy_graph(graph);

  return 0;
}
