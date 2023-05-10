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

#include "grin/test/config.h"

extern "C" {
#include "grin/include/common/error.h"
#include "grin/include/property/property.h"
#include "grin/include/property/propertylist.h"
#include "grin/include/property/propertytable.h"
#include "grin/include/property/topology.h"
#include "grin/include/property/type.h"
#include "grin/include/topology/adjacentlist.h"
#include "grin/include/topology/edgelist.h"
#include "grin/include/topology/structure.h"
#include "grin/include/topology/vertexlist.h"
}

void test_property_table_row(GRIN_GRAPH graph) {
  std::cout << "\n++++ test property: table (row) ++++" << std::endl;

  // create row
  auto row = grin_create_row(graph);

  // insert value to row
  int32_t value0 = 0;
  const char* value1 = "Test String";
  uint64_t value2 = 2;
  double value3 = 3.0;

  std::cout << "put value0: " << value0 << std::endl;
  std::cout << "put value1: " << value1 << std::endl;
  std::cout << "put value2: " << value2 << std::endl;
  std::cout << "put value3: " << value3 << std::endl;
  assert(grin_insert_value_to_row(graph, row, GRIN_DATATYPE::Int32, &value0) ==
         true);
  assert(grin_insert_value_to_row(graph, row, GRIN_DATATYPE::String, value1) ==
         true);
  assert(grin_insert_uint64_to_row(graph, row, value2) == true);
  assert(grin_insert_double_to_row(graph, row, value3) == true);

  // get value from row
  auto value0_ = grin_get_value_from_row(graph, row, GRIN_DATATYPE::Int32, 0);
  auto value1_ = grin_get_value_from_row(graph, row, GRIN_DATATYPE::String, 1);
  auto invalid_value =
      grin_get_value_from_row(graph, row, GRIN_DATATYPE::String, 100);
  assert(grin_get_last_error_code() == INVALID_VALUE && invalid_value == NULL);
  auto value2_ = grin_get_uint64_from_row(graph, row, 2);
  auto value3_ = grin_get_double_from_row(graph, row, 3);
  assert(grin_get_last_error_code() == NO_ERROR);

  // check value
  std::cout << "get value0: " << *static_cast<const int32_t*>(value0_)
            << std::endl;
  std::cout << "get value1: " << static_cast<const char*>(value1_) << std::endl;
  std::cout << "get value2: " << value2_ << std::endl;
  std::cout << "get value3: " << value3_ << std::endl;
  assert(*static_cast<const int32_t*>(value0_) == value0);
  assert(strcmp(static_cast<const char*>(value1_), value1) == 0);
  assert(value2_ == value2);
  assert(value3_ == value3);

  // destroy value
  grin_destroy_value(graph, GRIN_DATATYPE::Int32, value0_);
  grin_destroy_value(graph, GRIN_DATATYPE::String, value1_);

  // destroy row
  grin_destroy_row(graph, row);

  std::cout << "---- test property: table (row) completed ----" << std::endl;
}

void test_property_table_vertex(GRIN_GRAPH graph) {
  std::cout << "\n++++ test property: table (vertex) ++++" << std::endl;

  // get vertex type list
  auto vertex_type_list = grin_get_vertex_type_list(graph);
  size_t n = grin_get_vertex_type_list_size(graph, vertex_type_list);
  std::cout << "size of vertex type list = " << n << std::endl;

  // get vertex list
  auto vertex_list = grin_get_vertex_list(graph);

  for (auto i = 0; i < n; i++) {
    std::cout << "== vertex type " << i << ": ==" << std::endl;

    // get vertex property table by vertex type
    auto vertex_type =
        grin_get_vertex_type_from_list(graph, vertex_type_list, i);
    auto table = grin_get_vertex_property_table_by_type(graph, vertex_type);

    // select vertex list
    auto select_vertex_list =
        grin_select_type_for_vertex_list(graph, vertex_type, vertex_list);

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
      auto data_type = grin_get_vertex_property_data_type(graph, property);
      std::cout << "get value of property \"" << name << "\" for vertex 0"
                << std::endl;

      // get row from property table
      auto r = grin_get_row_from_vertex_property_table(graph, table, vertex,
                                                       property_list);
      auto value1 = grin_get_value_from_row(graph, r, data_type, 0);

      // get vertex property value from table
      auto value2 = grin_get_value_from_vertex_property_table(graph, table,
                                                              vertex, property);

      // check value from row and from table (int64)
      if (data_type == GRIN_DATATYPE::Int64) {
        assert(*static_cast<const int64_t*>(value1) ==
               *static_cast<const int64_t*>(value2));
        std::cout << "value of property \"" << name
                  << "\" for vertex 0 of vertex type " << i << ": "
                  << *static_cast<const int64_t*>(value1) << std::endl;
        std::cout << "check values from row and from table are equal (int64)"
                  << std::endl;
      }

      // check value from row and from table (string)
      if (data_type == GRIN_DATATYPE::String) {
        auto value1_ = grin_get_string_from_row(graph, r, 0);
        auto value2_ = grin_get_string_from_vertex_property_table(
            graph, table, vertex, property);
        assert(grin_get_last_error_code() == NO_ERROR);
        assert(strcmp(static_cast<const char*>(value1), value1_) == 0);
        assert(strcmp(static_cast<const char*>(value2), value2_) == 0);

        assert(strcmp(static_cast<const char*>(value1),
                      static_cast<const char*>(value2)) == 0);
        std::cout << "value of property \"" << name
                  << "\" for vertex 0 of vertex type " << i << ": "
                  << static_cast<const char*>(value1) << std::endl;
        std::cout << "check values from row and from table are equal (string)"
                  << std::endl;
      }

      // destroy
      grin_destroy_row(graph, r);
      grin_destroy_vertex(graph, vertex);
      grin_destroy_vertex_property(graph, property);
      grin_destroy_name(graph, name);
      grin_destroy_value(graph, data_type, value1);
      grin_destroy_value(graph, data_type, value2);
    }

    // destroy
    grin_destroy_vertex_property_list(graph, property_list);
    grin_destroy_vertex_list(graph, select_vertex_list);
    grin_destroy_vertex_property_table(graph, table);
    grin_destroy_vertex_type(graph, vertex_type);
  }

  // destroy
  grin_destroy_vertex_type_list(graph, vertex_type_list);
  grin_destroy_vertex_list(graph, vertex_list);

  std::cout << "---- test property: table (vertex) completed ----" << std::endl;
}

void test_property_table_edge(GRIN_GRAPH graph) {
  std::cout << "\n++++ test property: table (edge) ++++" << std::endl;

  // get edge type list
  auto edge_type_list = grin_get_edge_type_list(graph);
  size_t n = grin_get_edge_type_list_size(graph, edge_type_list);
  std::cout << "size of edge type list = " << n << std::endl;

  // get edge list
  auto edge_list = grin_get_edge_list(graph);

  for (auto i = 0; i < n; i++) {
    std::cout << "== edge type " << i << ": ==" << std::endl;

    // get edge property table by edge type
    auto edge_type = grin_get_edge_type_from_list(graph, edge_type_list, i);
    auto table = grin_get_edge_property_table_by_type(graph, edge_type);

    // select edge list
    auto select_edge_list =
        grin_select_type_for_edge_list(graph, edge_type, edge_list);

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
      auto data_type = grin_get_edge_property_data_type(graph, property);
      std::cout << "get value of property \"" << name << "\" for edge 0"
                << std::endl;

      // get row from property table
      auto r = grin_get_row_from_edge_property_table(graph, table, edge,
                                                     property_list);
      auto value1 = grin_get_value_from_row(graph, r, data_type, 0);

      // get edge property value from table
      auto value2 =
          grin_get_value_from_edge_property_table(graph, table, edge, property);

      // check value from row and from table (int64)
      if (data_type == GRIN_DATATYPE::Int64) {
        auto value1_ = grin_get_int64_from_row(graph, r, 0);
        auto value2_ = grin_get_int64_from_edge_property_table(graph, table,
                                                               edge, property);
        assert(grin_get_last_error_code() == NO_ERROR);
        assert(*static_cast<const int64_t*>(value1) == value1_);
        assert(*static_cast<const int64_t*>(value2) == value2_);

        assert(*static_cast<const int64_t*>(value1) ==
               *static_cast<const int64_t*>(value2));
        std::cout << "value of property \"" << name
                  << "\" for edge 0 of edge type " << i << ": "
                  << *static_cast<const std::int64_t*>(value1) << std::endl;
        std::cout << "check values from row and from table are equal (int64)"
                  << std::endl;
      }

      // check value from row and from table (string)
      if (data_type == GRIN_DATATYPE::String) {
        assert(strcmp(static_cast<const char*>(value1),
                      static_cast<const char*>(value2)) == 0);
        std::cout << "value of property \"" << name
                  << "\" for edge 0 of edge type " << i << ": "
                  << static_cast<const char*>(value1) << std::endl;
        std::cout << "check values from row and from table are equal (string)"
                  << std::endl;
      }

      // destroy
      grin_destroy_row(graph, r);
      grin_destroy_edge(graph, edge);
      grin_destroy_edge_property(graph, property);
      grin_destroy_name(graph, name);
      grin_destroy_value(graph, data_type, value1);
      grin_destroy_value(graph, data_type, value2);
    }

    // destroy
    grin_destroy_edge_property_list(graph, property_list);
    grin_destroy_edge_list_iter(graph, it);
    grin_destroy_edge_list(graph, select_edge_list);
    grin_destroy_edge_property_table(graph, table);
    grin_destroy_edge_type(graph, edge_type);
  }

  // destroy
  grin_destroy_edge_type_list(graph, edge_type_list);
  grin_destroy_edge_list(graph, edge_list);

  std::cout << "---- test property: table (edge) completed ----" << std::endl;
}

int main(int argc, char* argv[]) {
  // get graph from graph info of GraphAr
  std::string path = TEST_DATA_PATH;
  std::cout << "GraphInfo path = " << path << std::endl;

  char** args = new char*[1];
  args[0] = new char[path.length() + 1];
  snprintf(args[0], path.length() + 1, "%s", path.c_str());
  GRIN_GRAPH graph = grin_get_graph_from_storage(1, args);

  // test property table row
  test_property_table_row(graph);

  // test property table vertex
  test_property_table_vertex(graph);

  // test property table edge
  test_property_table_edge(graph);

  // destroy graph
  grin_destroy_graph(graph);

  return 0;
}
