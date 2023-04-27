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
#include "grin/include/property/property.h"
#include "grin/include/property/propertylist.h"
#include "grin/include/property/type.h"
#include "grin/include/topology/edgelist.h"
#include "grin/include/topology/structure.h"
#include "grin/include/topology/vertexlist.h"
}

void test_vertex_property(GRIN_GRAPH graph, GRIN_VERTEX_PROPERTY property,
                          GRIN_VERTEX_TYPE vertex_type) {
  // get property name
  auto name = grin_get_vertex_property_name(graph, property);
  std::cout << "name of vertex property: " << name << std::endl;

  // get property data type
  auto data_type = grin_get_vertex_property_data_type(graph, property);
  std::cout << "data type of vertex property: " << data_type << std::endl;

  // get vertex type from property
  auto vertex_type2 = grin_get_vertex_property_vertex_type(graph, property);
  assert(grin_equal_vertex_type(graph, vertex_type, vertex_type2) == true);

  // get vertex property by name
  auto property2 = grin_get_vertex_property_by_name(graph, vertex_type, name);
  assert(grin_equal_vertex_property(graph, property, property2) == true);

  // get vertex properties by name
  auto property_list = grin_get_vertex_properties_by_name(graph, name);
  auto n = grin_get_vertex_property_list_size(graph, property_list);
  std::cout << "number of vertex properties with this name: " << n << std::endl;

  // destroy
  grin_destroy_vertex_property(graph, property2);
  grin_destroy_name(graph, name);
  grin_destroy_vertex_type(graph, vertex_type2);
  grin_destroy_vertex_property_list(graph, property_list);
}

void test_vertex_property_list(GRIN_GRAPH graph) {
  std::cout << "\n++++ test property: property list (vertex) ++++" << std::endl;

  auto vertex_type_list = grin_get_vertex_type_list(graph);
  for (auto i = 0; i < grin_get_vertex_type_list_size(graph, vertex_type_list);
       i++) {
    // get property list from vertex type
    auto vertex_type =
        grin_get_vertex_type_from_list(graph, vertex_type_list, i);
    auto property_list =
        grin_get_vertex_property_list_by_type(graph, vertex_type);
    size_t n = grin_get_vertex_property_list_size(graph, property_list);

    std::cout << "\n==== test property: property (vertex) ++++" << std::endl;
    std::cout << "size of property list of vtype " << i << ": " << n
              << std::endl;

    // create property list
    auto new_property_list = grin_create_vertex_property_list(graph);

    for (auto i = 0; i < n; i++) {
      // get property from property list
      auto property =
          grin_get_vertex_property_from_list(graph, property_list, i);
      // test methods on property
      test_vertex_property(graph, property, vertex_type);
      // insert property to property list
      assert(grin_insert_vertex_property_to_list(graph, new_property_list,
                                                 property) == true);
      // destroy property
      grin_destroy_vertex_property(graph, property);
    }

    // compare size
    assert(grin_get_vertex_property_list_size(graph, new_property_list) == n);

    // destroy
    grin_destroy_vertex_type(graph, vertex_type);
    grin_destroy_vertex_property_list(graph, property_list);
    grin_destroy_vertex_property_list(graph, new_property_list);
  }

  // destroy
  grin_destroy_vertex_type_list(graph, vertex_type_list);

  std::cout << "---- test property: property list (vertex) completed ----"
            << std::endl;
}

void test_edge_property(GRIN_GRAPH graph, GRIN_EDGE_PROPERTY property,
                        GRIN_EDGE_TYPE edge_type) {
  // get property name
  auto name = grin_get_edge_property_name(graph, property);
  std::cout << "name of edge property: " << name << std::endl;

  // get property data type
  auto data_type = grin_get_edge_property_data_type(graph, property);
  std::cout << "data type of edge property: " << data_type << std::endl;

  // get edge type from property
  auto edge_type2 = grin_get_edge_property_edge_type(graph, property);
  assert(grin_equal_edge_type(graph, edge_type, edge_type2) == true);

  // get edge property by name
  auto property2 = grin_get_edge_property_by_name(graph, edge_type, name);
  assert(grin_equal_edge_property(graph, property, property2) == true);

  // get edge properties by name
  auto property_list = grin_get_edge_properties_by_name(graph, name);
  auto n = grin_get_edge_property_list_size(graph, property_list);
  std::cout << "number of edge properties with this name: " << n << std::endl;

  // destroy
  grin_destroy_name(graph, name);
  grin_destroy_edge_type(graph, edge_type2);
  grin_destroy_edge_property(graph, property2);
  grin_destroy_edge_property_list(graph, property_list);
}

void test_edge_property_list(GRIN_GRAPH graph) {
  std::cout << "\n++++ test property: property list (edge) ++++" << std::endl;

  auto edge_type_list = grin_get_edge_type_list(graph);
  for (auto i = 0; i < grin_get_edge_type_list_size(graph, edge_type_list);
       i++) {
    // get property list from edge type
    auto edge_type = grin_get_edge_type_from_list(graph, edge_type_list, i);
    auto property_list = grin_get_edge_property_list_by_type(graph, edge_type);
    size_t n = grin_get_edge_property_list_size(graph, property_list);

    std::cout << "\n==== test property: property (edge) ++++" << std::endl;
    std::cout << "size of property list of etype " << i << ": " << n
              << std::endl;

    // create property list
    auto new_property_list = grin_create_edge_property_list(graph);

    for (auto i = 0; i < n; i++) {
      // get property from property list
      auto property = grin_get_edge_property_from_list(graph, property_list, i);
      // test methods on property
      test_edge_property(graph, property, edge_type);
      // insert property to property list
      assert(grin_insert_edge_property_to_list(graph, new_property_list,
                                               property) == true);
      // destroy property
      grin_destroy_edge_property(graph, property);
    }

    // compare size
    assert(grin_get_edge_property_list_size(graph, new_property_list) == n);

    // destroy
    grin_destroy_edge_type(graph, edge_type);
    grin_destroy_edge_property_list(graph, property_list);
    grin_destroy_edge_property_list(graph, new_property_list);
  }

  // destroy
  grin_destroy_edge_type_list(graph, edge_type_list);

  std::cout << "---- test property: property list (edge) completed ----"
            << std::endl;
}

int main(int argc, char* argv[]) {
  // get graph from graph info of GraphAr
  std::string path = TEST_DATA_PATH;
  std::cout << "GraphInfo path = " << path << std::endl;

  char** args = new char*[1];
  args[0] = new char[path.length() + 1];
  snprintf(args[0], path.length() + 1, "%s", path.c_str());
  GRIN_GRAPH graph = grin_get_graph_from_storage(1, args);

  // test vertex property list
  test_vertex_property_list(graph);

  // test edge property list
  test_edge_property_list(graph);

  // destroy graph
  grin_destroy_graph(graph);

  return 0;
}
