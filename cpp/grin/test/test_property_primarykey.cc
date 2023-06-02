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

extern "C" {
#include "property/primarykey.h"
#include "property/property.h"
#include "property/propertylist.h"
#include "property/row.h"
#include "property/topology.h"
#include "property/type.h"
#include "topology/edgelist.h"
#include "topology/structure.h"
#include "topology/vertexlist.h"
}

void test_property_primarykey(GRIN_GRAPH graph) {
  std::cout << "\n++++ test property: primarykey ++++" << std::endl;

  // get vertex types with primary key
  auto primary_vertex_type_list =
      grin_get_vertex_types_with_primary_keys(graph);
  size_t n = grin_get_vertex_type_list_size(graph, primary_vertex_type_list);
  std::cout << "number of vertex types with primary keys: " << n << std::endl;

  for (auto idx = 0; idx < n; ++idx) {
    // get vertex type
    auto vertex_type =
        grin_get_vertex_type_from_list(graph, primary_vertex_type_list, idx);
    std::cout << "\n---- test vertex type with primary key: "
              << grin_get_vertex_type_name(graph, vertex_type) << " ----"
              << std::endl;

    // get the property list for primary key
    auto property_list =
        grin_get_primary_keys_by_vertex_type(graph, vertex_type);
    std::cout << "size of property list for primary key: "
              << grin_get_vertex_property_list_size(graph, property_list)
              << std::endl;

    // create row of primary keys for vertex A
    std::cout << "create row of primary key for vertex A" << std::endl;

    auto vertex_list = grin_get_vertex_list_by_type(graph, vertex_type);
    auto vertex = grin_get_vertex_from_list(graph, vertex_list, 20);
    auto row = grin_create_row(graph);
    auto property_list_size =
        grin_get_vertex_property_list_size(graph, property_list);
    for (auto i = 0; i < property_list_size; ++i) {
      auto property =
          grin_get_vertex_property_from_list(graph, property_list, i);
      ASSERT(grin_get_vertex_property_datatype(graph, property) ==
             GRIN_DATATYPE::Int64);
      auto value =
          grin_get_vertex_property_value_of_int64(graph, vertex, property);
      auto status = grin_insert_int64_to_row(graph, row, value);
      ASSERT(status == true);
      grin_destroy_vertex_property(graph, property);
    }

    // get vertex from primary key
    std::cout << "get vertex B from primary key" << std::endl;
    auto vertex2 = grin_get_vertex_by_primary_keys(graph, vertex_type, row);
    ASSERT(grin_equal_vertex(graph, vertex, vertex2) == true);
    std::cout << "(Correct) vertex A and vertex B are equal" << std::endl;

    // destroy
    grin_destroy_vertex_property_list(graph, property_list);
    grin_destroy_vertex_type(graph, vertex_type);
    grin_destroy_vertex_list(graph, vertex_list);
    grin_destroy_vertex(graph, vertex);
    grin_destroy_vertex(graph, vertex2);
    grin_destroy_row(graph, row);
  }
  // destroy vertex type list
  grin_destroy_vertex_type_list(graph, primary_vertex_type_list);

  std::cout << "---- test property: primarykey completed ----" << std::endl;
}

int main(int argc, char* argv[]) {
  // get graph from graph info of GraphAr
  std::string path = TEST_DATA_PATH;
  std::cout << "GraphInfo path = " << path << std::endl;

  char** args = new char*[1];
  args[0] = new char[path.length() + 1];
  snprintf(args[0], path.length() + 1, "%s", path.c_str());
  GRIN_GRAPH graph = grin_get_graph_from_storage(1, args);
  delete[] args[0];
  delete[] args;

  // test property primary key
  test_property_primarykey(graph);

  // destroy graph
  grin_destroy_graph(graph);

  return 0;
}
