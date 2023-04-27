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
#include "grin/include/property/primarykey.h"
#include "grin/include/property/property.h"
#include "grin/include/property/propertylist.h"
#include "grin/include/property/propertytable.h"
#include "grin/include/property/topology.h"
#include "grin/include/property/type.h"
#include "grin/include/topology/edgelist.h"
#include "grin/include/topology/structure.h"
#include "grin/include/topology/vertexlist.h"
}

void test_property_primarykey(GRIN_GRAPH graph) {
  std::cout << "\n++++ test property: primarykey ++++" << std::endl;

  // get vertex types with primary key
  auto primary_vertex_type_list =
      grin_get_vertex_types_with_primary_keys(graph);
  std::cout << "number of vertex types with primary keys: "
            << grin_get_vertex_type_list_size(graph, primary_vertex_type_list)
            << std::endl;

  size_t n = grin_get_vertex_type_list_size(graph, primary_vertex_type_list);
  if (n > 0) {
    // get primary key property list
    auto vertex_type =
        grin_get_vertex_type_from_list(graph, primary_vertex_type_list, 0);
    auto property_list =
        grin_get_primary_keys_by_vertex_type(graph, vertex_type);
    std::cout << "size of primary key property list "
              << grin_get_vertex_property_list_size(graph, property_list)
              << std::endl;

    // get row from property table
    std::cout << "get row from property table for vertex 0" << std::endl;
    auto vertex_list = grin_get_vertex_list(graph);
    auto select_vertex_list =
        grin_select_type_for_vertex_list(graph, vertex_type, vertex_list);
    auto vertex = grin_get_vertex_from_list(graph, select_vertex_list, 0);
    auto vertex_table =
        grin_get_vertex_property_table_by_type(graph, vertex_type);
    auto row = grin_get_row_from_vertex_property_table(graph, vertex_table,
                                                       vertex, property_list);

    // get vertex from primary keys
    std::cout << "get vertex 1 from primary keys" << std::endl;
    auto vertex2 = grin_get_vertex_by_primary_keys(graph, vertex_type, row);
    assert(grin_equal_vertex(graph, vertex, vertex2) == true);
    std::cout << "vertex 0 and vertex 1 are equal" << std::endl;

    // destroy
    grin_destroy_vertex_property_list(graph, property_list);
    grin_destroy_vertex_type(graph, vertex_type);
    grin_destroy_vertex_list(graph, vertex_list);
    grin_destroy_vertex_list(graph, select_vertex_list);
    grin_destroy_vertex(graph, vertex);
    grin_destroy_vertex(graph, vertex2);
    grin_destroy_vertex_property_table(graph, vertex_table);
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

  // test property primary key
  test_property_primarykey(graph);

  // destroy graph
  grin_destroy_graph(graph);

  return 0;
}
