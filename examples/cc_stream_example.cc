/** Copyright 2022 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless assertd by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#include <iostream>
#include <unordered_set>

#include "arrow/api.h"

#include "config.h"
#include "gar/graph.h"
#include "gar/graph_info.h"
#include "gar/reader/arrow_chunk_reader.h"
#include "gar/writer/arrow_chunk_writer.h"


int main(int argc, char* argv[]) {
  // read file and construct graph info
  std::string path =
      TEST_DATA_DIR + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto graph_info = GAR_NAMESPACE::GraphInfo::Load(path).value();

  // construct vertices collection
  std::string label = "person";
  assert(graph_info.GetVertexInfo(label).status().ok());
  auto maybe_vertices =
      GAR_NAMESPACE::ConstructVerticesCollection(graph_info, label);
  assert(maybe_vertices.status().ok());
  auto& vertices = maybe_vertices.value();
  int num_vertices = vertices.size();
  std::cout << "num_vertices: " << num_vertices << std::endl;

  // construct edges collection
  std::string src_label = "person", edge_label = "knows", dst_label = "person";
  auto maybe_edges = GAR_NAMESPACE::ConstructEdgesCollection(
      graph_info, src_label, edge_label, dst_label,
      GAR_NAMESPACE::AdjListType::ordered_by_source);
  assert(!maybe_edges.has_error());
  auto& edges = std::get<GAR_NAMESPACE::EdgesCollection<
      GAR_NAMESPACE::AdjListType::ordered_by_source>>(maybe_edges.value());

  // run cc algorithm
  std::vector<GAR_NAMESPACE::IdType> component(num_vertices);
  for (GAR_NAMESPACE::IdType i = 0; i < num_vertices; i++)
    component[i] = i;
  auto it_begin = edges.begin(), it_end = edges.end();
  for (int iter = 0;; iter++) {
    std::cout << "iter " << iter << std::endl;
    bool flag = false;
    for (auto it = it_begin; it != it_end; ++it) {
      GAR_NAMESPACE::IdType src = it.source(), dst = it.destination();
      if (component[src] < component[dst]) {
        component[dst] = component[src];
        flag = true;
      } else if (component[src] > component[dst]) {
        component[src] = component[dst];
        flag = true;
      }
    }
    if (!flag)
      break;
  }
  // count the number of connected components
  std::unordered_set<GAR_NAMESPACE::IdType> cc_count;
  GAR_NAMESPACE::IdType cc_num = 0;
  for (int i = 0; i < num_vertices; i++) {
    std::cout << i << ", component id: " << component[i] << std::endl;
    if (cc_count.find(component[i]) == cc_count.end()) {
      cc_count.insert(component[i]);
      ++cc_num;
    }
  }
  std::cout << "Total number of components: " << cc_num << std::endl;

  // extend the original vertex info and write results to gar using writer
  // construct property group
  GAR_NAMESPACE::Property cc = {
      "cc", GAR_NAMESPACE::DataType(GAR_NAMESPACE::Type::INT64), false};
  std::vector<GAR_NAMESPACE::Property> property_vector = {cc};
  GAR_NAMESPACE::PropertyGroup group(property_vector,
                                     GAR_NAMESPACE::FileType::PARQUET);
  // extend the vertex_info
  auto maybe_vertex_info = graph_info.GetVertexInfo(label);
  assert(maybe_vertex_info.status().ok());
  auto vertex_info = maybe_vertex_info.value();
  auto maybe_extend_info = vertex_info.Extend(group);
  assert(maybe_extend_info.status().ok());
  auto extend_info = maybe_extend_info.value();
  // dump the extened vertex info
  assert(extend_info.IsValidated());
  assert(extend_info.Dump().status().ok());
  assert(extend_info.Save("/tmp/person-new.vertex.yml").ok());
  // construct vertex property writer
  GAR_NAMESPACE::VertexPropertyWriter writer(extend_info, "/tmp/");
  // convert results to arrow::Table
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  schema_vector.push_back(arrow::field(
      cc.name, GAR_NAMESPACE::DataType::DataTypeToArrowDataType(cc.type)));
  arrow::Int64Builder array_builder;
  assert(array_builder.Reserve(num_vertices).ok());
  assert(array_builder.AppendValues(component).ok());
  std::shared_ptr<arrow::Array> array = array_builder.Finish().ValueOrDie();
  arrays.push_back(array);
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, arrays);
  // dump the results through writer
  assert(writer.WriteTable(table, group, 0).ok());
}
