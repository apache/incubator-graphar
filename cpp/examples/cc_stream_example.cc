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
#include <unordered_set>

#include "arrow/api.h"

#include "./config.h"
#include "graphar/api/arrow_writer.h"
#include "graphar/api/high_level_reader.h"

int main(int argc, char* argv[]) {
  // read file and construct graph info
  std::string path =
      GetTestingResourceRoot() + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto graph_info = graphar::GraphInfo::Load(path).value();

  // construct vertices collection
  std::string label = "person";
  ASSERT(graph_info->GetVertexInfo(label) != nullptr);
  auto maybe_vertices = graphar::VerticesCollection::Make(graph_info, label);
  ASSERT(maybe_vertices.status().ok());
  auto& vertices = maybe_vertices.value();
  int num_vertices = vertices->size();
  std::cout << "num_vertices: " << num_vertices << std::endl;

  // construct edges collection
  std::string src_type = "person", edge_type = "knows", dst_type = "person";
  auto maybe_edges =
      graphar::EdgesCollection::Make(graph_info, src_type, edge_type, dst_type,
                                     graphar::AdjListType::ordered_by_source);
  ASSERT(!maybe_edges.has_error());
  auto& edges = maybe_edges.value();

  // run cc algorithm
  std::vector<graphar::IdType> component(num_vertices);
  for (graphar::IdType i = 0; i < num_vertices; i++)
    component[i] = i;
  auto it_begin = edges->begin(), it_end = edges->end();
  for (int iter = 0;; iter++) {
    std::cout << "iter " << iter << std::endl;
    bool flag = false;
    for (auto it = it_begin; it != it_end; ++it) {
      graphar::IdType src = it.source(), dst = it.destination();
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
  std::unordered_set<graphar::IdType> cc_count;
  graphar::IdType cc_num = 0;
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
  graphar::Property cc("cc", graphar::int64(), false);
  std::vector<graphar::Property> property_vector = {cc};
  auto group =
      graphar::CreatePropertyGroup(property_vector, graphar::FileType::PARQUET);
  // extend the vertex_info
  auto vertex_info = graph_info->GetVertexInfo(label);
  auto maybe_extend_info = vertex_info->AddPropertyGroup(group);
  ASSERT(maybe_extend_info.status().ok());
  auto extend_info = maybe_extend_info.value();
  // dump the extened vertex info
  ASSERT(extend_info->IsValidated());
  ASSERT(extend_info->Dump().status().ok());
  ASSERT(extend_info->Save("/tmp/person-new.vertex.yml").ok());
  // construct vertex property writer
  graphar::VertexPropertyWriter writer(extend_info, "/tmp/");
  // convert results to arrow::Table
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  schema_vector.push_back(arrow::field(
      cc.name, graphar::DataType::DataTypeToArrowDataType(cc.type)));
  arrow::Int64Builder array_builder;
  ASSERT(array_builder.Reserve(num_vertices).ok());
  ASSERT(array_builder.AppendValues(component).ok());
  std::shared_ptr<arrow::Array> array = array_builder.Finish().ValueOrDie();
  arrays.push_back(array);
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, arrays);
  // dump the results through writer
  ASSERT(writer.WriteTable(table, group, 0).ok());
}
