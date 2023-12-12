/*
 * Copyright 2022-2023 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iostream>
#include <unordered_set>

#include "arrow/api.h"

#include "./config.h"
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
  ASSERT(graph_info.GetVertexInfo(label).status().ok());
  auto maybe_vertices =
      GAR_NAMESPACE::VerticesCollection::Make(graph_info, label);
  ASSERT(maybe_vertices.status().ok());
  auto& vertices = maybe_vertices.value();
  int num_vertices = vertices->size();
  std::cout << "num_vertices: " << num_vertices << std::endl;

  // construct edges collection
  std::string src_label = "person", edge_label = "knows", dst_label = "person";
  auto expect1 = GAR_NAMESPACE::EdgesCollection::Make(
      graph_info, src_label, edge_label, dst_label,
      GAR_NAMESPACE::AdjListType::ordered_by_source);
  ASSERT(!expect1.has_error());
  auto& edges1 = expect1.value();
  auto expect2 = GAR_NAMESPACE::EdgesCollection::Make(
      graph_info, src_label, edge_label, dst_label,
      GAR_NAMESPACE::AdjListType::ordered_by_dest);
  ASSERT(!expect2.has_error());
  auto& edges2 = expect2.value();

  // run cc algorithm
  std::vector<GAR_NAMESPACE::IdType> component(num_vertices);
  std::vector<bool> active[2];
  for (GAR_NAMESPACE::IdType i = 0; i < num_vertices; i++) {
    component[i] = i;
    active[0].push_back(true);
    active[1].push_back(false);
  }
  auto begin1 = edges1->begin(), end1 = edges1->end();
  auto begin2 = edges2->begin(), end2 = edges2->end();
  auto it1 = begin1;
  auto it2 = begin2;
  int count = num_vertices;
  for (int iter = 0; count > 0; iter++) {
    std::cout << "iter " << iter << ": " << count << std::endl;
    std::fill(active[1 - iter % 2].begin(), active[1 - iter % 2].end(), 0);
    count = 0;
    for (GAR_NAMESPACE::IdType vid = 0; vid < num_vertices; vid++) {
      if (active[iter % 2][vid]) {
        // find outgoing edges and update neighbors
        if (it1.first_src(begin1, vid)) {
          do {
            GAR_NAMESPACE::IdType src = it1.source(), dst = it1.destination();
            if (component[src] < component[dst]) {
              component[dst] = component[src];
              if (!active[1 - iter % 2][dst]) {
                active[1 - iter % 2][dst] = true;
                count++;
              }
            }
          } while (it1.next_src());
        }
        // find incoming edges and update neighbors
        if (it2.first_dst(begin2, vid)) {
          do {
            GAR_NAMESPACE::IdType src = it2.source(), dst = it2.destination();
            if (component[dst] < component[src]) {
              component[src] = component[dst];
              if (!active[1 - iter % 2][src]) {
                active[1 - iter % 2][src] = true;
                count++;
              }
            }
          } while (it2.next_dst());
        }
      }
    }
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
      "cc-push", GAR_NAMESPACE::DataType(GAR_NAMESPACE::Type::INT64), false};
  std::vector<GAR_NAMESPACE::Property> property_vector = {cc};
  GAR_NAMESPACE::PropertyGroup group(property_vector,
                                     GAR_NAMESPACE::FileType::PARQUET);
  // extend the vertex_info
  auto maybe_vertex_info = graph_info.GetVertexInfo(label);
  ASSERT(maybe_vertex_info.status().ok());
  auto vertex_info = maybe_vertex_info.value();
  auto maybe_extend_info = vertex_info.Extend(group);
  ASSERT(maybe_extend_info.status().ok());
  auto extend_info = maybe_extend_info.value();
  // dump the extened vertex info
  ASSERT(extend_info.IsValidated());
  ASSERT(extend_info.Dump().status().ok());
  ASSERT(extend_info.Save("/tmp/person-new-cc-push.vertex.yml").ok());
  // construct vertex property writer
  GAR_NAMESPACE::VertexPropertyWriter writer(extend_info, "/tmp/");
  // convert results to arrow::Table
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  schema_vector.push_back(arrow::field(
      cc.name, GAR_NAMESPACE::DataType::DataTypeToArrowDataType(cc.type)));
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
