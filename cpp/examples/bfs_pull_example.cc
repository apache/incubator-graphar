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

#include "arrow/api.h"

#include "./config.h"
#include "gar/api.h"
#include "gar/graph.h"
#include "gar/reader/arrow_chunk_reader.h"
#include "gar/writer/arrow_chunk_writer.h"

int main(int argc, char* argv[]) {
  // read file and construct graph info
  std::string path =
      TEST_DATA_DIR + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto graph_info = GAR_NAMESPACE::GraphInfo::Load(path).value();

  // construct vertices collection
  std::string label = "person";
  ASSERT(graph_info->GetVertexInfo(label) != nullptr);
  auto maybe_vertices =
      GAR_NAMESPACE::VerticesCollection::Make(graph_info, label);
  ASSERT(maybe_vertices.status().ok());
  auto& vertices = maybe_vertices.value();
  int num_vertices = vertices->size();
  std::cout << "num_vertices: " << num_vertices << std::endl;

  // construct edges collection
  std::string src_label = "person", edge_label = "knows", dst_label = "person";
  auto maybe_edges = GAR_NAMESPACE::EdgesCollection::Make(
      graph_info, src_label, edge_label, dst_label,
      GAR_NAMESPACE::AdjListType::ordered_by_dest);
  ASSERT(!maybe_edges.has_error());
  auto& edges = maybe_edges.value();

  // run bfs algorithm
  GAR_NAMESPACE::IdType root = 0;
  std::vector<int32_t> distance(num_vertices);
  for (GAR_NAMESPACE::IdType i = 0; i < num_vertices; i++)
    distance[i] = (i == root ? 0 : -1);
  auto it_begin = edges->begin(), it_end = edges->end();
  auto it = it_begin;
  for (int iter = 0;; iter++) {
    GAR_NAMESPACE::IdType count = 0;
    it.to_begin();
    for (GAR_NAMESPACE::IdType vid = 0; vid < num_vertices; vid++) {
      if (distance[vid] == -1) {
        if (!it.first_dst(it, vid))
          continue;
        // if (!it.first_dst(it_begin, vid)) continue;
        do {
          GAR_NAMESPACE::IdType src = it.source(), dst = it.destination();
          if (distance[src] == iter) {
            distance[dst] = distance[src] + 1;
            count++;
            break;
          }
        } while (it.next_dst());
      }
    }
    std::cout << "iter " << iter << ": " << count << " vertices." << std::endl;
    if (count == 0)
      break;
  }
  for (int i = 0; i < num_vertices; i++) {
    std::cout << i << ", distance: " << distance[i] << std::endl;
  }

  // extend the original vertex info and write results to gar using writer
  // construct property group
  GAR_NAMESPACE::Property bfs("bfs-pull", GAR_NAMESPACE::int32(), false);
  std::vector<GAR_NAMESPACE::Property> property_vector = {bfs};
  auto group = GAR_NAMESPACE::CreatePropertyGroup(
      property_vector, GAR_NAMESPACE::FileType::PARQUET);
  // extend the vertex_info
  auto vertex_info = graph_info->GetVertexInfo(label);
  auto maybe_extend_info = vertex_info->AddPropertyGroup(group);
  ASSERT(maybe_extend_info.status().ok());
  auto extend_info = maybe_extend_info.value();
  // dump the extened vertex info
  ASSERT(extend_info->IsValidated());
  ASSERT(extend_info->Dump().status().ok());
  ASSERT(extend_info->Save("/tmp/person-new-bfs-pull.vertex.yml").ok());
  // construct vertex property writer
  GAR_NAMESPACE::VertexPropertyWriter writer(extend_info, "/tmp/");
  // convert results to arrow::Table
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  schema_vector.push_back(arrow::field(
      bfs.name, GAR_NAMESPACE::DataType::DataTypeToArrowDataType(bfs.type)));
  arrow::Int32Builder array_builder;
  ASSERT(array_builder.Reserve(num_vertices).ok());
  ASSERT(array_builder.AppendValues(distance).ok());
  std::shared_ptr<arrow::Array> array = array_builder.Finish().ValueOrDie();
  arrays.push_back(array);
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, arrays);
  // dump the results through writer
  ASSERT(writer.WriteTable(table, group, 0).ok());
}
