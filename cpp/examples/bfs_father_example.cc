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

#include "arrow/api.h"

#include "./config.h"
#include "graphar/api/high_level_reader.h"
#include "graphar/api/high_level_writer.h"

int main(int argc, char* argv[]) {
  // read file and construct graph info
  std::string path =
      GetTestingResourceRoot() + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto graph_info = graphar::GraphInfo::Load(path).value();

  // get the person vertices of graph
  std::string type = "person";
  ASSERT(graph_info->GetVertexInfo(type) != nullptr);
  auto maybe_vertices = graphar::VerticesCollection::Make(graph_info, type);
  ASSERT(maybe_vertices.status().ok());
  auto vertices = maybe_vertices.value();
  int num_vertices = vertices->size();
  std::cout << "num_vertices: " << num_vertices << std::endl;

  // get the "person_knows_person" edges of graph
  std::string src_type = "person", edge_type = "knows", dst_type = "person";
  auto maybe_edges =
      graphar::EdgesCollection::Make(graph_info, src_type, edge_type, dst_type,
                                     graphar::AdjListType::unordered_by_source);
  ASSERT(!maybe_edges.has_error());
  auto& edges = maybe_edges.value();

  // run bfs algorithm
  graphar::IdType root = 0;
  std::vector<int32_t> distance(num_vertices);
  std::vector<graphar::IdType> pre(num_vertices);
  for (graphar::IdType i = 0; i < num_vertices; i++) {
    distance[i] = (i == root ? 0 : -1);
    pre[i] = (i == root ? root : -1);
  }
  auto it_begin = edges->begin(), it_end = edges->end();
  for (int iter = 0;; iter++) {
    graphar::IdType count = 0;
    for (auto it = it_begin; it != it_end; ++it) {
      auto src = it.source(), dst = it.destination();
      if (distance[src] == iter && distance[dst] == -1) {
        distance[dst] = distance[src] + 1;
        pre[dst] = src;
        count++;
      }
    }
    std::cout << "iter " << iter << ": " << count << " vertices." << std::endl;
    if (count == 0)
      break;
  }
  for (int i = 0; i < num_vertices; i++) {
    std::cout << i << ", distance: " << distance[i] << ", father: " << pre[i]
              << std::endl;
  }

  // Append the bfs result to the vertex info as a property group
  // and write to file
  // construct property group
  graphar::Property bfs("bfs", graphar::int32(), false);
  graphar::Property father("father", graphar::int64(), false);
  std::vector<graphar::Property> property_vector = {bfs, father};
  auto group =
      graphar::CreatePropertyGroup(property_vector, graphar::FileType::CSV);

  // extend the vertex_info
  auto vertex_info = graph_info->GetVertexInfo(type);
  auto maybe_extend_info = vertex_info->AddPropertyGroup(group);
  ASSERT(maybe_extend_info.status().ok());
  auto extend_info = maybe_extend_info.value();

  // dump the extened vertex info
  ASSERT(extend_info->IsValidated());
  ASSERT(extend_info->Dump().status().ok());
  ASSERT(extend_info->Save("/tmp/person-new-bfs-father.vertex.yml").ok());
  // construct vertex property writer
  graphar::VertexPropertyWriter writer(extend_info, "file:///tmp/");
  // convert results to arrow::Table
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  schema_vector.push_back(arrow::field(
      bfs.name, graphar::DataType::DataTypeToArrowDataType(bfs.type)));
  schema_vector.push_back(arrow::field(
      father.name, graphar::DataType::DataTypeToArrowDataType(father.type)));
  arrow::Int32Builder array_builder1;
  ASSERT(array_builder1.Reserve(num_vertices).ok());
  ASSERT(array_builder1.AppendValues(distance).ok());
  std::shared_ptr<arrow::Array> array1 = array_builder1.Finish().ValueOrDie();
  arrays.push_back(array1);

  arrow::Int64Builder array_builder2;
  ASSERT(array_builder2.Reserve(num_vertices).ok());
  for (int i = 0; i < num_vertices; i++) {
    if (pre[i] == -1) {
      ASSERT(array_builder2.AppendNull().ok());
    } else {
      auto it = vertices->find(pre[i]);
      auto father_id = it.property<int64_t>("id").value();
      ASSERT(array_builder2.Append(father_id).ok());
    }
  }
  std::shared_ptr<arrow::Array> array2 = array_builder2.Finish().ValueOrDie();
  arrays.push_back(array2);

  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, arrays);
  // dump the results through writer
  ASSERT(writer.WriteTable(table, group, 0).ok());

  // construct a new graph
  src_type = "person";
  edge_type = "bfs";
  dst_type = "person";
  int edge_chunk_size = 1024, src_chunk_size = 100, dst_chunk_size = 100;
  bool directed = true;
  auto version = graphar::InfoVersion::Parse("gar/v1").value();
  auto al = graphar::CreateAdjacentList(graphar::AdjListType::ordered_by_source,
                                        graphar::FileType::CSV);
  auto new_edge_info = graphar::CreateEdgeInfo(
      src_type, edge_type, dst_type, edge_chunk_size, src_chunk_size,
      dst_chunk_size, directed, {al}, {}, "", version);
  ASSERT(new_edge_info->IsValidated());
  // save & dump
  ASSERT(!new_edge_info->Dump().has_error());
  ASSERT(new_edge_info->Save("/tmp/person_bfs_person.edge.yml").ok());
  graphar::builder::EdgesBuilder edges_builder(
      new_edge_info, "file:///tmp/", graphar::AdjListType::ordered_by_source,
      num_vertices);
  for (int i = 0; i < num_vertices; i++) {
    if (i == root || pre[i] == -1)
      continue;
    graphar::builder::Edge e(pre[i], i);
    ASSERT(edges_builder.AddEdge(e).ok());
  }
  ASSERT(edges_builder.Dump().ok());
}
