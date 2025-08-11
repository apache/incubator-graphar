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

#include <arrow/util/type_fwd.h>
#include <iostream>

#include "arrow/api.h"
#include "arrow/filesystem/api.h"
#include "arrow/result.h"

#include "./config.h"
#include "graphar/api/arrow_writer.h"
#include "graphar/fwd.h"
#include "graphar/writer_util.h"

arrow::Result<std::shared_ptr<arrow::Table>> generate_vertex_table() {
  // property "id"
  arrow::Int64Builder i64builder;
  ARROW_RETURN_NOT_OK(i64builder.AppendValues({0, 1, 2}));
  std::shared_ptr<arrow::Array> i64array;
  ARROW_RETURN_NOT_OK(i64builder.Finish(&i64array));

  // property "firstName"
  arrow::StringBuilder strbuilder;
  ARROW_RETURN_NOT_OK(strbuilder.Append("John"));
  ARROW_RETURN_NOT_OK(strbuilder.Append("Jane"));
  ARROW_RETURN_NOT_OK(strbuilder.Append("Alice"));
  std::shared_ptr<arrow::Array> strarray;
  ARROW_RETURN_NOT_OK(strbuilder.Finish(&strarray));

  // property "lastName"
  arrow::StringBuilder strbuilder2;
  ARROW_RETURN_NOT_OK(strbuilder2.Append("Smith"));
  ARROW_RETURN_NOT_OK(strbuilder2.Append("Doe"));
  ARROW_RETURN_NOT_OK(strbuilder2.Append("Wonderland"));
  std::shared_ptr<arrow::Array> strarray2;
  ARROW_RETURN_NOT_OK(strbuilder2.Finish(&strarray2));

  // property "gender"
  arrow::StringBuilder strbuilder3;
  ARROW_RETURN_NOT_OK(strbuilder2.Append("male"));
  ARROW_RETURN_NOT_OK(strbuilder2.Append("female"));
  ARROW_RETURN_NOT_OK(strbuilder2.Append("female"));
  std::shared_ptr<arrow::Array> strarray3;
  ARROW_RETURN_NOT_OK(strbuilder2.Finish(&strarray3));

  // schema
  auto schema = arrow::schema({arrow::field("id", arrow::int64()),
                               arrow::field("firstName", arrow::utf8()),
                               arrow::field("lastName", arrow::utf8()),
                               arrow::field("gender", arrow::utf8())});
  return arrow::Table::Make(schema, {i64array, strarray, strarray2, strarray3});
}

arrow::Result<std::shared_ptr<arrow::Table>> generate_adj_list_table() {
  // source vertex id
  arrow::Int64Builder i64builder;
  ARROW_RETURN_NOT_OK(i64builder.AppendValues({1, 0, 0, 2}));
  std::shared_ptr<arrow::Array> i64array;
  ARROW_RETURN_NOT_OK(i64builder.Finish(&i64array));

  // destination vertex id
  arrow::Int64Builder i64builder2;
  ARROW_RETURN_NOT_OK(i64builder2.AppendValues({0, 1, 2, 1}));
  std::shared_ptr<arrow::Array> i64array2;
  ARROW_RETURN_NOT_OK(i64builder2.Finish(&i64array2));

  // schema
  auto schema = arrow::schema(
      {arrow::field(graphar::GeneralParams::kSrcIndexCol, arrow::int64()),
       arrow::field(graphar::GeneralParams::kDstIndexCol, arrow::int64())});
  return arrow::Table::Make(schema, {i64array, i64array2});
}

arrow::Result<std::shared_ptr<arrow::Table>> generate_edge_property_table() {
  // property "creationDate"
  arrow::StringBuilder strbuilder;
  ARROW_RETURN_NOT_OK(strbuilder.Append("2010-01-01"));
  ARROW_RETURN_NOT_OK(strbuilder.Append("2011-01-01"));
  ARROW_RETURN_NOT_OK(strbuilder.Append("2012-01-01"));
  ARROW_RETURN_NOT_OK(strbuilder.Append("2013-01-01"));
  std::shared_ptr<arrow::Array> strarray;
  ARROW_RETURN_NOT_OK(strbuilder.Finish(&strarray));

  // schema
  auto schema = arrow::schema({arrow::field("creationDate", arrow::utf8())});
  return arrow::Table::Make(schema, {strarray});
}

void vertex_property_writer(
    const std::shared_ptr<graphar::GraphInfo>& graph_info) {
  // create writer
  std::string vertex_meta_file =
      GetTestingResourceRoot() + "/ldbc_sample/parquet/" + "person.vertex.yml";
  auto vertex_meta = graphar::Yaml::LoadFile(vertex_meta_file).value();
  auto vertex_info = graphar::VertexInfo::Load(vertex_meta).value();
  ASSERT(vertex_info->GetType() == "person");
  auto builder = graphar::WriterOptions::ParquetOptionBuilder();
  builder.compression(arrow::Compression::ZSTD);
  auto maybe_writer = graphar::VertexPropertyWriter::Make(vertex_info, "/tmp/",
                                                          builder.build());
  ASSERT(maybe_writer.status().ok());
  auto writer = maybe_writer.value();

  // construct vertex property table
  auto table = generate_vertex_table().ValueOrDie();
  // print
  std::cout << "rows number of vertex table: " << table->num_rows()
            << std::endl;
  std::cout << "schema of vertex table: " << std::endl
            << table->schema()->ToString() << std::endl;

  // use writer
  // set validate level
  writer->SetValidateLevel(graphar::ValidateLevel::strong_validate);
  // write the table
  ASSERT(writer->WriteTable(table, 0).ok());
  // write the number of vertices
  ASSERT(writer->WriteVerticesNum(table->num_rows()).ok());
  std::cout << "writing vertex data successfully!" << std::endl;
  // check vertex count
  auto path = "/tmp/vertex/person/vertex_count";
  auto fs = arrow::fs::FileSystemFromUriOrPath(path).ValueOrDie();
  auto input = fs->OpenInputStream(path).ValueOrDie();
  auto num = input->Read(sizeof(graphar::IdType)).ValueOrDie();
  graphar::IdType* ptr = (graphar::IdType*) num->data();
  std::cout << "vertex count from reading written file: " << *ptr << std::endl;
}

void edge_chunk_writer(const std::shared_ptr<graphar::GraphInfo>& graph_info) {
  // construct writer
  std::string edge_meta_file = GetTestingResourceRoot() + "/ldbc_sample/csv/" +
                               "person_knows_person.edge.yml";
  auto edge_meta = graphar::Yaml::LoadFile(edge_meta_file).value();
  auto edge_info = graphar::EdgeInfo::Load(edge_meta).value();
  auto adj_list_type = graphar::AdjListType::ordered_by_source;
  auto maybe_writer =
      graphar::EdgeChunkWriter::Make(edge_info, "/tmp/", adj_list_type);
  ASSERT(maybe_writer.status().ok());
  auto writer = maybe_writer.value();

  // construct property chunk
  auto chunk = generate_edge_property_table().ValueOrDie();
  // print
  std::cout << "rows number of edge property chunk: " << chunk->num_rows()
            << std::endl;
  std::cout << "schema of edge property chunk: " << std::endl
            << chunk->schema()->ToString() << std::endl;
  // construct adj list table
  auto table = generate_adj_list_table().ValueOrDie();
  // print
  std::cout << "rows number of adj list table: " << table->num_rows()
            << std::endl;
  std::cout << "schema of adj list table: " << std::endl
            << table->schema()->ToString() << std::endl;

  // use writer
  // set validate level
  writer->SetValidateLevel(graphar::ValidateLevel::strong_validate);
  // write a property chunk
  auto pg = edge_info->GetPropertyGroup("creationDate");
  ASSERT(pg != nullptr);
  ASSERT(writer->WritePropertyChunk(chunk, pg, 0, 0).ok());
  // write adj list of vertex chunk 0 to files
  ASSERT(writer->SortAndWriteAdjListTable(table, 0, 0).ok());
  // write number of edges for vertex chunk 0
  ASSERT(writer->WriteEdgesNum(0, table->num_rows()).ok());
  // write number of vertices
  ASSERT(writer->WriteVerticesNum(903).ok());
  std::cout << "writing edge data successfully!" << std::endl;
  // check the number of edges
  auto path = "/tmp/edge/person_knows_person/ordered_by_source/edge_count0";
  auto fs = arrow::fs::FileSystemFromUriOrPath(path).ValueOrDie();
  std::shared_ptr<arrow::io::InputStream> input =
      fs->OpenInputStream(path).ValueOrDie();
  auto edge_num = input->Read(sizeof(graphar::IdType)).ValueOrDie();
  graphar::IdType* edge_num_ptr = (graphar::IdType*) edge_num->data();
  std::cout << "edge number from reading written file: " << *edge_num_ptr
            << std::endl;
}

int main(int argc, char* argv[]) {
  // read file and construct graph info
  std::string path =
      GetTestingResourceRoot() + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto graph_info = graphar::GraphInfo::Load(path).value();

  // vertex property writer
  std::cout << "Vertex property writer" << std::endl;
  std::cout << "----------------------" << std::endl;
  vertex_property_writer(graph_info);
  std::cout << std::endl;

  // edge property writer
  std::cout << "Edge property writer" << std::endl;
  std::cout << "--------------------" << std::endl;
  edge_chunk_writer(graph_info);
}
