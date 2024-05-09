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
#include "arrow/filesystem/api.h"

#include "./config.h"
#include "graphar/api.h"
#include "graphar/reader/arrow_chunk_reader.h"
#include "graphar/util/expression.h"

void vertex_property_chunk_reader(
    const std::shared_ptr<graphar::GraphInfo>& graph_info) {
  // create reader
  std::string label = "person", property_name = "gender";
  auto maybe_reader = graphar::VertexPropertyArrowChunkReader::Make(
      graph_info, label, property_name);
  ASSERT(maybe_reader.status().ok());
  auto reader = maybe_reader.value();

  // use reader
  auto result = reader->GetChunk();
  ASSERT(!result.has_error());
  std::cout << "chunk number: " << reader->GetChunkNum() << std::endl;
  auto table = result.value();
  std::cout << "rows number of first vertex property chunk: "
            << table->num_rows() << std::endl;
  std::cout << "schema of first vertex property chunk: " << std::endl
            << table->schema()->ToString() << std::endl;
  auto index_col =
      table->GetColumnByName(graphar::GeneralParams::kVertexIndexCol);
  ASSERT(index_col != nullptr);
  std::cout << "Internal id column: " << index_col->ToString() << " "
            << std::endl;
  // seek vertex id
  ASSERT(reader->seek(100).ok());
  result = reader->GetChunk();
  ASSERT(!result.has_error());
  table = result.value();
  index_col = table->GetColumnByName(graphar::GeneralParams::kVertexIndexCol);
  ASSERT(index_col != nullptr);
  std::cout << "Internal id column of vertex property chunk for vertex id 100: "
            << index_col->ToString() << " " << std::endl;
  // next chunk
  ASSERT(reader->next_chunk().ok());
  result = reader->GetChunk();
  ASSERT(!result.has_error());
  table = result.value();
  index_col = table->GetColumnByName(graphar::GeneralParams::kVertexIndexCol);
  ASSERT(index_col != nullptr);
  std::cout << "Internal id column of next chunk: " << index_col->ToString()
            << " " << std::endl;

  // reader with filter pushdown
  auto filter = graphar::_Equal(graphar::_Property("gender"),
                                graphar::_Literal("female"));
  std::vector<std::string> expected_cols{"firstName", "lastName"};
  auto maybe_filter_reader = graphar::VertexPropertyArrowChunkReader::Make(
      graph_info, label, property_name);
  ASSERT(maybe_filter_reader.status().ok());
  auto filter_reader = maybe_filter_reader.value();
  filter_reader->Filter(filter);
  filter_reader->Select(expected_cols);
  auto filter_result = filter_reader->GetChunk();
  ASSERT(!result.has_error());
  auto filter_table = filter_result.value();
  std::cout << "rows number of first filtered vertex property chunk: "
            << filter_table->num_rows() << std::endl;
  std::cout << "schema of first filtered vertex property chunk: " << std::endl
            << filter_table->schema()->ToString() << std::endl;
}

void adj_list_chunk_reader(
    const std::shared_ptr<graphar::GraphInfo>& graph_info) {
  // create reader
  std::string src_label = "person", edge_label = "knows", dst_label = "person";
  auto maybe_reader = graphar::AdjListArrowChunkReader::Make(
      graph_info, src_label, edge_label, dst_label,
      graphar::AdjListType::ordered_by_source);
  ASSERT(maybe_reader.status().ok());

  // use reader
  auto reader = maybe_reader.value();
  auto result = reader->GetChunk();
  ASSERT(!result.has_error());
  auto table = result.value();
  std::cout << "rows number of first adj_list chunk: " << table->num_rows()
            << std::endl;
  std::cout << "schema of first adj_list chunk: " << std::endl
            << table->schema()->ToString() << std::endl;
  // seek src
  ASSERT(reader->seek_src(100).ok());
  result = reader->GetChunk();
  ASSERT(!result.has_error());
  table = result.value();
  std::cout << "rows number of first adj_list chunk for outgoing edges of "
               "vertex id 100: "
            << table->num_rows() << std::endl;
  // next chunk
  ASSERT(reader->next_chunk().ok());
  result = reader->GetChunk();
  ASSERT(!result.has_error());
  table = result.value();
  std::cout << "rows number of next adj_list chunk: " << table->num_rows()
            << std::endl;
}

void adj_list_property_chunk_reader(
    const std::shared_ptr<graphar::GraphInfo>& graph_info) {
  // create reader
  std::string src_label = "person", edge_label = "knows", dst_label = "person",
              property_name = "creationDate";
  auto maybe_reader = graphar::AdjListPropertyArrowChunkReader::Make(
      graph_info, src_label, edge_label, dst_label, property_name,
      graphar::AdjListType::ordered_by_source);
  ASSERT(maybe_reader.status().ok());
  auto reader = maybe_reader.value();

  // use reader
  auto result = reader->GetChunk();
  ASSERT(!result.has_error());
  auto table = result.value();
  std::cout << "rows number of first adj_list property chunk: "
            << table->num_rows() << std::endl;
  std::cout << "schema of first adj_list property chunk: " << std::endl
            << table->schema()->ToString() << std::endl;
  // seek src
  ASSERT(reader->seek_src(100).ok());
  result = reader->GetChunk();
  ASSERT(!result.has_error());
  table = result.value();
  std::cout << "rows number of first adj_list property chunk for outgoing "
               "edges of vertex id 100: "
            << table->num_rows() << std::endl;
  // next chunk
  ASSERT(reader->next_chunk().ok());
  result = reader->GetChunk();
  ASSERT(!result.has_error());
  table = result.value();
  std::cout << "rows number of next adj_list property chunk: "
            << table->num_rows() << std::endl;

  // reader with filter pushdown
  auto expr1 =
      graphar::_LessThan(graphar::_Literal("2012-06-02T04:30:44.526+0000"),
                         graphar::_Property(property_name));
  auto expr2 = graphar::_Equal(graphar::_Property(property_name),
                               graphar::_Property(property_name));
  auto filter = graphar::_And(expr1, expr2);
  std::vector<std::string> expected_cols{"creationDate"};
  auto maybe_filter_reader = graphar::AdjListPropertyArrowChunkReader::Make(
      graph_info, src_label, edge_label, dst_label, property_name,
      graphar::AdjListType::ordered_by_source);
  ASSERT(maybe_filter_reader.status().ok());
  auto filter_reader = maybe_filter_reader.value();
  filter_reader->Filter(filter);
  filter_reader->Select(expected_cols);
  auto filter_result = filter_reader->GetChunk();
  ASSERT(!result.has_error());
  auto filter_table = filter_result.value();
  std::cout << "rows number of first filtered adj_list property chunk: "
            << filter_table->num_rows() << std::endl;
}

void adj_list_offset_chunk_reader(
    const std::shared_ptr<graphar::GraphInfo>& graph_info) {
  // create reader
  std::string src_label = "person", edge_label = "knows", dst_label = "person";
  auto maybe_reader = graphar::AdjListOffsetArrowChunkReader::Make(
      graph_info, src_label, edge_label, dst_label,
      graphar::AdjListType::ordered_by_source);
  ASSERT(maybe_reader.status().ok());
  auto reader = maybe_reader.value();

  // use reader
  auto result = reader->GetChunk();
  ASSERT(!result.has_error());
  auto array = result.value();
  std::cout << "length of first adj_list offset chunk: " << array->length()
            << std::endl;
  // next chunk
  ASSERT(reader->next_chunk().ok());
  result = reader->GetChunk();
  ASSERT(!result.has_error());
  array = result.value();
  std::cout << "length of next adj_list offset chunk: " << array->length()
            << std::endl;
  // seek vertex id
  ASSERT(reader->seek(900).ok());
  result = reader->GetChunk();
  ASSERT(!result.has_error());
  array = result.value();
  std::cout << "length of adj_list offset chunk for vertex id 900: "
            << array->length() << std::endl;
}

int main(int argc, char* argv[]) {
  // read file and construct graph info
  std::string path =
      TEST_DATA_DIR + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto graph_info = graphar::GraphInfo::Load(path).value();

  // vertex property chunk reader
  std::cout << "Vertex property chunk reader" << std::endl;
  std::cout << "----------------------------" << std::endl;
  vertex_property_chunk_reader(graph_info);
  std::cout << std::endl;

  // adj_list chunk reader
  std::cout << "Adj_list chunk reader" << std::endl;
  std::cout << "---------------------" << std::endl;
  adj_list_chunk_reader(graph_info);
  std::cout << std::endl;

  // adj_list property chunk reader
  std::cout << "Adj_list property chunk reader" << std::endl;
  std::cout << "------------------------------" << std::endl;
  adj_list_property_chunk_reader(graph_info);
  std::cout << std::endl;

  // adj_list offset chunk reader
  std::cout << "Adj_list offset chunk reader" << std::endl;
  std::cout << "----------------------------" << std::endl;
  adj_list_offset_chunk_reader(graph_info);
}
