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
#include <vector>

#include "arrow/api.h"
#include "arrow/filesystem/api.h"

#include "./config.h"
#include "graphar/api/arrow_reader.h"
#include "graphar/fwd.h"

void vertex_property_chunk_reader(
    const std::shared_ptr<graphar::GraphInfo>& graph_info) {
  // create reader (property group)
  std::string type = "person", property_name = "gender";
  auto property_group =
      graph_info->GetVertexInfo(type)->GetPropertyGroup(property_name);
  auto maybe_reader = graphar::VertexPropertyArrowChunkReader::Make(
      graph_info, type, property_group);
  ASSERT(maybe_reader.status().ok());
  auto reader = maybe_reader.value();

  // use reader
  auto result = reader->GetChunk(graphar::GetChunkVersion::V1);
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
  result = reader->GetChunk(graphar::GetChunkVersion::V1);
  ASSERT(!result.has_error());
  table = result.value();
  index_col = table->GetColumnByName(graphar::GeneralParams::kVertexIndexCol);
  ASSERT(index_col != nullptr);
  std::cout << "Internal id column of vertex property chunk for vertex id 100: "
            << index_col->ToString() << " " << std::endl;
  // next chunk
  ASSERT(reader->next_chunk().ok());
  result = reader->GetChunk(graphar::GetChunkVersion::V1);
  ASSERT(!result.has_error());
  table = result.value();
  index_col = table->GetColumnByName(graphar::GeneralParams::kVertexIndexCol);
  ASSERT(index_col != nullptr);
  std::cout << "Internal id column of next chunk: " << index_col->ToString()
            << " " << std::endl;

  // read specific one column
  std::string specific_col_name = "lastName";
  auto maybe_specific_reader = graphar::VertexPropertyArrowChunkReader::Make(
      graph_info, type, specific_col_name);
  ASSERT(maybe_specific_reader.status().ok());
  auto specific_reader = maybe_specific_reader.value();
  auto specific_result =
      specific_reader->GetChunk(graphar::GetChunkVersion::V1);
  ASSERT(!result.has_error());
  auto specific_table = specific_result.value();
  std::cout << "rows number of first specificed vertex property chunk: "
            << specific_table->num_rows() << std::endl;
  ASSERT(specific_table->num_columns() == 2);
  std::cout << "schema of first specificed vertex property chunk: " << std::endl
            << specific_table->schema()->ToString() << std::endl;
  index_col =
      specific_table->GetColumnByName(graphar::GeneralParams::kVertexIndexCol);
  ASSERT(index_col != nullptr);
  std::cout << "Internal id column: " << index_col->ToString() << " "
            << std::endl;
  auto specific_col = specific_table->GetColumnByName("lastName");
  ASSERT(specific_col != nullptr);
  std::cout << "Internal id column: " << specific_col->ToString() << " "
            << std::endl;
  // read specific one column V2
  specific_col_name = "lastName";
  maybe_specific_reader = graphar::VertexPropertyArrowChunkReader::Make(
      graph_info, type, specific_col_name);
  ASSERT(maybe_specific_reader.status().ok());
  specific_reader = maybe_specific_reader.value();
  specific_result = specific_reader->GetChunk(graphar::GetChunkVersion::V2);
  ASSERT(!specific_result.has_error());
  specific_table = specific_result.value();
  std::cout << "rows number of first specificed vertex property chunk (V2): "
            << specific_table->num_rows() << std::endl;
  ASSERT(specific_table->num_columns() == 2);
  std::cout << "schema of first specificed vertex property chunk (V2): "
            << std::endl
            << specific_table->schema()->ToString() << std::endl;
  index_col =
      specific_table->GetColumnByName(graphar::GeneralParams::kVertexIndexCol);
  ASSERT(index_col != nullptr);
  std::cout << "Internal id column: " << index_col->ToString() << " "
            << std::endl;
  specific_col = specific_table->GetColumnByName("lastName");
  ASSERT(specific_col != nullptr);
  std::cout << "Internal id column: " << specific_col->ToString() << " "
            << std::endl;

  // read specific columns
  std::vector<std::string> specific_cols = {"firstName", "lastName"};
  maybe_specific_reader = graphar::VertexPropertyArrowChunkReader::Make(
      graph_info, type, specific_cols, graphar::SelectType::PROPERTIES);
  ASSERT(maybe_specific_reader.status().ok());
  specific_reader = maybe_specific_reader.value();
  specific_result = specific_reader->GetChunk(graphar::GetChunkVersion::V1);
  ASSERT(!result.has_error());
  specific_table = specific_result.value();
  std::cout << "rows number of specificed vertex properties chunk: "
            << specific_table->num_rows() << std::endl;
  ASSERT(specific_table->num_columns() == specific_cols.size() + 1);
  std::cout << "schema of specificed vertex properties chunk: " << std::endl
            << specific_table->schema()->ToString() << std::endl;
  index_col =
      specific_table->GetColumnByName(graphar::GeneralParams::kVertexIndexCol);
  ASSERT(index_col != nullptr);
  std::cout << "Internal id column: " << index_col->ToString() << " "
            << std::endl;
  specific_col = specific_table->GetColumnByName("firstName");
  ASSERT(specific_col != nullptr);
  std::cout << "firstName column: " << specific_col->ToString() << " "
            << std::endl;
  specific_col = specific_table->GetColumnByName("lastName");
  ASSERT(specific_col != nullptr);
  std::cout << "lastName column: " << specific_col->ToString() << " "
            << std::endl;

  // read specific columns V2
  specific_cols = {"firstName", "lastName"};
  maybe_specific_reader = graphar::VertexPropertyArrowChunkReader::Make(
      graph_info, type, specific_cols, graphar::SelectType::PROPERTIES);
  ASSERT(maybe_specific_reader.status().ok());
  specific_reader = maybe_specific_reader.value();
  specific_result = specific_reader->GetChunk(graphar::GetChunkVersion::V2);
  ASSERT(!specific_result.has_error());
  specific_table = specific_result.value();
  std::cout << "rows number of specificed vertex properties chunk (V2): "
            << specific_table->num_rows() << std::endl;
  ASSERT(specific_table->num_columns() == specific_cols.size() + 1);
  std::cout << "schema of specificed vertex properties chunk (V2): "
            << std::endl
            << specific_table->schema()->ToString() << std::endl;
  index_col =
      specific_table->GetColumnByName(graphar::GeneralParams::kVertexIndexCol);
  ASSERT(index_col != nullptr);
  std::cout << "Internal id column: " << index_col->ToString() << " "
            << std::endl;
  specific_col = specific_table->GetColumnByName("firstName");
  ASSERT(specific_col != nullptr);
  std::cout << "firstName column: " << specific_col->ToString() << " "
            << std::endl;
  specific_col = specific_table->GetColumnByName("lastName");
  ASSERT(specific_col != nullptr);
  std::cout << "lastName column: " << specific_col->ToString() << " "
            << std::endl;

  // reader with filter pushdown
  auto filter = graphar::_Equal(graphar::_Property("gender"),
                                graphar::_Literal("female"));
  std::vector<std::string> expected_cols{"firstName", "lastName"};
  auto maybe_filter_reader = graphar::VertexPropertyArrowChunkReader::Make(
      graph_info, type, property_group);
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
  // reader with filter pushdown && select specific column
  maybe_filter_reader = graphar::VertexPropertyArrowChunkReader::Make(
      graph_info, type, {"firstName", "lastName"},
      graphar::SelectType::PROPERTIES);
  ASSERT(maybe_filter_reader.status().ok());
  filter_reader = maybe_filter_reader.value();
  filter_reader->Filter(filter);
  filter_reader->Select(expected_cols);
  filter_result = filter_reader->GetChunk();
  ASSERT(!result.has_error());
  filter_table = filter_result.value();
  std::cout << "rows number of first filtered vertex property chunk (select "
               "specific column): "
            << filter_table->num_rows() << std::endl;
  std::cout << "schema of first filtered vertex property chunk (select "
               "specific column): "
            << std::endl
            << filter_table->schema()->ToString() << std::endl;
}

void adj_list_chunk_reader(
    const std::shared_ptr<graphar::GraphInfo>& graph_info) {
  // create reader
  std::string src_type = "person", edge_type = "knows", dst_type = "person";
  auto maybe_reader = graphar::AdjListArrowChunkReader::Make(
      graph_info, src_type, edge_type, dst_type,
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
  std::string src_type = "person", edge_type = "knows", dst_type = "person",
              property_name = "creationDate";
  auto maybe_reader = graphar::AdjListPropertyArrowChunkReader::Make(
      graph_info, src_type, edge_type, dst_type, property_name,
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
      graph_info, src_type, edge_type, dst_type, property_name,
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
  std::string src_type = "person", edge_type = "knows", dst_type = "person";
  auto maybe_reader = graphar::AdjListOffsetArrowChunkReader::Make(
      graph_info, src_type, edge_type, dst_type,
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
      GetTestingResourceRoot() + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
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
