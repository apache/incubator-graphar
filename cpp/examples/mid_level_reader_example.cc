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

#include "arrow/api.h"
#include "arrow/filesystem/api.h"

#include "./config.h"
#include "gar/reader/arrow_chunk_reader.h"
#include "gar/util/expression.h"

void vertex_property_chunk_reader(const GAR_NAMESPACE::GraphInfo& graph_info) {
  // constuct reader
  std::string label = "person", property_name = "gender";
  ASSERT(graph_info.GetVertexInfo(label).status().ok());
  auto maybe_group = graph_info.GetVertexPropertyGroup(label, property_name);
  ASSERT(maybe_group.status().ok());
  auto group = maybe_group.value();
  auto maybe_reader = GAR_NAMESPACE::ConstructVertexPropertyArrowChunkReader(
      graph_info, label, group);
  ASSERT(maybe_reader.status().ok());
  auto reader = maybe_reader.value();

  // use reader
  auto result = reader.GetChunk();
  ASSERT(!result.has_error());
  auto range = reader.GetRange().value();
  std::cout << "chunk number: " << reader.GetChunkNum() << std::endl;
  std::cout << "range of fisrt vertex property chunk: " << range.first << " "
            << range.second << std::endl;
  auto table = result.value();
  std::cout << "rows number of first vertex property chunk: "
            << table->num_rows() << std::endl;
  std::cout << "schema of first vertex property chunk: " << std::endl
            << table->schema()->ToString() << std::endl;
  // seek vertex id
  ASSERT(reader.seek(100).ok());
  result = reader.GetChunk();
  ASSERT(!result.has_error());
  range = reader.GetRange().value();
  std::cout << "range of vertex property chunk for vertex id 100: "
            << range.first << " " << range.second << std::endl;
  // next chunk
  ASSERT(reader.next_chunk().ok());
  result = reader.GetChunk();
  ASSERT(!result.has_error());
  range = reader.GetRange().value();
  std::cout << "range of next vertex property chunk: " << range.first << " "
            << range.second << std::endl;

  // reader with filter pushdown
  auto filter = GAR_NAMESPACE::_Equal(GAR_NAMESPACE::_Property("gender"),
                                      GAR_NAMESPACE::_Literal("female"));
  std::vector<std::string> expected_cols{"firstName", "lastName"};
  auto maybe_filter_reader =
      GAR_NAMESPACE::ConstructVertexPropertyArrowChunkReader(graph_info, label,
                                                             group);
  ASSERT(maybe_filter_reader.status().ok());
  auto filter_reader = maybe_filter_reader.value();
  filter_reader.Filter(filter);
  filter_reader.Select(expected_cols);
  auto filter_result = filter_reader.GetChunk();
  ASSERT(!result.has_error());
  auto filter_table = filter_result.value();
  std::cout << "rows number of first filtered vertex property chunk: "
            << filter_table->num_rows() << std::endl;
  std::cout << "schema of first filtered vertex property chunk: " << std::endl
            << filter_table->schema()->ToString() << std::endl;
}

void adj_list_chunk_reader(const GAR_NAMESPACE::GraphInfo& graph_info) {
  // constuct reader
  std::string src_label = "person", edge_label = "knows", dst_label = "person";
  ASSERT(
      graph_info.GetEdgeInfo(src_label, edge_label, dst_label).status().ok());
  auto maybe_reader = GAR_NAMESPACE::ConstructAdjListArrowChunkReader(
      graph_info, src_label, edge_label, dst_label,
      GAR_NAMESPACE::AdjListType::ordered_by_source);
  ASSERT(maybe_reader.status().ok());

  // use reader
  auto reader = maybe_reader.value();
  auto result = reader.GetChunk();
  ASSERT(!result.has_error());
  auto table = result.value();
  std::cout << "rows number of first adj_list chunk: " << table->num_rows()
            << std::endl;
  std::cout << "schema of first adj_list chunk: " << std::endl
            << table->schema()->ToString() << std::endl;
  // seek src
  ASSERT(reader.seek_src(100).ok());
  result = reader.GetChunk();
  ASSERT(!result.has_error());
  table = result.value();
  std::cout << "rows number of first adj_list chunk for outgoing edges of "
               "vertex id 100: "
            << table->num_rows() << std::endl;
  // next chunk
  ASSERT(reader.next_chunk().ok());
  result = reader.GetChunk();
  ASSERT(!result.has_error());
  table = result.value();
  std::cout << "rows number of next adj_list chunk: " << table->num_rows()
            << std::endl;
}

void adj_list_property_chunk_reader(
    const GAR_NAMESPACE::GraphInfo& graph_info) {
  // constuct reader
  std::string src_label = "person", edge_label = "knows", dst_label = "person",
              property_name = "creationDate";
  auto maybe_group = graph_info.GetEdgePropertyGroup(
      src_label, edge_label, dst_label, property_name,
      GAR_NAMESPACE::AdjListType::ordered_by_source);
  ASSERT(maybe_group.status().ok());
  auto group = maybe_group.value();
  auto maybe_reader = GAR_NAMESPACE::ConstructAdjListPropertyArrowChunkReader(
      graph_info, src_label, edge_label, dst_label, group,
      GAR_NAMESPACE::AdjListType::ordered_by_source);
  ASSERT(maybe_reader.status().ok());
  auto reader = maybe_reader.value();

  // use reader
  auto result = reader.GetChunk();
  ASSERT(!result.has_error());
  auto table = result.value();
  std::cout << "rows number of first adj_list property chunk: "
            << table->num_rows() << std::endl;
  std::cout << "schema of first adj_list property chunk: " << std::endl
            << table->schema()->ToString() << std::endl;
  // seek src
  ASSERT(reader.seek_src(100).ok());
  result = reader.GetChunk();
  ASSERT(!result.has_error());
  table = result.value();
  std::cout << "rows number of first adj_list property chunk for outgoing "
               "edges of vertex id 100: "
            << table->num_rows() << std::endl;
  // next chunk
  ASSERT(reader.next_chunk().ok());
  result = reader.GetChunk();
  ASSERT(!result.has_error());
  table = result.value();
  std::cout << "rows number of next adj_list property chunk: "
            << table->num_rows() << std::endl;

  // reader with filter pushdown
  auto expr1 = GAR_NAMESPACE::_LessThan(
      GAR_NAMESPACE::_Literal("2012-06-02T04:30:44.526+0000"),
      GAR_NAMESPACE::_Property(property_name));
  auto expr2 = GAR_NAMESPACE::_Equal(GAR_NAMESPACE::_Property(property_name),
                                     GAR_NAMESPACE::_Property(property_name));
  auto filter = GAR_NAMESPACE::_And(expr1, expr2);
  std::vector<std::string> expected_cols{"creationDate"};
  auto maybe_filter_reader =
      GAR_NAMESPACE::ConstructAdjListPropertyArrowChunkReader(
          graph_info, src_label, edge_label, dst_label, group,
          GAR_NAMESPACE::AdjListType::ordered_by_source);
  ASSERT(maybe_filter_reader.status().ok());
  auto filter_reader = maybe_filter_reader.value();
  filter_reader.Filter(filter);
  filter_reader.Select(expected_cols);
  auto filter_result = filter_reader.GetChunk();
  ASSERT(!result.has_error());
  auto filter_table = filter_result.value();
  std::cout << "rows number of first filtered adj_list property chunk: "
            << filter_table->num_rows() << std::endl;
}

void adj_list_offset_chunk_reader(const GAR_NAMESPACE::GraphInfo& graph_info) {
  // constuct reader
  std::string src_label = "person", edge_label = "knows", dst_label = "person";
  ASSERT(
      graph_info.GetEdgeInfo(src_label, edge_label, dst_label).status().ok());
  auto maybe_reader = GAR_NAMESPACE::ConstructAdjListOffsetArrowChunkReader(
      graph_info, src_label, edge_label, dst_label,
      GAR_NAMESPACE::AdjListType::ordered_by_source);
  ASSERT(maybe_reader.status().ok());
  auto reader = maybe_reader.value();

  // use reader
  auto result = reader.GetChunk();
  ASSERT(!result.has_error());
  auto array = result.value();
  std::cout << "length of first adj_list offset chunk: " << array->length()
            << std::endl;
  // next chunk
  ASSERT(reader.next_chunk().ok());
  result = reader.GetChunk();
  ASSERT(!result.has_error());
  array = result.value();
  std::cout << "length of next adj_list offset chunk: " << array->length()
            << std::endl;
  // seek vertex id
  ASSERT(reader.seek(900).ok());
  result = reader.GetChunk();
  ASSERT(!result.has_error());
  array = result.value();
  std::cout << "length of adj_list offset chunk for vertex id 900: "
            << array->length() << std::endl;
}

int main(int argc, char* argv[]) {
  // read file and construct graph info
  std::string path =
      TEST_DATA_DIR + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto graph_info = GAR_NAMESPACE::GraphInfo::Load(path).value();

  // vertex property chunk reader
  std::cout << "Vertex property chunk reader" << std::endl;
  vertex_property_chunk_reader(graph_info);
  std::cout << std::endl;

  // adj_list chunk reader
  std::cout << "Adj_list chunk reader" << std::endl;
  adj_list_chunk_reader(graph_info);
  std::cout << std::endl;

  // adj_list property chunk reader
  std::cout << "Adj_list property chunk reader" << std::endl;
  adj_list_property_chunk_reader(graph_info);
  std::cout << std::endl;

  // adj_list offset chunk reader
  std::cout << "Adj_list offset chunk reader" << std::endl;
  adj_list_offset_chunk_reader(graph_info);
}
