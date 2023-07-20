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

#include <cstdlib>

#include "arrow/adapters/orc/adapter.h"
#include "arrow/api.h"
#include "arrow/csv/api.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/api.h"
#include "arrow/stl.h"
#include "arrow/util/uri.h"
#include "parquet/arrow/writer.h"

#include "./util.h"
#include "gar/reader/arrow_chunk_reader.h"
#include "gar/writer/arrow_chunk_writer.h"

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

TEST_CASE("test_vertex_property_arrow_chunk_reader") {
  std::string root;
  REQUIRE(GetTestResourceRoot(&root).ok());

  // read file and construct graph info
  std::string path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto maybe_graph_info = GAR_NAMESPACE::GraphInfo::Load(path);
  REQUIRE(maybe_graph_info.status().ok());
  auto graph_info = maybe_graph_info.value();

  // construct vertex chunk reader
  std::string label = "person", property_name = "id";
  REQUIRE(graph_info.GetVertexInfo(label).status().ok());
  auto maybe_group = graph_info.GetVertexPropertyGroup(label, property_name);
  REQUIRE(maybe_group.status().ok());
  auto group = maybe_group.value();
  auto maybe_reader = GAR_NAMESPACE::ConstructVertexPropertyArrowChunkReader(
      graph_info, label, group);
  REQUIRE(maybe_reader.status().ok());
  auto reader = maybe_reader.value();
  auto result = reader.GetChunk();
  REQUIRE(!result.has_error());
  auto range = reader.GetRange().value();
  auto table = result.value();
  REQUIRE(table->num_rows() == 100);
  REQUIRE(range.first == 0);
  REQUIRE(range.second == 100);

  // seek
  REQUIRE(reader.seek(100).ok());
  result = reader.GetChunk();
  REQUIRE(!result.has_error());
  range = reader.GetRange().value();
  table = result.value();
  REQUIRE(table->num_rows() == 100);
  REQUIRE(range.first == 100);
  REQUIRE(range.second == 200);
  REQUIRE(reader.next_chunk().ok());
  result = reader.GetChunk();
  REQUIRE(!result.has_error());
  range = reader.GetRange().value();
  table = result.value();
  REQUIRE(table->num_rows() == 100);
  REQUIRE(range.first == 200);
  REQUIRE(range.second == 300);
  REQUIRE(reader.seek(900).ok());
  result = reader.GetChunk();
  REQUIRE(!result.has_error());
  range = reader.GetRange().value();
  table = result.value();
  REQUIRE(table->num_rows() == 3);
  REQUIRE(range.first == 900);
  REQUIRE(range.second == 903);
  REQUIRE(reader.GetChunkNum() == 10);
  REQUIRE(reader.next_chunk().IsIndexError());

  REQUIRE(reader.seek(1024).IsIndexError());
}

TEST_CASE("test_adj_list_arrow_chunk_reader") {
  std::string root;
  REQUIRE(GetTestResourceRoot(&root).ok());

  // read file and construct graph info
  std::string path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto maybe_graph_info = GAR_NAMESPACE::GraphInfo::Load(path);
  REQUIRE(maybe_graph_info.status().ok());
  auto graph_info = maybe_graph_info.value();

  // construct adj list chunk reader
  std::string src_label = "person", edge_label = "knows", dst_label = "person";
  REQUIRE(
      graph_info.GetEdgeInfo(src_label, edge_label, dst_label).status().ok());
  auto maybe_reader = GAR_NAMESPACE::ConstructAdjListArrowChunkReader(
      graph_info, src_label, edge_label, dst_label,
      GAR_NAMESPACE::AdjListType::ordered_by_source);
  REQUIRE(maybe_reader.status().ok());
  auto reader = maybe_reader.value();
  auto result = reader.GetChunk();
  REQUIRE(!result.has_error());
  auto table = result.value();
  REQUIRE(table->num_rows() == 667);

  // seek
  REQUIRE(reader.seek(100).ok());
  result = reader.GetChunk();
  REQUIRE(!result.has_error());
  table = result.value();
  REQUIRE(table->num_rows() == 567);
  REQUIRE(reader.GetRowNumOfChunk() == 667);
  REQUIRE(reader.next_chunk().ok());
  result = reader.GetChunk();
  REQUIRE(!result.has_error());
  table = result.value();
  REQUIRE(table->num_rows() == 644);
  REQUIRE(reader.seek(1024).IsIndexError());

  // seek src & dst
  REQUIRE(reader.seek_src(100).ok());
  result = reader.GetChunk();
  REQUIRE(!result.has_error());
  table = result.value();
  REQUIRE(table->num_rows() == 644);
  REQUIRE(!reader.seek_dst(100).ok());

  REQUIRE(reader.seek_src(900).ok());
  result = reader.GetChunk();
  REQUIRE(!result.has_error());
  table = result.value();
  REQUIRE(table->num_rows() == 4);

  REQUIRE(reader.next_chunk().IsIndexError());
}

TEST_CASE("test_adj_list_property_arrow_chunk_reader") {
  std::string root;
  REQUIRE(GetTestResourceRoot(&root).ok());

  std::string path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto maybe_graph_info = GAR_NAMESPACE::GraphInfo::Load(path);
  REQUIRE(maybe_graph_info.status().ok());
  auto graph_info = maybe_graph_info.value();

  std::string src_label = "person", edge_label = "knows", dst_label = "person",
              property_name = "creationDate";
  auto maybe_group = graph_info.GetEdgePropertyGroup(
      src_label, edge_label, dst_label, property_name,
      GAR_NAMESPACE::AdjListType::ordered_by_source);
  REQUIRE(maybe_group.status().ok());
  auto group = maybe_group.value();
  auto maybe_reader = GAR_NAMESPACE::ConstructAdjListPropertyArrowChunkReader(
      graph_info, src_label, edge_label, dst_label, group,
      GAR_NAMESPACE::AdjListType::ordered_by_source);
  REQUIRE(maybe_reader.status().ok());
  auto reader = maybe_reader.value();
  auto result = reader.GetChunk();
  REQUIRE(!result.has_error());
  auto table = result.value();
  REQUIRE(table->num_rows() == 667);

  // seek
  REQUIRE(reader.seek(100).ok());
  result = reader.GetChunk();
  REQUIRE(!result.has_error());
  table = result.value();
  REQUIRE(table->num_rows() == 567);
  REQUIRE(reader.next_chunk().ok());
  result = reader.GetChunk();
  REQUIRE(!result.has_error());
  table = result.value();
  REQUIRE(table->num_rows() == 644);
  REQUIRE(reader.seek(1024).IsIndexError());

  // seek src & dst
  REQUIRE(reader.seek_src(100).ok());
  result = reader.GetChunk();
  REQUIRE(!result.has_error());
  table = result.value();
  REQUIRE(table->num_rows() == 644);
  REQUIRE(!reader.seek_dst(100).ok());

  REQUIRE(reader.seek_src(900).ok());
  result = reader.GetChunk();
  REQUIRE(!result.has_error());
  table = result.value();
  REQUIRE(table->num_rows() == 4);

  REQUIRE(reader.next_chunk().IsIndexError());
}

TEST_CASE("test_read_adj_list_offset_chunk_example") {
  std::string root;
  REQUIRE(GetTestResourceRoot(&root).ok());

  // read file and construct graph info
  std::string path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto maybe_graph_info = GAR_NAMESPACE::GraphInfo::Load(path);
  REQUIRE(maybe_graph_info.status().ok());
  auto graph_info = maybe_graph_info.value();

  // construct adj list chunk reader
  std::string src_label = "person", edge_label = "knows", dst_label = "person";
  REQUIRE(
      graph_info.GetEdgeInfo(src_label, edge_label, dst_label).status().ok());
  auto maybe_reader = GAR_NAMESPACE::ConstructAdjListOffsetArrowChunkReader(
      graph_info, src_label, edge_label, dst_label,
      GAR_NAMESPACE::AdjListType::ordered_by_source);
  REQUIRE(maybe_reader.status().ok());
  auto reader = maybe_reader.value();
  auto result = reader.GetChunk();
  REQUIRE(!result.has_error());
  auto array = result.value();
  REQUIRE(array->length() == 101);
  REQUIRE(reader.next_chunk().ok());
  result = reader.GetChunk();
  REQUIRE(!result.has_error());
  array = result.value();
  REQUIRE(array->length() == 101);

  // seek
  REQUIRE(reader.seek(900).ok());
  result = reader.GetChunk();
  REQUIRE(!result.has_error());
  array = result.value();
  REQUIRE(array->length() == 4);
  REQUIRE(reader.next_chunk().IsIndexError());
  REQUIRE(reader.seek(1024).IsIndexError());
}
