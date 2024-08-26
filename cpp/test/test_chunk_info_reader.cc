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

#include <cstdlib>

#include "./util.h"
#include "graphar/api/meta_reader.h"

#include <catch2/catch_test_macros.hpp>

namespace graphar {

TEST_CASE_METHOD(GlobalFixture, "ChunkInfoReader") {
  // read file and construct graph info
  std::string path =
      test_data_dir + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  std::string src_type = "person", edge_type = "knows", dst_type = "person";
  std::string vertex_property_name = "id";
  std::string edge_property_name = "creationDate";
  auto maybe_graph_info = GraphInfo::Load(path);
  REQUIRE(maybe_graph_info.status().ok());
  auto graph_info = maybe_graph_info.value();
  auto vertex_info = graph_info->GetVertexInfo(src_type);
  REQUIRE(vertex_info != nullptr);
  auto v_pg = vertex_info->GetPropertyGroup(vertex_property_name);
  REQUIRE(v_pg != nullptr);
  auto edge_info = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
  REQUIRE(edge_info != nullptr);
  auto e_pg = edge_info->GetPropertyGroup(edge_property_name);
  REQUIRE(e_pg != nullptr);

  SECTION("VertexPropertyChunkInfoReader") {
    // make from graph info and property name
    auto maybe_reader = VertexPropertyChunkInfoReader::Make(
        graph_info, src_type, vertex_property_name);
    REQUIRE(!maybe_reader.has_error());
    auto reader = maybe_reader.value();
    REQUIRE(reader->GetChunkNum() == 10);

    SECTION("Basics") {
      // get chunk file path & validate
      auto maybe_chunk_path = reader->GetChunk();
      REQUIRE(maybe_chunk_path.status().ok());
      std::string chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path ==
              test_data_dir + "/ldbc_sample/parquet/vertex/person/id/chunk0");
      REQUIRE(reader->seek(520).ok());
      maybe_chunk_path = reader->GetChunk();
      REQUIRE(maybe_chunk_path.status().ok());
      chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path ==
              test_data_dir + "/ldbc_sample/parquet/vertex/person/id/chunk5");
      REQUIRE(reader->next_chunk().ok());
      maybe_chunk_path = reader->GetChunk();
      REQUIRE(maybe_chunk_path.status().ok());
      chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path ==
              test_data_dir + "/ldbc_sample/parquet/vertex/person/id/chunk6");
      REQUIRE(reader->seek(900).ok());
      maybe_chunk_path = reader->GetChunk();
      chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path ==
              test_data_dir + "/ldbc_sample/parquet/vertex/person/id/chunk9");
      // now is end of the chunks
      REQUIRE(reader->next_chunk().IsIndexError());
      // test seek the id not in the chunks
      REQUIRE(reader->seek(100000).IsIndexError());
    }

    SECTION("Make from graph info and property group") {
      auto maybe_reader =
          VertexPropertyChunkInfoReader::Make(graph_info, src_type, v_pg);
      REQUIRE(!maybe_reader.has_error());
      auto reader = maybe_reader.value();
      REQUIRE(reader->GetChunkNum() == 10);
    }

    SECTION("Make from vertex info and property group") {
      auto maybe_reader = VertexPropertyChunkInfoReader::Make(
          vertex_info, v_pg, graph_info->GetPrefix());
      REQUIRE(!maybe_reader.has_error());
      auto reader = maybe_reader.value();
      REQUIRE(reader->GetChunkNum() == 10);
    }
  }

  SECTION("AdjListChunkInfoReader") {
    auto maybe_reader =
        AdjListChunkInfoReader::Make(graph_info, src_type, edge_type, dst_type,
                                     AdjListType::ordered_by_source);
    REQUIRE(maybe_reader.status().ok());
    auto reader = maybe_reader.value();

    SECTION("Basics") {
      // get chunk file path & validate
      auto maybe_chunk_path = reader->GetChunk();
      REQUIRE(maybe_chunk_path.status().ok());
      auto chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path == test_data_dir +
                                "/ldbc_sample/parquet/edge/person_knows_person/"
                                "ordered_by_source/adj_list/part0/chunk0");
      REQUIRE(reader->seek(100).ok());
      maybe_chunk_path = reader->GetChunk();
      REQUIRE(maybe_chunk_path.status().ok());
      chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path == test_data_dir +
                                "/ldbc_sample/parquet/edge/person_knows_person/"
                                "ordered_by_source/adj_list/part0/chunk0");
      REQUIRE(reader->next_chunk().ok());
      maybe_chunk_path = reader->GetChunk();
      REQUIRE(maybe_chunk_path.status().ok());
      chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path == test_data_dir +
                                "/ldbc_sample/parquet/edge/person_knows_person/"
                                "ordered_by_source/adj_list/part1/chunk0");

      // seek_src & seek_dst
      REQUIRE(reader->seek_src(100).ok());
      maybe_chunk_path = reader->GetChunk();
      REQUIRE(maybe_chunk_path.status().ok());
      chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path == test_data_dir +
                                "/ldbc_sample/parquet/edge/person_knows_person/"
                                "ordered_by_source/adj_list/part1/chunk0");
      REQUIRE(reader->seek_src(900).ok());
      maybe_chunk_path = reader->GetChunk();
      REQUIRE(maybe_chunk_path.status().ok());
      chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path == test_data_dir +
                                "/ldbc_sample/parquet/edge/person_knows_person/"
                                "ordered_by_source/adj_list/part9/chunk0");
      REQUIRE(reader->next_chunk().IsIndexError());

      // seek an invalid src id
      REQUIRE(reader->seek_src(1000).IsIndexError());
      REQUIRE(reader->seek_dst(100).IsInvalid());
    }

    SECTION("Make from edge info") {
      // test reader to read ordered by dest
      auto maybe_dst_reader = AdjListChunkInfoReader::Make(
          edge_info, AdjListType::ordered_by_dest, graph_info->GetPrefix());
      REQUIRE(maybe_dst_reader.status().ok());
      auto& dst_reader = maybe_dst_reader.value();
      REQUIRE(dst_reader->seek_dst(100).ok());
      auto maybe_chunk_path = dst_reader->GetChunk();
      REQUIRE(maybe_chunk_path.status().ok());
      auto chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path == test_data_dir +
                                "/ldbc_sample/parquet/edge/person_knows_person/"
                                "ordered_by_dest/adj_list/part1/chunk0");
      // seek an invalid dst id
      REQUIRE(dst_reader->seek_dst(1000).IsIndexError());
      REQUIRE(dst_reader->seek_src(100).IsInvalid());
    }
  }

  SECTION("AdjListOffsetChunkInfoReader") {
    auto maybe_offset_reader = AdjListOffsetChunkInfoReader::Make(
        graph_info, src_type, edge_type, dst_type,
        AdjListType::ordered_by_source);
    REQUIRE(maybe_offset_reader.status().ok());
    auto reader = maybe_offset_reader.value();

    SECTION("Basics") {
      // get chunk file path & validate
      auto maybe_chunk_path = reader->GetChunk();
      REQUIRE(maybe_chunk_path.status().ok());
      std::string chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path == test_data_dir +
                                "/ldbc_sample/parquet/edge/person_knows_person/"
                                "ordered_by_source/offset/chunk0");
      REQUIRE(reader->seek(520).ok());
      maybe_chunk_path = reader->GetChunk();
      REQUIRE(maybe_chunk_path.status().ok());
      chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path == test_data_dir +
                                "/ldbc_sample/parquet/edge/person_knows_person/"
                                "ordered_by_source/offset/chunk5");
      REQUIRE(reader->next_chunk().ok());
      maybe_chunk_path = reader->GetChunk();
      REQUIRE(maybe_chunk_path.status().ok());
      chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path == test_data_dir +
                                "/ldbc_sample/parquet/edge/person_knows_person/"
                                "ordered_by_source/offset/chunk6");
      REQUIRE(reader->seek(900).ok());
      maybe_chunk_path = reader->GetChunk();
      chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path == test_data_dir +
                                "/ldbc_sample/parquet/edge/person_knows_person/"
                                "ordered_by_source/offset/chunk9");
      // now is end of the chunks
      REQUIRE(reader->next_chunk().IsIndexError());

      // test seek the id not in the chunks
      REQUIRE(reader->seek(100000).IsIndexError());
    }

    SECTION("Make from edge info") {
      auto maybe_offset_reader = AdjListOffsetChunkInfoReader::Make(
          edge_info, AdjListType::ordered_by_dest, graph_info->GetPrefix());
      REQUIRE(maybe_offset_reader.status().ok());
    }
  }

  SECTION("AdjListPropertyChunkInfoReader") {
    auto maybe_property_reader = AdjListPropertyChunkInfoReader::Make(
        graph_info, src_type, edge_type, dst_type, edge_property_name,
        AdjListType::ordered_by_source);
    REQUIRE(maybe_property_reader.status().ok());
    auto reader = maybe_property_reader.value();

    SECTION("Basics") {
      // get chunk file path & validate
      auto maybe_chunk_path = reader->GetChunk();
      REQUIRE(maybe_chunk_path.status().ok());
      auto chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path == test_data_dir +
                                "/ldbc_sample/parquet/edge/person_knows_person/"
                                "ordered_by_source/creationDate/part0/chunk0");
      REQUIRE(reader->seek(100).ok());
      maybe_chunk_path = reader->GetChunk();
      REQUIRE(maybe_chunk_path.status().ok());
      chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path == test_data_dir +
                                "/ldbc_sample/parquet/edge/person_knows_person/"
                                "ordered_by_source/creationDate/part0/chunk0");
      REQUIRE(reader->next_chunk().ok());
      maybe_chunk_path = reader->GetChunk();
      REQUIRE(maybe_chunk_path.status().ok());
      chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path == test_data_dir +
                                "/ldbc_sample/parquet/edge/person_knows_person/"
                                "ordered_by_source/creationDate/part1/chunk0");

      // seek_src & seek_dst
      REQUIRE(reader->seek_src(100).ok());
      maybe_chunk_path = reader->GetChunk();
      REQUIRE(maybe_chunk_path.status().ok());
      chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path == test_data_dir +
                                "/ldbc_sample/parquet/edge/person_knows_person/"
                                "ordered_by_source/creationDate/part1/chunk0");
      REQUIRE(reader->seek_src(900).ok());
      maybe_chunk_path = reader->GetChunk();
      REQUIRE(maybe_chunk_path.status().ok());
      chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path == test_data_dir +
                                "/ldbc_sample/parquet/edge/person_knows_person/"
                                "ordered_by_source/creationDate/part9/chunk0");
      REQUIRE(reader->next_chunk().IsIndexError());

      // seek an invalid src id
      REQUIRE(reader->seek_src(1000).IsIndexError());
      REQUIRE(reader->seek_dst(100).IsInvalid());
    }

    SECTION("Make from graph info and property group") {
      // test reader to read ordered by dest
      auto maybe_dst_reader = AdjListPropertyChunkInfoReader::Make(
          graph_info, src_type, edge_type, dst_type, e_pg,
          AdjListType::ordered_by_dest);
      REQUIRE(maybe_dst_reader.status().ok());
      auto dst_reader = maybe_dst_reader.value();
      REQUIRE(dst_reader->seek_dst(100).ok());
      auto maybe_chunk_path = dst_reader->GetChunk();
      REQUIRE(maybe_chunk_path.status().ok());
      auto chunk_path = maybe_chunk_path.value();
      REQUIRE(chunk_path == test_data_dir +
                                "/ldbc_sample/parquet/edge/person_knows_person/"
                                "ordered_by_dest/creationDate/part1/chunk0");

      // seek an invalid dst id
      REQUIRE(dst_reader->seek_dst(1000).IsIndexError());
      REQUIRE(dst_reader->seek_src(100).IsInvalid());
    }

    SECTION("Make from vertex info and property group") {
      auto maybe_dst_reader = AdjListPropertyChunkInfoReader::Make(
          edge_info, v_pg, AdjListType::ordered_by_dest,
          graph_info->GetPrefix());
      REQUIRE(maybe_dst_reader.status().ok());
    }
  }
}
}  // namespace graphar
