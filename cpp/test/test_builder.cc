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

#include <time.h>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>

#include "arrow/api.h"
#include "arrow/csv/api.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/api.h"
#include "arrow/stl.h"
#include "arrow/util/uri.h"
#include "graphar/util.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

#include "./util.h"
#include "graphar/api/high_level_writer.h"

#include <catch2/catch_test_macros.hpp>
namespace graphar {
TEST_CASE_METHOD(GlobalFixture, "Test_vertices_builder") {
  std::cout << "Test vertex builder" << std::endl;

  // construct vertex builder
  std::string vertex_meta_file =
      test_data_dir + "/ldbc_sample/parquet/" + "person.vertex.yml";
  auto vertex_meta = Yaml::LoadFile(vertex_meta_file).value();
  auto vertex_info = VertexInfo::Load(vertex_meta).value();
  IdType start_index = 0;
  auto maybe_builder =
      builder::VerticesBuilder::Make(vertex_info, "/tmp/", start_index);
  REQUIRE(!maybe_builder.has_error());
  auto builder = maybe_builder.value();

  // get & set writer options
  WriterOptions::ParquetOptionBuilder parquetOptionBuilder;
  parquetOptionBuilder.compression(arrow::Compression::LZ4);
  builder->SetWriterOptions(parquetOptionBuilder.build());
  REQUIRE(builder->GetWriterOptions() != nullptr);

  // get & set validate level
  REQUIRE(builder->GetValidateLevel() == ValidateLevel::no_validate);
  builder->SetValidateLevel(ValidateLevel::strong_validate);
  REQUIRE(builder->GetValidateLevel() == ValidateLevel::strong_validate);

  // check different validate levels
  builder::Vertex v;
  v.AddProperty("id", "id_of_string");
  REQUIRE(builder->AddVertex(v, 0, ValidateLevel::no_validate).ok());
  REQUIRE(builder->AddVertex(v, 0, ValidateLevel::weak_validate).ok());
  auto st = builder->AddVertex(v, -2, ValidateLevel::weak_validate);
  REQUIRE(
      builder->AddVertex(v, -2, ValidateLevel::weak_validate).IsIndexError());
  REQUIRE(
      builder->AddVertex(v, 0, ValidateLevel::strong_validate).IsTypeError());
  v.AddProperty("invalid_name", "invalid_value");
  REQUIRE(builder->AddVertex(v, 0).IsKeyError());

  // clear vertices
  builder->Clear();
  REQUIRE(builder->GetNum() == 0);

  // add vertices
  std::ifstream fp(test_data_dir + "/ldbc_sample/person_0_0.csv");
  std::string line;
  getline(fp, line);
  int m = 4;
  std::vector<std::string> names;
  std::istringstream readstr(line);
  for (int i = 0; i < m; i++) {
    std::string name;
    getline(readstr, name, '|');
    names.push_back(name);
  }

  int lines = 0;
  while (getline(fp, line)) {
    lines++;
    std::string val;
    std::istringstream readstr(line);
    builder::Vertex v;
    for (int i = 0; i < m; i++) {
      getline(readstr, val, '|');
      if (i == 0) {
        int64_t x = 0;
        for (size_t j = 0; j < val.length(); j++)
          x = x * 10 + val[j] - '0';
        v.AddProperty(names[i], x);
      } else {
        v.AddProperty(names[i], val);
      }
    }
    REQUIRE(builder->AddVertex(v).ok());
  }

  // check the number of vertices in builder
  REQUIRE(builder->GetNum() == lines);

  // dump to files
  REQUIRE(builder->Dump().ok());

  // can not add new vertices after dumping
  REQUIRE(builder->AddVertex(v).IsInvalid());

  // check the number of vertices dumped
  auto fs = arrow::fs::FileSystemFromUriOrPath(test_data_dir).ValueOrDie();
  auto input =
      fs->OpenInputStream("/tmp/vertex/person/vertex_count").ValueOrDie();
  auto num = input->Read(sizeof(IdType)).ValueOrDie();
  const IdType* ptr = reinterpret_cast<const IdType*>(num->data());
  REQUIRE((*ptr) == start_index + builder->GetNum());
  // check parquet file compression
  auto parquet_file = "/tmp/vertex/person/id/chunk0";
  std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
  REQUIRE(graphar::util::OpenParquetArrowReader(
              parquet_file, arrow::default_memory_pool(), &parquet_reader)
              .ok());
  std::shared_ptr<arrow::Table> parquet_table;
  REQUIRE(parquet_reader->ReadTable(&parquet_table).ok());
  auto parquet_metadata = parquet_reader->parquet_reader()->metadata();
  auto row_group_meta = parquet_metadata->RowGroup(0);
  auto col_meta = row_group_meta->ColumnChunk(0);
  REQUIRE(col_meta->compression() == parquet::Compression::LZ4);
}

TEST_CASE_METHOD(GlobalFixture, "test_edges_builder") {
  std::cout << "Test edge builder" << std::endl;
  // construct edge builder
  std::string edge_meta_file =
      test_data_dir + "/ldbc_sample/parquet/" + "person_knows_person.edge.yml";
  auto edge_meta = Yaml::LoadFile(edge_meta_file).value();
  auto edge_info = EdgeInfo::Load(edge_meta).value();
  auto vertices_num = 903;
  auto maybe_builder = builder::EdgesBuilder::Make(
      edge_info, "/tmp/", AdjListType::ordered_by_dest, vertices_num);
  REQUIRE(!maybe_builder.has_error());
  auto builder = maybe_builder.value();

  // get & set writer options
  WriterOptions::ParquetOptionBuilder parquetOptionBuilder;
  parquetOptionBuilder.compression(arrow::Compression::LZ4);
  builder->SetWriterOptions(parquetOptionBuilder.build());
  REQUIRE(builder->GetWriterOptions() != nullptr);

  // get & set validate level
  REQUIRE(builder->GetValidateLevel() == ValidateLevel::no_validate);
  builder->SetValidateLevel(ValidateLevel::strong_validate);
  REQUIRE(builder->GetValidateLevel() == ValidateLevel::strong_validate);

  // check different validate levels
  builder::Edge e(0, 1);
  e.AddProperty("creationDate", 2020);
  REQUIRE(builder->AddEdge(e, ValidateLevel::no_validate).ok());
  REQUIRE(builder->AddEdge(e, ValidateLevel::weak_validate).ok());
  REQUIRE(builder->AddEdge(e, ValidateLevel::strong_validate).IsTypeError());
  e.AddProperty("invalid_name", "invalid_value");
  REQUIRE(builder->AddEdge(e).IsKeyError());

  // clear edges
  builder->Clear();
  REQUIRE(builder->GetNum() == 0);

  // add edges
  std::ifstream fp(test_data_dir + "/ldbc_sample/person_knows_person_0_0.csv");
  std::string line;
  getline(fp, line);
  std::vector<std::string> names;
  std::istringstream readstr(line);
  std::map<std::string, int64_t> mapping;
  int64_t cnt = 0, lines = 0;

  while (getline(fp, line)) {
    lines++;
    std::string val;
    std::istringstream readstr(line);
    int64_t s = 0, d = 0;
    for (int i = 0; i < 3; i++) {
      getline(readstr, val, '|');
      if (i == 0) {
        if (mapping.find(val) == mapping.end())
          mapping[val] = cnt++;
        s = mapping[val];
      } else if (i == 1) {
        if (mapping.find(val) == mapping.end())
          mapping[val] = cnt++;
        d = mapping[val];
      } else {
        builder::Edge e(s, d);
        e.AddProperty("creationDate", val);
        REQUIRE(builder->AddEdge(e).ok());
      }
    }
  }

  // check the number of edges in builder
  REQUIRE(builder->GetNum() == lines);

  // dump to files
  REQUIRE(builder->Dump().ok());

  // can not add new edges after dumping
  REQUIRE(builder->AddEdge(e).IsInvalid());

  // check the number of vertices dumped
  auto fs = arrow::fs::FileSystemFromUriOrPath(test_data_dir).ValueOrDie();
  auto input =
      fs->OpenInputStream(
            "/tmp/edge/person_knows_person/ordered_by_dest/vertex_count")
          .ValueOrDie();
  auto num = input->Read(sizeof(IdType)).ValueOrDie();
  const IdType* ptr = reinterpret_cast<const IdType*>(num->data());
  REQUIRE((*ptr) == vertices_num);

  // check parquet file compression
  auto parquet_file =
      "/tmp/edge/person_knows_person/ordered_by_dest/creationDate/part0/chunk0";
  std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
  REQUIRE(graphar::util::OpenParquetArrowReader(
              parquet_file, arrow::default_memory_pool(), &parquet_reader)
              .ok());
  std::shared_ptr<arrow::Table> parquet_table;
  REQUIRE(parquet_reader->ReadTable(&parquet_table).ok());
  auto parquet_metadata = parquet_reader->parquet_reader()->metadata();
  auto row_group_meta = parquet_metadata->RowGroup(0);
  auto col_meta = row_group_meta->ColumnChunk(0);
  REQUIRE(col_meta->compression() == parquet::Compression::LZ4);
}
}  // namespace graphar
