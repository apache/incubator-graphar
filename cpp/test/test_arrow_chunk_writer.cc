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
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "arrow/adapters/orc/adapter.h"
#include "arrow/api.h"
#include "arrow/csv/api.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/api.h"
#include "arrow/stl.h"
#include "arrow/util/uri.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

#include "./util.h"
#include "gar/graph_info.h"
#include "gar/writer/arrow_chunk_writer.h"

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

TEST_CASE("test_vertex_property_wrtier_from_file") {
  std::string root;
  REQUIRE(GetTestResourceRoot(&root).ok());

  REQUIRE(!root.empty());
  std::string path = root + "/ldbc_sample/person_0_0.csv";
  arrow::io::IOContext io_context = arrow::io::default_io_context();

  auto fs = arrow::fs::FileSystemFromUriOrPath(path).ValueOrDie();
  std::shared_ptr<arrow::io::InputStream> input =
      fs->OpenInputStream(path).ValueOrDie();

  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  parse_options.delimiter = '|';
  auto convert_options = arrow::csv::ConvertOptions::Defaults();

  // Instantiate TableReader from input stream and options
  auto maybe_reader = arrow::csv::TableReader::Make(
      io_context, input, read_options, parse_options, convert_options);
  REQUIRE(maybe_reader.ok());
  std::shared_ptr<arrow::csv::TableReader> reader = *maybe_reader;

  // Read table from CSV file
  auto maybe_table = reader->Read();
  REQUIRE(maybe_table.ok());
  std::shared_ptr<arrow::Table> table = *maybe_table;
  std::cout << table->num_rows() << ' ' << table->num_columns() << std::endl;

  std::string vertex_meta_file =
      root + "/ldbc_sample/parquet/" + "person.vertex.yml";
  auto vertex_meta = GAR_NAMESPACE::Yaml::LoadFile(vertex_meta_file).value();
  auto vertex_info = GAR_NAMESPACE::VertexInfo::Load(vertex_meta).value();
  REQUIRE(vertex_info.GetLabel() == "person");
  GAR_NAMESPACE::VertexPropertyWriter writer(vertex_info, "/tmp/");
  REQUIRE(writer.WriteTable(table, 0).ok());
  REQUIRE(writer.WriteVerticesNum(table->num_rows()).ok());

  input = fs->OpenInputStream("/tmp/vertex/person/vertex_count").ValueOrDie();
  auto num = input->Read(sizeof(GAR_NAMESPACE::IdType)).ValueOrDie();
  GAR_NAMESPACE::IdType* ptr = (GAR_NAMESPACE::IdType*) num->data();
  REQUIRE((*ptr) == table->num_rows());
}

TEST_CASE("test_orc_and_parquet_reader") {
  std::string root;
  REQUIRE(GetTestResourceRoot(&root).ok());

  arrow::Status st;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::string path1 = root + "/ldbc_sample/orc" +
                      "/vertex/person/firstName_lastName_gender/chunk1";
  std::string path2 = root + "/ldbc_sample/parquet" +
                      "/vertex/person/firstName_lastName_gender/chunk1";
  arrow::io::IOContext io_context = arrow::io::default_io_context();

  // Open ORC file reader
  auto fs1 = arrow::fs::FileSystemFromUriOrPath(path1).ValueOrDie();
  std::shared_ptr<arrow::io::RandomAccessFile> input1 =
      fs1->OpenInputFile(path1).ValueOrDie();
  std::unique_ptr<arrow::adapters::orc::ORCFileReader> reader =
      arrow::adapters::orc::ORCFileReader::Open(input1, pool).ValueOrDie();

  // Read entire file as a single Arrow table
  auto maybe_table = reader->Read();
  std::shared_ptr<arrow::Table> table1 = maybe_table.ValueOrDie();

  // Open Parquet file reader
  auto fs2 = arrow::fs::FileSystemFromUriOrPath(path2).ValueOrDie();
  std::shared_ptr<arrow::io::RandomAccessFile> input2 =
      fs2->OpenInputFile(path2).ValueOrDie();
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  st = parquet::arrow::OpenFile(input2, pool, &arrow_reader);

  // Read entire file as a single Arrow table
  std::shared_ptr<arrow::Table> table2;
  st = arrow_reader->ReadTable(&table2);

  REQUIRE(table1->GetColumnByName("firstName")->ToString() ==
          table2->GetColumnByName("firstName")->ToString());
  REQUIRE(table1->GetColumnByName("lastName")->ToString() ==
          table2->GetColumnByName("lastName")->ToString());
  REQUIRE(table1->GetColumnByName("gender")->ToString() ==
          table2->GetColumnByName("gender")->ToString());
}

TEST_CASE("test_edge_chunk_writer") {
  std::string root;
  REQUIRE(GetTestResourceRoot(&root).ok());

  arrow::Status st;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::string path = root +
                     "/ldbc_sample/parquet/edge/person_knows_person/"
                     "unordered_by_source/adj_list/part0/chunk0";
  auto fs = arrow::fs::FileSystemFromUriOrPath(path).ValueOrDie();
  std::shared_ptr<arrow::io::RandomAccessFile> input =
      fs->OpenInputFile(path).ValueOrDie();
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  st = parquet::arrow::OpenFile(input, pool, &arrow_reader);
  // Read entire file as a single Arrow table
  std::shared_ptr<arrow::Table> maybe_table;
  st = arrow_reader->ReadTable(&maybe_table);
  REQUIRE(st.ok());

  std::shared_ptr<arrow::Table> table =
      maybe_table
          ->RenameColumns({GAR_NAMESPACE::GeneralParams::kSrcIndexCol,
                           GAR_NAMESPACE::GeneralParams::kDstIndexCol})
          .ValueOrDie();
  std::cout << table->schema()->ToString() << std::endl;
  std::cout << table->num_rows() << ' ' << table->num_columns() << std::endl;

  // Write edges of vertex chunk 0 to files
  std::string edge_meta_file =
      root + "/ldbc_sample/csv/" + "person_knows_person.edge.yml";
  auto edge_meta = GAR_NAMESPACE::Yaml::LoadFile(edge_meta_file).value();
  auto edge_info = GAR_NAMESPACE::EdgeInfo::Load(edge_meta).value();
  GAR_NAMESPACE::EdgeChunkWriter writer(
      edge_info, "/tmp/", GAR_NAMESPACE::AdjListType::ordered_by_source);
  REQUIRE(writer.SortAndWriteAdjListTable(table, 0, 0).ok());

  // Write number of edges for vertex chunk 0
  REQUIRE(writer.WriteEdgesNum(0, table->num_rows()).ok());
  std::shared_ptr<arrow::io::InputStream> input2 =
      fs->OpenInputStream(
            "/tmp/edge/person_knows_person/ordered_by_source/adj_list/"
            "edge_count0")
          .ValueOrDie();
  auto num = input2->Read(sizeof(GAR_NAMESPACE::IdType)).ValueOrDie();
  GAR_NAMESPACE::IdType* ptr = (GAR_NAMESPACE::IdType*) num->data();
  REQUIRE((*ptr) == table->num_rows());
}
