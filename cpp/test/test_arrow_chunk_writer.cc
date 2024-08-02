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

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#ifdef ARROW_ORC
#include "arrow/adapters/orc/adapter.h"
#endif
#include "arrow/api.h"
#include "arrow/csv/api.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/api.h"
#include "arrow/stl.h"
#include "arrow/util/uri.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

#include "./util.h"
#include "graphar/api/arrow_writer.h"

#include <catch2/catch_test_macros.hpp>

namespace graphar {

TEST_CASE_METHOD(GlobalFixture, "TestVertexPropertyWriter") {
  std::string path = test_data_dir + "/ldbc_sample/person_0_0.csv";
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

  // Construct the writer
  std::string vertex_meta_file =
      test_data_dir + "/ldbc_sample/parquet/" + "person.vertex.yml";
  auto vertex_meta = Yaml::LoadFile(vertex_meta_file).value();
  auto vertex_info = VertexInfo::Load(vertex_meta).value();
  auto maybe_writer = VertexPropertyWriter::Make(vertex_info, "/tmp/");
  REQUIRE(!maybe_writer.has_error());
  auto writer = maybe_writer.value();

  // Get & set validate level
  REQUIRE(writer->GetValidateLevel() == ValidateLevel::no_validate);
  writer->SetValidateLevel(ValidateLevel::strong_validate);
  REQUIRE(writer->GetValidateLevel() == ValidateLevel::strong_validate);

  // Valid cases
  // Write the table
  REQUIRE(writer->WriteTable(table, 0).ok());
  // Write the number of vertices
  REQUIRE(writer->WriteVerticesNum(table->num_rows()).ok());

  // Check vertex count
  input = fs->OpenInputStream("/tmp/vertex/person/vertex_count").ValueOrDie();
  auto num = input->Read(sizeof(IdType)).ValueOrDie();
  const IdType* ptr = reinterpret_cast<const IdType*>(num->data());
  REQUIRE((*ptr) == table->num_rows());

  // Invalid cases
  // Invalid vertices number
  REQUIRE(writer->WriteVerticesNum(-1).IsInvalid());
  // Out of range
  REQUIRE(writer->WriteChunk(table, 0).IsInvalid());
  // Invalid chunk id
  auto chunk = table->Slice(0, vertex_info->GetChunkSize());
  REQUIRE(writer->WriteChunk(chunk, -1).IsIndexError());
  // Invalid property group
  Property p1("invalid_property", int32(), false);
  auto pg1 = CreatePropertyGroup({p1}, FileType::CSV);
  REQUIRE(writer->WriteTable(table, pg1, 0).IsKeyError());
  // Property not found in table
  std::shared_ptr<arrow::Table> tmp_table =
      table->RenameColumns({"original_id", "firstName", "lastName", "id"})
          .ValueOrDie();
  auto pg2 = vertex_info->GetPropertyGroup("firstName");
  REQUIRE(writer->WriteTable(tmp_table, pg2, 0).IsInvalid());
  // Invalid data type
  auto pg3 = vertex_info->GetPropertyGroup("id");
  REQUIRE(writer->WriteTable(tmp_table, pg3, 0).IsTypeError());

#ifdef ARROW_ORC
  SECTION("TestOrcParquetReader") {
    arrow::Status st;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::string path1 = test_data_dir + "/ldbc_sample/orc" +
                        "/vertex/person/firstName_lastName_gender/chunk1";
    std::string path2 = test_data_dir + "/ldbc_sample/parquet" +
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
#endif

  SECTION("TestEdgeChunkWriter") {
    arrow::Status st;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::string path = test_data_dir +
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
            ->RenameColumns(
                {GeneralParams::kSrcIndexCol, GeneralParams::kDstIndexCol})
            .ValueOrDie();
    std::cout << table->schema()->ToString() << std::endl;
    std::cout << table->num_rows() << ' ' << table->num_columns() << std::endl;

    // Construct the writer
    std::string edge_meta_file =
        test_data_dir + "/ldbc_sample/csv/" + "person_knows_person.edge.yml";
    auto edge_meta = Yaml::LoadFile(edge_meta_file).value();
    auto edge_info = EdgeInfo::Load(edge_meta).value();
    auto adj_list_type = AdjListType::ordered_by_source;
    auto maybe_writer =
        EdgeChunkWriter::Make(edge_info, "/tmp/", adj_list_type);
    REQUIRE(!maybe_writer.has_error());
    auto writer = maybe_writer.value();

    // Get & set validate level
    REQUIRE(writer->GetValidateLevel() == ValidateLevel::no_validate);
    writer->SetValidateLevel(ValidateLevel::strong_validate);
    REQUIRE(writer->GetValidateLevel() == ValidateLevel::strong_validate);

    // Valid cases
    // Write adj list of vertex chunk 0 to files
    REQUIRE(writer->SortAndWriteAdjListTable(table, 0, 0).ok());
    // Write number of edges for vertex chunk 0
    REQUIRE(writer->WriteEdgesNum(0, table->num_rows()).ok());
    // Write number of vertices
    REQUIRE(writer->WriteVerticesNum(903).ok());

    // Check the number of edges
    std::shared_ptr<arrow::io::InputStream> input2 =
        fs->OpenInputStream(
              "/tmp/edge/person_knows_person/ordered_by_source/edge_count0")
            .ValueOrDie();
    auto edge_num = input2->Read(sizeof(IdType)).ValueOrDie();
    const IdType* edge_num_ptr =
        reinterpret_cast<const IdType*>(edge_num->data());
    REQUIRE((*edge_num_ptr) == table->num_rows());

    // Check the number of vertices
    std::shared_ptr<arrow::io::InputStream> input3 =
        fs->OpenInputStream(
              "/tmp/edge/person_knows_person/ordered_by_source/vertex_count")
            .ValueOrDie();
    auto vertex_num = input3->Read(sizeof(IdType)).ValueOrDie();
    const IdType* vertex_num_ptr =
        reinterpret_cast<const IdType*>(vertex_num->data());
    REQUIRE((*vertex_num_ptr) == 903);

    // Invalid cases
    // Invalid count or index
    REQUIRE(writer->WriteEdgesNum(-1, 0).IsIndexError());
    REQUIRE(writer->WriteEdgesNum(0, -1).IsIndexError());
    REQUIRE(writer->WriteVerticesNum(-1).IsIndexError());
    // Out of range
    REQUIRE(writer->WriteOffsetChunk(table, 0).IsInvalid());
    // Invalid chunk id
    REQUIRE(writer->WriteAdjListChunk(table, -1, 0).IsIndexError());
    REQUIRE(writer->WriteAdjListChunk(table, 0, -1).IsIndexError());
    // Invalid adj list type
    auto invalid_adj_list_type = AdjListType::unordered_by_dest;
    auto maybe_writer2 =
        EdgeChunkWriter::Make(edge_info, "/tmp/", invalid_adj_list_type);
    REQUIRE(maybe_writer2.has_error());
    // Invalid property group
    Property p1("invalid_property", int32(), false);
    auto pg1 = CreatePropertyGroup({p1}, FileType::CSV);
    REQUIRE(writer->WritePropertyChunk(table, pg1, 0, 0).IsKeyError());
    // Property not found in table
    auto pg2 = edge_info->GetPropertyGroup("creationDate");
    REQUIRE(writer->WritePropertyChunk(table, pg2, 0, 0).IsInvalid());
    // Required columns not found
    std::shared_ptr<arrow::Table> tmp_table =
        table->RenameColumns({"creationDate", "tmp_property"}).ValueOrDie();
    REQUIRE(writer->WriteAdjListChunk(tmp_table, 0, 0).IsInvalid());
    // Invalid data type
    REQUIRE(writer->WritePropertyChunk(tmp_table, pg2, 0, 0).IsTypeError());
  }
}
}  // namespace graphar
