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

#include <parquet/types.h>
#include <fstream>
#include <iostream>
#include <ostream>
#include <sstream>
#include <string>

#include "arrow/api.h"
#include "graphar/label.h"
#include "graphar/util.h"
#include "graphar/writer_util.h"
#ifdef ARROW_ORC
#include "arrow/adapters/orc/adapter.h"
#endif
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
  std::string vertex_meta_file_parquet =
      test_data_dir + "/ldbc_sample/parquet/" + "person.vertex.yml";
  auto vertex_meta_parquet = Yaml::LoadFile(vertex_meta_file_parquet).value();
  auto vertex_info_parquet = VertexInfo::Load(vertex_meta_parquet).value();
  auto maybe_writer = VertexPropertyWriter::Make(vertex_info_parquet, "/tmp/");
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
  auto chunk = table->Slice(0, vertex_info_parquet->GetChunkSize());
  REQUIRE(writer->WriteChunk(chunk, -1).IsIndexError());
  // Invalid property group
  Property p1("invalid_property", int32(), false);
  auto pg1 = CreatePropertyGroup({p1}, FileType::CSV);
  REQUIRE(writer->WriteTable(table, pg1, 0).IsKeyError());
  // Property not found in table
  std::shared_ptr<arrow::Table> tmp_table =
      table->RenameColumns({"original_id", "firstName", "lastName", "id"})
          .ValueOrDie();
  auto pg2 = vertex_info_parquet->GetPropertyGroup("firstName");
  REQUIRE(writer->WriteTable(tmp_table, pg2, 0).IsInvalid());
  // Invalid data type
  auto pg3 = vertex_info_parquet->GetPropertyGroup("id");
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
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    st = graphar::util::OpenParquetArrowReader(path2, pool, &arrow_reader);

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
  SECTION("TestVertexPropertyWriterWithOption") {
    // csv file
    // Construct the writer
    std::string vertex_meta_file_csv =
        test_data_dir + "/ldbc_sample/csv/" + "person.vertex.yml";
    auto vertex_meta_csv = Yaml::LoadFile(vertex_meta_file_csv).value();
    auto vertex_info_csv = VertexInfo::Load(vertex_meta_csv).value();
    auto csv_options = WriterOptions::CSVOptionBuilder();
    auto wopt = csv_options.build();
    csv_options.include_header(true);
    csv_options.delimiter('|');
    auto maybe_writer =
        VertexPropertyWriter::Make(vertex_info_csv, "/tmp/option/", wopt);
    REQUIRE(!maybe_writer.has_error());
    auto writer = maybe_writer.value();
    REQUIRE(writer->WriteTable(table, 0).ok());
    // read csv file
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    parse_options.delimiter = '|';
    std::shared_ptr<arrow::io::InputStream> chunk0_input =
        fs->OpenInputStream(
              "/tmp/option/vertex/person/firstName_lastName_gender/chunk0")
            .ValueOrDie();
    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto csv_reader =
        arrow::csv::TableReader::Make(arrow::io::default_io_context(),
                                      chunk0_input, read_options, parse_options,
                                      arrow::csv::ConvertOptions::Defaults())
            .ValueOrDie();
    auto maybe_table = csv_reader->Read();
    REQUIRE(maybe_table.ok());
    std::shared_ptr<arrow::Table> csv_table = *maybe_table;
    REQUIRE(csv_table->num_rows() == vertex_info_csv->GetChunkSize());
    REQUIRE(csv_table->num_columns() ==
            static_cast<int>(vertex_info_csv->GetPropertyGroup("firstName")
                                 ->GetProperties()
                                 .size()) +
                1);
    // type parquet
    auto options_parquet_Builder = WriterOptions::ParquetOptionBuilder(wopt);
    options_parquet_Builder.compression(arrow::Compression::type::UNCOMPRESSED);
    options_parquet_Builder.enable_statistics(false);
    parquet::SortingColumn sc;
    sc.column_idx = 1;
    std::vector<::parquet::SortingColumn> columns = {sc};
    options_parquet_Builder.sorting_columns(columns)
        .enable_store_decimal_as_integer(true)
        .max_row_group_length(10);
    wopt = options_parquet_Builder.build();
    maybe_writer =
        VertexPropertyWriter::Make(vertex_info_parquet, "/tmp/option/", wopt);
    REQUIRE(!maybe_writer.has_error());
    writer = maybe_writer.value();
    REQUIRE(writer->WriteTable(table, 0).ok());
    // read parquet file
    std::string parquet_file =
        "/tmp/option/vertex/person/firstName_lastName_gender/chunk0";
    std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
    auto st = graphar::util::OpenParquetArrowReader(
        parquet_file, arrow::default_memory_pool(), &parquet_reader);
    REQUIRE(st.ok());
    std::shared_ptr<arrow::Table> parquet_table;
    st = parquet_reader->ReadTable(&parquet_table);
    REQUIRE(st.ok());
    auto parquet_metadata = parquet_reader->parquet_reader()->metadata();
    auto row_group_meta = parquet_metadata->RowGroup(0);
    auto col_meta = row_group_meta->ColumnChunk(0);
    REQUIRE(row_group_meta->sorting_columns().size() == 1);
    REQUIRE(row_group_meta->sorting_columns()[0].column_idx == 1);
    REQUIRE(col_meta->compression() == parquet::Compression::UNCOMPRESSED);
    REQUIRE(!col_meta->statistics());
    REQUIRE(parquet_table->num_rows() == vertex_info_parquet->GetChunkSize());
    REQUIRE(parquet_metadata->num_row_groups() ==
            parquet_table->num_rows() / 10);
    REQUIRE(parquet_table->num_columns() ==
            static_cast<int>(vertex_info_parquet->GetPropertyGroup("firstName")
                                 ->GetProperties()
                                 .size() +
                             1));
#ifdef ARROW_ORC
    std::string vertex_meta_file_orc =
        test_data_dir + "/ldbc_sample/orc/" + "person.vertex.yml";
    auto vertex_meta_orc = Yaml::LoadFile(vertex_meta_file_orc).value();
    auto vertex_info_orc = VertexInfo::Load(vertex_meta_orc).value();
    auto optionsOrcBuilder = WriterOptions::ORCOptionBuilder(wopt);
    optionsOrcBuilder.compression(arrow::Compression::type::ZSTD);
    wopt = optionsOrcBuilder.build();
    maybe_writer =
        VertexPropertyWriter::Make(vertex_info_orc, "/tmp/option/", wopt);
    REQUIRE(!maybe_writer.has_error());
    writer = maybe_writer.value();
    REQUIRE(writer->WriteTable(table, 0).ok());
    auto fs1 = arrow::fs::FileSystemFromUriOrPath(
                   "/tmp/option/vertex/person/firstName_lastName_gender/chunk0")
                   .ValueOrDie();
    std::shared_ptr<arrow::io::RandomAccessFile> input1 =
        fs1->OpenInputFile(
               "/tmp/option/vertex/person/firstName_lastName_gender/chunk0")
            .ValueOrDie();
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::unique_ptr<arrow::adapters::orc::ORCFileReader> reader =
        arrow::adapters::orc::ORCFileReader::Open(input1, pool).ValueOrDie();
    // Read entire file as a single Arrow table
    maybe_table = reader->Read();
    std::shared_ptr<arrow::Table> table1 = maybe_table.ValueOrDie();
    REQUIRE(reader->GetCompression() == parquet::Compression::ZSTD);
    REQUIRE(table1->num_rows() == vertex_info_parquet->GetChunkSize());
    REQUIRE(table1->num_columns() ==
            static_cast<int>(vertex_info_parquet->GetPropertyGroup("firstName")
                                 ->GetProperties()
                                 .size()) +
                1);
#endif
  }
}
TEST_CASE_METHOD(GlobalFixture, "TestEdgeChunkWriter") {
  arrow::Status st;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  std::string path = test_data_dir +
                     "/ldbc_sample/parquet/edge/person_knows_person/"
                     "unordered_by_source/adj_list/part0/chunk0";
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  st = graphar::util::OpenParquetArrowReader(path, pool, &arrow_reader);
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
  std::string edge_meta_file_csv =
      test_data_dir + "/ldbc_sample/csv/" + "person_knows_person.edge.yml";
  auto edge_meta_csv = Yaml::LoadFile(edge_meta_file_csv).value();
  auto edge_info_csv = EdgeInfo::Load(edge_meta_csv).value();
  auto adj_list_type = AdjListType::ordered_by_source;

  SECTION("TestEdgeChunkWriterWithoutOption") {
    auto maybe_writer =
        EdgeChunkWriter::Make(edge_info_csv, "/tmp/", adj_list_type);
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

    auto fs = arrow::fs::FileSystemFromUriOrPath("/tmp/edge/").ValueOrDie();
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
        EdgeChunkWriter::Make(edge_info_csv, "/tmp/", invalid_adj_list_type);
    REQUIRE(maybe_writer2.has_error());
    // Invalid property group
    Property p1("invalid_property", int32(), false);
    auto pg1 = CreatePropertyGroup({p1}, FileType::CSV);
    REQUIRE(writer->WritePropertyChunk(table, pg1, 0, 0).IsKeyError());
    // Property not found in table
    auto pg2 = edge_info_csv->GetPropertyGroup("creationDate");
    REQUIRE(writer->WritePropertyChunk(table, pg2, 0, 0).IsInvalid());
    // Required columns not found
    std::shared_ptr<arrow::Table> tmp_table =
        table->RenameColumns({"creationDate", "tmp_property"}).ValueOrDie();
    REQUIRE(writer->WriteAdjListChunk(tmp_table, 0, 0).IsInvalid());
    // Invalid data type
    REQUIRE(writer->WritePropertyChunk(tmp_table, pg2, 0, 0).IsTypeError());
  }
  SECTION("TestEdgeChunkWriterWithOption") {
    WriterOptions::CSVOptionBuilder csv_options_builder;
    csv_options_builder.include_header(true).delimiter('|');
    auto maybe_writer =
        EdgeChunkWriter::Make(edge_info_csv, "/tmp/option/", adj_list_type,
                              csv_options_builder.build());
    REQUIRE(!maybe_writer.has_error());
    auto writer = maybe_writer.value();

    // Valid: Write adj list with options
    REQUIRE(writer->SortAndWriteAdjListTable(table, 0, 0).ok());
    // Valid: Write edge count
    REQUIRE(writer->WriteEdgesNum(0, table->num_rows()).ok());
    // Valid: Write vertex count
    REQUIRE(writer->WriteVerticesNum(903).ok());

    // Read back CSV file and check delimiter/header
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    parse_options.delimiter = '|';
    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto fs = arrow::fs::FileSystemFromUriOrPath("/tmp/option/").ValueOrDie();
    std::shared_ptr<arrow::io::InputStream> chunk0_input =
        fs->OpenInputStream(
              "/tmp/option/edge/person_knows_person/ordered_by_source/adj_list/"
              "part0/chunk0")
            .ValueOrDie();
    auto csv_reader =
        arrow::csv::TableReader::Make(arrow::io::default_io_context(),
                                      chunk0_input, read_options, parse_options,
                                      arrow::csv::ConvertOptions::Defaults())
            .ValueOrDie();
    auto maybe_table2 = csv_reader->Read();
    REQUIRE(maybe_table2.ok());
    std::shared_ptr<arrow::Table> csv_table = *maybe_table2;
    REQUIRE(csv_table->num_rows() ==
            std::min(edge_info_csv->GetChunkSize(), table->num_rows()));
    REQUIRE(csv_table->num_columns() == table->num_columns());

    // Parquet option
    std::string edge_meta_file_parquet = test_data_dir +
                                         "/ldbc_sample/parquet/" +
                                         "person_knows_person.edge.yml";
    auto edge_meta_parquet = Yaml::LoadFile(edge_meta_file_parquet).value();
    auto edge_info_parquet = EdgeInfo::Load(edge_meta_parquet).value();
    auto optionsBuilderParquet = WriterOptions::ParquetOptionBuilder();
    optionsBuilderParquet.compression(arrow::Compression::type::UNCOMPRESSED);
    optionsBuilderParquet.enable_statistics(false);
    optionsBuilderParquet.enable_store_decimal_as_integer(true);
    optionsBuilderParquet.max_row_group_length(10);
    auto maybe_parquet_writer =
        EdgeChunkWriter::Make(edge_info_parquet, "/tmp/option/", adj_list_type,
                              optionsBuilderParquet.build());
    REQUIRE(!maybe_parquet_writer.has_error());
    auto parquet_writer = maybe_parquet_writer.value();
    REQUIRE(parquet_writer->SortAndWriteAdjListTable(table, 0, 0).ok());
    std::string parquet_file =
        "/tmp/option/edge/person_knows_person/ordered_by_source/adj_list/part0/"
        "chunk0";
    std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
    auto st = graphar::util::OpenParquetArrowReader(
        parquet_file, arrow::default_memory_pool(), &parquet_reader);
    REQUIRE(st.ok());
    std::shared_ptr<arrow::Table> parquet_table;
    st = parquet_reader->ReadTable(&parquet_table);
    REQUIRE(st.ok());
    auto parquet_metadata = parquet_reader->parquet_reader()->metadata();
    auto row_group_meta = parquet_metadata->RowGroup(0);
    auto col_meta = row_group_meta->ColumnChunk(0);
    REQUIRE(col_meta->compression() == parquet::Compression::UNCOMPRESSED);
    REQUIRE(!col_meta->statistics());
    REQUIRE(parquet_table->num_rows() ==
            std::min(table->num_rows(), edge_info_parquet->GetChunkSize()));
    REQUIRE(parquet_metadata->num_row_groups() ==
            parquet_table->num_rows() / 10 + 1);
    REQUIRE(parquet_table->num_columns() ==
            static_cast<int>(table->num_columns()));

#ifdef ARROW_ORC
    // ORC option
    std::string edge_meta_file_orc =
        test_data_dir + "/ldbc_sample/orc/" + "person_knows_person.edge.yml";
    auto edge_meta_orc = Yaml::LoadFile(edge_meta_file_orc).value();
    auto edge_info_orc = EdgeInfo::Load(edge_meta_orc).value();
    auto optionsBuilderOrc = WriterOptions::ORCOptionBuilder();
    optionsBuilderOrc.compression(arrow::Compression::type::ZSTD);
    auto maybe_orc_writer =
        EdgeChunkWriter::Make(edge_info_orc, "/tmp/option/", adj_list_type,
                              optionsBuilderOrc.build());
    REQUIRE(!maybe_orc_writer.has_error());
    auto orc_writer = maybe_orc_writer.value();
    REQUIRE(orc_writer->SortAndWriteAdjListTable(table, 0, 0).ok());
    auto orc_fs = arrow::fs::FileSystemFromUriOrPath(
                      "/tmp/option/edge/person_knows_person/ordered_by_source/"
                      "adj_list/part0/chunk0")
                      .ValueOrDie();
    std::shared_ptr<arrow::io::RandomAccessFile> orc_input =
        orc_fs
            ->OpenInputFile(
                "/tmp/option/edge/person_knows_person/ordered_by_source/"
                "adj_list/part0/chunk0")
            .ValueOrDie();
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::unique_ptr<arrow::adapters::orc::ORCFileReader> orc_reader =
        arrow::adapters::orc::ORCFileReader::Open(orc_input, pool).ValueOrDie();
    auto maybe_orc_table = orc_reader->Read();
    REQUIRE(maybe_orc_table.ok());
    std::shared_ptr<arrow::Table> orc_table = *maybe_orc_table;
    REQUIRE(orc_reader->GetCompression() == parquet::Compression::ZSTD);
    REQUIRE(orc_table->num_rows() == table->num_rows());
    REQUIRE(orc_table->num_columns() == table->num_columns());
#endif
  }
}
}  // namespace graphar
