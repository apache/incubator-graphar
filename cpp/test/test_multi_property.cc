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

#include <arrow/compute/api.h>
#include <cstddef>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include "arrow/api.h"
#include "arrow/filesystem/api.h"
#include "graphar/arrow/chunk_reader.h"
#include "graphar/arrow/chunk_writer.h"
#include "graphar/fwd.h"
#include "graphar/graph_info.h"
#include "graphar/types.h"
#include "parquet/arrow/writer.h"

#include "graphar/api/high_level_writer.h"
#include "graphar/writer_util.h"

#include "./util.h"

#include <catch2/catch_test_macros.hpp>

std::shared_ptr<arrow::Table> read_csv_to_table(const std::string& filename) {
  arrow::csv::ReadOptions read_options{};
  arrow::csv::ParseOptions parse_options{};
  arrow::csv::ConvertOptions convert_options{};

  parse_options.delimiter = '|';

  auto input =
      arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool())
          .ValueOrDie();

  auto reader = arrow::csv::TableReader::Make(arrow::io::default_io_context(),
                                              input, read_options,
                                              parse_options, convert_options)
                    .ValueOrDie();

  std::shared_ptr<arrow::Table> table;
  table = reader->Read().ValueOrDie();

  return table;
}

namespace graphar {
TEST_CASE_METHOD(GlobalFixture, "read multi-properties from csv file") {
  // read labels csv file as arrow table
  auto person_table = read_csv_to_table(test_data_dir + "/ldbc/person_0_0.csv");
  auto seed = static_cast<unsigned int>(time(NULL));
  int expected_row = rand_r(&seed) % person_table->num_rows();
  auto person_schema = person_table->schema();
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  auto value_builder = std::make_shared<arrow::StringBuilder>();
  arrow::ListBuilder builder(pool, value_builder);
  auto email_col_idx = person_table->schema()->GetFieldIndex("emails");
  std::string expected_emails =
      std::static_pointer_cast<arrow::StringArray>(
          person_table->column(email_col_idx)->chunk(0))
          ->GetString(expected_row);
  for (int64_t chunk_idx = 0;
       chunk_idx < person_table->column(email_col_idx)->num_chunks();
       ++chunk_idx) {
    auto chunk = person_table->column(email_col_idx)->chunk(chunk_idx);
    auto email_column = std::static_pointer_cast<arrow::StringArray>(chunk);
    for (int64_t row = 0; row < email_column->length(); ++row) {
      auto result = builder.Append();
      REQUIRE(result.ok());
      if (email_column->IsValid(row)) {
        std::string emails_string = email_column->GetString(row);
        auto row_emails = SplitString(emails_string, ';');
        for (const auto& email : row_emails) {
          REQUIRE(value_builder->Append(email).ok());
        }
      }
    }
  }
  std::shared_ptr<arrow::Array> array;
  builder.Finish(&array);
  auto person_emails_chunked_array = std::make_shared<arrow::ChunkedArray>(
      std::vector<std::shared_ptr<arrow::Array>>{array});
  int emailFieldIndex = person_schema->GetFieldIndex("emails");
  person_table = person_table->RemoveColumn(emailFieldIndex).ValueOrDie();
  person_schema = person_schema->RemoveField(emailFieldIndex).ValueOrDie();
  person_schema =
      person_schema
          ->AddField(person_schema->num_fields(),
                     arrow::field("emails", arrow::list(arrow::utf8())))
          .ValueOrDie();
  person_table = person_table
                     ->AddColumn(person_table->num_columns(),
                                 person_schema->fields().back(),
                                 person_emails_chunked_array)
                     .ValueOrDie();
  auto index = person_schema->GetFieldIndex("emails");
  auto emails_col = person_table->column(index)->chunk(0);
  auto result = std::static_pointer_cast<arrow::ListArray>(
      emails_col->View(arrow::list(arrow::utf8())).ValueOrDie());
  auto values = std::static_pointer_cast<arrow::StringArray>(result->values());
  int64_t start = result->value_offset(expected_row);
  int64_t end = result->value_offset(expected_row + 1);
  std::string emails = "";
  for (int64_t i = start; i < end; ++i) {
    emails += values->GetString(i);
    if (i < end - 1)
      emails += ";";
  }
  std::cout << "random row: " << expected_row << std::endl;
  REQUIRE(expected_emails == emails);
  // write to parquet file
  std::string path = test_data_dir + "/ldbc/parquet/" + "ldbc.graph.yml";
  auto graph_info = graphar::GraphInfo::Load(path).value();
  auto vertex_info = graph_info->GetVertexInfo("person");
  auto maybe_writer =
      VertexPropertyWriter::Make(vertex_info, "/tmp/ldbc/parquet/");
  REQUIRE(!maybe_writer.has_error());
  auto writer = maybe_writer.value();
  REQUIRE(writer->WriteTable(person_table, 0).ok());
  REQUIRE(writer->WriteVerticesNum(person_table->num_rows()).ok());

  auto maybe_reader = VertexPropertyArrowChunkReader::Make(
      vertex_info, vertex_info->GetPropertyGroup("emails"),
      "/tmp/ldbc/parquet/");
  REQUIRE(maybe_reader.status().ok());
  auto reader = maybe_reader.value();
  REQUIRE(reader->seek(expected_row).ok());
  auto table_result = reader->GetChunk();
  REQUIRE(table_result.status().ok());
  auto table = table_result.value();
  index = table->schema()->GetFieldIndex("emails");
  emails_col = table->column(index)->chunk(0);
  result = std::static_pointer_cast<arrow::ListArray>(
      emails_col->View(arrow::list(arrow::large_utf8())).ValueOrDie());
  auto email_result =
      std::static_pointer_cast<arrow::LargeStringArray>(result->value_slice(0));
  emails = "";
  end = email_result->length();
  for (int64_t i = 0; i < end; ++i) {
    emails += email_result->GetString(i);
    if (i < end - 1)
      emails += ";";
  }
  std::cout << emails << std::endl;
  REQUIRE(expected_emails == emails);
}
TEST_CASE_METHOD(GlobalFixture, "TestMultiProperty high level builder") {
  int vertex_count = 3;
  std::vector<std::string> property_names = {"id", "emails"};
  std::vector<int64_t> id = {0, 1, 2};
  std::vector<std::vector<std::string>> emails = {
      {"john@example.com", "john.work@example.com"},
      {"jane@example.com"},
      {"alice@example.com", "alice123@example.com",
       "a.wonderland@example.com"}};
  std::string vertex_meta_file =
      test_data_dir + "/ldbc/parquet/" + "person.vertex.yml";
  auto vertex_meta = graphar::Yaml::LoadFile(vertex_meta_file).value();
  auto vertex_info = graphar::VertexInfo::Load(vertex_meta).value();
  graphar::IdType start_index = 0;
  SECTION("add sample values to set property") {
    graphar::builder::VerticesBuilder builder(
        vertex_info, "/tmp/", start_index, nullptr,
        graphar::ValidateLevel::strong_validate);
    // prepare vertex data
    int vertex_count = 1;
    std::vector<std::vector<std::string>> emails = {
        {"john@example.com", "john@example.com"}};
    // add vertices
    for (int i = 0; i < vertex_count; i++) {
      graphar::builder::Vertex v;
      for (const auto& email : emails[i]) {
        v.AddProperty(graphar::Cardinality::SET, "emails",
                      email);  // Multi-property
      }
      REQUIRE(builder.AddVertex(v).IsKeyError());
    }
  }
  SECTION("test add single values to set property") {
    graphar::builder::VerticesBuilder builder(
        vertex_info, "/tmp/", start_index, nullptr,
        graphar::ValidateLevel::strong_validate);
    // prepare vertex data
    int vertex_count = 3;
    std::vector<std::string> emails = {"john@example.com", "jane@example.com",
                                       "alice@example.com"};
    for (int i = 0; i < vertex_count; i++) {
      graphar::builder::Vertex v;
      v.AddProperty("emails", emails[i]);
      REQUIRE(builder.AddVertex(v).ok());
    }
  }
  SECTION("test add multi values to single property") {
    auto single_email =
        CreatePropertyGroup({Property("single_email", string(), false)},
                            FileType::PARQUET, "single_email/");
    auto test_vertex_info = vertex_info->AddPropertyGroup(single_email).value();
    graphar::builder::VerticesBuilder builder(
        test_vertex_info, "/tmp/", start_index, nullptr,
        graphar::ValidateLevel::strong_validate);
    for (int i = 0; i < vertex_count; i++) {
      graphar::builder::Vertex v;
      v.AddProperty(graphar::Cardinality::LIST, "single_email", emails[i]);
      REQUIRE(builder.AddVertex(v).IsTypeError());
    }
  }
  SECTION("test add multi values to set property") {
    auto set_email = CreatePropertyGroup(
        {Property("set_email", string(), false, true, Cardinality::SET)},
        FileType::PARQUET, "set_email/");
    auto test_vertex_info = vertex_info->AddPropertyGroup(set_email).value();
    graphar::builder::VerticesBuilder builder(
        test_vertex_info, "/tmp/", start_index, nullptr,
        graphar::ValidateLevel::strong_validate);
    int vertex_count = 1;
    std::vector<std::vector<std::string>> emails = {
        {"john@example.com", "john@example.com"}};
    // add vertices
    for (int i = 0; i < vertex_count; i++) {
      graphar::builder::Vertex v;
      for (const auto& email : emails[i]) {
        v.AddProperty(graphar::Cardinality::LIST, "set_email",
                      email);  // Multi-property
      }
      REQUIRE(builder.AddVertex(v).IsKeyError());
    }
  }
  SECTION("test write to file") {
    graphar::builder::VerticesBuilder builder(
        vertex_info, "/tmp/", start_index, nullptr,
        graphar::ValidateLevel::strong_validate);
    // prepare vertex data
    // add vertices
    for (int i = 0; i < vertex_count; i++) {
      graphar::builder::Vertex v;
      v.AddProperty(property_names[0], id[i]);
      for (const auto& email : emails[i]) {
        v.AddProperty(graphar::Cardinality::SET, property_names[1], email);
      }
      REQUIRE(builder.AddVertex(v).ok());
    }
    auto st = builder.Dump();
    std::cout << st.message() << std::endl;
    REQUIRE(st.ok());
  }
  SECTION("test read from file") {
    // read from file
    auto maybe_reader = graphar::VertexPropertyArrowChunkReader::Make(
        vertex_info, vertex_info->GetPropertyGroup(property_names[1]), "/tmp/");
    REQUIRE(maybe_reader.status().ok());
    auto reader = maybe_reader.value();
    REQUIRE(reader->seek(0).ok());
    auto table_result = reader->GetChunk();
    REQUIRE(table_result.status().ok());
    auto table = table_result.value();
    auto index = table->schema()->GetFieldIndex(property_names[1]);
    auto emails_col = table->column(index)->chunk(0);
    auto result = std::static_pointer_cast<arrow::ListArray>(
        emails_col->View(arrow::list(arrow::large_utf8())).ValueOrDie());
    REQUIRE(result->length() == 3);
    for (int i = 0; i < result->length(); i++) {
      auto email_result = std::static_pointer_cast<arrow::LargeStringArray>(
          result->value_slice(i));
      for (int j = 0; j < email_result->length(); j++) {
        REQUIRE(emails[i][j] == email_result->GetString(j));
      }
    }
  }
}
}  // namespace graphar
