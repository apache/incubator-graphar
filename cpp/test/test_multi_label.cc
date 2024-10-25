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
#include <unordered_map>
#include <vector>

#include "arrow/api.h"
#include "arrow/csv/api.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/api.h"
#include "arrow/stl.h"
#include "arrow/util/uri.h"
#include "parquet/arrow/writer.h"

#include "./util.h"
#include "graphar/api/high_level_writer.h"

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
TEST_CASE_METHOD(GlobalFixture, "test_multi_label_builder") {
  std::cout << "Test multi label builder" << std::endl;

  // construct graph information from file
  // std::string path =
  //     test_data_dir + "/icij/parquet/" + "icij-offshoreleaks.graph.yml";
  std::string path = test_data_dir + "/ldbc/parquet/" + "ldbc.graph.yml";
  auto graph_info = graphar::GraphInfo::Load(path).value();
  auto vertex_info = graph_info->GetVertexInfo("organisation");

  // std::vector<std::string> labels = graph_info->GetLabels();
  auto labels = vertex_info->GetLabels();

  std::unordered_map<std::string, size_t> code;

  std::vector<std::vector<bool>> label_column_data;

  // read labels csv file as arrow table
  auto table =
      read_csv_to_table(test_data_dir + "/ldbc/modified_organisation_0_0.csv");
  //   auto table = read_csv_to_table(test_data_dir +
  //   "/ldbc_large/modified_organisation_0_0.csv");
  std::string table_message = table->ToString();

  auto schema = table->schema();
  std::cout << schema->ToString() << std::endl;
  // std::cout << table_message << std::endl;

  // write arrow table as chunk parquet
  auto maybe_writer = VertexPropertyWriter::Make(
      vertex_info,
      "/workspaces/incubator-graphar/cpp/build/testing/ldbc/parquet/");
  REQUIRE(!maybe_writer.has_error());
  auto writer = maybe_writer.value();
  REQUIRE(writer->WriteTable(table, 0).ok());
  REQUIRE(writer->WriteVerticesNum(table->num_rows()).ok());
}
}  // namespace graphar
