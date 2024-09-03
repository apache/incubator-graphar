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




// Helper function to split a string by a delimiter
std::vector<std::string> SplitString(const std::string& str, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(str);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}
std::shared_ptr<arrow::Table> readCSV(const std::string& filename, const std::vector<std::string>& labels) {
    // Open the CSV file
    auto input = arrow::io::ReadableFile::Open(filename).ValueOrDie();
    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    // Set the delimiter to '|'
    parse_options.delimiter = '|';

     // Create IOContext with the default memory pool
    arrow::io::IOContext io_context = arrow::io::IOContext(arrow::default_memory_pool());

    // Read the table from the CSV file using the IOContext
    auto reader = arrow::csv::TableReader::Make(io_context, input, read_options, parse_options, convert_options).ValueOrDie();
    auto table = reader->Read().ValueOrDie();

    // Find the :LABEL column index
    auto label_col_idx = table->schema()->GetFieldIndex(":LABEL");
    if (label_col_idx == -1) {
        std::cerr << "No :LABEL column found." << std::endl;
        return nullptr;
    }

    // Access the :LABEL column
    auto label_column = std::static_pointer_cast<arrow::StringArray>(table->column(label_col_idx)->chunk(0));

    // Create a map for labels to column indices
    std::unordered_map<std::string, int> label_to_index;
    for (size_t i = 0; i < labels.size(); ++i) {
        label_to_index[labels[i]] = i;
    }

    // Create a matrix of booleans with dimensions [number of rows, number of labels]
    std::vector<std::vector<bool>> bool_matrix(label_column->length(), std::vector<bool>(labels.size(), false));

    // Populate the matrix based on :LABEL column values
    for (int64_t row = 0; row < label_column->length(); ++row) {
        if (label_column->IsValid(row)) {
            std::string labels_string = label_column->GetString(row);
            auto row_labels = SplitString(labels_string, ';');

            for (const auto& lbl : row_labels) {
                if (label_to_index.find(lbl) != label_to_index.end()) {
                    bool_matrix[row][label_to_index[lbl]] = true;
                }
            }
        }
    }

    // Create Arrow arrays for each label column
    arrow::FieldVector fields;
    arrow::ArrayVector arrays;

    for (const auto& label : labels) {
        arrow::BooleanBuilder builder;
        for (const auto& row : bool_matrix) {
            builder.Append(row[label_to_index[label]]);
        }

        std::shared_ptr<arrow::Array> array;
        builder.Finish(&array);
        fields.push_back(arrow::field(label, arrow::boolean()));
        arrays.push_back(array);
    }

    // Create the Arrow Table with the boolean columns
    auto schema = std::make_shared<arrow::Schema>(fields);
    auto result_table = arrow::Table::Make(schema, arrays);

    return result_table;
}


std::shared_ptr<arrow::Table> read_csv_to_table(const std::string& filename) {
    arrow::csv::ReadOptions read_options{}; 
    arrow::csv::ParseOptions parse_options{}; 
    arrow::csv::ConvertOptions convert_options{};

    parse_options.delimiter = '|'; 

    auto input = arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool()).ValueOrDie();
    
    auto reader = arrow::csv::TableReader::Make(
        arrow::io::default_io_context(),
        input,
        read_options,
        parse_options,
        convert_options).ValueOrDie();

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
  std::string path =
      test_data_dir + "/ldbc/parquet/" + "ldbc.graph.yml";
  auto graph_info = graphar::GraphInfo::Load(path).value();
  auto vertex_info = graph_info->GetVertexInfo("organisation");
  
  // std::vector<std::string> labels = graph_info->GetLabels();
  auto labels = vertex_info->GetLabels();

  std::unordered_map<std::string, size_t> code;

  std::vector<std::vector<bool>> label_column_data;


  // read labels csv file as arrow table
  // auto table = readCSV(test_data_dir + "/ldbc/organisation_0_0.csv", labels);
  auto table = read_csv_to_table(test_data_dir + "/ldbc/organisation_0_0.csv");
  std::string table_message = table->ToString();

  
  auto schema = table->schema();
  std::cout << schema->ToString() << std::endl;
  // std::cout << table_message << std::endl;


  // write arrow table as chunk parquet
  auto maybe_writer = VertexPropertyWriter::Make(vertex_info, "/tmp/");
  REQUIRE(!maybe_writer.has_error());
  auto writer = maybe_writer.value();
  REQUIRE(writer->WriteTable(table, 0).ok());



  
  // IdType start_index = 0;
  // auto maybe_builder =
  //     builder::VerticesBuilder::Make(vertex_info, "/tmp/", start_index);
  // REQUIRE(!maybe_builder.has_error());
  // auto builder = maybe_builder.value();


  // // get & set validate level
  // REQUIRE(builder->GetValidateLevel() == ValidateLevel::no_validate);
  // builder->SetValidateLevel(ValidateLevel::strong_validate);
  // REQUIRE(builder->GetValidateLevel() == ValidateLevel::strong_validate);


  // // clear vertices
  // builder->Clear();
  // REQUIRE(builder->GetNum() == 0);

  // // add vertices
  // std::ifstream fp(test_data_dir + "/ldbc/organisation_0_0.csv");
  // std::string line;
  // getline(fp, line);
  // // erase BOM
  // if (!line.empty() && (unsigned char)line[0] == 0xEF && 
  //                        (unsigned char)line[1] == 0xBB && 
  //                        (unsigned char)line[2] == 0xBF) {
  //       line.erase(0, 3); // 
  //   }
  // int m = 4;
  // std::vector<std::string> names;
  // std::istringstream readstr(line);
  // for (int i = 0; i < m; i++) {
  //   std::string name;
  //   getline(readstr, name, '|');
  //   names.push_back(name);
  //   if(i == 1) continue;
  //   std::cout << "Name: '" << name << "', length: " << name.length() << std::endl;
  // }
  

  // int lines = 0;
  // while (getline(fp, line)) {
  //   lines++;
  //   std::string val;
  //   std::istringstream readstr(line);
  //   builder::Vertex v;
  //   for (int i = 0; i < m; i++) {
  //     getline(readstr, val, '|');
  //     if (i == 1) {
  //       continue; 
  //     }
  //     if (i == 0) {
  //       int64_t x = 0;
  //       for (size_t j = 0; j < val.length(); j++)
  //         x = x * 10 + val[j] - '0';
  //       v.AddProperty(names[i], x);
  //     } else {
  //       v.AddProperty(names[i], val);
  //     }
  //   }
  //   REQUIRE(builder->AddVertex(v).ok());
  // }

  // // check the number of vertices in builder
  // REQUIRE(builder->GetNum() == lines);

  // // dump to files
  // REQUIRE(builder->Dump().ok());

  // add labels



  // can not add new vertices after dumping
  // REQUIRE(builder->AddVertex(v).IsInvalid());

  // check the number of vertices dumped
  // auto fs = arrow::fs::FileSystemFromUriOrPath(test_data_dir).ValueOrDie();
  // auto input =
  //     fs->OpenInputStream("/tmp/vertex/osm_node/vertex_count").ValueOrDie();
  // auto num = input->Read(sizeof(IdType)).ValueOrDie();
  // const IdType* ptr = reinterpret_cast<const IdType*>(num->data());
  // REQUIRE((*ptr) == start_index + builder->GetNum());
}

// TEST_CASE_METHOD(GlobalFixture, "test_edges_builder") {
//   std::cout << "Test edge builder" << std::endl;
//   // construct edge builder
//   std::string edge_meta_file =
//       test_data_dir + "/openstreet/parquet/" + "osm_node_next.edge.yml";
//   auto edge_meta = Yaml::LoadFile(edge_meta_file).value();
//   auto edge_info = EdgeInfo::Load(edge_meta).value();
//   auto vertices_num = 69165;
//   auto maybe_builder = builder::EdgesBuilder::Make(
//       edge_info, "/tmp/", AdjListType::ordered_by_dest, vertices_num);
//   REQUIRE(!maybe_builder.has_error());
//   auto builder = maybe_builder.value();

//   // get & set validate level
//   REQUIRE(builder->GetValidateLevel() == ValidateLevel::no_validate);
//   builder->SetValidateLevel(ValidateLevel::strong_validate);
//   REQUIRE(builder->GetValidateLevel() == ValidateLevel::strong_validate);

//   // // check different validate levels
//   // builder::Edge e(0, 1);
//   // e.AddProperty("creationDate", 2020);
//   // REQUIRE(builder->AddEdge(e, ValidateLevel::no_validate).ok());
//   // REQUIRE(builder->AddEdge(e, ValidateLevel::weak_validate).ok());
//   // REQUIRE(builder->AddEdge(e, ValidateLevel::strong_validate).IsTypeError());
//   // e.AddProperty("invalid_name", "invalid_value");
//   // REQUIRE(builder->AddEdge(e).IsKeyError());

//   // // clear edges
//   // builder->Clear();
//   // REQUIRE(builder->GetNum() == 0);

//   // add edges
//   std::ifstream fp(test_data_dir + "/openstreet/openstreet_next.csv");
//   std::string line;
//   getline(fp, line);
//   std::vector<std::string> names;
//   std::istringstream readstr(line);
//   std::map<std::string, int64_t> mapping;
//   int64_t cnt = 0, lines = 0;

//   while (getline(fp, line)) {
//     lines++;
//     std::string val;
//     std::istringstream readstr(line);
//     int64_t s = 0, d = 0;
//     for (int i = 0; i < 3; i++) {
//       getline(readstr, val, ',');
//       if (i == 0) {
//         if (mapping.find(val) == mapping.end())
//           mapping[val] = cnt++;
//         s = mapping[val];
//       } else if (i == 1) {
//         if (mapping.find(val) == mapping.end())
//           mapping[val] = cnt++;
//         d = mapping[val];
//       } else {
//         builder::Edge e(s, d);
//         e.AddProperty("distance", val);
//         REQUIRE(builder->AddEdge(e).ok());
//       }
//     }
//   }

//   // check the number of edges in builder
//   REQUIRE(builder->GetNum() == lines);

//   // dump to files
//   REQUIRE(builder->Dump().ok());

//   // can not add new edges after dumping
//   // REQUIRE(builder->AddEdge(e).IsInvalid());

//   // check the number of vertices dumped
//   auto fs = arrow::fs::FileSystemFromUriOrPath(test_data_dir).ValueOrDie();
//   auto input =
//       fs->OpenInputStream(
//             "/tmp/edge/osm_node_next/ordered_by_dest/vertex_count")
//           .ValueOrDie();
//   auto num = input->Read(sizeof(IdType)).ValueOrDie();
//   const IdType* ptr = reinterpret_cast<const IdType*>(num->data());
//   REQUIRE((*ptr) == vertices_num);
// }
}  // namespace graphar
