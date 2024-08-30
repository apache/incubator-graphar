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
#include "parquet/arrow/writer.h"

#include "./util.h"
#include "graphar/api/high_level_writer.h"

#include <catch2/catch_test_macros.hpp>

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <iostream>


// 函数用于将int64类型的列转换为bool类型
std::shared_ptr<arrow::Table> ConvertInt64ToBool(const std::shared_ptr<arrow::Table>& table) {
    auto schema = table->schema();
    std::vector<std::shared_ptr<arrow::Field>> new_fields;
    std::vector<std::shared_ptr<arrow::ChunkedArray>> new_columns;

    for (int i = 0; i < schema->num_fields(); ++i) {
        auto field = schema->field(i);
        auto array = table->column(i);

        if (field->type()->id() == arrow::Type::INT64) {
            // 将int64数组转换为bool数组
            auto cast_options = arrow::compute::CastOptions::Safe(arrow::boolean());
            auto maybe_cast_array = arrow::compute::CallFunction("cast", {array}, &cast_options);
            if (!maybe_cast_array.ok()) {
                throw std::runtime_error("Failed to cast array to boolean: " + maybe_cast_array.status().ToString());
            }
            auto cast_array = maybe_cast_array->chunked_array();
            new_columns.push_back(cast_array);
        } else {
            // 其他类型列直接添加
            new_columns.push_back(array);
        }

        // 更新字段类型（如果是int64转为bool）
        if (field->type()->id() == arrow::Type::INT64) {
            new_fields.push_back(arrow::field(field->name(), arrow::boolean()));
        } else {
            new_fields.push_back(field);
        }
    }

    auto new_schema = arrow::schema(new_fields);
    return arrow::Table::Make(new_schema, new_columns);
}

std::shared_ptr<arrow::Table> ConvertInt64ToUInt8(const std::shared_ptr<arrow::Table>& table) {
    auto schema = table->schema();
    std::vector<std::shared_ptr<arrow::Field>> new_fields;
    std::vector<std::shared_ptr<arrow::ChunkedArray>> new_columns;

    for (int i = 0; i < schema->num_fields(); ++i) {
        auto field = schema->field(i);
        auto array = table->column(i);

        if (field->type()->id() == arrow::Type::INT64) {
            // 将int64数组转换为int8数组
            auto cast_options = arrow::compute::CastOptions::Safe(arrow::uint8());
            auto maybe_cast_array = arrow::compute::CallFunction("cast", {array}, &cast_options);
            if (!maybe_cast_array.ok()) {
                throw std::runtime_error("Failed to cast array to boolean: " + maybe_cast_array.status().ToString());
            }
            auto cast_array = maybe_cast_array->chunked_array();
            new_columns.push_back(cast_array);
        } else {
            // 其他类型列直接添加
            new_columns.push_back(array);
        }

        // 更新字段类型（如果是int64转为int8）
        if (field->type()->id() == arrow::Type::INT64) {
            new_fields.push_back(arrow::field(field->name(), arrow::uint8()));
        } else {
            new_fields.push_back(field);
        }
    }

    auto new_schema = arrow::schema(new_fields);
    return arrow::Table::Make(new_schema, new_columns);
}
std::shared_ptr<arrow::Table> read_csv_to_table(const std::string& filename) {
    arrow::csv::ReadOptions read_options{}; // 默认使用多线程
    arrow::csv::ParseOptions parse_options{}; // 默认分隔符为逗号
    arrow::csv::ConvertOptions convert_options{};

    parse_options.delimiter = ' '; //分隔符为空格

    // 使用with_resource方法来自动管理内存，避免手动管理shared_ptr
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
  std::string path =
      test_data_dir + "/icij/parquet/" + "icij-offshoreleaks.graph.yml";
  auto graph_info = graphar::GraphInfo::Load(path).value();
  auto vertex_info = graph_info->GetVertexInfo("icij_node");
  std::vector<std::string> labels = graph_info->GetLabels();

  std::unordered_map<std::string, size_t> code;

  std::vector<std::vector<bool>> label_column_data;
  // std::string message = graph_info->Dump().value();
  // std::cout << message << std::endl;


  // read labels csv file as arrow table
  auto table = read_csv_to_table(test_data_dir + "/icij/icij-offshoreleaks-44-nodes_labels.csv");
  std::string table_message = table->ToString();

  // transfer to boolean
  table = ConvertInt64ToBool(table);

  // transfer to int8
//   table = ConvertInt64ToUInt8(table);
  
  auto schema = table->schema();
  std::cout << schema->ToString() << std::endl;
  // std::cout << table_message << std::endl;

  // write arrow table as parquet
  // fs->WriteTableToFile(table, FileType::PARQUET, "/tmp/vertex/osm_node/labels");

  // write arrow table as chunk parquet
  auto maybe_writer = VertexPropertyWriter::Make(vertex_info, "/tmp/");
  REQUIRE(!maybe_writer.has_error());
  auto writer = maybe_writer.value();
  REQUIRE(writer->WriteLabelTable(table, 0, FileType::PARQUET).ok());



  
  // construct vertex builder
  // std::string vertex_meta_file =
  //     test_data_dir + "/openstreet/parquet/" + "osm_node.vertex.yml";
  // auto vertex_meta = Yaml::LoadFile(vertex_meta_file).value();
  // auto vertex_info = VertexInfo::Load(vertex_meta).value();
  IdType start_index = 0;
  auto maybe_builder =
      builder::VerticesBuilder::Make(vertex_info, "/tmp/", start_index);
  REQUIRE(!maybe_builder.has_error());
  auto builder = maybe_builder.value();

  // std::string message = vertex_info->Dump().value();
  // std::cout << message << std::endl;
  // std::cout << "Has id: " << vertex_info->HasProperty("id") << std::endl;
  // std::cout << "Has lon: " << vertex_info->HasProperty("lon") << std::endl;
  // std::cout << "Has lat: " << vertex_info->HasProperty("lat") << std::endl;

  // get & set validate level
  REQUIRE(builder->GetValidateLevel() == ValidateLevel::no_validate);
  builder->SetValidateLevel(ValidateLevel::strong_validate);
  REQUIRE(builder->GetValidateLevel() == ValidateLevel::strong_validate);


  // clear vertices
  builder->Clear();
  REQUIRE(builder->GetNum() == 0);

  // add vertices
  std::ifstream fp(test_data_dir + "/icij/icij-offshoreleaks-44-nodes_no_label.csv");
  std::string line;
  getline(fp, line);
  // erase BOM
  if (!line.empty() && (unsigned char)line[0] == 0xEF && 
                         (unsigned char)line[1] == 0xBB && 
                         (unsigned char)line[2] == 0xBF) {
        line.erase(0, 3); // 
    }
  int m = 1;
  std::vector<std::string> names;
  std::istringstream readstr(line);
  for (int i = 0; i < m; i++) {
    std::string name;
    getline(readstr, name, ',');
    names.push_back(name);
    std::cout << "Name: '" << name << "', length: " << name.length() << std::endl;
    for (int i = 0; i < name.length(); ++i) {
    std::cout << "Char: '" << name[i] << "', ASCII: " << static_cast<int>(name[i]) << std::endl;
    }
  }
  

  int lines = 0;
  while (getline(fp, line)) {
    lines++;
    std::string val;
    std::istringstream readstr(line);
    builder::Vertex v;
    for (int i = 0; i < m; i++) {
      getline(readstr, val, ',');
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
