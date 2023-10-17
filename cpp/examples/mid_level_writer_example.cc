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
#include <iostream>

#include "arrow/api.h"
#include "arrow/csv/api.h"
#include "arrow/filesystem/api.h"
#include "parquet/arrow/reader.h"

#include "./config.h"
#include "gar/writer/arrow_chunk_writer.h"

std::shared_ptr<arrow::Table> read_csv_file_and_return_table(
    const std::string& path) {
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
  ASSERT(maybe_reader.ok());
  std::shared_ptr<arrow::csv::TableReader> reader = *maybe_reader;

  // Read table from CSV file
  auto maybe_table = reader->Read();
  ASSERT(maybe_table.ok());
  std::shared_ptr<arrow::Table> table = *maybe_table;
  // output
  std::cout << "reading CSV file done" << std::endl;
  std::cout << "file path: " << path << std::endl;
  std::cout << "rows number of CSV file: " << table->num_rows() << std::endl;
  std::cout << "schema of CSV file: " << std::endl
            << table->schema()->ToString() << std::endl;
  return table;
}

std::shared_ptr<arrow::Table> read_parquet_file_and_return_table(
    const std::string& path, bool is_adj_list = false) {
  arrow::Status st;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  auto fs = arrow::fs::FileSystemFromUriOrPath(path).ValueOrDie();
  std::shared_ptr<arrow::io::RandomAccessFile> input =
      fs->OpenInputFile(path).ValueOrDie();
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  st = parquet::arrow::OpenFile(input, pool, &arrow_reader);
  // Read entire file as a single Arrow table
  std::shared_ptr<arrow::Table> maybe_table;
  st = arrow_reader->ReadTable(&maybe_table);
  ASSERT(st.ok());

  std::shared_ptr<arrow::Table> table;
  if (is_adj_list) {
    table = maybe_table
                ->RenameColumns({GAR_NAMESPACE::GeneralParams::kSrcIndexCol,
                                 GAR_NAMESPACE::GeneralParams::kDstIndexCol})
                .ValueOrDie();
  } else {
    table = maybe_table;
  }
  std::cout << "reading parquet file done" << std::endl;
  std::cout << "file path: " << path << std::endl;
  std::cout << "rows number of parquet file: " << table->num_rows()
            << std::endl;
  std::cout << "schema of parquet file: " << std::endl
            << table->schema()->ToString() << std::endl;
  return table;
}

void vertex_property_writer(const GAR_NAMESPACE::GraphInfo& graph_info) {
  // constuct writer
  std::string vertex_meta_file =
      TEST_DATA_DIR + "/ldbc_sample/parquet/" + "person.vertex.yml";
  auto vertex_meta = GAR_NAMESPACE::Yaml::LoadFile(vertex_meta_file).value();
  auto vertex_info = GAR_NAMESPACE::VertexInfo::Load(vertex_meta).value();
  ASSERT(vertex_info.GetLabel() == "person");
  GAR_NAMESPACE::VertexPropertyWriter writer(vertex_info, "/tmp/");

  // construct vertex property table from reading a CSV file
  auto table = read_csv_file_and_return_table(TEST_DATA_DIR +
                                              "/ldbc_sample/person_0_0.csv");

  // use writer
  // set validate level
  writer.SetValidateLevel(GAR_NAMESPACE::ValidateLevel::strong_validate);
  // write the table
  ASSERT(writer.WriteTable(table, 0).ok());
  // write the number of vertices
  ASSERT(writer.WriteVerticesNum(table->num_rows()).ok());
  std::cout << "writing vertex data successfully!" << std::endl;
  // check vertex count
  auto path = "/tmp/vertex/person/vertex_count";
  auto fs = arrow::fs::FileSystemFromUriOrPath(path).ValueOrDie();
  auto input = fs->OpenInputStream(path).ValueOrDie();
  auto num = input->Read(sizeof(GAR_NAMESPACE::IdType)).ValueOrDie();
  GAR_NAMESPACE::IdType* ptr = (GAR_NAMESPACE::IdType*) num->data();
  std::cout << "vertex count from reading written file: " << *ptr << std::endl;
}

void edge_chunk_writer(const GAR_NAMESPACE::GraphInfo& graph_info) {
  // constuct writer
  std::string edge_meta_file =
      TEST_DATA_DIR + "/ldbc_sample/csv/" + "person_knows_person.edge.yml";
  auto edge_meta = GAR_NAMESPACE::Yaml::LoadFile(edge_meta_file).value();
  auto edge_info = GAR_NAMESPACE::EdgeInfo::Load(edge_meta).value();
  auto adj_list_type = GAR_NAMESPACE::AdjListType::ordered_by_source;
  GAR_NAMESPACE::EdgeChunkWriter writer(edge_info, "/tmp/", adj_list_type);

  // construct property chunk from reading a Parquet file
  auto chunk = read_parquet_file_and_return_table(
      TEST_DATA_DIR +
      "/ldbc_sample/parquet/edge/person_knows_person/ordered_by_source/"
      "creationDate/part0/chunk0");
  // construct adj list table from reading a Parquet file
  auto table = read_parquet_file_and_return_table(
      TEST_DATA_DIR +
      "/ldbc_sample/parquet/edge/person_knows_person/unordered_by_source/"
      "adj_list/part0/chunk0");

  // use writer
  // set validate level
  writer.SetValidateLevel(GAR_NAMESPACE::ValidateLevel::strong_validate);
  // write a property chunk
  GAR_NAMESPACE::PropertyGroup pg =
      edge_info.GetPropertyGroup("creationDate", adj_list_type).value();
  ASSERT(writer.WritePropertyChunk(chunk, pg, 0, 0).ok());
  // write adj list of vertex chunk 0 to files
  ASSERT(writer.SortAndWriteAdjListTable(table, 0, 0).ok());
  // write number of edges for vertex chunk 0
  ASSERT(writer.WriteEdgesNum(0, table->num_rows()).ok());
  // write number of vertices
  ASSERT(writer.WriteVerticesNum(903).ok());
  std::cout << "writing edge data successfully!" << std::endl;
  // check the number of edges
  auto path = "/tmp/edge/person_knows_person/ordered_by_source/edge_count0";
  auto fs = arrow::fs::FileSystemFromUriOrPath(path).ValueOrDie();
  std::shared_ptr<arrow::io::InputStream> input =
      fs->OpenInputStream(path).ValueOrDie();
  auto edge_num = input->Read(sizeof(GAR_NAMESPACE::IdType)).ValueOrDie();
  GAR_NAMESPACE::IdType* edge_num_ptr =
      (GAR_NAMESPACE::IdType*) edge_num->data();
  std::cout << "edge number from reading written file: " << *edge_num_ptr
            << std::endl;
}

int main(int argc, char* argv[]) {
  // read file and construct graph info
  std::string path =
      TEST_DATA_DIR + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
  auto graph_info = GAR_NAMESPACE::GraphInfo::Load(path).value();

  // vertex property writer
  std::cout << "Vertex property writer" << std::endl;
  std::cout << "----------------------" << std::endl;
  vertex_property_writer(graph_info);
  std::cout << std::endl;

  // edge property writer
  std::cout << "Edge property writer" << std::endl;
  std::cout << "--------------------" << std::endl;
  edge_chunk_writer(graph_info);
}
