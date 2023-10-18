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

#include "arrow/api.h"
#include "arrow/csv/api.h"
#include "arrow/filesystem/api.h"
#include "parquet/arrow/reader.h"

#include "./config.h"
#include "gar/writer/edges_builder.h"
#include "gar/writer/vertices_builder.h"

void vertices_builder() {
  // construct vertices builder
  std::string vertex_meta_file =
      TEST_DATA_DIR + "/ldbc_sample/parquet/" + "person.vertex.yml";
  auto vertex_meta = GAR_NAMESPACE::Yaml::LoadFile(vertex_meta_file).value();
  auto vertex_info = GAR_NAMESPACE::VertexInfo::Load(vertex_meta).value();
  GAR_NAMESPACE::IdType start_index = 0;
  GAR_NAMESPACE::builder::VerticesBuilder builder(vertex_info, "/tmp/",
                                                  start_index);

  // set validate level
  builder.SetValidateLevel(GAR_NAMESPACE::ValidateLevel::strong_validate);

  // read data from a csv file
  std::ifstream fp(TEST_DATA_DIR + "/ldbc_sample/person_0_0.csv");
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

  // read data and add vertices
  while (getline(fp, line)) {
    std::string val;
    std::istringstream readstr(line);
    GAR_NAMESPACE::builder::Vertex v;
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
    ASSERT(builder.AddVertex(v).ok());
  }

  // dump
  std::cout << "vertex_count=" << builder.GetNum() << std::endl;
  ASSERT(builder.Dump().ok());
  std::cout << "dump vertices collection successfully!" << std::endl;

  // clear vertices
  builder.Clear();
  ASSERT(builder.GetNum() == 0);
}

void edges_builder() {
  // construct edges builder
  std::string edge_meta_file =
      TEST_DATA_DIR + "/ldbc_sample/parquet/" + "person_knows_person.edge.yml";
  auto edge_meta = GAR_NAMESPACE::Yaml::LoadFile(edge_meta_file).value();
  auto edge_info = GAR_NAMESPACE::EdgeInfo::Load(edge_meta).value();
  auto vertices_num = 903;
  GAR_NAMESPACE::builder::EdgesBuilder builder(
      edge_info, "/tmp/", GraphArchive::AdjListType::ordered_by_dest,
      vertices_num);

  // set validate level
  builder.SetValidateLevel(GAR_NAMESPACE::ValidateLevel::strong_validate);

  // read data from a csv file
  std::ifstream fp(TEST_DATA_DIR + "/ldbc_sample/person_knows_person_0_0.csv");
  std::string line;
  getline(fp, line);
  std::vector<std::string> names;
  std::istringstream readstr(line);
  std::map<std::string, int64_t> mapping;
  int64_t cnt = 0;

  // read data and add edges
  while (getline(fp, line)) {
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
        GAR_NAMESPACE::builder::Edge e(s, d);
        e.AddProperty("creationDate", val);
        ASSERT(builder.AddEdge(e).ok());
      }
    }
  }

  // dump
  std::cout << "edge_count=" << builder.GetNum() << std::endl;
  ASSERT(builder.Dump().ok());
  std::cout << "dump edges collection successfully!" << std::endl;

  // clear edges
  builder.Clear();
  ASSERT(builder.GetNum() == 0);
}

int main(int argc, char* argv[]) {
  // vertices builder
  std::cout << "Vertices builder" << std::endl;
  std::cout << "-------------------" << std::endl;
  vertices_builder();
  std::cout << std::endl;

  // edges builder
  std::cout << "Edges builder" << std::endl;
  std::cout << "----------------" << std::endl;
  edges_builder();
}
