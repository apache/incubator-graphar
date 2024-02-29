/*
 * Copyright 2022-2023 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <fstream>
#include <iostream>

#include "./config.h"
#include "gar/api.h"
#include "gar/writer/edges_builder.h"
#include "gar/writer/vertices_builder.h"

// using facebook_combined.txt from SNAP dataset
// available at https://snap.stanford.edu/data/ego-Facebook.html

/*------------------original dataset status------------------*/
#define GRAPH_NAME "facebook"
#define DATA_PATH "/tmp/snap/original_dataset/facebook/facebook_combined.txt"
#define VERTEX_COUNT 4039
#define IS_DIRECTED false
/*-----------------------GraphAr status---------------------*/
#define SAVE_PATH "/tmp/snap/" + graph_name + "/"
#define ADJLIST_TYPE GAR_NAMESPACE::AdjListType::ordered_by_source
#define PAYLOAD_TYPE GAR_NAMESPACE::FileType::CSV
#define VERTEX_CHUNK_SIZE 1024
#define EDGE_CHUNK_SIZE 1024 * 1024

int main(int argc, char* argv[]) {
  std::string graph_name = GRAPH_NAME;
  std::string save_path = SAVE_PATH;

  /*------------------construct vertex info------------------*/
  auto version = GAR_NAMESPACE::InfoVersion::Parse("gar/v1").value();

  // meta info
  std::string vertex_label = "node", vertex_prefix = "vertex/node/";

  // create vertex info
  auto vertex_info = GAR_NAMESPACE::CreateVertexInfo(
      vertex_label, VERTEX_CHUNK_SIZE, {}, vertex_prefix, version);

  // save & dump
  ASSERT(!vertex_info->Dump().has_error());
  ASSERT(vertex_info->Save(save_path + "node.vertex.yml").ok());

  /*------------------construct edge info------------------*/
  std::string src_label = "node", edge_label = "links", dst_label = "node",
              edge_prefix = "edge/node_links_node/";
  bool directed = IS_DIRECTED;

  // construct adjacent lists
  auto adjacent_lists = {
      GAR_NAMESPACE::CreateAdjacentList(ADJLIST_TYPE, PAYLOAD_TYPE)};
  // create edge info
  auto edge_info = GAR_NAMESPACE::CreateEdgeInfo(
      src_label, edge_label, dst_label, EDGE_CHUNK_SIZE, VERTEX_CHUNK_SIZE,
      VERTEX_CHUNK_SIZE, directed, adjacent_lists, {}, edge_prefix, version);

  // save & dump
  ASSERT(!edge_info->Dump().has_error());
  ASSERT(edge_info->Save(save_path + "node_links_node.edge.yml").ok());

  /*------------------construct graph info------------------*/
  // create graph info
  auto graph_info = GAR_NAMESPACE::CreateGraphInfo(
      graph_name, {vertex_info}, {edge_info}, save_path, version);
  // save & dump
  ASSERT(!graph_info->Dump().has_error());
  ASSERT(graph_info->Save(save_path + graph_name + ".graph.yml").ok());

  /*------------------construct vertices------------------*/
  // construct vertices builder
  GAR_NAMESPACE::IdType start_index = 0;
  auto v_builder = GAR_NAMESPACE::builder::VerticesBuilder::Make(
                       vertex_info, save_path, start_index)
                       .value();

  // prepare vertex data
  for (int i = 0; i < VERTEX_COUNT; i++) {
    GAR_NAMESPACE::builder::Vertex v;
    ASSERT(v_builder->AddVertex(v).ok());
  }

  // dump
  ASSERT(v_builder->GetNum() == VERTEX_COUNT);
  std::cout << "vertex_count=" << v_builder->GetNum() << std::endl;
  ASSERT(v_builder->Dump().ok());
  std::cout << "dump vertices collection successfully!" << std::endl;

  // clear vertices
  v_builder->Clear();

  /*------------------construct edges------------------*/
  // construct edges builder
  auto e_builder = GAR_NAMESPACE::builder::EdgesBuilder::Make(
                       edge_info, save_path, ADJLIST_TYPE, VERTEX_COUNT)
                       .value();
  // prepare edge data
  std::ifstream file(DATA_PATH);
  std::string line;
  while (std::getline(file, line)) {
    std::istringstream iss(line);
    // skip comments
    if (line[0] == '#') {
      continue;
    }
    int src, dst;
    if (!(iss >> src >> dst)) {
      break;
    }
    GAR_NAMESPACE::builder::Edge e(src, dst);
    ASSERT(e_builder->AddEdge(e).ok());
  }

  // dump
  std::cout << "edge_count=" << e_builder->GetNum() << std::endl;
  ASSERT(e_builder->Dump().ok());
  std::cout << "dump edges collection successfully!" << std::endl;

  // clear edges
  e_builder->Clear();

  return 0;
}
