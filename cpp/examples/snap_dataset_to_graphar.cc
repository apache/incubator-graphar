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

#include "./config.h"
#include "graphar/api/high_level_writer.h"

// using facebook_combined.txt from SNAP dataset
// available at https://snap.stanford.edu/data/ego-Facebook.html

/*------------------original dataset status------------------*/
#define GRAPH_NAME "facebook"
#define DATA_PATH "/tmp/snap/original_dataset/facebook/facebook_combined.txt"
#define VERTEX_COUNT 4039
#define IS_DIRECTED false
/*-----------------------GraphAr status---------------------*/
#define SAVE_PATH "/tmp/snap/" + graph_name + "/"
#define ADJLIST_TYPE graphar::AdjListType::ordered_by_source
#define PAYLOAD_TYPE graphar::FileType::CSV
#define VERTEX_CHUNK_SIZE 1024
#define EDGE_CHUNK_SIZE 1024 * 1024

int main(int argc, char* argv[]) {
  std::string graph_name = GRAPH_NAME;
  std::string save_path = SAVE_PATH;

  /*------------------construct vertex info------------------*/
  auto version = graphar::InfoVersion::Parse("gar/v1").value();

  // meta info
  std::string type = "node", vertex_prefix = "vertex/node/";

  // create vertex info
  auto vertex_info = graphar::CreateVertexInfo(type, VERTEX_CHUNK_SIZE, {}, {},
                                               vertex_prefix, version);

  // save & dump
  ASSERT(!vertex_info->Dump().has_error());
  ASSERT(vertex_info->Save(save_path + "node.vertex.yml").ok());

  /*------------------construct edge info------------------*/
  std::string src_type = "node", edge_type = "links", dst_type = "node",
              edge_prefix = "edge/node_links_node/";
  bool directed = IS_DIRECTED;

  // construct adjacent lists
  auto adjacent_lists = {
      graphar::CreateAdjacentList(ADJLIST_TYPE, PAYLOAD_TYPE)};
  // create edge info
  auto edge_info = graphar::CreateEdgeInfo(
      src_type, edge_type, dst_type, EDGE_CHUNK_SIZE, VERTEX_CHUNK_SIZE,
      VERTEX_CHUNK_SIZE, directed, adjacent_lists, {}, edge_prefix, version);

  // save & dump
  ASSERT(!edge_info->Dump().has_error());
  ASSERT(edge_info->Save(save_path + "node_links_node.edge.yml").ok());

  /*------------------construct graph info------------------*/
  // create graph info
  auto graph_info = graphar::CreateGraphInfo(
      graph_name, {vertex_info}, {edge_info}, {}, save_path, version);
  // save & dump
  ASSERT(!graph_info->Dump().has_error());
  ASSERT(graph_info->Save(save_path + graph_name + ".graph.yml").ok());

  /*------------------construct vertices------------------*/
  // construct vertices builder
  graphar::IdType start_index = 0;
  auto v_builder = graphar::builder::VerticesBuilder::Make(
                       vertex_info, save_path, start_index)
                       .value();

  // prepare vertex data
  for (int i = 0; i < VERTEX_COUNT; i++) {
    graphar::builder::Vertex v;
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
  auto e_builder = graphar::builder::EdgesBuilder::Make(
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
    graphar::builder::Edge e(src, dst);
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
