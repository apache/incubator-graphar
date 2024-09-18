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

#include <cassert>

#include "./config.h"
#include "graphar/api/info.h"

int main(int argc, char* argv[]) {
  /*------------------construct vertex info------------------*/
  auto version = graphar::InfoVersion::Parse("gar/v1").value();

  // meta info
  std::string type = "person", vertex_prefix = "vertex/person/";
  int chunk_size = 100;

  // construct properties and property groups
  auto property_vector_1 = {graphar::Property("id", graphar::int32(), true)};
  auto property_vector_2 = {
      graphar::Property("firstName", graphar::string(), false),
      graphar::Property("lastName", graphar::string(), false),
      graphar::Property("gender", graphar::string(), false)};

  auto group1 =
      graphar::CreatePropertyGroup(property_vector_1, graphar::FileType::CSV);
  auto group2 =
      graphar::CreatePropertyGroup(property_vector_2, graphar::FileType::ORC);

  // create vertex info
  auto vertex_info = graphar::CreateVertexInfo(type, chunk_size, {group1}, {},
                                               vertex_prefix, version);

  ASSERT(vertex_info != nullptr);
  ASSERT(vertex_info->GetType() == type);
  ASSERT(vertex_info->GetChunkSize() == chunk_size);
  ASSERT(vertex_info->GetPropertyGroups().size() == 1);
  ASSERT(vertex_info->HasProperty("id"));
  ASSERT(!vertex_info->HasProperty("firstName"));
  ASSERT(vertex_info->HasPropertyGroup(group1));
  ASSERT(!vertex_info->HasPropertyGroup(group2));
  ASSERT(vertex_info->IsPrimaryKey("id"));
  ASSERT(!vertex_info->IsPrimaryKey("gender"));
  ASSERT(vertex_info->GetPropertyType("id").value()->Equals(graphar::int32()));
  ASSERT(vertex_info->GetFilePath(group1, 0).value() ==
         "vertex/person/id/chunk0");

  // extend property groups & validate
  auto result = vertex_info->AddPropertyGroup(group2);
  ASSERT(result.status().ok());
  vertex_info = result.value();
  ASSERT(vertex_info->HasProperty("firstName"));
  ASSERT(vertex_info->HasPropertyGroup(group2));
  ASSERT(!vertex_info->IsPrimaryKey("gender"));
  ASSERT(vertex_info->IsValidated());

  // save & dump
  ASSERT(!vertex_info->Dump().has_error());
  ASSERT(vertex_info->Save("/tmp/person.vertex.yml").ok());

  /*------------------construct edge info------------------*/
  // meta info
  std::string src_type = "person", edge_type = "knows", dst_type = "person",
              edge_prefix = "edge/person_knows_person/";
  int edge_chunk_size = 1024, src_chunk_size = 100, dst_chunk_size = 100;
  bool directed = false;

  // construct adjacent lists
  auto adjacent_lists = {
      graphar::CreateAdjacentList(graphar::AdjListType::unordered_by_source,
                                  graphar::FileType::CSV),
      graphar::CreateAdjacentList(graphar::AdjListType::ordered_by_dest,
                                  graphar::FileType::CSV)};
  // construct properties and property groups
  auto property_vector_3 = {
      graphar::Property("creationDate", graphar::string(), false)};
  auto group3 = graphar::CreatePropertyGroup(property_vector_3,
                                             graphar::FileType::PARQUET);

  // create edge info
  auto edge_info = graphar::CreateEdgeInfo(
      src_type, edge_type, dst_type, edge_chunk_size, src_chunk_size,
      dst_chunk_size, directed, adjacent_lists, {group3}, edge_prefix, version);

  ASSERT(edge_info != nullptr);
  ASSERT(edge_info->GetSrcType() == src_type);
  ASSERT(edge_info->GetEdgeType() == edge_type);
  ASSERT(edge_info->GetDstType() == dst_type);
  ASSERT(edge_info->GetChunkSize() == edge_chunk_size);
  ASSERT(edge_info->GetSrcChunkSize() == src_chunk_size);
  ASSERT(edge_info->GetDstChunkSize() == dst_chunk_size);
  ASSERT(edge_info->IsDirected() == directed);

  ASSERT(edge_info->HasAdjacentListType(
      graphar::AdjListType::unordered_by_source));
  ASSERT(
      edge_info->GetAdjListFilePath(0, 0, graphar::AdjListType::ordered_by_dest)
          .value() ==
      "edge/person_knows_person/ordered_by_dest/adj_list/part0/chunk0");
  ASSERT(
      edge_info
          ->GetAdjListOffsetFilePath(0, graphar::AdjListType::ordered_by_dest)
          .value() == "edge/person_knows_person/ordered_by_dest/offset/chunk0");

  ASSERT(edge_info->HasPropertyGroup(group3));
  ASSERT(edge_info->HasProperty("creationDate"));
  ASSERT(
      edge_info
          ->GetPropertyFilePath(group3,
                                graphar::AdjListType::unordered_by_source, 0, 0)
          .value() ==
      "edge/person_knows_person/unordered_by_source/creationDate/part0/chunk0");
  ASSERT(edge_info->GetPropertyType("creationDate")
             .value()
             ->Equals(graphar::string()));
  ASSERT(!edge_info->IsPrimaryKey("creationDate"));

  // extend & validate
  auto new_adjacent_list = graphar::CreateAdjacentList(
      graphar::AdjListType::ordered_by_source, graphar::FileType::PARQUET);
  auto res1 = edge_info->AddAdjacentList(new_adjacent_list);
  ASSERT(res1.status().ok());
  edge_info = res1.value();
  ASSERT(
      edge_info->HasAdjacentListType(graphar::AdjListType::ordered_by_source));
  ASSERT(edge_info->IsValidated());
  // save & dump
  ASSERT(!edge_info->Dump().has_error());
  ASSERT(edge_info->Save("/tmp/person_knows_person.edge.yml").ok());

  /*------------------create graph info with vertex info and edge
   * info------------------*/
  // meta info
  std::string name = "graph", prefix = "file:///tmp/";

  // create graph info
  auto graph_info = graphar::CreateGraphInfo(name, {vertex_info}, {edge_info},
                                             {}, prefix, version);
  ASSERT(graph_info->GetName() == name);
  ASSERT(graph_info->GetPrefix() == prefix);
  ASSERT(graph_info->GetVertexInfos().size() == 1);
  ASSERT(graph_info->GetVertexInfo(type) != nullptr);
  auto vertex_info_from_graph = graph_info->GetVertexInfo(type);
  ASSERT(vertex_info_from_graph != nullptr);
  ASSERT(vertex_info_from_graph->HasPropertyGroup(group1));
  ASSERT(vertex_info_from_graph->HasPropertyGroup(group2));
  ASSERT(graph_info->GetEdgeInfos().size() == 1);
  auto edge_info_from_graph =
      graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
  ASSERT(edge_info_from_graph != nullptr);
  ASSERT(edge_info_from_graph->PropertyGroupNum() == 1);
  ASSERT(edge_info_from_graph->HasPropertyGroup(group3));
  ASSERT(graph_info->IsValidated());

  // save & dump
  ASSERT(!graph_info->Dump().has_error());
  ASSERT(graph_info->Save("/tmp/ldbc_sample.graph.yml").ok());
}
