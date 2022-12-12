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

#include "gar/graph_info.h"

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

TEST_CASE("test_construct_info_example") {
  /*------------------construct graph info------------------*/
  std::string name = "graph", prefix = "file:///tmp/";
  GAR_NAMESPACE::InfoVersion version(1);
  GAR_NAMESPACE::GraphInfo graph_info(name, version, prefix);
  // validate
  REQUIRE(graph_info.GetName() == name);
  REQUIRE(graph_info.GetPrefix() == prefix);
  const auto& vertex_infos = graph_info.GetAllVertexInfo();
  const auto& edge_infos = graph_info.GetAllEdgeInfo();
  REQUIRE(vertex_infos.size() == 0);
  REQUIRE(edge_infos.size() == 0);

  /*------------------construct vertex info------------------*/
  std::string vertex_label = "person", vertex_prefix = "vertex/person/";
  int chunk_size = 100;
  GAR_NAMESPACE::VertexInfo vertex_info(vertex_label, chunk_size, version,
                                        vertex_prefix);
  // validate
  REQUIRE(vertex_info.GetLabel() == vertex_label);
  REQUIRE(vertex_info.GetChunkSize() == chunk_size);
  REQUIRE(vertex_info.GetPropertyGroups().size() == 0);

  // construct properties and property groups
  GAR_NAMESPACE::Property id = {
      "id", GAR_NAMESPACE::DataType(GAR_NAMESPACE::Type::INT32), true};
  GAR_NAMESPACE::Property firstName = {
      "firstName", GAR_NAMESPACE::DataType(GAR_NAMESPACE::Type::STRING), false};
  GAR_NAMESPACE::Property lastName = {
      "lastName", GAR_NAMESPACE::DataType(GAR_NAMESPACE::Type::STRING), false};
  GAR_NAMESPACE::Property gender = {
      "gender", GAR_NAMESPACE::DataType(GAR_NAMESPACE::Type::STRING), false};
  std::vector<GAR_NAMESPACE::Property> property_vector_1 = {id},
                                       property_vector_2 = {firstName, lastName,
                                                            gender};
  GAR_NAMESPACE::PropertyGroup group1(property_vector_1,
                                      GAR_NAMESPACE::FileType::CSV);
  GAR_NAMESPACE::PropertyGroup group2(property_vector_2,
                                      GAR_NAMESPACE::FileType::ORC);

  // add property groups to vertex info & validate
  REQUIRE(vertex_info.AddPropertyGroup(group1).ok());
  REQUIRE(vertex_info.GetPropertyGroups()[0] == group1);
  REQUIRE(vertex_info.ContainProperty(id.name));
  REQUIRE(!vertex_info.ContainProperty(firstName.name));
  REQUIRE(vertex_info.ContainPropertyGroup(group1));
  REQUIRE(!vertex_info.ContainPropertyGroup(group2));
  REQUIRE(vertex_info.IsPrimaryKey(id.name).value());
  REQUIRE(!vertex_info.IsPrimaryKey(gender.name).status().ok());
  REQUIRE(vertex_info.GetPropertyType(id.name).value() == id.type);
  REQUIRE(vertex_info.GetFilePath(group1, 0).value() ==
          "vertex/person/id/part0/chunk0");

  // extend property groups & validate
  auto result = vertex_info.Extend(group2);
  REQUIRE(result.status().ok());
  vertex_info = result.value();
  REQUIRE(vertex_info.ContainProperty(firstName.name));
  REQUIRE(vertex_info.ContainPropertyGroup(group2));
  REQUIRE(vertex_info.GetPropertyGroup(firstName.name) == group2);
  REQUIRE(!vertex_info.IsPrimaryKey(gender.name).value());
  REQUIRE(vertex_info.IsValidated());

  // save & dump
  REQUIRE(!vertex_info.Dump().has_error());
  REQUIRE(vertex_info.Save("/tmp/person.vertex.yml").ok());

  /*------------------add vertex info to graph------------------*/
  graph_info.AddVertex(vertex_info);
  REQUIRE(graph_info.GetAllVertexInfo().size() == 1);
  REQUIRE(graph_info.GetVertexInfo(vertex_label).status().ok());
  REQUIRE(graph_info.GetVertexPropertyGroup(vertex_label, id.name).value() ==
          group1);
  REQUIRE(
      graph_info.GetVertexPropertyGroup(vertex_label, firstName.name).value() ==
      group2);
  graph_info.AddVertexInfoPath("person.vertex.yml");

  /*------------------construct edge info------------------*/
  std::string src_label = "person", edge_label = "knows", dst_label = "person",
              edge_prefix = "edge/person_knows_person/";
  int edge_chunk_size = 1024, src_chunk_size = 100, dst_chunk_size = 100;
  bool directed = false;
  GAR_NAMESPACE::EdgeInfo edge_info(
      src_label, edge_label, dst_label, edge_chunk_size, src_chunk_size,
      dst_chunk_size, directed, version, edge_prefix);
  REQUIRE(edge_info.GetSrcLabel() == src_label);
  REQUIRE(edge_info.GetEdgeLabel() == edge_label);
  REQUIRE(edge_info.GetDstLabel() == dst_label);
  REQUIRE(edge_info.GetChunkSize() == edge_chunk_size);
  REQUIRE(edge_info.GetSrcChunkSize() == src_chunk_size);
  REQUIRE(edge_info.GetDstChunkSize() == dst_chunk_size);
  REQUIRE(edge_info.IsDirected() == directed);

  // add adj list & validate
  REQUIRE(!edge_info.ContainAdjList(
      GAR_NAMESPACE::AdjListType::unordered_by_source));
  REQUIRE(edge_info
              .AddAdjList(GAR_NAMESPACE::AdjListType::unordered_by_source,
                          GAR_NAMESPACE::FileType::PARQUET)
              .ok());
  REQUIRE(edge_info.ContainAdjList(
      GAR_NAMESPACE::AdjListType::unordered_by_source));
  REQUIRE(edge_info
              .AddAdjList(GAR_NAMESPACE::AdjListType::ordered_by_dest,
                          GAR_NAMESPACE::FileType::PARQUET)
              .ok());
  REQUIRE(
      edge_info.GetAdjListFileType(GAR_NAMESPACE::AdjListType::ordered_by_dest)
          .value() == GAR_NAMESPACE::FileType::PARQUET);
  REQUIRE(
      edge_info
          .GetAdjListFilePath(0, 0, GAR_NAMESPACE::AdjListType::ordered_by_dest)
          .value() ==
      "edge/person_knows_person/ordered_by_dest/adj_list/part0/chunk0");
  REQUIRE(edge_info
              .GetAdjListOffsetFilePath(
                  0, GAR_NAMESPACE::AdjListType::ordered_by_dest)
              .value() ==
          "edge/person_knows_person/ordered_by_dest/offset/part0/chunk0");

  // add property group & validate
  GAR_NAMESPACE::Property creationDate = {
      "creationDate", GAR_NAMESPACE::DataType(GAR_NAMESPACE::Type::STRING),
      false};
  std::vector<GAR_NAMESPACE::Property> property_vector_3 = {creationDate};
  GAR_NAMESPACE::PropertyGroup group3(property_vector_3,
                                      GAR_NAMESPACE::FileType::PARQUET);
  REQUIRE(!edge_info.ContainPropertyGroup(
      group3, GAR_NAMESPACE::AdjListType::unordered_by_source));
  REQUIRE(!edge_info.ContainProperty(creationDate.name));
  REQUIRE(edge_info
              .AddPropertyGroup(group3,
                                GAR_NAMESPACE::AdjListType::unordered_by_source)
              .ok());
  REQUIRE(edge_info.ContainPropertyGroup(
      group3, GAR_NAMESPACE::AdjListType::unordered_by_source));
  REQUIRE(edge_info.ContainProperty(creationDate.name));
  REQUIRE(
      edge_info
          .GetPropertyGroups(GAR_NAMESPACE::AdjListType::unordered_by_source)
          .value()[0] == group3);
  REQUIRE(edge_info
              .GetPropertyGroup(creationDate.name,
                                GAR_NAMESPACE::AdjListType::unordered_by_source)
              .value() == group3);
  REQUIRE(!edge_info
               .GetPropertyGroup(creationDate.name,
                                 GAR_NAMESPACE::AdjListType::ordered_by_source)
               .status()
               .ok());
  REQUIRE(
      edge_info
          .GetPropertyFilePath(
              group3, GAR_NAMESPACE::AdjListType::unordered_by_source, 0, 0)
          .value() ==
      "edge/person_knows_person/unordered_by_source/creationDate/part0/chunk0");
  REQUIRE(edge_info.GetPropertyType(creationDate.name).value() ==
          creationDate.type);
  REQUIRE(edge_info.IsPrimaryKey(creationDate.name).value() ==
          creationDate.is_primary);

  // extend & validate
  auto res1 =
      edge_info.ExtendAdjList(GAR_NAMESPACE::AdjListType::ordered_by_source,
                              GAR_NAMESPACE::FileType::PARQUET);
  REQUIRE(res1.status().ok());
  edge_info = res1.value();
  REQUIRE(edge_info
              .GetAdjListFileType(GAR_NAMESPACE::AdjListType::ordered_by_source)
              .value() == GAR_NAMESPACE::FileType::PARQUET);
  auto res2 = edge_info.ExtendPropertyGroup(
      group3, GAR_NAMESPACE::AdjListType::ordered_by_source);
  REQUIRE(res2.status().ok());
  REQUIRE(edge_info.IsValidated());
  // save & dump
  REQUIRE(!edge_info.Dump().has_error());
  REQUIRE(edge_info.Save("/tmp/person_knows_person.edge.yml").ok());

  /*------------------add edge info to graph------------------*/
  graph_info.AddEdge(edge_info);
  graph_info.AddEdgeInfoPath("person_knows_person.edge.yml");
  REQUIRE(graph_info.GetAllEdgeInfo().size() == 1);
  REQUIRE(
      graph_info.GetEdgeInfo(src_label, edge_label, dst_label).status().ok());
  REQUIRE(graph_info
              .GetEdgePropertyGroup(
                  src_label, edge_label, dst_label, creationDate.name,
                  GAR_NAMESPACE::AdjListType::unordered_by_source)
              .value() == group3);
  REQUIRE(graph_info.IsValidated());

  // save & dump
  REQUIRE(!graph_info.Dump().has_error());
  REQUIRE(graph_info.Save("/tmp/ldbc_sample.graph.yml").ok());
}
