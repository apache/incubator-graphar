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


int main(int argc, char* argv[]) {
  /*------------------construct graph info------------------*/
  std::string name = "graph", prefix = "file:///tmp/";
  GAR_NAMESPACE::InfoVersion version(1);
  GAR_NAMESPACE::GraphInfo graph_info(name, version, prefix);
  // validate
  assert(graph_info.GetName() == name);
  assert(graph_info.GetPrefix() == prefix);
  const auto& vertex_infos = graph_info.GetVertexInfos();
  const auto& edge_infos = graph_info.GetEdgeInfos();
  assert(vertex_infos.size() == 0);
  assert(edge_infos.size() == 0);

  /*------------------construct vertex info------------------*/
  std::string vertex_label = "person", vertex_prefix = "vertex/person/";
  int chunk_size = 100;
  GAR_NAMESPACE::VertexInfo vertex_info(vertex_label, chunk_size, version,
                                        vertex_prefix);
  // validate
  assert(vertex_info.GetLabel() == vertex_label);
  assert(vertex_info.GetChunkSize() == chunk_size);
  assert(vertex_info.GetPropertyGroups().size() == 0);

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
  assert(vertex_info.AddPropertyGroup(group1).ok());
  assert(vertex_info.GetPropertyGroups()[0] == group1);
  assert(vertex_info.ContainProperty(id.name));
  assert(!vertex_info.ContainProperty(firstName.name));
  assert(vertex_info.ContainPropertyGroup(group1));
  assert(!vertex_info.ContainPropertyGroup(group2));
  assert(vertex_info.IsPrimaryKey(id.name).value());
  assert(!vertex_info.IsPrimaryKey(gender.name).status().ok());
  assert(vertex_info.GetPropertyType(id.name).value() == id.type);
  assert(vertex_info.GetFilePath(group1, 0).value() ==
          "vertex/person/id/chunk0");

  // extend property groups & validate
  auto result = vertex_info.Extend(group2);
  assert(result.status().ok());
  vertex_info = result.value();
  assert(vertex_info.ContainProperty(firstName.name));
  assert(vertex_info.ContainPropertyGroup(group2));
  assert(vertex_info.GetPropertyGroup(firstName.name) == group2);
  assert(!vertex_info.IsPrimaryKey(gender.name).value());
  assert(vertex_info.IsValidated());

  // save & dump
  assert(!vertex_info.Dump().has_error());
  assert(vertex_info.Save("/tmp/person.vertex.yml").ok());

  /*------------------add vertex info to graph------------------*/
  graph_info.AddVertex(vertex_info);
  assert(graph_info.GetVertexInfos().size() == 1);
  assert(graph_info.GetVertexInfo(vertex_label).status().ok());
  assert(graph_info.GetVertexPropertyGroup(vertex_label, id.name).value() ==
          group1);
  assert(
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
  assert(edge_info.GetSrcLabel() == src_label);
  assert(edge_info.GetEdgeLabel() == edge_label);
  assert(edge_info.GetDstLabel() == dst_label);
  assert(edge_info.GetChunkSize() == edge_chunk_size);
  assert(edge_info.GetSrcChunkSize() == src_chunk_size);
  assert(edge_info.GetDstChunkSize() == dst_chunk_size);
  assert(edge_info.IsDirected() == directed);

  // add adj list & validate
  assert(!edge_info.ContainAdjList(
      GAR_NAMESPACE::AdjListType::unordered_by_source));
  assert(edge_info
              .AddAdjList(GAR_NAMESPACE::AdjListType::unordered_by_source,
                          GAR_NAMESPACE::FileType::PARQUET)
              .ok());
  assert(edge_info.ContainAdjList(
      GAR_NAMESPACE::AdjListType::unordered_by_source));
  assert(edge_info
              .AddAdjList(GAR_NAMESPACE::AdjListType::ordered_by_dest,
                          GAR_NAMESPACE::FileType::PARQUET)
              .ok());
  assert(
      edge_info.GetFileType(GAR_NAMESPACE::AdjListType::ordered_by_dest)
          .value() == GAR_NAMESPACE::FileType::PARQUET);
  assert(
      edge_info
          .GetAdjListFilePath(0, 0, GAR_NAMESPACE::AdjListType::ordered_by_dest)
          .value() ==
      "edge/person_knows_person/ordered_by_dest/adj_list/part0/chunk0");
  assert(edge_info
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
  assert(!edge_info.ContainPropertyGroup(
      group3, GAR_NAMESPACE::AdjListType::unordered_by_source));
  assert(!edge_info.ContainProperty(creationDate.name));
  assert(edge_info
              .AddPropertyGroup(group3,
                                GAR_NAMESPACE::AdjListType::unordered_by_source)
              .ok());
  assert(edge_info.ContainPropertyGroup(
      group3, GAR_NAMESPACE::AdjListType::unordered_by_source));
  assert(edge_info.ContainProperty(creationDate.name));
  assert(
      edge_info
          .GetPropertyGroups(GAR_NAMESPACE::AdjListType::unordered_by_source)
          .value()[0] == group3);
  assert(edge_info
              .GetPropertyGroup(creationDate.name,
                                GAR_NAMESPACE::AdjListType::unordered_by_source)
              .value() == group3);
  assert(!edge_info
               .GetPropertyGroup(creationDate.name,
                                 GAR_NAMESPACE::AdjListType::ordered_by_source)
               .status()
               .ok());
  assert(
      edge_info
          .GetPropertyFilePath(
              group3, GAR_NAMESPACE::AdjListType::unordered_by_source, 0, 0)
          .value() ==
      "edge/person_knows_person/unordered_by_source/creationDate/part0/chunk0");
  assert(edge_info.GetPropertyType(creationDate.name).value() ==
          creationDate.type);
  assert(edge_info.IsPrimaryKey(creationDate.name).value() ==
          creationDate.is_primary);

  // extend & validate
  auto res1 =
      edge_info.ExtendAdjList(GAR_NAMESPACE::AdjListType::ordered_by_source,
                              GAR_NAMESPACE::FileType::PARQUET);
  assert(res1.status().ok());
  edge_info = res1.value();
  assert(edge_info
              .GetFileType(GAR_NAMESPACE::AdjListType::ordered_by_source)
              .value() == GAR_NAMESPACE::FileType::PARQUET);
  auto res2 = edge_info.ExtendPropertyGroup(
      group3, GAR_NAMESPACE::AdjListType::ordered_by_source);
  assert(res2.status().ok());
  assert(edge_info.IsValidated());
  // save & dump
  assert(!edge_info.Dump().has_error());
  assert(edge_info.Save("/tmp/person_knows_person.edge.yml").ok());

  /*------------------add edge info to graph------------------*/
  graph_info.AddEdge(edge_info);
  graph_info.AddEdgeInfoPath("person_knows_person.edge.yml");
  assert(graph_info.GetEdgeInfos().size() == 1);
  assert(
      graph_info.GetEdgeInfo(src_label, edge_label, dst_label).status().ok());
  assert(graph_info
              .GetEdgePropertyGroup(
                  src_label, edge_label, dst_label, creationDate.name,
                  GAR_NAMESPACE::AdjListType::unordered_by_source)
              .value() == group3);
  assert(graph_info.IsValidated());

  // save & dump
  assert(!graph_info.Dump().has_error());
  assert(graph_info.Save("/tmp/ldbc_sample.graph.yml").ok());
}
