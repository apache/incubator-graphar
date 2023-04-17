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

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>

#include "./util.h"

#include "gar/graph_info.h"
#include "gar/utils/version_parser.h"

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

TEST_CASE("test_graph_info") {
  std::string graph_name = "test_graph";
  std::string prefix = "test_prefix";
  GAR_NAMESPACE::InfoVersion version(1);
  GAR_NAMESPACE::GraphInfo graph_info(graph_name, version, prefix);
  REQUIRE(graph_info.GetName() == graph_name);
  REQUIRE(graph_info.GetPrefix() == prefix);
  REQUIRE(graph_info.GetVersion() == version);

  // test add vertex and get vertex info
  REQUIRE(graph_info.GetVertexInfos().size() == 0);
  GAR_NAMESPACE::VertexInfo vertex_info("test_vertex", 100, version,
                                        "test_vertex_prefix");
  auto st = graph_info.AddVertex(vertex_info);
  REQUIRE(st.ok());
  REQUIRE(graph_info.GetVertexInfos().size() == 1);
  auto maybe_vertex_info = graph_info.GetVertexInfo("test_vertex");
  REQUIRE(!maybe_vertex_info.has_error());
  REQUIRE(maybe_vertex_info.value().GetLabel() == "test_vertex");
  REQUIRE(maybe_vertex_info.value().GetPrefix() == "test_vertex_prefix");
  REQUIRE(graph_info.GetVertexInfo("test_not_exist").status().IsKeyError());
  // vertex info already exists
  REQUIRE(graph_info.AddVertex(vertex_info).IsInvalidOperation());

  // test add edge and get edge info
  REQUIRE(graph_info.GetEdgeInfos().size() == 0);
  std::string src_label = "test_vertex", edge_label = "test_edge",
              dst_label = "test_vertex";
  GAR_NAMESPACE::EdgeInfo edge_info(src_label, edge_label, dst_label, 1024, 100,
                                    100, true, version);
  st = graph_info.AddEdge(edge_info);
  REQUIRE(st.ok());
  REQUIRE(graph_info.GetEdgeInfos().size() == 1);
  auto maybe_edge_info =
      graph_info.GetEdgeInfo(src_label, edge_label, dst_label);
  REQUIRE(!maybe_edge_info.has_error());
  REQUIRE(maybe_edge_info.value().GetSrcLabel() == src_label);
  REQUIRE(maybe_edge_info.value().GetEdgeLabel() == edge_label);
  REQUIRE(maybe_edge_info.value().GetDstLabel() == dst_label);
  REQUIRE(graph_info.GetEdgeInfo("xxx", "xxx", "xxx").status().IsKeyError());
  // edge info already exists
  REQUIRE(graph_info.AddEdge(edge_info).IsInvalidOperation());

  REQUIRE(graph_info.GetVersion() == version);

  // TODO(@acezen): test dump

  std::string save_path(std::tmpnam(nullptr));
  REQUIRE(graph_info.Save(save_path).ok());
  REQUIRE(std::filesystem::exists(save_path));

  // TODO(@acezen) test GetVertexPropertyGroup

  // TODO(@acezen): test is validated
}

TEST_CASE("test_vertex_info") {
  std::string label = "test_vertex";
  int chunk_size = 100;
  GAR_NAMESPACE::InfoVersion version(1);
  GAR_NAMESPACE::VertexInfo v_info(label, chunk_size, version);
  REQUIRE(v_info.GetLabel() == label);
  REQUIRE(v_info.GetChunkSize() == chunk_size);
  REQUIRE(v_info.GetPrefix() == label + "/");  // default prefix is label + "/"
  REQUIRE(v_info.GetVersion() == version);

  // test add property group
  GAR_NAMESPACE::Property p;
  p.name = "id";
  p.type = GAR_NAMESPACE::DataType(GAR_NAMESPACE::Type::INT32);
  p.is_primary = true;
  GAR_NAMESPACE::PropertyGroup pg({p}, GAR_NAMESPACE::FileType::CSV);
  REQUIRE(v_info.GetPropertyGroups().size() == 0);
  REQUIRE(v_info.AddPropertyGroup(pg).ok());
  // same property group can not be added twice
  REQUIRE(v_info.AddPropertyGroup(pg).IsInvalidOperation());
  GAR_NAMESPACE::PropertyGroup pg2({p}, GAR_NAMESPACE::FileType::PARQUET);
  // same property can not be put in different property group
  REQUIRE(v_info.AddPropertyGroup(pg2).IsInvalidOperation());
  REQUIRE(v_info.GetPropertyGroups().size() == 1);

  // test get property meta
  REQUIRE(v_info.GetPropertyType(p.name) == p.type);
  REQUIRE(v_info.IsPrimaryKey(p.name) == p.is_primary);
  REQUIRE(v_info.GetPropertyType("not_exist_key").status().IsKeyError());
  REQUIRE(v_info.IsPrimaryKey("not_exist_key").status().IsKeyError());
  REQUIRE(v_info.ContainPropertyGroup(pg));
  REQUIRE(!v_info.ContainPropertyGroup(pg2));
  auto result = v_info.GetPropertyGroup(p.name);
  REQUIRE(!result.has_error());
  const auto& property_group = result.value();
  REQUIRE(property_group.GetProperties()[0].name == p.name);
  REQUIRE(v_info.GetPropertyGroup("not_exist_key").status().IsKeyError());

  // test get dir path
  std::string expected_dir_path = v_info.GetPrefix() + pg.GetPrefix();
  auto maybe_dir_path = v_info.GetPathPrefix(pg);
  REQUIRE(!maybe_dir_path.has_error());
  REQUIRE(maybe_dir_path.value() == expected_dir_path);
  // property group not exist
  REQUIRE(v_info.GetPathPrefix(pg2).status().IsKeyError());
  // test get file path
  auto maybe_path = v_info.GetFilePath(pg, 0);
  REQUIRE(!maybe_path.has_error());
  REQUIRE(maybe_path.value() == expected_dir_path + "chunk0");
  // property group not exist
  REQUIRE(v_info.GetFilePath(pg2, 0).status().IsKeyError());
  // vertex count file path
  auto maybe_path2 = v_info.GetVerticesNumFilePath();
  REQUIRE(!maybe_path2.has_error());
  REQUIRE(maybe_path2.value() == v_info.GetPrefix() + "vertex_count");

  // TODO(@acezen): test dump

  // test save
  std::string save_path(std::tmpnam(nullptr));
  REQUIRE(v_info.Save(save_path).ok());
  REQUIRE(std::filesystem::exists(save_path));

  // TODO(@acezen): test extend

  // TODO(@acezen): test is validated
}

TEST_CASE("test_edge_info") {
  std::string src_label = "person", edge_label = "knows", dst_label = "person";
  int chunk_size = 1024;
  int src_chunk_size = 100;
  int dst_chunk_size = 100;
  bool directed = true;
  GAR_NAMESPACE::InfoVersion version(1);
  GAR_NAMESPACE::EdgeInfo edge_info(src_label, edge_label, dst_label,
                                    chunk_size, src_chunk_size, dst_chunk_size,
                                    directed, version);
  REQUIRE(edge_info.GetSrcLabel() == src_label);
  REQUIRE(edge_info.GetEdgeLabel() == edge_label);
  REQUIRE(edge_info.GetDstLabel() == dst_label);
  REQUIRE(edge_info.GetChunkSize() == chunk_size);
  REQUIRE(edge_info.GetSrcChunkSize() == src_chunk_size);
  REQUIRE(edge_info.GetDstChunkSize() == dst_chunk_size);
  REQUIRE(edge_info.IsDirected() == directed);
  REQUIRE(edge_info.GetPrefix() ==
          src_label + "_" + edge_label + "_" + dst_label + "/");
  REQUIRE(edge_info.GetVersion() == version);

  auto adj_list_type = GAR_NAMESPACE::AdjListType::ordered_by_source;
  auto adj_list_type_not_exist = GAR_NAMESPACE::AdjListType::ordered_by_dest;
  auto file_type = GAR_NAMESPACE::FileType::PARQUET;
  REQUIRE(edge_info.AddAdjList(adj_list_type, file_type).ok());
  REQUIRE(edge_info.ContainAdjList(adj_list_type));
  // same adj list type can not be added twice
  REQUIRE(edge_info.AddAdjList(adj_list_type, file_type).IsInvalidOperation());
  auto file_type_result = edge_info.GetFileType(adj_list_type);
  REQUIRE(!file_type_result.has_error());
  REQUIRE(file_type_result.value() == file_type);
  auto prefix_of_adj_list_type =
      std::string(GraphArchive::AdjListTypeToString(adj_list_type)) + "/";
  auto adj_list_path_prefix = edge_info.GetAdjListPathPrefix(adj_list_type);
  REQUIRE(!adj_list_path_prefix.has_error());
  REQUIRE(adj_list_path_prefix.value() ==
          edge_info.GetPrefix() + prefix_of_adj_list_type + "adj_list/");
  auto adj_list_file_path = edge_info.GetAdjListFilePath(0, 0, adj_list_type);
  REQUIRE(!adj_list_file_path.has_error());
  REQUIRE(adj_list_file_path.value() ==
          adj_list_path_prefix.value() + "part0/chunk0");
  auto adj_list_offset_path_prefix =
      edge_info.GetOffsetPathPrefix(adj_list_type);
  REQUIRE(!adj_list_offset_path_prefix.has_error());
  REQUIRE(adj_list_offset_path_prefix.value() ==
          edge_info.GetPrefix() + prefix_of_adj_list_type + "offset/");
  auto adj_list_offset_file_path =
      edge_info.GetAdjListOffsetFilePath(0, adj_list_type);
  REQUIRE(!adj_list_offset_file_path.has_error());
  REQUIRE(adj_list_offset_file_path.value() ==
          adj_list_offset_path_prefix.value() + "chunk0");

  // adj list type not exist
  REQUIRE(!edge_info.ContainAdjList(adj_list_type_not_exist));
  REQUIRE(edge_info.GetFileType(adj_list_type_not_exist).status().IsKeyError());
  REQUIRE(edge_info.GetAdjListFilePath(0, 0, adj_list_type_not_exist)
              .status()
              .IsKeyError());
  REQUIRE(edge_info.GetAdjListPathPrefix(adj_list_type_not_exist)
              .status()
              .IsKeyError());
  REQUIRE(edge_info.GetAdjListOffsetFilePath(0, adj_list_type_not_exist)
              .status()
              .IsKeyError());
  REQUIRE(edge_info.GetOffsetPathPrefix(adj_list_type_not_exist)
              .status()
              .IsKeyError());

  GAR_NAMESPACE::Property p;
  p.name = "creationDate";
  p.type = GAR_NAMESPACE::DataType(GAR_NAMESPACE::Type::STRING);
  p.is_primary = false;
  GAR_NAMESPACE::PropertyGroup pg({p}, file_type);

  auto pgs = edge_info.GetPropertyGroups(adj_list_type);
  REQUIRE(pgs.status().ok());
  REQUIRE(pgs.value().size() == 0);
  REQUIRE(edge_info.AddPropertyGroup(pg, adj_list_type).ok());
  REQUIRE(edge_info.ContainPropertyGroup(pg, adj_list_type));
  pgs = edge_info.GetPropertyGroups(adj_list_type);
  REQUIRE(pgs.status().ok());
  REQUIRE(pgs.value().size() == 1);
  auto property_group_result =
      edge_info.GetPropertyGroup(p.name, adj_list_type);
  REQUIRE(!property_group_result.has_error());
  REQUIRE(property_group_result.value() == pg);
  auto data_type_result = edge_info.GetPropertyType(p.name);
  REQUIRE(!data_type_result.has_error());
  REQUIRE(data_type_result.value() == p.type);
  auto is_primary_result = edge_info.IsPrimaryKey(p.name);
  REQUIRE(!is_primary_result.has_error());
  REQUIRE(is_primary_result.value() == p.is_primary);
  auto property_path_path_prefix =
      edge_info.GetPropertyGroupPathPrefix(pg, adj_list_type);
  REQUIRE(!property_path_path_prefix.has_error());
  REQUIRE(property_path_path_prefix.value() ==
          edge_info.GetPrefix() + prefix_of_adj_list_type + pg.GetPrefix());
  auto property_file_path =
      edge_info.GetPropertyFilePath(pg, adj_list_type, 0, 0);
  REQUIRE(!property_file_path.has_error());
  REQUIRE(property_file_path.value() ==
          property_path_path_prefix.value() + "part0/chunk0");
  // test property not exist
  REQUIRE(edge_info.GetPropertyGroup("p_not_exist", adj_list_type)
              .status()
              .IsKeyError());
  REQUIRE(edge_info.GetPropertyType("p_not_exist").status().IsKeyError());
  REQUIRE(edge_info.IsPrimaryKey("p_not_exist").status().IsKeyError());

  // test property group not exist
  GAR_NAMESPACE::PropertyGroup pg_not_exist;
  REQUIRE(edge_info.GetPropertyFilePath(pg_not_exist, adj_list_type, 0, 0)
              .status()
              .IsKeyError());
  REQUIRE(edge_info.GetPropertyGroupPathPrefix(pg_not_exist, adj_list_type)
              .status()
              .IsKeyError());

  // test adj list not exist
  REQUIRE(edge_info.GetPropertyGroups(adj_list_type_not_exist)
              .status()
              .IsKeyError());
  REQUIRE(edge_info.GetPropertyGroup(p.name, adj_list_type_not_exist)
              .status()
              .IsKeyError());
  REQUIRE(edge_info.GetPropertyFilePath(pg, adj_list_type_not_exist, 0, 0)
              .status()
              .IsKeyError());
  REQUIRE(edge_info.GetPropertyGroupPathPrefix(pg, adj_list_type_not_exist)
              .status()
              .IsKeyError());
  REQUIRE(edge_info.GetEdgesNumFilePath(0, adj_list_type_not_exist)
              .status()
              .IsKeyError());

  // edge count file path
  auto maybe_path = edge_info.GetEdgesNumFilePath(0, adj_list_type);
  REQUIRE(!maybe_path.has_error());
  REQUIRE(maybe_path.value() ==
          edge_info.GetPrefix() + prefix_of_adj_list_type + "edge_count0");

  // test save
  std::string save_path(std::tmpnam(nullptr));
  REQUIRE(edge_info.Save(save_path).ok());
  REQUIRE(std::filesystem::exists(save_path));

  // TODO(@acezen): test extend

  // TODO(@acezen): test is validated
}

TEST_CASE("test_info_version") {
  GAR_NAMESPACE::InfoVersion info_version(1);
  REQUIRE(info_version.version() == 1);
  REQUIRE(info_version.user_define_types() == std::vector<std::string>({}));
  REQUIRE(info_version.ToString() == "gar/v1");
  REQUIRE(info_version.CheckType("int32") == true);
  REQUIRE(info_version.CheckType("date32") == false);

  GAR_NAMESPACE::InfoVersion info_version_2(1, {"t1", "t2"});
  REQUIRE(info_version_2.version() == 1);
  REQUIRE(info_version_2.user_define_types() ==
          std::vector<std::string>({"t1", "t2"}));
  REQUIRE(info_version_2.ToString() == "gar/v1 (t1,t2)");
  REQUIRE(info_version_2.CheckType("t1") == true);

  // raise error if version is not 1
  CHECK_THROWS_AS(GAR_NAMESPACE::InfoVersion(2), std::invalid_argument);

  std::string version_str = "gar/v1 (t1,t2)";
  auto info_version_result = GAR_NAMESPACE::InfoVersion::Parse(version_str);
  REQUIRE(!info_version_result.has_error());
  auto& info_version_3 = info_version_result.value();
  REQUIRE(info_version_3.version() == 1);
  REQUIRE(info_version_3.user_define_types() ==
          std::vector<std::string>({"t1", "t2"}));
  REQUIRE(info_version_3.ToString() == version_str);
  REQUIRE(info_version_3.CheckType("t1") == true);
}

TEST_CASE("test_graph_info_load_from_file") {
  std::string root;
  REQUIRE(GetTestResourceRoot(&root).ok());

  std::string path = root + "/ldbc_sample/csv/ldbc_sample.graph.yml";
  auto graph_info_result = GAR_NAMESPACE::GraphInfo::Load(path);
  if (graph_info_result.has_error()) {
    std::cout << graph_info_result.status().message() << std::endl;
  }
  REQUIRE(!graph_info_result.has_error());
  auto graph_info = graph_info_result.value();
  REQUIRE(graph_info.GetName() == "ldbc_sample");
  REQUIRE(graph_info.GetPrefix() == root + "/ldbc_sample/csv/");
  const auto& vertex_infos = graph_info.GetVertexInfos();
  const auto& edge_infos = graph_info.GetEdgeInfos();
  REQUIRE(vertex_infos.size() == 1);
  REQUIRE(edge_infos.size() == 1);
}

TEST_CASE("test_graph_info_load_from_s3") {
  std::string path =
      "s3://graphar/ldbc/ldbc.graph.yml"
      "?endpoint_override=graphscope.oss-cn-beijing.aliyuncs.com";
  auto graph_info_result = GAR_NAMESPACE::GraphInfo::Load(path);
  if (graph_info_result.has_error()) {
    std::cout << graph_info_result.status().message() << std::endl;
  }
  REQUIRE(!graph_info_result.has_error());
  auto graph_info = graph_info_result.value();
  REQUIRE(graph_info.GetName() == "ldbc");
  const auto& vertex_infos = graph_info.GetVertexInfos();
  const auto& edge_infos = graph_info.GetEdgeInfos();
  REQUIRE(vertex_infos.size() == 8);
  REQUIRE(edge_infos.size() == 23);
}
