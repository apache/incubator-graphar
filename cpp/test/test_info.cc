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

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>

#include "./util.h"

#include "graphar/api/info.h"
#include "graphar/fwd.h"
#include "graphar/status.h"

#include <catch2/catch_test_macros.hpp>

namespace graphar {

TEST_CASE_METHOD(GlobalFixture, "InfoVersion") {
  InfoVersion info_version(1);
  REQUIRE(info_version.version() == 1);
  REQUIRE(info_version.user_define_types() == std::vector<std::string>({}));
  REQUIRE(info_version.ToString() == "gar/v1");
  REQUIRE(info_version.CheckType("int32") == true);
  REQUIRE(info_version.CheckType("date32") == false);

  InfoVersion info_version_2(1, {"t1", "t2"});
  REQUIRE(info_version_2.version() == 1);
  REQUIRE(info_version_2.user_define_types() ==
          std::vector<std::string>({"t1", "t2"}));
  REQUIRE(info_version_2.ToString() == "gar/v1 (t1,t2)");
  REQUIRE(info_version_2.CheckType("t1") == true);

  // raise error if version is not 1
  CHECK_THROWS_AS(InfoVersion(2), std::invalid_argument);

  SECTION("Parse") {
    std::string version_str = "gar/v1 (t1,t2)";
    auto info_version_result = InfoVersion::Parse(version_str);
    REQUIRE(!info_version_result.has_error());
    auto& info_version_3 = info_version_result.value();
    REQUIRE(info_version_3->version() == 1);
    REQUIRE(info_version_3->user_define_types() ==
            std::vector<std::string>({"t1", "t2"}));
    REQUIRE(info_version_3->ToString() == version_str);
    REQUIRE(info_version_3->CheckType("t1") == true);
  }
}

TEST_CASE_METHOD(GlobalFixture, "Property") {
  Property p0("p0", int32(), true);
  Property p1("p1", int32(), false);

  REQUIRE(p0.name == "p0");
  REQUIRE(p0.type->ToTypeName() == int32()->ToTypeName());
  REQUIRE(p0.is_primary == true);
  REQUIRE(p0.is_nullable == false);
  REQUIRE(p1.is_primary == false);
  REQUIRE(p1.is_nullable == true);
}

TEST_CASE_METHOD(GlobalFixture, "PropertyGroup") {
  Property p0("p0", int32(), true);
  Property p1("p1", int32(), false, true, Cardinality::SINGLE);
  Property p2("p2", string(), false, true, Cardinality::LIST);
  Property p3("p3", float32(), false, true, Cardinality::SET);
  Property p4("p4", float64(), false);

  PropertyGroup pg0({p0, p1}, FileType::CSV, "p0_and_p1/");
  PropertyGroup pg1({p2, p3, p4}, FileType::PARQUET);
  SECTION("Properties") {
    REQUIRE(pg0.GetProperties().size() == 2);
    REQUIRE(pg1.GetProperties().size() == 3);
    REQUIRE(pg0.HasProperty("p0") == true);
    REQUIRE(pg0.HasProperty("p2") == false);
    REQUIRE(pg1.HasProperty("p2") == true);
    REQUIRE(pg1.HasProperty("p0") == false);
    auto& p = pg0.GetProperties()[0];
    REQUIRE(p.name == "p0");
    REQUIRE(p.type->ToTypeName() == int32()->ToTypeName());
    REQUIRE(p.is_primary == true);
    REQUIRE(p.is_nullable == false);
    // cardinality
    REQUIRE(p0.cardinality == Cardinality::SINGLE);
    REQUIRE(p1.cardinality == Cardinality::SINGLE);
    REQUIRE(p2.cardinality == Cardinality::LIST);
    REQUIRE(p3.cardinality == Cardinality::SET);
  }

  SECTION("FileType") {
    REQUIRE(pg0.GetFileType() == FileType::CSV);
    REQUIRE(pg1.GetFileType() == FileType::PARQUET);
  }

  SECTION("Prefix") {
    REQUIRE(pg0.GetPrefix() == "p0_and_p1/");
    REQUIRE(pg1.GetPrefix() == "p2_p3_p4/");
  }

  SECTION("IsValidate") {
    REQUIRE(pg0.IsValidated() == true);
    REQUIRE(pg1.IsValidated() == true);
    Property invalid_p0("invalid", nullptr, false);
    Property invalid_p1("", int32(), false);
    PropertyGroup invalid_pg0({invalid_p0}, FileType::CSV);
    PropertyGroup invalid_pg1({invalid_p1}, FileType::CSV);
    PropertyGroup invalid_pg2({p0, p0}, FileType::PARQUET);
    PropertyGroup invalid_pg3({}, FileType::CSV, "empty/");
    REQUIRE(invalid_pg0.IsValidated() == false);
    REQUIRE(invalid_pg1.IsValidated() == false);
    REQUIRE(invalid_pg2.IsValidated() == false);
    REQUIRE(invalid_pg3.IsValidated() == false);
  }

  SECTION("CreatePropertyGroup") {
    auto pg2 = CreatePropertyGroup({p0, p1}, FileType::CSV, "p0_and_p1/");
    REQUIRE(*pg2.get() == pg0);
    REQUIRE(!(pg0 == pg1));

    // not allow empty property group
    auto pg3 = CreatePropertyGroup({}, FileType::PARQUET);
    REQUIRE(pg3 == nullptr);
  }

  SECTION("Ostream") {
    std::stringstream ss;
    ss << pg0;
    REQUIRE(ss.str() == "p0_p1");
    ss.str("");
    ss << pg1;
    REQUIRE(ss.str() == "p2_p3_p4");
  }
}

TEST_CASE_METHOD(GlobalFixture, "AdjacentList") {
  AdjacentList adj_list0(AdjListType::unordered_by_source, FileType::CSV,
                         "adj_list0/");
  AdjacentList adj_list1(AdjListType::ordered_by_source, FileType::PARQUET);

  SECTION("AdjListType") {
    REQUIRE(adj_list0.GetType() == AdjListType::unordered_by_source);
    REQUIRE(adj_list1.GetType() == AdjListType::ordered_by_source);
  }

  SECTION("FileType") {
    REQUIRE(adj_list0.GetFileType() == FileType::CSV);
    REQUIRE(adj_list1.GetFileType() == FileType::PARQUET);
  }

  SECTION("Prefix") {
    REQUIRE(adj_list0.GetPrefix() == "adj_list0/");
    REQUIRE(adj_list1.GetPrefix() == "ordered_by_source/");
  }

  SECTION("IsValidate") {
    REQUIRE(adj_list0.IsValidated() == true);
    REQUIRE(adj_list1.IsValidated() == true);
  }

  SECTION("CreateAdjacentList") {
    auto adj_list2 = CreateAdjacentList(AdjListType::unordered_by_source,
                                        FileType::CSV, "unordered_by_source/");
    REQUIRE(adj_list2->GetType() == AdjListType::unordered_by_source);
    REQUIRE(adj_list2->GetFileType() == FileType::CSV);
    REQUIRE(adj_list2->GetPrefix() == "unordered_by_source/");
  }
}

TEST_CASE_METHOD(GlobalFixture, "VertexInfo") {
  std::string type = "test_vertex";
  int chunk_size = 100;
  auto version = std::make_shared<InfoVersion>(1);
  auto pg = CreatePropertyGroup(
      {Property("p0", int32(), true),
       Property("p1", string(), false, true, Cardinality::LIST),
       Property("p2", string(), false, true, Cardinality::SET)},
      FileType::PARQUET, "p0_p1/");
  auto vertex_info =
      CreateVertexInfo(type, chunk_size, {pg}, {}, "test_vertex", version);

  SECTION("Basics") {
    REQUIRE(vertex_info->GetType() == type);
    REQUIRE(vertex_info->GetChunkSize() == chunk_size);
    REQUIRE(vertex_info->GetPrefix() == "test_vertex");
    REQUIRE(vertex_info->version()->ToString() == "gar/v1");
  }

  SECTION("PropertyGroup") {
    REQUIRE(vertex_info->PropertyGroupNum() == 1);
    REQUIRE(*vertex_info->GetPropertyGroupByIndex(0) == *pg);
    REQUIRE(vertex_info->HasProperty("p0") == true);
    REQUIRE(vertex_info->HasPropertyGroup(pg) == true);
    REQUIRE(*vertex_info->GetPropertyGroup("p0") == *pg);
    REQUIRE(vertex_info->GetPropertyGroups().size() == 1);
    REQUIRE(*(vertex_info->GetPropertyGroups()[0]) == *pg);
    REQUIRE(vertex_info->GetPropertyType("p0").value()->ToTypeName() ==
            int32()->ToTypeName());
    REQUIRE(vertex_info->IsPrimaryKey("p0") == true);
    REQUIRE(vertex_info->IsPrimaryKey("p1") == false);
    REQUIRE(vertex_info->IsNullableKey("p0") == false);
    REQUIRE(vertex_info->IsNullableKey("p1") == true);
    REQUIRE(vertex_info->GetPropertyCardinality("p0").value() ==
            Cardinality::SINGLE);
    REQUIRE(vertex_info->GetPropertyCardinality("p1").value() ==
            Cardinality::LIST);
    REQUIRE(vertex_info->GetPropertyCardinality("p2").value() ==
            Cardinality::SET);
    REQUIRE(vertex_info->HasProperty("not_exist") == false);
    REQUIRE(vertex_info->IsPrimaryKey("not_exist") == false);
    REQUIRE(vertex_info->HasPropertyGroup(nullptr) == false);
  }

  SECTION("Path") {
    REQUIRE(vertex_info->GetPathPrefix(pg).value() == "test_vertex/p0_p1/");
    REQUIRE(vertex_info->GetFilePath(pg, 0).value() ==
            "test_vertex/p0_p1/chunk0");
    REQUIRE(vertex_info->GetVerticesNumFilePath().value() ==
            "test_vertex/vertex_count");
  }

  SECTION("IsValidate") {
    REQUIRE(vertex_info->IsValidated() == true);
    auto invalid_pg = CreatePropertyGroup(
        {Property("p0", list(string()), true, false, Cardinality::SET)},
        FileType::CSV);
    auto invalid_vertex_info0 = CreateVertexInfo(type, chunk_size, {invalid_pg},
                                                 {}, "test_vertex/", version);
    REQUIRE(invalid_vertex_info0->IsValidated() == false);
    invalid_pg =
        CreatePropertyGroup({Property("p0", nullptr, true)}, FileType::CSV);
    invalid_vertex_info0 = CreateVertexInfo(type, chunk_size, {invalid_pg}, {},
                                            "test_vertex/", version);
    REQUIRE(invalid_vertex_info0->IsValidated() == false);
    VertexInfo invalid_vertex_info1("", chunk_size, {pg}, {}, "test_vertex/",
                                    version);
    REQUIRE(invalid_vertex_info1.IsValidated() == false);
    VertexInfo invalid_vertex_info2(type, 0, {pg}, {}, "test_vertex/", version);
    REQUIRE(invalid_vertex_info2.IsValidated() == false);
    // check if prefix empty
    auto vertex_info_empty_prefix =
        CreateVertexInfo(type, chunk_size, {pg}, {}, "", version);
    REQUIRE(vertex_info_empty_prefix->IsValidated() == true);
  }

  SECTION("CreateVertexInfo") {
    auto vertex_info3 =
        CreateVertexInfo("", chunk_size, {pg}, {}, "test_vertex/");
    REQUIRE(vertex_info3 == nullptr);

    auto vertex_info4 = CreateVertexInfo(type, 0, {pg}, {}, "test_vertex/");
    REQUIRE(vertex_info4 == nullptr);
  }

  SECTION("Dump") {
    auto dump_result = vertex_info->Dump();
    REQUIRE(dump_result.status().ok());
    std::string expected = R"(chunk_size: 100
prefix: test_vertex
property_groups: 
  - file_type: parquet
    prefix: p0_p1/
    properties: 
      - data_type: int32
        is_nullable: false
        is_primary: true
        name: p0
      - cardinality: list
        data_type: string
        is_nullable: true
        is_primary: false
        name: p1
      - cardinality: set
        data_type: string
        is_nullable: true
        is_primary: false
        name: p2
type: test_vertex
version: gar/v1
)";
    REQUIRE(dump_result.value() == expected);
    auto vertex_info_empty_version =
        CreateVertexInfo(type, chunk_size, {pg}, {}, "test_vertex/");
    REQUIRE(vertex_info_empty_version->Dump().status().ok());
  }

  SECTION("Save") {
    // save to a simple output path
    REQUIRE(vertex_info->Save("/tmp/" + type + ".vertex.yml").ok());
    // save to a URI path
    REQUIRE(vertex_info->Save("file:///tmp/" + type + ".vertex.yml").ok());
  }

  SECTION("AddPropertyGroup") {
    auto pg3 = CreatePropertyGroup({Property("p3", int32(), false)},
                                   FileType::CSV, "p3/");
    auto maybe_extend_info = vertex_info->AddPropertyGroup(pg3);
    REQUIRE(maybe_extend_info.status().ok());
    auto extend_info = maybe_extend_info.value();
    REQUIRE(extend_info->PropertyGroupNum() == 2);
    REQUIRE(extend_info->HasProperty("p2") == true);
    REQUIRE(extend_info->HasPropertyGroup(pg3) == true);
    REQUIRE(extend_info->GetPropertyGroups().size() == 2);
    REQUIRE(*(extend_info->GetPropertyGroups()[1]) == *pg3);
    REQUIRE(extend_info->GetPropertyType("p3").value()->ToTypeName() ==
            int32()->ToTypeName());
    REQUIRE(extend_info->IsPrimaryKey("p3") == false);
    REQUIRE(extend_info->IsNullableKey("p3") == true);
    REQUIRE(extend_info->GetPropertyCardinality("p3") == Cardinality::SINGLE);
    auto extend_info3 = extend_info->AddPropertyGroup(pg3);
    REQUIRE(!extend_info3.status().ok());
  }

  SECTION("RemovePropertyGroup") {
    auto pg3 = CreatePropertyGroup({Property("p3", int32(), false)},
                                   FileType::CSV, "p3/");
    auto maybe_extend_info = vertex_info->AddPropertyGroup(pg3);
    REQUIRE(maybe_extend_info.status().ok());
    auto extend_info = maybe_extend_info.value();
    REQUIRE(extend_info->PropertyGroupNum() == 2);
    auto maybe_removed_info = extend_info->RemovePropertyGroup(pg3);
    REQUIRE(maybe_removed_info.status().ok());
    auto removed_info = maybe_removed_info.value();
    REQUIRE(removed_info->PropertyGroupNum() == 1);
    REQUIRE(removed_info->HasPropertyGroup(pg3) == false);
    auto maybe_removed_again = removed_info->RemovePropertyGroup(pg3);
    REQUIRE(!maybe_removed_again.status().ok());
  }
}

TEST_CASE_METHOD(GlobalFixture, "EdgeInfo") {
  std::string src_type = "person", edge_type = "knows", dst_type = "person";
  int chunk_size = 1024;
  int src_chunk_size = 100;
  int dst_chunk_size = 100;
  bool directed = true;
  auto version = std::make_shared<InfoVersion>(1);
  auto adj_list_type = AdjListType::ordered_by_source;
  auto adj_list =
      CreateAdjacentList(adj_list_type, FileType::CSV, "ordered_by_source/");
  auto pg = CreatePropertyGroup(
      {Property("p0", int32(), true), Property("p1", string(), false)},
      FileType::CSV, "p0_p1/");
  auto edge_info = CreateEdgeInfo(src_type, edge_type, dst_type, chunk_size,
                                  src_chunk_size, dst_chunk_size, directed,
                                  {adj_list}, {pg}, "test_edge/", version);

  SECTION("Basics") {
    REQUIRE(edge_info->GetSrcType() == src_type);
    REQUIRE(edge_info->GetEdgeType() == edge_type);
    REQUIRE(edge_info->GetDstType() == dst_type);
    REQUIRE(edge_info->GetChunkSize() == chunk_size);
    REQUIRE(edge_info->GetSrcChunkSize() == src_chunk_size);
    REQUIRE(edge_info->GetDstChunkSize() == dst_chunk_size);
    REQUIRE(edge_info->IsDirected() == directed);
    REQUIRE(edge_info->GetPrefix() == "test_edge/");
    REQUIRE(edge_info->version()->ToString() == "gar/v1");
  }

  SECTION("AdjacentList") {
    REQUIRE(edge_info->HasAdjacentListType(adj_list_type) == true);
    REQUIRE(edge_info->HasAdjacentListType(AdjListType::unordered_by_source) ==
            false);
    REQUIRE(edge_info->GetAdjacentList(adj_list_type)->GetType() ==
            adj_list_type);
    REQUIRE(edge_info->GetAdjacentList(adj_list_type)->GetFileType() ==
            FileType::CSV);
    REQUIRE(edge_info->GetAdjacentList(AdjListType::unordered_by_source) ==
            nullptr);
  }

  SECTION("PropertyGroup") {
    REQUIRE(edge_info->PropertyGroupNum() == 1);
    REQUIRE(*edge_info->GetPropertyGroupByIndex(0) == *pg);
    REQUIRE(edge_info->HasProperty("p0") == true);
    REQUIRE(edge_info->HasPropertyGroup(pg) == true);
    REQUIRE(*edge_info->GetPropertyGroup("p0") == *pg);
    REQUIRE(edge_info->GetPropertyGroups().size() == 1);
    REQUIRE(*(edge_info->GetPropertyGroups()[0]) == *pg);
    REQUIRE(edge_info->GetPropertyType("p0").value()->ToTypeName() ==
            int32()->ToTypeName());
    REQUIRE(edge_info->IsPrimaryKey("p0") == true);
    REQUIRE(edge_info->IsPrimaryKey("p1") == false);
    REQUIRE(edge_info->IsNullableKey("p0") == false);
    REQUIRE(edge_info->IsNullableKey("p1") == true);
    REQUIRE(edge_info->HasProperty("not_exist") == false);
    REQUIRE(edge_info->IsPrimaryKey("not_exist") == false);
    REQUIRE(edge_info->IsNullableKey("not_exist") == false);
    REQUIRE(edge_info->HasPropertyGroup(nullptr) == false);
  }

  SECTION("Path") {
    REQUIRE(edge_info->GetAdjListPathPrefix(adj_list_type).value() ==
            "test_edge/ordered_by_source/adj_list/");
    REQUIRE(edge_info->GetAdjListFilePath(0, 0, adj_list_type).value() ==
            "test_edge/ordered_by_source/adj_list/part0/chunk0");
    REQUIRE(edge_info->GetOffsetPathPrefix(adj_list_type).value() ==
            "test_edge/ordered_by_source/offset/");
    REQUIRE(edge_info->GetAdjListOffsetFilePath(0, adj_list_type).value() ==
            "test_edge/ordered_by_source/offset/chunk0");
    REQUIRE(edge_info->GetEdgesNumFilePath(0, adj_list_type).value() ==
            "test_edge/ordered_by_source/edge_count0");
    REQUIRE(edge_info->GetVerticesNumFilePath(adj_list_type).value() ==
            "test_edge/ordered_by_source/vertex_count");
    REQUIRE(edge_info->GetPropertyGroupPathPrefix(pg, adj_list_type).value() ==
            "test_edge/ordered_by_source/p0_p1/");
    REQUIRE(edge_info->GetPropertyFilePath(pg, adj_list_type, 0, 0).value() ==
            "test_edge/ordered_by_source/p0_p1/part0/chunk0");
  }

  SECTION("IsValidated") {
    REQUIRE(edge_info->IsValidated() == true);
    auto invalid_pg =
        CreatePropertyGroup({Property("p0", nullptr, true)}, FileType::CSV);
    auto invalid_edge_info0 =
        CreateEdgeInfo(src_type, edge_type, dst_type, chunk_size,
                       src_chunk_size, dst_chunk_size, directed, {adj_list},
                       {invalid_pg}, "test_edge/", version);
    REQUIRE(invalid_edge_info0->IsValidated() == false);
    // edgeInfo does not support list/set cardinality
    invalid_pg = CreatePropertyGroup(
        {Property("p_cardinality", string(), false, true, Cardinality::LIST)},
        FileType::PARQUET);
    auto cardinality_invalid_edge_info0 =
        CreateEdgeInfo(src_type, edge_type, dst_type, chunk_size,
                       src_chunk_size, dst_chunk_size, directed, {adj_list},
                       {invalid_pg}, "test_edge/", version);
    REQUIRE(invalid_edge_info0->IsValidated() == false);
    for (int i = 0; i < 3; i++) {
      std::vector<std::string> types = {src_type, edge_type, dst_type};
      types[i] = "";
      EdgeInfo invalid_edge_info1(types[0], types[1], types[2], chunk_size,
                                  src_chunk_size, dst_chunk_size, directed,
                                  {adj_list}, {pg}, "test_edge/", version);
      REQUIRE(invalid_edge_info1.IsValidated() == false);
    }
    for (int i = 0; i < 3; i++) {
      std::vector<int> sizes = {chunk_size, src_chunk_size, dst_chunk_size};
      sizes[i] = 0;
      EdgeInfo invalid_edge_info2(src_type, edge_type, dst_type, sizes[0],
                                  sizes[1], sizes[2], directed, {adj_list},
                                  {pg}, "test_edge/", version);
      REQUIRE(invalid_edge_info2.IsValidated() == false);
    }

    // check if prefix empty
    auto edge_info_with_empty_prefix = CreateEdgeInfo(
        src_type, edge_type, dst_type, chunk_size, src_chunk_size,
        dst_chunk_size, directed, {adj_list}, {pg}, "", version);
    REQUIRE(edge_info_with_empty_prefix->IsValidated() == true);
  }

  SECTION("CreateEdgeInfo") {
    for (int i = 0; i < 3; i++) {
      std::vector<std::string> types = {src_type, edge_type, dst_type};
      types[i] = "";
      auto edge_info = CreateEdgeInfo(types[0], types[1], types[2], chunk_size,
                                      src_chunk_size, dst_chunk_size, directed,
                                      {adj_list}, {pg}, "test_edge/", version);
      REQUIRE(edge_info == nullptr);
    }
    for (int i = 0; i < 3; i++) {
      std::vector<int> sizes = {chunk_size, src_chunk_size, dst_chunk_size};
      sizes[i] = 0;
      auto edge_info = CreateEdgeInfo(src_type, edge_type, dst_type, sizes[0],
                                      sizes[1], sizes[2], directed, {adj_list},
                                      {pg}, "test_edge/", version);
      REQUIRE(edge_info == nullptr);
    }
    auto edge_info_empty_adjlist = CreateEdgeInfo(
        src_type, edge_type, dst_type, chunk_size, src_chunk_size,
        dst_chunk_size, directed, {}, {pg}, "test_edge/");
    REQUIRE(edge_info_empty_adjlist == nullptr);
  }

  SECTION("Dump") {
    auto dump_result = edge_info->Dump();
    REQUIRE(dump_result.status().ok());
    std::string expected = R"(adj_lists: 
  - aligned_by: src
    file_type: csv
    ordered: true
    prefix: ordered_by_source/
chunk_size: 1024
directed: true
dst_chunk_size: 100
dst_type: person
edge_type: knows
prefix: test_edge/
property_groups: 
  - file_type: csv
    prefix: p0_p1/
    properties: 
      - data_type: int32
        is_nullable: false
        is_primary: true
        name: p0
      - data_type: string
        is_nullable: true
        is_primary: false
        name: p1
src_chunk_size: 100
src_type: person
version: gar/v1
)";
    REQUIRE(dump_result.value() == expected);
    auto edge_info_empty_version = CreateEdgeInfo(
        src_type, edge_type, dst_type, chunk_size, src_chunk_size,
        dst_chunk_size, directed, {adj_list}, {pg});
    REQUIRE(edge_info_empty_version->Dump().status().ok());
  }

  SECTION("Save") {
    // save to a simple output path
    REQUIRE(edge_info->Save("/tmp/" + edge_type + ".edge.yml").ok());
    // save to a URI path
    REQUIRE(edge_info->Save("file:///tmp/" + edge_type + ".edge.yml").ok());
  }

  SECTION("AddAdjacentList") {
    auto adj_list2 = CreateAdjacentList(AdjListType::ordered_by_dest,
                                        FileType::CSV, "ordered_by_dest/");
    auto maybe_extend_info = edge_info->AddAdjacentList(adj_list2);
    REQUIRE(maybe_extend_info.status().ok());
    auto extend_info = maybe_extend_info.value();
    REQUIRE(extend_info->HasAdjacentListType(AdjListType::ordered_by_dest) ==
            true);
    REQUIRE(
        extend_info->GetAdjacentList(AdjListType::ordered_by_dest)->GetType() ==
        AdjListType::ordered_by_dest);
    REQUIRE(extend_info->GetAdjacentList(AdjListType::ordered_by_dest)
                ->GetFileType() == FileType::CSV);
    auto extend_info2 = extend_info->AddAdjacentList(adj_list2);
    REQUIRE(!extend_info2.status().ok());
  }

  SECTION("RemoveAdjacentList") {
    auto adj_list2 = CreateAdjacentList(AdjListType::ordered_by_dest,
                                        FileType::CSV, "ordered_by_dest/");
    auto maybe_extend_info = edge_info->AddAdjacentList(adj_list2);
    REQUIRE(maybe_extend_info.status().ok());
    auto extend_info = maybe_extend_info.value();
    auto maybe_remove_info = extend_info->RemoveAdjacentList(adj_list2);
    REQUIRE(maybe_remove_info.status().ok());
    auto remove_info = maybe_remove_info.value();
    REQUIRE(remove_info->HasAdjacentListType(AdjListType::ordered_by_dest) ==
            false);
    auto remove_info2 = remove_info->RemoveAdjacentList(adj_list2);
    REQUIRE(!remove_info2.status().ok());
  }

  SECTION("AddPropertyGroup") {
    auto pg2 = CreatePropertyGroup({Property("p2", int32(), false)},
                                   FileType::CSV, "p2/");
    auto maybe_extend_info = edge_info->AddPropertyGroup(pg2);
    REQUIRE(maybe_extend_info.status().ok());
    auto extend_info = maybe_extend_info.value();
    REQUIRE(extend_info->PropertyGroupNum() == 2);
    REQUIRE(extend_info->HasProperty("p2") == true);
    REQUIRE(extend_info->HasPropertyGroup(pg2) == true);
    REQUIRE(extend_info->GetPropertyGroups().size() == 2);
    REQUIRE(*(extend_info->GetPropertyGroups()[1]) == *pg2);
    REQUIRE(extend_info->GetPropertyType("p2").value()->ToTypeName() ==
            int32()->ToTypeName());
    REQUIRE(extend_info->IsPrimaryKey("p2") == false);
    auto extend_info2 = extend_info->AddPropertyGroup(pg2);
    REQUIRE(!extend_info2.status().ok());
  }

  SECTION("RemovePropertyGroup") {
    auto pg2 = CreatePropertyGroup({Property("p2", int32(), false)},
                                   FileType::CSV, "p2/");
    auto maybe_extend_info = edge_info->AddPropertyGroup(pg2);
    REQUIRE(maybe_extend_info.status().ok());
    auto extend_info = maybe_extend_info.value();
    auto maybe_remove_info = extend_info->RemovePropertyGroup(pg2);
    REQUIRE(maybe_remove_info.status().ok());
    auto remove_info = maybe_remove_info.value();
    REQUIRE(remove_info->PropertyGroupNum() == 1);
    REQUIRE(remove_info->HasProperty("p2") == false);
    REQUIRE(remove_info->HasPropertyGroup(pg2) == false);
    REQUIRE(remove_info->GetPropertyGroups().size() == 1);
    auto remove_info2 = remove_info->RemovePropertyGroup(pg2);
    REQUIRE(!remove_info2.status().ok());
  }
}

TEST_CASE_METHOD(GlobalFixture, "GraphInfo") {
  std::string name = "test_graph";
  auto version = std::make_shared<InfoVersion>(1);
  auto pg = CreatePropertyGroup(
      {Property("p0", int32(), true), Property("p1", string(), false)},
      FileType::CSV, "p0_p1/");
  auto vertex_info =
      CreateVertexInfo("test_vertex", 100, {pg}, {}, "test_vertex/", version);
  std::unordered_map<std::string, std::string> extra_info = {
      {"category", "test graph"}};
  auto edge_info =
      CreateEdgeInfo("person", "knows", "person", 1024, 100, 100, true,
                     {CreateAdjacentList(AdjListType::ordered_by_source,
                                         FileType::CSV, "adj_list/")},
                     {pg}, "test_edge/", version);
  auto graph_info = CreateGraphInfo(name, {vertex_info}, {edge_info}, {},
                                    "test_graph/", version, extra_info);

  SECTION("Basics") {
    REQUIRE(graph_info->GetName() == name);
    REQUIRE(graph_info->GetPrefix() == "test_graph/");
    REQUIRE(graph_info->version()->ToString() == "gar/v1");
    REQUIRE(graph_info->GetExtraInfo().size() == 1);
    REQUIRE(graph_info->GetExtraInfo().find("category") !=
            graph_info->GetExtraInfo().end());
    REQUIRE(graph_info->GetExtraInfo().at("category") == "test graph");
  }

  SECTION("ExtraInfo") {
    auto graph_info_with_extra_info =
        CreateGraphInfo(name, {vertex_info}, {edge_info}, {}, "test_graph/",
                        version, {{"key1", "value1"}, {"key2", "value2"}});
    const auto& extra_info = graph_info_with_extra_info->GetExtraInfo();
    REQUIRE(extra_info.size() == 2);
    REQUIRE(extra_info.find("key1") != extra_info.end());
    REQUIRE(extra_info.at("key1") == "value1");
    REQUIRE(extra_info.find("key2") != extra_info.end());
    REQUIRE(extra_info.at("key2") == "value2");
  }

  SECTION("VertexInfo") {
    REQUIRE(graph_info->VertexInfoNum() == 1);
    REQUIRE(graph_info->GetVertexInfoByIndex(0)->GetType() == "test_vertex");
    REQUIRE(graph_info->GetVertexInfoByIndex(1) == nullptr);
    REQUIRE(graph_info->GetVertexInfo("test_vertex")->GetType() ==
            "test_vertex");
    REQUIRE(graph_info->GetVertexInfo("not_exist") == nullptr);
    REQUIRE(graph_info->GetVertexInfos().size() == 1);
    REQUIRE(graph_info->GetVertexInfos()[0]->GetType() == "test_vertex");
  }

  SECTION("EdgeInfo") {
    REQUIRE(graph_info->EdgeInfoNum() == 1);
    REQUIRE(graph_info->GetEdgeInfoByIndex(0)->GetEdgeType() == "knows");
    REQUIRE(graph_info->GetEdgeInfoByIndex(1) == nullptr);
    REQUIRE(
        graph_info->GetEdgeInfo("person", "knows", "person")->GetEdgeType() ==
        "knows");
    REQUIRE(graph_info->GetEdgeInfo("not_exist", "knows", "person") == nullptr);
    REQUIRE(graph_info->GetEdgeInfos().size() == 1);
    REQUIRE(graph_info->GetEdgeInfos()[0]->GetEdgeType() == "knows");
  }

  SECTION("IsValidated") {
    REQUIRE(graph_info->IsValidated() == true);
    auto invalid_vertex_info =
        CreateVertexInfo("", 100, {pg}, {}, "test_vertex/", version);
    auto invalid_graph_info0 = CreateGraphInfo(
        name, {invalid_vertex_info}, {edge_info}, {}, "test_graph/", version);
    REQUIRE(invalid_graph_info0->IsValidated() == false);
    auto invalid_edge_info =
        CreateEdgeInfo("", "knows", "person", 1024, 100, 100, true,
                       {CreateAdjacentList(AdjListType::ordered_by_source,
                                           FileType::CSV, "adj_list/")},
                       {pg}, "test_edge/", version);
    auto invalid_graph_info1 = CreateGraphInfo(
        name, {vertex_info}, {invalid_edge_info}, {}, "test_graph/", version);
    REQUIRE(invalid_graph_info1->IsValidated() == false);
    GraphInfo invalid_graph_info2("", {vertex_info}, {edge_info}, {},
                                  "test_graph/", version);
    REQUIRE(invalid_graph_info2.IsValidated() == false);
    GraphInfo invalid_graph_info3(name, {vertex_info}, {edge_info}, {}, "",
                                  version);
    REQUIRE(invalid_graph_info3.IsValidated() == false);
    // check if prefix empty, graph_info with empty prefix is invalid
    auto graph_info_with_empty_prefix =
        CreateGraphInfo(name, {vertex_info}, {edge_info}, {}, "", version);
    REQUIRE(graph_info_with_empty_prefix->IsValidated() == false);
  }

  SECTION("CreateGraphInfo") {
    auto graph_info_empty_name =
        CreateGraphInfo("", {vertex_info}, {edge_info}, {}, "test_graph/");
    REQUIRE(graph_info_empty_name == nullptr);
  }

  SECTION("Dump") {
    auto dump_result = graph_info->Dump();
    REQUIRE(dump_result.status().ok());
    std::string expected = R"(edges: 
  - person_knows_person.edge.yaml
extra_info: 
  - key: category
    value: test graph
name: test_graph
prefix: test_graph/
version: gar/v1
vertices: 
  - test_vertex.vertex.yaml
)";
    REQUIRE(dump_result.value() == expected);
    auto graph_info_empty_version =
        CreateGraphInfo(name, {vertex_info}, {edge_info}, {}, "test_graph/");
    REQUIRE(graph_info_empty_version->Dump().status().ok());
  }

  SECTION("Save") {
    // save to a simple output path
    REQUIRE(graph_info->Save("/tmp/" + name + ".graph.yml").ok());
    // save to a URI path
    REQUIRE(graph_info->Save("file:///tmp/" + name + ".graph.yml").ok());
  }

  SECTION("AddVertex") {
    auto vertex_info2 = CreateVertexInfo("test_vertex2", 100, {pg}, {},
                                         "test_vertex2/", version);
    auto maybe_extend_info = graph_info->AddVertex(vertex_info2);
    REQUIRE(maybe_extend_info.status().ok());
    auto extend_info = maybe_extend_info.value();
    REQUIRE(extend_info->VertexInfoNum() == 2);
    REQUIRE(extend_info->GetVertexInfoByIndex(1)->GetType() == "test_vertex2");
    REQUIRE(extend_info->GetVertexInfoByIndex(2) == nullptr);
    REQUIRE(extend_info->GetVertexInfo("test_vertex2")->GetType() ==
            "test_vertex2");
    REQUIRE(extend_info->GetVertexInfo("not_exist") == nullptr);
    REQUIRE(extend_info->GetVertexInfos().size() == 2);
    REQUIRE(extend_info->GetVertexInfos()[1]->GetType() == "test_vertex2");
    auto extend_info2 = extend_info->AddVertex(vertex_info2);
    REQUIRE(!extend_info2.status().ok());
  }

  SECTION("RemoveVertex") {
    auto vertex_info2 = CreateVertexInfo("test_vertex2", 100, {pg}, {},
                                         "test_vertex2/", version);
    auto maybe_extend_info = graph_info->AddVertex(vertex_info2);
    REQUIRE(maybe_extend_info.status().ok());
    auto extend_info = maybe_extend_info.value();
    auto maybe_remove_info = extend_info->RemoveVertex(vertex_info2);
    REQUIRE(maybe_remove_info.status().ok());
    auto remove_info = maybe_remove_info.value();
    REQUIRE(remove_info->GetVertexInfos().size() == 1);
    REQUIRE(remove_info->GetVertexInfoByIndex(1) == nullptr);
    REQUIRE(remove_info->GetVertexInfo("test_vertex2") == nullptr);
    REQUIRE(remove_info->GetVertexInfoByIndex(1) == nullptr);
    auto remove_info2 = remove_info->RemoveVertex(vertex_info2);
    REQUIRE(!remove_info2.status().ok());
  }

  SECTION("AddEdge") {
    auto edge_info2 =
        CreateEdgeInfo("person", "knows2", "person", 1024, 100, 100, true,
                       {CreateAdjacentList(AdjListType::ordered_by_source,
                                           FileType::CSV, "adj_list/")},
                       {pg}, "test_edge/", version);
    auto maybe_extend_info = graph_info->AddEdge(edge_info2);
    REQUIRE(maybe_extend_info.status().ok());
    auto extend_info = maybe_extend_info.value();
    REQUIRE(extend_info->EdgeInfoNum() == 2);
    REQUIRE(extend_info->GetEdgeInfoByIndex(1)->GetEdgeType() == "knows2");
    REQUIRE(extend_info->GetEdgeInfoByIndex(2) == nullptr);
    REQUIRE(
        extend_info->GetEdgeInfo("person", "knows2", "person")->GetEdgeType() ==
        "knows2");
    REQUIRE(extend_info->GetEdgeInfo("not_exist", "knows2", "person") ==
            nullptr);
    REQUIRE(extend_info->GetEdgeInfos().size() == 2);
    REQUIRE(extend_info->GetEdgeInfos()[1]->GetEdgeType() == "knows2");
    auto extend_info2 = extend_info->AddEdge(edge_info2);
    REQUIRE(!extend_info2.status().ok());
  }

  SECTION("RemoveEdge") {
    auto edge_info2 =
        CreateEdgeInfo("person", "knows2", "person", 1024, 100, 100, true,
                       {CreateAdjacentList(AdjListType::ordered_by_source,
                                           FileType::CSV, "adj_list/")},
                       {pg}, "test_edge/", version);
    auto maybe_extend_info = graph_info->AddEdge(edge_info2);
    REQUIRE(maybe_extend_info.status().ok());
    auto extend_info = maybe_extend_info.value();
    auto maybe_remove_info = extend_info->RemoveEdge(edge_info2);
    REQUIRE(maybe_remove_info.status().ok());
    auto remove_info = maybe_remove_info.value();
    REQUIRE(remove_info->EdgeInfoNum() == 1);
    REQUIRE(remove_info->GetEdgeInfoByIndex(1) == nullptr);
    REQUIRE(remove_info->GetEdgeInfo("person", "knows2", "person") == nullptr);
    REQUIRE(remove_info->GetEdgeInfos().size() == 1);
    auto remove_info2 = remove_info->RemoveEdge(edge_info2);
    REQUIRE(!remove_info2.status().ok());
  }
}

TEST_CASE_METHOD(GlobalFixture, "LoadFromYaml") {
  std::string vertex_info_yaml = R"(type: person
chunk_size: 100
prefix: vertex/person/
property_groups:
  - properties:
      - name: id
        data_type: int64
        is_primary: true
        is_nullable: false
    file_type: parquet
  - properties:
      - name: email
        data_type: string
        is_primary: false
        is_nullable: true
        cardinality: list
    file_type: parquet
  - properties:
      - name: firstName
        data_type: string
        is_primary: false
        is_nullable: true
      - name: lastName
        data_type: string
        is_primary: false
        is_nullable: true
      - name: gender
        data_type: string
        is_primary: false
        is_nullable: true
    file_type: parquet
version: gar/v1
)";
  std::string edge_info_yaml = R"(src_type: person
edge_type: knows
dst_type: person
chunk_size: 1024
src_chunk_size: 100
dst_chunk_size: 100
directed: false
prefix: edge/person_knows_person/
adj_lists:
  - ordered: false
    aligned_by: src
    file_type: parquet
  - ordered: true
    aligned_by: src
    file_type: parquet
  - ordered: true
    aligned_by: dst
    file_type: parquet
property_groups:
  - file_type: parquet
    properties:
      - name: creationDate
        data_type: string
        is_primary: false
        is_nullable: true
version: gar/v1
)";
  std::string graph_info_yaml = R"(name: ldbc_sample
prefix: /tmp/ldbc/
version: gar/v1
extra_info:
  - key: category
    value: test graph
)";

  SECTION("VertexInfo::Load") {
    auto maybe_vertex_info = VertexInfo::Load(vertex_info_yaml);
    REQUIRE(!maybe_vertex_info.has_error());
    auto vertex_info = maybe_vertex_info.value();
    REQUIRE(vertex_info->GetType() == "person");
    REQUIRE(vertex_info->GetChunkSize() == 100);
    REQUIRE(vertex_info->GetPrefix() == "vertex/person/");
    REQUIRE(vertex_info->version()->ToString() == "gar/v1");
    REQUIRE(vertex_info->GetPropertyCardinality("id").value() ==
            Cardinality::SINGLE);
    REQUIRE(vertex_info->GetPropertyCardinality("email").value() ==
            Cardinality::LIST);
  }

  SECTION("EdgeInfo::Load") {
    auto maybe_edge_info = EdgeInfo::Load(edge_info_yaml);
    REQUIRE(!maybe_edge_info.has_error());
    auto edge_info = maybe_edge_info.value();
    REQUIRE(edge_info->GetSrcType() == "person");
    REQUIRE(edge_info->GetEdgeType() == "knows");
    REQUIRE(edge_info->GetDstType() == "person");
  }

  edge_info_yaml = R"(src_type: person
edge_type: knows
dst_type: person
chunk_size: 1024
src_chunk_size: 100
dst_chunk_size: 100
directed: false
prefix: edge/person_knows_person/
adj_lists:
  - ordered: false
    aligned_by: src
    file_type: parquet
property_groups:
  - file_type: parquet
    properties:
      - name: creationDate
        data_type: string
        is_primary: false
        is_nullable: true
        cardinality: list
version: gar/v1
)";

  SECTION("EdgeInfo::Load with cardinality") {
    auto maybe_edge_info = EdgeInfo::Load(edge_info_yaml);
    REQUIRE(maybe_edge_info.has_error());
    REQUIRE(maybe_edge_info.status().code() == StatusCode::kYamlError);
    std::cout << maybe_edge_info.status().message() << std::endl;
  }

  SECTION("GraphInfo::Load") {
    auto maybe_graph_info = GraphInfo::Load(graph_info_yaml, "/");
    std::cout << maybe_graph_info.status().message() << std::endl;
    REQUIRE(!maybe_graph_info.has_error());
    auto graph_info = maybe_graph_info.value();
    REQUIRE(graph_info->GetName() == "ldbc_sample");
    REQUIRE(graph_info->GetPrefix() == "/tmp/ldbc/");
    REQUIRE(graph_info->version()->ToString() == "gar/v1");
    const auto& extra_info = graph_info->GetExtraInfo();
    REQUIRE(extra_info.size() == 1);
    REQUIRE(extra_info.find("category") != extra_info.end());
    REQUIRE(extra_info.at("category") == "test graph");
  }
}

/*
TODO(acezen): need to mock S3 server to test this case, this private
service is not available for public access.
*/
// TEST_CASE_METHOD(GlobalFixture, "LoadFromS3") {
//   // explicitly call InitS3 to initialize S3 APIs before using
//   // S3 file system.
//   InitializeS3();
//   std::string path =
//       "s3://graphar/ldbc/ldbc.graph.yml"
//       "?endpoint_override=graphscope.oss-cn-beijing.aliyuncs.com";
//   auto graph_info_result = GraphInfo::Load(path);
//   std::cout << graph_info_result.status().message() << std::endl;
//   REQUIRE(!graph_info_result.has_error());
//   auto graph_info = graph_info_result.value();
//   REQUIRE(graph_info->GetName() == "ldbc");
//   const auto& vertex_infos = graph_info->GetVertexInfos();
//   const auto& edge_infos = graph_info->GetEdgeInfos();
//   REQUIRE(vertex_infos.size() == 8);
//   REQUIRE(edge_infos.size() == 23);
//   // explicitly call FinalizeS3 to avoid memory leak
//   FinalizeS3();
// }

}  // namespace graphar
