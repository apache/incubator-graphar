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

package com.alibaba.graphar

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

class GraphInfoSuite extends AnyFunSuite {
  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  test("load graph info") {
    // read graph yaml
    val yaml_path = getClass.getClassLoader
      .getResource("gar-test/new/ldbc_sample/csv/ldbc_sample.graph.yml")
      .getPath
    val prefix =
      getClass.getClassLoader
        .getResource("gar-test/new/ldbc_sample/csv/")
        .getPath
    val graph_info = GraphInfo.loadGraphInfo(yaml_path, spark)

    val vertex_info = graph_info.getVertexInfo("person")
    assert(vertex_info.getLabel == "person")

    assert(graph_info.getName == "ldbc_sample")
    assert(graph_info.getPrefix == prefix)
    assert(graph_info.getVertices.size() == 1)
    val vertices = new java.util.ArrayList[String]
    vertices.add("person.vertex.yml")
    assert(graph_info.getVertices == vertices)
    assert(graph_info.getEdges.size() == 1)
    val edges = new java.util.ArrayList[String]
    edges.add("person_knows_person.edge.yml")
    assert(graph_info.getEdges == edges)
    assert(graph_info.getVersion == "gar/v1")
  }

  test("load vertex info") {
    val yaml_path = getClass.getClassLoader
      .getResource("gar-test/ldbc_sample/csv/person.vertex.yml")
      .getPath
    val vertex_info = VertexInfo.loadVertexInfo(yaml_path, spark)

    assert(vertex_info.getLabel == "person")
    assert(vertex_info.isValidated)
    assert(vertex_info.getVerticesNumFilePath() == "vertex/person/vertex_count")
    assert(vertex_info.getPrimaryKey() == "id")
    assert(vertex_info.getProperty_groups.size() == 2)
    assert(vertex_info.getVersion == "gar/v1")
    assert(vertex_info.getChunk_size == 100)

    for (i <- 0 to (vertex_info.getProperty_groups.size - 1)) {
      val property_group = vertex_info.getProperty_groups.get(i)
      assert(vertex_info.containPropertyGroup(property_group))
    }
    assert(vertex_info.containProperty("id"))
    val property_group = vertex_info.getPropertyGroup("id")
    assert(property_group.getProperties.size == 1)
    assert(property_group.getFile_type == "csv")
    assert(property_group.getFile_type_in_gar == FileType.CSV)
    assert(vertex_info.containPropertyGroup(property_group))
    assert(vertex_info.getPropertyType("id") == GarType.INT64)
    assert(vertex_info.isPrimaryKey("id"))
    assert(
      vertex_info.getFilePath(property_group, 0) == "vertex/person/id/chunk0"
    )
    assert(
      vertex_info.getFilePath(property_group, 4) == "vertex/person/id/chunk4"
    )
    assert(vertex_info.getPathPrefix(property_group) == "vertex/person/id/")

    assert(vertex_info.containProperty("firstName"))
    val property_group_2 = vertex_info.getPropertyGroup("firstName")
    assert(property_group_2.getProperties.size == 3)
    assert(property_group_2.getFile_type == "csv")
    assert(property_group_2.getFile_type_in_gar == FileType.CSV)
    assert(vertex_info.containPropertyGroup(property_group_2))
    assert(vertex_info.getPropertyType("firstName") == GarType.STRING)
    assert(vertex_info.isPrimaryKey("firstName") == false)
    assert(
      vertex_info.getFilePath(
        property_group_2,
        0
      ) == "vertex/person/firstName_lastName_gender/chunk0"
    )
    assert(
      vertex_info.getFilePath(
        property_group_2,
        4
      ) == "vertex/person/firstName_lastName_gender/chunk4"
    )
    assert(
      vertex_info.getPathPrefix(
        property_group_2
      ) == "vertex/person/firstName_lastName_gender/"
    )

    assert(vertex_info.containProperty("not_exist") == false)
    assertThrows[IllegalArgumentException](
      vertex_info.getPropertyGroup("not_exist")
    )
    assertThrows[IllegalArgumentException](
      vertex_info.getPropertyType("now_exist")
    )
    assertThrows[IllegalArgumentException](
      vertex_info.isPrimaryKey("now_exist")
    )
  }

  test("load edge info") {
    val yaml_path = getClass.getClassLoader
      .getResource("gar-test/ldbc_sample/csv/person_knows_person.edge.yml")
      .getPath
    val edge_info = EdgeInfo.loadEdgeInfo(yaml_path, spark)

    assert(edge_info.getSrc_label == "person")
    assert(edge_info.getDst_label == "person")
    assert(edge_info.getEdge_label == "knows")
    assert(edge_info.getChunk_size == 1024)
    assert(edge_info.getSrc_chunk_size == 100)
    assert(edge_info.getDst_chunk_size == 100)
    assert(edge_info.getDirected == false)
    assert(edge_info.getPrefix == "edge/person_knows_person/")
    assert(edge_info.getAdj_lists.size == 2)
    assert(edge_info.getVersion == "gar/v1")
    assert(edge_info.isValidated)
    assert(edge_info.getPrimaryKey == "")

    assert(edge_info.containAdjList(AdjListType.ordered_by_source))
    assert(
      edge_info.getAdjListPrefix(
        AdjListType.ordered_by_source
      ) == "ordered_by_source/"
    )
    assert(
      edge_info.getAdjListFileType(
        AdjListType.ordered_by_source
      ) == FileType.CSV
    )
    assert(edge_info.getProperty_groups().size == 1)
    assert(
      edge_info.getVerticesNumFilePath(
        AdjListType.ordered_by_source
      ) == "edge/person_knows_person/ordered_by_source/vertex_count"
    )
    assert(
      edge_info.getEdgesNumFilePath(
        0,
        AdjListType.ordered_by_source
      ) == "edge/person_knows_person/ordered_by_source/edge_count0"
    )
    assert(
      edge_info.getEdgesNumPathPrefix(
        AdjListType.ordered_by_source
      ) == "edge/person_knows_person/ordered_by_source/edge_count"
    )
    assert(
      edge_info.getAdjListFilePath(
        0,
        0,
        AdjListType.ordered_by_source
      ) == "edge/person_knows_person/ordered_by_source/adj_list/part0/chunk0"
    )
    assert(
      edge_info.getAdjListPathPrefix(
        0,
        AdjListType.ordered_by_source
      ) == "edge/person_knows_person/ordered_by_source/adj_list/part0/"
    )
    assert(
      edge_info.getAdjListFilePath(
        1,
        2,
        AdjListType.ordered_by_source
      ) == "edge/person_knows_person/ordered_by_source/adj_list/part1/chunk2"
    )
    assert(
      edge_info.getAdjListPathPrefix(
        1,
        AdjListType.ordered_by_source
      ) == "edge/person_knows_person/ordered_by_source/adj_list/part1/"
    )
    assert(
      edge_info.getAdjListPathPrefix(
        AdjListType.ordered_by_source
      ) == "edge/person_knows_person/ordered_by_source/adj_list/"
    )
    assert(
      edge_info.getAdjListOffsetFilePath(
        0,
        AdjListType.ordered_by_source
      ) == "edge/person_knows_person/ordered_by_source/offset/chunk0"
    )
    assert(
      edge_info.getAdjListOffsetFilePath(
        4,
        AdjListType.ordered_by_source
      ) == "edge/person_knows_person/ordered_by_source/offset/chunk4"
    )
    assert(
      edge_info.getOffsetPathPrefix(
        AdjListType.ordered_by_source
      ) == "edge/person_knows_person/ordered_by_source/offset/"
    )
    val property_group =
      edge_info.getProperty_groups().get(0)
    assert(
      edge_info.containPropertyGroup(
        property_group
      )
    )
    val property = property_group.getProperties.get(0)
    val property_name = property.getName
    assert(edge_info.containProperty(property_name))
    assert(
      edge_info.getPropertyGroup(
        property_name
      ) == property_group
    )
    assert(
      edge_info.getPropertyType(property_name) == property.getData_type_in_gar
    )
    assert(edge_info.isPrimaryKey(property_name) == property.getIs_primary)
    assert(
      edge_info.getPropertyFilePath(
        property_group,
        AdjListType.ordered_by_source,
        0,
        0
      ) == "edge/person_knows_person/ordered_by_source/creationDate/part0/chunk0"
    )
    assert(
      edge_info.getPropertyFilePath(
        property_group,
        AdjListType.ordered_by_source,
        1,
        2
      ) == "edge/person_knows_person/ordered_by_source/creationDate/part1/chunk2"
    )
    assert(
      edge_info.getPropertyGroupPathPrefix(
        property_group,
        AdjListType.ordered_by_source
      ) == "edge/person_knows_person/ordered_by_source/creationDate/"
    )

    assert(edge_info.containAdjList(AdjListType.ordered_by_dest))
    assert(
      edge_info.getAdjListPrefix(
        AdjListType.ordered_by_dest
      ) == "ordered_by_dest/"
    )
    assert(
      edge_info.getAdjListFileType(AdjListType.ordered_by_dest) == FileType.CSV
    )
    assert(edge_info.getProperty_groups().size == 1)
    assert(
      edge_info.getAdjListFilePath(
        0,
        0,
        AdjListType.ordered_by_dest
      ) == "edge/person_knows_person/ordered_by_dest/adj_list/part0/chunk0"
    )
    assert(
      edge_info.getAdjListPathPrefix(
        0,
        AdjListType.ordered_by_dest
      ) == "edge/person_knows_person/ordered_by_dest/adj_list/part0/"
    )
    assert(
      edge_info.getAdjListFilePath(
        1,
        2,
        AdjListType.ordered_by_dest
      ) == "edge/person_knows_person/ordered_by_dest/adj_list/part1/chunk2"
    )
    assert(
      edge_info.getAdjListPathPrefix(
        1,
        AdjListType.ordered_by_dest
      ) == "edge/person_knows_person/ordered_by_dest/adj_list/part1/"
    )
    assert(
      edge_info.getAdjListPathPrefix(
        AdjListType.ordered_by_dest
      ) == "edge/person_knows_person/ordered_by_dest/adj_list/"
    )
    assert(
      edge_info.getAdjListOffsetFilePath(
        0,
        AdjListType.ordered_by_dest
      ) == "edge/person_knows_person/ordered_by_dest/offset/chunk0"
    )
    assert(
      edge_info.getAdjListOffsetFilePath(
        4,
        AdjListType.ordered_by_dest
      ) == "edge/person_knows_person/ordered_by_dest/offset/chunk4"
    )
    assert(
      edge_info.getOffsetPathPrefix(
        AdjListType.ordered_by_dest
      ) == "edge/person_knows_person/ordered_by_dest/offset/"
    )
    val property_group_2 =
      edge_info.getProperty_groups().get(0)
    assert(
      edge_info.containPropertyGroup(
        property_group_2
      )
    )
    val property_2 = property_group_2.getProperties.get(0)
    val property_name_2 = property_2.getName
    assert(edge_info.containProperty(property_name_2))
    assert(
      edge_info.getPropertyGroup(
        property_name_2
      ) == property_group_2
    )
    assert(
      edge_info.getPropertyType(
        property_name_2
      ) == property_2.getData_type_in_gar
    )
    assert(edge_info.isPrimaryKey(property_name_2) == property_2.getIs_primary)
    assert(
      edge_info.getPropertyFilePath(
        property_group_2,
        AdjListType.ordered_by_dest,
        0,
        0
      ) == "edge/person_knows_person/ordered_by_dest/creationDate/part0/chunk0"
    )
    assert(
      edge_info.getPropertyFilePath(
        property_group_2,
        AdjListType.ordered_by_dest,
        1,
        2
      ) == "edge/person_knows_person/ordered_by_dest/creationDate/part1/chunk2"
    )
    assert(
      edge_info.getPropertyGroupPathPrefix(
        property_group_2,
        AdjListType.ordered_by_dest
      ) == "edge/person_knows_person/ordered_by_dest/creationDate/"
    )

    assert(edge_info.containAdjList(AdjListType.unordered_by_source) == false)
    assertThrows[IllegalArgumentException](
      edge_info.getAdjListPrefix(AdjListType.unordered_by_source)
    )
    assertThrows[IllegalArgumentException](
      edge_info.getAdjListFileType(AdjListType.unordered_by_source)
    )
    assert(
      edge_info.containPropertyGroup(
        property_group
      ) == false
    )
    assertThrows[IllegalArgumentException](
      edge_info.getVerticesNumFilePath(AdjListType.unordered_by_source)
    )
    assertThrows[IllegalArgumentException](
      edge_info.getEdgesNumFilePath(0, AdjListType.unordered_by_source)
    )
    assertThrows[IllegalArgumentException](
      edge_info.getEdgesNumPathPrefix(AdjListType.unordered_by_source)
    )
    assertThrows[IllegalArgumentException](
      edge_info.getAdjListOffsetFilePath(0, AdjListType.unordered_by_source)
    )
    assertThrows[IllegalArgumentException](
      edge_info.getAdjListFilePath(0, 0, AdjListType.unordered_by_source)
    )
    assertThrows[IllegalArgumentException](
      edge_info.getPropertyFilePath(
        property_group,
        AdjListType.unordered_by_source,
        0,
        0
      )
    )
    assert(edge_info.containProperty("not_exist") == false)
    assertThrows[IllegalArgumentException](
      edge_info.getPropertyGroup("not_exist")
    )
    assertThrows[IllegalArgumentException](
      edge_info.getPropertyType("not_exist")
    )
    assertThrows[IllegalArgumentException](edge_info.isPrimaryKey("not_exist"))
  }

  test("== of Property/PropertyGroup/AdjList") {
    val p1 = new Property()
    val p2 = new Property()
    assert(p1 == p2)
    p1.setName("name")
    assert(p1 != p2)
    p2.setName("name")
    assert(p1 == p2)
    p1.setData_type("INT64")
    assert(p1 != p2)
    p2.setData_type("INT64")
    assert(p1 == p2)
    val pg1 = new PropertyGroup()
    val pg2 = new PropertyGroup()
    assert(pg1 == pg2)
    pg1.setPrefix("/tmp")
    assert(pg1 != pg2)
    pg2.setPrefix("/tmp")
    assert(pg1 == pg2)
    pg1.setProperties(
      new java.util.ArrayList[Property](java.util.Arrays.asList(p1))
    )
    assert(pg1 != pg2)
    pg2.setProperties(
      new java.util.ArrayList[Property](java.util.Arrays.asList(p2))
    )
    assert(pg1 == pg2)
    val al1 = new AdjList()
    val al2 = new AdjList()
    assert(al1 == al2)
  }
}
