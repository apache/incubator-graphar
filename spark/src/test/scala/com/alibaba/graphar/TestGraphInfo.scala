package com.alibaba.graphar

import java.io.{File, FileInputStream}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import scala.beans.BeanProperty
import org.scalatest.funsuite.AnyFunSuite

class GraphInfoSuite extends AnyFunSuite {

  test("load graph info") {
    val input = getClass.getClassLoader.getResourceAsStream("ldbc_sample.graph.yml")
    val yaml = new Yaml(new Constructor(classOf[GraphInfo]))
    val graph_info = yaml.load(input).asInstanceOf[GraphInfo]

    assert(graph_info.getName == "ldbc_sample")
    assert(graph_info.getPrefix == "./" )
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
    val input = getClass.getClassLoader.getResourceAsStream("person.vertex.yml")
    val yaml = new Yaml(new Constructor(classOf[VertexInfo]))
    val vertex_info = yaml.load(input).asInstanceOf[VertexInfo]

    assert(vertex_info.getLabel == "person")
    assert(vertex_info.isValidated)
    assert(vertex_info.getVerticesNumFilePath() == "vertex/person/vertex_count")
    assert(vertex_info.getPrimaryKey() == "id")
    assert(vertex_info.getProperty_groups.size() == 2)
    assert(vertex_info.getVersion == "gar/v1")
    assert(vertex_info.getChunk_size == 100)

    for ( i <- 0 to (vertex_info.getProperty_groups.size - 1) ) {
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
    assert(vertex_info.getFilePath(property_group, 0) == "vertex/person/id/part0/chunk0")
    assert(vertex_info.getFilePath(property_group, 4) == "vertex/person/id/part4/chunk0")

    assert(vertex_info.containProperty("firstName"))
    val property_group_2 = vertex_info.getPropertyGroup("firstName")
    assert(property_group_2.getProperties.size == 3)
    assert(property_group_2.getFile_type == "csv")
    assert(property_group_2.getFile_type_in_gar == FileType.CSV)
    assert(vertex_info.containPropertyGroup(property_group_2))
    assert(vertex_info.getPropertyType("firstName") == GarType.STRING)
    assert(vertex_info.isPrimaryKey("firstName") == false)
    assert(vertex_info.getFilePath(property_group_2, 0) == "vertex/person/firstName_lastName_gender/part0/chunk0")
    assert(vertex_info.getFilePath(property_group_2, 4) == "vertex/person/firstName_lastName_gender/part4/chunk0")

    assert(vertex_info.containProperty("not_exist") == false)
    assertThrows[IllegalArgumentException](vertex_info.getPropertyGroup("not_exist"))
    assertThrows[IllegalArgumentException](vertex_info.getPropertyType("now_exist"))
    assertThrows[IllegalArgumentException](vertex_info.isPrimaryKey("now_exist"))
  }

  test("load edge info") {
    val input = getClass.getClassLoader.getResourceAsStream("person_knows_person.edge.yml")
    val yaml = new Yaml(new Constructor(classOf[EdgeInfo]))
    val edge_info = yaml.load(input).asInstanceOf[EdgeInfo]

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
    assert(edge_info.getAdjListPrefix(AdjListType.ordered_by_source) == "ordered_by_source/")
    assert(edge_info.getAdjListFileType(AdjListType.ordered_by_source) == FileType.CSV)
    assert(edge_info.getPropertyGroups(AdjListType.ordered_by_source).size == 1)
    assert(edge_info.getAdjListFilePath(0, 0, AdjListType.ordered_by_source) == "edge/person_knows_person/ordered_by_source/adj_list/part0/chunk0")
    assert(edge_info.getAdjListFilePath(1, 2, AdjListType.ordered_by_source) == "edge/person_knows_person/ordered_by_source/adj_list/part1/chunk2")
    assert(edge_info.getAdjListOffsetFilePath(0, AdjListType.ordered_by_source) == "edge/person_knows_person/ordered_by_source/offset/part0/chunk0")
    assert(edge_info.getAdjListOffsetFilePath(4, AdjListType.ordered_by_source) == "edge/person_knows_person/ordered_by_source/offset/part4/chunk0")
    val property_group = edge_info.getPropertyGroups(AdjListType.ordered_by_source).get(0)
    assert(edge_info.containPropertyGroup(property_group, AdjListType.ordered_by_source))
    val property = property_group.getProperties.get(0)
    val property_name = property.getName
    assert(edge_info.containProperty(property_name))
    assert(edge_info.getPropertyGroup(property_name, AdjListType.ordered_by_source) == property_group)
    assert(edge_info.getPropertyType(property_name) == property.getData_type_in_gar)
    assert(edge_info.isPrimaryKey(property_name) == property.getIs_primary)
    assert(edge_info.getPropertyFilePath(property_group, AdjListType.ordered_by_source, 0, 0) == "edge/person_knows_person/ordered_by_source/creationDate/part0/chunk0")
    assert(edge_info.getPropertyFilePath(property_group, AdjListType.ordered_by_source, 1, 2) == "edge/person_knows_person/ordered_by_source/creationDate/part1/chunk2")

    assert(edge_info.containAdjList(AdjListType.ordered_by_dest))
    assert(edge_info.getAdjListPrefix(AdjListType.ordered_by_dest) == "ordered_by_dest/")
    assert(edge_info.getAdjListFileType(AdjListType.ordered_by_dest) == FileType.CSV)
    assert(edge_info.getPropertyGroups(AdjListType.ordered_by_dest).size == 1)
    assert(edge_info.getAdjListFilePath(0, 0, AdjListType.ordered_by_dest) == "edge/person_knows_person/ordered_by_dest/adj_list/part0/chunk0")
    assert(edge_info.getAdjListFilePath(1, 2, AdjListType.ordered_by_dest) == "edge/person_knows_person/ordered_by_dest/adj_list/part1/chunk2")
    assert(edge_info.getAdjListOffsetFilePath(0, AdjListType.ordered_by_dest) == "edge/person_knows_person/ordered_by_dest/offset/part0/chunk0")
    assert(edge_info.getAdjListOffsetFilePath(4, AdjListType.ordered_by_dest) == "edge/person_knows_person/ordered_by_dest/offset/part4/chunk0")
    val property_group_2 = edge_info.getPropertyGroups(AdjListType.ordered_by_dest).get(0)
    assert(edge_info.containPropertyGroup(property_group_2, AdjListType.ordered_by_dest))
    val property_2 = property_group_2.getProperties.get(0)
    val property_name_2 = property_2.getName
    assert(edge_info.containProperty(property_name_2))
    assert(edge_info.getPropertyGroup(property_name_2, AdjListType.ordered_by_dest) == property_group_2)
    assert(edge_info.getPropertyType(property_name_2) == property_2.getData_type_in_gar)
    assert(edge_info.isPrimaryKey(property_name_2) == property_2.getIs_primary)
    assert(edge_info.getPropertyFilePath(property_group_2, AdjListType.ordered_by_dest, 0, 0) == "edge/person_knows_person/ordered_by_dest/creationDate/part0/chunk0")
    assert(edge_info.getPropertyFilePath(property_group_2, AdjListType.ordered_by_dest, 1, 2) == "edge/person_knows_person/ordered_by_dest/creationDate/part1/chunk2")

    assert(edge_info.containAdjList(AdjListType.unordered_by_source) == false)
    assertThrows[IllegalArgumentException](edge_info.getAdjListPrefix(AdjListType.unordered_by_source))
    assertThrows[IllegalArgumentException](edge_info.getAdjListFileType(AdjListType.unordered_by_source))
    assertThrows[IllegalArgumentException](edge_info.getPropertyGroups(AdjListType.unordered_by_source))
    assert(edge_info.containPropertyGroup(property_group, AdjListType.unordered_by_source) == false)
    assertThrows[IllegalArgumentException](edge_info.getAdjListOffsetFilePath(0, AdjListType.unordered_by_source))
    assertThrows[IllegalArgumentException](edge_info.getAdjListFilePath(0, 0, AdjListType.unordered_by_source))
    assertThrows[IllegalArgumentException](edge_info.getPropertyFilePath(property_group, AdjListType.unordered_by_source, 0, 0))
    assert(edge_info.containProperty("not_exist") == false)
    assertThrows[IllegalArgumentException](edge_info.getPropertyGroup("not_exist", AdjListType.ordered_by_source))
    assertThrows[IllegalArgumentException](edge_info.getPropertyType("not_exist"))
    assertThrows[IllegalArgumentException](edge_info.isPrimaryKey("not_exist"))
  }

  }
