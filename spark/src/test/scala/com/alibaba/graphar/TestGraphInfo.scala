package org.alibaba.graphar

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

  
}
