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
    assert(graph_info.getPrefix == "" )
    assert(graph_info.getVertices.size() == 1)
    assert(graph_info.getEdges.size() == 1)
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
  }

}
