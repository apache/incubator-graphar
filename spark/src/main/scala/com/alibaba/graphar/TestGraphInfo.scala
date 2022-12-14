package org.alibaba.graphar

import java.io.{File, FileInputStream}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import scala.beans.BeanProperty

object TestGraphInfo {
  def testGraph(): Unit = {
    println("----Test GraphInfo----")

    val filename = "/Users/lixue/Downloads/GraphInfo.yaml"
    val input = new FileInputStream(new File(filename))
    val yaml = new Yaml(new Constructor(classOf[GraphInfo]))
    val graph_info = yaml.load(input).asInstanceOf[GraphInfo]

    println(graph_info.getVertices)
    println(graph_info.getEdges)
  }

  def testVertex(): Unit = {
    println("----Test VertexInfo----")

    val filename = "/Users/lixue/Downloads/VertexInfo.yaml"
    val input = new FileInputStream(new File(filename))
    val yaml = new Yaml(new Constructor(classOf[VertexInfo]))
    val vertex_info = yaml.load(input).asInstanceOf[VertexInfo]

    println(vertex_info.getLabel)
    println(vertex_info.isValidated)
    println(vertex_info.getVerticesNumFilePath())
    println(vertex_info.getPrimaryKey())

    for ( i <- 0 to (vertex_info.getProperty_groups.size - 1) ) {
      val property_group = vertex_info.getProperty_groups.get(i)
      println(vertex_info.containPropertyGroup(property_group), property_group.getFile_type_in_gar)
      println(vertex_info.getFilePath(property_group, 0))
      for (j <- 0 to (property_group.getProperties.size - 1) ) {
        val property = property_group.getProperties.get(j)
        println(property.getName, property.getData_type_in_gar, property.getIs_primary)
      }
    }

    println(vertex_info.getPropertyType("id"), vertex_info.isPrimaryKey("id"), vertex_info.getPropertyGroup("id").getPrefix)
    println(vertex_info.getPropertyType("firstName"), vertex_info.isPrimaryKey("firstName"), vertex_info.getPropertyGroup("firstName").getPrefix)
  }

  def testEdge(): Unit = {
    println("----Test EdgeInfo----")

    val filename = "/Users/lixue/Downloads/EdgeInfo.yaml"
    val input = new FileInputStream(new File(filename))
    val yaml = new Yaml(new Constructor(classOf[EdgeInfo]))
    val edge_info = yaml.load(input).asInstanceOf[EdgeInfo]

    println(edge_info.getChunk_size)
    println(edge_info.isValidated)
    println(edge_info.getPrimaryKey)

    for ( i <- 0 to (edge_info.getAdj_lists.size - 1) ) {
      val adj_list = edge_info.getAdj_lists.get(i)
      println(adj_list.getAdjList_type_in_gar, adj_list.getProperty_groups.size)
      for (j <- 0 to (adj_list.getProperty_groups.size - 1) ) {
        val property_group = adj_list.getProperty_groups.get(j)
        println(edge_info.containPropertyGroup(property_group, adj_list.getAdjList_type_in_gar), edge_info.getPropertyFilePath(property_group, adj_list.getAdjList_type_in_gar, 0, 100))
      }
    }

    println(edge_info.containProperty("creationDate"), edge_info.containProperty("id"))
    println(edge_info.getPropertyType("creationDate"), edge_info.isPrimaryKey("creationDate"))
    println(edge_info.containAdjList(AdjListType.ordered_by_source), edge_info.containAdjList(AdjListType.unordered_by_dest))
    println(edge_info.getAdjListPrefix(AdjListType.ordered_by_source), edge_info.getAdjListFileType(AdjListType.ordered_by_source))
    println(edge_info.getPropertyGroups(AdjListType.unordered_by_source).size)
    println(edge_info.getPropertyGroup("creationDate", AdjListType.unordered_by_source).getPrefix)
    println(edge_info.getAdjListOffsetFilePath(0, AdjListType.unordered_by_source))
    println(edge_info.getAdjListFilePath(0, 100, AdjListType.ordered_by_source))
  }

  def main(args: Array[String]): Unit = {
    testGraph()
    testVertex()
    testEdge()
  }
}


