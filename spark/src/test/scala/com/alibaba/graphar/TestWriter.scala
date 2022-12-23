package com.alibaba.graphar

import com.alibaba.graphar.utils.IndexGenerator
import com.alibaba.graphar.writer.{VertexWriter, EdgeWriter}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

class WriterSuite extends AnyFunSuite {
  val spark = SparkSession.builder()
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  test("test vertex writer") {
    val file_path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/person_0_0.csv").getPath
    val vertex_df = spark.read.option("delimiter", "|").option("header", "true").csv(file_path)

    val graph_input = getClass.getClassLoader.getResourceAsStream("gar-test/ldbc_sample/csv/ldbc_sample.graph.yml")
    val graph_yaml = new Yaml(new Constructor(classOf[GraphInfo]))
    val graph_info = graph_yaml.load(graph_input).asInstanceOf[GraphInfo]

    val vertex_input = getClass.getClassLoader.getResourceAsStream("gar-test/ldbc_sample/csv/person.vertex.yml")
    val vertex_yaml = new Yaml(new Constructor(classOf[VertexInfo]))
    val vertex_info = vertex_yaml.load(vertex_input).asInstanceOf[VertexInfo]

    val writer = new VertexWriter(graph_info.getPrefix(), vertex_info, vertex_df)
    writer.writeVertexProperties()
  }

  test("test edge writer") {
    val file_path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/person_knows_person_0_0.csv").getPath
    val edge_df = spark.read.option("delimiter", "|").option("header", "true").csv(file_path)

    val graph_input = getClass.getClassLoader.getResourceAsStream("gar-test/ldbc_sample/csv/ldbc_sample.graph.yml")
    val graph_yaml = new Yaml(new Constructor(classOf[GraphInfo]))
    val graph_info = graph_yaml.load(graph_input).asInstanceOf[GraphInfo]

    val edge_input = getClass.getClassLoader.getResourceAsStream("gar-test/ldbc_sample/csv/person_knows_person.edge.yml")
    val edge_yaml = new Yaml(new Constructor(classOf[EdgeInfo]))
    val edge_info = edge_yaml.load(edge_input).asInstanceOf[EdgeInfo]

    val edge_df_with_index = IndexGenerator.generateSrcAndDstIndexUnitedlyForEdges(edge_df, "src", "dst")
    val writer = new EdgeWriter(graph_info.getPrefix(), edge_info, AdjListType.ordered_by_source, edge_df_with_index)
    writer.writeEdges()

  }
}
