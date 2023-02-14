package com.alibaba.graphar

import com.alibaba.graphar.writer.{VertexWriter, EdgeWriter}
import com.alibaba.graphar.reader.{EdgeReader}
import com.alibaba.graphar.utils.IndexGenerator

import java.net.URI
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.fs

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

object TestOdps {
  def testVertexWriter(spark: SparkSession): Unit = {
    val path = "oss://weibin-transfer/graphar/person_0_0.csv"
    val vertex_df = spark.read.option("delimiter", "|").option("header", "true").csv(path)

    // val graph_input = getClass.getClassLoader.getResourceAsStream("gar-test/ldbc_sample/csv/ldbc_sample.graph.yml")
    val graph_path = new fs.Path("oss://weibin-transfer/graphar/ldbc_sample/csv/ldbc_sample.graph.yml")
    val file_system = fs.FileSystem.get(graph_path.toUri(), spark.sparkContext.hadoopConfiguration)
    // val graph_path = new fs.Path("/Users/weibin/Dev/GraphAr/test/gar-test/ldbc_sample/csv/ldbc_sample.graph.yml")
    // val graph_input = new URL("oss://weibin-transfer/graphar/ldbc_sample/csv/ldbc_sample.graph.yml").openStream()
    val graph_input = file_system.open(graph_path)
    val graph_yaml = new Yaml(new Constructor(classOf[GraphInfo]))
    val graph_info = graph_yaml.load(graph_input).asInstanceOf[GraphInfo]

    val vertex_path = new fs.Path("oss://weibin-transfer/graphar/ldbc_sample/csv/person.vertex.yml")
    // val vertex_input = new URL("oss://weibin-transfer/graphar/ldbc_sample/csv/person.vertex.yml").openStream()
    // val vertex_path = new fs.Path("/Users/weibin/Dev/GraphAr/test/gar-test/ldbc_sample/csv/person.vertex.yml")
    // val vertex_input = getClass.getClassLoader.getResourceAsStream("gar-test/ldbc_sample/csv/person.vertex.yml")
    val vertex_input = file_system.open(vertex_path)
    val vertex_yaml = new Yaml(new Constructor(classOf[VertexInfo]))
    val vertex_info = vertex_yaml.load(vertex_input).asInstanceOf[VertexInfo]

    val writer = new VertexWriter(graph_info.getPrefix(), vertex_info, vertex_df)
    writer.writeVertexProperties()
  }

  def testOdpsWriter(spark: SparkSession): Unit = {
    // val path = "oss://weibin-transfer/graphar/person_0_0.csv"
    // val vertex_df = spark.read.option("delimiter", "|").option("header", "true").csv(path)
    val vertex_df = spark.table("com_friendster_str_v")

    // val graph_input = getClass.getClassLoader.getResourceAsStream("gar-test/ldbc_sample/csv/ldbc_sample.graph.yml")
    val graph_path = new fs.Path("oss://weibin-internal/graphar/cf/cf.graph.yml")
    val file_system = fs.FileSystem.get(graph_path.toUri(), spark.sparkContext.hadoopConfiguration)
    // val graph_path = new fs.Path("/Users/weibin/Dev/GraphAr/test/gar-test/ldbc_sample/csv/ldbc_sample.graph.yml")
    // val graph_input = new URL("oss://weibin-transfer/graphar/ldbc_sample/csv/ldbc_sample.graph.yml").openStream()
    val graph_input = file_system.open(graph_path)
    val graph_yaml = new Yaml(new Constructor(classOf[GraphInfo]))
    val graph_info = graph_yaml.load(graph_input).asInstanceOf[GraphInfo]

    val vertex_path = new fs.Path("oss://weibin-internal/graphar/cf/person.vertex.yml")
    // val vertex_input = new URL("oss://weibin-transfer/graphar/ldbc_sample/csv/person.vertex.yml").openStream()
    // val vertex_path = new fs.Path("/Users/weibin/Dev/GraphAr/test/gar-test/ldbc_sample/csv/person.vertex.yml")
    // val vertex_input = getClass.getClassLoader.getResourceAsStream("gar-test/ldbc_sample/csv/person.vertex.yml")
    val vertex_input = file_system.open(vertex_path)
    val vertex_yaml = new Yaml(new Constructor(classOf[VertexInfo]))
    val vertex_info = vertex_yaml.load(vertex_input).asInstanceOf[VertexInfo]

    val writer = new VertexWriter(graph_info.getPrefix(), vertex_info, vertex_df)
    writer.writeVertexProperties()
  }

  def testOdpsCfWriter(spark: SparkSession): Unit = {
    // val path = "oss://weibin-transfer/graphar/person_0_0.csv"
    // val vertex_df = spark.read.option("delimiter", "|").option("header", "true").csv(path)
    val edge_df = spark.table("com_friendster_e")

    val edge_path = new fs.Path("oss://weibin-internal/graphar/cf/person_knows_person.edge.yml")
    // val vertex_input = new URL("oss://weibin-transfer/graphar/ldbc_sample/csv/person.vertex.yml").openStream()
    // val vertex_path = new fs.Path("/Users/weibin/Dev/GraphAr/test/gar-test/ldbc_sample/csv/person.vertex.yml")
    // val vertex_input = getClass.getClassLoader.getResourceAsStream("gar-test/ldbc_sample/csv/person.vertex.yml")
    val file_system = fs.FileSystem.get(edge_path.toUri(), spark.sparkContext.hadoopConfiguration)
    val input = file_system.open(edge_path)
    val edge_yaml = new Yaml(new Constructor(classOf[EdgeInfo]))
    val edge_info = edge_yaml.load(input).asInstanceOf[EdgeInfo]
    val adj_list_type = AdjListType.ordered_by_source

    val edge_df_with_index = IndexGenerator.generateSrcAndDstIndexUnitedlyForEdges(edge_df, "src", "dst")
    val writer = new EdgeWriter("oss://weibin-internal/graphar/cf_nooffset/", edge_info, adj_list_type, edge_df_with_index)
    writer.writeEdges()
  }

  def testOdpsEdgeWriter(spark: SparkSession): Unit = {
    // val path = "oss://weibin-transfer/graphar/person_0_0.csv"
    // val vertex_df = spark.read.option("delimiter", "|").option("header", "true").csv(path)
    // val edge_df = spark.table("com_friendster_str_e")
    // val partiton_df = spark.sql("select * from ads_social_enchance_pseudo_inactive_feature_mini_train")
    val partiton_df = spark.sql("select * from p2p_v_partition where part='latest'")
    val drop_df = partiton_df.drop("part")

    println(drop_df.show())

    /*
    // val graph_input = getClass.getClassLoader.getResourceAsStream("gar-test/ldbc_sample/csv/ldbc_sample.graph.yml")
    val graph_path = new fs.Path("oss://weibin-internal/graphar/cf/cf.graph.yml")
    val file_system = fs.FileSystem.get(graph_path.toUri(), spark.sparkContext.hadoopConfiguration)
    // val graph_path = new fs.Path("/Users/weibin/Dev/GraphAr/test/gar-test/ldbc_sample/csv/ldbc_sample.graph.yml")
    // val graph_input = new URL("oss://weibin-transfer/graphar/ldbc_sample/csv/ldbc_sample.graph.yml").openStream()
    val graph_input = file_system.open(graph_path)
    val graph_yaml = new Yaml(new Constructor(classOf[GraphInfo]))
    val graph_info = graph_yaml.load(graph_input).asInstanceOf[GraphInfo]

    val edge_path = new fs.Path("oss://weibin-internal/graphar/cf/person_knows_person.edge.yml")
    // val vertex_input = new URL("oss://weibin-transfer/graphar/ldbc_sample/csv/person.vertex.yml").openStream()
    // val vertex_path = new fs.Path("/Users/weibin/Dev/GraphAr/test/gar-test/ldbc_sample/csv/person.vertex.yml")
    // val vertex_input = getClass.getClassLoader.getResourceAsStream("gar-test/ldbc_sample/csv/person.vertex.yml")
    val edge_input = file_system.open(edge_path)
    val edge_yaml = new Yaml(new Constructor(classOf[EdgeInfo]))
    val edge_info = edge_yaml.load(edge_input).asInstanceOf[EdgeInfo]

    val edge_df_with_index = IndexGenerator.generateSrcAndDstIndexUnitedlyForEdges(edge_df, "src", "dst")

    val writer = new EdgeWriter(graph_info.getPrefix(), edge_info, AdjListType.ordered_by_source, edge_df_with_index)
    writer.writeEdges()
    */
  }

  def testOdpsCfReader(spark: SparkSession): Unit = {
    // val path = "oss://weibin-transfer/graphar/person_0_0.csv"
    // val vertex_df = spark.read.option("delimiter", "|").option("header", "true").csv(path)
    // val edge_df = spark.table("com_friendster_e")

    val edge_path = new fs.Path("oss://weibin-internal/graphar/cf/person_knows_person.edge.yml")
    // val vertex_input = new URL("oss://weibin-transfer/graphar/ldbc_sample/csv/person.vertex.yml").openStream()
    // val vertex_path = new fs.Path("/Users/weibin/Dev/GraphAr/test/gar-test/ldbc_sample/csv/person.vertex.yml")
    // val vertex_input = getClass.getClassLoader.getResourceAsStream("gar-test/ldbc_sample/csv/person.vertex.yml")
    val file_system = fs.FileSystem.get(edge_path.toUri(), spark.sparkContext.hadoopConfiguration)
    val input = file_system.open(edge_path)
    val edge_yaml = new Yaml(new Constructor(classOf[EdgeInfo]))
    val edge_info = edge_yaml.load(input).asInstanceOf[EdgeInfo]
    val adj_list_type = AdjListType.ordered_by_source

    // val edge_df_with_index = IndexGenerator.generateSrcAndDstIndexUnitedlyForEdges(edge_df, "src", "dst")
    val reader = new EdgeReader("oss://weibin-internal/graphar/cf_nooffset/", edge_info, adj_list_type, spark)
    val edgeDf = reader.readEdges()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("GraphAr")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    import spark._
    import sqlContext.implicits._
    testOdpsCfReader(spark)
    spark.close()
  }
}