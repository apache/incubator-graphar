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

package com.alibaba.graphar.example

import com.alibaba.graphar.graph.GraphWriter
import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.connector.connector.NebulaDataFrameReader
import com.vesoft.nebula.connector.{NebulaConnectionConfig, ReadNebulaConfig}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object Nebula2GraphAr {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  private val DEFAULT_GRAPH_SPACE = "basketballplayer";

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))

    val spark = SparkSession
      .builder()
      .appName("NebulaGraph to GraphAr for basketballplayer Graph")
      .master("local")
      .config(sparkConf)
      .getOrCreate()

    // initialize a graph writer
    val writer: GraphWriter = new GraphWriter

    // put basketballplayer graph data into writer
    readAndPutDataIntoWriter(writer, spark)

    // output directory
    val outputPath: String = args(0)
    // vertex chunk size
    val vertexChunkSize: Long = args(1).toLong
    // edge chunk size
    val edgeChunkSize: Long = args(2).toLong
    // file type
    val fileType: String = args(3)

    // write in graph format
    writer.write(
      outputPath,
      spark,
      DEFAULT_GRAPH_SPACE + "graph",
      vertexChunkSize,
      edgeChunkSize,
      fileType
    )

    spark.close()
  }

  def readAndPutDataIntoWriter(
      writer: GraphWriter,
      spark: SparkSession
  ): Unit = {

    // read vertices with tag "player" from NebulaGraph as a DataFrame
    val playerDF = readVertexDF(spark, "player")
    LOG.info("player vertices count: " + playerDF.count())
    assert(playerDF.count() == 51)
    // put into writer, vertex tag is "player"
    writer.PutVertexData("player", playerDF)

    // read vertices with tag "team" from NebulaGraph as a DataFrame
    val teamDF = readVertexDF(spark, "team")
    LOG.info("team vertices count: " + teamDF.count())
    assert(teamDF.count() == 30)
    // put into writer, vertex tag is "team"
    writer.PutVertexData("team", teamDF)

    // read edges with type "player"->"follow"->"player" from NebulaGraph as a DataFrame
    val followDF = readEdgeDF(spark, "follow")
    LOG.info("follow edges count: " + followDF.count())
    assert(followDF.count() == 81)
    // put into writer, source vertex tag is "player", edge type is "follow"
    // target vertex tag is "player"
    writer.PutEdgeData(("player", "follow", "player"), followDF)

    // read edges with type "player"->"serve"->"team" from NebulaGraph as a DataFrame
    val serveDF = readEdgeDF(spark, "serve")
    LOG.info("serve edges count: " + serveDF.count())
    assert(serveDF.count() == 152)
    // put into writer, source vertex tag is "player", edge type is "serve"
    // target vertex tag is "team"
    writer.PutEdgeData(("player", "serve", "team"), serveDF)
  }

  private def readVertexDF(
      spark: SparkSession,
      vertexTag: String
  ): DataFrame = {
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withConenctionRetry(3)
        .build()

    val nebulaReadVertexConfig = ReadNebulaConfig
      .builder()
      .withSpace(DEFAULT_GRAPH_SPACE)
      .withLabel(vertexTag)
      .withNoColumn(false)
      .withReturnCols(List())
      .withLimit(10)
      .withPartitionNum(10)
      .build()

    val vertices = spark.read
      .nebula(config, nebulaReadVertexConfig)
      .loadVerticesToDF()
    vertices
  }

  def readEdgeDF(
      spark: SparkSession,
      edgeTag: String
  ): DataFrame = {
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withTimeout(6000)
        .withConenctionRetry(3)
        .build()

    val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace(DEFAULT_GRAPH_SPACE)
      .withLabel(edgeTag)
      .withNoColumn(false)
      .withReturnCols(List())
      .withLimit(10)
      .withPartitionNum(10)
      .build()

    val edges = spark.read.nebula(config, nebulaReadEdgeConfig).loadEdgesToDF()
    edges
  }
}
