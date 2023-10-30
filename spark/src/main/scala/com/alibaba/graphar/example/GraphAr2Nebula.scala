/**
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.alibaba.graphar.example

import com.alibaba.graphar.{GeneralParams, GraphInfo}
import com.alibaba.graphar.graph.GraphReader
import com.alibaba.graphar.util.Utils
import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.connector.connector.NebulaDataFrameWriter
import com.vesoft.nebula.connector.{
  NebulaConnectionConfig,
  WriteMode,
  WriteNebulaEdgeConfig,
  WriteNebulaVertexConfig
}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object GraphAr2Nebula {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  private val DEFAULT_GRAPH_SPACE = "basketballplayer";

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))

    val spark = SparkSession
      .builder()
      .master("local")
      .config(sparkConf)
      .getOrCreate()

    // path to the graph information file
    val graphInfoPath: String = args(0)
    val graphInfo = GraphInfo.loadGraphInfo(graphInfoPath, spark)

    // The edge data need to convert src and dst to the vertex id,
    // so we need to read the vertex data with index column.
    val graphData =
      GraphReader.read(graphInfoPath, spark, addVertexIndex = true)
    val vertexData = graphData._1
    val edgeData = graphData._2
    putVertexDataIntoNebula(graphInfo, vertexData)
    putEdgeDataIntoNebula(graphInfo, vertexData, edgeData)
  }

  private def putVertexDataIntoNebula(
      graphInfo: GraphInfo,
      vertexData: Map[String, DataFrame]
  ): Unit = {
    // write each vertex tag to Nebula
    vertexData.foreach { case (tag, df) =>
      val newDF = df.drop(GeneralParams.vertexIndexCol)
      val primaryKey = graphInfo.getVertexInfo(tag)
      writeVertex(tag, primaryKey.getPrimaryKey(), newDF)
    }
  }

  def putEdgeDataIntoNebula(
      graphInfo: GraphInfo,
      vertexData: Map[String, DataFrame],
      edgeData: Map[(String, String, String), Map[String, DataFrame]]
  ): Unit = {
    // write each edge type to Nebula
    edgeData.foreach { case (srcEdgeDstLabels, orderMap) =>
      val sourceTag = srcEdgeDstLabels._1
      val edgeType = srcEdgeDstLabels._2
      val targetTag = srcEdgeDstLabels._3
      val sourcePrimaryKey = graphInfo.getVertexInfo(sourceTag).getPrimaryKey()
      val targetPrimaryKey = graphInfo.getVertexInfo(targetTag).getPrimaryKey()
      val sourceDF = vertexData(sourceTag)
      val targetDF = vertexData(targetTag)

      // convert the source and target index column to the primary key column
      val df = Utils.joinEdgesWithVertexPrimaryKey(
        orderMap.head._2,
        sourceDF,
        targetDF,
        sourcePrimaryKey,
        targetPrimaryKey
      ) // use the first DataFrame of (adj_list_type_str, DataFrame) map

      writeEdge(edgeType, "src", "dst", "_rank", df)
    }
  }

  private def writeVertex(
      tag: String,
      idFieldName: String,
      df: DataFrame
  ): Unit = {
    val config = getNebulaConnectionConfig

    val nebulaWriterVertexConfig: WriteNebulaVertexConfig =
      WriteNebulaVertexConfig
        .builder()
        .withSpace(DEFAULT_GRAPH_SPACE)
        .withTag(tag)
        .withVidField(idFieldName)
        .withWriteMode(WriteMode.INSERT)
        .withVidAsProp(false)
        .withBatch(100)
        .build()

    df.write.nebula(config, nebulaWriterVertexConfig).writeVertices()
  }

  private def writeEdge(
      edgeType: String,
      srcField: String,
      dstField: String,
      rankField: String,
      df: DataFrame
  ): Unit = {

    val config = getNebulaConnectionConfig
    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withSpace(DEFAULT_GRAPH_SPACE)
      .withEdge(edgeType)
      .withSrcIdField(srcField)
      .withDstIdField(dstField)
      .withRankField(rankField)
      .withSrcAsProperty(false)
      .withDstAsProperty(false)
      .withRankAsProperty(false)
      .withBatch(1000)
      .build()

    df.write.nebula(config, nebulaWriteEdgeConfig).writeEdges()
  }

  private def getNebulaConnectionConfig: NebulaConnectionConfig = {
    NebulaConnectionConfig
      .builder()
      .withMetaAddress("127.0.0.1:9559")
      .withGraphAddress("127.0.0.1:9669")
      .withConenctionRetry(3)
      .build()
  }
}
