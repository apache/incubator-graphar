/** Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.example

import com.alibaba.graphar.datasources._
import com.alibaba.graphar.reader.{VertexReader, EdgeReader}
import com.alibaba.graphar.graph.GraphReader
import com.alibaba.graphar.{GeneralParams, GraphInfo}
import com.alibaba.graphar.utils.Utils

import java.io.{File, FileInputStream}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import scala.beans.BeanProperty
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.hadoop.fs.{Path, FileSystem}

object GrapAr2Neo4j {

  def main(args: Array[String]): Unit = {
    // connect to the Neo4j instance
    val spark = SparkSession.builder()
      .appName("GraphAr to Neo4j for Movie Graph")
      .config("neo4j.url", "bolt://localhost:7687")
      .config("neo4j.authentication.type", "basic")
      .config("neo4j.authentication.basic.username", sys.env.get("NEO4J_USR").get)
      .config("neo4j.authentication.basic.password", sys.env.get("NEO4J_PWD").get)
      .config("spark.master", "local")
      .getOrCreate()

    val graphInfoPath: String = args(0)
    val graphInfo = GraphInfo.loadGraphInfo(graphInfoPath, spark)

    val graphData = GraphReader.read(graphInfoPath, spark)
    val vertexData = graphData._1
    val edgeData = graphData._2

    putVertexDataIntoNeo4j(graphInfo, vertexData, spark)
    putEdgeDataIntoNeo4j(graphInfo, vertexData, edgeData, spark)
  }

  def putVertexDataIntoNeo4j(graphInfo: GraphInfo, vertexData: Map[String, DataFrame], spark: SparkSession): Unit = {
    vertexData.foreach { case (key, df) => {
      // write the vertices to Neo4j
      val primaryKey = graphInfo.getVertexInfo(key).getPrimaryKey()
      df.drop(GeneralParams.vertexIndexCol).write.format("org.neo4j.spark.DataSource")
        .mode(SaveMode.Overwrite)
        .option("labels", ":" + key)
        .option("node.keys", primaryKey)
        .save()
    }}
  }

  def putEdgeDataIntoNeo4j(graphInfo: GraphInfo, vertexData: Map[String, DataFrame], edgeData: Map[(String, String, String), Map[String, DataFrame]], spark: SparkSession): Unit = {
    edgeData.foreach { case (key, value) => {
      // write the edges to Neo4j
      val sourceLabel = key._1
      val edgeLabel = key._2
      val targetLabel = key._3
      val sourcePrimaryKey = graphInfo.getVertexInfo(sourceLabel).getPrimaryKey()
      val targetPrimaryKey = graphInfo.getVertexInfo(targetLabel).getPrimaryKey()
      val sourceDf = vertexData(sourceLabel)
      val targetDf = vertexData(targetLabel)
      val df = Utils.joinEdgesWithVertexPrimaryKey(value.head._2, sourceDf, targetDf, sourcePrimaryKey, targetPrimaryKey)  // use the first dataframe of (adj_list_type_str, dataframe) map

      val properties = if (edgeLabel == "REVIEWED") "rating,summary" else ""

      df.write.format("org.neo4j.spark.DataSource")
        .mode(SaveMode.Overwrite)
        .option("relationship", edgeLabel)
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.labels", ":" + sourceLabel)
        .option("relationship.source.save.mode", "match")
        .option("relationship.source.node.keys", "src:" + sourcePrimaryKey)
        .option("relationship.target.labels", ":" + targetLabel)
        .option("relationship.target.save.mode", "match")
        .option("relationship.target.node.keys", "dst:" + targetPrimaryKey)
        .option("relationship.properties", properties)
        .save()
    }}
  }
}
