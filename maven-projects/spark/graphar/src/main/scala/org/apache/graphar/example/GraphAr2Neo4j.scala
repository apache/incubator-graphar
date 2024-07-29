/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.graphar.example

import org.apache.graphar.graph.GraphReader
import org.apache.graphar.{GeneralParams, GraphInfo}
import org.apache.graphar.util.Utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object GraphAr2Neo4j {

  def main(args: Array[String]): Unit = {
    // connect to the Neo4j instance
    val spark = SparkSession
      .builder()
      .appName("GraphAr to Neo4j for Movie Graph")
      .config("neo4j.url", "bolt://localhost:7687")
      .config("neo4j.authentication.type", "basic")
      .config(
        "neo4j.authentication.basic.username",
        sys.env.get("NEO4J_USR").get
      )
      .config(
        "neo4j.authentication.basic.password",
        sys.env.get("NEO4J_PWD").get
      )
      .config("spark.master", "local")
      .getOrCreate()

    // path to the graph information file
    val graphInfoPath: String = args(0)
    val graphInfo = GraphInfo.loadGraphInfo(graphInfoPath, Some(spark))

    val graphData = GraphReader.read(graphInfoPath, spark)
    val vertexData = graphData._1
    val edgeData = graphData._2

    putVertexDataIntoNeo4j(graphInfo, vertexData, spark)
    putEdgeDataIntoNeo4j(graphInfo, vertexData, edgeData, spark)
  }

  def putVertexDataIntoNeo4j(
      graphInfo: GraphInfo,
      vertexData: Map[String, DataFrame],
      spark: SparkSession
  ): Unit = {
    // write each vertex type to Neo4j
    vertexData.foreach {
      case (key, df) => {
        val primaryKey = graphInfo.getVertexInfo(key).getPrimaryKey()
        // the vertex index column is not needed in Neo4j
        // write to Neo4j, refer to https://neo4j.com/docs/spark/current/writing/
        df.drop(GeneralParams.vertexIndexCol)
          .write
          .format("org.neo4j.spark.DataSource")
          .mode(SaveMode.Overwrite)
          .option("labels", ":" + key)
          .option("node.keys", primaryKey)
          .save()
      }
    }
  }

  def putEdgeDataIntoNeo4j(
      graphInfo: GraphInfo,
      vertexData: Map[String, DataFrame],
      edgeData: Map[(String, String, String), Map[String, DataFrame]],
      spark: SparkSession
  ): Unit = {
    // write each edge type to Neo4j
    edgeData.foreach {
      case (key, value) => {
        val sourceLabel = key._1
        val edgeLabel = key._2
        val targetLabel = key._3
        val sourcePrimaryKey =
          graphInfo.getVertexInfo(sourceLabel).getPrimaryKey()
        val targetPrimaryKey =
          graphInfo.getVertexInfo(targetLabel).getPrimaryKey()
        val sourceDf = vertexData(sourceLabel)
        val targetDf = vertexData(targetLabel)
        // convert the source and target index column to the primary key column
        val df = Utils.joinEdgesWithVertexPrimaryKey(
          value.head._2,
          sourceDf,
          targetDf,
          sourcePrimaryKey,
          targetPrimaryKey
        ) // use the first DataFrame of (adj_list_type_str, DataFrame) map

        // FIXME: use properties message in edge info
        val properties = if (edgeLabel == "REVIEWED") "rating,summary" else ""

        df.write
          .format("org.neo4j.spark.DataSource")
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
      }
    }
  }
}
