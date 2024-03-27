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

package com.alibaba.graphar.importer

import com.alibaba.graphar.datasources._
import com.alibaba.graphar.graph.GraphWriter

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.{DataFrame, SparkSession}

object Neo4j {

  // define the config format
  case class Gar(
      path: String,
      name: String,
      vertexChunkSize: Long,
      edgeChunkSize: Long,
      fileType: String
  )
  case class Neo4j(url: String, username: String, password: String)
  case class Vertex(label: String, properties: List[String])
  case class Edge(
      label: String,
      srcLabel: String,
      srcProp: String,
      dstLabel: String,
      dstProp: String,
      properties: List[String]
  )
  case class Schema(vertices: List[Vertex], edges: List[Edge])

  def main(args: Array[String]): Unit = {
    // read the config from the command line parameter
    var confPath: String = args(0)
    val confString = scala.io.Source.fromFile(confPath).mkString
    implicit val formats = DefaultFormats
    val confJson: JValue = parse(confString)

    // construct the config
    val gar = (confJson \ "gar").extract[Gar]
    val neo4j = (confJson \ "neo4j").extract[Neo4j]
    val schema = (confJson \ "schema").extract[Schema]

    // connect to the Neo4j instance
    val spark = SparkSession
      .builder()
      .appName("Neo4j to GraphAr for Movie Graph")
      .config("neo4j.url", neo4j.url)
      .config("neo4j.authentication.type", "basic")
      .config(
        "neo4j.authentication.basic.username",
        neo4j.username
      )
      .config(
        "neo4j.authentication.basic.password",
        neo4j.password
      )
      .config("spark.master", "local")
      .getOrCreate()

    // initialize a graph writer
    val writer: GraphWriter = new GraphWriter()

    // put movie graph data into writer
    readAndPutDataIntoWriter(writer, spark, schema)

    // write in graphar format
    writer.write(
      gar.path,
      spark,
      gar.name,
      gar.vertexChunkSize,
      gar.edgeChunkSize,
      gar.fileType
    )
  }

  // read data from Neo4j and put into writer
  def readAndPutDataIntoWriter(
      writer: GraphWriter,
      spark: SparkSession,
      schema: Schema
  ): Unit = {
    // read vertices
    for (vertex <- schema.vertices) {
      // construct the cypher
      val cypherBuf = new StringBuilder
      cypherBuf.append(s"MATCH (n:${vertex.label}) RETURN")
      for (prop <- vertex.properties) {
        cypherBuf.append(s" n.$prop AS $prop,")
      }

      // remove the last ","
      cypherBuf.deleteCharAt(cypherBuf.length - 1)

      // get the DataFrame
      val vertex_df = spark.read
        .format("org.neo4j.spark.DataSource")
        .option("query", cypherBuf.toString)
        .load()
      // put into writer
      writer.PutVertexData(vertex.label, vertex_df)
    }

    // read edges
    for (edge <- schema.edges) {
      // construct the cypher
      val cypherBuf = new StringBuilder
      cypherBuf
        .append(
          s"MATCH (a:${edge.srcLabel})-[r:${edge.label}]->(b:${edge.dstLabel}) RETURN"
        )
        .append(s" a.${edge.srcProp} AS src,")
        .append(s" b.${edge.dstProp} AS dst")
      for (prop <- edge.properties) {
        cypherBuf.append(s", r.$prop AS $prop")
      }

      // get the DataFrame
      val edge_df = spark.read
        .format("org.neo4j.spark.DataSource")
        .option("query", cypherBuf.toString)
        .load()
      // put into writer
      writer.PutEdgeData((edge.srcLabel, edge.label, edge.dstLabel), edge_df)
    }
  }
}
