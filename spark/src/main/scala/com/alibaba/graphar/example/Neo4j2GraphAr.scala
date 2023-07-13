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
import com.alibaba.graphar.graph.GraphWriter

import org.apache.spark.sql.{DataFrame, SparkSession}

object Neo4j2GraphAr {

  def main(args: Array[String]): Unit = {
    // connect to the Neo4j instance
    val spark = SparkSession.builder()
      .appName("Neo4j to GraphAr for Movie Graph")
      .config("neo4j.url", "bolt://localhost:7687")
      .config("neo4j.authentication.type", "basic")
      .config("neo4j.authentication.basic.username", sys.env.get("NEO4J_USR").get)
      .config("neo4j.authentication.basic.password", sys.env.get("NEO4J_PWD").get)
      .config("spark.master", "local")
      .getOrCreate()

    // initialize a graph writer
    val writer: GraphWriter = new GraphWriter()

    // put movie graph data into writer
    readAndPutDataIntoWriter(writer, spark)

    // write in graphar format
    val outputPath: String = args(0)
    val vertexChunkSize: Long = args(1).toLong
    val edgeChunkSize: Long = args(2).toLong
    val fileType: String = args(3)

    writer.write(outputPath, spark, "MovieGraph", vertexChunkSize, edgeChunkSize, fileType)
  }

  // read data from Neo4j and put into writer
  def readAndPutDataIntoWriter(writer: GraphWriter, spark: SparkSession): Unit = {
    // read vertices with label "Person" from Neo4j as a DataFrame
    val person_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("query", "MATCH (n:Person) RETURN n.name AS name, n.born as born")
      .load()
    // put into writer
    writer.PutVertexData("Person", person_df)

    // read vertices with label "Person" from Neo4j as a DataFrame
    val movie_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("query", "MATCH (n:Movie) RETURN n.title AS title, n.tagline as tagline")
      .load()
    // put into writer
    writer.PutVertexData("Movie", movie_df)

    // read edges with type "Person"->"PRODUCED"->"Movie" from Neo4j as a DataFrame
    val produced_edge_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("query", "MATCH (a:Person)-[r:PRODUCED]->(b:Movie) return a.name as src, b.title as dst")
      .load()
    // put into writer
    writer.PutEdgeData(("Person", "PRODUCED", "Movie"), produced_edge_df)

    val acted_in_edge_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("query", "MATCH (a:Person)-[r:ACTED_IN]->(b:Movie) return a.name as src, b.title as dst")
      .load()
    writer.PutEdgeData(("Person", "ACTED_IN", "Movie"), acted_in_edge_df)

    val directed_edge_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("query", "MATCH (a:Person)-[r:DIRECTED]->(b:Movie) return a.name as src, b.title as dst")
      .load()
    writer.PutEdgeData(("Person", "DIRECTED", "Movie"), directed_edge_df)

    val follows_edge_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("query", "MATCH (a:Person)-[r:FOLLOWS]->(b:Person) return a.name as src, b.name as dst")
      .load()
    writer.PutEdgeData(("Person", "FOLLOWS", "Person"), follows_edge_df)

    val reviewed_edge_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("query", "MATCH (a:Person)-[r:REVIEWED]->(b:Movie) return a.name as src, b.title as dst, r.rating as rating, r.summary as summary")
      .load()
    writer.PutEdgeData(("Person", "REVIEWED", "Movie"), reviewed_edge_df)

    val wrote_edge_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("query", "MATCH (a:Person)-[r:WROTE]->(b:Movie) return a.name as src, b.title as dst")
      .load()
    writer.PutEdgeData(("Person", "WROTE", "Movie"), wrote_edge_df)
  }
}
