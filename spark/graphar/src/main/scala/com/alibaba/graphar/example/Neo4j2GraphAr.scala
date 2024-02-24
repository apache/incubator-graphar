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

import org.apache.spark.sql.SparkSession

object Neo4j2GraphAr {

  def main(args: Array[String]): Unit = {
    // connect to the Neo4j instance
    val spark = SparkSession
      .builder()
      .appName("Neo4j to GraphAr for Movie Graph")
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
      .config("spark.sql.legacy.parquet.nanosAsLong", "false")
      .config("spark.sql.parquet.fieldId.read.enabled", "true")
      .config("spark.sql.parquet.fieldId.write.enabled", "true")
      .getOrCreate()

    // initialize a graph writer
    val writer: GraphWriter = new GraphWriter()

    // put movie graph data into writer
    readAndPutDataIntoWriter(writer, spark)

    // output directory
    val outputPath: String = args(0)
    // vertex chunk size
    val vertexChunkSize: Long = args(1).toLong
    // edge chunk size
    val edgeChunkSize: Long = args(2).toLong
    // file type
    val fileType: String = args(3)

    // write in graphar format
    writer.write(
      outputPath,
      spark,
      "MovieGraph",
      vertexChunkSize,
      edgeChunkSize,
      fileType
    )
  }

  // read data from Neo4j and put into writer
  def readAndPutDataIntoWriter(
      writer: GraphWriter,
      spark: SparkSession
  ): Unit = {
    // read vertices with label "Person" from Neo4j as a DataFrame
    // Note: set "schema.flatten.limit" to 1 to not sample null record infer type as string as far as possible,
    // if you want a perfect type inference, consider to user APOC.
    val person_df = spark.read
      .format("org.neo4j.spark.DataSource")
      .option("query", "MATCH (n:Person) RETURN n.name AS name, n.born as born")
      .option("schema.flatten.limit", 1)
      .load()
    // put into writer, vertex label is "Person"
    writer.PutVertexData("Person", person_df)

    // read vertices with label "Movie" from Neo4j as a DataFrame
    val movie_df = spark.read
      .format("org.neo4j.spark.DataSource")
      .option(
        "query",
        "MATCH (n:Movie) RETURN n.title AS title, n.tagline as tagline"
      )
      .option("schema.flatten.limit", 1)
      .load()
    // put into writer, vertex label is "Movie"
    writer.PutVertexData("Movie", movie_df)

    // read edges with type "Person"->"PRODUCED"->"Movie" from Neo4j as a DataFrame
    val produced_edge_df = spark.read
      .format("org.neo4j.spark.DataSource")
      .option(
        "query",
        "MATCH (a:Person)-[r:PRODUCED]->(b:Movie) return a.name as src, b.title as dst"
      )
      .option("schema.flatten.limit", 1)
      .load()
    // put into writer, source vertex label is "Person", edge label is "PRODUCED"
    // target vertex label is "Movie"
    writer.PutEdgeData(("Person", "PRODUCED", "Movie"), produced_edge_df)

    // read edges with type "Person"->"ACTED_IN"->"Movie" from Neo4j as a DataFrame
    val acted_in_edge_df = spark.read
      .format("org.neo4j.spark.DataSource")
      .option(
        "query",
        "MATCH (a:Person)-[r:ACTED_IN]->(b:Movie) return a.name as src, b.title as dst"
      )
      .option("schema.flatten.limit", 1)
      .load()
    // put into writer, source vertex label is "Person", edge label is "ACTED_IN"
    // target vertex label is "Movie"
    writer.PutEdgeData(("Person", "ACTED_IN", "Movie"), acted_in_edge_df)

    // read edges with type "Person"->"DIRECTED"->"Movie" from Neo4j as a DataFrame
    val directed_edge_df = spark.read
      .format("org.neo4j.spark.DataSource")
      .option(
        "query",
        "MATCH (a:Person)-[r:DIRECTED]->(b:Movie) return a.name as src, b.title as dst"
      )
      .option("schema.flatten.limit", 1)
      .load()
    // put into writer, source vertex label is "Person", edge label is "DIRECTED"
    // target vertex label is "Movie"
    writer.PutEdgeData(("Person", "DIRECTED", "Movie"), directed_edge_df)

    // read edges with type "Person"->"FOLLOWS"->"Person" from Neo4j as a DataFrame
    val follows_edge_df = spark.read
      .format("org.neo4j.spark.DataSource")
      .option(
        "query",
        "MATCH (a:Person)-[r:FOLLOWS]->(b:Person) return a.name as src, b.name as dst"
      )
      .option("schema.flatten.limit", 1)
      .load()
    // put into writer, source vertex label is "Person", edge label is "FOLLOWS"
    // target vertex label is "Person"
    writer.PutEdgeData(("Person", "FOLLOWS", "Person"), follows_edge_df)

    // read edges with type "Person"->"REVIEWED"->"Movie" from Neo4j as a DataFrame
    val reviewed_edge_df = spark.read
      .format("org.neo4j.spark.DataSource")
      .option(
        "query",
        "MATCH (a:Person)-[r:REVIEWED]->(b:Movie) return a.name as src, b.title as dst, r.rating as rating, r.summary as summary"
      )
      .option("schema.flatten.limit", 1)
      .load()
    // put into writer, source vertex label is "Person", edge label is "REVIEWED"
    // target vertex label is "Movie"
    writer.PutEdgeData(("Person", "REVIEWED", "Movie"), reviewed_edge_df)

    // read edges with type "Person"->"WROTE"->"Movie" from Neo4j as a DataFrame
    val wrote_edge_df = spark.read
      .format("org.neo4j.spark.DataSource")
      .option(
        "query",
        "MATCH (a:Person)-[r:WROTE]->(b:Movie) return a.name as src, b.title as dst"
      )
      .option("schema.flatten.limit", 1)
      .load()
    // put into writer, source vertex label is "Person", edge label is "WROTE"
    // target vertex label is "Movie"
    writer.PutEdgeData(("Person", "WROTE", "Movie"), wrote_edge_df)
  }
}
