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

package com.alibaba.graphar

import com.alibaba.graphar.datasources._
import com.alibaba.graphar.graph.GraphWriter

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class Neo4j2GraphArExample {
  // connect to the Neo4j instance
  val spark = SparkSession.builder()
    .config("neo4j.url", "bolt://localhost:7687")
    .config("neo4j.authentication.type", "basic")
    .config("neo4j.authentication.basic.username", sys.env.get("Neo4j_USR").get)
    .config("neo4j.authentication.basic.password", sys.env.get("Neo4j_PWD").get)
    .config("spark.master", "local")
    .getOrCreate()

  // read Person vertices from Neo4j and write to GraphAr
  def testReadMovieGraphFromNeo4j(): Unit = {
    // initialize a graph writer
    val writer = new GraphWriter()

    // read vertices with label "Person" from Neo4j as a DataFrame
    val person_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("query", "MATCH (n:Person) RETURN n.name AS name, n.born as born")
      .load()
    // display the DataFrame and its schema
    person_df.show()
    person_df.printSchema()
    // put into writer
    writer.PutVertexData("Person", person_df)

    // read vertices with label "Person" from Neo4j as a DataFrame
    val movie_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("query", "MATCH (n:Movie) RETURN n.title AS title, n.tagline as tagline")
      .load()
    // display the DataFrame and its schema
    movie_df.show()
    movie_df.printSchema()
    // put into writer
    writer.PutVertexData("Movie", movie_df)

    // read edges with type "Person"->"PRODUCED"->"Movie" from Neo4j as a DataFrame
    val produced_edge_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("query", "MATCH (a:Person)-[r:PRODUCED]->(b:Movie) return a.name as src, b.title as dst")
      .load()
    // display the DataFrame and its schema
    produced_edge_df.show()
    produced_edge_df.printSchema()
    // put into writer
    writer.PutEdgeData(("Person", "PRODUCED", "Movie"), produced_edge_df)

    val acted_in_edge_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("query", "MATCH (a:Person)-[r:ACTED_IN]->(b:Movie) return a.name as src, b.title as dst")
      .load()
    // display the DataFrame and its schema
    acted_in_edge_df.show()
    acted_in_edge_df.printSchema()
    writer.PutEdgeData(("Person", "ACTED_IN", "Movie"), acted_in_edge_df)

    val directed_edge_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("query", "MATCH (a:Person)-[r:DIRECTED]->(b:Movie) return a.name as src, b.title as dst")
      .load()
    // display the DataFrame and its schema
    directed_edge_df.show()
    directed_edge_df.printSchema()
    writer.PutEdgeData(("Person", "DIRECTED", "Movie"), directed_edge_df)

    val follows_edge_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("query", "MATCH (a:Person)-[r:FOLLOWS]->(b:Person) return a.name as src, b.name as dst")
      .load()
    // display the DataFrame and its schema
    follows_edge_df.show()
    follows_edge_df.printSchema()
    writer.PutEdgeData(("Person", "FOLLOWS", "Person"), follows_edge_df)

    val reviewed_edge_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("query", "MATCH (a:Person)-[r:REVIEWED]->(b:Movie) return a.name as src, b.title as dst, r.rating as rating, r.summary as summary")
      .load()
    // display the DataFrame and its schema
    reviewed_edge_df.show()
    reviewed_edge_df.printSchema()
    writer.PutEdgeData(("Person", "REVIEWED", "Movie"), reviewed_edge_df)

    val wrote_edge_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("query", "MATCH (a:Person)-[r:WROTE]->(b:Movie) return a.name as src, b.title as dst")
      .load()
    // display the DataFrame and its schema
    wrote_edge_df.show()
    wrote_edge_df.printSchema()
    writer.PutEdgeData(("Person", "WROTE", "Movie"), wrote_edge_df)

    // write to the path
    writer.write("/tmp/movie_graph/", "movie_graph", spark, 100, 1024, "csv")
  }
}
