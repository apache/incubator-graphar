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
import com.alibaba.graphar.utils.IndexGenerator
import com.alibaba.graphar.writer.{VertexWriter, EdgeWriter}

import java.io.{File, FileInputStream}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import scala.beans.BeanProperty
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.hadoop.fs.{Path, FileSystem}

class Neo4j2GraphArExample{
  // connect to the Neo4j instance
  val spark = SparkSession.builder()
    .config("neo4j.url", "bolt://localhost:7687")
    .config("neo4j.authentication.type", "basic")
    .config("neo4j.authentication.basic.username", sys.env.get("Neo4j_USR").get)
    .config("neo4j.authentication.basic.password", sys.env.get("Neo4j_PWD").get)
    .config("spark.master", "local")
    .getOrCreate()

  // read Person vertices from Neo4j and write to GraphAr
  def testReadPersonVerticesFromNeo4j(): Unit = {
    // read vertices with label "Person" from Neo4j as a DataFrame
    val person_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("labels", "Person")
      .load()
    // display the DataFrame and its schema
    person_df.show()
    person_df.printSchema()

    // read vertex info yaml
    val vertex_yaml_path = new Path(getClass.getClassLoader.getResource("gar-test/neo4j/person.vertex.yml").getPath)
    val fs = FileSystem.get(vertex_yaml_path.toUri(), spark.sparkContext.hadoopConfiguration)
    val vertex_input = fs.open(vertex_yaml_path)
    val vertex_yaml = new Yaml(new Constructor(classOf[VertexInfo]))
    val vertex_info = vertex_yaml.load(vertex_input).asInstanceOf[VertexInfo]

    // generate vertex index column for vertex dataframe
    val person_df_with_index = IndexGenerator.generateVertexIndexColumn(person_df)

    // create writer object for person
    val prefix : String = "/tmp/neo4j/"
    val writer = new VertexWriter(prefix, vertex_info, person_df_with_index)

    // use the DataFrame to generate GAR files
    writer.writeVertexProperties()
  }

  // read Movie vertices from Neo4j and write to GraphAr
  def testReadMovieVerticesFromNeo4j(): Unit = {
    // read vertices with label "Person" from Neo4j as a DataFrame
    val movie_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("labels", "Movie")
      .load()
    // display the DataFrame and its schema
    movie_df.show()
    movie_df.printSchema()

    // read vertex info yaml
    val vertex_yaml_path = new Path(getClass.getClassLoader.getResource("gar-test/neo4j/movie.vertex.yml").getPath)
    val fs = FileSystem.get(vertex_yaml_path.toUri(), spark.sparkContext.hadoopConfiguration)
    val vertex_input = fs.open(vertex_yaml_path)
    val vertex_yaml = new Yaml(new Constructor(classOf[VertexInfo]))
    val vertex_info = vertex_yaml.load(vertex_input).asInstanceOf[VertexInfo]

    // generate vertex index column for vertex dataframe
    val movie_df_with_index = IndexGenerator.generateVertexIndexColumn(movie_df)

    // create writer object for person
    val prefix : String = "/tmp/neo4j/"
    val writer = new VertexWriter(prefix, vertex_info, movie_df_with_index)

    // use the DataFrame to generate GAR files
    writer.writeVertexProperties()
  }

  // read edges from Neo4j and write to GraphAr
  def testReadEdgesFromNeo4j(): Unit = {
    // read vertices with label "Person" from Neo4j as a DataFrame
    val person_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("labels", "Person")
      .load()
    // read vertices with label "Movie" from Neo4j as a DataFrame
    val movie_df = spark.read.format("org.neo4j.spark.DataSource")
      .option("labels", "Movie")
      .load()

    // read edges with type "Person"->"PRODUCED"->"Movie" from Neo4j as a DataFrame
    val edge_df = spark.read.format("org.neo4j.spark.DataSource")
        .option("relationship", "PRODUCED")
        .option("relationship.source.labels", "Person")
        .option("relationship.target.labels", "Movie")
        .load()
    // display the DataFrame and its schema
    edge_df.show()
    edge_df.printSchema()

    // construct the Person vertex mapping with person_df
    val person_vertex_mapping = IndexGenerator.constructVertexIndexMapping(person_df, "<id>")
    // construct the Movie vertex mapping with movie_df
    val movie_vertex_mapping = IndexGenerator.constructVertexIndexMapping(movie_df, "<id>")

    // generate src index and dst index for edge DataFrame with vertex mapping
    val edge_df_with_src_index = IndexGenerator.generateSrcIndexForEdgesFromMapping(edge_df, "<source.id>", person_vertex_mapping)
    val edge_df_with_src_dst_index = IndexGenerator.generateDstIndexForEdgesFromMapping(edge_df_with_src_index, "<target.id>", movie_vertex_mapping)
    edge_df_with_src_dst_index.show()

    // read edge info yaml
    val edge_yaml_path = new Path(getClass.getClassLoader.getResource("gar-test/neo4j/person_produced_movie.edge.yml").getPath)
    val fs = FileSystem.get(edge_yaml_path.toUri(), spark.sparkContext.hadoopConfiguration)
    val edge_input = fs.open(edge_yaml_path)
    val edge_yaml = new Yaml(new Constructor(classOf[EdgeInfo]))
    val edge_info = edge_yaml.load(edge_input).asInstanceOf[EdgeInfo]

    // create writer object for the DataFrame with indices
    val prefix : String = "/tmp/neo4j/"
    val adj_list_type = AdjListType.ordered_by_source
    val vertex_num = person_df.count()
    val writer = new EdgeWriter(prefix, edge_info, adj_list_type, vertex_num, edge_df_with_src_dst_index)

    // generate the adj list and properties with GAR format
    writer.writeEdges()
  }
}
