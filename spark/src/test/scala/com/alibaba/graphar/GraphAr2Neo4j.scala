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
import com.alibaba.graphar.reader.{VertexReader, EdgeReader}

import java.io.{File, FileInputStream}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import scala.beans.BeanProperty
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.hadoop.fs.{Path, FileSystem}

class GraphAr2Neo4jExample {
  // connect to the Neo4j instance
  val spark = SparkSession.builder()
    .config("neo4j.url", "bolt://localhost:7687")
    .config("neo4j.authentication.type", "basic")
    .config("neo4j.authentication.basic.username", sys.env.get("Neo4j_USR").get)
    .config("neo4j.authentication.basic.password", sys.env.get("Neo4j_PWD").get)
    .config("spark.master", "local")
    .getOrCreate()

  // read Person vertices from GraphAr and write to Neo4j

  def testWriteVerticesToNeo4j(): Unit = {
    // read vertex info yaml
    val vertex_yaml = getClass.getClassLoader.getResource("gar-test/neo4j/person.vertex.yml").getPath
    val vertex_info = VertexInfo.loadVertexInfo(vertex_yaml, spark)

    // construct the vertex reader
    val prefix : String = "/tmp/neo4j/"
    val reader = new VertexReader(prefix, vertex_info, spark)

    // reading chunks for all property groups
    val vertex_df = reader.readAllVertexPropertyGroups(false)
    // display the DataFrame and its schema
    vertex_df.show()
    vertex_df.printSchema()

    // group vertices with the same labels together
    val labels_array = vertex_df.select("<labels>").distinct.collect.flatMap(_.toSeq)
    val vertex_df_array = labels_array.map(labels => vertex_df.where(vertex_df("<labels>") === labels))

    // each time write a group of vertices to Neo4j
    vertex_df_array.foreach(df => {
      // construct the string for labels
      val labels = df.first().getAs[Seq[String]]("<labels>")
      var str = ""
      labels.foreach(label => {str += ":" + label})

      // write the vertices to Neo4j
      df.drop("<id>").drop("<labels>")
      .write.format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Overwrite)
      .option("labels", str)
      .option("node.keys", "name")
      .save()
    })
  }

  // read edges from GraphAr and write to Neo4j
  def testWriteEdgesToNeo4j(): Unit = {
    // read person dataframe
    val person_yaml = getClass.getClassLoader.getResource("gar-test/neo4j/person.vertex.yml").getPath
    val person_info = VertexInfo.loadVertexInfo(person_yaml, spark)

    val prefix : String = "/tmp/neo4j/"
    val person_reader = new VertexReader(prefix, person_info, spark)
    val person_df = person_reader.readAllVertexPropertyGroups(true)
    person_df.show()

    // read movie dataframe
    val movie_yaml = getClass.getClassLoader.getResource("gar-test/neo4j/movie.vertex.yml").getPath
    val movie_info = VertexInfo.loadVertexInfo(movie_yaml, spark)

    val movie_reader = new VertexReader(prefix, movie_info, spark)
    val movie_df = movie_reader.readAllVertexPropertyGroups(true)
    movie_df.show()

    // read Person->Produced->Movie edges
    val edge_yaml = getClass.getClassLoader.getResource("gar-test/neo4j/person_produced_movie.edge.yml").getPath
    val edge_info = EdgeInfo.loadEdgeInfo(edge_yaml, spark)

    val adj_list_type = AdjListType.ordered_by_source
    val reader = new EdgeReader(prefix, edge_info, adj_list_type, spark)
    val edge_df = reader.readEdges(false)
    edge_df.show()
    
    // join the edge dataframe with the two vertex dataframes to get information of src & dst
    person_df.createOrReplaceTempView("src_vertex")
    movie_df.createOrReplaceTempView("dst_vertex")
    edge_df.createOrReplaceTempView("edge")
    val srcCol = GeneralParams.srcIndexCol;
    val dstCol = GeneralParams.dstIndexCol;
    val indexCol = GeneralParams.vertexIndexCol;
    val edge_df_with_src = spark.sql(f"select src_vertex.`name` as `person_name`, edge.* from edge inner join src_vertex on src_vertex.`$indexCol`=edge.`$srcCol`")
    edge_df_with_src.createOrReplaceTempView("edge")
    val edge_df_with_src_dst = spark.sql(f"select dst_vertex.`title` as `movie_title`, edge.* from edge inner join dst_vertex on dst_vertex.`$indexCol`=edge.`$dstCol`")
    edge_df_with_src_dst.show()

    // write the edges to Neo4j
    edge_df_with_src_dst
      .write.format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Overwrite)
      .option("relationship", "PRODUCED")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":Person")
      .option("relationship.source.save.mode", "match")
      .option("relationship.source.node.keys", "person_name:name")
      .option("relationship.target.labels", ":Movie")
      .option("relationship.target.save.mode", "match")
      .option("relationship.target.node.keys", "movie_title:title")
      .option("relationship.properties", "")
      .save()
  }
  
}
