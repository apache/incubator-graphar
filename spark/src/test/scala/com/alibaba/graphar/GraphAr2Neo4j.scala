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
import com.alibaba.graphar.reader.{VertexReader, EdgeReader}
import com.alibaba.graphar.writer.{VertexWriter, EdgeWriter}

import java.io.{File, FileInputStream}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import scala.beans.BeanProperty
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.hadoop.fs.{Path, FileSystem}
import org.scalatest.funsuite.AnyFunSuite

class GraphAr2Neo4jSuite extends AnyFunSuite {
  // connect to the Neo4j instance
  val spark = SparkSession.builder()
    .config("neo4j.url", "bolt://localhost:7687")
    .config("neo4j.authentication.type", "basic")
    .config("neo4j.authentication.basic.username", sys.env.get("Neo4j_USR").get)
    .config("neo4j.authentication.basic.password", sys.env.get("Neo4j_PWD").get)
    .config("spark.master", "local")
    .getOrCreate()

  test("read vertices from GraphAr and write to Neo4j") {
    // read vertex info yaml
    val file_path = getClass.getClassLoader.getResource("gar-test/neo4j/person.vertex.yml").getPath
    val vertex_yaml_path = new Path(file_path)
    val fs = FileSystem.get(vertex_yaml_path.toUri(), spark.sparkContext.hadoopConfiguration)
    val vertex_input = fs.open(vertex_yaml_path)
    val vertex_yaml = new Yaml(new Constructor(classOf[VertexInfo]))
    val vertex_info = vertex_yaml.load(vertex_input).asInstanceOf[VertexInfo]

    // construct the vertex reader
    val spark_session = SparkSession.builder()
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()
    val prefix : String = "/tmp/neo4j/"
    val reader = new VertexReader(prefix, vertex_info, spark_session)

    // reading chunks for all property groups
    val vertex_df = reader.readAllVertexPropertyGroups(false)
    // display the DataFrame and its schema
    vertex_df.show()
    vertex_df.printSchema()

    // use the dataframe to insert/update vertices to Neo4j
    /*vertex_df.write.format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Overwrite)
      .option("labels", ":Person")
      .option("node.keys", "name")
      .save() */
  }
}
