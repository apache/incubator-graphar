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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class ReaderSuite extends AnyFunSuite {
  val spark = SparkSession.builder()
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  test("read chunk files directly") {
    // read parquet files (vertex chunks)
    val parquet_file_path = "gar-test/ldbc_sample/parquet"
    val parquet_prefix = getClass.getClassLoader.getResource(parquet_file_path).getPath
    val parqeut_read_path = parquet_prefix + "/vertex/person/id"
    val df1 = spark.read.option("fileFormat", "parquet").format("com.alibaba.graphar.datasources.GarDataSource").load(parqeut_read_path)
    // validate reading results
    assert(df1.rdd.getNumPartitions == 10)
    assert(df1.count() == 903)
    //println(df1.rdd.collect().mkString("\n"))

    // read orc files (vertex chunks)
    val orc_file_path = "gar-test/ldbc_sample/orc"
    val orc_prefix = getClass.getClassLoader.getResource(orc_file_path).getPath
    val orc_read_path = orc_prefix + "/vertex/person/id"
    val df2 = spark.read.option("fileFormat", "orc").format("com.alibaba.graphar.datasources.GarDataSource").load(orc_read_path)
    // compare
    assert(df2.rdd.collect().deep == df1.rdd.collect().deep)

    // read csv files recursively (edge chunks)
    val csv_file_path = "gar-test/ldbc_sample/csv"
    val csv_prefix = getClass.getClassLoader.getResource(csv_file_path).getPath
    val csv_read_path = csv_prefix + "/edge/person_knows_person/ordered_by_source/adj_list"
    val df3 = spark.read.option("fileFormat", "csv").option("recursiveFileLookup", "true").format("com.alibaba.graphar.datasources.GarDataSource").load(csv_read_path)
    // validate reading results
    assert(df3.rdd.getNumPartitions == 11)
    assert(df3.count() == 6626)

    // throw an exception for unsupported file formats
    assertThrows[IllegalArgumentException](spark.read.option("fileFormat", "invalid").format("com.alibaba.graphar.datasources.GarDataSource").load(csv_read_path))
  }

  test("read vertex chunks") {
    val file_path = "gar-test/ldbc_sample/csv"
    val prefix = getClass.getClassLoader.getResource(file_path).getPath
    val vertex_input = getClass.getClassLoader.getResourceAsStream(file_path + "/person.vertex.yml")
    val vertex_yaml = new Yaml(new Constructor(classOf[VertexInfo]))
    val vertex_info = vertex_yaml.load(vertex_input).asInstanceOf[VertexInfo]

    val reader = new VertexReader(prefix, vertex_info, spark)
    assert(reader.readVerticesNumber() == 903)
    val property_group = vertex_info.getPropertyGroup("gender")
    val single_chunk_df = reader.readVertexPropertyChunk(property_group, 0)
    assert(single_chunk_df.columns.size == 3)
    assert(single_chunk_df.count() == 100)
    val property_df = reader.readVertexProperties(property_group)
    assert(property_df.columns.size == 3)
    assert(property_df.count() == 903)
    val vertex_df = reader.readAllVertexProperties()
    vertex_df.show()
    assert(vertex_df.columns.size == 4)
    assert(vertex_df.count() == 903)
    val vertex_df_with_index = reader.readAllVertexProperties(true)
    vertex_df_with_index.show()
    assert(vertex_df_with_index.columns.size == 5)
    assert(vertex_df_with_index.count() == 903)

    val invalid_property_group= new PropertyGroup()
    assertThrows[IllegalArgumentException](reader.readVertexPropertyChunk(invalid_property_group, 0))
    assertThrows[IllegalArgumentException](reader.readVertexProperties(invalid_property_group))
  }

  test("read edge chunks") {
    val file_path = "gar-test/ldbc_sample/csv"
    val prefix = getClass.getClassLoader.getResource(file_path).getPath
    val edge_input = getClass.getClassLoader.getResourceAsStream(file_path + "/person_knows_person.edge.yml")
    val edge_yaml = new Yaml(new Constructor(classOf[EdgeInfo]))
    val edge_info = edge_yaml.load(edge_input).asInstanceOf[EdgeInfo]

    val adj_list_type = AdjListType.ordered_by_source
    val reader = new EdgeReader(prefix, edge_info, adj_list_type, spark)
    val offset_df = reader.readOffset(0)
    assert(offset_df.columns.size == 1)
    assert(offset_df.count() == 101)
    val single_adj_list_df = reader.readAdjListChunk(2, 0)
    assert(single_adj_list_df.columns.size == 2)
    assert(single_adj_list_df.count() == 1024)
    val adj_list_df_chunk_2 = reader.readAdjListForVertexChunk(2)
    assert(adj_list_df_chunk_2.columns.size == 2)
    assert(adj_list_df_chunk_2.count() == 1077)
    val adj_list_df = reader.readAllAdjList()
    assert(adj_list_df.columns.size == 2)
    assert(adj_list_df.count() == 6626)

    val property_group = edge_info.getPropertyGroup("creationDate", adj_list_type)
    val single_property_df = reader.readEdgePropertyChunk(property_group, 2, 0)
    assert(single_property_df.columns.size == 1)
    assert(single_property_df.count() == 1024)
    val property_df_chunk_2 = reader.readEdgePropertiesForVertexChunk(property_group, 2)
    assert(property_df_chunk_2.columns.size == 1)
    assert(property_df_chunk_2.count() == 1077)
    val property_df = reader.readEdgeProperties(property_group)
    assert(property_df.columns.size == 1)
    assert(property_df.count() == 6626)
    val all_property_df_chunk_2 = reader.readAllEdgePropertiesForVertexChunk(2)
    assert(all_property_df_chunk_2.columns.size == 1)
    assert(all_property_df_chunk_2.count() == 1077)
    val all_property_df = reader.readAllEdgeProperties()
    assert(all_property_df.columns.size == 1)
    assert(all_property_df.count() == 6626)

    val edge_df_chunk_2 = reader.readEdgesForVertexChunk(2)
    edge_df_chunk_2.show()
    assert(edge_df_chunk_2.columns.size == 3)
    assert(edge_df_chunk_2.count() == 1077)
    val edge_df_chunk_2_with_index = reader.readEdgesForVertexChunk(2, true)
    edge_df_chunk_2_with_index.show()
    assert(edge_df_chunk_2_with_index.columns.size == 4)
    assert(edge_df_chunk_2_with_index.count() == 1077)
    val edge_df = reader.readEdges()
    edge_df.show()
    assert(edge_df.columns.size == 3)
    assert(edge_df.count() == 6626)
    val edge_df_with_index = reader.readEdges(true)
    edge_df_with_index.show()
    assert(edge_df_with_index.columns.size == 4)
    assert(edge_df_with_index.count() == 6626)

    val invalid_property_group= new PropertyGroup()
    assertThrows[IllegalArgumentException](reader.readEdgePropertyChunk(invalid_property_group, 0, 0))
    assertThrows[IllegalArgumentException](reader.readEdgePropertiesForVertexChunk(invalid_property_group, 0))
    assertThrows[IllegalArgumentException](reader.readEdgeProperties(invalid_property_group))

    val invalid_adj_list_type = AdjListType.unordered_by_dest
    assertThrows[IllegalArgumentException](new EdgeReader(prefix, edge_info, invalid_adj_list_type, spark))
  }

}
