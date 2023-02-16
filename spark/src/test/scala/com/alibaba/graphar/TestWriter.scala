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

import com.alibaba.graphar.utils.IndexGenerator
import com.alibaba.graphar.writer.{VertexWriter, EdgeWriter}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import org.apache.hadoop.fs.{Path, FileSystem}
import scala.io.Source.fromFile
import org.apache.spark.sql.internal.SQLConf.FILE_COMMIT_PROTOCOL_CLASS


class WriterSuite extends AnyFunSuite {
  val spark = SparkSession.builder()
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  test("test vertex writer with only vertex table") {
    // read vertex dataframe
    val file_path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/person_0_0.csv").getPath
    val vertex_df = spark.read.option("delimiter", "|").option("header", "true").csv(file_path)
    val fs = FileSystem.get(new Path(file_path).toUri(), spark.sparkContext.hadoopConfiguration)

    // read vertex yaml
    val vertex_yaml_path = new Path(getClass.getClassLoader.getResource("gar-test/ldbc_sample/parquet/person.vertex.yml").getPath)
    val vertex_input = fs.open(vertex_yaml_path)
    val vertex_yaml = new Yaml(new Constructor(classOf[VertexInfo]))
    val vertex_info = vertex_yaml.load(vertex_input).asInstanceOf[VertexInfo]

    // generate vertex index column for vertex dataframe
    val vertex_df_with_index = IndexGenerator.generateVertexIndexColumn(vertex_df)

    // create writer object for person and generate the properties with GAR format
    val prefix : String = "/tmp/"
    val writer = new VertexWriter(prefix, vertex_info, vertex_df_with_index)

    // write certain property group
    val property_group = vertex_info.getPropertyGroup("id")
    writer.writeVertexProperties(property_group)
    val id_chunk_path = new Path(prefix + vertex_info.getPathPrefix(property_group) + "chunk*")
    val id_chunk_files = fs.globStatus(id_chunk_path)
    assert(id_chunk_files.length == 10)
    writer.writeVertexProperties()
    val chunk_path = new Path(prefix + vertex_info.getPrefix() + "*/*")
    val chunk_files = fs.globStatus(chunk_path)
    assert(chunk_files.length == 20)

    assertThrows[IllegalArgumentException](new VertexWriter(prefix, vertex_info, vertex_df))
    val invalid_property_group= new PropertyGroup()
    assertThrows[IllegalArgumentException](writer.writeVertexProperties(invalid_property_group))

    // clean generated files and close FileSystem instance
    // fs.delete(new Path(prefix + "vertex"))
    fs.close()
  }

  test("test edge writer with only edge table") {
    // read edge dataframe
    val file_path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/person_knows_person_0_0.csv").getPath
    val edge_df = spark.read.option("delimiter", "|").option("header", "true").csv(file_path)
    val prefix : String = "/tmp/"
    val fs = FileSystem.get(new Path(file_path).toUri(), spark.sparkContext.hadoopConfiguration)

    // read edge yaml
    val edge_yaml_path = new Path(getClass.getClassLoader.getResource("gar-test/ldbc_sample/csv/person_knows_person.edge.yml").getPath)
    val edge_input = fs.open(edge_yaml_path)
    val edge_yaml = new Yaml(new Constructor(classOf[EdgeInfo]))
    val edge_info = edge_yaml.load(edge_input).asInstanceOf[EdgeInfo]
    val adj_list_type = AdjListType.ordered_by_source

    // generate vertex index for edge dataframe
    val edge_df_with_index = IndexGenerator.generateSrcAndDstIndexUnitedlyForEdges(edge_df, "src", "dst")

    // create writer object for person_knows_person and generate the adj list and properties with GAR format
    val writer = new EdgeWriter(prefix, edge_info, adj_list_type, edge_df_with_index)

    // test write adj list
    writer.writeAdjList()

    val adj_list_path_pattern = new Path(prefix + edge_info.getAdjListPathPrefix(adj_list_type) + "*/*")
    val adj_list_chunk_files = fs.globStatus(adj_list_path_pattern)
    assert(adj_list_chunk_files.length == 9)
    val offset_path_pattern = new Path(prefix + edge_info.getOffsetPathPrefix(adj_list_type) + "*")
    val offset_chunk_files = fs.globStatus(offset_path_pattern)
    assert(offset_chunk_files.length == 7)

    // test write property group
    val property_group = edge_info.getPropertyGroup("creationDate", adj_list_type)
    writer.writeEdgeProperties(property_group)

    val property_group_path_pattern = new Path(prefix + edge_info.getPropertyGroupPathPrefix(property_group, adj_list_type) + "*/*")
    val property_group_chunk_files = fs.globStatus(property_group_path_pattern)
    assert(property_group_chunk_files.length == 9)

    // test write edges
    writer.writeEdges()

    val invalid_property_group = new PropertyGroup()

    assertThrows[IllegalArgumentException](writer.writeEdgeProperties(invalid_property_group))
    // throw exception if not generate src index and dst index for edge dataframe
    assertThrows[IllegalArgumentException](new EdgeWriter(prefix, edge_info, AdjListType.ordered_by_source, edge_df))
    // throw exception if pass the adj list type not contain in edge info
    assertThrows[IllegalArgumentException](new EdgeWriter(prefix, edge_info, AdjListType.unordered_by_dest, edge_df_with_index))

    // clean generated files and close FileSystem instance
    fs.delete(new Path(prefix + "edge"))

    fs.close()
  }

  test("test edge writer with vertex table and edge table") {
    // read vertex dataframe
    val vertex_file_path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/person_0_0.csv").getPath
    val vertex_df = spark.read.option("delimiter", "|").option("header", "true").csv(vertex_file_path)

    // read edge dataframe
    val file_path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/person_knows_person_0_0.csv").getPath
    val edge_df = spark.read.option("delimiter", "|").option("header", "true").csv(file_path)

    val prefix : String = "/tmp/test2/"
    val fs = FileSystem.get(new Path(prefix).toUri(), spark.sparkContext.hadoopConfiguration)
    val adj_list_type = AdjListType.ordered_by_source

    // read vertex yaml
    val vertex_yaml_path = new Path(getClass.getClassLoader.getResource("gar-test/ldbc_sample/csv/person.vertex.yml").getPath)
    val vertex_input = fs.open(vertex_yaml_path)
    val vertex_yaml = new Yaml(new Constructor(classOf[VertexInfo]))
    val vertex_info = vertex_yaml.load(vertex_input).asInstanceOf[VertexInfo]

    // read edge yaml
    val edge_yaml_path = new Path(getClass.getClassLoader.getResource("gar-test/ldbc_sample/csv/person_knows_person.edge.yml").getPath)
    val edge_input = fs.open(edge_yaml_path)
    val edge_yaml = new Yaml(new Constructor(classOf[EdgeInfo]))
    val edge_info = edge_yaml.load(edge_input).asInstanceOf[EdgeInfo]

    // construct person vertex mapping with dataframe
    val vertex_mapping = IndexGenerator.constructVertexIndexMapping(vertex_df, vertex_info.getPrimaryKey())
    // generate src index and dst index for edge datafram with vertex mapping
    val edge_df_with_src_index = IndexGenerator.generateSrcIndexForEdgesFromMapping(edge_df, "src", vertex_mapping)
    val edge_df_with_src_dst_index = IndexGenerator.generateDstIndexForEdgesFromMapping(edge_df_with_src_index, "dst", vertex_mapping)

    // create writer object for person_knows_person and generate the adj list and properties with GAR format
    val writer = new EdgeWriter(prefix, edge_info, adj_list_type, edge_df_with_src_dst_index)

    // test write adj list
    writer.writeEdges()

    // clean generated files and close FileSystem instance
    fs.delete(new Path(prefix + "edge"))
    fs.close()
  }

  test("test new edge writer") {
    // read vertex dataframe
    val vertex_file_path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/person_0_0.csv").getPath
    val vertex_df = spark.read.option("delimiter", "|").option("header", "true").csv(vertex_file_path)

    // read edge dataframe
    val file_path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/person_knows_person_0_0.csv").getPath
    val edge_df = spark.read.option("delimiter", "|").option("header", "true").csv(file_path)

    val prefix : String = "/tmp/"
    val fs = FileSystem.get(new Path(prefix).toUri(), spark.sparkContext.hadoopConfiguration)
    val adj_list_type = AdjListType.ordered_by_source

    // read vertex yaml
    val vertex_yaml_path = new Path(getClass.getClassLoader.getResource("gar-test/ldbc_sample/csv/person.vertex.yml").getPath)
    val vertex_input = fs.open(vertex_yaml_path)
    val vertex_yaml = new Yaml(new Constructor(classOf[VertexInfo]))
    val vertex_info = vertex_yaml.load(vertex_input).asInstanceOf[VertexInfo]

    // read edge yaml
    val edge_yaml_path = new Path(getClass.getClassLoader.getResource("gar-test/ldbc_sample/csv/person_knows_person.edge.yml").getPath)
    val edge_input = fs.open(edge_yaml_path)
    val edge_yaml = new Yaml(new Constructor(classOf[EdgeInfo]))
    val edge_info = edge_yaml.load(edge_input).asInstanceOf[EdgeInfo]

    // construct person vertex mapping with dataframe
    val vertex_mapping = IndexGenerator.constructVertexIndexMapping(vertex_df, vertex_info.getPrimaryKey())
    // generate src index and dst index for edge datafram with vertex mapping
    val edge_df_with_src_index = IndexGenerator.generateSrcIndexForEdgesFromMapping(edge_df, "src", vertex_mapping)
    val edge_df_with_src_dst_index = IndexGenerator.generateDstIndexForEdgesFromMapping(edge_df_with_src_index, "dst", vertex_mapping)

    val writer = new EdgeWriter(prefix, edge_info, adj_list_type, edge_df_with_src_dst_index)
  }
}
