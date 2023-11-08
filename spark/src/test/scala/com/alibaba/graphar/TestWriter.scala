/**
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.alibaba.graphar

import com.alibaba.graphar.writer.{VertexWriter, EdgeWriter}

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.apache.hadoop.fs.{Path, FileSystem}
import scala.io.Source.fromFile

class WriterSuite extends AnyFunSuite {
  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  test("test vertex writer with only vertex table") {
    // read vertex DataFrame
    val file_path = getClass.getClassLoader
      .getResource("gar-test/ldbc_sample/person_0_0.csv")
      .getPath
    val vertex_df = spark.read
      .option("delimiter", "|")
      .option("header", "true")
      .csv(file_path)
    val fs = FileSystem.get(
      new Path(file_path).toUri(),
      spark.sparkContext.hadoopConfiguration
    )

    // read vertex yaml
    val vertex_yaml_path = getClass.getClassLoader
      .getResource("gar-test/ldbc_sample/parquet/person.vertex.yml")
      .getPath
    val vertex_info = VertexInfo.loadVertexInfo(vertex_yaml_path, spark)

    // generate vertex index column for vertex DataFrame
    val vertex_df_with_index =
      util.IndexGenerator.generateVertexIndexColumn(vertex_df)

    // create writer object for person and generate the properties with GAR format
    val prefix: String = "/tmp/"
    val writer = new VertexWriter(prefix, vertex_info, vertex_df_with_index)

    // write certain property group
    val property_group = vertex_info.getPropertyGroup("id")
    writer.writeVertexProperties(property_group)
    val id_chunk_path =
      new Path(prefix + vertex_info.getPathPrefix(property_group) + "chunk*")
    val id_chunk_files = fs.globStatus(id_chunk_path)
    assert(id_chunk_files.length == 10)
    writer.writeVertexProperties()
    val chunk_path = new Path(prefix + vertex_info.getPrefix() + "*/*")
    val chunk_files = fs.globStatus(chunk_path)
    assert(chunk_files.length == 20)
    val vertex_num_path = prefix + vertex_info.getVerticesNumFilePath()
    val number = util.FileSystem.readValue(
      vertex_num_path,
      spark.sparkContext.hadoopConfiguration
    )
    assert(number.toInt == vertex_df.count())

    assertThrows[IllegalArgumentException](
      new VertexWriter(prefix, vertex_info, vertex_df)
    )
    val invalid_property_group = new PropertyGroup()
    assertThrows[IllegalArgumentException](
      writer.writeVertexProperties(invalid_property_group)
    )

    // clean generated files and close FileSystem instance
    fs.delete(new Path(prefix + "vertex"))
    fs.close()
  }

  test("test edge writer with only edge table") {
    // read edge DataFrame
    val file_path = getClass.getClassLoader
      .getResource("gar-test/ldbc_sample/person_knows_person_0_0.csv")
      .getPath
    val edge_df = spark.read
      .option("delimiter", "|")
      .option("header", "true")
      .csv(file_path)
    val prefix: String = "/tmp/test1/"
    val fs = FileSystem.get(
      new Path(file_path).toUri(),
      spark.sparkContext.hadoopConfiguration
    )

    // read edge yaml
    val edge_yaml_path = getClass.getClassLoader
      .getResource("gar-test/ldbc_sample/csv/person_knows_person.edge.yml")
      .getPath
    val edge_info = EdgeInfo.loadEdgeInfo(edge_yaml_path, spark)
    val adj_list_type = AdjListType.ordered_by_source

    // generate vertex index for edge DataFrame
    val srcDf = edge_df.select("src").withColumnRenamed("src", "vertex")
    val dstDf = edge_df.select("dst").withColumnRenamed("dst", "vertex")
    val vertex_num = srcDf.union(dstDf).distinct().count()
    val vertex_chunk_size = edge_info.getSrc_chunk_size()
    val vertex_chunk_num =
      (vertex_num + vertex_chunk_size - 1) / vertex_chunk_size
    val edge_df_with_index = util.IndexGenerator
      .generateSrcAndDstIndexUnitedlyForEdges(edge_df, "src", "dst")

    // create writer object for person_knows_person and generate the adj list and properties with GAR format
    val writer = new EdgeWriter(
      prefix,
      edge_info,
      adj_list_type,
      vertex_num,
      edge_df_with_index
    )

    // test write adj list
    writer.writeAdjList()

    // validate vertex number & edge number
    val vertex_num_path =
      prefix + edge_info.getVerticesNumFilePath(adj_list_type)
    val number = util.FileSystem.readValue(
      vertex_num_path,
      spark.sparkContext.hadoopConfiguration
    )
    assert(number.toInt == vertex_num)
    val edge_num_path_pattern =
      new Path(prefix + edge_info.getEdgesNumPathPrefix(adj_list_type) + "*")
    val edge_num_files = fs.globStatus(edge_num_path_pattern)
    assert(edge_num_files.length == vertex_chunk_num)
    val edge_num = edge_num_files
      .map(file =>
        util.FileSystem
          .readValue(
            file.getPath().toString(),
            spark.sparkContext.hadoopConfiguration
          )
          .toInt
      )
      .sum
    assert(edge_num == edge_df.count())

    // validate number of chunk files
    val adj_list_path_pattern =
      new Path(prefix + edge_info.getAdjListPathPrefix(adj_list_type) + "*/*")
    val adj_list_chunk_files = fs.globStatus(adj_list_path_pattern)
    assert(adj_list_chunk_files.length == 9)
    val offset_path_pattern =
      new Path(prefix + edge_info.getOffsetPathPrefix(adj_list_type) + "*")
    val offset_chunk_files = fs.globStatus(offset_path_pattern)
    assert(offset_chunk_files.length == vertex_chunk_num)

    // test write property group
    val property_group =
      edge_info.getPropertyGroup("creationDate", adj_list_type)
    writer.writeEdgeProperties(property_group)

    val property_group_path_pattern = new Path(
      prefix + edge_info.getPropertyGroupPathPrefix(
        property_group,
        adj_list_type
      ) + "*/*"
    )
    val property_group_chunk_files = fs.globStatus(property_group_path_pattern)
    assert(property_group_chunk_files.length == 9)

    // test write edges
    writer.writeEdges()

    val invalid_property_group = new PropertyGroup()

    assertThrows[IllegalArgumentException](
      writer.writeEdgeProperties(invalid_property_group)
    )
    // throw exception if not generate src index and dst index for edge DataFrame
    assertThrows[IllegalArgumentException](
      new EdgeWriter(
        prefix,
        edge_info,
        AdjListType.ordered_by_source,
        vertex_num,
        edge_df
      )
    )
    // throw exception if pass the adj list type not contain in edge info
    assertThrows[IllegalArgumentException](
      new EdgeWriter(
        prefix,
        edge_info,
        AdjListType.unordered_by_dest,
        vertex_num,
        edge_df_with_index
      )
    )

    // clean generated files and close FileSystem instance
    fs.delete(new Path(prefix + "edge"))

    fs.close()
  }

  test("test edge writer with vertex table and edge table") {
    // read vertex DataFrame
    val vertex_file_path = getClass.getClassLoader
      .getResource("gar-test/ldbc_sample/person_0_0.csv")
      .getPath
    val vertex_df = spark.read
      .option("delimiter", "|")
      .option("header", "true")
      .csv(vertex_file_path)
    val vertex_num = vertex_df.count()

    // read edge DataFrame
    val file_path = getClass.getClassLoader
      .getResource("gar-test/ldbc_sample/person_knows_person_0_0.csv")
      .getPath
    val edge_df = spark.read
      .option("delimiter", "|")
      .option("header", "true")
      .csv(file_path)

    val prefix: String = "/tmp/test2/"
    val fs = FileSystem.get(
      new Path(prefix).toUri(),
      spark.sparkContext.hadoopConfiguration
    )
    val adj_list_type = AdjListType.ordered_by_source

    // read vertex yaml
    val vertex_yaml_path = getClass.getClassLoader
      .getResource("gar-test/ldbc_sample/csv/person.vertex.yml")
      .getPath
    val vertex_info = VertexInfo.loadVertexInfo(vertex_yaml_path, spark)

    // read edge yaml
    val edge_yaml_path = getClass.getClassLoader
      .getResource("gar-test/ldbc_sample/csv/person_knows_person.edge.yml")
      .getPath
    val edge_info = EdgeInfo.loadEdgeInfo(edge_yaml_path, spark)
    val vertex_chunk_size = edge_info.getSrc_chunk_size()
    val vertex_chunk_num =
      (vertex_num + vertex_chunk_size - 1) / vertex_chunk_size

    // construct person vertex mapping with DataFrame
    val vertex_mapping = util.IndexGenerator.constructVertexIndexMapping(
      vertex_df,
      vertex_info.getPrimaryKey()
    )
    // generate src index and dst index for edge DataFrame with vertex mapping
    val edge_df_with_src_index = util.IndexGenerator
      .generateSrcIndexForEdgesFromMapping(edge_df, "src", vertex_mapping)
    val edge_df_with_src_dst_index =
      util.IndexGenerator.generateDstIndexForEdgesFromMapping(
        edge_df_with_src_index,
        "dst",
        vertex_mapping
      )

    // create writer object for person_knows_person and generate the adj list and properties with GAR format
    val writer = new EdgeWriter(
      prefix,
      edge_info,
      adj_list_type,
      vertex_num,
      edge_df_with_src_dst_index
    )

    // test write adj list
    writer.writeAdjList()

    // validate vertex number & edge number
    val vertex_num_path =
      prefix + edge_info.getVerticesNumFilePath(adj_list_type)
    val number = util.FileSystem.readValue(
      vertex_num_path,
      spark.sparkContext.hadoopConfiguration
    )
    assert(number.toInt == vertex_num)
    val edge_num_path_pattern =
      new Path(prefix + edge_info.getEdgesNumPathPrefix(adj_list_type) + "*")
    val edge_num_files = fs.globStatus(edge_num_path_pattern)
    assert(edge_num_files.length == vertex_chunk_num)
    val edge_num = edge_num_files
      .map(file =>
        util.FileSystem
          .readValue(
            file.getPath().toString(),
            spark.sparkContext.hadoopConfiguration
          )
          .toInt
      )
      .sum
    assert(edge_num == edge_df.count())

    // validate adj list chunks
    val adj_list_path_pattern =
      new Path(prefix + edge_info.getAdjListPathPrefix(adj_list_type) + "*/*")
    val adj_list_chunk_files = fs.globStatus(adj_list_path_pattern)
    assert(adj_list_chunk_files.length == 11)
    // validate offset chunks
    val offset_path_pattern =
      new Path(prefix + edge_info.getOffsetPathPrefix(adj_list_type) + "*")
    val offset_chunk_files = fs.globStatus(offset_path_pattern)
    assert(offset_chunk_files.length == vertex_chunk_num)
    // compare with correct offset chunk value
    val offset_file_path =
      prefix + edge_info.getAdjListOffsetFilePath(0, adj_list_type)
    val correct_offset_file_path = getClass.getClassLoader
      .getResource(
        "gar-test/ldbc_sample/csv/edge/person_knows_person/ordered_by_source/offset/chunk0"
      )
      .getPath
    val generated_offset_array = fromFile(offset_file_path).getLines.toArray
    val expected_offset_array =
      fromFile(correct_offset_file_path).getLines.toArray
    assert(generated_offset_array.sameElements(expected_offset_array))

    // test write property group
    val property_group =
      edge_info.getPropertyGroup("creationDate", adj_list_type)
    writer.writeEdgeProperties(property_group)
    val property_group_path_pattern = new Path(
      prefix + edge_info.getPropertyGroupPathPrefix(
        property_group,
        adj_list_type
      ) + "*/*"
    )
    val property_group_chunk_files = fs.globStatus(property_group_path_pattern)
    assert(property_group_chunk_files.length == 11)

    writer.writeEdges()

    // clean generated files and close FileSystem instance
    fs.delete(new Path(prefix + "edge"))
    fs.close()
  }
}
