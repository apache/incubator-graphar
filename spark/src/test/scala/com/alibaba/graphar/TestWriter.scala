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

class WriterSuite extends AnyFunSuite {
  val spark = SparkSession.builder()
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  test("test vertex writer with only vertex table") {
    // read vertex dataframe
    val file_path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/person_0_0.csv").getPath
    val vertex_df = spark.read.option("delimiter", "|").option("header", "true").csv(file_path)

    // read graph yaml
    val graph_yaml_path = new Path(getClass.getClassLoader.getResource("gar-test/ldbc_sample/parquet/ldbc_sample.graph.yml").getPath)
    val fs = FileSystem.get(graph_yaml_path.toUri(), spark.sparkContext.hadoopConfiguration)
    val graph_input = fs.open(graph_yaml_path)
    val graph_yaml = new Yaml(new Constructor(classOf[GraphInfo]))
    val graph_info = graph_yaml.load(graph_input).asInstanceOf[GraphInfo]

    // read vertex yaml
    val vertex_yaml_path = new Path(getClass.getClassLoader.getResource("gar-test/ldbc_sample/parquet/person.vertex.yml").getPath)
    val vertex_input = fs.open(vertex_yaml_path)
    val vertex_yaml = new Yaml(new Constructor(classOf[VertexInfo]))
    val vertex_info = vertex_yaml.load(vertex_input).asInstanceOf[VertexInfo]

    // generate vertex index column for vertex dataframe
    val vertex_df_with_index = IndexGenerator.generateVertexIndexColumn(vertex_df)

    // create writer object for person and generate the properties with GAR format
    val writer = new VertexWriter(graph_info.getPrefix(), vertex_info, vertex_df_with_index)

    // write certain property group
    val property_group = vertex_info.getPropertyGroup("id")
    writer.writeVertexProperties(property_group)
    val id_chunk_path = new Path(graph_info.getPrefix() + vertex_info.getDirPath(property_group) + "chunk*")
    val id_chunk_files = fs.globStatus(id_chunk_path)
    assert(id_chunk_files.length == 10)
    fs.delete(id_chunk_path, true)
    writer.writeVertexProperties()
    val chunk_path = new Path(graph_info.getPrefix() + vertex_info.getPrefix() + "*/*")
    val chunk_files = fs.globStatus(chunk_path)
    assert(chunk_files.length == 20)

    assertThrows[IllegalArgumentException](new VertexWriter(graph_info.getPrefix(), vertex_info, vertex_df))
    val invalid_property_group= new PropertyGroup()
    assertThrows[IllegalArgumentException](writer.writeVertexProperties(invalid_property_group))

    // close FileSystem instance
    fs.close()
  }

  test("test edge writer with only edge table") {
    // read edge dataframe
    val file_path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/person_knows_person_0_0.csv").getPath
    val edge_df = spark.read.option("delimiter", "|").option("header", "true").csv(file_path)

    // read graph yaml
    val graph_yaml_path = new Path(getClass.getClassLoader.getResource("gar-test/ldbc_sample/parquet/ldbc_sample.graph.yml").getPath)
    val fs = FileSystem.get(graph_yaml_path.toUri(), spark.sparkContext.hadoopConfiguration)
    val graph_input = fs.open(graph_yaml_path)
    val graph_yaml = new Yaml(new Constructor(classOf[GraphInfo]))
    val graph_info = graph_yaml.load(graph_input).asInstanceOf[GraphInfo]

    // read edge yaml
    val edge_yaml_path = new Path(getClass.getClassLoader.getResource("gar-test/ldbc_sample/parquet/person_knows_person.edge.yml").getPath)
    val edge_input = fs.open(edge_yaml_path)
    val edge_yaml = new Yaml(new Constructor(classOf[EdgeInfo]))
    val edge_info = edge_yaml.load(edge_input).asInstanceOf[EdgeInfo]

    // generate vertex index for edge dataframe
    val edge_df_with_index = IndexGenerator.generateSrcAndDstIndexUnitedlyForEdges(edge_df, "src", "dst")

    // create writer object for person_knows_person and generate the adj list and properties with GAR format
    val writer = new EdgeWriter(graph_info.getPrefix(), edge_info, AdjListType.ordered_by_source, edge_df_with_index)
    writer.writeEdges()

    // throw exception if not generate src index and dst index for edge dataframe
    assertThrows[IllegalArgumentException](new EdgeWriter(graph_info.getPrefix(), edge_info, AdjListType.ordered_by_source, edge_df))
    // throw exception if pass the adj list type not contain in edge info
    assertThrows[IllegalArgumentException](new EdgeWriter(graph_info.getPrefix(), edge_info, AdjListType.unorded_by_dest, edge_df_with_index))

    // close FileSystem instance
    fs.close()
  }

  test("test edge writer with vertex table and edge table") {
    // read vertex dataframe
    val vertex_file_path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/person_0_0.csv").getPath
    val vertex_df = spark.read.option("delimiter", "|").option("header", "true").csv(vertex_file_path)

    // read edge dataframe
    val file_path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/person_knows_person_0_0.csv").getPath
    val edge_df = spark.read.option("delimiter", "|").option("header", "true").csv(file_path)


    // read graph yaml
    val graph_yaml_path = new Path(getClass.getClassLoader.getResource("gar-test/ldbc_sample/parquet/ldbc_sample.graph.yml").getPath)
    val fs = FileSystem.get(graph_yaml_path.toUri(), spark.sparkContext.hadoopConfiguration)
    val graph_input = fs.open(graph_yaml_path)
    val graph_yaml = new Yaml(new Constructor(classOf[GraphInfo]))
    val graph_info = graph_yaml.load(graph_input).asInstanceOf[GraphInfo]

    // read vertex yaml
    val vertex_yaml_path = new Path(getClass.getClassLoader.getResource("gar-test/ldbc_sample/parquet/person.vertex.yml").getPath)
    val vertex_input = fs.open(vertex_yaml_path)
    val vertex_yaml = new Yaml(new Constructor(classOf[VertexInfo]))
    val vertex_info = vertex_yaml.load(vertex_input).asInstanceOf[VertexInfo]

    // read edge yaml
    val edge_yaml_path = new Path(getClass.getClassLoader.getResource("gar-test/ldbc_sample/parquet/person_knows_person.edge.yml").getPath)
    val edge_input = fs.open(edge_yaml_path)
    val edge_yaml = new Yaml(new Constructor(classOf[EdgeInfo]))
    val edge_info = edge_yaml.load(edge_input).asInstanceOf[EdgeInfo]

    // construct person vertex mapping with dataframe
    val vertex_mapping = IndexGenerator.constructVertexIndexMapping(vertex_df, vertex_info.getPrimaryKey())
    // generate src index and dst index for edge datafram with vertex mapping
    val edge_df_with_src_index = IndexGenerator.generateSrcIndexForEdgesFromMapping(edge_df, "src", vertex_mapping)
    val edge_df_with_src_dst_index = IndexGenerator.generateDstIndexForEdgesFromMapping(edge_df_with_src_index, "dst", vertex_mapping)

    // create writer object for person_knows_person and generate the adj list and properties with GAR format
    val writer = new EdgeWriter(graph_info.getPrefix(), edge_info, AdjListType.ordered_by_source, edge_df_with_src_dst_index)
    writer.writeEdges()

    // close FileSystem instance
    fs.close()
  }
}
