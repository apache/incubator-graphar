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

package com.alibaba.graphar

import com.alibaba.graphar.graph.GraphTransformer

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{Path, FileSystem}
import org.scalatest.funsuite.AnyFunSuite

class TestGraphTransformerSuite extends AnyFunSuite {
  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  test("transform graphs by yaml paths") {
    // conduct transformation
    val source_path = getClass.getClassLoader
      .getResource("gar-test/new/ldbc_sample/parquet/ldbc_sample.graph.yml")
      .getPath
    val dest_path = getClass.getClassLoader
      .getResource("gar-test/transformer/ldbc_sample_new.graph.yml")
      .getPath
    GraphTransformer.transform(source_path, dest_path, spark)

    val dest_graph_info = GraphInfo.loadGraphInfo(dest_path, spark)
    val prefix = dest_graph_info.getPrefix
    val fs = FileSystem.get(
      new Path(prefix).toUri(),
      spark.sparkContext.hadoopConfiguration
    )

    // validate vertex chunks
    val vertex_chunk_path = new Path(prefix + "vertex/person/" + "*/*")
    val vertex_chunk_files = fs.globStatus(vertex_chunk_path)
    assert(vertex_chunk_files.length == 38)
    // validate edge chunks
    val adj_list_chunk_path = new Path(
      prefix + "edge/person_knows_person/unordered_by_dest/adj_list/" + "*/*"
    )
    val adj_list_chunk_files = fs.globStatus(adj_list_chunk_path)
    assert(adj_list_chunk_files.length == 20)
    val edge_chunk_path = new Path(
      prefix + "edge/person_knows_person/ordered_by_source/creationDate/" + "*/*"
    )
    val edge_chunk_files = fs.globStatus(edge_chunk_path)
    assert(edge_chunk_files.length == 20)

    // clean generated files and close FileSystem instance
    fs.delete(new Path(prefix + "vertex"))
    fs.delete(new Path(prefix + "edge"))
    fs.close()
  }

  test("transform graphs by graph infos") {
    // load source graph info
    val source_path = getClass.getClassLoader
      .getResource("gar-test/ldbc_sample/parquet/ldbc_sample.graph.yml")
      .getPath
    val source_graph_info = GraphInfo.loadGraphInfo(source_path, spark)

    // load dest graph info
    val dest_path = getClass.getClassLoader
      .getResource("gar-test/transformer/ldbc_sample_new.graph.yml")
      .getPath
    val dest_graph_info = GraphInfo.loadGraphInfo(dest_path, spark)

    // conduct transformation
    GraphTransformer.transform(source_graph_info, dest_graph_info, spark)

    val prefix = dest_graph_info.getPrefix
    val fs = FileSystem.get(
      new Path(prefix).toUri(),
      spark.sparkContext.hadoopConfiguration
    )

    // validate vertex chunks
    val vertex_chunk_path = new Path(prefix + "vertex/person/" + "*/*")
    val vertex_chunk_files = fs.globStatus(vertex_chunk_path)
    assert(vertex_chunk_files.length == 38)
    // validate edge chunks
    val adj_list_chunk_path = new Path(
      prefix + "edge/person_knows_person/unordered_by_dest/adj_list/" + "*/*"
    )
    val adj_list_chunk_files = fs.globStatus(adj_list_chunk_path)
    assert(adj_list_chunk_files.length == 20)
    val edge_chunk_path = new Path(
      prefix + "edge/person_knows_person/ordered_by_source/creationDate/" + "*/*"
    )
    val edge_chunk_files = fs.globStatus(edge_chunk_path)
    assert(edge_chunk_files.length == 20)

    // clean generated files and close FileSystem instance
    fs.delete(new Path(prefix + "vertex"))
    fs.delete(new Path(prefix + "edge"))
    fs.close()
  }
}
