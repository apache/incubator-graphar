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

import com.alibaba.graphar.GraphInfo
import com.alibaba.graphar.graph.GraphWriter
import com.alibaba.graphar.utils

import java.io.{File, FileInputStream}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.fs.{Path, FileSystem}
import org.scalatest.funsuite.AnyFunSuite

class TestGraphWriterSuite extends AnyFunSuite {
  val spark = SparkSession.builder()
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  test("write graphs by graph infos") {
    // load graph info
    val path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/parquet/ldbc_sample.graph.yml").getPath
    val graph_info = GraphInfo.loadGraphInfo(path, spark)
    val prefix = "/tmp/test_graph_writer"
    graph_info.setPrefix(prefix)  // avoid overwite gar-test files
    val fs = FileSystem.get(new Path(prefix).toUri(), spark.sparkContext.hadoopConfiguration)

    // prepare the dataframes
    val vertex_file_path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/person_0_0.csv").getPath
    val vertex_df = spark.read.option("delimiter", "|").option("header", "true").csv(vertex_file_path)
    val vertex_dataframes: Map[String, DataFrame] = Map("person" -> vertex_df)
    val file_path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/person_knows_person_0_0.csv").getPath
    val edge_df = spark.read.option("delimiter", "|").option("header", "true").csv(file_path)
    val edge_dataframes: Map[String, DataFrame] = Map("person_knows_person" -> edge_df)

    // conduct writing
    GraphWriter.write(graph_info, vertex_dataframes, edge_dataframes, spark)
    val vertex_info = graph_info.getVertexInfo("person")
    val chunk_path = new Path(prefix + vertex_info.getPrefix() + "*/*")
    val chunk_files = fs.globStatus(chunk_path)
    assert(chunk_files.length == 20)
    val vertex_num_path = prefix + vertex_info.getVerticesNumFilePath()
    val number = utils.FileSystem.readValue(vertex_num_path, spark.sparkContext.hadoopConfiguration)
    assert(number.toInt == vertex_df.count())

    val edgeInfos = graph_info.getEdgeInfos()
    edgeInfos.foreach { case (key, edgeInfo) => {
      val adj_lists = edgeInfo.getAdj_lists
      val adj_list_it = adj_lists.iterator
      while (adj_list_it.hasNext()) {
        val adj_list = adj_list_it.next()
        val adj_list_type = adj_list.getAdjList_type_in_gar
        val adj_list_type_str = adj_list.getAdjList_type
        val adj_list_path_pattern = new Path(prefix + edgeInfo.getAdjListPathPrefix(adj_list_type) + "*/*")
        val adj_list_chunk_files = fs.globStatus(adj_list_path_pattern)
        assert(adj_list_chunk_files.length > 0)
        if (adj_list_type == AdjListType.ordered_by_source || adj_list_type == AdjListType.ordered_by_dest) {
          val offset_path_pattern = new Path(prefix + edgeInfo.getOffsetPathPrefix(adj_list_type) + "*")
          val offset_chunk_files = fs.globStatus(offset_path_pattern)
          assert(offset_chunk_files.length > 0)
        }
      }
    }}

    // cleaning generated files
    fs.delete(new Path(prefix))
    fs.close()
  }
}
