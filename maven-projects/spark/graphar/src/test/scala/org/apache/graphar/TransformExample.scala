/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.graphar

import org.apache.graphar.reader.{VertexReader, EdgeReader}
import org.apache.graphar.writer.{VertexWriter, EdgeWriter}

import org.apache.hadoop.fs.{Path, FileSystem}

class TransformExampleSuite extends BaseTestSuite {

  test("transform file type") {
    // read from orc files
    val prefix = testData + "/ldbc_sample/orc/"
    val vertex_yaml = prefix + "person.vertex.yml"
    val vertex_info = VertexInfo.loadVertexInfo(vertex_yaml, Some(spark))

    val reader = new VertexReader(prefix, vertex_info, spark)
    val vertices_num = reader.readVerticesNumber()
    val vertex_df_with_index = reader.readAllVertexPropertyGroups()
    assert(vertex_df_with_index.count() == vertices_num)

    // write to parquet files
    val output_prefix: String = "/tmp/example/"
    val output_vertex_yaml = testData + "/ldbc_sample/parquet/person.vertex.yml"
    val output_vertex_info =
      VertexInfo.loadVertexInfo(output_vertex_yaml, Some(spark))

    val writer =
      new VertexWriter(output_prefix, output_vertex_info, vertex_df_with_index)
    writer.writeVertexProperties()
    val chunk_path =
      new Path(output_prefix + output_vertex_info.getPrefix() + "*/*")
    val fs =
      FileSystem.get(chunk_path.toUri(), spark.sparkContext.hadoopConfiguration)
    val chunk_files = fs.globStatus(chunk_path)
    assert(chunk_files.length == 20)

    // clean generated files and close FileSystem instance
    fs.delete(new Path(output_prefix + "vertex"))
    fs.close()
  }

  test("transform adjList type") {
    val prefix = testData + "/ldbc_sample/parquet/"
    // get vertex num
    val vertex_yaml = prefix + "person.vertex.yml"
    val vertex_info = VertexInfo.loadVertexInfo(vertex_yaml, Some(spark))
    // construct the vertex reader
    val vreader = new VertexReader(prefix, vertex_info, spark)
    val vertexNum = vreader.readVerticesNumber()
    // read edges of unordered_by_source type
    val edge_yaml = prefix + "person_knows_person.edge.yml"
    val edge_info = EdgeInfo.loadEdgeInfo(edge_yaml, Some(spark))

    val adj_list_type = AdjListType.unordered_by_source
    val reader = new EdgeReader(prefix, edge_info, adj_list_type, spark)
    val edgeNum = reader.readEdgesNumber()
    val adj_list_df = reader.readAllAdjList(false)
    assert(adj_list_df.columns.size == 2)
    assert(adj_list_df.count() == edgeNum)
    assert(vertexNum == reader.readVerticesNumber)

    // write edges in ordered_by_source type
    val output_adj_list_type = AdjListType.ordered_by_source
    val output_prefix: String = "/tmp/example/"
    val writer = new EdgeWriter(
      output_prefix,
      edge_info,
      output_adj_list_type,
      vertexNum,
      adj_list_df
    )
    writer.writeAdjList()
    // validate the output files
    val adj_list_path_pattern = new Path(
      output_prefix + edge_info.getAdjListPathPrefix(
        output_adj_list_type
      ) + "*/*"
    )
    val fs = FileSystem.get(
      adj_list_path_pattern.toUri(),
      spark.sparkContext.hadoopConfiguration
    )
    val adj_list_chunk_files = fs.globStatus(adj_list_path_pattern)
    assert(adj_list_chunk_files.length == 11)
    val offset_path_pattern = new Path(
      output_prefix + edge_info.getOffsetPathPrefix(output_adj_list_type) + "*"
    )
    val offset_chunk_files = fs.globStatus(offset_path_pattern)
    assert(offset_chunk_files.length == 10)
    // validate vertex number & edge number
    val vertex_num_path =
      output_prefix + edge_info.getVerticesNumFilePath(output_adj_list_type)
    val number = util.FileSystem.readValue(
      vertex_num_path,
      spark.sparkContext.hadoopConfiguration
    )
    assert(number.toInt == vertexNum)
    val edge_num_path_pattern = new Path(
      output_prefix + edge_info.getEdgesNumPathPrefix(
        output_adj_list_type
      ) + "*"
    )
    val edge_num_files = fs.globStatus(edge_num_path_pattern)
    val tot_num = edge_num_files
      .map(file =>
        util.FileSystem
          .readValue(
            file.getPath().toString(),
            spark.sparkContext.hadoopConfiguration
          )
          .toInt
      )
      .sum
    assert(tot_num == edgeNum)

    // clean generated files and close FileSystem instance
    fs.delete(new Path(output_prefix + "edge"))
    fs.close()
  }

}
