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

import org.apache.graphar.graph.GraphReader

class TestGraphReaderSuite extends BaseTestSuite {

  test("read graphs by yaml paths") {
    // conduct reading
    val graph_path = testData + "/ldbc_sample/parquet/ldbc_sample.graph.yml"
    val vertex_edge_df_pair = GraphReader.read(graph_path, spark)
    val vertex_dataframes = vertex_edge_df_pair._1
    val edge_dataframes = vertex_edge_df_pair._2

    assert(vertex_dataframes.size == 1)
    assert(vertex_dataframes contains "person")
    val person_df =
      vertex_dataframes("person").drop(GeneralParams.vertexIndexCol)
    assert(person_df.columns.size == 4)
    assert(person_df.count() == 903)

    assert(edge_dataframes.size == 1)
    assert(edge_dataframes contains ("person", "knows", "person"))
    val adj_list_type_dataframes =
      edge_dataframes(("person", "knows", "person"))
    assert(adj_list_type_dataframes.size == 3)
  }

  test("read graphs by graph infos") {
    // load graph info
    val path = testData + "/ldbc_sample/parquet/ldbc_sample.graph.yml"
    val graph_info = GraphInfo.loadGraphInfo(path, spark)

    // conduct reading
    val vertex_edge_df_pair = GraphReader.readWithGraphInfo(graph_info, spark)
    val vertex_dataframes = vertex_edge_df_pair._1
    val edge_dataframes = vertex_edge_df_pair._2

    assert(vertex_dataframes.size == 1)
    assert(vertex_dataframes contains "person")
    val person_df =
      vertex_dataframes("person").drop(GeneralParams.vertexIndexCol)
    assert(person_df.columns.size == 4)
    assert(person_df.count() == 903)

    val edgeInfos = graph_info.getEdgeInfos()
    assert(edge_dataframes.size == edgeInfos.size)
    edgeInfos.foreach {
      case (key, edgeInfo) => {
        val edge_tag = (
          edgeInfo.getSrc_type(),
          edgeInfo.getEdge_type(),
          edgeInfo.getDst_type()
        )
        assert(edge_dataframes contains edge_tag)
        val adj_list_type_dataframes = edge_dataframes(edge_tag)
        val adj_lists = edgeInfo.getAdj_lists
        assert(adj_list_type_dataframes.size == adj_lists.size)
        val adj_list_it = adj_lists.iterator
        while (adj_list_it.hasNext()) {
          val adj_list = adj_list_it.next()
          val adj_list_type = adj_list.getAdjList_type_in_gar
          val adj_list_type_str = adj_list.getAdjList_type
          assert(adj_list_type_dataframes contains adj_list_type_str)
          val df = adj_list_type_dataframes(adj_list_type_str)
          assert(df.count == 6626)
        }
      }
    }
  }
}
