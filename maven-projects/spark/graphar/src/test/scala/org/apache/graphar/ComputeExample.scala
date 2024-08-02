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

import org.apache.spark.graphx._

class ComputeExampleSuite extends BaseTestSuite {

  test("run cc using graphx") {
    // read vertex DataFrame
    val prefix = testData + "/ldbc_sample/parquet/"
    val vertex_yaml = prefix + "/person.vertex.yml"
    val vertex_info = VertexInfo.loadVertexInfo(vertex_yaml, spark)

    val vertex_reader = new VertexReader(prefix, vertex_info, spark)
    val vertices_num = vertex_reader.readVerticesNumber()
    val vertex_df = vertex_reader.readAllVertexPropertyGroups()
    vertex_df.show()
    assert(vertex_df.columns.size == 5)
    assert(vertex_df.count() == vertices_num)

    // read edge DataFrame
    val edge_yaml = prefix + "person_knows_person.edge.yml"
    val edge_info = EdgeInfo.loadEdgeInfo(edge_yaml, spark)
    val adj_list_type = AdjListType.ordered_by_source

    val edge_reader = new EdgeReader(prefix, edge_info, adj_list_type, spark)
    val edges_num = edge_reader.readEdgesNumber()
    val edge_df = edge_reader.readAllAdjList(false)
    edge_df.show()
    assert(edge_reader.readVerticesNumber() == vertices_num)
    assert(edge_df.columns.size == 2)
    assert(edge_df.count() == edges_num)

    // construct the graph for GraphX
    val vertex_rdd: VertexRDD[String] = VertexRDD(
      vertex_df.rdd.map(i =>
        (i(0).asInstanceOf[Number].longValue, i(1).toString)
      )
    )
    val edge_rdd = edge_df.rdd.map(i =>
      (i(0).asInstanceOf[Number].longValue, i(1).asInstanceOf[Number].longValue)
    )
    val graph = Graph.fromEdgeTuples[Null](edge_rdd, null)
    // find the connected components
    val cc = graph.connectedComponents().vertices
    val ccById = vertex_rdd.leftOuterJoin(cc).map {
      case (index, (id, Some(cc))) => (id, cc)
      case (index, (id, None))     => (id, index)
    }
    // print the result
    println(ccById.collect().mkString("\n"))
  }

}
