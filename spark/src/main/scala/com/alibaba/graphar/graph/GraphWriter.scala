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

package com.alibaba.graphar.graph

import com.alibaba.graphar.{AdjListType, GraphInfo, VertexInfo, EdgeInfo}
import com.alibaba.graphar.writer.{VertexWriter, EdgeWriter}
import com.alibaba.graphar.utils.IndexGenerator

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/** The helper object for write graph through the definitions of their infos. */
object GraphWriter {
  private def writeAllVertices(prefix: String,
                               vertexInfos: Map[String, VertexInfo],
                               vertex_num_map: Map[String, Long],
                               vertexDataFrames: Map[String, DataFrame],
                               spark: SparkSession): Unit = {
    vertexInfos.foreach { case (label, vertexInfo) => {
      val vertex_num = vertex_num_map(label)
      val df_with_index = IndexGenerator.generateVertexIndexColumn(vertexDataFrames(label))
      val writer = new VertexWriter(prefix, vertexInfo, df_with_index, Some(vertex_num))
      writer.writeVertexProperties()
    }}
  }

  private def writeAllEdges(prefix: String,
                            vertexInfos: Map[String, VertexInfo],
                            edgeInfos: Map[String, EdgeInfo],
                            vertex_num_map: Map[String, Long],
                            vertexDataFrames: Map[String, DataFrame],
                            edgeDataFrames: Map[String, DataFrame],
                            spark: SparkSession): Unit = {
    edgeInfos.foreach { case (key, edgeInfo) => {
      val srcLabel = edgeInfo.getSrc_label
      val dstLabel = edgeInfo.getDst_label
      val edge_key = edgeInfo.getConcatKey()
      val src_vertex_index_mapping = IndexGenerator.constructVertexIndexMapping(vertexDataFrames(srcLabel), vertexInfos(srcLabel).getPrimaryKey())
      val dst_vertex_index_mapping = {
        if (srcLabel == dstLabel)
          src_vertex_index_mapping
        else
          IndexGenerator.constructVertexIndexMapping(vertexDataFrames(dstLabel), vertexInfos(dstLabel).getPrimaryKey())
      }
      val edge_df_with_index = IndexGenerator.generateSrcAndDstIndexForEdgesFromMapping(edgeDataFrames(edge_key), src_vertex_index_mapping, dst_vertex_index_mapping)

      val adj_lists = edgeInfo.getAdj_lists
      val adj_list_it = adj_lists.iterator
      while (adj_list_it.hasNext()) {
        val adj_list_type = adj_list_it.next().getAdjList_type_in_gar
        val vertex_num = {
          if (adj_list_type == AdjListType.ordered_by_source || adj_list_type == AdjListType.unordered_by_source) {
            vertex_num_map(srcLabel)
          } else {
            vertex_num_map(dstLabel)
          }
        }
        val writer = new EdgeWriter(prefix, edgeInfo, adj_list_type, vertex_num, edge_df_with_index)
        writer.writeEdges()
      }
    }}
  }

  def write(graphInfo: GraphInfo, vertexDataFrames: Map[String, DataFrame], edgeDataFrames: Map[String, DataFrame], spark: SparkSession): Unit = {
    // get the vertex num of each vertex dataframe
    val vertex_num_map: Map[String, Long] = vertexDataFrames.map { case (k, v) => (k, v.count()) }
    val prefix = graphInfo.getPrefix
    val vertex_infos = graphInfo.getVertexInfos()
    val edge_infos = graphInfo.getEdgeInfos()

    // write vertices
    writeAllVertices(prefix, vertex_infos, vertex_num_map, vertexDataFrames, spark)

    // write edges
    writeAllEdges(prefix, vertex_infos, edge_infos, vertex_num_map, vertexDataFrames, edgeDataFrames, spark)
  }

  def write(graphInfoPath: String, vertexDataFrames: Map[String, DataFrame], edgeDataFrames: Map[String, DataFrame], spark: SparkSession): Unit = {
    // load graph info
    val graph_info = GraphInfo.loadGraphInfo(graphInfoPath, spark)

    // conduct writing
    write(graph_info, vertexDataFrames, edgeDataFrames, spark)
  }
}
