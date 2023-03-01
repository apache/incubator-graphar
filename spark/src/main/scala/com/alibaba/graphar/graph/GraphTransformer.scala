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

import com.alibaba.graphar.{GeneralParams, AdjListType, GraphInfo, VertexInfo, EdgeInfo}
import com.alibaba.graphar.reader.{VertexReader, EdgeReader}
import com.alibaba.graphar.writer.{VertexWriter, EdgeWriter}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/** The helper object for transforming graphs through the definitions of their infos. */
object GraphTransformer {
  /** Construct the map of (vertex label -> VertexInfo) for a graph. */
  private def constructVertexInfoMap(prefix: String, graphInfo: GraphInfo, spark: SparkSession): Map[String, VertexInfo] = {
    var vertex_infos_map: Map[String, VertexInfo] = Map()
    val vertices_yaml = graphInfo.getVertices
    val vertices_it = vertices_yaml.iterator
    while (vertices_it.hasNext()) {
      val file_name = vertices_it.next()
      val path = prefix + file_name
      val vertex_info = VertexInfo.loadVertexInfo(path, spark)
      vertex_infos_map += (vertex_info.getLabel -> vertex_info)
    }
    return vertex_infos_map
  }

  /** Construct the map of (edge label -> EdgeInfo) for a graph. */
  private def constructEdgeInfoMap(prefix: String, graphInfo: GraphInfo, spark: SparkSession): Map[String, EdgeInfo] = {
    var edge_infos_map: Map[String, EdgeInfo] = Map()
    val edges_yaml = graphInfo.getEdges
    val edges_it = edges_yaml.iterator
    while (edges_it.hasNext()) {
      val file_name = edges_it.next()
      val path = prefix + file_name
      val edge_info = EdgeInfo.loadEdgeInfo(path, spark)
      val key = edge_info.getSrc_label + GeneralParams.regularSeperator + edge_info.getEdge_label + GeneralParams.regularSeperator + edge_info.getDst_label
      edge_infos_map += (key -> edge_info)
    }
    return edge_infos_map
  }

  /** Transform the vertex chunks following the meta data defined in graph info objects.
   *
   * @param sourceGraphInfo The info object for the source graph.
   * @param destGraphInfo The info object for the destination graph.
   * @param sourceVertexInfosMap The map of (vertex label -> VertexInfo) for the source graph.
   * @param spark The Spark session for the transformer.
   */
  private def transformAllVertices(sourceGraphInfo: GraphInfo, destGraphInfo: GraphInfo, sourceVertexInfosMap: Map[String, VertexInfo], spark: SparkSession): Unit = {
    val source_prefix = sourceGraphInfo.getPrefix
    val dest_prefix = destGraphInfo.getPrefix

    // traverse vertex infos of the destination graph
    val dest_vertices_it = destGraphInfo.getVertices.iterator
    while (dest_vertices_it.hasNext()) {
      // load dest edge info
      val path = dest_prefix + dest_vertices_it.next()
      val dest_vertex_info = VertexInfo.loadVertexInfo(path, spark)
      // load source vertex info
      val label = dest_vertex_info.getLabel()
      if (!sourceVertexInfosMap.contains(label)) {
        throw new IllegalArgumentException
      }
      val source_vertex_info = sourceVertexInfosMap(label)
      // read vertex chunks from the source graph
      val reader = new VertexReader(source_prefix, source_vertex_info, spark)
      val df = reader.readAllVertexPropertyGroups(true)
      // write vertex chunks for the dest graph
      val writer = new VertexWriter(dest_prefix, dest_vertex_info, df)
      writer.writeVertexProperties()
    }
  }

  /** Transform the edge chunks following the meta data defined in graph info objects.
   *
   * @param sourceGraphInfo The info object for the source graph.
   * @param destGraphInfo The info object for the destination graph.
   * @param sourceVertexInfosMap The map of (vertex label -> VertexInfo) for the source graph.
   * @param sourceEdgeInfosMap The map of (edge label -> EdgeInfo) for the source graph.
   * @param spark The Spark session for the transformer.
   */
  private def transformAllEdges(sourceGraphInfo: GraphInfo, destGraphInfo: GraphInfo, sourceVertexInfosMap: Map[String, VertexInfo], sourceEdgeInfosMap: Map[String, EdgeInfo], spark: SparkSession): Unit = {
    val source_prefix = sourceGraphInfo.getPrefix
    val dest_prefix = destGraphInfo.getPrefix

    // traverse edge infos of the destination graph
    val dest_edges_it = destGraphInfo.getEdges.iterator
    while (dest_edges_it.hasNext()) {
      // load dest edge info
      val path = dest_prefix + dest_edges_it.next()
      val dest_edge_info = EdgeInfo.loadEdgeInfo(path, spark)
      // load source edge info
      val key = dest_edge_info.getSrc_label + GeneralParams.regularSeperator + dest_edge_info.getEdge_label + GeneralParams.regularSeperator + dest_edge_info.getDst_label
      if (!sourceEdgeInfosMap.contains(key)) {
        throw new IllegalArgumentException
      }
      val source_edge_info = sourceEdgeInfosMap(key)
      var has_loaded = false
      var df = spark.emptyDataFrame

      // traverse all adjList types
      val dest_adj_lists = dest_edge_info.getAdj_lists
      val adj_list_it = dest_adj_lists.iterator
      while (adj_list_it.hasNext()) {
        val dest_adj_list_type = adj_list_it.next().getAdjList_type_in_gar

        // load edge DataFrame from the source graph
        if (!has_loaded) {
          val source_adj_lists = source_edge_info.getAdj_lists
          var source_adj_list_type = dest_adj_list_type
          if (!source_edge_info.containAdjList(dest_adj_list_type))
            if (source_adj_lists.size() > 0)
              source_adj_list_type = source_adj_lists.get(0).getAdjList_type_in_gar
          // read edge chunks from source graph
          val reader = new EdgeReader(source_prefix, source_edge_info, source_adj_list_type, spark)
          df = reader.readEdges(false)
          has_loaded = true
        }

        // read vertices number
        val vertex_label = {
          if (dest_adj_list_type == AdjListType.ordered_by_source || dest_adj_list_type == AdjListType.unordered_by_source) 
            dest_edge_info.getSrc_label 
          else 
            dest_edge_info.getDst_label
        }
        if (!sourceVertexInfosMap.contains(vertex_label)) {
          throw new IllegalArgumentException
        }
        val vertex_info = sourceVertexInfosMap(vertex_label)
        val reader = new VertexReader(source_prefix, vertex_info, spark)
        val vertex_num = reader.readVerticesNumber()

        // write edge chunks for dest graph
        val writer = new EdgeWriter(dest_prefix, dest_edge_info, dest_adj_list_type, vertex_num, df)
        writer.writeEdges()
      }
    }
  }

  /** Transform the graphs following the meta data defined in graph info objects.
   *
   * @param sourceGraphInfo The info object for the source graph.
   * @param destGraphInfo The info object for the destination graph.
   * @param spark The Spark session for the transformer.
   */
  def transform(sourceGraphInfo: GraphInfo, destGraphInfo: GraphInfo, spark: SparkSession): Unit = {
    val source_prefix = sourceGraphInfo.getPrefix
    val dest_prefix = destGraphInfo.getPrefix

    // construct the (vertex label -> vertex info) map for the source graph
    val source_vertex_infos_map = constructVertexInfoMap(source_prefix, sourceGraphInfo, spark)
    // construct the (edge label -> edge info) map for the source graph
    val source_edge_infos_map = constructEdgeInfoMap(source_prefix, sourceGraphInfo, spark)

    // transform and generate vertex data chunks
    transformAllVertices(sourceGraphInfo, destGraphInfo, source_vertex_infos_map, spark)

    // transform and generate edge data chunks
    transformAllEdges(sourceGraphInfo, destGraphInfo, source_vertex_infos_map, source_edge_infos_map, spark)
  }

  /** Transform the graphs following the meta data defined in info files.
   *
   * @param sourceGraphInfoPath The path of the graph info yaml file for the source graph.
   * @param destGraphInfoPath The path of the graph info yaml file for the destination graph.
   * @param spark The Spark session for the transformer.
   */
  def transform(sourceGraphInfoPath: String, destGraphInfoPath: String, spark: SparkSession): Unit = {
    // load source graph info
    val source_graph_info = GraphInfo.loadGraphInfo(sourceGraphInfoPath, spark)

    // load dest graph info
    val dest_graph_info = GraphInfo.loadGraphInfo(destGraphInfoPath, spark)

    // conduct transformation
    transform(source_graph_info, dest_graph_info, spark)
  }
}
