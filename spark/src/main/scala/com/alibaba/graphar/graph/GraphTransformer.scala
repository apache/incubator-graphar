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

package com.alibaba.graphar.graph

import com.alibaba.graphar.{AdjListType, GraphInfo, VertexInfo, EdgeInfo}
import com.alibaba.graphar.reader.{VertexReader, EdgeReader}
import com.alibaba.graphar.writer.{VertexWriter, EdgeWriter}

import org.apache.spark.sql.SparkSession

/**
 * The helper object for transforming graphs through the definitions of their
 * infos.
 */
object GraphTransformer {

  /**
   * Transform the vertex chunks following the meta data defined in graph info
   * objects.
   *
   * @param sourceGraphInfo
   *   The info object for the source graph.
   * @param destGraphInfo
   *   The info object for the destination graph.
   * @param sourceVertexInfosMap
   *   The map of (vertex label -> VertexInfo) for the source graph.
   * @param spark
   *   The Spark session for the transformer.
   */
  private def transformAllVertices(
      sourceGraphInfo: GraphInfo,
      destGraphInfo: GraphInfo,
      sourceVertexInfosMap: Map[String, VertexInfo],
      spark: SparkSession
  ): Unit = {
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
        throw new IllegalArgumentException(
          "vertex info of " + label + " not found in graph info."
        )
      }
      val source_vertex_info = sourceVertexInfosMap(label)
      // read vertex chunks from the source graph
      val reader = new VertexReader(source_prefix, source_vertex_info, spark)
      val df = reader.readAllVertexPropertyGroups()
      df.persist(GeneralParams.defaultStorageLevel)
      // write vertex chunks for the dest graph
      val writer = new VertexWriter(dest_prefix, dest_vertex_info, df)
      writer.writeVertexProperties()
      df.unpersist()
    }
  }

  /**
   * Transform the edge chunks following the meta data defined in graph info
   * objects.
   *
   * @param sourceGraphInfo
   *   The info object for the source graph.
   * @param destGraphInfo
   *   The info object for the destination graph.
   * @param sourceVertexInfosMap
   *   The map of (vertex label -> VertexInfo) for the source graph.
   * @param sourceEdgeInfosMap
   *   The map of (edge label -> EdgeInfo) for the source graph.
   * @param spark
   *   The Spark session for the transformer.
   */
  private def transformAllEdges(
      sourceGraphInfo: GraphInfo,
      destGraphInfo: GraphInfo,
      sourceVertexInfosMap: Map[String, VertexInfo],
      sourceEdgeInfosMap: Map[String, EdgeInfo],
      spark: SparkSession
  ): Unit = {
    val source_prefix = sourceGraphInfo.getPrefix
    val dest_prefix = destGraphInfo.getPrefix

    // traverse edge infos of the destination graph
    val dest_edges_it = destGraphInfo.getEdges.iterator
    while (dest_edges_it.hasNext()) {
      // load dest edge info
      val path = dest_prefix + dest_edges_it.next()
      val dest_edge_info = EdgeInfo.loadEdgeInfo(path, spark)
      // load source edge info
      val key = dest_edge_info.getConcatKey()
      if (!sourceEdgeInfosMap.contains(key)) {
        throw new IllegalArgumentException(
          "edge info of " + key + " not found in graph info."
        )
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
              source_adj_list_type =
                source_adj_lists.get(0).getAdjList_type_in_gar
          // read edge chunks from source graph
          val reader = new EdgeReader(
            source_prefix,
            source_edge_info,
            source_adj_list_type,
            spark
          )
          df = reader.readEdges(false)
          has_loaded = true
          df.persist(GeneralParams.defaultStorageLevel)
        }

        // read vertices number
        val vertex_label = {
          if (
            dest_adj_list_type == AdjListType.ordered_by_source || dest_adj_list_type == AdjListType.unordered_by_source
          )
            dest_edge_info.getSrc_label
          else
            dest_edge_info.getDst_label
        }
        if (!sourceVertexInfosMap.contains(vertex_label)) {
          throw new IllegalArgumentException(
            "vertex info of " + vertex_label + " not found in graph info."
          )
        }
        val vertex_info = sourceVertexInfosMap(vertex_label)
        val reader = new VertexReader(source_prefix, vertex_info, spark)
        val vertex_num = reader.readVerticesNumber()

        // write edge chunks for dest graph
        val writer = new EdgeWriter(
          dest_prefix,
          dest_edge_info,
          dest_adj_list_type,
          vertex_num,
          df
        )
        writer.writeEdges()
        df.unpersist()
      }
    }
  }

  /**
   * Transform the graphs following the meta data defined in graph info objects.
   *
   * @param sourceGraphInfo
   *   The info object for the source graph.
   * @param destGraphInfo
   *   The info object for the destination graph.
   * @param spark
   *   The Spark session for the transformer.
   */
  def transform(
      sourceGraphInfo: GraphInfo,
      destGraphInfo: GraphInfo,
      spark: SparkSession
  ): Unit = {
    // transform and generate vertex data chunks
    transformAllVertices(
      sourceGraphInfo,
      destGraphInfo,
      sourceGraphInfo.getVertexInfos(),
      spark
    )

    // transform and generate edge data chunks
    transformAllEdges(
      sourceGraphInfo,
      destGraphInfo,
      sourceGraphInfo.getVertexInfos(),
      sourceGraphInfo.getEdgeInfos(),
      spark
    )
  }

  /**
   * Transform the graphs following the meta data defined in info files.
   *
   * @param sourceGraphInfoPath
   *   The path of the graph info yaml file for the source graph.
   * @param destGraphInfoPath
   *   The path of the graph info yaml file for the destination graph.
   * @param spark
   *   The Spark session for the transformer.
   */
  def transform(
      sourceGraphInfoPath: String,
      destGraphInfoPath: String,
      spark: SparkSession
  ): Unit = {
    // load source graph info
    val source_graph_info = GraphInfo.loadGraphInfo(sourceGraphInfoPath, spark)

    // load dest graph info
    val dest_graph_info = GraphInfo.loadGraphInfo(destGraphInfoPath, spark)

    // conduct transformation
    transform(source_graph_info, dest_graph_info, spark)
  }
}
