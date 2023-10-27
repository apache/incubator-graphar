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

package com.alibaba.graphar.graph

import com.alibaba.graphar.{AdjListType, GraphInfo, GeneralParams}
import com.alibaba.graphar.writer.{VertexWriter, EdgeWriter}
import com.alibaba.graphar.util.IndexGenerator
import com.alibaba.graphar.util.Utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedWriter, OutputStreamWriter}

/** GraphWriter is a class to help to write graph data in graph format. */
class GraphWriter() {

  /**
   * Put the vertex DataFrame into writer.
   *
   * @param label
   *   label of vertex.
   * @param df
   *   DataFrame of the vertex type.
   * @param primaryKey
   *   primary key of the vertex type, default is empty, which take the first
   *   property column as primary key.
   */
  def PutVertexData(
      label: String,
      df: DataFrame,
      primaryKey: String = ""
  ): Unit = {
    if (vertices.exists(_._1 == label)) {
      throw new IllegalArgumentException
    }
    vertices += label -> df
    vertexNums += label -> df.count
    primaryKeys += label -> primaryKey
  }

  /**
   * Put the egde datafrme into writer.
   * @param relation
   *   3-Tuple (source label, edge label, target label) to indicate edge type.
   * @param df
   *   data frame of edge type.
   */
  def PutEdgeData(relation: (String, String, String), df: DataFrame): Unit = {
    if (edges.exists(_._1 == relation)) {
      throw new IllegalArgumentException
    }
    edges += relation -> df
  }

  /**
   * Write the graph data in graphar format with graph info.
   * @param graphInfo
   *   the graph info object for the graph.
   * @param spark
   *   the spark session for the writing.
   */
  def write(graphInfo: GraphInfo, spark: SparkSession): Unit = {
    val vertexInfos = graphInfo.getVertexInfos()
    val edgeInfos = graphInfo.getEdgeInfos()
    val prefix = graphInfo.getPrefix()
    var indexMappings: scala.collection.mutable.Map[String, DataFrame] =
      scala.collection.mutable.Map[String, DataFrame]()
    vertexInfos.foreach {
      case (label, vertexInfo) => {
        val vertex_num = vertexNums(label)
        val primaryKey = primaryKeys(label)
        val df_and_mapping = IndexGenerator
          .generateVertexIndexColumnAndIndexMapping(vertices(label), primaryKey)
        val df_with_index = df_and_mapping._1
        indexMappings += label -> df_and_mapping._2
        val writer =
          new VertexWriter(prefix, vertexInfo, df_with_index, Some(vertex_num))
        writer.writeVertexProperties()
      }
    }

    edgeInfos.foreach {
      case (key, edgeInfo) => {
        val srcLabel = edgeInfo.getSrc_label
        val dstLabel = edgeInfo.getDst_label
        val edgeLabel = edgeInfo.getEdge_label
        val src_vertex_index_mapping = indexMappings(srcLabel)
        val dst_vertex_index_mapping = {
          if (srcLabel == dstLabel)
            src_vertex_index_mapping
          else
            indexMappings(dstLabel)
        }
        val edge_df_with_index =
          IndexGenerator.generateSrcAndDstIndexForEdgesFromMapping(
            edges((srcLabel, edgeLabel, dstLabel)),
            src_vertex_index_mapping,
            dst_vertex_index_mapping
          )

        val adj_lists = edgeInfo.getAdj_lists
        val adj_list_it = adj_lists.iterator
        while (adj_list_it.hasNext()) {
          val adj_list_type = adj_list_it.next().getAdjList_type_in_gar
          val vertex_num = {
            if (
              adj_list_type == AdjListType.ordered_by_source || adj_list_type == AdjListType.unordered_by_source
            ) {
              vertexNums(srcLabel)
            } else {
              vertexNums(dstLabel)
            }
          }
          val writer = new EdgeWriter(
            prefix,
            edgeInfo,
            adj_list_type,
            vertex_num,
            edge_df_with_index
          )
          writer.writeEdges()
        }
      }
    }
  }

  /**
   * Write the graph data in graphar format with path of the graph info yaml.
   * @param graphInfoPath
   *   the path of the graph info yaml.
   * @param spark
   *   the spark session for the writing.
   */
  def write(graphInfoPath: String, spark: SparkSession): Unit = {
    // load graph info
    val graph_info = GraphInfo.loadGraphInfo(graphInfoPath, spark)
    write(graph_info, spark)
  }

  /**
   * Write graph data in graphar format.
   *
   * @param path
   *   the directory to write.
   * @param spark
   *   the spark session for the writing.
   * @param name
   *   the name of graph, default is 'grpah'
   * @param vertex_chunk_size
   *   the chunk size for vertices, default is 2^18
   * @param edge_chunk_size
   *   the chunk size for edges, default is 2^22
   * @param file_type
   *   the file type for data payload file, support [parquet, orc, csv], default
   *   is parquet.
   * @param version
   *   version of graphar format, default is v1.
   */
  def write(
      path: String,
      spark: SparkSession,
      name: String = "graph",
      vertex_chunk_size: Long = GeneralParams.defaultVertexChunkSize,
      edge_chunk_size: Long = GeneralParams.defaultEdgeChunkSize,
      file_type: String = GeneralParams.defaultFileType,
      version: String = GeneralParams.defaultVersion
  ): Unit = {
    val vertex_schemas: scala.collection.mutable.Map[String, StructType] =
      scala.collection.mutable.Map[String, StructType]()
    val edge_schemas
        : scala.collection.mutable.Map[(String, String, String), StructType] =
      scala.collection.mutable.Map[(String, String, String), StructType]()
    vertices.foreach {
      case (key, df) => {
        vertex_schemas += key -> df.schema
      }
    }
    edges.foreach {
      case (key, df) => {
        edge_schemas += key -> new StructType(
          df.schema.drop(2).toArray
        ) // drop the src, dst fileds
      }
    }
    val graph_info = Utils.generateGraphInfo(
      path,
      name,
      true,
      vertex_chunk_size,
      edge_chunk_size,
      file_type,
      version,
      vertex_schemas,
      edge_schemas,
      primaryKeys
    )
    // dump infos to file
    saveInfoToFile(graph_info, spark)
    // write out the data
    write(graph_info, spark)
  }

  private def saveInfoToFile(
      graphInfo: GraphInfo,
      spark: SparkSession
  ): Unit = {
    val vertexInfos = graphInfo.getVertexInfos()
    val edgeInfos = graphInfo.getEdgeInfos()
    val prefix = graphInfo.getPrefix()
    val fs = FileSystem.get(
      new Path(prefix).toUri(),
      spark.sparkContext.hadoopConfiguration
    )
    vertexInfos.foreach {
      case (key, vertexInfo) => {
        val yamlString = vertexInfo.dump()
        val filePath = new Path(prefix + key + ".vertex.yml")
        val outputStream = fs.create(filePath)
        val writer = new BufferedWriter(new OutputStreamWriter(outputStream))
        writer.write(yamlString)
        writer.close()
        outputStream.close()
      }
    }
    edgeInfos.foreach {
      case (key, edgeInfo) => {
        val yamlString = edgeInfo.dump()
        val filePath = new Path(prefix + key + ".edge.yml")
        val outputStream = fs.create(filePath)
        val writer = new BufferedWriter(new OutputStreamWriter(outputStream))
        writer.write(yamlString)
        writer.close()
        outputStream.close()
      }
    }

    val yamlString = graphInfo.dump()
    val filePath = new Path(prefix + "/" + graphInfo.getName() + ".graph.yml")
    val outputStream = fs.create(filePath)
    val writer = new BufferedWriter(new OutputStreamWriter(outputStream))
    writer.write(yamlString)
    writer.close()
    outputStream.close()
  }

  val vertices: scala.collection.mutable.Map[String, DataFrame] =
    scala.collection.mutable.Map[String, DataFrame]()
  val edges: scala.collection.mutable.Map[(String, String, String), DataFrame] =
    scala.collection.mutable.Map[(String, String, String), DataFrame]()
  val vertexNums: scala.collection.mutable.Map[String, Long] =
    scala.collection.mutable.Map[String, Long]()
  val primaryKeys: scala.collection.mutable.Map[String, String] =
    scala.collection.mutable.Map[String, String]()
}
