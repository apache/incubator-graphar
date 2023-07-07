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

package com.alibaba.graphar.utils

import scala.util.matching.Regex
import org.apache.spark.sql.types._

import com.alibaba.graphar.{PropertyGroup, Property, AdjList, GraphInfo, VertexInfo, EdgeInfo}

object Utils {

  private val REDACTION_REPLACEMENT_TEXT = "*********(redacted)"

  /**
   * Redact the sensitive information in the given string.
   */
  // folk of Utils.redact of spark
  def redact(regex: Option[Regex], text: String): String = {
    regex match {
      case None => text
      case Some(r) =>
        if (text == null || text.isEmpty) {
          text
        } else {
          r.replaceAllIn(text, REDACTION_REPLACEMENT_TEXT)
        }
    }
  }

  def generate_graph_info(path: String, graphName: String, directed: Boolean, vertexChunkSize: Long, edgeChunkSize: Long, fileType: String,
                          vertexSchemas: scala.collection.mutable.Map[String, StructType], edgeSchemas: scala.collection.mutable.Map[(String, String, String), StructType]): GraphInfo = {
    val info = new GraphInfo()
    info.setName(graphName)
    info.setPrefix(path + "/")
    info.setVersion("gar/v1")

    vertexSchemas.foreach { case (key, schema) => {
      val vertex_info = new VertexInfo()
      val prefix = "vertex/" + key + "/"
      vertex_info.setPrefix(prefix)
      vertex_info.setLabel(key)
      vertex_info.setChunk_size(vertexChunkSize)
      vertex_info.setVersion("gar/v1")
      vertex_info.getProperty_groups().add(new PropertyGroup())
      val property_group = vertex_info.getProperty_groups().get(0)
      property_group.setFile_type(fileType)
      val properties = property_group.getProperties()
      schema.foreach { case filed => {
        val property = new Property()
        property.setName(filed.name)
        property.setData_type("string")
        if (properties.size() == 0) {
          property.setIs_primary(true)
        } else {
          property.setIs_primary(false)
        }
        properties.add(property)
      }}
      info.addVertexInfo(vertex_info)
      info.vertices.add(key + ".vertex.yml")
    }}

    edgeSchemas.foreach { case (key, schema) => {
      val edge_info = new EdgeInfo()
      edge_info.setSrc_label(key._1)
      edge_info.setEdge_label(key._2)
      edge_info.setDst_label(key._3)
      edge_info.setChunk_size(edgeChunkSize)
      edge_info.setSrc_chunk_size(vertexChunkSize)
      edge_info.setDst_chunk_size(vertexChunkSize)
      edge_info.setDirected(directed)
      val prefix = "edge/" + edge_info.getConcatKey() + "/"
      edge_info.setVersion("gar/v1")
      edge_info.setPrefix(prefix)
      val csr_adj_list = new AdjList()
      csr_adj_list.setOrdered(true)
      csr_adj_list.setAligned_by("src")
      csr_adj_list.setFile_type(fileType)
      val csc_adj_list = new AdjList()
      csc_adj_list.setOrdered(true)
      csc_adj_list.setAligned_by("dst")
      csc_adj_list.setFile_type(fileType)
      if (schema.length > 0) {
        val property_group = new PropertyGroup()
        property_group.setFile_type(fileType)
        val properties = property_group.getProperties()
        schema.foreach { case filed => {
          val property = new Property()
          property.setName(filed.name)
          property.setData_type("string")
          properties.add(property)
        }}
        csr_adj_list.getProperty_groups().add(property_group)
        csc_adj_list.getProperty_groups().add(property_group)
      }
      edge_info.getAdj_lists().add(csr_adj_list)
      edge_info.getAdj_lists().add(csc_adj_list)
      info.addEdgeInfo(edge_info)
      info.edges.add(edge_info.getConcatKey() + ".edge.yml")
    }}
    return info
  }
}