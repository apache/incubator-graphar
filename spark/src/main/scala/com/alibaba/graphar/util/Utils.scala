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

package com.alibaba.graphar.util

import scala.util.matching.Regex
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.alibaba.graphar.{
  PropertyGroup,
  Property,
  AdjList,
  GraphInfo,
  VertexInfo,
  EdgeInfo,
  GeneralParams
}

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

  def sparkDataType2GraphArTypeName(dataType: DataType): String = {
    val typeName = dataType.typeName
    val grapharTypeName = typeName match {
      case "string"  => "string"
      case "integer" => "int"
      case "long"    => "int64"
      case "double"  => "double"
      case "boolean" => "bool"
      case _ =>
        throw new IllegalArgumentException(
          "Expected string, integral, double or boolean type, got " + typeName + " type"
        )
    }
    return grapharTypeName
  }

  /**
   * Generate graph info with schema of graph data.
   * @param path
   *   prefix of graph info
   * @param graphName
   *   name of graph
   * @param directed
   *   directed or not of graph
   * @param vertexChunkSize
   *   chunk size for every vertex type
   * @param edgeChunkSize
   *   chunk size for every edge type
   * @param fileType
   *   file type for payload data file, support [csv, orc, parquet]
   * @param vertexSchemas
   *   schemas of every vertex type
   * @param edgeSchemas
   *   schemas of every edge type
   * @return
   *   graph info
   */
  def generateGraphInfo(
      path: String,
      graphName: String,
      directed: Boolean,
      vertexChunkSize: Long,
      edgeChunkSize: Long,
      fileType: String,
      version: String,
      vertexSchemas: scala.collection.mutable.Map[String, StructType],
      edgeSchemas: scala.collection.mutable.Map[
        (String, String, String),
        StructType
      ],
      primaryKeys: scala.collection.mutable.Map[String, String]
  ): GraphInfo = {
    val info = new GraphInfo()
    info.setName(graphName)
    info.setPrefix(path + "/")
    info.setVersion("gar/" + version)

    vertexSchemas.foreach {
      case (key, schema) => {
        val vertexInfo = new VertexInfo()
        val prefix = "vertex/" + key + "/"
        vertexInfo.setPrefix(prefix)
        vertexInfo.setLabel(key)
        vertexInfo.setChunk_size(vertexChunkSize)
        vertexInfo.setVersion("gar/" + version)
        vertexInfo.getProperty_groups().add(new PropertyGroup())
        val propertyGroup = vertexInfo.getProperty_groups().get(0)
        propertyGroup.setFile_type(fileType)
        val properties = propertyGroup.getProperties()
        schema.foreach {
          case field => {
            val property = new Property()
            property.setName(field.name)
            property.setData_type(sparkDataType2GraphArTypeName(field.dataType))
            val isPrimary: Boolean =
              if (
                (primaryKeys(key) == "" && properties
                  .size() == 0) || field.name == primaryKeys
              ) true
              else false
            property.setIs_primary(isPrimary)
            properties.add(property)
          }
        }
        info.addVertexInfo(vertexInfo)
        info.vertices.add(key + ".vertex.yml")
      }
    }

    edgeSchemas.foreach {
      case (key, schema) => {
        val edgeInfo = new EdgeInfo()
        edgeInfo.setSrc_label(key._1)
        edgeInfo.setEdge_label(key._2)
        edgeInfo.setDst_label(key._3)
        edgeInfo.setChunk_size(edgeChunkSize)
        edgeInfo.setSrc_chunk_size(vertexChunkSize)
        edgeInfo.setDst_chunk_size(vertexChunkSize)
        edgeInfo.setDirected(directed)
        val prefix = "edge/" + edgeInfo.getConcatKey() + "/"
        edgeInfo.setVersion("gar/" + version)
        edgeInfo.setPrefix(prefix)
        val csrAdjList = new AdjList()
        csrAdjList.setOrdered(true)
        csrAdjList.setAligned_by("src")
        csrAdjList.setFile_type(fileType)
        val cscAdjList = new AdjList()
        cscAdjList.setOrdered(true)
        cscAdjList.setAligned_by("dst")
        cscAdjList.setFile_type(fileType)
        if (schema.length > 0) {
          val propertyGroup = new PropertyGroup()
          propertyGroup.setFile_type(fileType)
          val properties = propertyGroup.getProperties()
          schema.foreach {
            case field => {
              val property = new Property()
              property.setName(field.name)
              property.setData_type(
                sparkDataType2GraphArTypeName(field.dataType)
              )
              properties.add(property)
            }
          }
          csrAdjList.getProperty_groups().add(propertyGroup)
          cscAdjList.getProperty_groups().add(propertyGroup)
        }
        edgeInfo.getAdj_lists().add(csrAdjList)
        edgeInfo.getAdj_lists().add(cscAdjList)
        info.addEdgeInfo(edgeInfo)
        info.edges.add(edgeInfo.getConcatKey() + ".edge.yml")
      }
    }
    return info
  }

  /**
   * Join and convert source index and target index to primary key in edges
   * @param edgeDf
   *   edge data frame
   * @param sourceDf
   *   source vertex data frame
   * @param targetDf
   *   target vertex data frame
   * @param sourceKey
   *   source vertex primary key
   * @param targetKey
   *   target vertex primary key
   * @return
   *   new edge data frame
   */
  def joinEdgesWithVertexPrimaryKey(
      edgeDf: DataFrame,
      sourceDf: DataFrame,
      targetDf: DataFrame,
      sourceKey: String,
      targetKey: String
  ): DataFrame = {
    val spark: SparkSession = edgeDf.sparkSession
    sourceDf.createOrReplaceTempView("source_table")
    targetDf.createOrReplaceTempView("target_table")
    edgeDf.createOrReplaceTempView("edge_table")
    val srcCol = GeneralParams.srcIndexCol
    val dstCol = GeneralParams.dstIndexCol
    val indexCol = GeneralParams.vertexIndexCol
    val edge_df_with_src = spark
      .sql(
        f"select source_table.`$sourceKey` as `src`, edge_table.* from edge_table inner join source_table on source_table.`$indexCol`=edge_table.`$srcCol`"
      )
      .drop(srcCol)
    edge_df_with_src.createOrReplaceTempView("edge_table")
    val edge_df_with_src_dst = spark
      .sql(
        f"select target_table.`$targetKey` as `dst`, edge_table.* from edge_table inner join target_table on target_table.`$indexCol`=edge_table.`$dstCol`"
      )
      .drop(dstCol)
    edge_df_with_src_dst
  }
}
