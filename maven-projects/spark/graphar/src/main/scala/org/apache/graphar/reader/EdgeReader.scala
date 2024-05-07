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

package org.apache.graphar.reader

import org.apache.graphar.util.{IndexGenerator, DataFrameConcat}
import org.apache.graphar.{EdgeInfo, FileType, AdjListType, PropertyGroup}
import org.apache.graphar.util.FileSystem

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

/**
 * Reader for edge chunks.
 *
 * @constructor
 *   create a new edge reader with edge info and AdjList type.
 * @param prefix
 *   the absolute prefix.
 * @param edgeInfo
 *   the edge info that describes the edge type.
 * @param adjListType
 *   the adj list type for the edge.
 * @param spark
 *   spark session for the reader to read chunks as Spark DataFrame.
 *
 * Note that constructor would raise IllegalArgumentException if edge info does
 * not support given adjListType.
 */
class EdgeReader(
    prefix: String,
    edgeInfo: EdgeInfo,
    adjListType: AdjListType.Value,
    spark: SparkSession
) {
  if (edgeInfo.containAdjList(adjListType) == false) {
    throw new IllegalArgumentException(
      "Edge info does not contain adj list type: " + AdjListType
        .AdjListTypeToString(
          adjListType
        )
    )
  }

  /** Load the total number of src/dst vertices for this edge type. */
  def readVerticesNumber(): Long = {
    val file_path = prefix + "/" + edgeInfo.getVerticesNumFilePath(adjListType)
    val number =
      FileSystem.readValue(file_path, spark.sparkContext.hadoopConfiguration)
    return number
  }

  /** Load the chunk number of src/dst vertices. */
  def readVertexChunkNumber(): Long = {
    val vertices_number = readVerticesNumber
    var vertex_chunk_size = edgeInfo.getSrc_chunk_size
    if (
      adjListType == AdjListType.ordered_by_dest || adjListType == AdjListType.unordered_by_dest
    ) {
      vertex_chunk_size = edgeInfo.getDst_chunk_size
    }
    val vertex_chunk_number =
      (vertices_number + vertex_chunk_size - 1) / vertex_chunk_size
    return vertex_chunk_number
  }

  /**
   * Load the number of edges for the vertex chunk.
   *
   * @param chunk_index
   *   index of vertex chunk
   * @return
   *   the number of edges
   */
  def readEdgesNumber(chunk_index: Long): Long = {
    val file_path =
      prefix + "/" + edgeInfo.getEdgesNumFilePath(chunk_index, adjListType)
    val number =
      FileSystem.readValue(file_path, spark.sparkContext.hadoopConfiguration)
    return number
  }

  /** Load the total number of edges for this edge type. */
  def readEdgesNumber(): Long = {
    val vertex_chunk_number = readVertexChunkNumber
    var number: Long = 0
    for (i <- 0L until vertex_chunk_number) {
      number += readEdgesNumber(i)
    }
    return number
  }

  /**
   * Load a single offset chunk as a DataFrame.
   *
   * @param chunk_index
   *   index of offset chunk
   * @return
   *   offset chunk DataFrame. Raise IllegalArgumentException if adjListType is
   *   not AdjListType.ordered_by_source or AdjListType.ordered_by_dest.
   */
  def readOffset(chunk_index: Long): DataFrame = {
    if (
      adjListType != AdjListType.ordered_by_source && adjListType != AdjListType.ordered_by_dest
    ) {
      throw new IllegalArgumentException(
        "Adj list type must be ordered_by_source or ordered_by_dest."
      )
    }
    val file_type_in_gar = edgeInfo.getAdjListFileType(adjListType)
    val file_type = FileType.FileTypeToString(file_type_in_gar)
    val file_path =
      prefix + edgeInfo.getAdjListOffsetFilePath(chunk_index, adjListType)
    val df = spark.read
      .option("fileFormat", file_type)
      .option("header", "true")
      .format("org.apache.graphar.datasources.GarDataSource")
      .load(file_path)
    return df
  }

  /**
   * Load a single AdjList chunk as a DataFrame.
   *
   * @param vertex_chunk_index
   *   index of vertex chunk
   * @param chunk_index
   *   index of AdjList chunk.
   * @return
   *   AdjList chunk DataFrame
   */
  def readAdjListChunk(
      vertex_chunk_index: Long,
      chunk_index: Long
  ): DataFrame = {
    val file_type_in_gar = edgeInfo.getAdjListFileType(adjListType)
    val file_type = FileType.FileTypeToString(file_type_in_gar)
    val file_path = prefix + edgeInfo.getAdjListFilePath(
      vertex_chunk_index,
      chunk_index,
      adjListType
    )
    val df = spark.read
      .option("fileFormat", file_type)
      .option("header", "true")
      .format("org.apache.graphar.datasources.GarDataSource")
      .load(file_path)
    return df
  }

  /**
   * Load all AdjList chunks for a vertex chunk as a DataFrame.
   *
   * @param vertex_chunk_index
   *   index of vertex chunk.
   * @param addIndex
   *   flag that add edge index column or not in the final DataFrame.
   * @return
   *   DataFrame of all AdjList chunks of vertices in given vertex chunk.
   */
  def readAdjListForVertexChunk(
      vertex_chunk_index: Long,
      addIndex: Boolean = true
  ): DataFrame = {
    val file_type_in_gar = edgeInfo.getAdjListFileType(adjListType)
    val file_type = FileType.FileTypeToString(file_type_in_gar)
    val file_path =
      prefix + edgeInfo.getAdjListPathPrefix(vertex_chunk_index, adjListType)
    val df = spark.read
      .option("fileFormat", file_type)
      .option("header", "true")
      .format("org.apache.graphar.datasources.GarDataSource")
      .load(file_path)
    if (addIndex) {
      return IndexGenerator.generateEdgeIndexColumn(df)
    } else {
      return df
    }
  }

  /**
   * Load all AdjList chunks for this edge type as a DataFrame.
   *
   * @param addIndex
   *   flag that add index column or not in the final DataFrame.
   * @return
   *   DataFrame of all AdjList chunks.
   */
  def readAllAdjList(addIndex: Boolean = true): DataFrame = {
    val file_type_in_gar = edgeInfo.getAdjListFileType(adjListType)
    val file_type = FileType.FileTypeToString(file_type_in_gar)
    val file_path = prefix + edgeInfo.getAdjListPathPrefix(adjListType)
    val df = spark.read
      .option("fileFormat", file_type)
      .option("header", "true")
      .option("recursiveFileLookup", "true")
      .format("org.apache.graphar.datasources.GarDataSource")
      .load(file_path)
    if (addIndex) {
      return IndexGenerator.generateEdgeIndexColumn(df)
    } else {
      return df
    }
  }

  /**
   * Load a single edge property chunk as a DataFrame.
   *
   * @param propertyGroup
   *   property group.
   * @param vertex_chunk_index
   *   index of vertex chunk.
   * @param chunk_index
   *   index of property group chunk.
   * @return
   *   property group chunk DataFrame. If edge info does not contain the
   *   property group, raise an IllegalArgumentException error.
   */
  def readEdgePropertyChunk(
      propertyGroup: PropertyGroup,
      vertex_chunk_index: Long,
      chunk_index: Long
  ): DataFrame = {
    if (edgeInfo.containPropertyGroup(propertyGroup) == false) {
      throw new IllegalArgumentException(
        "Edge info does not contain property group or adj list type."
      )
    }
    val file_type = propertyGroup.getFile_type()
    val file_path = prefix + edgeInfo.getPropertyFilePath(
      propertyGroup,
      adjListType,
      vertex_chunk_index,
      chunk_index
    )
    val df = spark.read
      .option("fileFormat", file_type)
      .option("header", "true")
      .format("org.apache.graphar.datasources.GarDataSource")
      .load(file_path)
    return df
  }

  /**
   * Load the chunks for a property group of a vertex chunk as a DataFrame.
   *
   * @param propertyGroup
   *   property group.
   * @param vertex_chunk_index
   *   index of vertex chunk.
   * @param addIndex
   *   flag that add edge index column or not in the final DataFrame.
   * @return
   *   DataFrame that contains all property group chunks of vertices in given
   *   vertex chunk. If edge info does not contain the property group, raise an
   *   IllegalArgumentException error.
   */
  def readEdgePropertyGroupForVertexChunk(
      propertyGroup: PropertyGroup,
      vertex_chunk_index: Long,
      addIndex: Boolean = true
  ): DataFrame = {
    if (edgeInfo.containPropertyGroup(propertyGroup) == false) {
      throw new IllegalArgumentException(
        "Edge info does not contain property group or adj list type."
      )
    }
    val file_type = propertyGroup.getFile_type()
    val file_path = prefix + edgeInfo.getPropertyGroupPathPrefix(
      propertyGroup,
      adjListType,
      vertex_chunk_index
    )
    val df = spark.read
      .option("fileFormat", file_type)
      .option("header", "true")
      .format("org.apache.graphar.datasources.GarDataSource")
      .load(file_path)
    if (addIndex) {
      return IndexGenerator.generateEdgeIndexColumn(df)
    } else {
      return df
    }
  }

  /**
   * Load all chunks for a property group as a DataFrame.
   *
   * @param propertyGroup
   *   property group.
   * @param addIndex
   *   flag that add edge index column or not in the final DataFrame.
   * @return
   *   DataFrame that contains all chunks of property group. If edge info does
   *   not contain the property group, raise an IllegalArgumentException error.
   */
  def readEdgePropertyGroup(
      propertyGroup: PropertyGroup,
      addIndex: Boolean = true
  ): DataFrame = {
    if (edgeInfo.containPropertyGroup(propertyGroup) == false) {
      throw new IllegalArgumentException(
        "Edge info does not contain property group or adj list type."
      )
    }
    val file_type = propertyGroup.getFile_type()
    val file_path =
      prefix + edgeInfo.getPropertyGroupPathPrefix(propertyGroup, adjListType)
    val df = spark.read
      .option("fileFormat", file_type)
      .option("header", "true")
      .option("recursiveFileLookup", "true")
      .format("org.apache.graphar.datasources.GarDataSource")
      .load(file_path)
    if (addIndex) {
      return IndexGenerator.generateEdgeIndexColumn(df)
    } else {
      return df
    }
  }

  /**
   * Load the chunks for multiple property groups of a vertex chunk as a
   * DataFrame.
   *
   * @param propertyGroups
   *   list of property groups.
   * @param vertex_chunk_index
   *   index of vertex chunk.
   * @param addIndex
   *   flag that add edge index column or not in the final DataFrame.
   * @return
   *   DataFrame that contains all property groups chunks of a vertex chunk.
   */
  def readMultipleEdgePropertyGroupsForVertexChunk(
      propertyGroups: java.util.ArrayList[PropertyGroup],
      vertex_chunk_index: Long,
      addIndex: Boolean = true
  ): DataFrame = {
    val len: Int = propertyGroups.size
    if (len == 0) {
      return spark.emptyDataFrame
    }

    val pg0: PropertyGroup = propertyGroups.get(0)
    val df0 =
      readEdgePropertyGroupForVertexChunk(pg0, vertex_chunk_index, false)
    if (len == 1) {
      if (addIndex) {
        return IndexGenerator.generateEdgeIndexColumn(df0)
      } else {
        return df0
      }
    }

    var rdd = df0.rdd
    var schema_array = df0.schema.fields
    for (i <- 1 to len - 1) {
      val pg: PropertyGroup = propertyGroups.get(i)
      val new_df =
        readEdgePropertyGroupForVertexChunk(pg, vertex_chunk_index, false)
      schema_array = Array.concat(schema_array, new_df.schema.fields)
      rdd = DataFrameConcat.concatRdd(rdd, new_df.rdd)
    }

    val schema = StructType(schema_array)
    val df = spark.createDataFrame(rdd, schema)
    if (addIndex) {
      return IndexGenerator.generateEdgeIndexColumn(df)
    } else {
      return df
    }
  }

  /**
   * Load the chunks for multiple property groups as a DataFrame.
   *
   * @param propertyGroups
   *   list of property groups.
   * @param addIndex
   *   flag that add edge index column or not in the final DataFrame.
   * @return
   *   DataFrame tha contains all property groups chunks of edge.
   */
  def readMultipleEdgePropertyGroups(
      propertyGroups: java.util.ArrayList[PropertyGroup],
      addIndex: Boolean = true
  ): DataFrame = {
    val len: Int = propertyGroups.size
    if (len == 0) {
      return spark.emptyDataFrame
    }

    val pg0: PropertyGroup = propertyGroups.get(0)
    val df0 = readEdgePropertyGroup(pg0, false)
    if (len == 1) {
      if (addIndex) {
        return IndexGenerator.generateEdgeIndexColumn(df0)
      } else {
        return df0
      }
    }

    var rdd = df0.rdd
    var schema_array = df0.schema.fields
    for (i <- 1 to len - 1) {
      val pg: PropertyGroup = propertyGroups.get(i)
      val new_df = readEdgePropertyGroup(pg, false)
      schema_array = Array.concat(schema_array, new_df.schema.fields)
      rdd = DataFrameConcat.concatRdd(rdd, new_df.rdd)
    }

    val schema = StructType(schema_array)
    val df = spark.createDataFrame(rdd, schema)
    if (addIndex) {
      return IndexGenerator.generateEdgeIndexColumn(df)
    } else {
      return df
    }
  }

  /**
   * Load the chunks for all property groups of a vertex chunk as a DataFrame.
   *
   * @param vertex_chunk_index
   *   index of vertex chunk.
   * @param addIndex
   *   flag that add edge index column or not in the final DataFrame.
   * @return
   *   DataFrame that contains all property groups chunks of a vertex chunk.
   */
  def readAllEdgePropertyGroupsForVertexChunk(
      vertex_chunk_index: Long,
      addIndex: Boolean = true
  ): DataFrame = {
    val property_groups = edgeInfo.getProperty_groups()
    return readMultipleEdgePropertyGroupsForVertexChunk(
      property_groups,
      vertex_chunk_index,
      addIndex
    )
  }

  /**
   * Load the chunks for all property groups as a DataFrame.
   *
   * @param addIndex
   *   flag that add edge index column or not in the final DataFrame.
   * @return
   *   DataFrame tha contains all property groups chunks of edge.
   */
  def readAllEdgePropertyGroups(addIndex: Boolean = true): DataFrame = {
    val property_groups = edgeInfo.getProperty_groups()
    return readMultipleEdgePropertyGroups(property_groups, addIndex)
  }

  /**
   * Load the chunks for the AdjList and all property groups for a vertex chunk
   * as a DataFrame.
   *
   * @param vertex_chunk_index
   *   index of vertex chunk
   * @param addIndex
   *   flag that add edge index column or not in the final DataFrame.
   * @return
   *   DataFrame that contains all chunks of AdjList and property groups of
   *   vertices in given vertex chunk.
   */
  def readEdgesForVertexChunk(
      vertex_chunk_index: Long,
      addIndex: Boolean = true
  ): DataFrame = {
    val adjList_df = readAdjListForVertexChunk(vertex_chunk_index, false)
    val properties_df =
      readAllEdgePropertyGroupsForVertexChunk(vertex_chunk_index, false)
    val df = DataFrameConcat.concat(adjList_df, properties_df)
    if (addIndex) {
      return IndexGenerator.generateEdgeIndexColumn(df)
    } else {
      return df
    }
  }

  /**
   * Load the chunks for the AdjList and all property groups as a DataFrame.
   *
   * @param addIndex
   *   flag that add edge index column or not in the final DataFrame.
   * @return
   *   DataFrame that contains all chunks of AdjList and property groups of
   *   edge.
   */
  def readEdges(addIndex: Boolean = true): DataFrame = {
    val adjList_df = readAllAdjList(false)
    val property_groups = edgeInfo.getProperty_groups()
    val df = if (property_groups.size == 0) {
      adjList_df
    } else {
      val properties_df = readAllEdgePropertyGroups(false)
      DataFrameConcat.concat(adjList_df, properties_df)
    }
    if (addIndex) {
      return IndexGenerator.generateEdgeIndexColumn(df)
    } else {
      return df
    }
  }
}
