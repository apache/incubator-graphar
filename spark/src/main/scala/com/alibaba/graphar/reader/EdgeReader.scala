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

package com.alibaba.graphar.reader

import com.alibaba.graphar.utils.{IndexGenerator}
import com.alibaba.graphar.{GeneralParams, EdgeInfo, FileType, AdjListType, PropertyGroup}
import com.alibaba.graphar.datasources._

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/** Reader for edge chunks.
 *
 * @constructor create a new edge reader with edge info and AdjList type.
 * @param prefix the absolute perfix.
 * @param edgeInfo the edge info that describes the edge type.
 * @param adjListType the adj list type for the edge.
 * @param spark spark session for the reader to read chunks as Spark DataFrame.
 *
 * Note that constructor would raise IllegalArgumentException if edge info does not support given adjListType.
 */
class EdgeReader(prefix: String,  edgeInfo: EdgeInfo, adjListType: AdjListType.Value, spark:SparkSession) {
  if (edgeInfo.containAdjList(adjListType) == false) {
    throw new IllegalArgumentException
  }

  /** Load a single offset chunk as a DataFrame.
   *
   * @param chunk_index index of offset chunk
   * @return offset chunk DataFrame. Raise IllegalArgumentException if adjListType is not
   *         AdjListType.ordered_by_source or AdjListType.ordered_by_dest.
   */
  def readOffset(chunk_index: Long): DataFrame = {
    if (adjListType != AdjListType.ordered_by_source && adjListType != AdjListType.ordered_by_dest)
      throw new IllegalArgumentException
    val file_type_in_gar = edgeInfo.getAdjListFileType(adjListType)
    val file_type = FileType.FileTypeToString(file_type_in_gar)
    val file_path = prefix + "/" + edgeInfo.getAdjListOffsetFilePath(chunk_index, adjListType)
    val df = spark.read.option("fileFormat", file_type).format("com.alibaba.graphar.datasources.GarDataSource").load(file_path)
    return df
  }

  /** Load a single AdjList chunk as a DataFrame.
   *
   * @param vertex_chunk_index index of vertex chunk
   * @param chunk_index index of AdjList chunk.
   * @return AdjList chunk DataFrame
   */
  def readAdjListChunk(vertex_chunk_index: Long, chunk_index: Long): DataFrame = {
    val file_type_in_gar = edgeInfo.getAdjListFileType(adjListType)
    val file_type = FileType.FileTypeToString(file_type_in_gar)
    val file_path = prefix + "/" + edgeInfo.getAdjListFilePath(vertex_chunk_index, chunk_index, adjListType)
    val df = spark.read.option("fileFormat", file_type).format("com.alibaba.graphar.datasources.GarDataSource").load(file_path)
    return df
  }

  /** Load all AdjList chunks for a vertex chunk as a DataFrame.
   *
   * @param vertex_chunk_index index of vertex chunk.
   * @param addIndex flag that add edge index column or not in the final DataFrame.
   * @return DataFrame of all AdjList chunks of vertices in given vertex chunk.
   */
  def readAdjListForVertexChunk(vertex_chunk_index: Long, addIndex: Boolean = false): DataFrame = {
    val file_type_in_gar = edgeInfo.getAdjListFileType(adjListType)
    val file_type = FileType.FileTypeToString(file_type_in_gar)
    val file_path = prefix + "/" + edgeInfo.getAdjListPathPrefix(vertex_chunk_index, adjListType)
    val df = spark.read.option("fileFormat", file_type).format("com.alibaba.graphar.datasources.GarDataSource").load(file_path)
    if (addIndex) {
      return IndexGenerator.generateEdgeIndexColumn(df)
    } else {
      return df
    }
  }

  /** Load all AdjList chunks for this edge type as a DataFrame.
   *
   * @param addIndex flag that add index column or not in the final DataFrame.
   * @return DataFrame of all AdjList chunks.
   */
  def readAllAdjList(addIndex: Boolean = false): DataFrame = {
    val file_type_in_gar = edgeInfo.getAdjListFileType(adjListType)
    val file_type = FileType.FileTypeToString(file_type_in_gar)
    val file_path = prefix + "/" + edgeInfo.getAdjListPathPrefix(adjListType)
    val df = spark.read.option("fileFormat", file_type).option("recursiveFileLookup", "true").format("com.alibaba.graphar.datasources.GarDataSource").load(file_path)
    if (addIndex) {
      return IndexGenerator.generateEdgeIndexColumn(df)
    } else {
      return df
    }
  }

  /** Load a single edge property chunk as a DataFrame.
   *
   * @param propertyGroup property group.
   * @param vertex_chunk_index index of vertex chunk.
   * @param chunk_index index of property group chunk.
   * @return property group chunk DataFrame. If edge info does not contain the property group,
   *         raise an IllegalArgumentException error.
   */
  def readEdgePropertyChunk(propertyGroup: PropertyGroup, vertex_chunk_index: Long, chunk_index: Long): DataFrame = {
    if (edgeInfo.containPropertyGroup(propertyGroup, adjListType) == false)
      throw new IllegalArgumentException
    val file_type = propertyGroup.getFile_type()
    val file_path = prefix + "/" + edgeInfo.getPropertyFilePath(propertyGroup, adjListType, vertex_chunk_index, chunk_index)
    val df = spark.read.option("fileFormat", file_type).format("com.alibaba.graphar.datasources.GarDataSource").load(file_path)
    return df
  }

  /** Load the chunks for a property group of a vertex chunk as a DataFrame.
   *
   * @param propertyGroup property group.
   * @param vertex_chunk_index index of vertex chunk.
   * @param addIndex  flag that add edge index column or not in the final DataFrame.
   * @return DataFrame that contains all property group chunks of vertices in given vertex chunk.
   *         If edge info does not contain the property group, raise an IllegalArgumentException error.
   */
  def readEdgePropertiesForVertexChunk(propertyGroup: PropertyGroup, vertex_chunk_index: Long, addIndex: Boolean = false): DataFrame = {
    if (edgeInfo.containPropertyGroup(propertyGroup, adjListType) == false)
      throw new IllegalArgumentException
    val file_type = propertyGroup.getFile_type()
    val file_path = prefix + "/" + edgeInfo.getPropertyGroupPathPrefix(propertyGroup, adjListType, vertex_chunk_index)
    val df = spark.read.option("fileFormat", file_type).format("com.alibaba.graphar.datasources.GarDataSource").load(file_path)
    if (addIndex) {
      return IndexGenerator.generateEdgeIndexColumn(df)
    } else {
      return df
    }
  }

  /** Load all chunks for a property group as a DataFrame.
   *
   * @param propertyGroup property group.
   * @param addIndex flag that add edge index column or not in the final DataFrame.
   * @return DataFrame that contains all chunks of property group.
   *         If edge info does not contain the property group, raise an IllegalArgumentException error.
   */
  def readEdgeProperties(propertyGroup: PropertyGroup, addIndex: Boolean = false): DataFrame = {
    if (edgeInfo.containPropertyGroup(propertyGroup, adjListType) == false)
      throw new IllegalArgumentException
    val file_type = propertyGroup.getFile_type()
    val file_path = prefix + "/" + edgeInfo.getPropertyGroupPathPrefix(propertyGroup, adjListType)
    val df = spark.read.option("fileFormat", file_type).option("recursiveFileLookup", "true").format("com.alibaba.graphar.datasources.GarDataSource").load(file_path)
    if (addIndex) {
      return IndexGenerator.generateEdgeIndexColumn(df)
    } else {
      return df
    }
  }

  /** Load the chunks for all property groups of a vertex chunk as a DataFrame.
   *
   * @param vertex_chunk_index index of vertex chunk.
   * @param addIndex flag that add edge index column or not in the final DataFrame.
   * @return DataFrame that contains all property groups chunks of a vertex chunk.
   */
  def readAllEdgePropertiesForVertexChunk(vertex_chunk_index: Long, addIndex: Boolean = false): DataFrame = {
    var df = spark.emptyDataFrame
    val property_groups = edgeInfo.getPropertyGroups(adjListType)
    val len: Int = property_groups.size
    for ( i <- 0 to len - 1 ) {
      val pg: PropertyGroup = property_groups.get(i)
      val new_df = readEdgePropertiesForVertexChunk(pg, vertex_chunk_index, true)
      if (i == 0)
        df = new_df
      else
        df = df.join(new_df, Seq(GeneralParams.edgeIndexCol))
    }
    df = df.sort(GeneralParams.edgeIndexCol)
    if (addIndex == false)
      df = df.drop(GeneralParams.edgeIndexCol)
    return df
  }

  /** Load the chunks for all property groups as a DataFrame.
   *
   * @param addIndex flag that add edge index column or not in the final DataFrame.
   * @return DataFrame tha contains all property groups chunks of edge.
   */
  def readAllEdgeProperties(addIndex: Boolean = false): DataFrame = {
    var df = spark.emptyDataFrame
    val property_groups = edgeInfo.getPropertyGroups(adjListType)
    val len: Int = property_groups.size
    for ( i <- 0 to len - 1 ) {
      val pg: PropertyGroup = property_groups.get(i)
      val new_df = readEdgeProperties(pg, true)
      if (i == 0)
        df = new_df
      else
        df = df.join(new_df, Seq(GeneralParams.edgeIndexCol))
    }
    df = df.sort(GeneralParams.edgeIndexCol)
    if (addIndex == false)
      df = df.drop(GeneralParams.edgeIndexCol)
    return df
  }

  /** Load the chunks for the AdjList and all property groups for a vertex chunk as a DataFrame.
   *
   * @param vertex_chunk_index index of vertex chunk
   * @param addIndex flag that add edge index column or not in the final DataFrame.
   * @return DataFrame that contains all chunks of AdjList and property groups of vertices in given vertex chunk.
   */
  def readEdgesForVertexChunk(vertex_chunk_index: Long, addIndex: Boolean = false): DataFrame = {
    val adjList_df = readAdjListForVertexChunk(vertex_chunk_index, true)
    val properties_df = readAllEdgePropertiesForVertexChunk(vertex_chunk_index, true)
    var df = adjList_df.join(properties_df, Seq(GeneralParams.edgeIndexCol)).sort(GeneralParams.edgeIndexCol)
    if (addIndex == false)
      df = df.drop(GeneralParams.edgeIndexCol)
    return df
  }

  /** Load the chunks for the AdjList and all property groups as a DataFrame.
   *
   * @param addIndex flag that add edge index column or not in the final DataFrame.
   * @return DataFrame that contains all chunks of AdjList and property groups of edge.
   */
  def readEdges(addIndex: Boolean = false): DataFrame = {
    val adjList_df = readAllAdjList(true)
    val properties_df = readAllEdgeProperties(true)
    var df = adjList_df.join(properties_df, Seq(GeneralParams.edgeIndexCol)).sort(GeneralParams.edgeIndexCol)
    if (addIndex == false)
      df = df.drop(GeneralParams.edgeIndexCol)
    return df
  }
}
