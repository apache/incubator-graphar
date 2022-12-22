package com.alibaba.graphar

import java.io.{File}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

class EdgeReader(prefix: String,  edgeInfo: EdgeInfo, adjListType: AdjListType.Value,spark:SparkSession) {
  // load a single offset chunk as a DataFrame
  def readOffset(chunk_index: Long): DataFrame = {
    if (edgeInfo.containAdjList(adjListType) == false)
      throw new IllegalArgumentException
    if (adjListType != AdjListType.ordered_by_source && adjListType != AdjListType.ordered_by_dest)
      throw new IllegalArgumentException
    val file_type_in_gar = edgeInfo.getAdjListFileType(adjListType)
    val file_type = FileType.FileTypeToString(file_type_in_gar)
    val file_path = prefix + "/" + edgeInfo.getAdjListOffsetFilePath(chunk_index, adjListType)
    val df = spark.read.format(file_type).load(file_path)
    return df
  }

  // load a single AdjList chunk as a DataFrame
  def readAdjListChunk(vertex_chunk_index: Long, chunk_index: Long): DataFrame = {
    if (edgeInfo.containAdjList(adjListType) == false)
      throw new IllegalArgumentException
    val file_type_in_gar = edgeInfo.getAdjListFileType(adjListType)
    val file_type = FileType.FileTypeToString(file_type_in_gar)
    val file_path = prefix + "/" + edgeInfo.getAdjListFilePath(vertex_chunk_index, chunk_index, adjListType)
    val df = spark.read.format(file_type).load(file_path)
    return df
  }

  // load all AdjList chunks for a vertex chunk as a DataFrame
  def readAdjListForVertexChunk(vertex_chunk_index: Long, addIndex: Boolean = false): DataFrame = {
    if (edgeInfo.containAdjList(adjListType) == false)
      throw new IllegalArgumentException
    val file_path = prefix + "/" + edgeInfo.getAdjListFilePath(vertex_chunk_index, adjListType)
    val chunk_number = new File(file_path).list().length
    var df = spark.emptyDataFrame
    for ( i <- 0 to chunk_number - 1) {
      val new_df = readAdjListChunk(vertex_chunk_index, i)
      if (i == 0)
        df = new_df
      else
        df = df.union(new_df)
    }
    if (addIndex)
      df = IndexGenerator.generateEdgeIndexColumn(df)
    return df
  }

  // load all AdjList chunks for this edge type as a DataFrame
  def readAllAdjList(addIndex: Boolean = false): DataFrame = {
    if (edgeInfo.containAdjList(adjListType) == false)
      throw new IllegalArgumentException
    val file_path = prefix + "/" + edgeInfo.getAdjListDirPath(adjListType)
    val vertex_chunk_number = new File(file_path).list().length
    var df = spark.emptyDataFrame
    for ( i <- 0 to vertex_chunk_number - 1) {
      val new_df = readAdjListForVertexChunk(i)
      if (i == 0)
        df = new_df
      else
        df = df.union(new_df)
    }
    if (addIndex)
      df = IndexGenerator.generateEdgeIndexColumn(df)
    return df
  }

  // load a single edge property chunk as a DataFrame
  def readEdgePropertyChunk(propertyGroup: PropertyGroup, vertex_chunk_index: Long, chunk_index: Long): DataFrame = {
    if (edgeInfo.containAdjList(adjListType) == false)
      throw new IllegalArgumentException
    if (edgeInfo.containPropertyGroup(propertyGroup, adjListType) == false)
      throw new IllegalArgumentException
    val file_type = propertyGroup.getFile_type();
    val file_path = prefix + "/" + edgeInfo.getPropertyFilePath(propertyGroup, adjListType, vertex_chunk_index, chunk_index)
    val df = spark.read.format(file_type).load(file_path)
    return df
  }

  // load the chunks for a property group of a vertex chunk as a DataFrame
  def readEdgePropertiesForVertexChunk(propertyGroup: PropertyGroup, vertex_chunk_index: Long, addIndex: Boolean = false): DataFrame = {
    if (edgeInfo.containAdjList(adjListType) == false)
      throw new IllegalArgumentException
    if (edgeInfo.containPropertyGroup(propertyGroup, adjListType) == false)
      throw new IllegalArgumentException
    val file_path = prefix + "/" + edgeInfo.getPropertyFilePath(propertyGroup, adjListType, vertex_chunk_index)
    val chunk_number = new File(file_path).list().length
    var df = spark.emptyDataFrame
    for ( i <- 0 to chunk_number - 1) {
      val new_df = readEdgePropertyChunk(propertyGroup, vertex_chunk_index, i)
      if (i == 0)
        df = new_df
      else
        df = df.union(new_df)
    }
    if (addIndex)
      df = IndexGenerator.generateEdgeIndexColumn(df)
    return df
  }

  // load all chunks for a property group as a DataFrame
  def readEdgeProperties(propertyGroup: PropertyGroup, addIndex: Boolean = false): DataFrame = {
    if (edgeInfo.containAdjList(adjListType) == false)
      throw new IllegalArgumentException
    if (edgeInfo.containPropertyGroup(propertyGroup, adjListType) == false)
      throw new IllegalArgumentException
    val file_path = prefix + "/" + edgeInfo.getPropertyDirPath(propertyGroup, adjListType)
    val vertex_chunk_number = new File(file_path).list().length
    var df = spark.emptyDataFrame
    for ( i <- 0 to vertex_chunk_number - 1) {
      val new_df = readEdgePropertiesForVertexChunk(propertyGroup, i)
      if (i == 0)
        df = new_df
      else
        df = df.union(new_df)
    }
    if (addIndex)
      df = IndexGenerator.generateEdgeIndexColumn(df)
    return df
  }

  // load the chunks for all property groups of a vertex chunk as a DataFrame
  def readAllEdgePropertiesForVertexChunk(vertex_chunk_index: Long, addIndex: Boolean = false): DataFrame = {
    if (edgeInfo.containAdjList(adjListType) == false)
      throw new IllegalArgumentException
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
  
  // load the chunks for all property groups as a DataFrame
  def readAllEdgeProperties(addIndex: Boolean = false): DataFrame = {
    if (edgeInfo.containAdjList(adjListType) == false)
      throw new IllegalArgumentException
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

  // load the chunks for the AdjList and all property groups for a vertex chunk as a DataFrame
  def readEdgesForVertexChunk(vertex_chunk_index: Long, addIndex: Boolean = false): DataFrame = {
    if (edgeInfo.containAdjList(adjListType) == false)
      throw new IllegalArgumentException
    val adjList_df = readAdjListForVertexChunk(vertex_chunk_index, true)
    val properties_df = readAllEdgePropertiesForVertexChunk(vertex_chunk_index, true)
    var df = adjList_df.join(properties_df, Seq(GeneralParams.edgeIndexCol)).sort(GeneralParams.edgeIndexCol)
    if (addIndex == false)
      df = df.drop(GeneralParams.edgeIndexCol)
    return df
  }

  // load the chunks for the AdjList and all property groups as a DataFrame
  def readEdges(addIndex: Boolean = false): DataFrame = {
    if (edgeInfo.containAdjList(adjListType) == false)
      throw new IllegalArgumentException
    val adjList_df = readAllAdjList(true)
    val properties_df = readAllEdgeProperties(true)
    var df = adjList_df.join(properties_df, Seq(GeneralParams.edgeIndexCol)).sort(GeneralParams.edgeIndexCol);
    if (addIndex == false)
      df = df.drop(GeneralParams.edgeIndexCol)
    return df
  }
}