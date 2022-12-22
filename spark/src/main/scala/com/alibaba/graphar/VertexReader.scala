package com.alibaba.graphar

import java.io.{File, FileInputStream, DataInputStream}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

class VertexReader(prefix: String, vertexInfo: VertexInfo, spark: SparkSession) {
  val vertices_number = readVerticesNumber()
  val chunk_size = vertexInfo.getChunk_size()
  var chunk_number = vertices_number / chunk_size
  if (vertices_number % chunk_size != 0)
    chunk_number = chunk_number + 1
 
  // load the total number of vertices for this vertex type
  def readVerticesNumber(): Long = {
    val file_path = prefix + "/" + vertexInfo.getVerticesNumFilePath()
    val input = new DataInputStream(new FileInputStream(file_path))
    val number = java.lang.Long.reverseBytes(input.readLong())
    input.close()
    return number
  }

  // load a single vertex property chunk as a DataFrame
  def readVertexPropertyChunk(propertyGroup: PropertyGroup, chunk_index: Long): DataFrame = {
    if (vertexInfo.containPropertyGroup(propertyGroup) == false) 
      throw new IllegalArgumentException
    val file_type = propertyGroup.getFile_type()
    val file_path = prefix + "/" + vertexInfo.getFilePath(propertyGroup, chunk_index)
    val df = spark.read.format(file_type).load(file_path)
    return df
  }

  // load all chunks for a property group as a DataFrame
  def readVertexProperties(propertyGroup: PropertyGroup, addIndex: Boolean = false): DataFrame = {
    if (vertexInfo.containPropertyGroup(propertyGroup) == false) 
      throw new IllegalArgumentException
    var df = spark.emptyDataFrame
    for ( i <- 0L to chunk_number - 1) {
      val new_df = readVertexPropertyChunk(propertyGroup, i)
      if (i == 0)
        df = new_df
      else
        df = df.union(new_df)
    }
    if (addIndex)
      df = IndexGenerator.generateVertexIndexColumn(df)
    return df
  }

  // load the chunks for all property groups as a DataFrame
  def readAllVertexProperties(addIndex: Boolean = false): DataFrame = {
    var df = spark.emptyDataFrame
    val property_groups = vertexInfo.getProperty_groups()
    val len: Int = property_groups.size
    for ( i <- 0 to len - 1 ) {
      val pg: PropertyGroup = property_groups.get(i)
      val new_df = readVertexProperties(pg, true)
      if (i == 0)
        df = new_df
      else
        df = df.join(new_df, Seq(GeneralParams.vertexIndexCol))
    }
    df = df.sort(GeneralParams.vertexIndexCol)
    if (addIndex == false)
      df = df.drop(GeneralParams.vertexIndexCol)
    return df
  }
}