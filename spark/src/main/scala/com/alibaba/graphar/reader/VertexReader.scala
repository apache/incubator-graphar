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

package com.alibaba.graphar.reader

import com.alibaba.graphar.util.{IndexGenerator, DataFrameConcat}
import com.alibaba.graphar.{GeneralParams, VertexInfo, FileType, PropertyGroup}
import com.alibaba.graphar.datasources._
import com.alibaba.graphar.util.FileSystem

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

/**
 * Reader for vertex chunks.
 *
 * @constructor
 *   create a new vertex reader with vertex info.
 * @param prefix
 *   the absolute prefix.
 * @param vertexInfo
 *   the vertex info that describes the vertex type.
 * @param spark
 *   spark session for the reader to read chunks as Spark DataFrame.
 */
class VertexReader(
    prefix: String,
    vertexInfo: VertexInfo,
    spark: SparkSession
) {

  /** Load the total number of vertices for this vertex type. */
  def readVerticesNumber(): Long = {
    val file_path = prefix + "/" + vertexInfo.getVerticesNumFilePath()
    val number =
      FileSystem.readValue(file_path, spark.sparkContext.hadoopConfiguration)
    return number
  }

  /**
   * Load a single vertex property chunk as a DataFrame.
   *
   * @param propertyGroup
   *   property group.
   * @param chunk_index
   *   index of vertex chunk.
   * @return
   *   vertex property chunk DataFrame. Raise IllegalArgumentException if the
   *   property group not contained.
   */
  def readVertexPropertyChunk(propertyGroup: PropertyGroup, chunk_index: Long): DataFrame = {
    if (!vertexInfo.containPropertyGroup(propertyGroup)) {
      throw new IllegalArgumentException
    }
    val file_type = propertyGroup.getFile_type()
    val file_path = prefix + vertexInfo.getFilePath(propertyGroup, chunk_index)
    val df = spark.read
      .option("fileFormat", file_type)
      .option("header", "true")
      .format("com.alibaba.graphar.datasources.GarDataSource")
      .load(file_path)
    return df
  }

  /**
   * Load all chunks for a property group as a DataFrame.
   *
   * @param propertyGroup
   *   property group.
   * @param addIndex
   *   flag that add vertex index column or not in the final DataFrame.
   * @return
   *   DataFrame that contains all chunks of property group. Raise
   *   IllegalArgumentException if the property group not contained.
   */
  def readVertexPropertyGroup(propertyGroup: PropertyGroup, addIndex: Boolean = true): DataFrame = {
    if (!vertexInfo.containPropertyGroup(propertyGroup)) {
      throw new IllegalArgumentException
    }
    val file_type = propertyGroup.getFile_type()
    val file_path = prefix + vertexInfo.getPathPrefix(propertyGroup)
    val df = spark.read.option("fileFormat", file_type).option("header", "true").format("com.alibaba.graphar.datasources.GarDataSource").load(file_path)

    if (addIndex) {
      IndexGenerator.generateVertexIndexColumn(df)
    } else {
      df
    }
  }

  /**
   * Load the chunks for multiple property groups as a DataFrame.
   *
   * @param propertyGroups
   *   list of property groups.
   * @param addIndex
   *   flag that add vertex index column or not in the final DataFrame.
   * @return
   *   DataFrame that contains all chunks of property group. Raise
   *   IllegalArgumentException if the property group not contained.
   */
  def readMultipleVertexPropertyGroups(
      propertyGroups: java.util.ArrayList[PropertyGroup],
      addIndex: Boolean = true
  ): DataFrame = {
    val len: Int = propertyGroups.size
    if (len == 0) {
      return spark.emptyDataFrame
    }

    val pg0: PropertyGroup = propertyGroups.get(0)
    val df0 = readVertexPropertyGroup(pg0, false)
    if (len == 1) {
      if (addIndex) {
        return IndexGenerator.generateVertexIndexColumn(df0)
      } else {
        return df0
      }
    }

    var rdd = df0.rdd
    var schema_array = df0.schema.fields
    for ( i <- 1 until len) {
      val pg: PropertyGroup = propertyGroups.get(i)
      val new_df = readVertexPropertyGroup(pg, false)
      schema_array = Array.concat(schema_array, new_df.schema.fields)
      rdd = DataFrameConcat.concatRdd(rdd, new_df.rdd)
    }

    val schema = StructType(schema_array)
    val df = spark.createDataFrame(rdd, schema)
    if (addIndex) {
      IndexGenerator.generateVertexIndexColumn(df)
    } else {
      df
    }
  }

  /**
   * Load the chunks for all property groups as a DataFrame.
   *
   * @param addIndex
   *   flag that add vertex index column or not in the final DataFrame.
   * @return
   *   DataFrame that contains all property group chunks of vertex.
   */
  def readAllVertexPropertyGroups(addIndex: Boolean = true): DataFrame = {
    val property_groups = vertexInfo.getProperty_groups()
    return readMultipleVertexPropertyGroups(property_groups, addIndex)
  }
}
