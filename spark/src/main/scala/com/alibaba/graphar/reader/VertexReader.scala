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
import com.alibaba.graphar.{GeneralParams, VertexInfo, FileType, PropertyGroup}
import com.alibaba.graphar.datasources._

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/** Reader for vertex chunks.
 *
 * @constructor create a new vertex reader with vertex info.
 * @param prefix the absolute prefix.
 * @param vertexInfo the vertex info that describes the vertex type.
 * @param spark spark session for the reader to read chunks as Spark DataFrame.
 */
class VertexReader(prefix: String, vertexInfo: VertexInfo, spark: SparkSession) {
  private val vertices_number = readVerticesNumber()
  private val chunk_size = vertexInfo.getChunk_size()
  private var chunk_number = vertices_number / chunk_size
  if (vertices_number % chunk_size != 0)
    chunk_number = chunk_number + 1
 
  /** Load the total number of vertices for this vertex type. */
  def readVerticesNumber(): Long = {
    val file_path = prefix + "/" + vertexInfo.getVerticesNumFilePath()
    val path = new Path(file_path)
    val file_system = FileSystem.get(path.toUri(), spark.sparkContext.hadoopConfiguration)
    val input = file_system.open(path)
    val number = java.lang.Long.reverseBytes(input.readLong())
    file_system.close()
    return number
  }

  /** Load a single vertex property chunk as a DataFrame.
   *
   * @param propertyGroup property group.
   * @param chunk_index index of vertex chunk.
   * @return vertex property chunk DataFrame. Raise IllegalArgumentException if the property group not contained.
   */
  def readVertexPropertyChunk(propertyGroup: PropertyGroup, chunk_index: Long): DataFrame = {
    if (vertexInfo.containPropertyGroup(propertyGroup) == false) 
      throw new IllegalArgumentException
    val file_type = propertyGroup.getFile_type()
    val file_path = prefix + "/" + vertexInfo.getFilePath(propertyGroup, chunk_index)
    val df = spark.read.option("fileFormat", file_type).option("header", "true").format("com.alibaba.graphar.datasources.GarDataSource").load(file_path)
    return df
  }

  /** Load all chunks for a property group as a DataFrame.
   *
   * @param propertyGroup property group.
   * @param addIndex flag that add vertex index column or not in the final DataFrame.
   * @return DataFrame that contains all chunks of property group.
   *         Raise IllegalArgumentException if the property group not contained.
   */
  def readVertexProperties(propertyGroup: PropertyGroup, addIndex: Boolean = false): DataFrame = {
    if (vertexInfo.containPropertyGroup(propertyGroup) == false) 
      throw new IllegalArgumentException
    val file_type = propertyGroup.getFile_type()
    val file_path = prefix + "/" + vertexInfo.getPathPrefix(propertyGroup)
    val df = spark.read.option("fileFormat", file_type).option("header", "true").format("com.alibaba.graphar.datasources.GarDataSource").load(file_path)
    
    if (addIndex) {
      return IndexGenerator.generateVertexIndexColumn(df)
    } else {
      return df
    }
  }

  /** Load the chunks for all property groups as a DataFrame.
   *
   * @param addIndex flag that add vertex index column or not in the final DataFrame.
   * @return DataFrame that contains all property group chunks of vertex.
   */
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
