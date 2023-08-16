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

package com.alibaba.graphar.writer

import com.alibaba.graphar.util.{FileSystem, ChunkPartitioner, IndexGenerator}
import com.alibaba.graphar.{GeneralParams, VertexInfo, FileType, AdjListType, PropertyGroup}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.types.{LongType, StructField}

import scala.collection.SortedMap
import scala.collection.mutable.ArrayBuffer


/** Helper object for VertexWriter class. */
object VertexWriter {
  private def repartitionAndSort(vertexDf: DataFrame, chunkSize: Long, vertexNum: Long): DataFrame = {
    val vertex_df_schema = vertexDf.schema
    val index = vertex_df_schema.fieldIndex(GeneralParams.vertexIndexCol)
    val partition_num = ((vertexNum + chunkSize - 1) / chunkSize).toInt
    val rdd = vertexDf.rdd.map(row => (row(index).asInstanceOf[Long], row))

    // repartition
    val partitioner = new ChunkPartitioner(partition_num, chunkSize)
    val chunks_rdd = rdd.repartitionAndSortWithinPartitions(partitioner).values
    vertexDf.sparkSession.createDataFrame(chunks_rdd, vertex_df_schema)
  }
}

/** Writer for vertex DataFrame.
 *
 * @constructor create a new writer for vertex DataFrame with vertex info.
 * @param prefix the absolute prefix.
 * @param vertexInfo the vertex info that describes the vertex type.
 * @param vertexDf the input vertex DataFrame.
 */
class VertexWriter(prefix: String, vertexInfo: VertexInfo, vertexDf: DataFrame, numVertices: Option[Long] = None) {
  private val spark = vertexDf.sparkSession
  validate()
  private val vertexNum: Long = numVertices match {
    case None => vertexDf.count()
    case _ => numVertices.get
  }
  writeVertexNum()

  private var chunks:DataFrame = VertexWriter.repartitionAndSort(vertexDf, vertexInfo.getChunk_size(), vertexNum)

  private def validate(): Unit = {
    // check if vertex dataframe contains the index_filed
    val index_filed = StructField(GeneralParams.vertexIndexCol, LongType)
    if (vertexDf.schema.contains(index_filed) == false) {
      throw new IllegalArgumentException
    }
  }

  private def writeVertexNum(): Unit = {
    val outputPath = prefix + vertexInfo.getVerticesNumFilePath()
    FileSystem.writeValue(vertexNum, outputPath, spark.sparkContext.hadoopConfiguration)
  }

  /** Generate chunks of the property group for vertex dataframe.
   *
   * @param propertyGroup property group
   */
  def writeVertexProperties(propertyGroup: PropertyGroup): Unit = {
    // check if contains the property group
    if (vertexInfo.containPropertyGroup(propertyGroup) == false) {
      throw new IllegalArgumentException
    }

    // write out the chunks
    val output_prefix = prefix + vertexInfo.getPathPrefix(propertyGroup)
    val property_list = ArrayBuffer[String]()
    val it = propertyGroup.getProperties().iterator
    while (it.hasNext()) {
      val property = it.next()
      property_list += "`" + property.getName() + "`"
    }
    val pg_df = chunks.select(property_list.map(col): _*)
    FileSystem.writeDataFrame(pg_df, propertyGroup.getFile_type(), output_prefix, None, None)
  }

  /** Generate chunks of all property groups for vertex dataframe. */
  def writeVertexProperties(): Unit = {
    val property_groups = vertexInfo.getProperty_groups()
    val it = property_groups.iterator
    while (it.hasNext()) {
      val property_group = it.next()
      writeVertexProperties(property_group)
    }
  }
}

