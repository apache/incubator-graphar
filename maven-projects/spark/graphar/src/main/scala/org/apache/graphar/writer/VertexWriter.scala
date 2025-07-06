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

package org.apache.graphar.writer

import org.apache.graphar.util.{ChunkPartitioner, FileSystem, IndexGenerator}
import org.apache.graphar.{GeneralParams, PropertyGroup, VertexInfo}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{LongType, StructField}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable.ArrayBuffer

/** Helper object for VertexWriter class. */
object VertexWriter {
  private def repartitionAndSort(
      vertexDf: DataFrame,
      chunkSize: Long,
      vertexNum: Long
  ): DataFrame = {
    val vertexDfWithIndex = vertexDf.schema.contains(
      StructField(GeneralParams.vertexIndexCol, LongType)
    ) match {
      case true => vertexDf
      case _    => IndexGenerator.generateVertexIndexColumn(vertexDf)
    }
    val vertex_df_schema = vertexDfWithIndex.schema
    val index = vertex_df_schema.fieldIndex(GeneralParams.vertexIndexCol)
    val partition_num = ((vertexNum + chunkSize - 1) / chunkSize).toInt
    val rdd =
      vertexDfWithIndex.rdd.map(row => (row(index).asInstanceOf[Long], row))

    // repartition
    val partitioner = new ChunkPartitioner(partition_num, chunkSize)
    val chunks_rdd = rdd.repartitionAndSortWithinPartitions(partitioner).values
    vertexDf.sparkSession.createDataFrame(chunks_rdd, vertex_df_schema)
  }
}

/**
 * Writer for vertex DataFrame.
 *
 * @constructor
 *   create a new writer for vertex DataFrame with vertex info.
 * @param prefix
 *   the absolute prefix.
 * @param vertexInfo
 *   the vertex info that describes the vertex type.
 * @param vertexDf
 *   the input vertex DataFrame.
 * @param numVertices
 *   the number of vertices, if negative value is passed, then vertexDF.count
 *   will be used
 */
class VertexWriter(
    prefix: String,
    vertexInfo: VertexInfo,
    vertexDf: DataFrame,
    numVertices: Long = -1
) {
  private val spark = vertexDf.sparkSession
  vertexDf.persist(
    GeneralParams.defaultStorageLevel
  ) // cache the vertex DataFrame
  validate()
  private val vertexNum: Long =
    if (numVertices < 0) vertexDf.count else numVertices
  writeVertexNum()

  private var chunks: DataFrame = VertexWriter.repartitionAndSort(
    vertexDf,
    vertexInfo.getChunk_size(),
    vertexNum
  )
  vertexDf.unpersist() // unpersist the vertex DataFrame
  chunks.persist(GeneralParams.defaultStorageLevel)

  private def validate(): Unit = {
    // check if vertex DataFrame contains the index_field
    val index_field = StructField(GeneralParams.vertexIndexCol, LongType)
    if (vertexDf.schema.contains(index_field) == false) {
      throw new IllegalArgumentException(
        "vertex DataFrame must contain index column."
      )
    }
  }

  private def writeVertexNum(): Unit = {
    val outputPath = prefix + vertexInfo.getVerticesNumFilePath()
    FileSystem.writeValue(
      vertexNum,
      outputPath,
      spark.sparkContext.hadoopConfiguration
    )
  }

  def getVertexNum(): Long = vertexNum

  /**
   * Generate chunks of the property group for vertex DataFrame.
   *
   * @param propertyGroup
   *   property group
   */
  def writeVertexProperties(propertyGroup: PropertyGroup): Unit = {
    // check if contains the property group
    if (vertexInfo.containPropertyGroup(propertyGroup) == false) {
      throw new IllegalArgumentException(
        "property group not contained in vertex info."
      )
    }

    // write out the chunks
    val output_prefix = prefix + vertexInfo.getPathPrefix(propertyGroup)
    val property_list = ArrayBuffer[String]()
    property_list += "`" + GeneralParams.vertexIndexCol + "`"
    val it = propertyGroup.getProperties().iterator
    while (it.hasNext()) {
      val property = it.next()
      property_list += "`" + property.getName() + "`"
    }
    val pg_df = chunks.select(property_list.map(col).toSeq: _*)
    FileSystem.writeDataFrame(
      pg_df,
      propertyGroup.getFile_type(),
      output_prefix,
      None,
      None
    )
  }

  /** Generate chunks of all property groups for vertex DataFrame. */
  def writeVertexProperties(): Unit = {
    val property_groups = vertexInfo.getProperty_groups()
    val it = property_groups.iterator
    while (it.hasNext()) {
      val property_group = it.next()
      writeVertexProperties(property_group)
    }
  }

  def writeVertexLabels(): Unit = {
    if (vertexInfo.labels.isEmpty) {
      throw new IllegalArgumentException(
        "vertex does not have labels."
      )
    }

    // write out the chunks
    val output_prefix = prefix + vertexInfo.prefix + "/labels"
    val labels_list = vertexInfo.labels.toSeq
    val label_num = labels_list.length
    val labels_list_rdd =
      chunks.select(col(GeneralParams.kLabelCol)).rdd.map { row =>
        val labels = row.getSeq(0)
        val bools = new Array[Boolean](label_num)
        var i = 0
        while (i < label_num) {
          bools(i) = labels.contains(labels_list(i))
          i += 1
        }
        Row.fromSeq(bools)
      }
    val schema = StructType(
      labels_list.map(label =>
        StructField(label, BooleanType, nullable = false)
      )
    )
    val labelDf = spark.createDataFrame(labels_list_rdd, schema)
    labelDf.show()
    FileSystem.writeDataFrame(
      labelDf,
      "parquet",
      output_prefix,
      None,
      None
    )
  }

  override def finalize(): Unit = {
    chunks.unpersist()
  }

}
