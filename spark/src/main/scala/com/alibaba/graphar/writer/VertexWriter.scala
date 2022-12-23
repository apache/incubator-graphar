package com.alibaba.graphar.writer

import com.alibaba.graphar.utils.{FileSystem, VertexChunkPartitioner, IndexGenerator}
import com.alibaba.graphar.{GeneralParams, VertexInfo, FileType, AdjListType, PropertyGroup}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

import scala.collection.SortedMap
import scala.collection.mutable.ArrayBuffer


object VertexWriter {
  private def repartitionAndSort(vertexDf: DataFrame, chunkSize: Long): DataFrame = {
    val vertex_df_schema = vertexDf.schema
    val index = vertex_df_schema.fieldIndex(GeneralParams.vertexIndexCol)
    val partition_num = Math.ceil(vertexDf.count / chunkSize.toDouble).toInt
    val rdd = vertexDf.rdd.map(row => (row(index).asInstanceOf[Long], row))

    // repartition
    val partitioner = new VertexChunkPartitioner(partition_num, chunkSize)
    val chunks_rdd = rdd.repartitionAndSortWithinPartitions(partitioner).values
    vertexDf.sparkSession.createDataFrame(chunks_rdd, vertex_df_schema)
  }
}

class VertexWriter(prefix: String, vertexInfo: VertexInfo, vertexDf: DataFrame) {
  private var chunks:DataFrame = vertexDf.sparkSession.emptyDataFrame
  private val spark = vertexDf.sparkSession

  def writeVertexProperties(propertyGroup: PropertyGroup): Unit = {
    if (chunks.isEmpty) {
      chunks = VertexWriter.repartitionAndSort(vertexDf, vertexInfo.getChunk_size())
    }

    // write out the chunks
    val output_prefix = prefix + vertexInfo.getDirPath(propertyGroup)
    val property_list = ArrayBuffer[String]()
    val it = propertyGroup.getProperties().iterator
    while (it.hasNext()) {
      val property = it.next()
      property_list += property.getName()
    }
    val pg_df = chunks.select(property_list.map(col): _*)
    FileSystem.writeDataFrame(pg_df, propertyGroup.getFile_type(), output_prefix)
  }

  def writeVertexProperties(): Unit = {
    val property_groups = vertexInfo.getProperty_groups()
    val it = property_groups.iterator
    while (it.hasNext()) {
      val property_group = it.next()
      writeVertexProperties(property_group)
    }
  }
}

