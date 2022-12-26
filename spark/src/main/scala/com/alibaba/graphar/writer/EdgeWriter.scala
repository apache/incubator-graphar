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

import com.alibaba.graphar.utils.{FileSystem, VertexChunkPartitioner}
import com.alibaba.graphar.{GeneralParams, EdgeInfo, FileType, AdjListType, PropertyGroup}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{LongType, StructField}
import org.apache.spark.util.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

import scala.collection.SortedMap
import scala.collection.mutable.ArrayBuffer

object EdgeWriter {
  // split the whole edge dataframe into chunk dataframes by vertex chunk size.
  private def split(edgeDf: DataFrame, keyColumnName: String, vertexChunkSize: Long): Seq[DataFrame] = {
    // split the dataframe to mutiple daraframes by vertex chunk
    edgeDf.cache()
    val spark = edgeDf.sparkSession
    import spark.implicits._
    val df_schema = edgeDf.schema
    val index = df_schema.fieldIndex(keyColumnName)
    val vertex_chunk_num = math.floor(edgeDf.agg(max(keyColumnName)).head().getLong(0) / vertexChunkSize.toDouble).toInt
    val chunks: Seq[DataFrame] = (0 to vertex_chunk_num).map {i => edgeDf.where(edgeDf(keyColumnName) >= (i * vertexChunkSize)  and edgeDf(keyColumnName) < ((i + 1) * vertexChunkSize))}
    return chunks
  }

  // repartition the chunk dataframe by edge chunk size (this is for COO)
  private def repartition(chunkDf: DataFrame, keyColumnName: String, edgeChunkSize: Long): DataFrame = {
    // repartition the dataframe by edge chunk size
    val spark = chunkDf.sparkSession
    import spark.implicits._
    val df_schema = chunkDf.schema
    val index = df_schema.fieldIndex(keyColumnName)
    val df_rdd = chunkDf.rdd.map(row => (row(index).asInstanceOf[Long], row))

    // generate global edge id for each record of dataframe
    val parition_counts = df_rdd
      .mapPartitionsWithIndex((i, ps) => Array((i, ps.size)).iterator, preservesPartitioning = true)
      .collectAsMap()
    val aggregatedPartitionCounts = SortedMap(parition_counts.toSeq: _*)
      .foldLeft((0L, Map.empty[Int, Long])) { case ((total, map), (i, c)) =>
        (total + c, map + (i -> total))
      }
      ._2
    val broadcastedPartitionCounts = spark.sparkContext.broadcast(aggregatedPartitionCounts)
    val rdd_with_eid = df_rdd.mapPartitionsWithIndex((i, ps) => {
      val start = broadcastedPartitionCounts.value(i)
      for { ((k, row), j) <- ps.zipWithIndex } yield (start + j, row)
    })
    val partition_num = Math.ceil(chunkDf.count() / edgeChunkSize.toDouble).toInt
    val partitioner = new VertexChunkPartitioner(partition_num, edgeChunkSize)
    val chunks = rdd_with_eid.partitionBy(partitioner).values
    spark.createDataFrame(chunks, df_schema)
  }

  // repartition and sort the chunk dataframe by edge chunk size (this is for CSR/CSC)
  private def sortAndRepartition(chunkDf: DataFrame, keyColumnName: String, edgeChunkSize: Long): DataFrame = {
    // repartition the dataframe by edge chunk size
    val spark = chunkDf.sparkSession
    import spark.implicits._
    val df_schema = chunkDf.schema
    val index = df_schema.fieldIndex(keyColumnName)
    val rdd_ordered = chunkDf.rdd.map(row => (row(index).asInstanceOf[Long], row)).sortByKey()

    // generate global edge id for each record of dataframe
    val parition_counts = rdd_ordered
      .mapPartitionsWithIndex((i, ps) => Array((i, ps.size)).iterator, preservesPartitioning = true)
      .collectAsMap()
    val aggregatedPartitionCounts = SortedMap(parition_counts.toSeq: _*)
      .foldLeft((0L, Map.empty[Int, Long])) { case ((total, map), (i, c)) =>
        (total + c, map + (i -> total))
      }
      ._2
    val broadcastedPartitionCounts = spark.sparkContext.broadcast(aggregatedPartitionCounts)
    val rdd_with_eid = rdd_ordered.mapPartitionsWithIndex((i, ps) => {
      val start = broadcastedPartitionCounts.value(i)
      for { ((k, row), j) <- ps.zipWithIndex } yield (start + j, row)
    })
    val partition_num = Math.ceil(chunkDf.count() / edgeChunkSize.toDouble).toInt
    val partitioner = new VertexChunkPartitioner(partition_num, edgeChunkSize)
    val chunks = rdd_with_eid.repartitionAndSortWithinPartitions(partitioner).values
    spark.createDataFrame(chunks, df_schema)
  }
}

class EdgeWriter(prefix: String,  edgeInfo: EdgeInfo, adjListType: AdjListType.Value, edgeDf: DataFrame) {
  private var chunks: Seq[DataFrame] = preprocess()

  // convert the edge dataframe to chunk dataframes
  private def preprocess(): Seq[DataFrame] = {
    // chunk if edge info contains the adj list type
    if (edgeInfo.containAdjList(adjListType) == false) {
      throw new IllegalArgumentException
    }

    // check the src index and dst index column exist
    val src_filed = StructField(GeneralParams.srcIndexCol, LongType, false)
    val dst_filed = StructField(GeneralParams.dstIndexCol, LongType, false)
    val schema = edgeDf.schema
    if (schema.contains(src_filed) == false || schema.contains(dst_filed) == false) {
      throw new IllegalArgumentException
    }
    var vertex_chunk_size: Long = 0
    var primaryColName: String = ""
    if (adjListType == AdjListType.ordered_by_source || adjListType == AdjListType.unordered_by_source) {
      vertex_chunk_size = edgeInfo.getSrc_chunk_size()
      primaryColName = GeneralParams.srcIndexCol
    } else {
      vertex_chunk_size = edgeInfo.getDst_chunk_size()
      primaryColName = GeneralParams.dstIndexCol
    }
    val edges_of_vertex_chunks = EdgeWriter.split(edgeDf, primaryColName, vertex_chunk_size)
    val vertex_chunk_num = edges_of_vertex_chunks.length
    if (adjListType == AdjListType.ordered_by_source || adjListType == AdjListType.ordered_by_dest) {
      val processed_chunks: Seq[DataFrame] = (0 until vertex_chunk_num).map {i => EdgeWriter.sortAndRepartition(edges_of_vertex_chunks(i), primaryColName, edgeInfo.getChunk_size())}
      return processed_chunks
    } else {
      val processed_chunks: Seq[DataFrame] = (0 until vertex_chunk_num).map {i => EdgeWriter.repartition(edges_of_vertex_chunks(i), primaryColName, edgeInfo.getChunk_size())}
      return processed_chunks
    }
  }

  // generate the Offset chunks files from edge dataframe for this edge type
  private def writeOffset(): Unit = {
    val file_type = edgeInfo.getAdjListFileType(adjListType)
    var chunk_index: Long = 0
    for (chunk <- chunks) {
      val output_prefix = prefix + edgeInfo.getAdjListOffsetDirPath(adjListType)
      if (adjListType == AdjListType.ordered_by_source) {
        val offset_chunk = chunk.select(GeneralParams.srcIndexCol).groupBy(GeneralParams.srcIndexCol).count().coalesce(1).orderBy(GeneralParams.srcIndexCol).select("count")
        FileSystem.writeDataFrame(offset_chunk, FileType.FileTypeToString(file_type), output_prefix)
      } else {
        val offset_chunk = chunk.select(GeneralParams.dstIndexCol).groupBy(GeneralParams.dstIndexCol).count().coalesce(1).orderBy(GeneralParams.dstIndexCol).select("count")
        FileSystem.writeDataFrame(offset_chunk, FileType.FileTypeToString(file_type), output_prefix)
      }
      chunk_index = chunk_index + 1
    }
  }

  // generate the chunks of AdjList from edge dataframe for this edge type
  def writeAdjList(): Unit = {
    val file_type = edgeInfo.getAdjListFileType(adjListType)
    var chunk_index: Long = 0
    for (chunk <- chunks) {
      val output_prefix = prefix + edgeInfo.getAdjListFilePath(chunk_index, adjListType)
      val adj_list_chunk = chunk.select(GeneralParams.srcIndexCol, GeneralParams.dstIndexCol)
      FileSystem.writeDataFrame(adj_list_chunk, FileType.FileTypeToString(file_type), output_prefix)
      chunk_index = chunk_index + 1
    }

    if (adjListType == AdjListType.ordered_by_source || adjListType == AdjListType.ordered_by_dest) {
      writeOffset()
    }
  }

  // generate the chunks of the property group from edge dataframe
  def writeEdgeProperties(propertyGroup: PropertyGroup): Unit = {
    // select the columns uses property names
    // write the rows in batch (with batch size = ChunkSize)
    // return a WriterMessge
    if (edgeInfo.containPropertyGroup(propertyGroup, adjListType) == false) {
      throw new IllegalArgumentException
    }

    val output_prefix = prefix + edgeInfo.getPropertyDirPath(propertyGroup, adjListType)
    val property_list = ArrayBuffer[String]()
    val p_it = propertyGroup.getProperties().iterator
    while (p_it.hasNext()) {
      val property = p_it.next()
      property_list += property.getName()
    }
    var chunk_index: Long = 0
    for (chunk <- chunks) {
      val property_group_chunk = chunk.select(property_list.map(col): _*)
      FileSystem.writeDataFrame(property_group_chunk, propertyGroup.getFile_type(), output_prefix)
      chunk_index = chunk_index + 1
    }
  }

  // generate the chunks of all property groups from edge dataframe
  def writeEdgeProperties(): Unit = {
      // select the columns uses property names
      // write the rows in batch (with batch size = ChunkSize)
      // return a WriterMessge
    val property_groups = edgeInfo.getPropertyGroups(adjListType)
    val it = property_groups.iterator
    while (it.hasNext()) {
      val property_group = it.next()
      writeEdgeProperties(property_group)
    }
  }

  // generate the chunks for the AdjList and all property groups from edge dataframe
  def writeEdges(): Unit = {
    writeAdjList()
    writeEdgeProperties()
  }
}


