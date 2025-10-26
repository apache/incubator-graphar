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

package org.apache.graphar.datasources.ldbc.stream.writer

import org.apache.graphar.datasources.ldbc.stream.core.{EntitySchemaDetector}
import org.apache.graphar.datasources.ldbc.stream.model.{LdbcEntityType, VertexEntityType, EdgeEntityType}
import org.apache.graphar.datasources.ldbc.stream.processor.{PropertyGroupManager, ContinuousIdGenerator}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

// GraphAr standard component imports
import org.apache.graphar.graph.GraphWriter

import java.io.{File, FileWriter, BufferedWriter}
import java.net.URI
import scala.collection.mutable

/**
 * Streaming GraphAr writer
 *
 * Responsible for writing processed streaming data to GraphAr format, implements:
 * 1. Shard writes according to chunk size
 * 2. Generate GraphAr standard metadata files
 * 3. Support multiple file formats (csv/parquet/orc)
 * 4. Maintain offset information and indices
 */
class StreamingGraphArWriter(
 private val output_path: String,
 private val graph_name: String,
 private val vertex_chunk_size: Long = 1024L,
 private val edge_chunk_size: Long = 1024L,
 private val file_type: String = "parquet"
)(implicit spark: SparkSession) extends Serializable {

 private val logger: Logger = LoggerFactory.getLogger(classOf[StreamingGraphArWriter])

 // Chunk counter: EntityType -> PropertyGroup -> chunk count (maintain streaming management)
 private val chunkCounters = mutable.Map[String, mutable.Map[String, Long]]()

 // Offset mapping: EntityType -> PropertyGroup -> chunk -> Offset information (maintain index feature)
 private val offsetmappings = mutable.Map[String, mutable.Map[String, mutable.Map[Long, OffsetInfo]]]()

 // EntityStatistics
 private val entityStatistics = mutable.Map[String, EntityWriteStatistics]()

 // Data collector: used for GraphWriter metadata generation
 private val collectedDataFrames = mutable.Map[String, DataFrame]()

 // Schema mapping: store schema information for each entity
 private val schemamapping = mutable.Map[String, EntitySchemaDetector.EntitySchema]()

 logger.info(s"StreamingGraphArWriter initialized: output_path=$output_path, graph_name=$graph_name, using GraphWriter mode")

 /**
 * Get path conforming to GraphAr standard
 * According to GraphAr specification, vertex data should be in vertex/ directory, edge data should be in edge/ directory
 */
 private def getstandardGraphArPath(entityType: LdbcEntityType): String = {
 entityType match {
 case vertexType if entityType.isVertex =>
 s"$output_path/vertex/${entityType.name}"
 case edgeType if entityType.isEdge =>
 // For edges, use src_edge_dst naming format
 val edgeTypeImpl = entityType.asInstanceOf[EdgeEntityType]
 s"$output_path/edge/${edgeTypeImpl.srcVertexType.name}_${entityType.name}_${edgeTypeImpl.dstVertexType.name}"
 case _ =>
 // Fall back to original logic
 s"$output_path/${entityType.tableName}"
 }
 }

 /**
 * Write entity data to GraphAr format (maintain streaming chunk management)
 */
 def writeEntity(
 dataFrame: DataFrame,
 schema: EntitySchemaDetector.EntitySchema,
 propertyGroup: String
): WriteResult = {

 val entityType = schema.entityType
 val entityName = entityType.name
 val collectKey = s"${entityName}_${propertyGroup}"

 logger.info(s"Writing entity data in streaming mode: entity=$entityName, propertyGroup=$propertyGroup, rows=${dataFrame.count()}")

 try {
 // Store schema information for subsequent metadata generation
 schemamapping(collectKey) = schema

 // Also collect DataFrame for GraphWriter metadata generation
 collectedDataFrames.get(collectKey) match {
 case Some(existingDF) =>
 collectedDataFrames(collectKey) = existingDF.union(dataFrame)
 case None =>
 collectedDataFrames(collectKey) = dataFrame
 }

 // Execute streaming chunk write (maintain original logic)
 // Initializecounter
 val entityCounters = chunkCounters.getOrElseUpdate(entityName, mutable.Map[String, Long]())
 val currentChunkId = entityCounters.getOrElse(propertyGroup, 0L)

 // Determine chunk size
 val chunkSize = if (entityType.isVertex) vertex_chunk_size else edge_chunk_size

 // Calculate needed chunk count
 val totalRows = dataFrame.count()
 val chunksNeeded = math.ceil(totalRows.toDouble / chunkSize).toLong

 val writeResults = mutable.ListBuffer[ChunkWriteResult]()

 // Write in chunks (maintain streaming processing)
 for (chunkIndex <- 0L until chunksNeeded) {
 val startRow = chunkIndex * chunkSize
 val endRow = math.min((chunkIndex + 1) * chunkSize, totalRows)

 // Use limit and drop combined to implement pagination
 val chunkData = if (startRow == 0) {
 dataFrame.limit((endRow - startRow).toInt)
 } else {
 // Use row_number() window function to implement pagination
 import org.apache.spark.sql.expressions.Window
 import org.apache.spark.sql.functions._

 val windowSpec = Window.orderBy(monotonically_increasing_id())
 val numberedDF = dataFrame.withColumn("row_number", row_number().over(windowSpec))
 numberedDF
.filter(col("row_number") > startRow && col("row_number") <= endRow)
.drop("row_number")
 }

 val chunkWriteResult = writeChunk(
 chunkData,
 entityType,
 propertyGroup,
 currentChunkId + chunkIndex,
 startRow
)

 writeResults += chunkWriteResult
 }

 // Update counter
 entityCounters(propertyGroup) = currentChunkId + chunksNeeded

 // Update statistics
 updateEntityStatistics(entityName, propertyGroup, totalRows, chunksNeeded)

 WriteResult(
 success = true,
 entityName = entityName,
 propertyGroup = propertyGroup,
 totalRows = totalRows,
 chunksWritten = chunksNeeded,
 chunkResults = writeResults.toList
)

 } catch {
 case e: Exception =>
 logger.error(s"Failed to write entity data in streaming mode: entity=$entityName, propertyGroup=$propertyGroup", e)
 WriteResult(
 success = false,
 entityName = entityName,
 propertyGroup = propertyGroup,
 totalRows = 0L,
 chunksWritten = 0L,
 chunkResults = List.empty,
 error = Some(e.getMessage)
)
 }
 }




 /**
 * Generate GraphAr metadata files (use standard GraphWriter)
 */
 def generateGraphMetadata(schemas: List[EntitySchemaDetector.EntitySchema]): Unit = {
 logger.info("Using standard GraphWriter to generate GraphAr format data and metadata files")

 try {
 val writer = new GraphWriter()

 // Separate vertex and edge DataFrames
 val vertexDataFrames = mutable.Map[String, DataFrame]()
 val edgeDataFrames = mutable.Map[(String, String, String), DataFrame]()

 // Process collected DataFrames
 collectedDataFrames.foreach { case (collectKey, dataFrame) =>
 schemamapping.get(collectKey) match {
 case Some(schema) =>
 val entityType = schema.entityType

 if (entityType.isVertex) {
 // Vertex data
 val vertexType = entityType.name
 vertexDataFrames.get(vertexType) match {
 case Some(existingDF) =>
 vertexDataFrames(vertexType) = existingDF.union(dataFrame)
 case None =>
 vertexDataFrames(vertexType) = dataFrame
 }
 logger.info(s"Preparing to write vertex data: $vertexType, record count: ${dataFrame.count()}")

 } else if (entityType.isEdge) {
 // Edge data - need to parse relationships
 val edgeEntityType = entityType.asInstanceOf[EdgeEntityType]
 val relation = (
 edgeEntityType.srcVertexType.name,
 entityType.name,
 edgeEntityType.dstVertexType.name
)

 edgeDataFrames.get(relation) match {
 case Some(existingDF) =>
 edgeDataFrames(relation) = existingDF.union(dataFrame)
 case None =>
 edgeDataFrames(relation) = dataFrame
 }
 logger.info(s"Preparing to write edge data: ${relation._1}->${relation._2}->${relation._3}, record count: ${dataFrame.count()}")
 }

 case None =>
 logger.warn(s"Schema information not found: $collectKey")
 }
 }

 // Add data to GraphWriter
 vertexDataFrames.foreach { case (vertexType, dataFrame) =>
 writer.PutVertexData(vertexType, dataFrame)
 logger.info(s"Adding vertex data to GraphWriter: $vertexType")
 }

 edgeDataFrames.foreach { case (relation, dataFrame) =>
 writer.PutEdgeData(relation, dataFrame)
 logger.info(s"Adding edge data to GraphWriter: ${relation._1}->${relation._2}->${relation._3}")
 }

 // Write in one operation, automatically generate YAML metadata files
 writer.write(
 output_path,
 spark,
 graph_name,
 vertex_chunk_size,
 edge_chunk_size,
 file_type
)

 logger.info(s"GraphWriter write completed: output_path=$output_path, graph_name=$graph_name")

 } catch {
 case e: Exception =>
 logger.error("Failed to generate data and metadata files using GraphWriter", e)
 throw e
 }
 }

 /**
 * Write single chunk (core streaming processing method)
 */
 private def writeChunk(
 chunkData: DataFrame,
 entityType: LdbcEntityType,
 propertyGroup: String,
 chunkId: Long,
 startOffset: Long
): ChunkWriteResult = {

 val entityName = entityType.name

 // Build output path conforming to GraphAr standard
 val standardPath = getstandardGraphArPath(entityType)
 val propertyGroupDir = s"$standardPath/$propertyGroup"
 val chunkPath = s"$propertyGroupDir/chunk$chunkId"

 // Ensure directory exists
 new File(propertyGroupDir).mkdirs()

 try {
 // Write data file
 file_type.toLowerCase match {
 case "parquet" =>
 chunkData.write.mode("overwrite").parquet(chunkPath)

 case "csv" =>
 chunkData.write
.mode("overwrite")
.option("header", "true")
.option("encoding", "UTF-8")
.csv(chunkPath)

 case "orc" =>
 chunkData.write.mode("overwrite").orc(chunkPath)

 case _ =>
 logger.warn(s"Unsupported file type: $file_type, using parquet")
 chunkData.write.mode("overwrite").parquet(chunkPath)
 }

 val rowCount = chunkData.count()

 // RecordOffsetinformation
 recordOffsetInfo(entityName, propertyGroup, chunkId, startOffset, rowCount)

 logger.debug(s"Successfully wrote chunk: $chunkPath, row count: $rowCount")

 ChunkWriteResult(
 success = true,
 chunkId = chunkId,
 chunkPath = chunkPath,
 rowCount = rowCount,
 startOffset = startOffset,
 endOffset = startOffset + rowCount - 1
)

 } catch {
 case e: Exception =>
 logger.error(s"Failed to write chunk: $chunkPath", e)
 ChunkWriteResult(
 success = false,
 chunkId = chunkId,
 chunkPath = chunkPath,
 rowCount = 0L,
 startOffset = startOffset,
 endOffset = startOffset,
 error = Some(e.getMessage)
)
 }
 }

 /**
 * Record offset information (maintain index feature)
 */
 private def recordOffsetInfo(
 entityName: String,
 propertyGroup: String,
 chunkId: Long,
 startOffset: Long,
 rowCount: Long
): Unit = {

 val entityOffsets = offsetmappings.getOrElseUpdate(entityName, mutable.Map[String, mutable.Map[Long, OffsetInfo]]())
 val groupOffsets = entityOffsets.getOrElseUpdate(propertyGroup, mutable.Map[Long, OffsetInfo]())

 groupOffsets(chunkId) = OffsetInfo(
 chunkId = chunkId,
 startOffset = startOffset,
 endOffset = startOffset + rowCount - 1,
 rowCount = rowCount
)
 }

 /**
 * UpdateEntityStatistics
 */
 private def updateEntityStatistics(
 entityName: String,
 propertyGroup: String,
 rowCount: Long,
 chunkCount: Long
): Unit = {

 val stats = entityStatistics.getOrElseUpdate(entityName, EntityWriteStatistics(
 entityName = entityName,
 totalRows = 0L,
 totalChunks = 0L,
 propertyGroups = mutable.Map[String, PropertyGroupStats]()
))

 stats.totalRows += rowCount
 stats.totalChunks += chunkCount

 val groupStats = stats.propertyGroups.getOrElseUpdate(propertyGroup, PropertyGroupStats(
 propertyGroup = propertyGroup,
 rowCount = 0L,
 chunkCount = 0L
))

 groupStats.rowCount += rowCount
 groupStats.chunkCount += chunkCount
 }

 /**
 * Generate offset index files (maintain index feature)
 */
 def generateOffsetIndex(): Unit = {
 logger.info("Generating offset index files")

 offsetmappings.foreach { case (entityName, entityOffsets) =>
 entityOffsets.foreach { case (propertyGroup, groupOffsets) =>
 val indexPath = s"$output_path/$entityName/$propertyGroup/offset.idx"
 val writer = new BufferedWriter(new FileWriter(indexPath))

 try {
 writer.write("# GraphAr Offset Index\\n")
 writer.write("# Format: chunk_id,start_offset,end_offset,row_count\\n")

 groupOffsets.toSeq.sortBy(_._1).foreach { case (chunkId, offsetInfo) =>
 writer.write(s"${offsetInfo.chunkId},${offsetInfo.startOffset},${offsetInfo.endOffset},${offsetInfo.rowCount}\\n")
 }

 logger.debug(s"Offset index file generated: $indexPath")

 } finally {
 writer.close()
 }
 }
 }
 }

 /**
 * Get offset information
 */
 def getOffsetmappings(): Map[String, Map[String, Map[Long, OffsetInfo]]] = {
 offsetmappings.mapValues(_.mapValues(_.toMap).toMap).toMap
 }










 /**
 * Get write statistics
 */
 def getWriteStatistics(): Map[String, EntityWriteStatistics] = {
 entityStatistics.toMap
 }
}

// Maintain all necessary case classes

/**
 * Offsetinformation
 */
case class OffsetInfo(
 chunkId: Long,
 startOffset: Long,
 endOffset: Long,
 rowCount: Long
)

/**
 * chunkWriteResult
 */
case class ChunkWriteResult(
 success: Boolean,
 chunkId: Long,
 chunkPath: String,
 rowCount: Long,
 startOffset: Long,
 endOffset: Long,
 error: Option[String] = None
)

/**
 * WriteResult
 */
case class WriteResult(
 success: Boolean,
 entityName: String,
 propertyGroup: String,
 totalRows: Long,
 chunksWritten: Long,
 chunkResults: List[ChunkWriteResult],
 error: Option[String] = None
)

/**
 * PropertyGroupStatistics
 */
case class PropertyGroupStats(
 propertyGroup: String,
 var rowCount: Long,
 var chunkCount: Long
)

/**
 * EntityWriteStatistics
 */
case class EntityWriteStatistics(
 entityName: String,
 var totalRows: Long,
 var totalChunks: Long,
 propertyGroups: mutable.Map[String, PropertyGroupStats]
)

