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

package org.apache.graphar.datasources.ldbc.stream.processor

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * Chunked offset calculator
 *
 * GraphAr uses chunked storage to optimize large-scale graph data access performance:
 * 1. Divide large amounts of data into fixed-size chunks
 * 2. Data within each chunk is stored contiguously, improving locality
 * 3. Support parallel access to different chunks
 * 4. Facilitate memory management and I/O optimization
 *
 * For vertex data: chunk_size is typically set to 1024-4096
 * For edge data: chunk_size can be set larger, such as 8192-16384
 */
class OffsetCalculator(chunkSize: Long) {

 private val logger: Logger = LoggerFactory.getLogger(classOf[OffsetCalculator])

 require(chunkSize > 0, "Chunk size must be positive")

 logger.info(s"OffsetCalculator initialized with chunk size: $chunkSize")

 /**
 * Calculate chunk index and intra-chunk offset based on continuous ID
 *
 * @param internalId Continuous internal ID (starting from 0)
 * @return (chunkIndex, offset) tuple
 * - chunkIndex: Chunk index to which this ID belongs (starting from 0)
 * - offset: Offset of this ID within the chunk (starting from 0)
 */
 def calculateOffset(internalId: Long): (Int, Long) = {
 require(internalId >= 0, s"Internal ID must be non-negative, got: $internalId")

 val chunkIndex = (internalId / chunkSize).toInt
 val offset = internalId % chunkSize

 logger.debug(s"ID $internalId -> chunk $chunkIndex, offset $offset")

 (chunkIndex, offset)
 }

 /**
 * Calculate continuous ID from chunk index and offset
 *
 * @param chunkIndex ChunkIndex
 * @param offset Offset within chunk
 * @return Continuous ID
 */
 def calculateInternalId(chunkIndex: Int, offset: Long): Long = {
 require(chunkIndex >= 0, s"Chunk index must be non-negative, got: $chunkIndex")
 require(offset >= 0 && offset < chunkSize, s"Offset must be in range [0, $chunkSize), got: $offset")

 val internalId = chunkIndex.toLong * chunkSize + offset
 logger.debug(s"Chunk $chunkIndex, offset $offset -> ID $internalId")

 internalId
 }

 /**
 * Calculate ID range for specified chunk
 *
 * @param chunkIndex ChunkIndex
 * @return (startId, endId) ID range contained in this chunk (includes startId, excludes endId)
 */
 def getChunkIdRange(chunkIndex: Int): (Long, Long) = {
 require(chunkIndex >= 0, s"Chunk index must be non-negative, got: $chunkIndex")

 val startId = chunkIndex.toLong * chunkSize
 val endId = startId + chunkSize

 (startId, endId)
 }

 /**
 * Calculate number of chunks needed to store specified count of entities
 *
 * @param entityCount EntityCount
 * @return Number of chunks needed
 */
 def calculateChunkCount(entityCount: Long): Int = {
 require(entityCount >= 0, s"Entity count must be non-negative, got: $entityCount")

 if (entityCount == 0) {
 0
 } else {
 Math.ceil(entityCount.toDouble / chunkSize).toInt
 }
 }

 /**
 * Get fill rate for specified chunk
 *
 * @param chunkIndex ChunkIndex
 * @param totalEntities Total entity count
 * @return Fill rate (0.0 - 1.0)
 */
 def getChunkFillRatio(chunkIndex: Int, totalEntities: Long): Double = {
 require(chunkIndex >= 0, s"Chunk index must be non-negative, got: $chunkIndex")
 require(totalEntities >= 0, s"Total entities must be non-negative, got: $totalEntities")

 val (startId, endId) = getChunkIdRange(chunkIndex)

 if (startId >= totalEntities) {
 // Chunk completely empty
 0.0
 } else if (endId <= totalEntities) {
 // Chunk completely filled
 1.0
 } else {
 // Chunk partially filled
 val entitiesInChunk = totalEntities - startId
 entitiesInChunk.toDouble / chunkSize
 }
 }

 /**
 * Generate chunking statistics
 *
 * @param totalEntities Total entity count
 * @return Chunk statistics
 */
 def generateChunkStatistics(totalEntities: Long): ChunkStatistics = {
 require(totalEntities >= 0, s"Total entities must be non-negative, got: $totalEntities")

 val totalChunks = calculateChunkCount(totalEntities)

 if (totalChunks == 0) {
 return ChunkStatistics(
 totalEntities = 0,
 chunkSize = chunkSize,
 totalChunks = 0,
 fullChunks = 0,
 partialChunks = 0,
 lastChunkSize = 0,
 averageFillRatio = 0.0
)
 }

 val fullChunks = if (totalEntities % chunkSize == 0) totalChunks else totalChunks - 1
 val partialChunks = totalChunks - fullChunks
 val lastChunkSize = if (totalEntities % chunkSize == 0) chunkSize else totalEntities % chunkSize

 val totalCapacity = totalChunks.toLong * chunkSize
 val averageFillRatio = totalEntities.toDouble / totalCapacity

 ChunkStatistics(
 totalEntities = totalEntities,
 chunkSize = chunkSize,
 totalChunks = totalChunks,
 fullChunks = fullChunks,
 partialChunks = partialChunks,
 lastChunkSize = lastChunkSize,
 averageFillRatio = averageFillRatio
)
 }

 /**
 * Verify chunking configuration rationality
 */
 def validateChunkConfiguration(maxExpectedEntities: Long): ValidationResult = {
 val errors = mutable.ListBuffer[String]()

 // Check if chunk size is reasonable
 if (chunkSize < 64) {
 errors += s"Chunk size too small ($chunkSize), may cause excessive overhead"
 } else if (chunkSize > 65536) {
 errors += s"Chunk size too large ($chunkSize), may cause memory issues"
 }

 // Check if expected chunk count is reasonable
 if (maxExpectedEntities > 0) {
 val expectedChunks = calculateChunkCount(maxExpectedEntities)
 if (expectedChunks > 10000) {
 errors += s"Too many chunks expected ($expectedChunks), consider increasing chunk size"
 } else if (expectedChunks < 2 && maxExpectedEntities > chunkSize / 2) {
 errors += s"Too few chunks ($expectedChunks), consider decreasing chunk size for better parallelism"
 }
 }

 if (errors.isEmpty) {
 ValidationResult.success()
 } else {
 ValidationResult.failure(errors.toList)
 }
 }

 /**
 * Output detailed chunking statistics information
 */
 def logChunkStatistics(statistics: ChunkStatistics): Unit = {
 logger.info("=== Chunk Statistics ===")
 logger.info(s"Total entities: ${statistics.totalEntities}")
 logger.info(s"Chunk size: ${statistics.chunkSize}")
 logger.info(s"Total chunks: ${statistics.totalChunks}")
 logger.info(s"Full chunks: ${statistics.fullChunks}")
 logger.info(s"Partial chunks: ${statistics.partialChunks}")
 logger.info(s"Last chunk size: ${statistics.lastChunkSize}")
 logger.info(s"Average fill ratio: ${(statistics.averageFillRatio * 100).formatted("%.1f")}%")
 }

 /**
 * GetCurrentConfigureinformation
 */
 def getConfiguration: OffsetCalculatorConfig = {
 OffsetCalculatorConfig(chunkSize)
 }
}

/**
 * Chunk statistics
 */
case class ChunkStatistics(
 totalEntities: Long,
 chunkSize: Long,
 totalChunks: Int,
 fullChunks: Int,
 partialChunks: Int,
 lastChunkSize: Long,
 averageFillRatio: Double
)

/**
 * OffsetCalculator configuration
 */
case class OffsetCalculatorConfig(
 chunkSize: Long
)