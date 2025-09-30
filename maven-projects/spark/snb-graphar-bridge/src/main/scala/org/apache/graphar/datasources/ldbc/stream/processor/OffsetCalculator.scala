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
 * 分块偏移计算器
 *
 * GraphAr使用分块存储来优化大规模图数据的访问性能：
 * 1. 将大量数据分割为固定大小的块（chunk）
 * 2. 每个块内的数据连续存储，提高局部性
 * 3. 支持并行访问不同的块
 * 4. 便于内存管理和I/O优化
 *
 * 对于顶点数据：chunk_size通常设置为1024-4096
 * 对于边数据：chunk_size可以设置更大，如8192-16384
 */
class OffsetCalculator(chunkSize: Long) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[OffsetCalculator])

  require(chunkSize > 0, "Chunk size must be positive")

  logger.info(s"OffsetCalculator initialized with chunk size: $chunkSize")

  /**
   * 根据连续ID计算分块索引和块内偏移
   *
   * @param internalId 连续的内部ID（从0开始）
   * @return (chunkIndex, offset) 元组
   *         - chunkIndex: 该ID所属的块索引（从0开始）
   *         - offset: 该ID在块内的偏移（从0开始）
   */
  def calculateOffset(internalId: Long): (Int, Long) = {
    require(internalId >= 0, s"Internal ID must be non-negative, got: $internalId")

    val chunkIndex = (internalId / chunkSize).toInt
    val offset = internalId % chunkSize

    logger.debug(s"ID $internalId -> chunk $chunkIndex, offset $offset")

    (chunkIndex, offset)
  }

  /**
   * 根据分块索引和偏移反向计算连续ID
   *
   * @param chunkIndex 块索引
   * @param offset 块内偏移
   * @return 连续ID
   */
  def calculateInternalId(chunkIndex: Int, offset: Long): Long = {
    require(chunkIndex >= 0, s"Chunk index must be non-negative, got: $chunkIndex")
    require(offset >= 0 && offset < chunkSize, s"Offset must be in range [0, $chunkSize), got: $offset")

    val internalId = chunkIndex.toLong * chunkSize + offset
    logger.debug(s"Chunk $chunkIndex, offset $offset -> ID $internalId")

    internalId
  }

  /**
   * 计算指定块的ID范围
   *
   * @param chunkIndex 块索引
   * @return (startId, endId) 该块包含的ID范围（包含startId，不包含endId）
   */
  def getChunkIdRange(chunkIndex: Int): (Long, Long) = {
    require(chunkIndex >= 0, s"Chunk index must be non-negative, got: $chunkIndex")

    val startId = chunkIndex.toLong * chunkSize
    val endId = startId + chunkSize

    (startId, endId)
  }

  /**
   * 计算存储指定数量实体需要的块数
   *
   * @param entityCount 实体数量
   * @return 需要的块数
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
   * 获取指定块的填充率
   *
   * @param chunkIndex 块索引
   * @param totalEntities 总实体数量
   * @return 填充率（0.0 - 1.0）
   */
  def getChunkFillRatio(chunkIndex: Int, totalEntities: Long): Double = {
    require(chunkIndex >= 0, s"Chunk index must be non-negative, got: $chunkIndex")
    require(totalEntities >= 0, s"Total entities must be non-negative, got: $totalEntities")

    val (startId, endId) = getChunkIdRange(chunkIndex)

    if (startId >= totalEntities) {
      // 块完全空
      0.0
    } else if (endId <= totalEntities) {
      // 块完全填满
      1.0
    } else {
      // 块部分填充
      val entitiesInChunk = totalEntities - startId
      entitiesInChunk.toDouble / chunkSize
    }
  }

  /**
   * 生成分块统计信息
   *
   * @param totalEntities 总实体数量
   * @return 分块统计信息
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
   * 验证分块配置的合理性
   */
  def validateChunkConfiguration(maxExpectedEntities: Long): ValidationResult = {
    val errors = mutable.ListBuffer[String]()

    // 检查块大小是否合理
    if (chunkSize < 64) {
      errors += s"Chunk size too small ($chunkSize), may cause excessive overhead"
    } else if (chunkSize > 65536) {
      errors += s"Chunk size too large ($chunkSize), may cause memory issues"
    }

    // 检查预期的块数量是否合理
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
   * 输出分块统计的详细信息
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
   * 获取当前配置信息
   */
  def getConfiguration: OffsetCalculatorConfig = {
    OffsetCalculatorConfig(chunkSize)
  }
}

/**
 * 分块统计信息
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
 * 偏移计算器配置
 */
case class OffsetCalculatorConfig(
  chunkSize: Long
)