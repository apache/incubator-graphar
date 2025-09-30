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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

/**
 * 统一ID管理器
 *
 * 解决静态数据（RDD生成）和流式数据的顶点ID空间冲突问题
 * 基于LDBC scale factor预分配ID空间，确保全局ID一致性
 */
class UnifiedIdManager(scaleFactor: Double, config: Map[String, String] = Map.empty) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[UnifiedIdManager])

  // 基于LDBC scale factor计算的实体数量预估
  private val entityCountEstimates = calculateEntityCounts(scaleFactor)

  // ID空间分配表：实体类型 -> ID空间信息
  private val idSpaceAllocation = mutable.Map[String, IdSpaceInfo]()

  // 全局ID计数器（原子操作保证线程安全）
  private val globalIdCounter = new AtomicLong(0)

  // 初始化标志
  @volatile private var initialized = false

  logger.info(s"UnifiedIdManager初始化: scaleFactor=$scaleFactor")

  /**
   * 初始化ID空间分配
   */
  def initialize(): Unit = {
    if (initialized) {
      logger.warn("UnifiedIdManager已经初始化，跳过重复初始化")
      return
    }

    synchronized {
      if (!initialized) {
        logger.info("开始初始化ID空间分配")

        var currentOffset = 0L

        // 静态实体ID空间分配（预留足够空间）
        val staticEntities = List("Person", "Organisation", "Place", "Tag", "TagClass")
        staticEntities.foreach { entityType =>
          val estimatedCount = entityCountEstimates.getOrElse(entityType, 1000L)
          val reservedSpace = (estimatedCount * 1.2).toLong // 预留20%空间

          idSpaceAllocation(entityType) = IdSpaceInfo(
            entityType = entityType,
            startId = currentOffset,
            endId = currentOffset + reservedSpace - 1,
            currentPosition = new AtomicLong(currentOffset),
            entityCategory = "static"
          )

          logger.info(s"静态实体 $entityType ID空间: [$currentOffset, ${currentOffset + reservedSpace - 1}], 估算数量: $estimatedCount")
          currentOffset += reservedSpace
        }

        // 动态实体ID空间分配
        val dynamicEntities = List("Forum", "Post", "Comment", "Photo")
        dynamicEntities.foreach { entityType =>
          val estimatedCount = entityCountEstimates.getOrElse(entityType, 10000L)
          val reservedSpace = (estimatedCount * 1.5).toLong // 动态数据预留更多空间

          idSpaceAllocation(entityType) = IdSpaceInfo(
            entityType = entityType,
            startId = currentOffset,
            endId = currentOffset + reservedSpace - 1,
            currentPosition = new AtomicLong(currentOffset),
            entityCategory = "dynamic"
          )

          logger.info(s"动态实体 $entityType ID空间: [$currentOffset, ${currentOffset + reservedSpace - 1}], 估算数量: $estimatedCount")
          currentOffset += reservedSpace
        }

        globalIdCounter.set(currentOffset)
        initialized = true

        logger.info(s"ID空间初始化完成，总预分配空间: $currentOffset")
      }
    }
  }

  /**
   * 为指定实体类型分配ID范围
   */
  def allocateIdRange(entityType: String, requestedCount: Long): IdRange = {
    if (!initialized) {
      throw new IllegalStateException("UnifiedIdManager未初始化，请先调用initialize()方法")
    }

    val spaceInfo = idSpaceAllocation.getOrElseUpdate(entityType, {
      logger.warn(s"实体类型 $entityType 未在预分配空间中，动态创建ID空间")
      expandIdSpace(entityType, requestedCount)
    })

    val startId = spaceInfo.currentPosition.getAndAdd(requestedCount)
    val endId = startId + requestedCount - 1

    // 检查是否超出预分配空间
    if (endId > spaceInfo.endId) {
      logger.warn(s"实体 $entityType ID空间不足 (请求: $requestedCount, 剩余: ${spaceInfo.endId - startId + 1})，自动扩展")
      expandIdSpace(entityType, requestedCount * 2)
    }

    logger.debug(s"为实体 $entityType 分配ID范围: [$startId, $endId], 数量: $requestedCount")

    IdRange(startId, endId, requestedCount)
  }

  /**
   * 为DataFrame分配顶点ID
   */
  def assignVertexIds(df: DataFrame, entityType: String): DataFrame = {
    val rowCount = df.count()
    val idRange = allocateIdRange(entityType, rowCount)

    logger.info(s"为实体 $entityType 分配顶点ID: 记录数=$rowCount, ID范围=[${idRange.startId}, ${idRange.endId}]")

    // 为DataFrame添加连续的_graphArVertexIndex列
    df.withColumn("_graphArVertexIndex", monotonically_increasing_id() + idRange.startId)
      .withColumn("_entityType", lit(entityType))
      .withColumn("_idSpaceCategory", lit(getEntityCategory(entityType)))
  }

  /**
   * 获取ID空间使用报告
   */
  def getIdSpaceReport(): Map[String, IdSpaceInfo] = {
    idSpaceAllocation.toMap
  }

  /**
   * 获取实体的ID空间类别
   */
  def getEntityCategory(entityType: String): String = {
    idSpaceAllocation.get(entityType) match {
      case Some(spaceInfo) => spaceInfo.entityCategory
      case None => "unknown"
    }
  }

  /**
   * 检查ID管理器是否已初始化
   */
  def isInitialized: Boolean = initialized

  /**
   * 动态扩展ID空间
   */
  private def expandIdSpace(entityType: String, additionalSpace: Long): IdSpaceInfo = {
    val currentGlobalOffset = globalIdCounter.getAndAdd(additionalSpace)

    val expandedInfo = IdSpaceInfo(
      entityType = entityType,
      startId = currentGlobalOffset,
      endId = currentGlobalOffset + additionalSpace - 1,
      currentPosition = new AtomicLong(currentGlobalOffset),
      entityCategory = "expanded"
    )

    logger.info(s"扩展实体 $entityType ID空间: [$currentGlobalOffset, ${currentGlobalOffset + additionalSpace - 1}]")
    expandedInfo
  }

  /**
   * 基于LDBC scale factor计算各实体类型的预估数量
   */
  private def calculateEntityCounts(scaleFactor: Double): Map[String, Long] = {
    // 基于LDBC SNB规范的实体数量计算公式
    val basePersonCount = (1000 * scaleFactor).toLong

    Map(
      "Person" -> basePersonCount,
      "Organisation" -> (basePersonCount * 0.01).toLong.max(100),
      "Place" -> (basePersonCount * 0.05).toLong.max(500),
      "Tag" -> (basePersonCount * 0.1).toLong.max(1000),
      "TagClass" -> (basePersonCount * 0.001).toLong.max(10),
      "Forum" -> (basePersonCount * 0.2).toLong.max(2000),
      "Post" -> (basePersonCount * 2.0).toLong.max(20000),
      "Comment" -> (basePersonCount * 5.0).toLong.max(50000),
      "Photo" -> (basePersonCount * 0.5).toLong.max(5000)
    )
  }

  /**
   * 生成ID空间使用统计
   */
  def generateUsageStatistics(): IdSpaceUsageStatistics = {
    val entityStats: List[EntityIdUsage] = idSpaceAllocation.map { case (entityType, spaceInfo) =>
      val allocated = spaceInfo.currentPosition.get() - spaceInfo.startId
      val total = spaceInfo.endId - spaceInfo.startId + 1
      val utilizationRate = allocated.toDouble / total

      EntityIdUsage(
        entityType = entityType,
        category = spaceInfo.entityCategory,
        allocatedIds = allocated,
        totalReserved = total,
        utilizationRate = utilizationRate,
        startId = spaceInfo.startId,
        currentPosition = spaceInfo.currentPosition.get()
      )
    }.toList

    val totalAllocated = entityStats.map(_.allocatedIds).sum
    val totalReserved = entityStats.map(_.totalReserved).sum

    IdSpaceUsageStatistics(
      totalAllocatedIds = totalAllocated,
      totalReservedSpace = totalReserved,
      overallUtilization = totalAllocated.toDouble / totalReserved,
      entityUsages = entityStats
    )
  }
}

/**
 * ID空间信息
 */
case class IdSpaceInfo(
  entityType: String,
  startId: Long,
  endId: Long,
  currentPosition: AtomicLong,
  entityCategory: String
)

/**
 * ID范围
 */
case class IdRange(startId: Long, endId: Long, count: Long)

/**
 * 实体ID使用情况
 */
case class EntityIdUsage(
  entityType: String,
  category: String,
  allocatedIds: Long,
  totalReserved: Long,
  utilizationRate: Double,
  startId: Long,
  currentPosition: Long
)

/**
 * ID空间使用统计
 */
case class IdSpaceUsageStatistics(
  totalAllocatedIds: Long,
  totalReservedSpace: Long,
  overallUtilization: Double,
  entityUsages: List[EntityIdUsage]
)