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
 * Unified ID Manager
 *
 * Solves vertex ID space conflict between static data (RDD-generated) and
 * streaming data Pre-allocates ID space based on LDBC scale factor to ensure
 * global ID consistency
 */
class UnifiedIdManager(
    scaleFactor: Double,
    config: Map[String, String] = Map.empty
) {

  private val logger: Logger =
    LoggerFactory.getLogger(classOf[UnifiedIdManager])

  // Entity count estimates based on LDBC scale factor
  private val entityCountEstimates = calculateEntityCounts(scaleFactor)

  // ID space allocation table: entity type -> ID space info
  private val idSpaceAllocation = mutable.Map[String, IdSpaceInfo]()

  // Global ID counter (atomic operations ensure thread safety)
  private val globalIdCounter = new AtomicLong(0)

  // Initialization flag
  @volatile private var initialized = false

  logger.info(s"UnifiedIdManager initialized: scaleFactor=$scaleFactor")

  /**
   * Initialize ID space allocation
   *
   * Pre-allocates ID space for all entity types based on scale factor. This
   * method is thread-safe and idempotent.
   */
  def initialize(): Unit = {
    if (initialized) {
      logger.warn(
        "UnifiedIdManager already initialized, skipping duplicate initialization"
      )
      return
    }

    synchronized {
      if (!initialized) {
        logger.info("Starting ID space allocation initialization")

        var currentOffset = 0L

        // Static entity ID space allocation (reserve sufficient space)
        val staticEntities =
          List("Person", "Organisation", "Place", "Tag", "TagClass")
        staticEntities.foreach { entityType =>
          val estimatedCount = entityCountEstimates.getOrElse(entityType, 1000L)
          val reservedSpace =
            (estimatedCount * 1.2).toLong // Reserve 20% extra space

          idSpaceAllocation(entityType) = IdSpaceInfo(
            entityType = entityType,
            startId = currentOffset,
            endId = currentOffset + reservedSpace - 1,
            currentPosition = new AtomicLong(currentOffset),
            entityCategory = "static"
          )

          logger.info(
            s"Static entity $entityType ID space: [$currentOffset, ${currentOffset + reservedSpace - 1}], estimated count: $estimatedCount"
          )
          currentOffset += reservedSpace
        }

        // Dynamic entity ID space allocation
        val dynamicEntities = List("Forum", "Post", "Comment", "Photo")
        dynamicEntities.foreach { entityType =>
          val estimatedCount =
            entityCountEstimates.getOrElse(entityType, 10000L)
          val reservedSpace =
            (estimatedCount * 1.5).toLong // Reserve more space for dynamic data

          idSpaceAllocation(entityType) = IdSpaceInfo(
            entityType = entityType,
            startId = currentOffset,
            endId = currentOffset + reservedSpace - 1,
            currentPosition = new AtomicLong(currentOffset),
            entityCategory = "dynamic"
          )

          logger.info(
            s"Dynamic entity $entityType ID space: [$currentOffset, ${currentOffset + reservedSpace - 1}], estimated count: $estimatedCount"
          )
          currentOffset += reservedSpace
        }

        globalIdCounter.set(currentOffset)
        initialized = true

        logger.info(
          s"ID space initialization completed, total pre-allocated space: $currentOffset"
        )
      }
    }
  }

  /**
   * Allocate ID range for specified entity type
   *
   * @param entityType
   *   Entity type requesting ID allocation
   * @param requestedCount
   *   Number of IDs to allocate
   * @return
   *   IdRange containing start, end, and count of allocated IDs
   * @throws IllegalStateException
   *   if manager is not initialized
   */
  def allocateIdRange(entityType: String, requestedCount: Long): IdRange = {
    if (!initialized) {
      throw new IllegalStateException(
        "UnifiedIdManager not initialized, please call initialize() first"
      )
    }

    val spaceInfo = idSpaceAllocation.getOrElseUpdate(
      entityType, {
        logger.warn(
          s"Entity type $entityType not in pre-allocated space, dynamically creating ID space"
        )
        expandIdSpace(entityType, requestedCount)
      }
    )

    val startId = spaceInfo.currentPosition.getAndAdd(requestedCount)
    val endId = startId + requestedCount - 1

    // Check if exceeding pre-allocated space
    if (endId > spaceInfo.endId) {
      logger.warn(
        s"Entity $entityType ID space insufficient (requested: $requestedCount, remaining: ${spaceInfo.endId - startId + 1}), auto-expanding"
      )
      expandIdSpace(entityType, requestedCount * 2)
    }

    logger.debug(
      s"Allocated ID range for entity $entityType: [$startId, $endId], count: $requestedCount"
    )

    IdRange(startId, endId, requestedCount)
  }

  /**
   * Assign vertex IDs to DataFrame
   *
   * Allocates a continuous ID range for the DataFrame and adds
   * _graphArVertexIndex column.
   *
   * @param df
   *   DataFrame to assign IDs to
   * @param entityType
   *   Entity type for ID space allocation
   * @return
   *   DataFrame with added _graphArVertexIndex, _entityType, and
   *   _idSpaceCategory columns
   */
  def assignVertexIds(df: DataFrame, entityType: String): DataFrame = {
    val rowCount = df.count()
    val idRange = allocateIdRange(entityType, rowCount)

    logger.info(
      s"Assigned vertex IDs for entity $entityType: record count=$rowCount, ID range=[${idRange.startId}, ${idRange.endId}]"
    )

    // Add continuous _graphArVertexIndex column to DataFrame
    df.withColumn(
      "_graphArVertexIndex",
      monotonically_increasing_id() + idRange.startId
    ).withColumn("_entityType", lit(entityType))
      .withColumn("_idSpaceCategory", lit(getEntityCategory(entityType)))
  }

  /**
   * Get ID space usage report
   *
   * @return
   *   Map of entity type to IdSpaceInfo showing current ID allocation state
   */
  def getIdSpaceReport(): Map[String, IdSpaceInfo] = {
    idSpaceAllocation.toMap
  }

  /**
   * Get ID space category for entity
   *
   * @param entityType
   *   Entity type to query
   * @return
   *   Category string ("static", "dynamic", "expanded", or "unknown")
   */
  def getEntityCategory(entityType: String): String = {
    idSpaceAllocation.get(entityType) match {
      case Some(spaceInfo) => spaceInfo.entityCategory
      case None            => "unknown"
    }
  }

  /**
   * Check if ID manager is initialized
   *
   * @return
   *   true if initialize() has been called successfully
   */
  def isInitialized: Boolean = initialized

  /**
   * Dynamically expand ID space
   */
  private def expandIdSpace(
      entityType: String,
      additionalSpace: Long
  ): IdSpaceInfo = {
    val currentGlobalOffset = globalIdCounter.getAndAdd(additionalSpace)

    val expandedInfo = IdSpaceInfo(
      entityType = entityType,
      startId = currentGlobalOffset,
      endId = currentGlobalOffset + additionalSpace - 1,
      currentPosition = new AtomicLong(currentGlobalOffset),
      entityCategory = "expanded"
    )

    logger.info(
      s"Expanded entity $entityType ID space: [$currentGlobalOffset, ${currentGlobalOffset + additionalSpace - 1}]"
    )
    expandedInfo
  }

  /**
   * Calculate estimated entity counts based on LDBC scale factor
   */
  private def calculateEntityCounts(scaleFactor: Double): Map[String, Long] = {
    // Entity count calculation formulas based on LDBC SNB specification
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
   * Generate ID space usage statistics
   *
   * @return
   *   IdSpaceUsageStatistics containing detailed usage information for all
   *   entity types
   */
  def generateUsageStatistics(): IdSpaceUsageStatistics = {
    val entityStats: List[EntityIdUsage] = idSpaceAllocation.map {
      case (entityType, spaceInfo) =>
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
 * ID space information
 */
case class IdSpaceInfo(
    entityType: String,
    startId: Long,
    endId: Long,
    currentPosition: AtomicLong,
    entityCategory: String
)

/**
 * ID range
 */
case class IdRange(startId: Long, endId: Long, count: Long)

/**
 * Entity ID usage information
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
 * ID space usage statistics
 */
case class IdSpaceUsageStatistics(
    totalAllocatedIds: Long,
    totalReservedSpace: Long,
    overallUtilization: Double,
    entityUsages: List[EntityIdUsage]
)
