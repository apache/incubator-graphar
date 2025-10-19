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

package org.apache.graphar.datasources.ldbc.stream.core

import org.apache.graphar.datasources.ldbc.stream.processor.UnifiedIdManager
import org.apache.graphar.datasources.ldbc.stream.writer.OffsetInfo
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * GraphAr Data Collector
 *
 * Unified collection of metadata from static data (RDD source) and streaming data (chunk source)
 * Provides data foundation for subsequent strategy selection and output processing
 */
class GraphArDataCollector(
  output_path: String,
  graph_name: String,
  val idManager: UnifiedIdManager
)(implicit spark: SparkSession) extends Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[GraphArDataCollector])

  // Static data collection (RDD source)
  private val staticDataFrames = mutable.Map[String, DataFrame]()
  private val staticEdgeFrames = mutable.Map[(String, String, String), DataFrame]()
  private val staticSchemas = mutable.Map[String, StructType]()

  // Streaming data metadata collection (chunk source)
  private val streamingEntityInfo = mutable.Map[String, StreamingEntityInfo]()
  private val streamingSchemas = mutable.Map[String, StructType]()

  // Collection statistics
  private var staticVertexCount = 0L
  private var staticEdgeCount = 0L
  private var streamingVertexCount = 0L
  private var streamingEdgeCount = 0L

  logger.info(s"GraphArDataCollector initialized: output_path=$output_path, graph_name=$graph_name")

  /**
   * Add static vertex data
   */
  def addStaticVertexData(entityType: String, df: DataFrame): Unit = {
    logger.info(s"Collecting static vertex data: $entityType")

    // Ensure IDs are assigned
    val dfWithIds = if (df.columns.contains("_graphArVertexIndex")) {
      logger.debug(s"Entity $entityType already contains _graphArVertexIndex column")
      df
    } else {
      logger.debug(s"Assigning vertex IDs to entity $entityType")
      idManager.assignVertexIds(df, entityType)
    }

    val recordCount = dfWithIds.count()
    staticDataFrames(entityType) = dfWithIds
    staticSchemas(entityType) = dfWithIds.schema
    staticVertexCount += recordCount

    logger.info(s"Static vertex data collection completed: $entityType, record count: $recordCount")
  }

  /**
   * Add static edge data
   */
  def addStaticEdgeData(relation: (String, String, String), df: DataFrame): Unit = {
    val (srcType, edgeType, dstType) = relation
    logger.info(s"Collecting static edge data: ${srcType}_${edgeType}_${dstType}")

    val recordCount = df.count()
    staticEdgeFrames(relation) = df
    staticSchemas(s"${srcType}_${edgeType}_${dstType}") = df.schema
    staticEdgeCount += recordCount

    logger.info(s"Static edge data collection completed: ${srcType}_${edgeType}_${dstType}, record count: $recordCount")
  }

  /**
   * Record streaming entity information
   */
  def recordStreamingEntity(
    entityType: String,
    chunkCount: Long,
    totalRows: Long,
    schema: StructType,
    offsetMapping: Map[String, Map[Long, OffsetInfo]]
  ): Unit = {
    logger.info(s"Recording streaming entity info: $entityType, chunks: $chunkCount, rows: $totalRows")

    streamingEntityInfo(entityType) = StreamingEntityInfo(
      entityType = entityType,
      chunkCount = chunkCount,
      totalRows = totalRows,
      schema = schema,
      offsetMapping = offsetMapping
    )

    streamingSchemas(entityType) = schema

    if (isVertexEntity(entityType)) {
      streamingVertexCount += totalRows
    } else {
      streamingEdgeCount += totalRows
    }

    logger.info(s"Streaming entity info recording completed: $entityType")
  }

  /**
   * Get static DataFrame collection
   */
  def getStaticDataFrames(): Map[String, DataFrame] = staticDataFrames.toMap

  /**
   * Get static edge DataFrame collection
   */
  def getStaticEdgeFrames(): Map[(String, String, String), DataFrame] = staticEdgeFrames.toMap

  /**
   * Get streaming entity information collection
   */
  def getStreamingEntityInfo(): Map[String, StreamingEntityInfo] = streamingEntityInfo.toMap

  /**
   * Get all static schemas
   */
  def getStaticSchemas(): Map[String, StructType] = staticSchemas.toMap

  /**
   * Get all streaming schemas
   */
  def getStreamingSchemas(): Map[String, StructType] = streamingSchemas.toMap

  /**
   * Get collection statistics
   */
  def getCollectionStatistics(): DataCollectionStatistics = {
    val totalStaticEntities = staticDataFrames.size + staticEdgeFrames.size
    val totalStreamingEntities = streamingEntityInfo.size
    val totalVertices = staticVertexCount + streamingVertexCount
    val totalEdges = staticEdgeCount + streamingEdgeCount

    val staticEntityTypes = staticDataFrames.keys.toList
    val streamingEntityTypes = streamingEntityInfo.keys.toList
    val allEntityTypes = (staticEntityTypes ++ streamingEntityTypes).distinct

    DataCollectionStatistics(
      totalEntities = totalStaticEntities + totalStreamingEntities,
      staticEntities = totalStaticEntities,
      streamingEntities = totalStreamingEntities,
      totalVertices = totalVertices,
      totalEdges = totalEdges,
      staticVertices = staticVertexCount,
      streamingVertices = streamingVertexCount,
      staticEdges = staticEdgeCount,
      streamingEdges = streamingEdgeCount,
      staticEntityTypes = staticEntityTypes,
      streamingEntityTypes = streamingEntityTypes,
      allEntityTypes = allEntityTypes,
      streamingRatio = streamingVertexCount.toDouble / totalVertices
    )
  }

  /**
   * Generate data overview report
   */
  def generateDataOverview(): DataOverviewReport = {
    val statistics = getCollectionStatistics()
    val idUsageStats = idManager.generateUsageStatistics()

    // Analyze schema compatibility
    val schemaCompatibility = analyzeSchemaCompatibility()

    DataOverviewReport(
      graphName = graph_name,
      outputPath = output_path,
      collectionStats = statistics,
      idUsageStats = idUsageStats,
      schemaCompatibility = schemaCompatibility,
      recommendations = generateRecommendations(statistics)
    )
  }

  /**
   * Analyze schema compatibility
   */
  private def analyzeSchemaCompatibility(): SchemaCompatibilityReport = {
    val conflicts = mutable.ListBuffer[SchemaConflict]()
    val compatibleEntities = mutable.ListBuffer[String]()

    // Check schema conflicts between static and streaming data
    val commonEntities = staticSchemas.keySet.intersect(streamingSchemas.keySet)

    commonEntities.foreach { entityType =>
      val staticSchema = staticSchemas(entityType)
      val streamingSchema = streamingSchemas(entityType)

      val entityConflicts = detectSchemaConflicts(entityType, staticSchema, streamingSchema)
      if (entityConflicts.nonEmpty) {
        conflicts ++= entityConflicts
      } else {
        compatibleEntities += entityType
      }
    }

    SchemaCompatibilityReport(
      totalEntitiesChecked = commonEntities.size,
      conflictCount = conflicts.size,
      compatibleEntities = compatibleEntities.toList,
      conflicts = conflicts.toList,
      isFullyCompatible = conflicts.isEmpty
    )
  }

  /**
   * Detect conflicts between two schemas
   */
  private def detectSchemaConflicts(
    entityType: String,
    staticSchema: StructType,
    streamingSchema: StructType
  ): List[SchemaConflict] = {

    val conflicts = mutable.ListBuffer[SchemaConflict]()

    staticSchema.fields.foreach { staticField =>
      streamingSchema.fields.find(_.name == staticField.name) match {
        case Some(streamingField) if staticField.dataType != streamingField.dataType =>
          conflicts += SchemaConflict(
            entityType = entityType,
            fieldName = staticField.name,
            staticType = staticField.dataType.typeName,
            streamingType = streamingField.dataType.typeName,
            conflictType = "type_mismatch"
          )
        case Some(streamingField) if staticField.nullable != streamingField.nullable =>
          conflicts += SchemaConflict(
            entityType = entityType,
            fieldName = staticField.name,
            staticType = s"nullable=${staticField.nullable}",
            streamingType = s"nullable=${streamingField.nullable}",
            conflictType = "nullable_mismatch"
          )
        case None =>
          conflicts += SchemaConflict(
            entityType = entityType,
            fieldName = staticField.name,
            staticType = staticField.dataType.typeName,
            streamingType = "MISSING",
            conflictType = "missing_field"
          )
        case _ => // Compatible
      }
    }

    conflicts.toList
  }

  /**
   * Generate processing recommendations
   */
  private def generateRecommendations(stats: DataCollectionStatistics): List[String] = {
    val recommendations = mutable.ListBuffer[String]()

    // Recommendations based on data scale
    if (stats.totalVertices > 10000000) {
      recommendations += "Large data scale (>10M vertices), recommend using hybrid format output to optimize performance"
    } else if (stats.totalVertices < 1000000) {
      recommendations += "Moderate data scale (<1M vertices), recommend using fully standardized output for compatibility"
    }

    // Recommendations based on streaming data ratio
    if (stats.streamingRatio > 0.8) {
      recommendations += s"High streaming data ratio (${(stats.streamingRatio * 100).toInt}%), recommend maintaining chunk format for performance advantages"
    } else if (stats.streamingRatio < 0.2) {
      recommendations += s"Static data dominates (${((1 - stats.streamingRatio) * 100).toInt}%), recommend using standard GraphWriter"
    }

    // Recommendations based on entity type count
    if (stats.allEntityTypes.length > 15) {
      recommendations += "Many entity types, recommend enabling intelligent schema merging to handle complexity"
    }

    recommendations.toList
  }

  /**
   * Check if entity is a vertex entity
   */
  def isVertexEntity(entityType: String): Boolean = {
    val vertexEntities = Set("Person", "Organisation", "Place", "Tag", "TagClass", "Forum", "Post", "Comment", "Photo")
    vertexEntities.contains(entityType)
  }

  /**
   * Get total entity count
   */
  def getTotalEntityCount(): Int = {
    staticDataFrames.size + staticEdgeFrames.size + streamingEntityInfo.size
  }

  /**
   * Clean up collected data (for memory management)
   */
  def cleanup(): Unit = {
    logger.info("Cleaning up GraphArDataCollector cached data")

    staticDataFrames.clear()
    staticEdgeFrames.clear()
    staticSchemas.clear()
    streamingEntityInfo.clear()
    streamingSchemas.clear()

    staticVertexCount = 0L
    staticEdgeCount = 0L
    streamingVertexCount = 0L
    streamingEdgeCount = 0L

    logger.info("GraphArDataCollector cleanup completed")
  }
}

/**
 * Streaming entity information
 */
case class StreamingEntityInfo(
  entityType: String,
  chunkCount: Long,
  totalRows: Long,
  schema: StructType,
  offsetMapping: Map[String, Map[Long, OffsetInfo]]
)

/**
 * Data collection statistics
 */
case class DataCollectionStatistics(
  totalEntities: Int,
  staticEntities: Int,
  streamingEntities: Int,
  totalVertices: Long,
  totalEdges: Long,
  staticVertices: Long,
  streamingVertices: Long,
  staticEdges: Long,
  streamingEdges: Long,
  staticEntityTypes: List[String],
  streamingEntityTypes: List[String],
  allEntityTypes: List[String],
  streamingRatio: Double
)

/**
 * Schema conflict information
 */
case class SchemaConflict(
  entityType: String,
  fieldName: String,
  staticType: String,
  streamingType: String,
  conflictType: String
) {
  def description: String = s"Entity $entityType field $fieldName: $conflictType (static=$staticType, streaming=$streamingType)"
}

/**
 * Schema compatibility report
 */
case class SchemaCompatibilityReport(
  totalEntitiesChecked: Int,
  conflictCount: Int,
  compatibleEntities: List[String],
  conflicts: List[SchemaConflict],
  isFullyCompatible: Boolean
)

/**
 * Data overview report
 */
case class DataOverviewReport(
  graphName: String,
  outputPath: String,
  collectionStats: DataCollectionStatistics,
  idUsageStats: org.apache.graphar.datasources.ldbc.stream.processor.IdSpaceUsageStatistics,
  schemaCompatibility: SchemaCompatibilityReport,
  recommendations: List[String]
)