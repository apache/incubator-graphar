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
 * GraphAr数据收集器
 *
 * 统一收集静态数据（RDD来源）和流式数据（chunk来源）的元信息
 * 为后续的策略选择和输出处理提供数据基础
 */
class GraphArDataCollector(
  outputPath: String,
  graphName: String,
  val idManager: UnifiedIdManager
)(implicit spark: SparkSession) extends Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[GraphArDataCollector])

  // 静态数据收集（RDD来源）
  private val staticDataFrames = mutable.Map[String, DataFrame]()
  private val staticEdgeFrames = mutable.Map[(String, String, String), DataFrame]()
  private val staticSchemas = mutable.Map[String, StructType]()

  // 流式数据元信息收集（chunk来源）
  private val streamingEntityInfo = mutable.Map[String, StreamingEntityInfo]()
  private val streamingSchemas = mutable.Map[String, StructType]()

  // 收集统计信息
  private var staticVertexCount = 0L
  private var staticEdgeCount = 0L
  private var streamingVertexCount = 0L
  private var streamingEdgeCount = 0L

  logger.info(s"GraphArDataCollector初始化: outputPath=$outputPath, graphName=$graphName")

  /**
   * 添加静态顶点数据
   */
  def addStaticVertexData(entityType: String, df: DataFrame): Unit = {
    logger.info(s"收集静态顶点数据: $entityType")

    // 确保ID已分配
    val dfWithIds = if (df.columns.contains("_graphArVertexIndex")) {
      logger.debug(s"实体 $entityType 已包含_graphArVertexIndex列")
      df
    } else {
      logger.debug(s"为实体 $entityType 分配顶点ID")
      idManager.assignVertexIds(df, entityType)
    }

    val recordCount = dfWithIds.count()
    staticDataFrames(entityType) = dfWithIds
    staticSchemas(entityType) = dfWithIds.schema
    staticVertexCount += recordCount

    logger.info(s"静态顶点数据收集完成: $entityType, 记录数: $recordCount")
  }

  /**
   * 添加静态边数据
   */
  def addStaticEdgeData(relation: (String, String, String), df: DataFrame): Unit = {
    val (srcType, edgeType, dstType) = relation
    logger.info(s"收集静态边数据: ${srcType}_${edgeType}_${dstType}")

    val recordCount = df.count()
    staticEdgeFrames(relation) = df
    staticSchemas(s"${srcType}_${edgeType}_${dstType}") = df.schema
    staticEdgeCount += recordCount

    logger.info(s"静态边数据收集完成: ${srcType}_${edgeType}_${dstType}, 记录数: $recordCount")
  }

  /**
   * 记录流式实体信息
   */
  def recordStreamingEntity(
    entityType: String,
    chunkCount: Long,
    totalRows: Long,
    schema: StructType,
    offsetMapping: Map[String, Map[Long, OffsetInfo]]
  ): Unit = {
    logger.info(s"记录流式实体信息: $entityType, chunks: $chunkCount, rows: $totalRows")

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

    logger.info(s"流式实体信息记录完成: $entityType")
  }

  /**
   * 获取静态DataFrame集合
   */
  def getStaticDataFrames(): Map[String, DataFrame] = staticDataFrames.toMap

  /**
   * 获取静态边DataFrame集合
   */
  def getStaticEdgeFrames(): Map[(String, String, String), DataFrame] = staticEdgeFrames.toMap

  /**
   * 获取流式实体信息集合
   */
  def getStreamingEntityInfo(): Map[String, StreamingEntityInfo] = streamingEntityInfo.toMap

  /**
   * 获取所有静态Schema
   */
  def getStaticSchemas(): Map[String, StructType] = staticSchemas.toMap

  /**
   * 获取所有流式Schema
   */
  def getStreamingSchemas(): Map[String, StructType] = streamingSchemas.toMap

  /**
   * 获取收集统计信息
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
   * 生成数据概览报告
   */
  def generateDataOverview(): DataOverviewReport = {
    val statistics = getCollectionStatistics()
    val idUsageStats = idManager.generateUsageStatistics()

    // 分析Schema兼容性
    val schemaCompatibility = analyzeSchemaCompatibility()

    DataOverviewReport(
      graphName = graphName,
      outputPath = outputPath,
      collectionStats = statistics,
      idUsageStats = idUsageStats,
      schemaCompatibility = schemaCompatibility,
      recommendations = generateRecommendations(statistics)
    )
  }

  /**
   * 分析Schema兼容性
   */
  private def analyzeSchemaCompatibility(): SchemaCompatibilityReport = {
    val conflicts = mutable.ListBuffer[SchemaConflict]()
    val compatibleEntities = mutable.ListBuffer[String]()

    // 检查静态和流式数据的Schema冲突
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
   * 检测两个Schema之间的冲突
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
        case _ => // 兼容
      }
    }

    conflicts.toList
  }

  /**
   * 生成处理建议
   */
  private def generateRecommendations(stats: DataCollectionStatistics): List[String] = {
    val recommendations = mutable.ListBuffer[String]()

    // 基于数据规模的建议
    if (stats.totalVertices > 10000000) {
      recommendations += "数据规模较大(>1000万顶点)，建议使用混合格式输出以优化性能"
    } else if (stats.totalVertices < 1000000) {
      recommendations += "数据规模适中(<100万顶点)，建议使用完全标准化输出确保兼容性"
    }

    // 基于流式数据比例的建议
    if (stats.streamingRatio > 0.8) {
      recommendations += s"流式数据占比较高(${(stats.streamingRatio * 100).toInt}%)，建议保持chunk格式以维持性能优势"
    } else if (stats.streamingRatio < 0.2) {
      recommendations += s"静态数据占主导(${((1 - stats.streamingRatio) * 100).toInt}%)，建议使用标准GraphWriter处理"
    }

    // 基于实体类型数量的建议
    if (stats.allEntityTypes.length > 15) {
      recommendations += "实体类型较多，建议启用智能Schema合并以处理复杂度"
    }

    recommendations.toList
  }

  /**
   * 判断是否为顶点实体
   */
  def isVertexEntity(entityType: String): Boolean = {
    val vertexEntities = Set("Person", "Organisation", "Place", "Tag", "TagClass", "Forum", "Post", "Comment", "Photo")
    vertexEntities.contains(entityType)
  }

  /**
   * 获取总实体数量
   */
  def getTotalEntityCount(): Int = {
    staticDataFrames.size + staticEdgeFrames.size + streamingEntityInfo.size
  }

  /**
   * 清理收集的数据（用于内存管理）
   */
  def cleanup(): Unit = {
    logger.info("清理GraphArDataCollector缓存数据")

    staticDataFrames.clear()
    staticEdgeFrames.clear()
    staticSchemas.clear()
    streamingEntityInfo.clear()
    streamingSchemas.clear()

    staticVertexCount = 0L
    staticEdgeCount = 0L
    streamingVertexCount = 0L
    streamingEdgeCount = 0L

    logger.info("GraphArDataCollector清理完成")
  }
}

/**
 * 流式实体信息
 */
case class StreamingEntityInfo(
  entityType: String,
  chunkCount: Long,
  totalRows: Long,
  schema: StructType,
  offsetMapping: Map[String, Map[Long, OffsetInfo]]
)

/**
 * 数据收集统计信息
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
 * Schema冲突信息
 */
case class SchemaConflict(
  entityType: String,
  fieldName: String,
  staticType: String,
  streamingType: String,
  conflictType: String
) {
  def description: String = s"实体 $entityType 字段 $fieldName: $conflictType (static=$staticType, streaming=$streamingType)"
}

/**
 * Schema兼容性报告
 */
case class SchemaCompatibilityReport(
  totalEntitiesChecked: Int,
  conflictCount: Int,
  compatibleEntities: List[String],
  conflicts: List[SchemaConflict],
  isFullyCompatible: Boolean
)

/**
 * 数据概览报告
 */
case class DataOverviewReport(
  graphName: String,
  outputPath: String,
  collectionStats: DataCollectionStatistics,
  idUsageStats: org.apache.graphar.datasources.ldbc.stream.processor.IdSpaceUsageStatistics,
  schemaCompatibility: SchemaCompatibilityReport,
  recommendations: List[String]
)