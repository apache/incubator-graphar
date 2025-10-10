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

// GraphAr标准组件导入
import org.apache.graphar.graph.GraphWriter

import java.io.{File, FileWriter, BufferedWriter}
import java.net.URI
import scala.collection.mutable

/**
 * 流式GraphAr写入器
 *
 * 负责将处理后的流式数据写入GraphAr格式，实现：
 * 1. 按chunk大小分片写入
 * 2. 生成GraphAr标准元数据文件
 * 3. 支持多种文件格式 (csv/parquet/orc)
 * 4. 维护偏移量信息和索引
 */
class StreamingGraphArWriter(
    private val outputPath: String,
    private val graphName: String,
    private val vertexChunkSize: Long = 1024L,
    private val edgeChunkSize: Long = 1024L,
    private val fileType: String = "parquet"
)(implicit spark: SparkSession) extends Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[StreamingGraphArWriter])

  // chunk计数器：实体类型 -> 属性组 -> chunk计数（恢复流式管理）
  private val chunkCounters = mutable.Map[String, mutable.Map[String, Long]]()

  // 偏移量映射：实体类型 -> 属性组 -> chunk -> 偏移量信息（恢复索引功能）
  private val offsetMappings = mutable.Map[String, mutable.Map[String, mutable.Map[Long, OffsetInfo]]]()

  // 实体统计信息
  private val entityStatistics = mutable.Map[String, EntityWriteStatistics]()

  // 数据收集器：用于GraphWriter元数据生成
  private val collectedDataFrames = mutable.Map[String, DataFrame]()

  // Schema映射：存储各实体的schema信息
  private val schemaMapping = mutable.Map[String, EntitySchemaDetector.EntitySchema]()

  logger.info(s"StreamingGraphArWriter初始化: outputPath=$outputPath, graphName=$graphName，使用GraphWriter模式")

  /**
   * 获取符合GraphAr标准的路径
   * 根据GraphAr规范，顶点数据应在vertex/目录下，边数据应在edge/目录下
   */
  private def getStandardGraphArPath(entityType: LdbcEntityType): String = {
    entityType match {
      case vertexType if entityType.isVertex =>
        s"$outputPath/vertex/${entityType.name}"
      case edgeType if entityType.isEdge =>
        // 对于边，使用 src_edge_dst 的命名格式
        val edgeTypeImpl = entityType.asInstanceOf[EdgeEntityType]
        s"$outputPath/edge/${edgeTypeImpl.srcVertexType.name}_${entityType.name}_${edgeTypeImpl.dstVertexType.name}"
      case _ =>
        // 回退到原有逻辑
        s"$outputPath/${entityType.tableName}"
    }
  }

  /**
   * 写入实体数据到GraphAr格式（恢复流式chunk管理）
   */
  def writeEntity(
      dataFrame: DataFrame,
      schema: EntitySchemaDetector.EntitySchema,
      propertyGroup: String
  ): WriteResult = {

    val entityType = schema.entityType
    val entityName = entityType.name
    val collectKey = s"${entityName}_${propertyGroup}"

    logger.info(s"流式写入实体数据: entity=$entityName, propertyGroup=$propertyGroup, rows=${dataFrame.count()}")

    try {
      // 存储schema信息用于后续元数据生成
      schemaMapping(collectKey) = schema

      // 同时收集DataFrame用于GraphWriter元数据生成
      collectedDataFrames.get(collectKey) match {
        case Some(existingDF) =>
          collectedDataFrames(collectKey) = existingDF.union(dataFrame)
        case None =>
          collectedDataFrames(collectKey) = dataFrame
      }

      // 执行流式chunk写入（恢复原有逻辑）
      // 初始化计数器
      val entityCounters = chunkCounters.getOrElseUpdate(entityName, mutable.Map[String, Long]())
      val currentChunkId = entityCounters.getOrElse(propertyGroup, 0L)

      // 确定chunk大小
      val chunkSize = if (entityType.isVertex) vertexChunkSize else edgeChunkSize

      // 计算需要的chunk数量
      val totalRows = dataFrame.count()
      val chunksNeeded = math.ceil(totalRows.toDouble / chunkSize).toLong

      val writeResults = mutable.ListBuffer[ChunkWriteResult]()

      // 分chunk写入（恢复流式处理）
      for (chunkIndex <- 0L until chunksNeeded) {
        val startRow = chunkIndex * chunkSize
        val endRow = math.min((chunkIndex + 1) * chunkSize, totalRows)

        // 使用limit和drop组合实现分页
        val chunkData = if (startRow == 0) {
          dataFrame.limit((endRow - startRow).toInt)
        } else {
          // 使用row_number() window function来实现分页
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

      // 更新计数器
      entityCounters(propertyGroup) = currentChunkId + chunksNeeded

      // 更新统计信息
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
        logger.error(s"流式写入实体数据失败: entity=$entityName, propertyGroup=$propertyGroup", e)
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
   * 生成GraphAr元数据文件（使用标准GraphWriter）
   */
  def generateGraphMetadata(schemas: List[EntitySchemaDetector.EntitySchema]): Unit = {
    logger.info("使用标准GraphWriter生成GraphAr格式数据和元数据文件")

    try {
      val writer = new GraphWriter()

      // 分离顶点和边的DataFrame
      val vertexDataFrames = mutable.Map[String, DataFrame]()
      val edgeDataFrames = mutable.Map[(String, String, String), DataFrame]()

      // 处理收集的DataFrame
      collectedDataFrames.foreach { case (collectKey, dataFrame) =>
        schemaMapping.get(collectKey) match {
          case Some(schema) =>
            val entityType = schema.entityType

            if (entityType.isVertex) {
              // 顶点数据
              val vertexType = entityType.name
              vertexDataFrames.get(vertexType) match {
                case Some(existingDF) =>
                  vertexDataFrames(vertexType) = existingDF.union(dataFrame)
                case None =>
                  vertexDataFrames(vertexType) = dataFrame
              }
              logger.info(s"准备写入顶点数据: $vertexType，记录数: ${dataFrame.count()}")

            } else if (entityType.isEdge) {
              // 边数据 - 需要解析关系
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
              logger.info(s"准备写入边数据: ${relation._1}->${relation._2}->${relation._3}，记录数: ${dataFrame.count()}")
            }

          case None =>
            logger.warn(s"找不到schema信息: $collectKey")
        }
      }

      // 将数据添加到GraphWriter
      vertexDataFrames.foreach { case (vertexType, dataFrame) =>
        writer.PutVertexData(vertexType, dataFrame)
        logger.info(s"添加顶点数据到GraphWriter: $vertexType")
      }

      edgeDataFrames.foreach { case (relation, dataFrame) =>
        writer.PutEdgeData(relation, dataFrame)
        logger.info(s"添加边数据到GraphWriter: ${relation._1}->${relation._2}->${relation._3}")
      }

      // 一键写入，自动生成YAML元数据文件
      writer.write(
        outputPath,
        spark,
        graphName,
        vertexChunkSize,
        edgeChunkSize,
        fileType
      )

      logger.info(s"GraphWriter写入完成: outputPath=$outputPath, graphName=$graphName")

    } catch {
      case e: Exception =>
        logger.error("使用GraphWriter生成数据和元数据文件失败", e)
        throw e
    }
  }

  /**
   * 写入单个chunk（恢复流式处理核心方法）
   */
  private def writeChunk(
      chunkData: DataFrame,
      entityType: LdbcEntityType,
      propertyGroup: String,
      chunkId: Long,
      startOffset: Long
  ): ChunkWriteResult = {

    val entityName = entityType.name

    // 构建符合GraphAr标准的输出路径
    val standardPath = getStandardGraphArPath(entityType)
    val propertyGroupDir = s"$standardPath/$propertyGroup"
    val chunkPath = s"$propertyGroupDir/chunk$chunkId"

    // 确保目录存在
    new File(propertyGroupDir).mkdirs()

    try {
      // 写入数据文件
      fileType.toLowerCase match {
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
          logger.warn(s"不支持的文件类型: $fileType，使用parquet")
          chunkData.write.mode("overwrite").parquet(chunkPath)
      }

      val rowCount = chunkData.count()

      // 记录偏移量信息
      recordOffsetInfo(entityName, propertyGroup, chunkId, startOffset, rowCount)

      logger.debug(s"成功写入chunk: $chunkPath，行数: $rowCount")

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
        logger.error(s"写入chunk失败: $chunkPath", e)
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
   * 记录偏移量信息（恢复索引功能）
   */
  private def recordOffsetInfo(
      entityName: String,
      propertyGroup: String,
      chunkId: Long,
      startOffset: Long,
      rowCount: Long
  ): Unit = {

    val entityOffsets = offsetMappings.getOrElseUpdate(entityName, mutable.Map[String, mutable.Map[Long, OffsetInfo]]())
    val groupOffsets = entityOffsets.getOrElseUpdate(propertyGroup, mutable.Map[Long, OffsetInfo]())

    groupOffsets(chunkId) = OffsetInfo(
      chunkId = chunkId,
      startOffset = startOffset,
      endOffset = startOffset + rowCount - 1,
      rowCount = rowCount
    )
  }

  /**
   * 更新实体统计信息
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
   * 生成偏移量索引文件（恢复索引功能）
   */
  def generateOffsetIndex(): Unit = {
    logger.info("生成偏移量索引文件")

    offsetMappings.foreach { case (entityName, entityOffsets) =>
      entityOffsets.foreach { case (propertyGroup, groupOffsets) =>
        val indexPath = s"$outputPath/$entityName/$propertyGroup/offset.idx"
        val writer = new BufferedWriter(new FileWriter(indexPath))

        try {
          writer.write("# GraphAr Offset Index\\n")
          writer.write("# Format: chunk_id,start_offset,end_offset,row_count\\n")

          groupOffsets.toSeq.sortBy(_._1).foreach { case (chunkId, offsetInfo) =>
            writer.write(s"${offsetInfo.chunkId},${offsetInfo.startOffset},${offsetInfo.endOffset},${offsetInfo.rowCount}\\n")
          }

          logger.debug(s"偏移量索引文件生成: $indexPath")

        } finally {
          writer.close()
        }
      }
    }
  }

  /**
   * 获取偏移量信息
   */
  def getOffsetMappings(): Map[String, Map[String, Map[Long, OffsetInfo]]] = {
    offsetMappings.mapValues(_.mapValues(_.toMap).toMap).toMap
  }










  /**
   * 获取写入统计信息
   */
  def getWriteStatistics(): Map[String, EntityWriteStatistics] = {
    entityStatistics.toMap
  }
}

// 恢复所有必要的case classes

/**
 * 偏移量信息
 */
case class OffsetInfo(
  chunkId: Long,
  startOffset: Long,
  endOffset: Long,
  rowCount: Long
)

/**
 * chunk写入结果
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
 * 写入结果
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
 * 属性组统计信息
 */
case class PropertyGroupStats(
  propertyGroup: String,
  var rowCount: Long,
  var chunkCount: Long
)

/**
 * 实体写入统计信息
 */
case class EntityWriteStatistics(
  entityName: String,
  var totalRows: Long,
  var totalChunks: Long,
  propertyGroups: mutable.Map[String, PropertyGroupStats]
)

