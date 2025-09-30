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

package org.apache.graphar.datasources.ldbc.stream.strategy

import org.apache.graphar.datasources.ldbc.stream.core.{GraphArDataCollector, StreamingEntityInfo, DataCollectionStatistics}
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * 智能输出策略选择器
 *
 * 基于多维度评估自动选择最优的输出处理策略：
 * 1. 完全标准化：所有数据转换为标准GraphAr格式
 * 2. 混合文档化：保持混合格式但生成标准YAML描述
 */
class EnhancedOutputStrategySelector {

  private val logger: Logger = LoggerFactory.getLogger(classOf[EnhancedOutputStrategySelector])

  /**
   * 选择最优策略
   */
  def selectOptimalStrategy(
    staticData: Map[String, DataFrame],
    streamingData: Map[String, StreamingEntityInfo],
    systemResources: SystemResourceInfo
  ): StrategyDecision = {

    logger.info("开始智能策略选择分析")

    // 1. 数据规模分析
    val dataMetrics = analyzeDataMetrics(staticData, streamingData)
    logger.info(s"数据规模分析完成: ${dataMetrics}")

    // 2. 系统资源评估
    val resourceMetrics = analyzeResourceMetrics(systemResources)
    logger.info(s"资源评估完成: ${resourceMetrics}")

    // 3. 处理复杂度评估
    val complexityMetrics = analyzeProcessingComplexity(dataMetrics)
    logger.info(s"复杂度评估完成: ${complexityMetrics}")

    // 4. 综合决策
    val decision = makeStrategyDecision(dataMetrics, resourceMetrics, complexityMetrics)

    logger.info(s"策略选择完成: ${decision.strategy}, 置信度: ${(decision.confidence * 100).toInt}%")
    logger.info(s"决策理由: ${decision.reasoning.mkString("; ")}")

    decision
  }

  /**
   * 基于数据收集器选择策略
   */
  def selectStrategyFromCollector(
    dataCollector: GraphArDataCollector,
    systemResources: SystemResourceInfo
  ): StrategyDecision = {
    selectOptimalStrategy(
      dataCollector.getStaticDataFrames(),
      dataCollector.getStreamingEntityInfo(),
      systemResources
    )
  }

  /**
   * 分析数据规模指标
   */
  private def analyzeDataMetrics(
    staticData: Map[String, DataFrame],
    streamingData: Map[String, StreamingEntityInfo]
  ): DataMetrics = {

    val staticRowCount = staticData.values.map(_.count()).sum
    val streamingRowCount = streamingData.values.map(_.totalRows).sum
    val totalRowCount = staticRowCount + streamingRowCount

    val staticChunkCount = staticData.size.toLong
    val streamingChunkCount = streamingData.values.map(_.chunkCount).sum

    val streamingRatio = if (totalRowCount > 0) streamingRowCount.toDouble / totalRowCount else 0.0
    val chunkComplexity = if (staticChunkCount + streamingChunkCount > 0) {
      streamingChunkCount.toDouble / (staticChunkCount + streamingChunkCount)
    } else 0.0

    // 计算数据密度（平均每个chunk的记录数）
    val avgChunkSize = if (streamingChunkCount > 0) streamingRowCount.toDouble / streamingChunkCount else 0.0

    DataMetrics(
      totalRows = totalRowCount,
      staticRows = staticRowCount,
      streamingRows = streamingRowCount,
      streamingRatio = streamingRatio,
      chunkComplexity = chunkComplexity,
      entityCount = staticData.size + streamingData.size,
      avgChunkSize = avgChunkSize,
      staticEntityCount = staticData.size,
      streamingEntityCount = streamingData.size
    )
  }

  /**
   * 分析系统资源指标
   */
  private def analyzeResourceMetrics(systemResources: SystemResourceInfo): ResourceMetrics = {
    val runtime = Runtime.getRuntime
    val maxMemory = runtime.maxMemory()
    val freeMemory = runtime.freeMemory()
    val totalMemory = runtime.totalMemory()
    val usedMemory = totalMemory - freeMemory

    val memoryPressure = usedMemory.toDouble / maxMemory
    val availableMemoryGB = (maxMemory - usedMemory) / (1024.0 * 1024.0 * 1024.0)

    ResourceMetrics(
      availableMemoryGB = availableMemoryGB,
      memoryPressure = memoryPressure,
      maxMemoryGB = maxMemory / (1024.0 * 1024.0 * 1024.0),
      cpuCores = runtime.availableProcessors(),
      diskIOCapacity = systemResources.diskIOCapacity
    )
  }

  /**
   * 分析处理复杂度指标
   */
  private def analyzeProcessingComplexity(dataMetrics: DataMetrics): ComplexityMetrics = {
    // 转换复杂度评估
    val conversionComplexity = if (dataMetrics.streamingRatio > 0.7) {
      ComplexityLevel.HIGH
    } else if (dataMetrics.streamingRatio > 0.3) {
      ComplexityLevel.MEDIUM
    } else {
      ComplexityLevel.LOW
    }

    // Schema合并复杂度
    val schemaMergeComplexity = if (dataMetrics.entityCount > 15) {
      ComplexityLevel.HIGH
    } else if (dataMetrics.entityCount > 8) {
      ComplexityLevel.MEDIUM
    } else {
      ComplexityLevel.LOW
    }

    // I/O复杂度
    val ioComplexity = if (dataMetrics.chunkComplexity > 0.6 || dataMetrics.avgChunkSize < 1000) {
      ComplexityLevel.HIGH
    } else if (dataMetrics.chunkComplexity > 0.3) {
      ComplexityLevel.MEDIUM
    } else {
      ComplexityLevel.LOW
    }

    // 数据规模复杂度
    val scaleComplexity = if (dataMetrics.totalRows > 50000000) { // 5000万行
      ComplexityLevel.HIGH
    } else if (dataMetrics.totalRows > 5000000) { // 500万行
      ComplexityLevel.MEDIUM
    } else {
      ComplexityLevel.LOW
    }

    ComplexityMetrics(
      conversionComplexity = conversionComplexity,
      schemaMergeComplexity = schemaMergeComplexity,
      ioComplexity = ioComplexity,
      scaleComplexity = scaleComplexity
    )
  }

  /**
   * 综合决策算法
   */
  private def makeStrategyDecision(
    dataMetrics: DataMetrics,
    resourceMetrics: ResourceMetrics,
    complexityMetrics: ComplexityMetrics
  ): StrategyDecision = {

    var score = 0.0
    val reasoning = mutable.ListBuffer[String]()
    val weights = mutable.Map[String, Double]()

    // 数据规模权重 (40%)
    val dataScaleWeight = 0.4
    if (dataMetrics.streamingRows > 10000000) { // 1000万行以上
      val dataScore = 0.6
      score += dataScore * dataScaleWeight
      weights("large_streaming_data") = dataScore
      reasoning += s"大规模流式数据(${dataMetrics.streamingRows / 1000000}M行)，倾向混合格式"
    } else if (dataMetrics.streamingRows < 1000000) { // 100万行以下
      val dataScore = -0.5
      score += dataScore * dataScaleWeight
      weights("small_data") = dataScore
      reasoning += s"小规模数据(${dataMetrics.streamingRows / 1000}K行)，倾向完全标准化"
    }

    // 内存压力权重 (25%)
    val memoryWeight = 0.25
    if (resourceMetrics.memoryPressure > 0.75) {
      val memoryScore = 0.8
      score += memoryScore * memoryWeight
      weights("high_memory_pressure") = memoryScore
      reasoning += s"内存压力高(${(resourceMetrics.memoryPressure * 100).toInt}%)，避免大规模转换"
    } else if (resourceMetrics.memoryPressure < 0.3) {
      val memoryScore = -0.4
      score += memoryScore * memoryWeight
      weights("low_memory_pressure") = memoryScore
      reasoning += s"内存充足(${(resourceMetrics.availableMemoryGB * 10).toInt / 10.0}GB可用)，支持完全转换"
    }

    // 处理复杂度权重 (20%)
    val complexityWeight = 0.2
    val highComplexityCount = List(
      complexityMetrics.conversionComplexity,
      complexityMetrics.schemaMergeComplexity,
      complexityMetrics.ioComplexity,
      complexityMetrics.scaleComplexity
    ).count(_ == ComplexityLevel.HIGH)

    if (highComplexityCount >= 2) {
      val complexityScore = 0.6
      score += complexityScore * complexityWeight
      weights("high_complexity") = complexityScore
      reasoning += s"处理复杂度高($highComplexityCount/4个高复杂度项)，倾向保持原格式"
    }

    // 流式数据比例权重 (10%)
    val ratioWeight = 0.1
    if (dataMetrics.streamingRatio > 0.8) {
      val ratioScore = 0.3
      score += ratioScore * ratioWeight
      weights("high_streaming_ratio") = ratioScore
      reasoning += s"流式数据占主导(${(dataMetrics.streamingRatio * 100).toInt}%)，保持chunk优势"
    } else if (dataMetrics.streamingRatio < 0.2) {
      val ratioScore = -0.3
      score += ratioScore * ratioWeight
      weights("low_streaming_ratio") = ratioScore
      reasoning += s"静态数据占主导(${((1 - dataMetrics.streamingRatio) * 100).toInt}%)，适合标准化"
    }

    // Chunk质量权重 (5%)
    val chunkQualityWeight = 0.05
    if (dataMetrics.avgChunkSize < 500) {
      val chunkScore = 0.2
      score += chunkScore * chunkQualityWeight
      weights("small_chunks") = chunkScore
      reasoning += s"Chunk较小(平均${dataMetrics.avgChunkSize.toInt}行)，转换开销大"
    }

    // 决策阈值
    val strategy = if (score > 0.15) {
      OutputStrategy.HYBRID_DOCUMENTED
    } else {
      OutputStrategy.COMPLETE_STANDARD
    }

    // 计算置信度
    val confidence = (Math.abs(score) * 2).min(1.0).max(0.1)

    // 生成决策理由总结
    if (reasoning.isEmpty) {
      reasoning += "数据规模和系统资源均衡，采用默认策略"
    }

    StrategyDecision(
      strategy = strategy,
      confidence = confidence,
      score = score,
      reasoning = reasoning.toList,
      weights = weights.toMap,
      metrics = DecisionMetrics(dataMetrics, resourceMetrics, complexityMetrics)
    )
  }
}

/**
 * 输出策略枚举
 */
object OutputStrategy extends Enumeration {
  type OutputStrategy = Value
  val COMPLETE_STANDARD, HYBRID_DOCUMENTED = Value
}

/**
 * 复杂度等级
 */
object ComplexityLevel extends Enumeration {
  type ComplexityLevel = Value
  val LOW, MEDIUM, HIGH = Value
}

/**
 * 数据规模指标
 */
case class DataMetrics(
  totalRows: Long,
  staticRows: Long,
  streamingRows: Long,
  streamingRatio: Double,
  chunkComplexity: Double,
  entityCount: Int,
  avgChunkSize: Double,
  staticEntityCount: Int,
  streamingEntityCount: Int
) {
  override def toString: String = {
    s"DataMetrics(total=${totalRows / 1000}K, streaming=${(streamingRatio * 100).toInt}%, " +
      s"entities=$entityCount, avgChunk=${avgChunkSize.toInt})"
  }
}

/**
 * 资源指标
 */
case class ResourceMetrics(
  availableMemoryGB: Double,
  memoryPressure: Double,
  maxMemoryGB: Double,
  cpuCores: Int,
  diskIOCapacity: String
) {
  override def toString: String = {
    s"ResourceMetrics(mem=${(availableMemoryGB * 10).toInt / 10.0}GB available, " +
      s"pressure=${(memoryPressure * 100).toInt}%, cores=$cpuCores)"
  }
}

/**
 * 复杂度指标
 */
case class ComplexityMetrics(
  conversionComplexity: ComplexityLevel.Value,
  schemaMergeComplexity: ComplexityLevel.Value,
  ioComplexity: ComplexityLevel.Value,
  scaleComplexity: ComplexityLevel.Value
) {
  override def toString: String = {
    s"ComplexityMetrics(conversion=$conversionComplexity, schema=$schemaMergeComplexity, " +
      s"io=$ioComplexity, scale=$scaleComplexity)"
  }
}

/**
 * 策略决策结果
 */
case class StrategyDecision(
  strategy: OutputStrategy.Value,
  confidence: Double,
  score: Double,
  reasoning: List[String],
  weights: Map[String, Double],
  metrics: DecisionMetrics
) {
  def isHighConfidence: Boolean = confidence > 0.7
  def isLowConfidence: Boolean = confidence < 0.4

  override def toString: String = {
    s"StrategyDecision($strategy, confidence=${(confidence * 100).toInt}%, score=${(score * 100).toInt})"
  }
}

/**
 * 决策指标汇总
 */
case class DecisionMetrics(
  dataMetrics: DataMetrics,
  resourceMetrics: ResourceMetrics,
  complexityMetrics: ComplexityMetrics
)

/**
 * 系统资源信息
 */
case class SystemResourceInfo(
  diskIOCapacity: String = "standard"
)

/**
 * 系统资源信息伴生对象
 */
object SystemResourceInfo {
  def current(): SystemResourceInfo = {
    SystemResourceInfo(
      diskIOCapacity = "standard" // 可以扩展为实际的IO性能检测
    )
  }
}