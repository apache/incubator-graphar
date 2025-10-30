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

import org.apache.graphar.datasources.ldbc.stream.core.{
  GraphArDataCollector,
  StreamingEntityInfo,
  DataCollectionStatistics
}
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * Intelligent output strategy selector
 *
 * Based on multi-dimensional assessment, automatically selects optimal output
 * processing strategy:
 *   1. Complete standardization: All data converted to standard GraphAr format
 *      2. Hybrid documentation: Maintain mixed format but generate standard
 *      YAML descriptions
 */
class EnhancedOutputStrategySelector {

  private val logger: Logger =
    LoggerFactory.getLogger(classOf[EnhancedOutputStrategySelector])

  /**
   * Select optimal strategy
   */
  def selectOptimalStrategy(
      staticData: Map[String, DataFrame],
      streamingData: Map[String, StreamingEntityInfo],
      systemResources: SystemResourceInfo
  ): StrategyDecision = {

    logger.info("Starting intelligent strategy selection analysis")

    // 1. Analyze data scale
    val dataMetrics = analyzeDataMetrics(staticData, streamingData)
    logger.info(s"Data metrics analysis completed: ${dataMetrics}")

    // 2. Assess system resources
    val resourceMetrics = analyzeResourceMetrics(systemResources)
    logger.info(s"Resource assessment completed: ${resourceMetrics}")

    // 3. Assess processing complexity
    val complexityMetrics = analyzeProcessingComplexity(dataMetrics)
    logger.info(s"Complexity assessment completed: ${complexityMetrics}")

    // 4. Make comprehensive decision
    val decision =
      makeStrategyDecision(dataMetrics, resourceMetrics, complexityMetrics)

    logger.info(
      s"Strategy selection completed: ${decision.strategy}, confidence: ${(decision.confidence * 100).toInt}%"
    )
    logger.info(s"Decision reasoning: ${decision.reasoning.mkString("; ")}")

    decision
  }

  /**
   * Select strategy based on data collector
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
   * Analyze data scale metrics
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

    val streamingRatio =
      if (totalRowCount > 0) streamingRowCount.toDouble / totalRowCount else 0.0
    val chunkComplexity = if (staticChunkCount + streamingChunkCount > 0) {
      streamingChunkCount.toDouble / (staticChunkCount + streamingChunkCount)
    } else 0.0

    // Calculate data density (average records per chunk)
    val avgChunkSize =
      if (streamingChunkCount > 0)
        streamingRowCount.toDouble / streamingChunkCount
      else 0.0

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
   * Analyze system resource metrics
   */
  private def analyzeResourceMetrics(
      systemResources: SystemResourceInfo
  ): ResourceMetrics = {
    val runtime = Runtime.getRuntime
    val maxMemory = runtime.maxMemory()
    val freeMemory = runtime.freeMemory()
    val totalMemory = runtime.totalMemory()
    val usedMemory = totalMemory - freeMemory

    val memoryPressure = usedMemory.toDouble / maxMemory
    val availableMemoryGB =
      (maxMemory - usedMemory) / (1024.0 * 1024.0 * 1024.0)

    ResourceMetrics(
      availableMemoryGB = availableMemoryGB,
      memoryPressure = memoryPressure,
      maxMemoryGB = maxMemory / (1024.0 * 1024.0 * 1024.0),
      cpuCores = runtime.availableProcessors(),
      diskIOCapacity = systemResources.diskIOCapacity
    )
  }

  /**
   * Analyze processing complexity metrics
   */
  private def analyzeProcessingComplexity(
      dataMetrics: DataMetrics
  ): ComplexityMetrics = {
    // Assess conversion complexity
    val conversionComplexity = if (dataMetrics.streamingRatio > 0.7) {
      ComplexityLevel.HIGH
    } else if (dataMetrics.streamingRatio > 0.3) {
      ComplexityLevel.MEDIUM
    } else {
      ComplexityLevel.LOW
    }

    // Schema merge complexity
    val schemaMergeComplexity = if (dataMetrics.entityCount > 15) {
      ComplexityLevel.HIGH
    } else if (dataMetrics.entityCount > 8) {
      ComplexityLevel.MEDIUM
    } else {
      ComplexityLevel.LOW
    }

    // I/O complexity
    val ioComplexity =
      if (
        dataMetrics.chunkComplexity > 0.6 || dataMetrics.avgChunkSize < 1000
      ) {
        ComplexityLevel.HIGH
      } else if (dataMetrics.chunkComplexity > 0.3) {
        ComplexityLevel.MEDIUM
      } else {
        ComplexityLevel.LOW
      }

    // Data scale complexity
    val scaleComplexity =
      if (dataMetrics.totalRows > 50000000) { // 50 million rows
        ComplexityLevel.HIGH
      } else if (dataMetrics.totalRows > 5000000) { // 5 million rows
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
   * Comprehensive decision algorithm
   */
  private def makeStrategyDecision(
      dataMetrics: DataMetrics,
      resourceMetrics: ResourceMetrics,
      complexityMetrics: ComplexityMetrics
  ): StrategyDecision = {

    var score = 0.0
    val reasoning = mutable.ListBuffer[String]()
    val weights = mutable.Map[String, Double]()

    // Data scale weight (40%)
    val dataScaleWeight = 0.4
    if (dataMetrics.streamingRows > 10000000) { // 10 million rows and above
      val dataScore = 0.6
      score += dataScore * dataScaleWeight
      weights("large_streaming_data") = dataScore
      reasoning += s"Large-scale streaming data (${dataMetrics.streamingRows / 1000000}M rows), favoring hybrid format"
    } else if (dataMetrics.streamingRows < 1000000) { // 1 million rows and below
      val dataScore = -0.5
      score += dataScore * dataScaleWeight
      weights("small_data") = dataScore
      reasoning += s"Small-scale data (${dataMetrics.streamingRows / 1000}K rows), favoring complete standardization"
    }

    // Memory pressure weight (25%)
    val memoryWeight = 0.25
    if (resourceMetrics.memoryPressure > 0.75) {
      val memoryScore = 0.8
      score += memoryScore * memoryWeight
      weights("high_memory_pressure") = memoryScore
      reasoning += s"High memory pressure (${(resourceMetrics.memoryPressure * 100).toInt}%), avoiding large-scale conversion"
    } else if (resourceMetrics.memoryPressure < 0.3) {
      val memoryScore = -0.4
      score += memoryScore * memoryWeight
      weights("low_memory_pressure") = memoryScore
      reasoning += s"Sufficient memory (${(resourceMetrics.availableMemoryGB * 10).toInt / 10.0}GB available), supporting complete conversion"
    }

    // Processing complexity weight (20%)
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
      reasoning += s"High processing complexity ($highComplexityCount/4 high complexity items), favoring original format preservation"
    }

    // Streaming data ratio weight (10%)
    val ratioWeight = 0.1
    if (dataMetrics.streamingRatio > 0.8) {
      val ratioScore = 0.3
      score += ratioScore * ratioWeight
      weights("high_streaming_ratio") = ratioScore
      reasoning += s"Streaming data dominant (${(dataMetrics.streamingRatio * 100).toInt}%), maintaining chunk advantages"
    } else if (dataMetrics.streamingRatio < 0.2) {
      val ratioScore = -0.3
      score += ratioScore * ratioWeight
      weights("low_streaming_ratio") = ratioScore
      reasoning += s"Static data dominant (${((1 - dataMetrics.streamingRatio) * 100).toInt}%), suitable for standardization"
    }

    // Chunk quality weight (5%)
    val chunkQualityWeight = 0.05
    if (dataMetrics.avgChunkSize < 500) {
      val chunkScore = 0.2
      score += chunkScore * chunkQualityWeight
      weights("small_chunks") = chunkScore
      reasoning += s"Small chunks (average ${dataMetrics.avgChunkSize.toInt} rows), high conversion overhead"
    }

    // Decision threshold
    val strategy = if (score > 0.15) {
      OutputStrategy.HYBRID_DOCUMENTED
    } else {
      OutputStrategy.COMPLETE_STANDARD
    }

    // Calculate confidence
    val confidence = (Math.abs(score) * 2).min(1.0).max(0.1)

    // Generate decision reasoning summary
    if (reasoning.isEmpty) {
      reasoning += "Data scale and system resources balanced, using default strategy"
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
 * Output strategy enumeration
 */
object OutputStrategy extends Enumeration {
  type OutputStrategy = Value
  val COMPLETE_STANDARD, HYBRID_DOCUMENTED = Value
}

/**
 * Complexity level
 */
object ComplexityLevel extends Enumeration {
  type ComplexityLevel = Value
  val LOW, MEDIUM, HIGH = Value
}

/**
 * Data scale metrics
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
 * Resource metrics
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
 * Complexity metrics
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
 * Strategy decision result
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
 * Decision metrics summary
 */
case class DecisionMetrics(
    dataMetrics: DataMetrics,
    resourceMetrics: ResourceMetrics,
    complexityMetrics: ComplexityMetrics
)

/**
 * System resource information
 */
case class SystemResourceInfo(
    diskIOCapacity: String = "standard"
)

/**
 * System resource information companion object
 */
object SystemResourceInfo {
  def current(): SystemResourceInfo = {
    SystemResourceInfo(
      diskIOCapacity =
        "standard" // Can be extended with actual IO performance detection
    )
  }
}
