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

package org.apache.graphar.datasources.ldbc.bridge

import ldbc.snb.datagen.util.GeneratorConfiguration
import org.apache.graphar.datasources.ldbc.model.{ConversionResult, StreamingConversionResult, ValidationResult}
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
 * LDBC桥接器统一接口
 *
 * 遵循GraphAr生态系统的设计模式，提供统一的API接口：
 * 1. 标准化的write方法签名（与GraphWriter一致）
 * 2. 统一的配置参数验证
 * 3. 一致的错误处理和返回类型
 * 4. 专注于流式处理模式
 */
trait LdbcBridgeInterface {

  /**
   * 统一的写入方法（遵循GraphAr GraphWriter模式）
   *
   * @param path 输出路径
   * @param spark SparkSession实例
   * @param name 图名称
   * @param vertex_chunk_size 顶点分块大小
   * @param edge_chunk_size 边分块大小
   * @param file_type 文件类型 (csv|parquet|orc)
   * @return 转换结果
   */
  def write(
    path: String,
    spark: SparkSession,
    name: String,
    vertex_chunk_size: Long,
    edge_chunk_size: Long,
    file_type: String
  ): Try[ConversionResult]

  /**
   * 验证配置参数（统一的验证逻辑）
   */
  def validateConfiguration(
    mode: String,
    outputPath: String,
    vertexChunkSize: Long,
    edgeChunkSize: Long,
    fileType: String
  ): ValidationResult

  /**
   * 获取支持的处理模式
   */
  def getSupportedModes(): List[String]

  /**
   * 获取桥接器类型标识
   */
  def getBridgeType(): String
}

/**
 * 流式桥接器接口扩展
 *
 * 为流式处理提供额外的配置和控制方法
 */
trait StreamingBridgeInterface extends LdbcBridgeInterface {

  /**
   * 流式处理专用写入方法
   */
  def writeStreaming(
    config: StreamingConfiguration
  )(implicit spark: SparkSession): Try[StreamingConversionResult]
}

/**
 * 流式配置
 */
case class StreamingConfiguration(
  ldbcConfigPath: String,
  outputPath: String,
  scaleFactor: String = "0.003",
  graphName: String = "ldbc_social_network_streaming",
  vertexChunkSize: Long = 1024L,
  edgeChunkSize: Long = 1024L,
  fileType: String = "parquet"
) extends BridgeConfiguration


/**
 * 统一配置基类
 */
trait BridgeConfiguration {
  def outputPath: String
  def graphName: String
  def vertexChunkSize: Long
  def edgeChunkSize: Long
  def fileType: String

  /**
   * 验证配置有效性
   */
  def validate(): ValidationResult = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    if (outputPath.trim.isEmpty) {
      errors += "Output path cannot be empty"
    }

    if (graphName.trim.isEmpty) {
      errors += "Graph name cannot be empty"
    }

    if (vertexChunkSize <= 0) {
      errors += s"Vertex chunk size must be positive, got: $vertexChunkSize"
    }

    if (edgeChunkSize <= 0) {
      errors += s"Edge chunk size must be positive, got: $edgeChunkSize"
    }

    val supportedFileTypes = Set("csv", "parquet", "orc")
    if (!supportedFileTypes.contains(fileType.toLowerCase)) {
      errors += s"Unsupported file type: $fileType. Supported types: ${supportedFileTypes.mkString(", ")}"
    }

    if (errors.isEmpty) {
      ValidationResult.success()
    } else {
      ValidationResult.failure(errors.toList)
    }
  }
}

/**
 * 使配置实现统一验证
 */
object BridgeConfiguration {
  implicit class ConfigurationOps(config: BridgeConfiguration) {
    def isValid: Boolean = config.validate().isSuccess
  }
}

/**
 * 桥接器工厂
 */
object LdbcBridgeFactory {

  /**
   * 创建适当的桥接器实例
   */
  def createBridge(bridgeType: String): LdbcBridgeInterface = {
    bridgeType.toLowerCase match {
      case "streaming" => new LdbcStreamingBridge()
      case _ => throw new IllegalArgumentException(s"Unsupported bridge type: $bridgeType. Supported types: streaming")
    }
  }

  /**
   * 获取所有支持的桥接器类型
   */
  def getSupportedBridgeTypes(): List[String] = {
    List("streaming")
  }
}