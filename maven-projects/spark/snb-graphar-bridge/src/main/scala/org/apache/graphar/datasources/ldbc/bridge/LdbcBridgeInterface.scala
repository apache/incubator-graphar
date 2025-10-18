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
 * Unified interface for LDBC bridge
 *
 * Follows GraphAr ecosystem design patterns, providing a unified API interface:
 * 1. Standardized write method signature (consistent with GraphWriter)
 * 2. Unified configuration parameter validation
 * 3. Consistent error handling and return types
 * 4. Focus on streaming processing mode
 */
trait LdbcBridgeInterface {

  /**
   * Unified write method (following GraphAr GraphWriter pattern)
   *
   * @param path Output path
   * @param spark SparkSession instance
   * @param name Graph name
   * @param vertex_chunk_size Vertex chunk size
   * @param edge_chunk_size Edge chunk size
   * @param file_type File type (csv|parquet|orc)
   * @return Conversion result
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
   * Validate configuration parameters (unified validation logic)
   *
   * @param mode Processing mode
   * @param outputPath Output path for GraphAr files
   * @param vertexChunkSize Chunk size for vertex data
   * @param edgeChunkSize Chunk size for edge data
   * @param fileType File format (csv, parquet, or orc)
   * @return ValidationResult indicating whether configuration is valid
   */
  def validateConfiguration(
    mode: String,
    output_path: String,
    vertex_chunk_size: Long,
    edge_chunk_size: Long,
    file_type: String
  ): ValidationResult

  /**
   * Get supported processing modes
   *
   * @return List of supported processing mode names
   */
  def getSupportedModes(): List[String]

  /**
   * Get bridge type identifier
   *
   * @return String identifier for this bridge type (e.g., "streaming")
   */
  def getBridgeType(): String
}

/**
 * Streaming bridge interface extension
 *
 * Provides additional configuration and control methods for streaming processing
 */
trait StreamingBridgeInterface extends LdbcBridgeInterface {

  /**
   * Streaming processing dedicated write method
   *
   * @param config Streaming configuration parameters
   * @param spark Implicit SparkSession for distributed processing
   * @return Try wrapping StreamingConversionResult with conversion statistics
   */
  def writeStreaming(
    config: StreamingConfiguration
  )(implicit spark: SparkSession): Try[StreamingConversionResult]
}

/**
 * Streaming configuration
 */
case class StreamingConfiguration(
  ldbc_config_path: String,
  output_path: String,
  scale_factor: String = "0.003",
  graph_name: String = "ldbc_social_network_streaming",
  vertex_chunk_size: Long = 1024L,
  edge_chunk_size: Long = 1024L,
  file_type: String = "parquet"
) extends BridgeConfiguration


/**
 * Unified configuration base class
 */
trait BridgeConfiguration {
  /** Output path for GraphAr files */
  def output_path: String

  /** Graph name used in metadata */
  def graph_name: String

  /** Vertex chunk size for data partitioning */
  def vertex_chunk_size: Long

  /** Edge chunk size for data partitioning */
  def edge_chunk_size: Long

  /** File format (csv, parquet, or orc) */
  def file_type: String

  /**
   * Validate configuration validity
   */
  def validate(): ValidationResult = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    if (output_path.trim.isEmpty) {
      errors += "Output path cannot be empty"
    }

    if (graph_name.trim.isEmpty) {
      errors += "Graph name cannot be empty"
    }

    if (vertex_chunk_size <= 0) {
      errors += s"Vertex chunk size must be positive, got: $vertex_chunk_size"
    }

    if (edge_chunk_size <= 0) {
      errors += s"Edge chunk size must be positive, got: $edge_chunk_size"
    }

    val supportedFileTypes = Set("csv", "parquet", "orc")
    if (!supportedFileTypes.contains(file_type.toLowerCase)) {
      errors += s"Unsupported file type: $file_type. Supported types: ${supportedFileTypes.mkString(", ")}"
    }

    if (errors.isEmpty) {
      ValidationResult.success()
    } else {
      ValidationResult.failure(errors.toList)
    }
  }
}

/**
 * Enable configuration to implement unified validation
 */
object BridgeConfiguration {
  implicit class ConfigurationOps(config: BridgeConfiguration) {
    def isValid: Boolean = config.validate().isSuccess
  }
}

/**
 * Bridge factory
 */
object LdbcBridgeFactory {

  /**
   * Create appropriate bridge instance
   *
   * @param bridgeType Type of bridge to create (e.g., "streaming")
   * @return Instance of requested bridge type
   * @throws IllegalArgumentException if bridge type is not supported
   */
  def createBridge(bridgeType: String): LdbcBridgeInterface = {
    bridgeType.toLowerCase match {
      case "streaming" => new LdbcStreamingBridge()
      case _ => throw new IllegalArgumentException(s"Unsupported bridge type: $bridgeType. Supported types: streaming")
    }
  }

  /**
   * Get all supported bridge types
   *
   * @return List of bridge type identifiers that can be created
   */
  def getSupportedBridgeTypes(): List[String] = {
    List("streaming")
  }
}