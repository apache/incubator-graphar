/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.graphar.datasources.ldbc.examples

import org.apache.graphar.datasources.ldbc.bridge.{LdbcStreamingBridge, StreamingConfiguration}
import org.apache.graphar.datasources.ldbc.model.ValidationResult
import org.apache.graphar.datasources.ldbc.util.LdbcConfigUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success}

/**
 * LDBCstreaming bridgeexample
 *
 * support completeLDBC SNBdatasetProcess,includingdynamic entity:
 * - Forum, Post, Comment, Likeand other dynamic entities
 * - Person and related static entities
 * - Total coverage: 77.3% (17/22 entity types)
 */
object LdbcStreamingExample {
 private val logger: Logger = LoggerFactory.getLogger(this.getClass)

 def main(args: Array[String]): Unit = {
 val spark = SparkSession
.builder()
.appName("LDBC Streaming Bridge Example")
.master("local[*]")
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
.getOrCreate()

 try {
 logger.info("Starting LDBC Streaming Bridge Example - Supporting Dynamic Entities")

 if (args.length < 3) {
 logger.error("Usage: LdbcStreamingExample <scale_factor> <output_path> <format> [mode]")
 logger.info("scale_factor: LDBC scale factor (e.g., '0.003', '0.1', '1')")
 logger.info("output_path: output directory for GraphAr files")
 logger.info("format: 'csv', 'parquet', 'orc' (default: parquet)")
 logger.info("mode: 'streaming' (default), 'demo', 'benchmark'")
 logger.info("")
 logger.info("Example commands:")
 logger.info(" # Basic streaming processing")
 logger.info(" LdbcStreamingExample 0.003 /tmp/graphar_streaming/ parquet streaming")
 logger.info(" # Demo mode with detailed logging")
 logger.info(" LdbcStreamingExample 0.003 /tmp/graphar_demo/ csv demo")
 System.exit(1)
 }

 val scaleFactor = args(0)
 val outputPath = args(1)
 val fileType = args(2).toLowerCase match {
 case "csv" => "csv"
 case "parquet" => "parquet"
 case "orc" => "orc"
 case _ =>
 logger.warn(s"Unsupported format ${args(2)}, using parquet")
 "parquet"
 }
 val mode = if (args.length > 3) args(3).toLowerCase else "streaming"

 mode match {
 case "demo" =>
 runDemo(scaleFactor, outputPath, fileType)(spark)
 case "benchmark" =>
 runBenchmark(scaleFactor, outputPath, fileType)(spark)
 case _ =>
 runStreaming(scaleFactor, outputPath, fileType)(spark)
 }

 } catch {
 case e: Exception =>
 logger.error("Streaming example execution failed", e)
 System.exit(1)
 } finally {
 spark.stop()
 }
 }

 /**
 * Run basic streaming process
 */
 def runStreaming(
 scaleFactor: String,
 outputPath: String,
 fileType: String
)(implicit spark: SparkSession): Unit = {

 logger.info("=== LDBC Streaming Processing (Complete Dataset) ===")
 logger.info(s"Scale Factor: $scaleFactor")
 logger.info(s"output Path: $outputPath")
 logger.info(s"File Format: $fileType")
 logger.info("Coverage: 77.3% of LDBC entities (including dynamic entities)")

 // Createstreaming bridge
 val streamingBridge = LdbcStreamingBridge.createDefault()

 // Configure streaming process
 val streamingConfig = StreamingConfiguration(
 ldbc_config_path = "ldbc_config.properties", // willUsedefaultConfigure
 output_path = outputPath,
 scale_factor = scaleFactor,
 graph_name = "ldbc_social_network_streaming",
 vertex_chunk_size = 1024L,
 edge_chunk_size = 1024L,
 file_type = fileType
)

 // VerifyConfigure
 val validation = validateStreamingConfiguration(streamingConfig)
 if (!validation.isSuccess) {
 logger.error(s"Configuration validation failed: ${validation.getErrors.mkString(", ")}")
 throw new IllegalArgumentException("Invalid streaming configuration")
 }

 // Executestreaming conversion
 val result = streamingBridge.writeStreaming(streamingConfig)

 result match {
 case Success(streamingResult) =>
 logger.info("✓ Streaming conversion completed successfully!")
 logger.info("=== Streaming Results ===")
 logger.info(s"Processing Duration: ${streamingResult.processingDurationMs}ms")
 logger.info(s"output Path: ${streamingResult.outputPath}")
 logger.info(s"Graph Name: ${streamingResult.graphName}")
 logger.info(s"Success: ${streamingResult.success}")

 if (streamingResult.success) {
 logger.info("✓ Complete LDBC dataset converted to GraphAr format!")
 logger.info("✓ Includes: Forum, Post, Comment, Like + Person entities")
 logger.info("✓ GraphAr files are ready for graph analytics!")
 }

 case Failure(exception) =>
 logger.error("✗ Streaming conversion failed", exception)
 throw exception
 }
 }

 /**
 * Run demo mode (verbose logging)
 */
 def runDemo(
 scaleFactor: String,
 outputPath: String,
 fileType: String
)(implicit spark: SparkSession): Unit = {

 logger.info("=== LDBC Streaming Demo Mode ===")
 logger.info("This mode demonstrates the complete streaming processing pipeline")

 // Show supported entity types
 logger.info("Supported Entity Types:")
 logger.info(" Vertices: Person, Forum, Post, Comment")
 logger.info(" Edges: knows, hasInterest, workAt, studyAt, isLocatedIn")
 logger.info(" hasCreator, containerOf, replyOf, likes")
 logger.info(" Total Coverage: 17/22 LDBC entities (77.3%)")

 // Show configuration information
 val streamingBridge = LdbcStreamingBridge.createDefault()
 val capability = streamingBridge.getCapabilitySummary()

 logger.info(s"Streaming Mode Entities: ${capability.streamingModeEntities.size}")
 logger.info(s"Coverage Percentage: ${capability.coveragePercentage}%")
 logger.info(s"Hybrid Mode Supported: ${capability.hybridModeSupported}")

 // Run actual process
 runStreaming(scaleFactor, outputPath, fileType)

 logger.info("Demo completed - Check output directory for GraphAr files")
 }

 /**
 * Run performance benchmark test
 */
 def runBenchmark(
 scaleFactor: String,
 outputPath: String,
 fileType: String,
 iterations: Int = 3
)(implicit spark: SparkSession): Unit = {

 logger.info(s"=== LDBC Streaming Benchmark (${iterations} iterations) ===")
 logger.info("Measuring streaming processing performance for complete LDBC dataset")

 val times = (1 to iterations).map { i =>
 logger.info(s"Benchmark iteration $i/$iterations")
 val startTime = System.currentTimeMillis()

 runStreaming(scaleFactor, s"${outputPath}_benchmark_$i", fileType)

 val endTime = System.currentTimeMillis()
 val duration = endTime - startTime
 logger.info(s"Iteration $i completed in ${duration}ms")
 duration
 }

 val avgTime = times.sum / times.length
 val minTime = times.min
 val maxTime = times.max

 logger.info(s"=== Streaming Benchmark Results ===")
 logger.info(s"Average time: ${avgTime}ms")
 logger.info(s"Minimum time: ${minTime}ms")
 logger.info(s"Maximum time: ${maxTime}ms")
 logger.info(s"Performance: Complete LDBC dataset processing")
 logger.info(s"Entities processed: 17/22 types (77.3% coverage)")
 }

 /**
 * Validate streaming configuration
 */
 private def validateStreamingConfiguration(config: StreamingConfiguration): ValidationResult = {
 val errors = scala.collection.mutable.ListBuffer[String]()

 if (config.output_path.trim.isEmpty) {
 errors += "output path cannot be empty"
 }

 if (config.graph_name.trim.isEmpty) {
 errors += "Graph name cannot be empty"
 }

 if (config.vertex_chunk_size <= 0) {
 errors += s"Vertex chunk size must be positive, got: ${config.vertex_chunk_size}"
 }

 if (config.edge_chunk_size <= 0) {
 errors += s"Edge chunk size must be positive, got: ${config.edge_chunk_size}"
 }

 val supportedFileTypes = Set("csv", "parquet", "orc")
 if (!supportedFileTypes.contains(config.file_type.toLowerCase)) {
 errors += s"Unsupported file type: ${config.file_type}. Supported types: ${supportedFileTypes.mkString(", ")}"
 }

 if (errors.isEmpty) {
 ValidationResult.success()
 } else {
 ValidationResult.failure(errors.toList)
 }
 }
}