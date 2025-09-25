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

package org.apache.graphar.datasources.ldbc.examples

import org.apache.graphar.datasources.ldbc.bridge.LdbcDirectBridge
import org.apache.graphar.datasources.ldbc.util.LdbcConfigUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success}

object LdbcDirectBridgeExample {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("LDBC Direct Bridge Example")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    try {
      logger.info("Starting LDBC Direct Bridge Example")

      if (args.length < 2) {
        logger.error("Usage: LdbcDirectBridgeExample <scale_factor> <output_path> [format]")
        logger.info("scale_factor: LDBC scale factor (e.g., '0.1', '1')")
        logger.info("output_path: Output directory for GraphAr files")
        logger.info("format: 'fast' (test mode), 'csv', 'parquet', 'orc' (default: parquet)")
        System.exit(1)
      }

      val scaleFactor = args(0)
      val outputPath = args(1)
      val param3 = if (args.length > 2) args(2) else "parquet"
      val (mode, fileType) = param3.toLowerCase match {
        case "fast" => ("fast", "parquet")
        case "csv" => ("optimized", "csv")
        case "parquet" => ("optimized", "parquet")
        case "orc" => ("optimized", "orc")
        case _ => ("optimized", "parquet")
      }

      runExample(scaleFactor, outputPath, mode, fileType)(spark)

    } catch {
      case e: Exception =>
        logger.error("Example execution failed", e)
        System.exit(1)
    } finally {
      spark.stop()
    }
  }

  def runExample(
    scaleFactor: String,
    outputPath: String,
    mode: String,
    fileType: String
  )(implicit spark: SparkSession): Unit = {

    logger.info(s"=== LDBC Direct Bridge Example (Driver-side Generation) ===")
    logger.info(s"Scale Factor: $scaleFactor")
    logger.info(s"Output Path: $outputPath")
    logger.info(s"Mode: $mode")
    logger.info("Using NEW Driver-side generation to avoid blocking!")

    val config = mode.toLowerCase match {
      case "fast" =>
        logger.info("Using fast test configuration")
        LdbcConfigUtils.createFastTestConfig(scaleFactor)
      case _ =>
        logger.info("Using optimized configuration")
        LdbcConfigUtils.createOptimizedConfig(scaleFactor)
    }

    if (!LdbcConfigUtils.validateConfig(config)) {
      throw new IllegalArgumentException("Invalid LDBC configuration")
    }

    // NEW APPROACH: Use Driver-side generation instead of SparkPersonGenerator
    logger.info("Using Driver-side generation (NO MORE BLOCKING!)")
    val bridge = new LdbcDirectBridge()

    val result = bridge.convertToGraphArFromConfig(
      config = config,
      outputPath = outputPath,
      graphName = "ldbc_social_network",
      vertexChunkSize = 1024L,
      edgeChunkSize = 1024L,
      fileType = fileType
    )

    result match {
      case Success(conversionResult) =>
        logger.info("✓ Conversion completed successfully!")
        logger.info("=== Conversion Results ===")
        logger.info(s"Persons: ${conversionResult.personCount}")
        logger.info(s"Knows relationships: ${conversionResult.knowsCount}")
        logger.info(s"Interest relationships: ${conversionResult.interestCount}")
        logger.info(s"Work relationships: ${conversionResult.workAtCount}")
        logger.info(s"Study relationships: ${conversionResult.studyAtCount}")
        logger.info(s"Location relationships: ${conversionResult.locationCount}")
        logger.info(s"Output path: ${conversionResult.outputPath}")

        val totalRelationships = conversionResult.knowsCount +
          conversionResult.interestCount +
          conversionResult.workAtCount +
          conversionResult.studyAtCount +
          conversionResult.locationCount

        logger.info(s"Total relationships: $totalRelationships")
        logger.info("GraphAr files are ready for use!")

      case Failure(exception) =>
        logger.error("✗ Conversion failed", exception)
        throw exception
    }
  }

  def runBenchmark(
    scaleFactor: String,
    outputPath: String,
    iterations: Int = 3
  )(implicit spark: SparkSession): Unit = {
    logger.info(s"=== Performance Benchmark with Driver-side Generation (${iterations} iterations) ===")

    val times = (1 to iterations).map { i =>
      logger.info(s"Benchmark iteration $i/$iterations")
      val startTime = System.currentTimeMillis()

      runExample(scaleFactor, s"${outputPath}_benchmark_$i", "optimized", "parquet")

      val endTime = System.currentTimeMillis()
      val duration = endTime - startTime
      logger.info(s"Iteration $i completed in ${duration}ms")
      duration
    }

    val avgTime = times.sum / times.length
    val minTime = times.min
    val maxTime = times.max

    logger.info(s"=== Benchmark Results (Driver-side Generation) ===")
    logger.info(s"Average time: ${avgTime}ms")
    logger.info(s"Minimum time: ${minTime}ms")
    logger.info(s"Maximum time: ${maxTime}ms")
    logger.info("Note: These results use Driver-side generation to avoid initialization blocking")
  }
}