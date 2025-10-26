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

import org.apache.graphar.datasources.ldbc.bridge.LdbcGraphArBridge
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Try, Success, Failure}

/**
 * LDBCEnhanceddual-trackarchitecture example
 *
 * Demonstrate newLdbcGraphArBridgedual-trackprocessing capability:
 * 1. static data（Personetc)through RDDgeneratorProcess
 * 2. dynamic datathrough streamingarchitecture processing
 * 3. Intelligent selectionoutput policyand generatestandard GraphAr format
 */
object LdbcEnhancedBridgeExample {

 private val logger: Logger = LoggerFactory.getLogger(LdbcEnhancedBridgeExample.getClass)

 def main(args: Array[String]): Unit = {
 // Parameter verification
 if (args.length < 6) {
 printUsage()
 System.exit(1)
 }

 try {
 // Parse parameters
 val scaleFactor = args(0).toDouble
 val outputPath = args(1)
 val graphName = args(2)
 val vertexChunkSize = args(3).toLong
 val edgeChunkSize = args(4).toLong
 val fileType = args(5)

 logger.info("=" * 80)
 logger.info("LDBC Enhanced Dual-Track Architecture Example Started")
 logger.info("=" * 80)
 logger.info(s"Parameter Configuration:")
 logger.info(s" Scale Factor: $scaleFactor")
 logger.info(s" output Path: $outputPath")
 logger.info(s" Graph Name: $graphName")
 logger.info(s" Vertex Chunk Size: $vertexChunkSize")
 logger.info(s" Edge Chunk Size: $edgeChunkSize")
 logger.info(s" File Type: $fileType")
 logger.info("=" * 80)

 // Initialize SparkSession
 val spark = SparkSession.builder()
.appName(s"LDBC Enhanced Bridge Example - Scale $scaleFactor")
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
.config("ldbc.scale.factor", scaleFactor.toString)
.getOrCreate()

 logger.info(s"Spark session initialized: ${spark.sparkContext.applicationId}")

 try {
 // Create enhanceddual-track bridge
 val enhancedBridge = new LdbcGraphArBridge()
 logger.info("LdbcGraphArBridge dual-track architecture initialized")

 // Record start time
 val startTime = System.currentTimeMillis()

 // Execute dual-track conversion
 logger.info("Starting enhanced dual-track LDBC to GraphAr conversion...")
 val result = enhancedBridge.write(
 path = outputPath,
 spark = spark,
 name = graphName,
 vertex_chunk_size = vertexChunkSize,
 edge_chunk_size = edgeChunkSize,
 file_type = fileType
)

 val endTime = System.currentTimeMillis()
 val duration = (endTime - startTime) / 1000.0

 // Process result
 result match {
 case Success(conversionResult) =>
 logger.info("=" * 80)
 logger.info("Dual-track conversion completed successfully!")
 logger.info("=" * 80)
 logger.info("Conversion Results:")
 logger.info(s" Person Count: ${conversionResult.personCount}")
 logger.info(s" Knows relationship Count: ${conversionResult.knowsCount}")
 logger.info(s" Interest relationship Count: ${conversionResult.interestCount}")
 logger.info(s" WorkAt relationship Count: ${conversionResult.workAtCount}")
 logger.info(s" StudyAt relationship Count: ${conversionResult.studyAtCount}")
 logger.info(s" Location relationship Count: ${conversionResult.locationCount}")
 logger.info(s" output Path: ${conversionResult.outputPath}")
 logger.info(s" Conversion Duration: ${duration} seconds")

 if (conversionResult.warnings.nonEmpty) {
 logger.info("Processing Information:")
 conversionResult.warnings.foreach(warning => logger.info(s" - $warning"))
 }
 logger.info("=" * 80)

 // Verify output
 verifyoutput(outputPath, spark)

 case Failure(exception) =>
 logger.error("=" * 80)
 logger.error("Dual-track conversion failed!")
 logger.error("=" * 80)
 logger.error(s"Error Message: ${exception.getMessage}")
 logger.error(s"Conversion Duration: ${duration} seconds")
 logger.error("=" * 80)
 exception.printStackTrace()
 System.exit(1)
 }

 } finally {
 // Clean up Spark resources
 spark.stop()
 logger.info("Spark session closed")
 }

 } catch {
 case e: NumberFormatException =>
 logger.error(s"Parameter format error: ${e.getMessage}")
 printUsage()
 System.exit(1)
 case e: Exception =>
 logger.error("Program execution failed", e)
 System.exit(1)
 }
 }

 /**
 * Verify outputresult
 */
 private def verifyoutput(outputPath: String, spark: SparkSession): Unit = {
 logger.info("Starting output verification...")

 try {
 import java.nio.file.{Files, Paths}
 val outputDir = Paths.get(outputPath)

 // Check basic directory structure
 val vertexDir = outputDir.resolve("vertex")
 val edgeDir = outputDir.resolve("edge")

 if (Files.exists(vertexDir)) {
 logger.info("✓ vertex/ directory exists")
 val vertexDirs = Files.list(vertexDir).toArray().length
 logger.info(s" Found $vertexDirs vertex entity directories")
 } else {
 logger.warn("✗ vertex/ directory not found")
 }

 if (Files.exists(edgeDir)) {
 logger.info("✓ edge/ directory exists")
 val edgeDirs = Files.list(edgeDir).toArray().length
 logger.info(s" Found $edgeDirs edge relationship directories")
 } else {
 logger.warn("✗ edge/ directory not found")
 }

 // Check YAML metadata file
 val graphYaml = outputDir.resolve(s"${Paths.get(outputPath).getFileName}.graph.yml")
 if (Files.exists(graphYaml)) {
 logger.info("✓ Graph YAML metadata file exists")
 } else {
 logger.warn("✗ Graph YAML metadata file missing")
 }

 logger.info("output verification completed")

 } catch {
 case e: Exception =>
 logger.warn(s"Error occurred during output verification: ${e.getMessage}")
 }
 }

 /**
 * Print usage information
 */
 private def printUsage(): Unit = {
 logger.info("LDBC Enhanced Dual-Track Architecture Example")
 logger.info("Usage: LdbcEnhancedBridgeExample <scaleFactor> <outputPath> <graphName> <vertexChunkSize> <edgeChunkSize> <fileType>")
 logger.info("")
 logger.info("Parameter Description:")
 logger.info(" scaleFactor - LDBC data scale factor (e.g., 0.003)")
 logger.info(" outputPath - GraphAr output path (e.g., /tmp/graphar_enhanced)")
 logger.info(" graphName - Graph name (e.g., ldbc_snb)")
 logger.info(" vertexChunkSize - Vertex chunk size (e.g., 1024)")
 logger.info(" edgeChunkSize - Edge chunk size (e.g., 1024)")
 logger.info(" fileType - File format (csv/parquet/orc)")
 logger.info("")
 logger.info("Example:")
 logger.info(" spark-submit --class org.apache.graphar.datasources.ldbc.examples.LdbcEnhancedBridgeExample \\")
 logger.info(" --master local[*] \\")
 logger.info(" --jars../ldbc-snb-datagen/target/ldbc_snb_datagen_xxx.jar \\")
 logger.info(" target/snb-graphar-bridge-0.13.0-SNAPSHOT.jar \\")
 logger.info(" 0.003 /tmp/graphar_enhanced ldbc_snb 1024 1024 parquet")
 }
}