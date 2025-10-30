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
import org.apache.graphar.datasources.ldbc.model.{
  ConversionResult,
  StreamingConversionResult,
  ValidationResult
}
import org.apache.graphar.datasources.ldbc.stream.core.{LdbcStreamingIntegrator}
import org.apache.graphar.datasources.ldbc.stream.model.IntegrationStatistics
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

/**
 * LDBC streaming bridge (dedicated to streaming processing)
 *
 * Provides complete LDBC streaming processing functionality:
 *   1. Supports streaming processing mode for complete LDBC dataset 2.
 *      Standardized configuration and validation 3. GraphAr format output
 */
class LdbcStreamingBridge extends StreamingBridgeInterface with Serializable {

  private val logger: Logger =
    LoggerFactory.getLogger(classOf[LdbcStreamingBridge])

  /**
   * Unified write method (following GraphAr GraphWriter interface)
   */
  override def write(
      path: String,
      spark: SparkSession,
      name: String,
      vertex_chunk_size: Long,
      edge_chunk_size: Long,
      file_type: String
  ): Try[ConversionResult] = {
    logger.info(
      s"LdbcStreamingBridge.write() called with path=$path, name=$name"
    )
    logger.info("Using streaming mode for all write operations")

    // Convert to streaming configuration and call streaming processing
    val streamingConfig = StreamingConfiguration(
      ldbc_config_path = "ldbc_config.properties",
      output_path = path,
      scale_factor = "0.1", // Default scale factor
      graph_name = name,
      vertex_chunk_size = vertex_chunk_size,
      edge_chunk_size = edge_chunk_size,
      file_type = file_type
    )

    writeStreaming(streamingConfig)(spark).map(_.asInstanceOf[ConversionResult])
  }

  /**
   * Streaming processing dedicated write method (using new streaming
   * architecture)
   */
  override def writeStreaming(
      config: StreamingConfiguration
  )(implicit spark: SparkSession): Try[StreamingConversionResult] = Try {

    logger.info("=== STREAMING MODE (FULL IMPLEMENTATION) ===")
    logger.info(
      "Using GraphArActivityOutputStream for true streaming processing"
    )
    logger.info(
      "Supporting complete LDBC entities: Person, Forum, Post, Comment, and all relationships"
    )

    val startTime = System.currentTimeMillis()

    // Create streaming integrator
    val streamingIntegrator = new LdbcStreamingIntegrator(
      output_path = config.output_path,
      graph_name = config.graph_name,
      vertex_chunk_size = config.vertex_chunk_size,
      edge_chunk_size = config.edge_chunk_size,
      file_type = config.file_type
    )

    try {
      // Create LDBC configuration
      val ldbcConfig = createLdbcConfiguration(config)

      // Execute streaming conversion
      val conversionResult =
        streamingIntegrator.executeStreamingConversion(ldbcConfig)

      conversionResult match {
        case scala.util.Success(result) =>
          logger.info(
            s"✓ Streaming conversion completed: ${result.processingDurationMs}ms"
          )
          logger.info(
            s"✓ Supported entity types: ${streamingIntegrator.getSupportedEntityTypes().mkString(", ")}"
          )
          logger.info(
            s"✓ Processed entities: ${result.integrationStatistics.processedEntities.mkString(", ")}"
          )
          logger.info(
            s"✓ Total records: ${result.integrationStatistics.totalRecords}"
          )
          logger.info(
            "✓ Streaming conversion completed, real LDBC data generation successful"
          )
          result

        case scala.util.Failure(exception) =>
          logger.error("✗ Streaming conversion failed", exception)
          throw exception // Directly throw exception, no fallback
      }

    } finally {
      streamingIntegrator.cleanup()
    }
  }

  /**
   * Create LDBC configuration
   */
  private def createLdbcConfiguration(
      config: StreamingConfiguration
  ): GeneratorConfiguration = {
    val ldbcConfig = new GeneratorConfiguration(
      new java.util.HashMap[String, String]()
    )

    // *** Complete LDBC configuration based on params_default.ini ***

    // Basic probability parameters
    ldbcConfig.map.put("generator.baseProbCorrelated", "0.95")
    ldbcConfig.map.put("generator.blockSize", "10000")
    ldbcConfig.map.put("generator.degreeDistribution", "Facebook")
    ldbcConfig.map.put("generator.delta", "10000")
    ldbcConfig.map.put("generator.flashmobTagDistExp", "0.4")
    ldbcConfig.map.put("generator.flashmobTagMaxLevel", "20")
    ldbcConfig.map.put("generator.flashmobTagMinLevel", "1")
    ldbcConfig.map.put("generator.flashmobTagsPerMonth", "5")
    ldbcConfig.map.put("generator.groupModeratorProb", "0.05")
    ldbcConfig.map.put("generator.knowsGenerator", "Distance")
    ldbcConfig.map.put("generator.limitProCorrelated", "0.2")

    // Content size parameters
    ldbcConfig.map.put("generator.maxCommentSize", "185")
    ldbcConfig.map.put("generator.maxCompanies", "3")
    ldbcConfig.map.put("generator.maxEmails", "5")
    ldbcConfig.map.put("generator.maxLargeCommentSize", "2000")
    ldbcConfig.map.put("generator.maxLargePostSize", "2000")
    ldbcConfig.map.put("generator.maxNumComments", "20")
    ldbcConfig.map.put("generator.maxNumFlashmobPostPerMonth", "30")
    ldbcConfig.map.put("generator.maxNumFriends", "1000")
    ldbcConfig.map.put("generator.maxNumGroupCreatedPerPerson", "4")
    ldbcConfig.map.put("generator.maxNumGroupFlashmobPostPerMonth", "10")
    ldbcConfig.map.put("generator.maxNumGroupPostPerMonth", "10")
    ldbcConfig.map.put("generator.maxNumLike", "1000")
    ldbcConfig.map.put("generator.maxNumMemberGroup", "100")
    ldbcConfig.map.put("generator.maxNumPhotoAlbumsPerMonth", "1")
    ldbcConfig.map.put("generator.maxNumPhotoPerAlbums", "20")
    ldbcConfig.map.put("generator.maxNumPopularPlaces", "2")
    ldbcConfig.map.put("generator.maxNumPostPerMonth", "30")
    ldbcConfig.map.put("generator.maxNumTagPerFlashmobPost", "80")
    ldbcConfig.map.put("generator.maxNumTagsPerPerson", "80")
    ldbcConfig.map.put("generator.maxTextSize", "250")

    // Minimum value parameters
    ldbcConfig.map.put("generator.minCommentSize", "75")
    ldbcConfig.map.put("generator.minLargeCommentSize", "700")
    ldbcConfig.map.put("generator.minLargePostSize", "700")
    ldbcConfig.map.put("generator.minNumTagsPerPerson", "1")
    ldbcConfig.map.put("generator.minTextSize", "85")

    // Probability parameters
    ldbcConfig.map.put("generator.missingRatio", "0.2")
    ldbcConfig.map.put("generator.person.similarity", "GeoDistance")
    ldbcConfig.map.put("generator.probAnotherBrowser", "0.01")
    ldbcConfig.map.put("generator.probCommentDeleted", "0.036")
    ldbcConfig.map.put("generator.probDiffIPinTravelSeason", "0.1")
    ldbcConfig.map.put("generator.probDiffIPnotTravelSeason", "0.02")
    ldbcConfig.map.put("generator.probEnglish", "0.6")
    ldbcConfig.map.put("generator.probForumDeleted", "0.01")
    ldbcConfig.map.put("generator.probInterestFlashmobTag", "0.8")
    ldbcConfig.map.put("generator.probKnowsDeleted", "0.05")
    ldbcConfig.map.put("generator.probLikeDeleted", "0.048")
    ldbcConfig.map.put("generator.probMembDeleted", "0.05")
    ldbcConfig.map.put("generator.probPersonDeleted", "0.07")
    ldbcConfig.map.put("generator.probPhotoDeleted", "0.054")
    ldbcConfig.map.put("generator.probPopularPlaces", "0.9")
    ldbcConfig.map.put("generator.probRandomPerLevel", "0.005")
    ldbcConfig.map.put("generator.probSecondLang", "0.2")
    ldbcConfig.map.put(
      "generator.probTopUniv",
      "0.9"
    ) // *** This is the missing critical parameter ***
    ldbcConfig.map.put("generator.probUnCorrelatedCompany", "0.05")
    ldbcConfig.map.put("generator.probUnCorrelatedOrganisation", "0.005")

    // Ratio parameters
    ldbcConfig.map.put("generator.ratioLargeComment", "0.001")
    ldbcConfig.map.put("generator.ratioLargePost", "0.001")
    ldbcConfig.map.put("generator.ratioReduceText", "0.8")

    // Core parameters (using passed scale factor)
    ldbcConfig.map.put("generator.scaleFactor", config.scale_factor)
    ldbcConfig.map.put("generator.numPersons", "10000")
    ldbcConfig.map.put("generator.numYears", "3")
    ldbcConfig.map.put("generator.startYear", "2010")
    ldbcConfig.map.put("generator.outputDir", config.output_path)
    ldbcConfig.map.put("generator.tagCountryCorrProb", "0.5")
    ldbcConfig.map.put("hadoop.numThreads", "1")

    // LDBC prefix parameters (maintaining backward compatibility)
    ldbcConfig.map.put(
      "ldbc.snb.datagen.generator.scaleFactor",
      config.scale_factor
    )
    ldbcConfig.map.put("ldbc.snb.datagen.generator.mode", "streaming")
    ldbcConfig.map.put("ldbc.snb.datagen.generator.numThreads", "4")

    // Enable all dynamic entity generation
    ldbcConfig.map.put(
      "ldbc.snb.datagen.generator.enableDynamicEntities",
      "true"
    )
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enableForum", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enablePost", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enableComment", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enableLike", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enablePhoto", "true")

    // Enable static entity generation (newly added)
    ldbcConfig.map.put(
      "ldbc.snb.datagen.generator.enableStaticEntities",
      "true"
    )
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enablePlace", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enableTag", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enableTagClass", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enableOrganisation", "true")

    // Static data dictionary configuration
    ldbcConfig.map.put("ldbc.snb.datagen.dictionary.loadPlaces", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.dictionary.loadTags", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.dictionary.loadCompanies", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.dictionary.loadUniversities", "true")

    // Ensure static entity integrity
    ldbcConfig.map.put("ldbc.snb.datagen.staticdata.includeHierarchy", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.staticdata.includeRelations", "true")

    // Serializer configuration
    ldbcConfig.map.put("serializer.outputDir", config.output_path)
    ldbcConfig.map.put("serializer.format", "graphar_streaming")
    ldbcConfig.map.put("serializer.compressed", "false")

    logger.info(
      s"LDBC complete configuration created, scale factor: ${config.scale_factor}"
    )
    logger.info("✓ Enabled dynamic entities: Forum, Post, Comment, Like, Photo")
    logger.info("✓ Enabled static entities: Place, Tag, TagClass, Organisation")
    logger.debug(
      s"Key parameters: generator.probTopUniv=0.9, generator.baseProbCorrelated=0.95"
    )
    logger.debug(s"Data generation time range: 2010-2013")
    ldbcConfig
  }

  /**
   * Validate configuration parameters
   */
  def validateConfiguration(
      mode: String,
      output_path: String,
      vertex_chunk_size: Long,
      edge_chunk_size: Long
  ): ValidationResult = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    // Validate processing mode
    if (!getSupportedModes().contains(mode)) {
      errors += s"Unsupported processing mode: $mode. Supported modes: ${getSupportedModes().mkString(", ")}"
    }

    // Validate output path
    if (output_path.trim.isEmpty) {
      errors += "Output path cannot be empty"
    }

    // Validate chunk sizes
    if (vertex_chunk_size <= 0) {
      errors += s"Vertex chunk size must be positive, got: $vertex_chunk_size"
    }

    if (edge_chunk_size <= 0) {
      errors += s"Edge chunk size must be positive, got: $edge_chunk_size"
    }

    if (errors.isEmpty) {
      ValidationResult.success()
    } else {
      ValidationResult.failure(errors.toList)
    }
  }

  /**
   * Get bridge type identifier
   */
  override def getBridgeType(): String = "streaming"

  /**
   * Validate configuration parameters
   */
  override def validateConfiguration(
      mode: String,
      output_path: String,
      vertex_chunk_size: Long,
      edge_chunk_size: Long,
      file_type: String
  ): ValidationResult = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    if (!getSupportedModes().contains(mode)) {
      errors += s"Unsupported mode: $mode. Supported: ${getSupportedModes().mkString(", ")}"
    }

    if (output_path.trim.isEmpty) {
      errors += "Output path cannot be empty"
    }

    if (vertex_chunk_size <= 0) {
      errors += s"Vertex chunk size must be positive: $vertex_chunk_size"
    }

    if (edge_chunk_size <= 0) {
      errors += s"Edge chunk size must be positive: $edge_chunk_size"
    }

    val supportedTypes = Set("csv", "parquet", "orc")
    if (!supportedTypes.contains(file_type.toLowerCase)) {
      errors += s"Unsupported file type: $file_type. Supported: ${supportedTypes.mkString(", ")}"
    }

    if (errors.isEmpty) {
      ValidationResult.success()
    } else {
      ValidationResult.failure(errors.toList)
    }
  }

  /**
   * Get supported processing modes
   */
  override def getSupportedModes(): List[String] = {
    List("streaming")
  }

  /**
   * Get processing capability summary
   *
   * @return
   *   ProcessingCapability containing all supported entities and relationships
   */
  def getCapabilitySummary(): ProcessingCapability = {
    ProcessingCapability(
      batchModeEntities = List(), // No longer supports batch processing mode
      streamingModeEntities = List(
        // Dynamic vertex entities
        "Person",
        "Forum",
        "Post",
        "Comment",
        // Static vertex entities
        "Place",
        "Tag",
        "TagClass",
        "Organisation",
        // Dynamic edge relationships
        "knows",
        "hasInterest",
        "workAt",
        "studyAt",
        "isLocatedIn",
        "hasCreator",
        "containerOf",
        "replyOf",
        "likes",
        "hasModerator",
        "hasMember",
        // Static edge relationships
        "Place_isPartOf_Place",
        "Tag_hasType_TagClass",
        "TagClass_isSubclassOf_TagClass",
        "Organisation_isLocatedIn_Place"
      ),
      hybridModeSupported = false,
      autoModeSupported = false
    )
  }
}

/**
 * Processing capability summary
 *
 * @param batchModeEntities
 *   Entity types supported in batch mode
 * @param streamingModeEntities
 *   Entity types supported in streaming mode
 * @param hybridModeSupported
 *   Whether hybrid processing mode is supported
 * @param autoModeSupported
 *   Whether auto mode selection is supported
 */
case class ProcessingCapability(
    batchModeEntities: List[String],
    streamingModeEntities: List[String],
    hybridModeSupported: Boolean,
    autoModeSupported: Boolean
) {

  /**
   * Calculate streaming mode entity coverage percentage
   *
   * @return
   *   Coverage percentage against LDBC SNB standard (22 total entities)
   */
  def coveragePercentage: Double = {
    // LDBC SNB standard total of 22 entities and relationship types (complete coverage target)
    val totalEntities = 22
    streamingModeEntities.length.toDouble / totalEntities * 100
  }
}

/**
 * Convenience constructor
 */
object LdbcStreamingBridge {

  /**
   * Create streaming bridge with default configuration
   *
   * @return
   *   New LdbcStreamingBridge instance with default settings
   */
  def createDefault(): LdbcStreamingBridge = {
    new LdbcStreamingBridge()
  }

  /**
   * Create streaming bridge with custom configuration
   *
   * @return
   *   New LdbcStreamingBridge instance
   */
  def create(): LdbcStreamingBridge = {
    new LdbcStreamingBridge()
  }
}
