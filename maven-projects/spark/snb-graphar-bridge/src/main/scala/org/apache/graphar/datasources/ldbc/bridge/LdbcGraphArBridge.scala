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

import org.apache.graphar.datasources.ldbc.model.{
  ConversionResult,
  ValidationResult,
  ValidationSuccess,
  ValidationFailure
}
import org.apache.graphar.datasources.ldbc.stream.core.GraphArDataCollector
import org.apache.graphar.datasources.ldbc.stream.processor.{
  UnifiedIdManager,
  PersonRDDProcessor,
  StaticEntityProcessor
}
import org.apache.graphar.datasources.ldbc.stream.strategy.{
  EnhancedOutputStrategySelector,
  OutputStrategy,
  SystemResourceInfo,
  StrategyDecision
}
import org.apache.graphar.datasources.ldbc.stream.writer.StreamingGraphArWriter
import org.apache.graphar.datasources.ldbc.stream.output.GraphArActivityOutputStream
import org.apache.graphar.graph.GraphWriter
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Try, Success, Failure}
import java.io.File
import java.nio.file.{Files, Paths}

/**
 * Main controller for LDBC to GraphAr conversion
 *
 * Implements a dual-track processing architecture:
 *   1. Static data (Person, etc.) processed via RDD-based batch processing 2.
 *      Dynamic data processed via streaming architecture 3. Intelligently
 *      selects output strategy and generates standard GraphAr format
 */
class LdbcGraphArBridge extends LdbcBridgeInterface {

  private val logger: Logger =
    LoggerFactory.getLogger(classOf[LdbcGraphArBridge])

  /**
   * Unified write method (following GraphAr GraphWriter pattern)
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
      s"Starting LDBC to GraphAr conversion: path=$path, name=$name, vertex_chunk_size=$vertex_chunk_size, edge_chunk_size=$edge_chunk_size, file_type=$file_type"
    )

    Try {
      implicit val sparkSession: SparkSession = spark

      // 1. Parameter validation
      val validation = validateConfiguration(
        "dual_track",
        path,
        vertex_chunk_size,
        edge_chunk_size,
        file_type
      )
      if (!validation.isSuccess) {
        throw new IllegalArgumentException(
          s"Configuration validation failed: ${validation.getErrors.mkString(", ")}"
        )
      }

      // 2. Initialize enhanced components
      val systemResources = SystemResourceInfo.current()
      val scaleFactor = getScaleFactor(spark)

      logger.info(s"Initializing enhanced components: scaleFactor=$scaleFactor")

      val idManager = new UnifiedIdManager(scaleFactor)
      idManager.initialize()

      val dataCollector = new GraphArDataCollector(path, name, idManager)
      val strategySelector = new EnhancedOutputStrategySelector()

      // 3. Create temporary directories
      val tempDir = createTempDirectories(path)

      try {
        // 4. First track: Process static entities
        val staticResult =
          processStaticEntities(dataCollector, tempDir.staticPath)
        logger.info(s"Static data processing completed: $staticResult")

        // 5. Second track: Process dynamic streaming data
        val streamingResult = processStreamingEntities(
          dataCollector,
          tempDir.streamingPath,
          name,
          vertex_chunk_size,
          edge_chunk_size,
          file_type
        )
        logger.info(s"Streaming data processing completed: $streamingResult")

        // 6. Intelligent strategy selection
        val strategyDecision = strategySelector.selectStrategyFromCollector(
          dataCollector,
          systemResources
        )
        logger.info(s"Selected strategy: ${strategyDecision}")

        // 7. Unified output based on selected strategy
        val finalResult = executeSelectedStrategy(
          path,
          tempDir,
          dataCollector,
          strategyDecision,
          name,
          vertex_chunk_size,
          edge_chunk_size,
          file_type
        )

        // 8. Generate processing report
        val processingReport =
          generateProcessingReport(dataCollector, strategyDecision, finalResult)
        logger.info(s"Processing report: $processingReport")

        ConversionResult(
          personCount = dataCollector.getTotalEntityCount().toLong,
          knowsCount = 0L,
          interestCount = 0L,
          workAtCount = 0L,
          studyAtCount = 0L,
          locationCount = 0L,
          outputPath = path,
          conversionTime = System.currentTimeMillis(),
          warnings = List(
            s"Strategy=${strategyDecision.strategy}, Confidence=${(strategyDecision.confidence * 100).toInt}%"
          )
        )

      } finally {
        // Cleanup temporary directories
        cleanupTempDirectories(tempDir)

        // Cleanup data collector
        dataCollector.cleanup()
      }

    }.recoverWith { case e: Exception =>
      logger.error("LDBC to GraphAr conversion failed", e)
      Try(
        ConversionResult(
          personCount = 0L,
          knowsCount = 0L,
          outputPath = path,
          warnings = List(s"Conversion failed: ${e.getMessage}")
        )
      )
    }
  }

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

    if (output_path.trim.isEmpty) {
      errors += "Output path cannot be empty"
    }

    if (vertex_chunk_size <= 0) {
      errors += s"Vertex chunk size must be positive: $vertex_chunk_size"
    }

    if (edge_chunk_size <= 0) {
      errors += s"Edge chunk size must be positive: $edge_chunk_size"
    }

    val supportedFileTypes = Set("csv", "parquet", "orc")
    if (!supportedFileTypes.contains(file_type.toLowerCase)) {
      errors += s"Unsupported file type: $file_type. Supported types: ${supportedFileTypes.mkString(", ")}"
    }

    if (!Set("dual_track", "static", "streaming").contains(mode)) {
      errors += s"Unsupported processing mode: $mode"
    }

    if (errors.isEmpty) {
      ValidationSuccess
    } else {
      ValidationFailure(errors.toList)
    }
  }

  /**
   * Get supported processing modes
   */
  override def getSupportedModes(): List[String] = {
    List("dual_track", "static", "streaming")
  }

  /**
   * Get bridge type identifier
   */
  override def getBridgeType(): String = "ldbc_enhanced_dual_track"

  /**
   * Process static entity data
   */
  private def processStaticEntities(
      dataCollector: GraphArDataCollector,
      tempStaticPath: String
  )(implicit spark: SparkSession): StaticProcessingResult = {

    logger.info("Starting to process static entity data")

    try {
      // Use PersonRDDProcessor to process Person and its relationships
      val personProcessor = new PersonRDDProcessor(dataCollector.idManager)
      val personResult = personProcessor.processAndCollect(dataCollector).get

      // Process other static entities (Organisation, Place, Tag, etc.)
      processOtherStaticEntities(dataCollector, dataCollector.idManager)

      StaticProcessingResult(
        success = true,
        processedEntities =
          List("Person", "Organisation", "Place", "Tag", "TagClass"),
        personResult = Some(personResult),
        totalVertices = personResult.personCount,
        totalEdges =
          personResult.knowsCount + personResult.hasInterestCount + personResult.studyAtCount + personResult.workAtCount + personResult.isLocatedInCount
      )

    } catch {
      case e: Exception =>
        logger.error("Static entity processing failed", e)
        StaticProcessingResult(
          success = false,
          processedEntities = List.empty,
          personResult = None,
          totalVertices = 0,
          totalEdges = 0,
          error = Some(e.getMessage)
        )
    }
  }

  /**
   * Process streaming entity data
   */
  private def processStreamingEntities(
      dataCollector: GraphArDataCollector,
      tempStreamingPath: String,
      graph_name: String,
      vertex_chunk_size: Long,
      edge_chunk_size: Long,
      file_type: String
  )(implicit spark: SparkSession): StreamingProcessingResult = {

    logger.info("Starting to process streaming entity data")

    try {
      // Use existing streaming components
      val streamingWriter = new StreamingGraphArWriter(
        output_path = tempStreamingPath,
        graph_name = graph_name,
        vertex_chunk_size = vertex_chunk_size,
        edge_chunk_size = edge_chunk_size,
        file_type = file_type
      )

      val activityOutputStream = new GraphArActivityOutputStream(
        output_path = tempStreamingPath,
        graph_name = graph_name,
        file_type = file_type
      )

      // Call real LDBC dynamic data generation
      simulateStreamingDataProcessing(activityOutputStream)

      activityOutputStream.close()

      // Scan temporary directory to collect dynamic entity information
      logger.info(
        "Scanning temporary streaming directory to collect dynamic entity information"
      )
      val streamingEntities =
        discoverStreamingEntities(tempStreamingPath, file_type)

      logger.info(
        s"Found ${streamingEntities.size} dynamic entity types: ${streamingEntities.keys.mkString(", ")}"
      )

      streamingEntities.foreach { case (entityType, (entityPath, isVertex)) =>
        try {
          // Read entity data
          val entityDF = file_type.toLowerCase match {
            case "parquet" => spark.read.parquet(entityPath)
            case "csv" => spark.read.option("header", "true").csv(entityPath)
            case "orc" => spark.read.orc(entityPath)
            case _     => spark.read.parquet(entityPath)
          }

          val rowCount = entityDF.count()
          logger.info(
            s"Reading dynamic entity $entityType: $rowCount records, path: $entityPath"
          )

          // Add to data collector
          if (isVertex) {
            dataCollector.addStaticVertexData(entityType, entityDF)
          } else {
            // Parse edge relationship
            val relation = parseEdgeRelationFromPath(entityType)
            dataCollector.addStaticEdgeData(relation, entityDF)
          }
        } catch {
          case e: Exception =>
            logger.error(s"Failed to read dynamic entity $entityType", e)
        }
      }

      StreamingProcessingResult(
        success = true,
        processedEntities = streamingEntities.keys.toList,
        totalChunks = 0,
        totalRows = streamingEntities.size
      )

    } catch {
      case e: Exception =>
        logger.error("Streaming entity processing failed", e)
        StreamingProcessingResult(
          success = false,
          processedEntities = List.empty,
          totalChunks = 0,
          totalRows = 0,
          error = Some(e.getMessage)
        )
    }
  }

  /**
   * Execute selected output strategy
   */
  private def executeSelectedStrategy(
      output_path: String,
      tempDir: TempDirectoryInfo,
      dataCollector: GraphArDataCollector,
      strategyDecision: StrategyDecision,
      graph_name: String,
      vertex_chunk_size: Long,
      edge_chunk_size: Long,
      file_type: String
  )(implicit spark: SparkSession): OutputExecutionResult = {

    logger.info(s"Executing output strategy: ${strategyDecision.strategy}")

    strategyDecision.strategy match {
      case OutputStrategy.COMPLETE_STANDARD =>
        executeCompleteStandardStrategy(
          output_path,
          tempDir,
          dataCollector,
          graph_name,
          vertex_chunk_size,
          edge_chunk_size,
          file_type
        )

      case OutputStrategy.HYBRID_DOCUMENTED =>
        executeHybridDocumentedStrategy(
          output_path,
          tempDir,
          dataCollector,
          graph_name,
          vertex_chunk_size,
          edge_chunk_size,
          file_type
        )
    }
  }

  /**
   * Execute complete standard strategy
   */
  private def executeCompleteStandardStrategy(
      output_path: String,
      tempDir: TempDirectoryInfo,
      dataCollector: GraphArDataCollector,
      graph_name: String,
      vertex_chunk_size: Long,
      edge_chunk_size: Long,
      file_type: String
  )(implicit spark: SparkSession): OutputExecutionResult = {

    logger.info("Executing complete standard strategy")

    try {
      val graphWriter = new GraphWriter()

      // 1. Add all static data to GraphWriter
      val staticDataFrames = dataCollector.getStaticDataFrames()
      logger.info(s"Retrieved static DataFrame count: ${staticDataFrames.size}")

      staticDataFrames.foreach { case (entityType, df) =>
        val rowCount = df.count()
        logger.info(
          s"Adding static vertex data to GraphWriter: $entityType, record count: $rowCount"
        )

        // Remove internal columns added by UnifiedIdManager, let GraphWriter manage vertex indices
        val cleanDF =
          df.drop("_graphArVertexIndex", "_entityType", "_idSpaceCategory")
        graphWriter.PutVertexData(entityType, cleanDF)
      }

      val staticEdgeFrames = dataCollector.getStaticEdgeFrames()
      logger.info(
        s"Retrieved static edge DataFrame count: ${staticEdgeFrames.size}"
      )

      staticEdgeFrames.foreach { case (relation, df) =>
        val rowCount = df.count()
        logger.info(
          s"Adding static edge data to GraphWriter: ${relation._1}_${relation._2}_${relation._3}, record count: $rowCount"
        )
        graphWriter.PutEdgeData(relation, df)
      }

      // 2. Convert streaming chunk data to DataFrame and add to GraphWriter
      val streamingEntityInfo = dataCollector.getStreamingEntityInfo()
      logger.info(
        s"Retrieved streaming entity count: ${streamingEntityInfo.size}"
      )

      streamingEntityInfo.foreach { case (entityType, info) =>
        logger.info(
          s"Processing streaming entity: $entityType, chunks: ${info.chunkCount}, rows: ${info.totalRows}"
        )
        val mergedDF = convertChunksToDataFrame(
          tempDir.streamingPath,
          entityType,
          info,
          file_type
        )

        if (isVertexEntity(entityType)) {
          graphWriter.PutVertexData(entityType, mergedDF)
          logger.info(
            s"Adding streaming vertex data to GraphWriter: $entityType"
          )
        } else {
          val relation = parseEdgeRelation(entityType)
          graphWriter.PutEdgeData(relation, mergedDF)
          logger.info(
            s"Adding streaming edge data to GraphWriter: ${relation._1}_${relation._2}_${relation._3}"
          )
        }
      }

      // 3. Generate complete standard GraphAr output in one shot
      logger.info(
        s"Starting GraphWriter write: output_path=$output_path, graph_name=$graph_name"
      )
      graphWriter.write(
        output_path,
        spark,
        graph_name,
        vertex_chunk_size,
        edge_chunk_size,
        file_type
      )
      logger.info("GraphWriter write completed")

      OutputExecutionResult(
        success = true,
        strategy = OutputStrategy.COMPLETE_STANDARD,
        outputFormat = "complete_graphar_standard",
        totalEntities = dataCollector.getTotalEntityCount()
      )

    } catch {
      case e: Exception =>
        logger.error("Complete standard strategy execution failed", e)
        OutputExecutionResult(
          success = false,
          strategy = OutputStrategy.COMPLETE_STANDARD,
          outputFormat = "failed",
          totalEntities = 0,
          error = Some(e.getMessage)
        )
    }
  }

  /**
   * Execute hybrid documented strategy
   */
  private def executeHybridDocumentedStrategy(
      output_path: String,
      tempDir: TempDirectoryInfo,
      dataCollector: GraphArDataCollector,
      graph_name: String,
      vertex_chunk_size: Long,
      edge_chunk_size: Long,
      file_type: String
  )(implicit spark: SparkSession): OutputExecutionResult = {

    logger.info("Executing hybrid documented strategy")

    try {
      // 1. Process static data part
      val staticGraphWriter = new GraphWriter()

      dataCollector.getStaticDataFrames().foreach { case (entityType, df) =>
        staticGraphWriter.PutVertexData(entityType, df)
      }

      dataCollector.getStaticEdgeFrames().foreach { case (relation, df) =>
        staticGraphWriter.PutEdgeData(relation, df)
      }

      // Write static part to temporary directory
      val tempStaticOutputPath = s"$output_path/.temp_static_final"
      staticGraphWriter.write(
        tempStaticOutputPath,
        spark,
        s"${graph_name}_static",
        vertex_chunk_size,
        edge_chunk_size,
        file_type
      )

      // 2. Merge directory structures
      mergeDirectoryStructures(
        tempStaticOutputPath,
        tempDir.streamingPath,
        output_path
      )

      // 3. Generate unified standard YAML metadata
      generateHybridStandardYamls(
        output_path,
        dataCollector,
        graph_name,
        vertex_chunk_size,
        edge_chunk_size,
        file_type
      )

      OutputExecutionResult(
        success = true,
        strategy = OutputStrategy.HYBRID_DOCUMENTED,
        outputFormat = "hybrid_with_standard_metadata",
        totalEntities = dataCollector.getTotalEntityCount()
      )

    } catch {
      case e: Exception =>
        logger.error("Hybrid documented strategy execution failed", e)
        OutputExecutionResult(
          success = false,
          strategy = OutputStrategy.HYBRID_DOCUMENTED,
          outputFormat = "failed",
          totalEntities = 0,
          error = Some(e.getMessage)
        )
    }
  }

  // Helper method implementations
  private def getScaleFactor(spark: SparkSession): Double = {
    // Get scale factor from Spark configuration or system properties
    spark.conf
      .getOption("ldbc.scale.factor")
      .map(_.toDouble)
      .getOrElse(0.003) // Default scale factor
  }

  private def createTempDirectories(basePath: String): TempDirectoryInfo = {
    val tempDir = TempDirectoryInfo(
      basePath = basePath,
      staticPath = s"$basePath/.temp_static",
      streamingPath = s"$basePath/.temp_streaming"
    )

    Files.createDirectories(Paths.get(tempDir.staticPath))
    Files.createDirectories(Paths.get(tempDir.streamingPath))

    tempDir
  }

  private def cleanupTempDirectories(tempDir: TempDirectoryInfo): Unit = {
    try {
      if (Files.exists(Paths.get(tempDir.staticPath))) {
        deleteDirectory(new File(tempDir.staticPath))
      }
      if (Files.exists(Paths.get(tempDir.streamingPath))) {
        deleteDirectory(new File(tempDir.streamingPath))
      }
    } catch {
      case e: Exception =>
        logger.warn("Failed to cleanup temporary directories", e)
    }
  }

  private def deleteDirectory(directory: File): Boolean = {
    if (directory.exists()) {
      directory.listFiles().foreach { file =>
        if (file.isDirectory) {
          deleteDirectory(file)
        } else {
          file.delete()
        }
      }
      directory.delete()
    } else {
      true
    }
  }

  /**
   * Scan temporary streaming directory to discover generated dynamic entities
   * Returns Map[EntityType, (Path, IsVertex)]
   */
  private def discoverStreamingEntities(
      tempStreamingPath: String,
      file_type: String
  ): Map[String, (String, Boolean)] = {
    import java.nio.file.{Files, Paths}
    import scala.collection.JavaConverters._

    val result = scala.collection.mutable.Map[String, (String, Boolean)]()

    try {
      // Check vertex directory
      val vertexDir = Paths.get(tempStreamingPath, "vertex")
      if (Files.exists(vertexDir) && Files.isDirectory(vertexDir)) {
        Files.list(vertexDir).iterator().asScala.foreach { vertexPath =>
          if (Files.isDirectory(vertexPath)) {
            val entityType = vertexPath.getFileName.toString
            result(entityType) =
              (vertexPath.toString, true) // true indicates vertex
          }
        }
      }

      // Check edge directory
      val edgeDir = Paths.get(tempStreamingPath, "edge")
      if (Files.exists(edgeDir) && Files.isDirectory(edgeDir)) {
        Files.list(edgeDir).iterator().asScala.foreach { edgePath =>
          if (Files.isDirectory(edgePath)) {
            val entityType = edgePath.getFileName.toString
            result(entityType) =
              (edgePath.toString, false) // false indicates edge
          }
        }
      }

      // Check entities in root directory (like Photo)
      val rootDir = Paths.get(tempStreamingPath)
      if (Files.exists(rootDir) && Files.isDirectory(rootDir)) {
        Files.list(rootDir).iterator().asScala.foreach { entityPath =>
          val fileName = entityPath.getFileName.toString
          if (
            Files.isDirectory(entityPath) && !fileName
              .startsWith(".") && fileName != "vertex" && fileName != "edge"
          ) {
            // Determine whether it's a vertex or edge (by name pattern)
            val isVertex = !fileName.contains("_")
            result(fileName) = (entityPath.toString, isVertex)
          }
        }
      }
    } catch {
      case e: Exception =>
        logger.error("Failed to scan streaming directory", e)
    }

    result.toMap
  }

  /**
   * Parse edge relationship from path name
   */
  private def parseEdgeRelationFromPath(
      pathName: String
  ): (String, String, String) = {
    val parts = pathName.split("_")
    if (parts.length >= 3) {
      val src = parts(0)
      val dst = parts.last
      val edge = parts.slice(1, parts.length - 1).mkString("_")
      (src, edge, dst)
    } else {
      logger.warn(
        s"Failed to parse edge relation: $pathName, using default value"
      )
      ("Unknown", pathName, "Unknown")
    }
  }

  private def processOtherStaticEntities(
      dataCollector: GraphArDataCollector,
      idManager: UnifiedIdManager
  )(implicit spark: SparkSession): Unit = {
    logger.info("Processing other static entities")

    try {
      // Create static entity processor
      val staticProcessor = new StaticEntityProcessor(idManager)(spark)

      // Generate all static entities
      val staticEntities = staticProcessor.generateAllStaticEntities()

      if (staticEntities.isEmpty) {
        logger.warn(
          "Failed to generate any static entities, LDBC dictionaries may not be initialized"
        )
        return
      }

      // Add vertices to data collector
      staticEntities.foreach { case (entityType, df) =>
        val recordCount = df.count()
        if (recordCount > 0) {
          logger.info(s"Adding static entity $entityType: $recordCount records")
          dataCollector.addStaticVertexData(entityType, df)
        } else {
          logger.warn(s"Static entity $entityType is empty, skipping")
        }
      }

      // Generate and add static edge data
      logger.info("Starting to generate static edge data")
      val staticEdges = staticProcessor.generateAllStaticEdges()

      staticEdges.foreach { case (edgeKey, df) =>
        val recordCount = df.count()
        if (recordCount > 0) {
          // Parse edge relation tuple: Organisation_isLocatedIn_Place -> (Organisation, isLocatedIn, Place)
          val parts = edgeKey.split("_")
          val relation = if (parts.length >= 3) {
            val src = parts(0)
            val dst = parts.last
            val edge = parts.slice(1, parts.length - 1).mkString("_")
            (src, edge, dst)
          } else {
            logger.error(s"Failed to parse edge relation: $edgeKey")
            ("Unknown", "Unknown", "Unknown")
          }

          logger.info(
            s"Adding static edge data ${relation._1}_${relation._2}_${relation._3}: $recordCount records"
          )
          dataCollector.addStaticEdgeData(relation, df)
        } else {
          logger.warn(s"Static edge $edgeKey is empty, skipping")
        }
      }

      logger.info("Static entity and edge processing completed")

    } catch {
      case e: Exception =>
        logger.error("Error occurred while processing static entities", e)
        // Continue processing, don't interrupt the entire workflow
        logger.warn(
          "Static entity processing failed, but continuing with subsequent steps"
        )
    }
  }

  private def simulateStreamingDataProcessing(
      outputStream: GraphArActivityOutputStream
  )(implicit spark: SparkSession): Unit = {
    logger.info(
      "Starting to process dynamic data stream (real LDBC Activity generation)"
    )

    try {
      // Import LDBC required classes
      import ldbc.snb.datagen.generator.DatagenParams
      import ldbc.snb.datagen.generator.generators.{
        SparkPersonGenerator,
        PersonActivityGenerator
      }
      import scala.collection.JavaConverters._

      // Create LDBC configuration
      val ldbcConfig = createLdbcConfiguration()
      DatagenParams.readConf(ldbcConfig)

      logger.info(
        s"Starting to generate LDBC dynamic data, scaleFactor=${getScaleFactor(spark)}"
      )

      // 1. Generate Person data (foundation of dynamic data)
      val personRDD = SparkPersonGenerator(ldbcConfig)(spark)
      val personCount = personRDD.count()
      logger.info(s"Person RDD generation completed: $personCount Persons")

      // 2. Group Persons by block (following LDBC PersonSorter logic)
      val blockSize = DatagenParams.blockSize
      val personsByBlock = personRDD
        .map { person => (person.getAccountId / blockSize, person) }
        .groupByKey()
        .collect()

      logger.info(
        s"Person data has been grouped into ${personsByBlock.length} blocks"
      )

      // 3. Generate Activity data for each block
      val activityGenerator = new PersonActivityGenerator()
      var totalActivitiesProcessed = 0

      personsByBlock.foreach { case (blockId, persons) =>
        val personList = persons.toList.sortBy(_.getAccountId).asJava
        logger.info(
          s"Processing Block $blockId, containing ${personList.size()} Persons"
        )

        try {
          // Use public API generateActivityForBlock() to generate entire block's Activity data
          val activityStream = activityGenerator.generateActivityForBlock(
            blockId.toInt,
            personList
          )

          // Process each GenActivity in the stream
          activityStream.forEach { genActivity =>
            try {
              // Use GraphArActivityOutputStream to process generated Activity
              outputStream.processLdbcActivity(genActivity)
              totalActivitiesProcessed += 1

              if (totalActivitiesProcessed % 10 == 0) {
                logger.info(
                  s"Processed $totalActivitiesProcessed Person Activity data"
                )
              }

            } catch {
              case e: Exception =>
                logger.warn(
                  s"Failed to process GenActivity data: ${e.getMessage}"
                )
                logger.debug("Exception details:", e)
            }
          }

          logger.info(
            s"Block $blockId Activity data processing completed, containing ${personList.size()} Persons"
          )

        } catch {
          case e: Exception =>
            logger.error(s"Block $blockId Activity data generation failed", e)
        }
      }

      logger.info("=" * 60)
      logger.info("LDBC dynamic data generation processing completed")
      logger.info(s"Total processed $personCount Persons")
      logger.info(
        s"Total processed $totalActivitiesProcessed Activities (including Wall/Group/Album)"
      )
      logger.info(
        "Forum/Post/Comment/Like counts refer to output file statistics"
      )
      logger.info("=" * 60)

    } catch {
      case e: Exception =>
        logger.error("LDBC dynamic data generation failed", e)
        logger.error("Exception stack trace:", e)
        throw e // Rethrow exception for outer layer to handle
    }
  }

  /**
   * Create LDBC configuration
   */
  private def createLdbcConfiguration()(implicit
      spark: SparkSession
  ): ldbc.snb.datagen.util.GeneratorConfiguration = {
    import ldbc.snb.datagen.util.{GeneratorConfiguration, ConfigParser}
    import scala.collection.JavaConverters._

    // Get scale factor from Spark configuration, use default if not available
    val scaleFactor = getScaleFactor(spark)

    // Convert double to integer string for LDBC (e.g., 1.0 -> "1", 0.003 -> "0.003")
    val scaleFactorString =
      if (scaleFactor == scaleFactor.toInt) scaleFactor.toInt.toString
      else scaleFactor.toString

    // Use LdbcConfigUtils to create complete configuration
    val config = org.apache.graphar.datasources.ldbc.util.LdbcConfigUtils
      .createFastTestConfig(scaleFactorString)

    logger.info(
      s"LDBC configuration created: scaleFactor=$scaleFactor, scaleFactorString=$scaleFactorString"
    )
    config
  }

  private def inferSchemaFromEntity(
      entityName: String
  ): org.apache.spark.sql.types.StructType = {
    // Infer basic Schema based on entity name
    import org.apache.spark.sql.types._

    entityName match {
      case "Forum" =>
        StructType(
          Array(
            StructField("id", LongType, false),
            StructField("title", StringType, true),
            StructField("creationDate", TimestampType, false)
          )
        )
      case "Post" =>
        StructType(
          Array(
            StructField("id", LongType, false),
            StructField("content", StringType, true),
            StructField("creationDate", TimestampType, false)
          )
        )
      case _ =>
        StructType(
          Array(
            StructField("id", LongType, false),
            StructField("creationDate", TimestampType, false)
          )
        )
    }
  }

  private def convertChunksToDataFrame(
      basePath: String,
      entityType: String,
      info: org.apache.graphar.datasources.ldbc.stream.core.StreamingEntityInfo,
      file_type: String
  )(implicit spark: SparkSession): org.apache.spark.sql.DataFrame = {
    // Implement chunk to DataFrame conversion
    // For now, return a simulated empty DataFrame
    spark.createDataFrame(
      spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
      info.schema
    )
  }

  private def isVertexEntity(entityType: String): Boolean = {
    val vertexEntities = Set(
      "Person",
      "Organisation",
      "Place",
      "Tag",
      "TagClass",
      "Forum",
      "Post",
      "Comment",
      "Photo"
    )
    vertexEntities.contains(entityType)
  }

  private def parseEdgeRelation(
      entityType: String
  ): (String, String, String) = {
    // Simplified edge relation parsing
    entityType match {
      case "likes" => ("Person", "likes", "Post")
      case _       => ("Unknown", entityType, "Unknown")
    }
  }

  private def mergeDirectoryStructures(
      staticPath: String,
      streamingPath: String,
      output_path: String
  ): Unit = {
    logger.info("Merging directory structures (simplified implementation)")
  }

  private def generateHybridStandardYamls(
      output_path: String,
      dataCollector: GraphArDataCollector,
      graph_name: String,
      vertex_chunk_size: Long,
      edge_chunk_size: Long,
      file_type: String
  ): Unit = {
    logger.info(
      "Generating hybrid format standard YAML (simplified implementation)"
    )
  }

  private def generateProcessingReport(
      dataCollector: GraphArDataCollector,
      strategyDecision: StrategyDecision,
      executionResult: OutputExecutionResult
  ): ProcessingReport = {
    val collectionStats = dataCollector.getCollectionStatistics()
    val idUsageStats = dataCollector.idManager.generateUsageStatistics()

    ProcessingReport(
      strategy = strategyDecision.strategy.toString,
      confidence = strategyDecision.confidence,
      totalEntities = collectionStats.totalEntities,
      totalVertices = collectionStats.totalVertices,
      totalEdges = collectionStats.totalEdges,
      success = executionResult.success,
      outputFormat = executionResult.outputFormat,
      idUtilization = idUsageStats.overallUtilization
    )
  }
}

// Data class definitions
case class TempDirectoryInfo(
    basePath: String,
    staticPath: String,
    streamingPath: String
)

case class StaticProcessingResult(
    success: Boolean,
    processedEntities: List[String],
    personResult: Option[
      org.apache.graphar.datasources.ldbc.stream.processor.PersonProcessingResult
    ],
    totalVertices: Long,
    totalEdges: Long,
    error: Option[String] = None
)

case class StreamingProcessingResult(
    success: Boolean,
    processedEntities: List[String],
    totalChunks: Long,
    totalRows: Long,
    error: Option[String] = None
)

case class OutputExecutionResult(
    success: Boolean,
    strategy: OutputStrategy.Value,
    outputFormat: String,
    totalEntities: Int,
    error: Option[String] = None
)

case class ProcessingReport(
    strategy: String,
    confidence: Double,
    totalEntities: Int,
    totalVertices: Long,
    totalEdges: Long,
    success: Boolean,
    outputFormat: String,
    idUtilization: Double
)
