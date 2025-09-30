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
import org.apache.graphar.datasources.ldbc.stream.core.{LdbcStreamingIntegrator}
import org.apache.graphar.datasources.ldbc.stream.model.IntegrationStatistics
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

/**
 * LDBC流式桥接器（专用于流式处理）
 *
 * 提供完整的LDBC流式处理功能：
 * 1. 支持流式处理模式，处理完整的LDBC数据集
 * 2. 标准化的配置和验证
 * 3. GraphAr格式输出
 */
class LdbcStreamingBridge extends StreamingBridgeInterface with Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[LdbcStreamingBridge])

  /**
   * 统一的写入方法（遵循GraphAr GraphWriter接口）
   */
  override def write(
    path: String,
    spark: SparkSession,
    name: String,
    vertex_chunk_size: Long,
    edge_chunk_size: Long,
    file_type: String
  ): Try[ConversionResult] = {
    logger.info(s"LdbcStreamingBridge.write() called with path=$path, name=$name")
    logger.info("Using streaming mode for all write operations")

    // 转换为流式配置并调用流式处理
    val streamingConfig = StreamingConfiguration(
      ldbcConfigPath = "ldbc_config.properties",
      outputPath = path,
      scaleFactor = "0.1", // 默认规模因子
      graphName = name,
      vertexChunkSize = vertex_chunk_size,
      edgeChunkSize = edge_chunk_size,
      fileType = file_type
    )

    writeStreaming(streamingConfig)(spark).map(_.asInstanceOf[ConversionResult])
  }

  /**
   * 流式处理专用写入方法（使用新的流式架构）
   */
  override def writeStreaming(
    config: StreamingConfiguration
  )(implicit spark: SparkSession): Try[StreamingConversionResult] = Try {

    logger.info("=== STREAMING MODE (FULL IMPLEMENTATION) ===")
    logger.info("使用GraphArActivityOutputStream进行真正的流式处理")
    logger.info("支持完整的LDBC实体：Person, Forum, Post, Comment, 以及所有关系")

    val startTime = System.currentTimeMillis()

    // 创建流式集成器
    val streamingIntegrator = new LdbcStreamingIntegrator(
      outputPath = config.outputPath,
      graphName = config.graphName,
      vertexChunkSize = config.vertexChunkSize,
      edgeChunkSize = config.edgeChunkSize,
      fileType = config.fileType
    )

    try {
      // 创建LDBC配置
      val ldbcConfig = createLdbcConfiguration(config)

      // 执行流式转换
      val conversionResult = streamingIntegrator.executeStreamingConversion(ldbcConfig)

      conversionResult match {
        case scala.util.Success(result) =>
          logger.info(s"✓ 流式转换完成: ${result.processingDurationMs}ms")
          logger.info(s"✓ 支持的实体类型: ${streamingIntegrator.getSupportedEntityTypes().mkString(", ")}")
          logger.info(s"✓ 处理的实体: ${result.integrationStatistics.processedEntities.mkString(", ")}")
          logger.info(s"✓ 总记录数: ${result.integrationStatistics.totalRecords}")
          logger.info("✓ 流式转换完成，真实LDBC数据生成成功")
          result

        case scala.util.Failure(exception) =>
          logger.error("✗ 流式转换失败", exception)
          throw exception // 直接抛出异常，不降级
      }

    } finally {
      streamingIntegrator.cleanup()
    }
  }

  /**
   * 创建LDBC配置
   */
  private def createLdbcConfiguration(config: StreamingConfiguration): GeneratorConfiguration = {
    val ldbcConfig = new GeneratorConfiguration(new java.util.HashMap[String, String]())

    // *** 基于params_default.ini的完整LDBC配置 ***

    // 基础概率参数
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

    // 内容大小参数
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

    // 最小值参数
    ldbcConfig.map.put("generator.minCommentSize", "75")
    ldbcConfig.map.put("generator.minLargeCommentSize", "700")
    ldbcConfig.map.put("generator.minLargePostSize", "700")
    ldbcConfig.map.put("generator.minNumTagsPerPerson", "1")
    ldbcConfig.map.put("generator.minTextSize", "85")

    // 概率参数
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
    ldbcConfig.map.put("generator.probTopUniv", "0.9") // *** 这是缺失的关键参数 ***
    ldbcConfig.map.put("generator.probUnCorrelatedCompany", "0.05")
    ldbcConfig.map.put("generator.probUnCorrelatedOrganisation", "0.005")

    // 比例参数
    ldbcConfig.map.put("generator.ratioLargeComment", "0.001")
    ldbcConfig.map.put("generator.ratioLargePost", "0.001")
    ldbcConfig.map.put("generator.ratioReduceText", "0.8")

    // 核心参数（使用传入的规模因子）
    ldbcConfig.map.put("generator.scaleFactor", config.scaleFactor)
    ldbcConfig.map.put("generator.numPersons", "10000")
    ldbcConfig.map.put("generator.numYears", "3")
    ldbcConfig.map.put("generator.startYear", "2010")
    ldbcConfig.map.put("generator.outputDir", config.outputPath)
    ldbcConfig.map.put("generator.tagCountryCorrProb", "0.5")
    ldbcConfig.map.put("hadoop.numThreads", "1")

    // LDBC前缀参数（保持向后兼容）
    ldbcConfig.map.put("ldbc.snb.datagen.generator.scaleFactor", config.scaleFactor)
    ldbcConfig.map.put("ldbc.snb.datagen.generator.mode", "streaming")
    ldbcConfig.map.put("ldbc.snb.datagen.generator.numThreads", "4")

    // 启用所有动态实体生成
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enableDynamicEntities", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enableForum", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enablePost", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enableComment", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enableLike", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enablePhoto", "true")

    // 启用静态实体生成（新增）
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enableStaticEntities", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enablePlace", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enableTag", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enableTagClass", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.generator.enableOrganisation", "true")

    // 静态数据字典配置
    ldbcConfig.map.put("ldbc.snb.datagen.dictionary.loadPlaces", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.dictionary.loadTags", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.dictionary.loadCompanies", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.dictionary.loadUniversities", "true")

    // 确保静态实体完整性
    ldbcConfig.map.put("ldbc.snb.datagen.staticdata.includeHierarchy", "true")
    ldbcConfig.map.put("ldbc.snb.datagen.staticdata.includeRelations", "true")

    // 序列化器配置
    ldbcConfig.map.put("serializer.outputDir", config.outputPath)
    ldbcConfig.map.put("serializer.format", "graphar_streaming")
    ldbcConfig.map.put("serializer.compressed", "false")

    logger.info(s"LDBC完整配置创建完成，规模因子: ${config.scaleFactor}")
    logger.info("✓ 启用动态实体: Forum, Post, Comment, Like, Photo")
    logger.info("✓ 启用静态实体: Place, Tag, TagClass, Organisation")
    logger.debug(s"关键参数: generator.probTopUniv=0.9, generator.baseProbCorrelated=0.95")
    logger.debug(s"数据生成时间范围: 2010-2013")
    ldbcConfig
  }


  /**
   * 验证配置参数
   */
  def validateConfiguration(
                             mode: String,
                             outputPath: String,
                             vertexChunkSize: Long,
                             edgeChunkSize: Long
                           ): ValidationResult = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    // 验证处理模式
    if (!getSupportedModes().contains(mode)) {
      errors += s"Unsupported processing mode: $mode. Supported modes: ${getSupportedModes().mkString(", ")}"
    }

    // 验证输出路径
    if (outputPath.trim.isEmpty) {
      errors += "Output path cannot be empty"
    }

    // 验证块大小
    if (vertexChunkSize <= 0) {
      errors += s"Vertex chunk size must be positive, got: $vertexChunkSize"
    }

    if (edgeChunkSize <= 0) {
      errors += s"Edge chunk size must be positive, got: $edgeChunkSize"
    }

    if (errors.isEmpty) {
      ValidationResult.success()
    } else {
      ValidationResult.failure(errors.toList)
    }
  }

  /**
   * 获取桥接器类型标识
   */
  override def getBridgeType(): String = "streaming"

  /**
   * 验证配置参数
   */
  override def validateConfiguration(
    mode: String,
    outputPath: String,
    vertexChunkSize: Long,
    edgeChunkSize: Long,
    fileType: String
  ): ValidationResult = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    if (!getSupportedModes().contains(mode)) {
      errors += s"Unsupported mode: $mode. Supported: ${getSupportedModes().mkString(", ")}"
    }

    if (outputPath.trim.isEmpty) {
      errors += "Output path cannot be empty"
    }

    if (vertexChunkSize <= 0) {
      errors += s"Vertex chunk size must be positive: $vertexChunkSize"
    }

    if (edgeChunkSize <= 0) {
      errors += s"Edge chunk size must be positive: $edgeChunkSize"
    }

    val supportedTypes = Set("csv", "parquet", "orc")
    if (!supportedTypes.contains(fileType.toLowerCase)) {
      errors += s"Unsupported file type: $fileType. Supported: ${supportedTypes.mkString(", ")}"
    }

    if (errors.isEmpty) {
      ValidationResult.success()
    } else {
      ValidationResult.failure(errors.toList)
    }
  }

  /**
   * 获取支持的处理模式
   */
  override def getSupportedModes(): List[String] = {
    List("streaming")
  }

  /**
   * 获取处理能力摘要
   */
  def getCapabilitySummary(): ProcessingCapability = {
    ProcessingCapability(
      batchModeEntities = List(), // 不再支持批处理模式
      streamingModeEntities = List(
        // 动态顶点实体
        "Person", "Forum", "Post", "Comment",
        // 静态顶点实体
        "Place", "Tag", "TagClass", "Organisation",
        // 动态边关系
        "knows", "hasInterest", "workAt", "studyAt", "isLocatedIn",
        "hasCreator", "containerOf", "replyOf", "likes", "hasModerator", "hasMember",
        // 静态边关系
        "Place_isPartOf_Place", "Tag_hasType_TagClass", "TagClass_isSubclassOf_TagClass", "Organisation_isLocatedIn_Place"
      ),
      hybridModeSupported = false,
      autoModeSupported = false
    )
  }
}

/**
 * 处理能力摘要
 */
case class ProcessingCapability(
  batchModeEntities: List[String],
  streamingModeEntities: List[String],
  hybridModeSupported: Boolean,
  autoModeSupported: Boolean
) {
  def coveragePercentage: Double = {
    // LDBC SNB标准总共22个实体和关系类型（完整覆盖目标）
    val totalEntities = 22
    streamingModeEntities.length.toDouble / totalEntities * 100
  }
}

/**
 * 便捷构造器
 */
object LdbcStreamingBridge {

  /**
   * 创建默认配置的流式桥接器
   */
  def createDefault(): LdbcStreamingBridge = {
    new LdbcStreamingBridge()
  }

  /**
   * 创建自定义配置的流式桥接器
   */
  def create(): LdbcStreamingBridge = {
    new LdbcStreamingBridge()
  }
}