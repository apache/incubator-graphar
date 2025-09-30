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
 * LDBC增强双轨制架构示例
 *
 * 展示新的LdbcGraphArBridge双轨制处理能力：
 * 1. 静态数据（Person等）通过RDD生成器处理
 * 2. 动态数据通过流式架构处理
 * 3. 智能选择输出策略并生成标准GraphAr格式
 */
object LdbcEnhancedBridgeExample {

  private val logger: Logger = LoggerFactory.getLogger(LdbcEnhancedBridgeExample.getClass)

  def main(args: Array[String]): Unit = {
    // 参数验证
    if (args.length < 6) {
      printUsage()
      System.exit(1)
    }

    try {
      // 解析参数
      val scaleFactor = args(0).toDouble
      val outputPath = args(1)
      val graphName = args(2)
      val vertexChunkSize = args(3).toLong
      val edgeChunkSize = args(4).toLong
      val fileType = args(5)

      logger.info("=" * 80)
      logger.info("LDBC增强双轨制架构示例启动")
      logger.info("=" * 80)
      logger.info(s"参数配置:")
      logger.info(s"  Scale Factor: $scaleFactor")
      logger.info(s"  输出路径: $outputPath")
      logger.info(s"  图名称: $graphName")
      logger.info(s"  顶点分块大小: $vertexChunkSize")
      logger.info(s"  边分块大小: $edgeChunkSize")
      logger.info(s"  文件类型: $fileType")
      logger.info("=" * 80)

      // 初始化Spark会话
      val spark = SparkSession.builder()
        .appName(s"LDBC Enhanced Bridge Example - Scale $scaleFactor")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("ldbc.scale.factor", scaleFactor.toString)
        .getOrCreate()

      logger.info(s"Spark会话初始化完成: ${spark.sparkContext.applicationId}")

      try {
        // 创建增强双轨制桥接器
        val enhancedBridge = new LdbcGraphArBridge()
        logger.info("LdbcGraphArBridge双轨制架构初始化完成")

        // 记录开始时间
        val startTime = System.currentTimeMillis()

        // 执行双轨制转换
        logger.info("开始执行增强双轨制LDBC到GraphAr转换...")
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

        // 处理结果
        result match {
          case Success(conversionResult) =>
            logger.info("=" * 80)
            logger.info("双轨制转换成功完成！")
            logger.info("=" * 80)
            logger.info("转换结果:")
            logger.info(s"  Person数量: ${conversionResult.personCount}")
            logger.info(s"  Knows关系数量: ${conversionResult.knowsCount}")
            logger.info(s"  Interest关系数量: ${conversionResult.interestCount}")
            logger.info(s"  WorkAt关系数量: ${conversionResult.workAtCount}")
            logger.info(s"  StudyAt关系数量: ${conversionResult.studyAtCount}")
            logger.info(s"  Location关系数量: ${conversionResult.locationCount}")
            logger.info(s"  输出路径: ${conversionResult.outputPath}")
            logger.info(s"  转换耗时: ${duration}秒")

            if (conversionResult.warnings.nonEmpty) {
              logger.info("处理信息:")
              conversionResult.warnings.foreach(warning => logger.info(s"  - $warning"))
            }
            logger.info("=" * 80)

            // 验证输出
            verifyOutput(outputPath, spark)

          case Failure(exception) =>
            logger.error("=" * 80)
            logger.error("双轨制转换失败！")
            logger.error("=" * 80)
            logger.error(s"错误信息: ${exception.getMessage}")
            logger.error(s"转换耗时: ${duration}秒")
            logger.error("=" * 80)
            exception.printStackTrace()
            System.exit(1)
        }

      } finally {
        // 清理Spark资源
        spark.stop()
        logger.info("Spark会话已清理")
      }

    } catch {
      case e: NumberFormatException =>
        logger.error(s"参数格式错误: ${e.getMessage}")
        printUsage()
        System.exit(1)
      case e: Exception =>
        logger.error("程序执行失败", e)
        System.exit(1)
    }
  }

  /**
   * 验证输出结果
   */
  private def verifyOutput(outputPath: String, spark: SparkSession): Unit = {
    logger.info("开始验证输出结果...")

    try {
      import java.nio.file.{Files, Paths}
      val outputDir = Paths.get(outputPath)

      // 检查基本目录结构
      val vertexDir = outputDir.resolve("vertex")
      val edgeDir = outputDir.resolve("edge")

      if (Files.exists(vertexDir)) {
        logger.info("✓ vertex/ 目录存在")
        val vertexDirs = Files.list(vertexDir).toArray().length
        logger.info(s"  发现 $vertexDirs 个顶点实体目录")
      } else {
        logger.warn("✗ vertex/ 目录不存在")
      }

      if (Files.exists(edgeDir)) {
        logger.info("✓ edge/ 目录存在")
        val edgeDirs = Files.list(edgeDir).toArray().length
        logger.info(s"  发现 $edgeDirs 个边关系目录")
      } else {
        logger.warn("✗ edge/ 目录不存在")
      }

      // 检查YAML元数据文件
      val graphYaml = outputDir.resolve(s"${Paths.get(outputPath).getFileName}.graph.yml")
      if (Files.exists(graphYaml)) {
        logger.info("✓ 图YAML元数据文件存在")
      } else {
        logger.warn("✗ 图YAML元数据文件缺失")
      }

      logger.info("输出验证完成")

    } catch {
      case e: Exception =>
        logger.warn(s"输出验证过程中出现错误: ${e.getMessage}")
    }
  }

  /**
   * 打印使用说明
   */
  private def printUsage(): Unit = {
    println("LDBC增强双轨制架构示例")
    println("用法: LdbcEnhancedBridgeExample <scaleFactor> <outputPath> <graphName> <vertexChunkSize> <edgeChunkSize> <fileType>")
    println("")
    println("参数说明:")
    println("  scaleFactor      - LDBC数据规模因子 (例如: 0.003)")
    println("  outputPath       - GraphAr输出路径 (例如: /tmp/graphar_enhanced)")
    println("  graphName        - 图名称 (例如: ldbc_snb)")
    println("  vertexChunkSize  - 顶点分块大小 (例如: 1024)")
    println("  edgeChunkSize    - 边分块大小 (例如: 1024)")
    println("  fileType         - 文件格式 (csv/parquet/orc)")
    println("")
    println("示例:")
    println("  spark-submit --class org.apache.graphar.datasources.ldbc.examples.LdbcEnhancedBridgeExample \\")
    println("    --master local[*] \\")
    println("    --jars ../ldbc-snb-datagen/target/ldbc_snb_datagen_xxx.jar \\")
    println("    target/snb-graphar-bridge-0.13.0-SNAPSHOT.jar \\")
    println("    0.003 /tmp/graphar_enhanced ldbc_snb 1024 1024 parquet")
  }
}