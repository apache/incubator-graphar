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

import org.apache.spark.sql.SparkSession
import org.apache.graphar.datasources.ldbc.stream.processor.{UnifiedIdManager, StaticEntityProcessor}
import org.apache.graphar.datasources.ldbc.stream.core.GraphArDataCollector
import org.apache.graphar.graph.GraphWriter
import org.slf4j.{Logger, LoggerFactory}

/**
 * 静态实体生成测试
 *
 * 这个测试程序用于验证StaticEntityProcessor能否正确生成静态实体数据
 * 并使用GraphWriter写入GraphAr格式
 */
object StaticEntityTest {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val outputPath = if (args.length > 0) args(0) else "/tmp/graphar_static_only"

    logger.info(s"静态实体测试开始，输出路径: $outputPath")

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Static Entity Test")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    try {
      // 1. 初始化ID管理器
      val idManager = new UnifiedIdManager(0.003)
      idManager.initialize()

      // 2. 创建数据收集器
      val dataCollector = new GraphArDataCollector(outputPath, "static_test", idManager)

      // 3. 生成静态实体
      val staticProcessor = new StaticEntityProcessor(idManager)
      val staticEntities = staticProcessor.generateAllStaticEntities()

      if (staticEntities.isEmpty) {
        logger.error("未能生成任何静态实体数据")
        return
      }

      // 4. 添加到数据收集器
      staticEntities.foreach { case (entityType, df) =>
        val count = df.count()
        logger.info(s"生成静态实体 $entityType: $count 条记录")
        if (count > 0) {
          dataCollector.addStaticVertexData(entityType, df)
          // 显示前5条数据
          logger.info(s"$entityType 数据示例:")
          df.show(5, truncate = false)
        }
      }

      // 5. 使用GraphWriter写入
      val graphWriter = new GraphWriter()

      // 添加所有静态顶点数据
      dataCollector.getStaticDataFrames().foreach { case (entityType, df) =>
        val cleanDF = df.drop("_graphArVertexIndex", "_entityType", "_idSpaceCategory")
        graphWriter.PutVertexData(entityType, cleanDF)
        logger.info(s"已添加 $entityType 到GraphWriter")
      }

      // 6. 写入GraphAr格式
      logger.info("开始写入GraphAr格式...")
      graphWriter.write(outputPath, spark, "static_test", 256L, 256L, "parquet")

      // 7. 验证输出
      val vertexPath = s"$outputPath/vertex"
      val vertexDir = new java.io.File(vertexPath)
      if (vertexDir.exists() && vertexDir.isDirectory) {
        val subdirs = vertexDir.listFiles().filter(_.isDirectory)
        logger.info(s"生成的顶点目录:")
        subdirs.foreach(dir => logger.info(s"  - ${dir.getName}"))

        // 读取并显示每种实体的记录数
        subdirs.foreach { dir =>
          val entityType = dir.getName
          try {
            val df = spark.read.parquet(s"$vertexPath/$entityType")
            logger.info(s"$entityType 实体记录数: ${df.count()}")
          } catch {
            case e: Exception =>
              logger.warn(s"无法读取 $entityType: ${e.getMessage}")
          }
        }
      } else {
        logger.error("未找到vertex目录")
      }

      logger.info("静态实体测试完成!")

    } catch {
      case e: Exception =>
        logger.error("静态实体测试失败", e)
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}