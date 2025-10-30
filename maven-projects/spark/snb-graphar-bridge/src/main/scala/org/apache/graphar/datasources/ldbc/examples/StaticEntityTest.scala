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
import org.apache.graphar.datasources.ldbc.stream.processor.{
  UnifiedIdManager,
  StaticEntityProcessor
}
import org.apache.graphar.datasources.ldbc.stream.core.GraphArDataCollector
import org.apache.graphar.graph.GraphWriter
import org.slf4j.{Logger, LoggerFactory}

/**
 * static entityGeneratetest
 *
 * This test program is used to verify whether StaticEntityProcessor correctly
 * generates static data and use GraphWriter to write GraphAr format
 */
object StaticEntityTest {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val outputPath =
      if (args.length > 0) args(0) else "/tmp/graphar_static_only"

    logger.info(s"Static entity test started, output path: $outputPath")

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("Static Entity Test")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    try {
      // 1. InitializeIDmanager
      val idManager = new UnifiedIdManager(0.003)
      idManager.initialize()

      // 2. Create data collector
      val dataCollector =
        new GraphArDataCollector(outputPath, "static_test", idManager)

      // 3. Generatestatic entity
      val staticProcessor = new StaticEntityProcessor(idManager)
      val staticEntities = staticProcessor.generateAllStaticEntities()

      if (staticEntities.isEmpty) {
        logger.error("Failed to generate any static entity data")
        return
      }

      // 4. Add to data collector
      staticEntities.foreach { case (entityType, df) =>
        val count = df.count()
        logger.info(s"Generated static entity $entityType: $count records")
        if (count > 0) {
          dataCollector.addStaticVertexData(entityType, df)
          // Display first 5 records
          logger.info(s"$entityType sample data:")
          df.show(5, truncate = false)
        }
      }

      // 5. Use GraphWriterWrite
      val graphWriter = new GraphWriter()

      // Add all static vertex data
      dataCollector.getStaticDataFrames().foreach { case (entityType, df) =>
        val cleanDF =
          df.drop("_graphArVertexIndex", "_entityType", "_idSpaceCategory")
        graphWriter.PutVertexData(entityType, cleanDF)
        logger.info(s"Added $entityType to GraphWriter")
      }

      // 6. write GraphAr format
      logger.info("Starting to write to GraphAr format...")
      graphWriter.write(outputPath, spark, "static_test", 256L, 256L, "parquet")

      // 7. Verify output
      val vertexPath = s"$outputPath/vertex"
      val vertexDir = new java.io.File(vertexPath)
      if (vertexDir.exists() && vertexDir.isDirectory) {
        val subdirs = vertexDir.listFiles().filter(_.isDirectory)
        logger.info(s"Generated vertex directories:")
        subdirs.foreach(dir => logger.info(s" - ${dir.getName}"))

        // Read and display record count for each entity
        subdirs.foreach { dir =>
          val entityType = dir.getName
          try {
            val df = spark.read.parquet(s"$vertexPath/$entityType")
            logger.info(s"$entityType entity record count: ${df.count()}")
          } catch {
            case e: Exception =>
              logger.warn(s"Unable to read $entityType: ${e.getMessage}")
          }
        }
      } else {
        logger.error("Vertex directory not found")
      }

      logger.info("Static entity test completed!")

    } catch {
      case e: Exception =>
        logger.error("Static entity test failed", e)
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
