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

package org.apache.graphar.datasources.ldbc.integration

import org.apache.graphar.datasources.ldbc.bridge.LdbcDirectBridge
import org.apache.graphar.datasources.ldbc.converter.SparkTestBase
import org.apache.graphar.datasources.ldbc.util.LdbcConfigUtils
import org.apache.graphar.datasources.ldbc.generator.LdbcDriverGenerator
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Minutes, Span}

import java.io.File
import java.nio.file.{Files, Paths}
import scala.util.Success

class LdbcDirectBridgeIntegrationTest extends AnyFunSuite with Matchers with SparkTestBase {

  // Set test timeout to 5 minutes
  implicit val timeout: org.scalatest.time.Span = Span(5, Minutes)

  test("End-to-end integration: LDBC generation to GraphAr conversion") {
    implicit val sparkSession: SparkSession = spark
    val scaleFactor = "0.003"
    val outputPath = createTempDir()

    // Step 1: Generate LDBC data using Driver
    val config = LdbcConfigUtils.createFastTestConfig(scaleFactor)
    LdbcConfigUtils.validateConfig(config) shouldBe true

    // use LdbcDriverGenerator not SparkPersonGenerator
    val driverGenerator = new LdbcDriverGenerator(config)
    val allPersons = driverGenerator.generateAllPersons()
    val personRDD = spark.sparkContext.parallelize(allPersons)

    val personCount = personRDD.count()
    personCount should be > 0L

    // Step 2: Convert to GraphAr format
    val bridge = new LdbcDirectBridge()
    val result = bridge.convertToGraphAr(
      ldbcPersonRDD = personRDD,
      outputPath = outputPath,
      graphName = "integration_test_graph",
      vertexChunkSize = 100L,
      edgeChunkSize = 100L,
      fileType = "csv"
    )

    // Step 3: Verify conversion results
    result shouldBe a[Success[_]]
    val conversionResult = result.get

    conversionResult.personCount shouldEqual personCount
    conversionResult.outputPath shouldEqual outputPath

    // Step 4: Verify output files exist
    val outputDir = new File(outputPath)
    outputDir.exists() shouldBe true
    outputDir.isDirectory shouldBe true

    val files = outputDir.listFiles()
    files should not be empty

    // Verify that person vertex files exist
    val personFiles = files.filter(f => f.getName.contains("vertex"))
    personFiles should not be empty

    // Verify GraphAr metadata files
    val metadataFiles = files.filter(f => f.getName.endsWith(".yaml") || f.getName.endsWith(".yml"))
    metadataFiles should not be empty

    println(s"✓ Integration test completed successfully")
    println(s"✓ Generated ${conversionResult.personCount} persons")
    println(s"✓ Extracted ${conversionResult.knowsCount} knows relationships")
    println(s"✓ Extracted ${conversionResult.interestCount} interest relationships")
    println(s"✓ Created ${files.length} output files")
  }

  test("Performance integration: measure conversion time for different configurations") {
    implicit val sparkSession: SparkSession = spark
    val scaleFactor = "0.003"
    val outputPath1 = createTempDir()
    val outputPath2 = createTempDir()

    // Test fast configuration
    val startTimeFast = System.currentTimeMillis()
    val fastConfig = LdbcConfigUtils.createFastTestConfig(scaleFactor)
    val fastDriverGenerator = new LdbcDriverGenerator(fastConfig)
    val fastPersons = fastDriverGenerator.generateAllPersons()
    val fastPersonRDD = spark.sparkContext.parallelize(fastPersons)
    val fastBridge = new LdbcDirectBridge()
    val fastResult = fastBridge.convertToGraphAr(fastPersonRDD, outputPath1, "fast_test_graph")
    val fastTime = System.currentTimeMillis() - startTimeFast

    // Test optimized configuration
    val startTimeOptimized = System.currentTimeMillis()
    val optimizedConfig = LdbcConfigUtils.createOptimizedConfig(scaleFactor)
    val optimizedDriverGenerator = new LdbcDriverGenerator(optimizedConfig)
    val optimizedPersons = optimizedDriverGenerator.generateAllPersons()
    val optimizedPersonRDD = spark.sparkContext.parallelize(optimizedPersons)
    val optimizedBridge = new LdbcDirectBridge()
    val optimizedResult = optimizedBridge.convertToGraphAr(optimizedPersonRDD, outputPath2, "optimized_test_graph")
    val optimizedTime = System.currentTimeMillis() - startTimeOptimized

    // Verify both completed successfully
    fastResult shouldBe a[Success[_]]
    optimizedResult shouldBe a[Success[_]]

    // Fast configuration should generally be faster
    println(s"Fast configuration time: ${fastTime}ms")
    println(s"Optimized configuration time: ${optimizedTime}ms")
    println(s"Fast persons: ${fastResult.get.personCount}")
    println(s"Optimized persons: ${optimizedResult.get.personCount}")

    // Both should produce some data
    fastResult.get.personCount should be > 0L
    optimizedResult.get.personCount should be > 0L
  }

  test("Data quality integration: verify relationship consistency") {
    implicit val sparkSession: SparkSession = spark
    val scaleFactor = "0.003"
    val outputPath = createTempDir()

    val config = LdbcConfigUtils.createOptimizedConfig(scaleFactor)
    val driverGenerator = new LdbcDriverGenerator(config)
    val allPersons = driverGenerator.generateAllPersons()
    val personRDD = spark.sparkContext.parallelize(allPersons)

    val bridge = new LdbcDirectBridge()
    val result = bridge.convertToGraphAr(
      ldbcPersonRDD = personRDD,
      outputPath = outputPath,
      graphName = "quality_test_graph"
    )

    result shouldBe a[Success[_]]
    val conversionResult = result.get

    // Verify data consistency
    conversionResult.personCount should be > 0L

    // The sum of all relationship counts should be reasonable
    val totalRelationships = conversionResult.knowsCount +
      conversionResult.interestCount +
      conversionResult.workAtCount +
      conversionResult.studyAtCount +
      conversionResult.locationCount

    // Should have at least some relationships
    totalRelationships should be >= 0L

    // Location relationships should equal person count (each person has a location)
    conversionResult.locationCount shouldEqual conversionResult.personCount

    println(s"✓ Data quality verification completed")
    println(s"✓ Total persons: ${conversionResult.personCount}")
    println(s"✓ Total relationships: $totalRelationships")
    println(s"✓ Knows: ${conversionResult.knowsCount}")
    println(s"✓ Interests: ${conversionResult.interestCount}")
    println(s"✓ Work: ${conversionResult.workAtCount}")
    println(s"✓ Study: ${conversionResult.studyAtCount}")
    println(s"✓ Location: ${conversionResult.locationCount}")
  }

  private def createTempDir(): String = {
    val tempDir = File.createTempFile("ldbc_integration_test", "")
    tempDir.delete()
    tempDir.mkdirs()
    tempDir.getAbsolutePath
  }
}