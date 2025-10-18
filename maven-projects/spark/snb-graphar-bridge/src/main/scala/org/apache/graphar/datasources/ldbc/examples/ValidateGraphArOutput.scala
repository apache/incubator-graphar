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
import org.apache.graphar.datasources.ldbc.validator.DataIntegrityValidator
import org.slf4j.{Logger, LoggerFactory}

/**
 * Validation runner for GraphAr output.
 *
 * This program validates the integrity of GraphAr output files generated
 * from LDBC SNB data, checking:
 * - Edge reference integrity (no dangling edges)
 * - ID mapping consistency (university/company ID ranges)
 * - GraphAr index column correctness
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass="org.apache.graphar.datasources.ldbc.examples.ValidateGraphArOutput" \
 *     -Dexec.args="/tmp/graphar_output"
 */
object ValidateGraphArOutput {

  private val logger: Logger = LoggerFactory.getLogger(ValidateGraphArOutput.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      logger.info("Usage: ValidateGraphArOutput <graphar_output_path>")
      logger.info("Example: ValidateGraphArOutput /tmp/graphar_output")
      System.exit(1)
    }

    val graphArPath = args(0)
    logger.info(s"Validating GraphAr output at: $graphArPath")
    logger.info("")

    // Create Spark session
    val spark = SparkSession.builder()
      .appName("GraphAr Data Integrity Validation")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      // Create validator
      val validator = new DataIntegrityValidator(graphArPath, spark)

      // Print report header
      logger.info("=" * 80)
      logger.info("# GraphAr Data Integrity Validation Report")
      logger.info("=" * 80)
      logger.info("")
      logger.info(s"GraphAr output path: $graphArPath")
      logger.info("")
      logger.info("| Validation Check | Total Records | Invalid Src | Invalid Dst | Status |")
      logger.info("|------------------|---------------|-------------|-------------|--------|")

      // Run all validation checks
      val reports = validator.validateAll()

      // Print each report
      reports.foreach { report =>
        logger.info(report.toMarkdown)
      }

      // Print summary
      val passCount = reports.count(_.passed)
      val totalCount = reports.length

      logger.info("")
      logger.info("=" * 80)
      logger.info(s"## Summary: $passCount/$totalCount checks passed")
      logger.info("=" * 80)
      logger.info("")

      if (passCount == totalCount) {
        logger.info("✓ All validation checks passed!")
        logger.info("")
        logger.info("Data quality assessment:")
        logger.info("- Edge reference integrity: 100% ✅")
        logger.info("- ID mapping consistency: 100% ✅")
        logger.info("- GraphAr index columns: 100% ✅")
        logger.info("")
        System.exit(0)
      } else {
        val failCount = totalCount - passCount
        logger.info(s"❌ Found $failCount validation failures.")
        logger.info("")
        logger.info("Failed checks:")
        reports.filter(!_.passed).foreach { report =>
          logger.info(s"- ${report.edgeName}")
          report.errorMessage.foreach(msg => logger.info(s"  Error: $msg"))
        }
        logger.info("")
        logger.info("Recommendation: Please check the data generation process, fix the issues above and re-validate.")
        logger.info("")
        System.exit(1)
      }
    } catch {
      case e: Exception =>
        logger.info("")
        logger.info("=" * 80)
        logger.info("❌ Error occurred during validation")
        logger.info("=" * 80)
        logger.info("")
        logger.info(s"Error message: ${e.getMessage}")
        e.printStackTrace()
        logger.info("")
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
}
