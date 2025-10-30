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

import org.apache.graphar.datasources.ldbc.LdbcTestBase
import org.apache.graphar.datasources.ldbc.bridge.LdbcGraphArBridge
import org.apache.graphar.datasources.ldbc.validator.DataIntegrityValidator

/**
 * Schema validation test suite
 *
 * Tests GraphAr output schema integrity using DataIntegrityValidator
 */
class SchemaValidationTest extends LdbcTestBase {

  test("should pass all data integrity validations") {
    val outputPath = createTempDir()
    val bridge = new LdbcGraphArBridge()

    // Generate GraphAr data
    val result = bridge.write(
      path = outputPath,
      spark = spark,
      name = "ldbc_test",
      vertex_chunk_size = 100,
      edge_chunk_size = 100,
      file_type = "parquet"
    )

    assert(
      result.isSuccess,
      s"Data generation should succeed before validation"
    )

    // Run comprehensive validation
    val validator = new DataIntegrityValidator(outputPath, spark)
    val reports = validator.validateAll()

    // Print validation results
    println("\n" + "=" * 80)
    println("GraphAr Data Integrity Validation Report")
    println("=" * 80)
    println(
      "| Validation Check | Total Records | Invalid Src | Invalid Dst | Status |"
    )
    println(
      "|-----------------|---------------|-------------|-------------|---------|"
    )
    reports.foreach(report => println(report.toMarkdown))
    println("=" * 80)

    // Count failures
    val failures = reports.filterNot(_.passed)
    val totalChecks = reports.size
    val passedChecks = reports.count(_.passed)

    println(s"\nSummary: $passedChecks/$totalChecks checks passed")

    if (failures.nonEmpty) {
      println("\nFailed checks:")
      failures.foreach { report =>
        println(
          s"  ❌ ${report.edgeName}: ${report.errorMessage.getOrElse("Unknown error")}"
        )
      }
    }
    println()

    // Assert all validations passed
    assert(
      failures.isEmpty,
      s"${failures.size} validation checks failed. See output above for details."
    )
  }
}
