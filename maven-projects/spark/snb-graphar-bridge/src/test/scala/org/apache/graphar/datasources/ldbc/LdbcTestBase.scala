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

package org.apache.graphar.datasources.ldbc

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import java.nio.file.Files

/**
 * Base test class for LDBC to GraphAr bridge tests
 *
 * Provides common test infrastructure including:
 * - Spark session setup with LDBC configuration
 * - Test data directory management
 * - Resource cleanup
 */
trait LdbcTestBase extends AnyFunSuite with BeforeAndAfterAll {

  /** Spark session for testing (shared across all tests in a suite) */
  var spark: SparkSession = _

  /**
   * Initialize Spark session before running tests
   * Configured for local execution with LDBC SF0.003 scale factor
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .appName("LDBC GraphAr Bridge Test")
      .master("local[2]")
      .config("ldbc.scale.factor", "0.003")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }

  /**
   * Clean up Spark session after all tests complete
   */
  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.stop()
      }
    } finally {
      super.afterAll()
    }
  }

  /**
   * Create a temporary directory for test output
   * @return Absolute path to the temporary directory
   */
  def createTempDir(): String = {
    Files.createTempDirectory("graphar_test_").toString
  }
}
