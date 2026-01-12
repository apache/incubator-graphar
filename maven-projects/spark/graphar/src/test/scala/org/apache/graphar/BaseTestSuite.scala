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

package org.apache.graphar

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

abstract class BaseTestSuite extends AnyFunSuite with BeforeAndAfterAll {

  var testData: String = _
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    def resolveTestData(): String = {
      var testDataPath: String =
        Option(System.getenv("GAR_TEST_DATA"))
          .orElse(Option(System.getProperty("gar.test.data")))
          .orNull

      if (testDataPath == null) {
        val candidates = Seq("../../testing", "../testing", "testing")
        candidates.foreach { p =>
          val dir = new java.io.File(p).getAbsoluteFile
          val marker =
            new java.io.File(dir, "ldbc_sample/csv/ldbc_sample.graph.yml")
          if (dir.exists() && dir.isDirectory && marker.isFile) {
            testDataPath = dir.getAbsolutePath
            return testDataPath
          }
        }
      }

      if (testDataPath != null) {
        val dir = new java.io.File(testDataPath)
        val marker =
          new java.io.File(dir, "ldbc_sample/csv/ldbc_sample.graph.yml")
        if (dir.exists() && dir.isDirectory && marker.isFile) {
          return testDataPath
        }
      }

      throw new RuntimeException(
        "GAR_TEST_DATA not found or invalid. " +
          "Please set GAR_TEST_DATA environment variable to point to the testing directory " +
          "or ensure the testing directory exists with ldbc_sample/csv/ldbc_sample.graph.yml"
      )
    }

    testData = resolveTestData()
    spark = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    // spark.stop()
    super.afterAll()
  }
}
