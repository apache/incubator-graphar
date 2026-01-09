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
      Option(System.getenv("GAR_TEST_DATA"))
        .orElse(Option(System.getProperty("gar.test.data")))
        .getOrElse {
          val candidates = Seq("../../testing", "../testing", "testing")
          candidates
            .map(p => new java.io.File(p).getAbsoluteFile)
            .find(d =>
              new java.io.File(d, "ldbc_sample/csv/ldbc_sample.graph.yml")
                .exists()
            )
            .map(_.getAbsolutePath)
            .getOrElse(
              throw new IllegalArgumentException("GAR_TEST_DATA not found")
            )
        }
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
