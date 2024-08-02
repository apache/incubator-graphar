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
    if (System.getenv("GAR_TEST_DATA") == null) {
      throw new IllegalArgumentException("GAR_TEST_DATA is not set")
    }
    testData = System.getenv("GAR_TEST_DATA")
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
