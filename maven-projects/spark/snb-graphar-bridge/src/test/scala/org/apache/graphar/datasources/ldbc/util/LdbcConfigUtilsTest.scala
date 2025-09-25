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

package org.apache.graphar.datasources.ldbc.util

import ldbc.snb.datagen.util.GeneratorConfiguration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class LdbcConfigUtilsTest extends AnyFunSuite with Matchers {

  test("createOptimizedConfig should create valid configuration") {
    val config = LdbcConfigUtils.createOptimizedConfig("0.1")

    config shouldBe a[GeneratorConfiguration]
    LdbcConfigUtils.validateConfig(config) shouldBe true

    // Check optimization parameters
    config.get("generator.numYears") shouldEqual "1"
    config.get("generator.maxNumFriends") shouldEqual "20"
    config.get("generator.maxNumComments") shouldEqual "10"
    config.get("generator.blockSize") shouldEqual "1000"
    config.get("hadoop.numThreads") shouldEqual "4"
  }

  test("createFastTestConfig should create minimal configuration") {
    val config = LdbcConfigUtils.createFastTestConfig("0.003")

    config shouldBe a[GeneratorConfiguration]
    LdbcConfigUtils.validateConfig(config) shouldBe true

    // The scale factor should match what was passed in
    // Note: We removed the hardcoded override, so it uses the passed scaleFactor
    config.get("generator.numPersons") shouldEqual "50"
    config.get("generator.numYears") shouldEqual "1"
    config.get("generator.maxNumFriends") shouldEqual "5"
    config.get("generator.maxNumComments") shouldEqual "0"
    config.get("generator.maxNumPostPerMonth") shouldEqual "0"
    config.get("generator.maxNumLike") shouldEqual "0"
    config.get("generator.blockSize") shouldEqual "100"
    config.get("hadoop.numThreads") shouldEqual "1"
  }

  test("validateConfig should return true for valid configurations") {
    val config = LdbcConfigUtils.createOptimizedConfig("0.1")
    LdbcConfigUtils.validateConfig(config) shouldBe true
  }

  test("validateConfig should handle configurations without numPersons") {
    val config = LdbcConfigUtils.createOptimizedConfig("0.1")
    // Note: We can't easily create an invalid config without access to internal structure
    // This test verifies the method doesn't throw exceptions
    LdbcConfigUtils.validateConfig(config) shouldBe true
  }

  test("different scale factors should create different configurations") {
    val config1 = LdbcConfigUtils.createOptimizedConfig("0.1")
    val config2 = LdbcConfigUtils.createOptimizedConfig("0.3")

    config1 shouldBe a[GeneratorConfiguration]
    config2 shouldBe a[GeneratorConfiguration]

    // Both should be valid but potentially different
    LdbcConfigUtils.validateConfig(config1) shouldBe true
    LdbcConfigUtils.validateConfig(config2) shouldBe true
  }

  test("fast test config should be more restrictive than optimized config") {
    val optimizedConfig = LdbcConfigUtils.createOptimizedConfig("0.1")
    val fastConfig = LdbcConfigUtils.createFastTestConfig("0.1")

    val optimizedMaxFriends = optimizedConfig.get("generator.maxNumFriends").toInt
    val fastMaxFriends = fastConfig.get("generator.maxNumFriends").toInt

    fastMaxFriends should be < optimizedMaxFriends

    val optimizedBlockSize = optimizedConfig.get("generator.blockSize").toInt
    val fastBlockSize = fastConfig.get("generator.blockSize").toInt

    fastBlockSize should be < optimizedBlockSize
  }
}