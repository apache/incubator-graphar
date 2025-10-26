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

package org.apache.graphar.datasources.ldbc.processor

import org.apache.graphar.datasources.ldbc.LdbcTestBase
import org.apache.graphar.datasources.ldbc.util.LdbcConfigUtils
import ldbc.snb.datagen.generator.generators.SparkPersonGenerator
import ldbc.snb.datagen.generator.DatagenParams
import ldbc.snb.datagen.generator.dictionary.Dictionaries

/**
 * Test suite for Person entity processing
 *
 * Validates LDBC Person generation and processing logic
 */
class PersonProcessorTest extends LdbcTestBase {

  test("should generate correct number of persons for SF0.003") {
    // Create LDBC configuration for SF0.003
    val config = LdbcConfigUtils.createFastTestConfig("0.003")

    // Initialize LDBC parameters and dictionaries
    DatagenParams.readConf(config)
    Dictionaries.loadDictionaries()

    // Generate Person RDD
    val personRDD = SparkPersonGenerator(config)(spark)
    val count = personRDD.count()

    assert(count == 50, s"SF0.003 should generate 50 persons according to LDBC specification, but got $count")
  }
}
