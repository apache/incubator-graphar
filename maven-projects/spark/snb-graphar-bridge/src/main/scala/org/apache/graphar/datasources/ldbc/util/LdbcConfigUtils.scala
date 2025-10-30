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

import ldbc.snb.datagen.util.{ConfigParser, GeneratorConfiguration}

/** LDBCConfiguretoolClass */
object LdbcConfigUtils {

  /** Create simplified complexity optimized LDBC configuration */
  def createOptimizedConfig(scaleFactor: String): GeneratorConfiguration = {
    val configMap = new java.util.HashMap[String, String]()

    // LoaddefaultParameter
    val defaultParams = getClass.getResourceAsStream("/params_default.ini")
    if (defaultParams != null) {
      configMap.putAll(ConfigParser.readConfig(defaultParams))
      defaultParams.close()
    }

    // Load scale factor configuration
    configMap.putAll(ConfigParser.scaleFactorConf(scaleFactor))

    // Optimize configuration to speed up processing
    configMap.put("generator.numYears", "1") // Reduce data years to 1 year
    configMap.put("generator.maxNumFriends", "20") // Limit maximum friend count
    configMap.put("generator.maxNumComments", "10") // Reduce comment count
    configMap.put("generator.maxNumPostPerMonth", "10") // Reduce post count
    configMap.put("generator.maxNumLike", "100") // Reduce like count
    configMap.put("generator.blockSize", "1000") // Reasonable chunk size
    configMap.put("hadoop.numThreads", "4") // Use multiple threads

    new GeneratorConfiguration(configMap)
  }

  /**
   * Create production environment configuration, generate realistic LDBC data
   * (including posts, comments, likes, etc)
   */
  def createFastTestConfig(scaleFactor: String): GeneratorConfiguration = {
    val configMap = new java.util.HashMap[String, String]()

    // LoaddefaultParameter
    val defaultParams = getClass.getResourceAsStream("/params_default.ini")
    if (defaultParams != null) {
      configMap.putAll(ConfigParser.readConfig(defaultParams))
      defaultParams.close()
    }

    // Load scale factor configuration (use passed scale factor parameter)
    configMap.putAll(ConfigParser.scaleFactorConf(scaleFactor))

    // Realistic LDBC data generation configuration
    configMap.put("generator.numYears", "3") // 3 years of realistic data
    configMap.put("generator.maxNumFriends", "50") // Realistic friend count
    configMap.put(
      "generator.maxNumComments",
      "100"
    ) // Enable comment generation
    configMap.put(
      "generator.maxNumPostPerMonth",
      "20"
    ) // Enable post generation
    configMap.put("generator.maxNumLike", "100") // Enable like generation
    configMap.put("generator.blockSize", "1000") // Reasonable chunk size
    configMap.put(
      "hadoop.numThreads",
      "4"
    ) // Multi-threading improves performance

    new GeneratorConfiguration(configMap)
  }

  /** Validate LDBC configuration validity */
  def validateConfig(config: GeneratorConfiguration): Boolean = {
    try {
      val numPersons = config.get("generator.numPersons")
      numPersons != null && numPersons.toInt > 0
    } catch {
      case _: Exception => false
    }
  }
}
