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

/**
 * LDBC Configuration utilities
 */
object LdbcConfigUtils {

  /**
   * Create optimized LDBC configuration with reduced complexity
   */
  def createOptimizedConfig(scaleFactor: String): GeneratorConfiguration = {
    val configMap = new java.util.HashMap[String, String]()

    // Load default parameters
    val defaultParams = getClass.getResourceAsStream("/params_default.ini")
    if (defaultParams != null) {
      configMap.putAll(ConfigParser.readConfig(defaultParams))
      defaultParams.close()
    }

    // Load scale factor configuration
    configMap.putAll(ConfigParser.scaleFactorConf(scaleFactor))

    // Optimizations for faster processing
    configMap.put("generator.numYears", "1")              // Reduce to 1 year of data
    configMap.put("generator.maxNumFriends", "20")        // Limit max friends
    configMap.put("generator.maxNumComments", "10")       // Reduce comments
    configMap.put("generator.maxNumPostPerMonth", "10")   // Reduce posts
    configMap.put("generator.maxNumLike", "100")          // Reduce likes
    configMap.put("generator.blockSize", "1000")          // Reasonable block size
    configMap.put("hadoop.numThreads", "4")               // Use multiple threads

    new GeneratorConfiguration(configMap)
  }

  /**
   * Create ultra-fast configuration for testing (minimal data)
   */
  def createFastTestConfig(scaleFactor: String): GeneratorConfiguration = {
    val configMap = new java.util.HashMap[String, String]()

    // Load default parameters
    val defaultParams = getClass.getResourceAsStream("/params_default.ini")
    if (defaultParams != null) {
      configMap.putAll(ConfigParser.readConfig(defaultParams))
      defaultParams.close()
    }

    // Load scale factor configuration (use the provided scaleFactor parameter)
    configMap.putAll(ConfigParser.scaleFactorConf(scaleFactor))

    // Minimal configuration for fast testing (override some settings for speed)
    configMap.put("generator.numYears", "1")              // 1 year data
    configMap.put("generator.maxNumFriends", "5")         // Max 5 friends
    configMap.put("generator.maxNumComments", "0")        // No comments
    configMap.put("generator.maxNumPostPerMonth", "0")    // No posts
    configMap.put("generator.maxNumLike", "0")            // No likes
    configMap.put("generator.blockSize", "100")           // Small blocks
    configMap.put("hadoop.numThreads", "1")               // Single thread

    new GeneratorConfiguration(configMap)
  }

  /**
   * Validate LDBC configuration
   */
  def validateConfig(config: GeneratorConfiguration): Boolean = {
    try {
      val numPersons = config.get("generator.numPersons")
      numPersons != null && numPersons.toInt > 0
    } catch {
      case _: Exception => false
    }
  }
}