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
/** LDBC配置工具类 */
object LdbcConfigUtils {

  /** 创建简化复杂度的优化版LDBC配置 */
  def createOptimizedConfig(scaleFactor: String): GeneratorConfiguration = {
    val configMap = new java.util.HashMap[String, String]()

    // 加载默认参数
    val defaultParams = getClass.getResourceAsStream("/params_default.ini")
    if (defaultParams != null) {
      configMap.putAll(ConfigParser.readConfig(defaultParams))
      defaultParams.close()
    }

    // 加载规模因子配置
    configMap.putAll(ConfigParser.scaleFactorConf(scaleFactor))
    
    // 优化配置以加快处理速度
    configMap.put("generator.numYears", "1")              // 减少数据年份至1年
    configMap.put("generator.maxNumFriends", "20")        // 限制最大好友数
    configMap.put("generator.maxNumComments", "10")       // 减少评论数量
    configMap.put("generator.maxNumPostPerMonth", "10")   // 减少帖子数量
    configMap.put("generator.maxNumLike", "100")          // 减少点赞数量
    configMap.put("generator.blockSize", "1000")          // 合理的块大小
    configMap.put("hadoop.numThreads", "4")               // 使用多线程

    new GeneratorConfiguration(configMap)
  }

  /** 创建生产环境配置，生成真实LDBC数据（包含帖子、评论、点赞等） */
  def createFastTestConfig(scaleFactor: String): GeneratorConfiguration = {
    val configMap = new java.util.HashMap[String, String]()

    // 加载默认参数
    val defaultParams = getClass.getResourceAsStream("/params_default.ini")
    if (defaultParams != null) {
      configMap.putAll(ConfigParser.readConfig(defaultParams))
      defaultParams.close()
    }

    // 加载规模因子配置（使用传入的scaleFactor参数）
    configMap.putAll(ConfigParser.scaleFactorConf(scaleFactor))

    // 真实LDBC数据生成配置
    configMap.put("generator.numYears", "3")              // 3年的真实数据
    configMap.put("generator.maxNumFriends", "50")        // 真实的好友数量
    configMap.put("generator.maxNumComments", "100")      // 启用评论生成
    configMap.put("generator.maxNumPostPerMonth", "20")   // 启用帖子生成
    configMap.put("generator.maxNumLike", "100")          // 启用点赞生成
    configMap.put("generator.blockSize", "1000")          // 合理的块大小
    configMap.put("hadoop.numThreads", "4")               // 多线程提升性能

    new GeneratorConfiguration(configMap)
  }

  /** 验证LDBC配置有效性 */
  def validateConfig(config: GeneratorConfiguration): Boolean = {
    try {
      val numPersons = config.get("generator.numPersons")
      numPersons != null && numPersons.toInt > 0
    } catch {
      case _: Exception => false
    }
  }
}