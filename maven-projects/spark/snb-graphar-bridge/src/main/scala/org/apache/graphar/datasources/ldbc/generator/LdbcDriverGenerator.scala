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

package org.apache.graphar.datasources.ldbc.generator

import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.generator.DatagenParams
import ldbc.snb.datagen.generator.generators.PersonGenerator
import ldbc.snb.datagen.util.GeneratorConfiguration
import ldbc.snb.datagen.generator.dictionary.Dictionaries
import ldbc.snb.datagen.generator.vocabulary.SN
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Driver-side single-threaded LDBC Person generator
 *
 * This generator solves the LDBC initialization blocking problem by:
 * 1. Running all initialization on Driver side (single-threaded)
 * 2. Generating all Person entities in Driver memory
 * 3. Then distributing the generated data to Spark RDD
 *
 * This approach completely avoids the distributed initialization competition
 * that causes blocking in SparkPersonGenerator.
 */
class LdbcDriverGenerator(config: GeneratorConfiguration) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[LdbcDriverGenerator])
  private var initialized = false

  /**
   * Generate all persons on Driver side using single-threaded approach
   */
  def generateAllPersons(): List[Person] = {
    ensureInitialized()

    val totalPersons = DatagenParams.numPersons
    val blockSize = DatagenParams.blockSize
    val numBlocks = Math.ceil(totalPersons.toDouble / blockSize).toInt

    logger.info(s"Starting Driver-side generation: $totalPersons persons in $numBlocks blocks")

    val personGenerator = new PersonGenerator(config, config.get("generator.distribution.degreeDistribution"))
    val allPersons = mutable.ListBuffer[Person]()

    for (blockId <- 0 until numBlocks) {
      val currentBlockSize = Math.min(blockSize, totalPersons - blockId * blockSize).toInt
      val personIterator = personGenerator.generatePersonBlock(blockId, currentBlockSize)

      personIterator.asScala.take(currentBlockSize).foreach { person =>
        allPersons += person
      }

      logger.info(s"Generated block $blockId: $currentBlockSize persons (total: ${allPersons.size})")
    }

    logger.info(s"✓ Driver-side generation completed: ${allPersons.size} persons")
    allPersons.toList
  }

  /**
   * Generate persons in batches to avoid Driver memory issues
   */
  def generatePersonsInBatches(batchSize: Int = 10000): Iterator[List[Person]] = {
    ensureInitialized()

    new Iterator[List[Person]] {
      private var currentBlock = 0
      private val totalPersons = DatagenParams.numPersons
      private val totalBlocks = Math.ceil(totalPersons.toDouble / batchSize).toInt
      private val personGenerator = new PersonGenerator(config, config.get("generator.distribution.degreeDistribution"))

      override def hasNext: Boolean = currentBlock < totalBlocks

      override def next(): List[Person] = {
        val currentBlockSize = Math.min(batchSize, totalPersons - currentBlock * batchSize).toInt
        val personIterator = personGenerator.generatePersonBlock(currentBlock, currentBlockSize)
        val batch = personIterator.asScala.take(currentBlockSize).toList
        currentBlock += 1

        logger.info(s"Generated batch ${currentBlock}/$totalBlocks: ${batch.size} persons")
        batch
      }
    }
  }

  /**
   * Critical: Initialize LDBC on Driver side only once
   * This completely avoids the distributed initialization competition
   */
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      logger.info("Starting LDBC initialization on Driver side...")

      val startTime = System.currentTimeMillis()

      // Step 1: Read configuration parameters
      DatagenParams.readConf(config)
      logger.info("✓ DatagenParams configuration loaded")

      // Step 2: Load dictionaries (this is what causes the blocking!)
      logger.info("Loading dictionaries (this may take 1-2 minutes)...")
      Dictionaries.loadDictionaries()
      logger.info("✓ Dictionaries loaded successfully")

      // Step 3: Initialize SN (Social Network) parameters
      SN.initialize()
      logger.info("✓ SN parameters initialized")

      val initTime = System.currentTimeMillis() - startTime
      logger.info(s"✓ Driver-side LDBC initialization completed in ${initTime}ms")

      initialized = true
    }
  }

  /**
   * Get initialization status
   */
  def isInitialized: Boolean = initialized

  /**
   * Get total number of persons to be generated
   */
  def getTotalPersonCount: Long = {
    if (!initialized) ensureInitialized()
    DatagenParams.numPersons
  }
}