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

package org.apache.graphar.datasources.ldbc.converter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.graphar.datasources.ldbc.model.ValidationResult

/**
 * Base trait for LDBC data converters
 *
 * @tparam T LDBC native data type
 * @tparam R Intermediate data model type
 */
trait LdbcDataConverter[T, R] extends Serializable {

  /**
   * Convert LDBC native data type to GraphAr compatible DataFrame
   *
   * @param rdd LDBC generated native RDD
   * @param spark SparkSession instance
   * @return DataFrame that meets GraphAr format requirements
   */
  def convert(rdd: RDD[T])(implicit spark: SparkSession): DataFrame

  /**
   * Get converted GraphAr schema
   *
   * @return GraphAr compatible StructType
   */
  def getGraphArSchema(): StructType

  /**
   * Validate conversion result integrity
   *
   * @param original Original RDD
   * @param converted Converted DataFrame
   * @return Validation result
   */
  def validateConversion(original: RDD[T], converted: DataFrame): ValidationResult = {
    val originalCount = original.count()
    val convertedCount = converted.count()

    if (originalCount != convertedCount) {
      ValidationResult.failure(s"Record count mismatch: original $originalCount, converted $convertedCount")
    } else {
      ValidationResult.success()
    }
  }
}