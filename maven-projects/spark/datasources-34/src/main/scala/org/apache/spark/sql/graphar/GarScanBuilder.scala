/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Derived from Apache Spark 3.3.4
// https://github.com/apache/spark/blob/18db204/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileScanBuilder.scala

package org.apache.spark.sql.graphar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScanBuilder
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScanBuilder
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/** GarScanBuilder is a class to build the file scan for GarDataSource. */
case class GarScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap,
    formatName: String
) extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {
  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }

  private var filters: Array[Filter] = Array.empty

  override def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = {
    this.filters = dataFilters
    formatName match {
      case "csv"     => Array.empty[Filter]
      case "json"    => Array.empty[Filter]
      case "orc"     => pushedOrcFilters
      case "parquet" => pushedParquetFilters
      case _ =>
        throw new IllegalArgumentException("Invalid format name: " + formatName)
    }
  }

  private lazy val pushedParquetFilters: Array[Filter] = {
    if (!sparkSession.sessionState.conf.parquetFilterPushDown) {
      Array.empty[Filter]
    } else {
      val builder =
        ParquetScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
      builder.pushDataFilters(this.filters)
    }
  }

  private lazy val pushedOrcFilters: Array[Filter] = {
    if (!sparkSession.sessionState.conf.orcFilterPushDown) {
      Array.empty[Filter]
    } else {
      val builder =
        OrcScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
      builder.pushDataFilters(this.filters)
    }
  }

  // Check if the file format supports nested schema pruning.
  override protected val supportsNestedSchemaPruning: Boolean =
    formatName match {
      case "csv"  => false
      case "json" => false
      case "orc"  => sparkSession.sessionState.conf.nestedSchemaPruningEnabled
      case "parquet" =>
        sparkSession.sessionState.conf.nestedSchemaPruningEnabled
      case _ =>
        throw new IllegalArgumentException("Invalid format name: " + formatName)
    }

  /** Build the file scan for GarDataSource. */
  override def build(): Scan = {
    GarScan(
      sparkSession,
      hadoopConf,
      fileIndex,
      dataSchema,
      readDataSchema(),
      readPartitionSchema(),
      pushedDataFilters,
      options,
      formatName
    )
  }
}
