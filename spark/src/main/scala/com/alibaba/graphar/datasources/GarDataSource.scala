/** Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.datasources

import org.apache.spark.sql.execution.datasources.v2.parquet._

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class GarDataSource extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]

  override def shortName(): String = "gar"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    println(options.get("fileFormat"))
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    GarTable(tableName, sparkSession, optionsWithoutPaths, paths, None, getFallbackFileFormat(options))
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    GarTable(
      tableName, sparkSession, optionsWithoutPaths, paths, Some(schema),  getFallbackFileFormat(options))
  }

  private def getFileTypeInString(options: CaseInsensitiveStringMap) = {
    options.get("fileFormat")
  }

  private def getFallbackFileFormat(options: CaseInsensitiveStringMap): Class[_ <: FileFormat] = getFileTypeInString(options) match {
      case "csv"=> classOf[CSVFileFormat]
      case "orc" => classOf[OrcFileFormat]
      case "parquet" => classOf[ParquetFileFormat]
      case _ => throw new IllegalArgumentException
  }

}