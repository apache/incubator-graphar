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

// Derived from Apache Spark 3.1.1
// https://github.com/apache/spark/blob/1d550c4/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileTable.scala

package org.apache.graphar.datasources

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.csv.CSVDataSource
import org.apache.spark.sql.execution.datasources.orc.OrcUtils
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.graphar.datasources.csv.CSVWriteBuilder
import org.apache.graphar.datasources.parquet.ParquetWriteBuilder
import org.apache.graphar.datasources.orc.OrcWriteBuilder

/** GarTable is a class to represent the graph data in GraphAr as a table. */
case class GarTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat]
) extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  /** Construct a new scan builder. */
  override def newScanBuilder(
      options: CaseInsensitiveStringMap
  ): GarScanBuilder =
    new GarScanBuilder(
      sparkSession,
      fileIndex,
      schema,
      dataSchema,
      options,
      formatName
    )

  /**
   * Infer the schema of the table through the methods of the actual file
   * format.
   */
  override def inferSchema(files: Seq[FileStatus]): Option[StructType] =
    formatName match {
      case "csv" => {
        val parsedOptions = new CSVOptions(
          options.asScala.toMap,
          columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
          sparkSession.sessionState.conf.sessionLocalTimeZone
        )

        CSVDataSource(parsedOptions).inferSchema(
          sparkSession,
          files,
          parsedOptions
        )
      }
      case "orc" =>
        OrcUtils.inferSchema(sparkSession, files, options.asScala.toMap)
      case "parquet" =>
        ParquetUtils.inferSchema(sparkSession, options.asScala.toMap, files)
      case _ =>
        throw new IllegalArgumentException("Invalid format name: " + formatName)
    }

  /** Construct a new write builder according to the actual file format. */
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    formatName match {
      case "csv" =>
        new CSVWriteBuilder(paths, formatName, supportsDataType, info)
      case "orc" =>
        new OrcWriteBuilder(paths, formatName, supportsDataType, info)
      case "parquet" =>
        new ParquetWriteBuilder(paths, formatName, supportsDataType, info)
      case _ =>
        throw new IllegalArgumentException("Invalid format name: " + formatName)
    }

  /**
   * Check if a data type is supported. Note: Currently, the GraphAr data source
   * only supports several atomic data types. To support additional data types
   * such as Struct, Array and Map, revise this function to handle them case by
   * case as the commented code shows.
   */
  override def supportsDataType(dataType: DataType): Boolean = dataType match {
    // case _: AnsiIntervalType => false

    case _: AtomicType => true

    // case st: StructType => st.forall { f => supportsDataType(f.dataType) }

    case ArrayType(elementType, _) =>
      formatName match {
        case "orc"     => supportsDataType(elementType)
        case "parquet" => supportsDataType(elementType)
        case _         => false
      }

    // case MapType(keyType, valueType, _) =>
    //   supportsDataType(keyType) && supportsDataType(valueType)

    // case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

    case _ => false
  }

  /** The actual file format for storing the data in GraphAr. */
  override def formatName: String = options.get("fileFormat")
}
