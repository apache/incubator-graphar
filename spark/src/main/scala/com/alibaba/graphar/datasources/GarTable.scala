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

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.csv.CSVDataSource
import org.apache.spark.sql.execution.datasources.orc.OrcUtils
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.execution.datasources.v2.csv.CSVWrite
import org.apache.spark.sql.execution.datasources.v2.orc.OrcWrite
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetWrite
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class GarTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  override def newScanBuilder(options: CaseInsensitiveStringMap): GarScanBuilder =
    new GarScanBuilder(sparkSession, fileIndex, schema, dataSchema, options, formatName)

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = formatName match {
    case "csv" => {
      val parsedOptions = new CSVOptions(
      options.asScala.toMap,
      columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone)

      CSVDataSource(parsedOptions).inferSchema(sparkSession, files, parsedOptions)
    }
    case "orc" => OrcUtils.inferSchema(sparkSession, files, options.asScala.toMap)
    case "parquet" => ParquetUtils.inferSchema(sparkSession, options.asScala.toMap, files)
    case _ => throw new IllegalArgumentException
  }
    
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = formatName match {
    case "csv" => new WriteBuilder {
      override def build(): Write = CSVWrite(paths, formatName, supportsDataType, info)
    }
    case "orc" => new WriteBuilder {
      override def build(): Write = OrcWrite(paths, formatName, supportsDataType, info)
    }
    case "parquet" => new WriteBuilder {
      override def build(): Write = ParquetWrite(paths, formatName, supportsDataType, info)
    }
    case _ => throw new IllegalArgumentException
  }

  override def supportsDataType(dataType: DataType): Boolean = dataType match {
    // case _: AnsiIntervalType => false

    case _: AtomicType => true

    // case st: StructType => st.forall { f => supportsDataType(f.dataType) }

    // case ArrayType(elementType, _) => supportsDataType(elementType)

    // case MapType(keyType, valueType, _) =>
    //   supportsDataType(keyType) && supportsDataType(valueType)

    // case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

    case _ => false
  }

  override def formatName: String = options.get("fileFormat")
}
