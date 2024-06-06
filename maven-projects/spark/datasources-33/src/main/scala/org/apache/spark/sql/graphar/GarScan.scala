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
// https://github.com/apache/spark/blob/18db204/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileScan.scala

package org.apache.spark.sql.graphar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions.{ExprUtils, Expression}
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetOptions, ParquetReadSupport, ParquetWriteSupport}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.csv.CSVPartitionReaderFactory
import org.apache.spark.sql.execution.datasources.v2.orc.OrcPartitionReaderFactory
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetPartitionReaderFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/** GarScan is a class to implement the file scan for GarDataSource. */
case class GarScan(
                    sparkSession: SparkSession,
                    hadoopConf: Configuration,
                    fileIndex: PartitioningAwareFileIndex,
                    dataSchema: StructType,
                    readDataSchema: StructType,
                    readPartitionSchema: StructType,
                    pushedFilters: Array[Filter],
                    options: CaseInsensitiveStringMap,
                    formatName: String,
                    partitionFilters: Seq[Expression] = Seq.empty,
                    dataFilters: Seq[Expression] = Seq.empty
                  ) extends FileScan {

  /** The gar format is not splitable. */
  override def isSplitable(path: Path): Boolean = false

  /** Create the reader factory according to the actual file format. */
  override def createReaderFactory(): PartitionReaderFactory =
    formatName match {
      case "csv" => createCSVReaderFactory()
      case "orc" => createOrcReaderFactory()
      case "parquet" => createParquetReaderFactory()
      case _ =>
        throw new IllegalArgumentException("Invalid format name: " + formatName)
    }

  // Create the reader factory for the CSV format.
  private def createCSVReaderFactory(): PartitionReaderFactory = {
    val columnPruning = sparkSession.sessionState.conf.csvColumnPruning &&
      !readDataSchema.exists(
        _.name == sparkSession.sessionState.conf.columnNameOfCorruptRecord
      )

    val parsedOptions: CSVOptions = new CSVOptions(
      options.asScala.toMap,
      columnPruning = columnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord
    )

    // Check a field requirement for corrupt records here to throw an exception in a driver side
    ExprUtils.verifyColumnNameOfCorruptRecord(
      dataSchema,
      parsedOptions.columnNameOfCorruptRecord
    )
    // Don't push any filter which refers to the "virtual" column which cannot present in the input.
    // Such filters will be applied later on the upper layer.
    val actualFilters =
      pushedFilters.filterNot(
        _.references.contains(parsedOptions.columnNameOfCorruptRecord)
      )

    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf =
      sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf)
    )
    // The partition values are already truncated in `FileScan.partitions`.
    // We should use `readPartitionSchema` as the partition schema here.
    CSVPartitionReaderFactory(
      sparkSession.sessionState.conf,
      broadcastedConf,
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      parsedOptions,
      actualFilters
    )
  }

  // Create the reader factory for the Orc format.
  private def createOrcReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf)
    )
    // The partition values are already truncated in `FileScan.partitions`.
    // We should use `readPartitionSchema` as the partition schema here.
    OrcPartitionReaderFactory(
      sqlConf = sparkSession.sessionState.conf,
      broadcastedConf = broadcastedConf,
      dataSchema = dataSchema,
      readDataSchema = readDataSchema,
      partitionSchema = readPartitionSchema,
      filters = pushedFilters,
      aggregation = None
    )
  }

  // Create the reader factory for the Parquet format.
  private def createParquetReaderFactory(): PartitionReaderFactory = {
    val readDataSchemaAsJson = readDataSchema.json
    hadoopConf.set(
      ParquetInputFormat.READ_SUPPORT_CLASS,
      classOf[ParquetReadSupport].getName
    )
    hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      readDataSchemaAsJson
    )
    hadoopConf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, readDataSchemaAsJson)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      sparkSession.sessionState.conf.sessionLocalTimeZone
    )
    hadoopConf.setBoolean(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      sparkSession.sessionState.conf.nestedSchemaPruningEnabled
    )
    hadoopConf.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      sparkSession.sessionState.conf.caseSensitiveAnalysis
    )

    ParquetWriteSupport.setSchema(readDataSchema, hadoopConf)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      sparkSession.sessionState.conf.isParquetBinaryAsString
    )
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp
    )
    hadoopConf.setBoolean(
      SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key,
      sparkSession.sessionState.conf.legacyParquetNanosAsLong
    )
    hadoopConf.setBoolean(
      SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key,
      sparkSession.sessionState.conf.parquetFieldIdReadEnabled
    )

    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf)
    )
    val sqlConf = sparkSession.sessionState.conf
    ParquetPartitionReaderFactory(
      sqlConf = sqlConf,
      broadcastedConf = broadcastedConf,
      dataSchema = dataSchema,
      readDataSchema = readDataSchema,
      partitionSchema = readPartitionSchema,
      filters = pushedFilters,
      aggregation = None,
      new ParquetOptions(options.asCaseSensitiveMap.asScala.toMap, sqlConf)
    )
  }

  /**
   * Override "partitions" of
   * org.apache.spark.sql.execution.datasources.v2.FileScan to disable splitting
   * and sort the files by file paths instead of by file sizes. Note: This
   * implementation does not support to partition attributes.
   */
  override protected def partitions: Seq[FilePartition] = {
    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)
    val maxSplitBytes =
      FilePartition.maxSplitBytes(sparkSession, selectedPartitions)

    val splitFiles = selectedPartitions.flatMap { partition =>
      val partitionValues = partition.values
      partition.files
        .flatMap { file =>
          val filePath = file.getPath
          PartitionedFileUtil.splitFiles(
            sparkSession = sparkSession,
            file = file,
            filePath = filePath,
            isSplitable = isSplitable(filePath),
            maxSplitBytes = maxSplitBytes,
            partitionValues = partitionValues
          )
        }
        .toArray
        .sortBy(_.filePath)
    }

    getFilePartitions(sparkSession, splitFiles)
  }

  /**
   * Override "getFilePartitions" of
   * org.apache.spark.sql.execution.datasources.FilePartition to assign each
   * chunk file in GraphAr to a single partition.
   */
  private def getFilePartitions(
                                 sparkSession: SparkSession,
                                 partitionedFiles: Seq[PartitionedFile]
                               ): Seq[FilePartition] = {
    val partitions = new ArrayBuffer[FilePartition]
    val currentFiles = new ArrayBuffer[PartitionedFile]

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        // Copy to a new Array.
        val newPartition = FilePartition(partitions.size, currentFiles.toArray)
        partitions += newPartition
      }
      currentFiles.clear()
    }
    // Assign a file to each partition
    partitionedFiles.foreach { file =>
      closePartition()
      // Add the given file to the current partition.
      currentFiles += file
    }
    closePartition()
    partitions.toSeq
  }

  /** Check if two objects are equal. */
  override def equals(obj: Any): Boolean = obj match {
    case g: GarScan =>
      super.equals(g) && dataSchema == g.dataSchema && options == g.options &&
        equivalentFilters(
          pushedFilters,
          g.pushedFilters
        ) && formatName == g.formatName
    case _ => false
  }

  /** Get the hash code of the object. */
  override def hashCode(): Int = formatName match {
    case "csv" => super.hashCode()
    case "orc" => getClass.hashCode()
    case "parquet" => getClass.hashCode()
    case _ =>
      throw new IllegalArgumentException("Invalid format name: " + formatName)
  }

  /** Get the description string of the object. */
  override def description(): String = {
    super.description() + ", PushedFilters: " + seqToString(pushedFilters)
  }

  /** Get the meta data map of the object. */
  override def getMetaData(): Map[String, String] = {
    super.getMetaData() ++ Map("PushedFilters" -> seqToString(pushedFilters))
  }

  /** Construct the file scan with filters. */
  def withFilters(
                   partitionFilters: Seq[Expression],
                   dataFilters: Seq[Expression]
                 ): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
}
