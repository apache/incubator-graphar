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

package org.apache.graphar.datasources

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.graphar.GarTable
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._
import scala.util.matching.Regex

// Derived from Apache Spark 3.1.1
// https://github.com/apache/spark/blob/1d550c4/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileDataSourceV2.scala

/**
 * GarDataSource is a class to provide gar files as the data source for spark.
 */
class GarDataSource extends TableProvider with DataSourceRegister {
  private val REDACTION_REPLACEMENT_TEXT = "*********(redacted)"

  /**
   * Redact the sensitive information in the given string.
   */
  // Copy of redact from graphar Utils
  private def redact(regex: Option[Regex], text: String): String = {
    regex match {
      case None => text
      case Some(r) =>
        if (text == null || text.isEmpty) {
          text
        } else {
          r.replaceAllIn(text, REDACTION_REPLACEMENT_TEXT)
        }
    }
  }

  /** The default fallback file format is Parquet. */
  def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]

  lazy val sparkSession = SparkSession.active

  /** The string that represents the format name. */
  override def shortName(): String = "gar"

  protected def getPaths(map: CaseInsensitiveStringMap): Seq[String] = {
    val objectMapper = new ObjectMapper()
    val paths = Option(map.get("paths"))
      .map { pathStr =>
        objectMapper.readValue(pathStr, classOf[Array[String]]).toSeq
      }
      .getOrElse(Seq.empty)
    paths ++ Option(map.get("path")).toSeq
  }

  protected def getOptionsWithoutPaths(
      map: CaseInsensitiveStringMap
  ): CaseInsensitiveStringMap = {
    val withoutPath = map.asCaseSensitiveMap().asScala.filterKeys { k =>
      !k.equalsIgnoreCase("path") && !k.equalsIgnoreCase("paths")
    }
    new CaseInsensitiveStringMap(withoutPath.toMap.asJava)
  }

  protected def getTableName(
      map: CaseInsensitiveStringMap,
      paths: Seq[String]
  ): String = {
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(
      map.asCaseSensitiveMap().asScala.toMap
    )
    val name = shortName() + " " + paths
      .map(qualifiedPathName(_, hadoopConf))
      .mkString(",")
    redact(sparkSession.sessionState.conf.stringRedactionPattern, name)
  }

  private def qualifiedPathName(
      path: String,
      hadoopConf: Configuration
  ): String = {
    val hdfsPath = new Path(path)
    val fs = hdfsPath.getFileSystem(hadoopConf)
    hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toString
  }

  /** Provide a table from the data source. */
  def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    GarTable(
      tableName,
      sparkSession,
      optionsWithoutPaths,
      paths,
      None,
      getFallbackFileFormat(options)
    )
  }

  /** Provide a table from the data source with specific schema. */
  def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    GarTable(
      tableName,
      sparkSession,
      optionsWithoutPaths,
      paths,
      Some(schema),
      getFallbackFileFormat(options)
    )
  }

  override def supportsExternalMetadata(): Boolean = true

  private var t: Table = null

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    if (t == null) t = getTable(options)
    t.schema()
  }

  override def inferPartitioning(
      options: CaseInsensitiveStringMap
  ): Array[Transform] = {
    Array.empty
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    // If the table is already loaded during schema inference, return it directly.
    if (t != null) {
      t
    } else {
      getTable(new CaseInsensitiveStringMap(properties), schema)
    }
  }

  // Get the actual fall back file format.
  private def getFallbackFileFormat(
      options: CaseInsensitiveStringMap
  ): Class[_ <: FileFormat] = options.get("fileFormat") match {
    case "csv"     => classOf[CSVFileFormat]
    case "orc"     => classOf[OrcFileFormat]
    case "parquet" => classOf[ParquetFileFormat]
    case "json"    => classOf[JsonFileFormat]
    case _         => throw new IllegalArgumentException
  }
}
