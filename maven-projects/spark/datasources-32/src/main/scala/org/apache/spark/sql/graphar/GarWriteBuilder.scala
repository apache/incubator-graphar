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
// https://github.com/apache/spark/blob/1d550c4/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/FileWriteBuilder.scala

package org.apache.spark.sql.graphar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.v2.FileBatchWrite
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, DataSource, OutputWriterFactory, WriteJobDescription}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.SerializableConfiguration

import java.util.UUID
import scala.collection.JavaConverters._

abstract class GarWriteBuilder(
                                paths: Seq[String],
                                formatName: String,
                                supportsDataType: DataType => Boolean,
                                info: LogicalWriteInfo
                              ) extends WriteBuilder {
  private val schema = info.schema()
  private val queryId = info.queryId()
  private val options = info.options()

  override def buildForBatch(): BatchWrite = {
    val sparkSession = SparkSession.active
    validateInputs(sparkSession.sessionState.conf.caseSensitiveAnalysis)
    val path = new Path(paths.head)
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf =
      sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val job = getJobInstance(hadoopConf, path)
    val committer = new GarCommitProtocol(
      java.util.UUID.randomUUID().toString,
      paths.head,
      options.asScala.toMap,
      false
    )
    lazy val description =
      createWriteJobDescription(
        sparkSession,
        hadoopConf,
        job,
        paths.head,
        options.asScala.toMap
      )

    committer.setupJob(job)
    new FileBatchWrite(job, description, committer)
  }

  def prepareWrite(
                    sqlConf: SQLConf,
                    job: Job,
                    options: Map[String, String],
                    dataSchema: StructType
                  ): OutputWriterFactory

  private def validateInputs(caseSensitiveAnalysis: Boolean): Unit = {
    assert(schema != null, "Missing input data schema")
    assert(queryId != null, "Missing query ID")

    if (paths.length != 1) {
      throw new IllegalArgumentException(
        "Expected exactly one path to be specified, but " +
          s"got: ${paths.mkString(", ")}"
      )
    }
    val pathName = paths.head
    DataSource.validateSchema(schema)

    schema.foreach { field =>
      if (!supportsDataType(field.dataType)) {
        throw new IllegalArgumentException(
          s"$formatName data source does not support ${field.dataType.catalogString} data type."
        )
      }
    }
  }

  private def getJobInstance(hadoopConf: Configuration, path: Path): Job = {
    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    FileOutputFormat.setOutputPath(job, path)
    job
  }

  private def createWriteJobDescription(
                                         sparkSession: SparkSession,
                                         hadoopConf: Configuration,
                                         job: Job,
                                         pathName: String,
                                         options: Map[String, String]
                                       ): WriteJobDescription = {
    val caseInsensitiveOptions = CaseInsensitiveMap(options)
    // Note: prepareWrite has side effect. It sets "job".
    val outputWriterFactory =
      prepareWrite(
        sparkSession.sessionState.conf,
        job,
        caseInsensitiveOptions,
        schema
      )
    // same as schema.toAttributes which is private of spark package
    val allColumns: Seq[AttributeReference] = schema.map(f =>
      AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
    )
    val metrics: Map[String, SQLMetric] = BasicWriteJobStatsTracker.metrics
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    val statsTracker =
      new BasicWriteJobStatsTracker(serializableHadoopConf, metrics)
    // TODO: after partitioning is supported in V2:
    //       1. filter out partition columns in `dataColumns`.
    //       2. Don't use Seq.empty for `partitionColumns`.
    new WriteJobDescription(
      uuid = UUID.randomUUID().toString,
      serializableHadoopConf =
        new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = allColumns,
      dataColumns = allColumns,
      partitionColumns = Seq.empty,
      bucketIdExpression = None,
      path = pathName,
      customPartitionLocations = Map.empty,
      maxRecordsPerFile = caseInsensitiveOptions
        .get("maxRecordsPerFile")
        .map(_.toLong)
        .getOrElse(sparkSession.sessionState.conf.maxRecordsPerFile),
      timeZoneId = caseInsensitiveOptions
        .get(DateTimeUtils.TIMEZONE_OPTION)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone),
      statsTrackers = Seq(statsTracker)
    )
  }
}
