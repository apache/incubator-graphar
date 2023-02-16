package com.alibaba.graphar.datasources

import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.execution.datasources.csv.CsvOutputWriter
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, WriteBuilder, SupportsOverwrite, SupportsTruncate}
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, DataSource, OutputWriterFactory, WriteJobDescription}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.sql.execution.datasources.v2.FileBatchWrite
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.sources.Filter

class GarWriteBuilder(paths: Seq[String],
                      formatName: String,
                      supportsDataType: DataType => Boolean,
                      info: LogicalWriteInfo) extends WriteBuilder with SupportsOverwrite

  with SupportsTruncate {
  private val schema = info.schema()
  private val queryId = info.queryId()
  private val options = info.options()

  override def buildForBatch(): BatchWrite = {
    val sparkSession = SparkSession.active
    validateInputs(sparkSession.sessionState.conf.caseSensitiveAnalysis)
    val path = new Path(paths.head)
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val job = getJobInstance(hadoopConf, path)
    val committer = new GarCommitProtocol(java.util.UUID.randomUUID().toString, paths.head, options.asScala.toMap, false)
    lazy val description =
      createWriteJobDescription(sparkSession, hadoopConf, job, paths.head, options.asScala.toMap)

    committer.setupJob(job)
    new FileBatchWrite(job, description, committer)
  }

  def prepareWrite(sqlConf: SQLConf,
                   job: Job,
                   options: Map[String, String],
                   dataSchema: StructType): OutputWriterFactory = {
    val conf = job.getConfiguration
    val csvOptions = new CSVOptions(
      options,
      columnPruning = sqlConf.csvColumnPruning,
      sqlConf.sessionLocalTimeZone)
    csvOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }

    new OutputWriterFactory {
      override def newInstance(path: String,
                               dataSchema: StructType,
                               context: TaskAttemptContext): OutputWriter = {
        new CsvOutputWriter(path, dataSchema, context, csvOptions)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".csv" + CodecStreams.getCompressionExtension(context)
      }
    }
  }

  private def validateInputs(caseSensitiveAnalysis: Boolean): Unit = {
    assert(schema != null, "Missing input data schema")
    assert(queryId != null, "Missing query ID")

    if (paths.length != 1) {
      throw new IllegalArgumentException("Expected exactly one path to be specified, but " +
        s"got: ${paths.mkString(", ")}")
    }
    val pathName = paths.head
    // SchemaUtils.checkColumnNameDuplication(schema.fields.map(_.name),
    //   s"when inserting into $pathName", caseSensitiveAnalysis)
    DataSource.validateSchema(schema)

    // schema.foreach { field =>
    //   if (!supportsDataType(field.dataType)) {
    //     throw new AnalysisException(
    //       s"$formatName data source does not support ${field.dataType.catalogString} data type.")
    //   }
    // }
  }

  private def getJobInstance(hadoopConf: Configuration, path: Path): Job = {
    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    FileOutputFormat.setOutputPath(job, path)
    job
  }

  private def createWriteJobDescription(sparkSession: SparkSession,
                                        hadoopConf: Configuration,
                                        job: Job,
                                        pathName: String,
                                        options: Map[String, String]): WriteJobDescription = {
    val caseInsensitiveOptions = CaseInsensitiveMap(options)
    // Note: prepareWrite has side effect. It sets "job".
    val outputWriterFactory =
      prepareWrite(sparkSession.sessionState.conf, job, caseInsensitiveOptions, schema)
    // val allColumns = schema.toAttributes
    val allColumns: Seq[AttributeReference] = GarHadoopWriterUtils.schemaToAttributes(schema)
    val metrics: Map[String, SQLMetric] = BasicWriteJobStatsTracker.metrics
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    val statsTracker = new BasicWriteJobStatsTracker(serializableHadoopConf, metrics)
    // TODO: after partitioning is supported in V2:
    //       1. filter out partition columns in `dataColumns`.
    //       2. Don't use Seq.empty for `partitionColumns`.
    new WriteJobDescription(
      uuid = UUID.randomUUID().toString,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = allColumns,
      dataColumns = allColumns,
      partitionColumns = Seq.empty,
      bucketIdExpression = None,
      path = pathName,
      customPartitionLocations = Map.empty,
      maxRecordsPerFile = caseInsensitiveOptions.get("maxRecordsPerFile").map(_.toLong)
        .getOrElse(sparkSession.sessionState.conf.maxRecordsPerFile),
      timeZoneId = caseInsensitiveOptions.get(DateTimeUtils.TIMEZONE_OPTION)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone),
      statsTrackers = Seq(statsTracker)
    )
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    new GarWriteBuilder(paths, formatName, supportsDataType, info)
  }

  override def truncate(): WriteBuilder = {
    new GarWriteBuilder(paths, formatName, supportsDataType, info)
  }
}