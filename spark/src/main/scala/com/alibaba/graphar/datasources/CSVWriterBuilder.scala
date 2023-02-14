package com.alibaba.graphar.datasources

import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.execution.datasources.csv.CsvOutputWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}

class CSVWriteBuilder(
                       paths: Seq[String],
                       formatName: String,
                       supportsDataType: DataType => Boolean,
                       info: LogicalWriteInfo)
  extends GarWriteBuilder(paths, formatName, supportsDataType, info) {
  override def prepareWrite(
                             sqlConf: SQLConf,
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
      override def newInstance(
                                path: String,
                                dataSchema: StructType,
                                context: TaskAttemptContext): OutputWriter = {
        new CsvOutputWriter(path, dataSchema, context, csvOptions)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".csv" + CodecStreams.getCompressionExtension(context)
      }
    }
  }
}