package com.alibaba.graphar.datasources.orc

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.orc.OrcConf.{COMPRESS, MAPRED_OUTPUT_SCHEMA}
import org.apache.orc.mapred.OrcStruct

import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.execution.datasources.orc.{OrcOptions, OrcUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import com.alibaba.graphar.datasources.GarWriteBuilder

object OrcWriteBuilder {
  // the getQuotedSchemaString method of spark OrcFileFormat
  private def getQuotedSchemaString(dataType: DataType): String = dataType match {
    case StructType(fields) =>
      fields.map(f => s"`${f.name}`:${getQuotedSchemaString(f.dataType)}")
        .mkString("struct<", ",", ">")
    case ArrayType(elementType, _) =>
      s"array<${getQuotedSchemaString(elementType)}>"
    case MapType(keyType, valueType, _) =>
      s"map<${getQuotedSchemaString(keyType)},${getQuotedSchemaString(valueType)}>"
    case _ => // UDT and others
      dataType.catalogString
  }
}

class OrcWriteBuilder(paths: Seq[String],
                      formatName: String,
                      supportsDataType: DataType => Boolean,
                      info: LogicalWriteInfo)
  extends GarWriteBuilder(paths, formatName, supportsDataType, info) {

  override def prepareWrite(sqlConf: SQLConf,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    val orcOptions = new OrcOptions(options, sqlConf)

    val conf = job.getConfiguration

    conf.set(MAPRED_OUTPUT_SCHEMA.getAttribute, OrcWriteBuilder.getQuotedSchemaString(dataSchema))

    conf.set(COMPRESS.getAttribute, orcOptions.compressionCodec)

    conf.asInstanceOf[JobConf]
      .setOutputFormat(classOf[org.apache.orc.mapred.OrcOutputFormat[OrcStruct]])

    new OutputWriterFactory {
      override def newInstance(
                                path: String,
                                dataSchema: StructType,
                                context: TaskAttemptContext): OutputWriter = {
        new OrcOutputWriter(path, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        val compressionExtension: String = {
          val name = context.getConfiguration.get(COMPRESS.getAttribute)
          OrcUtils.extensionsForCompressionCodecNames.getOrElse(name, "")
        }

        compressionExtension + ".orc"
      }
    }
  }
}
