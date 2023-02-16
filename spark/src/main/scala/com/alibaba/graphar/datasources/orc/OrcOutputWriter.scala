package com.alibaba.graphar.datasources.orc

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.orc.OrcFile
import org.apache.orc.mapred.{OrcOutputFormat => OrcMapRedOutputFormat, OrcStruct}
import org.apache.orc.mapreduce.{OrcMapreduceRecordWriter, OrcOutputFormat}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.orc.{OrcSerializer, OrcUtils}
import org.apache.spark.sql.types._

class OrcOutputWriter(
                                    val path: String,
                                    dataSchema: StructType,
                                    context: TaskAttemptContext)
  extends OutputWriter {

  private[this] val serializer = new OrcSerializer(dataSchema)

  private val recordWriter = {
    val orcOutputFormat = new OrcOutputFormat[OrcStruct]() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        new Path(path)
      }
    }
    val filename = orcOutputFormat.getDefaultWorkFile(context, ".orc")
    val options = OrcMapRedOutputFormat.buildOptions(context.getConfiguration)
    val writer = OrcFile.createWriter(filename, options)
    val recordWriter = new OrcMapreduceRecordWriter[OrcStruct](writer)
    OrcUtils.addSparkVersionMetadata(writer)
    recordWriter
  }

  override def write(row: InternalRow): Unit = {
    recordWriter.write(NullWritable.get(), serializer.serialize(row))
  }

  override def close(): Unit = {
    recordWriter.close(context)
  }
}
