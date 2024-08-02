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
// https://github.com/apache/spark/blob/1d550c4/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/orc/ORCWriteBuilder.scala

package org.apache.spark.sql.graphar.orc

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.orc.OrcConf.{COMPRESS, MAPRED_OUTPUT_SCHEMA}
import org.apache.orc.mapred.OrcStruct
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.execution.datasources.orc.{OrcOptions, OrcUtils}
import org.apache.spark.sql.execution.datasources.{
  OutputWriter,
  OutputWriterFactory
}
import org.apache.spark.sql.graphar.GarWriteBuilder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

object OrcWriteBuilder {
  // the getQuotedSchemaString method of spark OrcFileFormat
  private def getQuotedSchemaString(dataType: DataType): String =
    dataType match {
      case StructType(fields) =>
        fields
          .map(f => s"`${f.name}`:${getQuotedSchemaString(f.dataType)}")
          .mkString("struct<", ",", ">")
      case ArrayType(elementType, _) =>
        s"array<${getQuotedSchemaString(elementType)}>"
      case MapType(keyType, valueType, _) =>
        s"map<${getQuotedSchemaString(keyType)},${getQuotedSchemaString(valueType)}>"
      case _ => // UDT and others
        dataType.catalogString
    }
}

class OrcWriteBuilder(
    paths: Seq[String],
    formatName: String,
    supportsDataType: DataType => Boolean,
    info: LogicalWriteInfo
) extends GarWriteBuilder(paths, formatName, supportsDataType, info) {

  override def prepareWrite(
      sqlConf: SQLConf,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType
  ): OutputWriterFactory = {
    val orcOptions = new OrcOptions(options, sqlConf)

    val conf = job.getConfiguration

    conf.set(
      MAPRED_OUTPUT_SCHEMA.getAttribute,
      OrcWriteBuilder.getQuotedSchemaString(dataSchema)
    )

    conf.set(COMPRESS.getAttribute, orcOptions.compressionCodec)

    conf
      .asInstanceOf[JobConf]
      .setOutputFormat(
        classOf[org.apache.orc.mapred.OrcOutputFormat[OrcStruct]]
      )

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext
      ): OutputWriter = {
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
