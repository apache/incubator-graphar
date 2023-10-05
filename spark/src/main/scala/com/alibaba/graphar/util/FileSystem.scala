/**
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.alibaba.graphar.util

import org.json4s._
import org.json4s.jackson.Serialization.write

import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import com.alibaba.graphar.GeneralParams

/** Helper object to write dataframe to chunk files */
object FileSystem {

  /**
   * Write input dataframe to output path with certain file format.
   *
   * @param dataframe
   *   DataFrame to write out.
   * @param fileType
   *   output file format type, the value could be csv|parquet|orc.
   * @param outputPrefix
   *   output path prefix.
   * @param startChunkIndex
   *   the start index of chunk.
   */
  def writeDataFrame(
      dataFrame: DataFrame,
      fileType: String,
      outputPrefix: String,
      offsetStartChunkIndex: Option[Int],
      aggNumListOfEdgeChunk: Option[Array[Long]]
  ): Unit = {
    val spark = dataFrame.sparkSession
    // TODO: Make the hard-code setting to configurable
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    spark.conf.set("parquet.enable.summary-metadata", "false")
    spark.conf.set("spark.sql.orc.compression.codec", "zstd")
    spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
    // first check the outputPrefix exists, if not, create it
    val path = new Path(outputPrefix)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(path)) {
      fs.mkdirs(path)
    }
    fs.close()

    // write offset chunks dataframe
    if (!offsetStartChunkIndex.isEmpty) {
      return dataFrame.write
        .mode("append")
        .option("header", "true")
        .option("fileFormat", fileType)
        .option(
          GeneralParams.offsetStartChunkIndexKey,
          offsetStartChunkIndex.get
        )
        .format("com.alibaba.graphar.datasources.GarDataSource")
        .save(outputPrefix)
    }
    // write edge chunks dataframe
    if (!aggNumListOfEdgeChunk.isEmpty) {
      implicit val formats =
        DefaultFormats // initialize a default formats for json4s
      return dataFrame.write
        .mode("append")
        .option("header", "true")
        .option("fileFormat", fileType)
        .option(
          GeneralParams.aggNumListOfEdgeChunkKey,
          write(aggNumListOfEdgeChunk.get)
        )
        .format("com.alibaba.graphar.datasources.GarDataSource")
        .save(outputPrefix)
    }
    // write vertex chunks dataframe
    dataFrame.write
      .mode("append")
      .option("header", "true")
      .option("fileFormat", fileType)
      .format("com.alibaba.graphar.datasources.GarDataSource")
      .save(outputPrefix)
  }

  def writeValue(
      value: Long,
      outputPath: String,
      hadoopConfig: Configuration
  ): Unit = {
    val path = new Path(outputPath)
    val fs = path.getFileSystem(hadoopConfig)
    val output = fs.create(path, true) // create or overwrite
    // consistent with c++ library, convert to little-endian
    output.writeLong(java.lang.Long.reverseBytes(value))
    output.close()
  }

  def readValue(inputPath: String, hadoopConfig: Configuration): Long = {
    val path = new Path(inputPath)
    val fs = path.getFileSystem(hadoopConfig)
    val input = fs.open(path)
    // consistent with c++ library, little-endian in file, convert to big-endian
    val num = java.lang.Long.reverseBytes(input.readLong())
    fs.close()
    return num
  }
}
