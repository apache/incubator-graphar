/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.graphar.util

import org.json4s._
import org.json4s.jackson.Serialization.write

import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import org.apache.graphar.GeneralParams

/** Helper object to write DataFrame to chunk files */
object FileSystem {

  /**
   * Write input DataFrame to output path with certain file format.
   *
   * @param dataFrame
   *   DataFrame to write out.
   * @param fileType
   *   output file format type, the value could be csv|parquet|orc.
   * @param outputPrefix
   *   output path prefix.
   * @param offsetStartChunkIndex[Optional]
   *   the start index of offset chunk, if not empty, that means writing a
   *   offset DataFrame.
   * @param aggNumListOfEdgeChunk[Optional]
   *   the aggregated number list of edge chunk, if not empty, that means
   *   writing a edge DataFrame.
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

    // write offset chunks DataFrame
    if (!offsetStartChunkIndex.isEmpty) {
      return dataFrame.write
        .mode("append")
        .option("header", "true")
        .option("fileFormat", fileType)
        .option(
          GeneralParams.offsetStartChunkIndexKey,
          offsetStartChunkIndex.get
        )
        .format("org.apache.graphar.datasources.GarDataSource")
        .save(outputPrefix)
    }
    // write edge chunks DataFrame
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
        .format("org.apache.graphar.datasources.GarDataSource")
        .save(outputPrefix)
    }
    // write vertex chunks DataFrame
    dataFrame.write
      .mode("append")
      .option("header", "true")
      .option("fileFormat", fileType)
      .format("org.apache.graphar.datasources.GarDataSource")
      .save(outputPrefix)
  }

  /**
   * Write input value to output path.
   *
   * @param value
   *   Value to write out.
   * @param outputPrefix
   *   output path prefix.
   * @param hadoopConfig
   *   hadoop configuration.
   */
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
    fs.close()
  }

  /**
   * Read a value from input path.
   *
   * @param inputPath
   *   Input path.
   * @param hadoopConfig
   *   hadoop configuration.
   * @return
   *   The value read from input path.
   */
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
