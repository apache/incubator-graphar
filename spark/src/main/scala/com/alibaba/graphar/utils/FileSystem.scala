package com.alibaba.graphar.utils

import java.net.URI
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs

object FileSystem {
  private def renameSparkGeneratedFiles(spark: SparkSession, filePrefix: String): Unit = {
    val sc = spark.sparkContext
    val file_system = fs.FileSystem.get(new URI(filePrefix), spark.sparkContext.hadoopConfiguration)
    val path_pattern = new fs.Path(filePrefix + "part*")
    val files = file_system.globStatus(path_pattern)
    for (i <- 0 until files.length) {
      val file_name = files(i).getPath.getName
      val new_file_name = "chunk" + i.toString
      file_system.rename(new fs.Path(filePrefix + file_name), new fs.Path(filePrefix + new_file_name))
    }
  }

  def writeDataFrame(dataFrame: DataFrame, fileType: String, outputPrefix: String): Unit = {
    val spark = dataFrame.sparkSession
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    spark.conf.set("parquet.enable.summary-metadata", "false")
    dataFrame.write.mode("overwrite").format(fileType).save(outputPrefix)
    renameSparkGeneratedFiles(spark, outputPrefix)
  }
}