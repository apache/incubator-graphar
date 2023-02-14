package com.alibaba.graphar.datasources

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import scala.util.{DynamicVariable, Random}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{JobConf, JobID}

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.expressions.AttributeReference

/**
 * A helper object that provide common utils used during saving an RDD using a Hadoop OutputFormat
 * (both from the old mapred API and the new mapreduce API)
 */
object GarHadoopWriterUtils {

  private val RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES = 256
  private val RAND = new Random()

  /**
   * Create a job ID.
   *
   * @param time (current) time
   * @param id job number
   * @return a job ID
   */
  def createJobID(time: Date, id: Int): JobID = {
    if (id < 0) {
      throw new IllegalArgumentException("Job number is negative")
    }
    val jobtrackerID = createJobTrackerID(time)
    new JobID(jobtrackerID, id)
  }

  /**
   * Generate an ID for a job tracker.
   * @param time (current) time
   * @return a string for a job ID
   */
  def createJobTrackerID(time: Date): String = {
    val base = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(time)
    var l1 = RAND.nextLong()
    if (l1 < 0) {
      l1 = -l1
    }
    base + l1
  }

  def createPathFromString(path: String, conf: JobConf): Path = {
    if (path == null) {
      throw new IllegalArgumentException("Output path is null")
    }
    val outputPath = new Path(path)
    val fs = outputPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException("Incorrectly formatted output path")
    }
    outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }

  def schemaToAttributes(schema: StructType): Seq[AttributeReference] = {
    schema.map( f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
    // scema.fieldNames.map( name => AttributeReference(name, ))
  }
}