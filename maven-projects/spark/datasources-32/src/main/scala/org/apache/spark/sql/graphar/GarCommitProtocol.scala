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
// https://github.com/apache/spark/blob/1d550c4/core/src/main/scala/org/apache/spark/internal/io/HadoopMapReduceCommitProtocol.scala

package org.apache.spark.sql.graphar

import org.apache.graphar.GeneralParams
import org.apache.hadoop.mapreduce._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol
import org.json4s._
import org.json4s.jackson.JsonMethods._

object GarCommitProtocol {
  private def binarySearchPair(aggNums: Array[Int], key: Int): (Int, Int) = {
    var low = 0
    var high = aggNums.length - 1
    var mid = 0
    while (low <= high) {
      mid = (high + low) / 2;
      if (
        aggNums(mid) <= key && (mid == aggNums.length - 1 || aggNums(
          mid + 1
        ) > key)
      ) {
        return (mid, key - aggNums(mid))
      } else if (aggNums(mid) > key) {
        high = mid - 1
      } else {
        low = mid + 1
      }
    }
    return (low, key - aggNums(low))
  }
}

class GarCommitProtocol(
    jobId: String,
    path: String,
    options: Map[String, String],
    dynamicPartitionOverwrite: Boolean = false
) extends SQLHadoopMapReduceCommitProtocol(
      jobId,
      path,
      dynamicPartitionOverwrite
    )
    with Serializable
    with Logging {

  // override getFilename to customize the file name
  override def getFilename(
      taskContext: TaskAttemptContext,
      ext: String
  ): String = {
    val partitionId = taskContext.getTaskAttemptID.getTaskID.getId
    if (options.contains(GeneralParams.offsetStartChunkIndexKey)) {
      // offset chunk file name, looks like chunk0
      val chunk_index = options
        .get(GeneralParams.offsetStartChunkIndexKey)
        .get
        .toInt + partitionId
      return f"chunk$chunk_index"
    }
    if (options.contains(GeneralParams.aggNumListOfEdgeChunkKey)) {
      // edge chunk file name, looks like part0/chunk0
      val jValue = parse(
        options.get(GeneralParams.aggNumListOfEdgeChunkKey).get
      )
      implicit val formats =
        DefaultFormats // initialize a default formats for json4s
      val aggNums: Array[Int] = Extraction.extract[Array[Int]](jValue)
      val chunkPair: (Int, Int) =
        GarCommitProtocol.binarySearchPair(aggNums, partitionId)
      val vertex_chunk_index: Int = chunkPair._1
      val edge_chunk_index: Int = chunkPair._2
      return f"part$vertex_chunk_index/chunk$edge_chunk_index"
    }
    // vertex chunk file name, looks like chunk0
    return f"chunk$partitionId"
  }
}
