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

import org.apache.spark.Partitioner

/**
 * Partitioner for vertex/edge DataFrame to partition by chunk size.
 *
 * @constructor
 *   create a new chunk partitioner
 * @param partitions
 *   partition num.
 * @param chunk_size
 *   size of vertex or edge chunk.
 */
class ChunkPartitioner(partitions: Int, chunk_size: Long) extends Partitioner {
  require(
    partitions >= 0,
    s"Number of partitions ($partitions) cannot be negative."
  )

  def numPartitions: Int = partitions

  def chunkSize: Long = chunk_size

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _    => (key.asInstanceOf[Long] / chunk_size).toInt
  }

  override def equals(other: Any): Boolean = other match {
    case h: ChunkPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

class EdgeChunkPartitioner(
    partitions: Int,
    eidBeginOfVertexChunks: Array[Long],
    aggEdgeChunkSumOfVertexChunks: Array[Long],
    edgeChunkSize: Int
) extends Partitioner {
  require(
    partitions >= 0,
    s"Number of partitions ($partitions) cannot be negative."
  )

  def numPartitions: Int = partitions

  def chunkSize: Long = edgeChunkSize.toLong

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => {
      val vertexChunkIndex = binarySeach(key.asInstanceOf[Long])
      val edgeChunkIndex = aggEdgeChunkSumOfVertexChunks(vertexChunkIndex)
      val edgeIdBegin = eidBeginOfVertexChunks(vertexChunkIndex)
      edgeChunkIndex.toInt + ((key
        .asInstanceOf[Long] - edgeIdBegin) / chunkSize).toInt
    }
  }

  private def binarySeach(key: Long): Int = {
    var low = 0
    var high = eidBeginOfVertexChunks.length - 1
    var mid = 0
    while (low <= high) {
      mid = (high + low) / 2;
      if (
        eidBeginOfVertexChunks(mid) <= key &&
        (mid == eidBeginOfVertexChunks.length - 1 || eidBeginOfVertexChunks(
          mid + 1
        ) > key)
      ) {
        return mid
      } else if (eidBeginOfVertexChunks(mid) > key) {
        high = mid - 1
      } else {
        low = mid + 1
      }
    }
    return low
  }

  override def equals(other: Any): Boolean = other match {
    case h: EdgeChunkPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}
