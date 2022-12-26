/** Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.utils

import org.apache.spark.sql.types._
import org.apache.spark.Partitioner


class VertexChunkPartitioner(partitions: Int, chunk_size: Long) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def chunkSize: Long = chunk_size

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => (key.asInstanceOf[Long] / chunk_size).toInt
  }

  override def equals(other: Any): Boolean = other match {
    case h: VertexChunkPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

class EdgeChunkPartitioner(partitions: Int, start_eids: Array[Long], start_indices: Array[Long], edge_chunk_size: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def chunkSize: Int = edge_chunk_size

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => {
      val vertex_chunk_index = binarySeach(key.asInstanceOf[Long])
      val chunk_start = start_indices(vertex_chunk_index)
      (chunk_start.toInt + Math.floor((key.asInstanceOf[Long] - start_eids(vertex_chunk_index)) / edge_chunk_size.toDouble).toInt)
    }
  }
  private def binarySeach(key: Long): Int = {
    var low = 0
    var high = start_indices.length
    var loop_cond = true
    var mid = 0
    while (low <= high && loop_cond) {
      mid = (high + low) / 2;
      if (start_eids(mid) <= key && start_eids(mid + 1) > key) {
        loop_cond = false
      } else if (start_eids(mid) > key) {
        high = mid - 1
      } else {
        low = mid + 1
      }
    }
    if (low <= high) {
      mid
    } else {
      low
    }
  }

  override def equals(other: Any): Boolean = other match {
    case h: VertexChunkPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}
