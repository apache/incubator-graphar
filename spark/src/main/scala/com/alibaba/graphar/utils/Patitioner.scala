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

/** Partitioner for vertex/edge DataFrame to partition by chunk size.
 *
 * @constructor create a new chunk partitioner
 * @param partitions partition num.
 * @chunk_size size of vertex or edge chunk.
 *
 */
class ChunkPartitioner(partitions: Int, chunk_size: Long) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def chunkSize: Long = chunk_size

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => (key.asInstanceOf[Long] / chunk_size).toInt
  }

  override def equals(other: Any): Boolean = other match {
    case h: ChunkPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}
