/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.edges;

import com.alibaba.graphar.graphinfo.GraphInfo;
import com.alibaba.graphar.types.AdjListType;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.graphar.graphinfo.EdgeInfo;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.util.Result;

/** EdgesCollection is designed for reading a collection of edges. */
public interface EdgesCollection extends CXXPointer {
  /**
   * Construct the collection for a range of edges.
   *
   * @param graphInfo The GraphInfo for the graph.
   * @param srcLabel The source vertex label.
   * @param edgeLabel The edge label.
   * @param dstLabel The destination vertex label.
   * @param adjListType The adjList type.
   * @param vertexChunkBegin The index of the beginning vertex chunk.
   * @param vertexChunkEnd The index of the end vertex chunk (not included).
   * @return The constructed collection or error.
   */
  static EdgesCollection create(
      final GraphInfo graphInfo,
      String srcLabel,
      String edgeLabel,
      String dstLabel,
      AdjListType adjListType,
      long vertexChunkBegin,
      long vertexChunkEnd) {
    Result<EdgeInfo> maybeEdgeInfo =
        graphInfo.getEdgeInfo(
            StdString.create(srcLabel), StdString.create(edgeLabel), StdString.create(dstLabel));
    if (maybeEdgeInfo.hasError()) {
      throw new RuntimeException(
          "graphInfo to create EdgesCollection has error: "
              + maybeEdgeInfo.status().message().toJavaString());
    }
    EdgeInfo edgeInfo = maybeEdgeInfo.value();
    if (!edgeInfo.containAdjList(adjListType)) {
      throw new RuntimeException(
          "The edge " + edgeLabel + " of adj list type " + adjListType + " doesn't exist.");
    }
    switch (adjListType) {
      case ordered_by_source:
        return EdgesCollectionOrderedBySource.factory.create(
            edgeInfo, graphInfo.getPrefix(), vertexChunkBegin, vertexChunkEnd);
      case ordered_by_dest:
        return EdgesCollectionOrderedByDest.factory.create(
            edgeInfo, graphInfo.getPrefix(), vertexChunkBegin, vertexChunkEnd);
      case unordered_by_source:
        return EdgesCollectionUnorderedBySource.factory.create(
            edgeInfo, graphInfo.getPrefix(), vertexChunkBegin, vertexChunkEnd);
      case unordered_by_dest:
        return EdgesCollectionUnorderedByDest.factory.create(
            edgeInfo, graphInfo.getPrefix(), vertexChunkBegin, vertexChunkEnd);
    }
    throw new RuntimeException("Unknown adj list type: " + adjListType);
  }

  /**
   * Construct the collection for a range of edges.
   *
   * @param graphInfo The GraphInfo for the graph.
   * @param srcLabel The source vertex label.
   * @param edgeLabel The edge label.
   * @param dstLabel The destination vertex label.
   * @param adjListType The adjList type.
   * @return The constructed collection or error.
   */
  static EdgesCollection create(
      final GraphInfo graphInfo,
      String srcLabel,
      String edgeLabel,
      String dstLabel,
      AdjListType adjListType) {
    return create(graphInfo, srcLabel, edgeLabel, dstLabel, adjListType, 0, Long.MAX_VALUE);
  }

  /** The iterator pointing to the first edge. */
  EdgeIter begin();

  /** The iterator pointing to the past-the-end element. */
  EdgeIter end();

  /**
   * Construct and return the iterator pointing to the first out-going edge of the vertex with
   * specific id after the input iterator.
   *
   * @param id The vertex id.
   * @param from The input iterator.
   * @return The new constructed iterator.
   */
  @FFINameAlias("find_src")
  EdgeIter findSrc(long id, EdgeIter from);

  /**
   * Construct and return the iterator pointing to the first incoming edge of the vertex with
   * specific id after the input iterator.
   *
   * @param id The vertex id.
   * @param from The input iterator.
   * @return The new constructed iterator.
   */
  @FFINameAlias("find_dst")
  EdgeIter findDst(long id, EdgeIter from);

  /** Get the number of edges in the collection. */
  long size();
}
