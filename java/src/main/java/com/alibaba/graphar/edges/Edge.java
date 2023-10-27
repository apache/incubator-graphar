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

import static com.alibaba.graphar.util.CppClassName.GAR_EDGE;
import static com.alibaba.graphar.util.CppClassName.GAR_ID_TYPE;
import static com.alibaba.graphar.util.CppClassName.STD_STRING;
import static com.alibaba.graphar.util.CppHeaderName.GAR_GRAPH_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFISkip;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphar.readers.arrowchunk.AdjListArrowChunkReader;
import com.alibaba.graphar.readers.arrowchunk.AdjListPropertyArrowChunkReader;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.stdcxx.StdVector;
import com.alibaba.graphar.util.Result;

/** Edge contains information of certain edge. */
@FFIGen
@FFITypeAlias(GAR_EDGE)
@CXXHead(GAR_GRAPH_H)
public interface Edge extends CXXPointer {

  Factory factory = FFITypeFactory.getFactory(Edge.class);

  /**
   * Get source id of the edge.
   *
   * @return The id of the source vertex.
   */
  @FFITypeAlias(GAR_ID_TYPE)
  long source();

  /**
   * Get destination id of the edge.
   *
   * @return The id of the destination vertex.
   */
  @FFITypeAlias(GAR_ID_TYPE)
  long destination();

  /**
   * Get the value for a property of the current vertex.
   *
   * @param property StdString that describe property.
   * @param tObject An object that instance of the return type. Supporting types:StdString, Long
   *     <p>e.g.<br>
   *     StdString name = StdString.create("name");<br>
   *     StdString nameProperty = vertexIter.property(name, name);
   *     <p>If you don't want to create an object, cast `Xxx` class to `XxxGen` and call this method
   *     with `(ReturnType) null`.<br>
   *     e.g.<br>
   *     StdString nameProperty = ((VertexIterGen)vertexIter).property(StdString.create("name"),
   *     (StdString) null);
   * @return Result: The property value or error.
   */
  @CXXTemplate(cxx = STD_STRING, java = "com.alibaba.graphar.stdcxx.StdString")
  @CXXTemplate(cxx = "int64_t", java = "java.lang.Long")
  @CXXValue
  <T> Result<T> property(@CXXReference StdString property, @FFISkip T tObject);

  interface Factory {
    /**
     * Initialize the Edge.
     *
     * @param adjListReader The reader for reading the adjList.
     * @param propertyReaders A set of readers for reading the edge properties.
     */
    Edge create(
        @CXXReference AdjListArrowChunkReader adjListReader,
        @CXXReference StdVector<AdjListPropertyArrowChunkReader> propertyReaders);
  }
}
