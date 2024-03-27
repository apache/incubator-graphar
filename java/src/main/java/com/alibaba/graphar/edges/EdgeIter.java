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

/*
 * Copyright 2022-2023 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.edges;

import static com.alibaba.graphar.util.CppClassName.GAR_EDGE_ITER;
import static com.alibaba.graphar.util.CppClassName.GAR_ID_TYPE;
import static com.alibaba.graphar.util.CppClassName.STD_STRING;
import static com.alibaba.graphar.util.CppHeaderName.GAR_GRAPH_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISkip;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphar.graphinfo.EdgeInfo;
import com.alibaba.graphar.stdcxx.StdSharedPtr;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.types.AdjListType;
import com.alibaba.graphar.util.IndexConverter;
import com.alibaba.graphar.util.Result;

/** The iterator for traversing a type of edges. */
@FFIGen
@FFITypeAlias(GAR_EDGE_ITER)
@CXXHead(GAR_GRAPH_H)
public interface EdgeIter extends CXXPointer {

    /** Construct and return the edge of the current offset. */
    @CXXOperator("*")
    @CXXValue
    Edge get();

    /** Get the source vertex id for the current edge. */
    @FFITypeAlias(GAR_ID_TYPE)
    long source();

    /** Get the destination vertex id for the current edge. */
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
     *     <p>If you don't want to create an object, cast `Xxx` class to `XxxGen` and call this
     *     method with `(ReturnType) null`.<br>
     *     e.g.<br>
     *     StdString nameProperty = ((VertexIterGen)vertexIter).property(StdString.create("name"),
     *     (StdString) null);
     * @return Result: The property value or error.
     */
    @CXXTemplate(cxx = STD_STRING, java = "com.alibaba.graphar.stdcxx.StdString")
    @CXXTemplate(cxx = "int64_t", java = "java.lang.Long")
    @CXXValue
    <T> Result<T> property(@CXXReference StdString property, @FFISkip T tObject);

    /** The prefix increment operator. */
    @CXXOperator("++")
    @CXXReference
    EdgeIter inc();

    /** The equality operator. */
    @CXXOperator("==")
    boolean eq(@CXXReference EdgeIter rhs);

    /** Get the global index of the current edge chunk. */
    @FFINameAlias("global_chunk_index")
    @FFITypeAlias(GAR_ID_TYPE)
    long globalChunkIndex();

    /** Get the current offset in the current chunk. */
    @FFINameAlias("cur_offset")
    @FFITypeAlias(GAR_ID_TYPE)
    long curOffset();

    /**
     * Let the input iterator to point to the first out-going edge of the vertex with specific id
     * after the current position of the iterator.
     *
     * @param from The input iterator.
     * @param id The vertex id.
     * @return If such edge is found or not.
     */
    @FFINameAlias("first_src")
    boolean firstSrc(@CXXReference EdgeIter from, @FFITypeAlias(GAR_ID_TYPE) long id);

    /**
     * Let the input iterator to point to the first incoming edge of the vertex with specific id
     * after the current position of the iterator.
     *
     * @param from The input iterator.
     * @param id The vertex id.
     * @return If such edge is found or not.
     */
    @FFINameAlias("first_dst")
    boolean firstDst(@CXXReference EdgeIter from, @FFITypeAlias(GAR_ID_TYPE) long id);

    /** Let the iterator to point to the beginning. */
    @FFINameAlias("to_begin")
    void toBegin();

    /** Check if the current position is the end. */
    @FFINameAlias("is_end")
    boolean isEnd();

    /** Point to the next edge with the same source, return false if not found. */
    @FFINameAlias("next_src")
    boolean nextSrc();

    /** Point to the next edge with the same destination, return false if not found. */
    @FFINameAlias("next_dst")
    boolean nextDst();

    /** Point to the next edge with the specific source, return false if not found. */
    @FFINameAlias("next_src")
    boolean nextSrc(@FFITypeAlias(GAR_ID_TYPE) long id);

    /** Point to the next edge with the specific destination, return false if not found. */
    @FFINameAlias("next_dst")
    boolean nextDst(@FFITypeAlias(GAR_ID_TYPE) long id);

    @FFIFactory
    interface Factory {
        /**
         * Initialize the iterator.
         *
         * @param edgeInfo The edge info that describes the edge type.
         * @param prefix The absolute prefix.
         * @param adjListType The type of adjList.
         * @param globalChunkIndex The global index of the current edge chunk.
         * @param offset The current offset in the current edge chunk.
         * @param chunkBegin The index of the first chunk.
         * @param chunkEnd The index of the last chunk.
         * @param indexConverter The converter for transforming the edge chunk indices.
         */
        EdgeIter create(
                @CXXReference EdgeInfo edgeInfo,
                @CXXReference StdString prefix,
                @CXXValue AdjListType adjListType,
                @FFITypeAlias(GAR_ID_TYPE) long globalChunkIndex,
                @FFITypeAlias(GAR_ID_TYPE) long offset,
                @FFITypeAlias(GAR_ID_TYPE) long chunkEnd,
                @FFITypeAlias(GAR_ID_TYPE) long chunkBegin,
                @CXXValue StdSharedPtr<IndexConverter> indexConverter);

        /** Copy constructor. */
        EdgeIter create(@CXXReference EdgeIter other);
    }
}
