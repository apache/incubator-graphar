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

import static com.alibaba.graphar.util.CppClassName.GAR_EDGES_COLLECTION;
import static com.alibaba.graphar.util.CppHeaderName.GAR_GRAPH_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import java.util.Iterator;

/** EdgesCollection is designed for reading a collection of edges. */
@FFIGen
@FFITypeAlias(GAR_EDGES_COLLECTION)
@CXXHead(GAR_GRAPH_H)
public interface EdgesCollection extends CXXPointer, Iterable<Edge> {
    /** The iterator pointing to the first edge. */
    @CXXValue
    EdgeIter begin();

    /** The iterator pointing to the past-the-end element. */
    @CXXValue
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
    @CXXValue
    EdgeIter findSrc(long id, @CXXReference EdgeIter from);

    /**
     * Construct and return the iterator pointing to the first incoming edge of the vertex with
     * specific id after the input iterator.
     *
     * @param id The vertex id.
     * @param from The input iterator.
     * @return The new constructed iterator.
     */
    @FFINameAlias("find_dst")
    @CXXValue
    EdgeIter findDst(long id, @CXXReference EdgeIter from);

    /** Get the number of edges in the collection. */
    long size();

    /** Implement Iterable interface to support for-each loop. */
    default Iterator<Edge> iterator() {
        return new Iterator<Edge>() {
            EdgeIter current = begin();
            EdgeIter end = end();

            @Override
            public boolean hasNext() {
                return !current.isEnd();
            }

            @Override
            public Edge next() {
                Edge ret = current.get();
                current.inc();
                return ret;
            }
        };
    }
}
