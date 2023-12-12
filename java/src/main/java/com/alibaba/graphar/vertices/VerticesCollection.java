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


package com.alibaba.graphar.vertices;

import static com.alibaba.graphar.util.CppClassName.GAR_ID_TYPE;
import static com.alibaba.graphar.util.CppClassName.GAR_VERTICES_COLLECTION;
import static com.alibaba.graphar.util.CppHeaderName.GAR_GRAPH_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFITypeAlias;
import java.util.Iterator;

/** VerticesCollection is designed for reading a collection of vertices. */
@FFIGen
@FFITypeAlias(GAR_VERTICES_COLLECTION)
@CXXHead(GAR_GRAPH_H)
public interface VerticesCollection extends CXXPointer, Iterable<Vertex> {

    /** The iterator pointing to the first vertex. */
    @CXXValue
    VertexIter begin();

    /** The iterator pointing to the past-the-end element. */
    @CXXValue
    VertexIter end();

    /** The iterator pointing to the vertex with specific id. */
    @CXXValue
    VertexIter find(@FFITypeAlias(GAR_ID_TYPE) long id);

    long size();

    /** Implement Iterable interface to support for-each loop. */
    default Iterator<Vertex> iterator() {
        return new Iterator<Vertex>() {
            VertexIter current = begin();
            VertexIter end = end();

            @Override
            public boolean hasNext() {
                return !current.eq(end);
            }

            @Override
            public Vertex next() {
                Vertex ret = current.get();
                current.inc();
                return ret;
            }
        };
    }
}
