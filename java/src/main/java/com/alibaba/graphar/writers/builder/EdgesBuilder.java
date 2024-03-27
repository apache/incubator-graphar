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

package com.alibaba.graphar.writers.builder;

import static com.alibaba.graphar.util.CppClassName.GAR_BUILDER_EDGES_BUILDER;
import static com.alibaba.graphar.util.CppClassName.GAR_ID_TYPE;
import static com.alibaba.graphar.util.CppHeaderName.GAR_EDGES_BUILDER_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphar.graphinfo.EdgeInfo;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.types.AdjListType;
import com.alibaba.graphar.types.ValidateLevel;
import com.alibaba.graphar.util.Status;

/** EdgeBuilder is designed for building and writing a collection of edges. */
@FFIGen
@FFITypeAlias(GAR_BUILDER_EDGES_BUILDER)
@CXXHead(GAR_EDGES_BUILDER_H)
public interface EdgesBuilder extends CXXPointer {

    Factory factory = FFITypeFactory.getFactory(EdgesBuilder.class);

    /**
     * Set the validate level.
     *
     * @param validateLevel The validate level to set.
     */
    @FFINameAlias("SetValidateLevel")
    void setValidateLevel(@CXXValue ValidateLevel validateLevel);

    /**
     * Get the validate level.
     *
     * @return The validate level of this writer.
     */
    @FFINameAlias("GetValidateLevel")
    @CXXValue
    ValidateLevel getValidateLevel();

    /** Clear the edges in this EdgessBuilder. */
    @FFINameAlias("Clear")
    void clear();

    /**
     * Add an edge to the collection.
     *
     * <p>The validate_level for this operation could be:
     *
     * <p>ValidateLevel::default_validate: to use the validate_level of the builder, which set
     * through the constructor or the SetValidateLevel method;
     *
     * <p>ValidateLevel::no_validate: without validation;
     *
     * <p>ValidateLevel::weak_validate: to validate if the adj_list type is valid, and the data in
     * builder is not saved;
     *
     * <p>ValidateLevel::strong_validate: besides weak_validate, also validate the schema of the
     * edge is consistent with the info defined.
     *
     * @param e The edge to add.
     * @param validateLevel The validate level for this operation, which is the builder's validate
     *     level by default.
     * @return Status: ok or Status::Invalid error.
     */
    @FFINameAlias("AddEdge")
    @CXXValue
    Status addEdge(@CXXReference Edge e, @CXXValue ValidateLevel validateLevel);

    /**
     * Add an edge to the collection.
     *
     * @param e The edge to add.
     * @return Status: ok or Status::Invalid error.
     */
    @FFINameAlias("AddEdge")
    @CXXValue
    Status addEdge(@CXXReference Edge e);

    /**
     * Get the current number of edges in the collection.
     *
     * @return The current number of edges in the collection.
     */
    @FFINameAlias("GetNum")
    @FFITypeAlias(GAR_ID_TYPE)
    long getNum();

    /**
     * Dump the collection into files.
     *
     * @return Status: ok or error.
     */
    @FFINameAlias("Dump")
    @CXXValue
    Status dump();

    @FFIFactory
    interface Factory {
        /**
         * Initialize the EdgesBuilder.
         *
         * @param edgeInfo The edge info that describes the vertex type.
         * @param prefix The absolute prefix.
         * @param adjListType The adj list type of the edges.
         * @param numVertices The total number of vertices for source or destination.
         * @param validateLevel The global validate level for the writer, with no validate by
         *     default. It could be ValidateLevel::no_validate, ValidateLevel::weak_validate or
         *     ValidateLevel::strong_validate, but could not be ValidateLevel::default_validate.
         */
        EdgesBuilder create(
                @CXXValue EdgeInfo edgeInfo,
                @CXXReference StdString prefix,
                @CXXValue AdjListType adjListType,
                @FFITypeAlias(GAR_ID_TYPE) long numVertices,
                @CXXValue ValidateLevel validateLevel);

        /**
         * Initialize the EdgesBuilder.
         *
         * @param edgeInfo The edge info that describes the vertex type.
         * @param prefix The absolute prefix.
         * @param adjListType The adj list type of the edges.
         * @param numVertices The total number of vertices for source or destination.
         */
        EdgesBuilder create(
                @CXXValue EdgeInfo edgeInfo,
                @CXXReference StdString prefix,
                @CXXValue AdjListType adjListType,
                @FFITypeAlias(GAR_ID_TYPE) long numVertices);
    }
}
