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

package com.alibaba.graphar.writers.builder;

import static com.alibaba.graphar.util.CppClassName.GAR_BUILDER_VERTICES_BUILDER;
import static com.alibaba.graphar.util.CppClassName.GAR_ID_TYPE;
import static com.alibaba.graphar.util.CppHeaderName.GAR_VERTICES_BUILDER_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphar.graphinfo.VertexInfo;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.types.ValidateLevel;
import com.alibaba.graphar.util.Status;

/** VertexBuilder is designed for building and writing a collection of vertices. */
@FFIGen
@FFITypeAlias(GAR_BUILDER_VERTICES_BUILDER)
@CXXHead(GAR_VERTICES_BUILDER_H)
public interface VerticesBuilder extends CXXPointer {

    Factory factory = FFITypeFactory.getFactory(VerticesBuilder.class);

    /** Clear the vertices in this VerciesBuilder. */
    @FFINameAlias("Clear")
    void clear();

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

    /**
     * Add a vertex with the given index.
     *
     * <p>The validate_level for this operation could be:
     *
     * <p>ValidateLevel::default_validate: to use the validate_level of the builder, which set
     * through the constructor or the SetValidateLevel method;
     *
     * <p>ValidateLevel::no_validate: without validation;
     *
     * <p>ValidateLevel::weak_validate: to validate if the start index and the vertex index is
     * valid, and the data in builder is not saved;
     *
     * <p>ValidateLevel::strong_validate: besides weak_validate, also validate the schema of the
     * vertex is consistent with the info defined.
     *
     * @param v The vertex to add.
     * @param index The given index, -1 means the next unused index.
     * @param validateLevel The validate level for this operation, which is the builder's validate
     *     level by default.
     * @return Status: ok or Status::Invalid error.
     */
    @FFINameAlias("AddVertex")
    @CXXValue
    Status addVertex(
            @CXXReference Vertex v,
            @FFITypeAlias(GAR_ID_TYPE) long index, // NOLINT
            @CXXValue ValidateLevel validateLevel);

    /**
     * Add a vertex with the given index.
     *
     * @param v The vertex to add.
     * @return Status: ok or Status::Invalid error.
     */
    @FFINameAlias("AddVertex")
    @CXXValue
    Status addVertex(@CXXReference Vertex v);

    /**
     * Add a vertex with the given index.
     *
     * @param v The vertex to add.
     * @param index The given index, -1 means the next unused index.
     * @return Status: ok or Status::Invalid error.
     */
    @FFINameAlias("AddVertex")
    @CXXValue
    Status addVertex(@CXXReference Vertex v, @FFITypeAlias(GAR_ID_TYPE) long index // NOLINT
            );

    /**
     * Get the current number of vertices in the collection.
     *
     * @return The current number of vertices in the collection.
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
         * Initialize the VerciesBuilder.
         *
         * @param vertexInfo The vertex info that describes the vertex type.
         * @param prefix The absolute prefix.
         * @param startVertexIndex The start index of the vertices' collection.
         * @param validateLevel The global validate level for the writer, with no validate by
         *     default. It could be ValidateLevel::no_validate, ValidateLevel::weak_validate or
         *     ValidateLevel::strong_validate, but could not be ValidateLevel::default_validate.
         */
        VerticesBuilder create(
                @CXXReference VertexInfo vertexInfo,
                @CXXReference StdString prefix,
                @FFITypeAlias(GAR_ID_TYPE) long startVertexIndex,
                @CXXValue ValidateLevel validateLevel);

        /**
         * Initialize the VerciesBuilder.
         *
         * @param vertexInfo The vertex info that describes the vertex type.
         * @param prefix The absolute prefix.
         */
        VerticesBuilder create(@CXXReference VertexInfo vertexInfo, @CXXReference StdString prefix);

        /**
         * Initialize the VerciesBuilder.
         *
         * @param vertexInfo The vertex info that describes the vertex type.
         * @param prefix The absolute prefix.
         * @param startVertexIndex The start index of the vertices' collection.
         */
        VerticesBuilder create(
                @CXXReference VertexInfo vertexInfo,
                @CXXReference StdString prefix,
                @FFITypeAlias(GAR_ID_TYPE) long startVertexIndex);
    }
}
