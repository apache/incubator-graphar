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

package com.alibaba.graphar.writers;

import static com.alibaba.graphar.util.CppClassName.GAR_BUILDER_VERTEX_PROPERTY_WRITER;
import static com.alibaba.graphar.util.CppClassName.GAR_ID_TYPE;
import static com.alibaba.graphar.util.CppHeaderName.GAR_ARROW_CHUNK_WRITER_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphar.arrow.ArrowTable;
import com.alibaba.graphar.graphinfo.PropertyGroup;
import com.alibaba.graphar.graphinfo.VertexInfo;
import com.alibaba.graphar.stdcxx.StdSharedPtr;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.types.ValidateLevel;
import com.alibaba.graphar.util.Status;

/**
 * The writer for vertex property group chunks.
 *
 * <p>Notes: For each writing operation, a validate_level could be set, which will be used to
 * validate the data before writing. The validate_level could be:
 *
 * <p>ValidateLevel::default_validate: to use the validate_level of the writer, which set through
 * the constructor or the SetValidateLevel method;
 *
 * <p>ValidateLevel::no_validate: without validation;
 *
 * <p>ValidateLevel::weak_validate: to validate if the vertex count or vertex chunk index is
 * non-negative, the property group exists and the size of input_table is not larger than the vertex
 * chunk size;
 *
 * <p>ValidateLevel::strong_validate: besides weak_validate, also validate the schema of input_table
 * is consistent with that of property group; for writing operations without input_table, such as
 * writing vertices number or copying file, the strong_validate is same as weak_validate.
 */
@FFIGen
@FFITypeAlias(GAR_BUILDER_VERTEX_PROPERTY_WRITER)
@CXXHead(GAR_ARROW_CHUNK_WRITER_H)
public interface VertexPropertyWriter extends CXXPointer {

    Factory factory = FFITypeFactory.getFactory(VertexPropertyWriter.class);

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
     * Write the number of vertices into the file.
     *
     * @param count The number of vertices.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WriteVerticesNum")
    @CXXValue
    Status writeVerticesNum(
            @FFITypeAlias(GAR_ID_TYPE) long count, @CXXValue ValidateLevel validateLevel);

    /**
     * Write the number of vertices into the file.
     *
     * @param count The number of vertices.
     * @return Status: ok or error.
     */
    @FFINameAlias("WriteVerticesNum")
    @CXXValue
    Status writeVerticesNum(@FFITypeAlias(GAR_ID_TYPE) long count);

    /**
     * Copy a file as a vertex property group chunk.
     *
     * @param fileName The file to copy.
     * @param propertyGroup The property group.
     * @param chunkIndex The index of the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WriteChunk")
    @CXXValue
    Status writeChunk(
            @CXXReference StdString fileName,
            @CXXReference PropertyGroup propertyGroup,
            @FFITypeAlias(GAR_ID_TYPE) long chunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Validate and write a single property group for a single vertex chunk.
     *
     * @param inputTable The table containing data.
     * @param propertyGroup The property group.
     * @param chunkIndex The index of the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WriteChunk")
    @CXXValue
    Status writeChunk(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @CXXReference PropertyGroup propertyGroup,
            @FFITypeAlias(GAR_ID_TYPE) long chunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Write all property groups of a single vertex chunk to corresponding files.
     *
     * @param inputTable The table containing data.
     * @param chunkIndex The index of the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WriteChunk")
    @CXXValue
    Status writeChunk(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @FFITypeAlias(GAR_ID_TYPE) long chunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Write all property groups of a single vertex chunk to corresponding files.
     *
     * @param inputTable The table containing data.
     * @param chunkIndex The index of the vertex chunk.
     * @return Status: ok or error.
     */
    @FFINameAlias("WriteChunk")
    @CXXValue
    Status writeChunk(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @FFITypeAlias(GAR_ID_TYPE) long chunkIndex);

    /**
     * Write a single property group for multiple vertex chunks to corresponding files.
     *
     * @param inputTable The table containing data.
     * @param propertyGroup The property group.
     * @param startChunkIndex The start index of the vertex chunks.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WriteTable")
    @CXXValue
    Status writeTable(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @CXXReference PropertyGroup propertyGroup,
            @FFITypeAlias(GAR_ID_TYPE) long startChunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Write all property groups for multiple vertex chunks to corresponding files.
     *
     * @param inputTable The table containing data.
     * @param startChunkIndex The start index of the vertex chunks.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WriteTable")
    @CXXValue
    Status writeTable(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @FFITypeAlias(GAR_ID_TYPE) long startChunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Write all property groups for multiple vertex chunks to corresponding files.
     *
     * @param inputTable The table containing data.
     * @param startChunkIndex The start index of the vertex chunks.
     * @return Status: ok or error.
     */
    @FFINameAlias("WriteTable")
    @CXXValue
    Status writeTable(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @FFITypeAlias(GAR_ID_TYPE) long startChunkIndex);

    @FFIFactory
    interface Factory {
        /**
         * Initialize the VertexPropertyWriter.
         *
         * @param vertexInfo The vertex info that describes the vertex type.
         * @param prefix The absolute prefix.
         * @param validateLevel The global validate level for the writer, with no validate by
         *     default. It could be ValidateLevel::no_validate, ValidateLevel::weak_validate or
         *     ValidateLevel::strong_validate, but could not be ValidateLevel::default_validate.
         */
        VertexPropertyWriter create(
                @CXXReference VertexInfo vertexInfo,
                @CXXReference StdString prefix,
                @CXXValue ValidateLevel validateLevel);

        /**
         * Initialize the VertexPropertyWriter.
         *
         * @param vertexInfo The vertex info that describes the vertex type.
         * @param prefix The absolute prefix.
         */
        VertexPropertyWriter create(
                @CXXReference VertexInfo vertexInfo, @CXXReference StdString prefix);
    }
}
