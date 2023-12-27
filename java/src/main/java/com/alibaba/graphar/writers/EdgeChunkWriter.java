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

import static com.alibaba.graphar.util.CppClassName.GAR_EDGE_CHUNK_WRITER;
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
import com.alibaba.graphar.graphinfo.EdgeInfo;
import com.alibaba.graphar.graphinfo.PropertyGroup;
import com.alibaba.graphar.stdcxx.StdSharedPtr;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.types.AdjListType;
import com.alibaba.graphar.types.ValidateLevel;
import com.alibaba.graphar.util.Status;

/**
 * The writer for edge (adj list, offset and property group) chunks.
 *
 * <p>Notes: For each writing operation, a validate_level could be set, which will be used to
 * validate the data before writing. The validate_level could be:
 *
 * <p>ValidateLevel::default_validate: to use the validate_level of the writer, which set through
 * the constructor or the SetValidateLevel method;
 *
 * <p>ValidateLevel::no_validate: without validation;
 *
 * <p>ValidateLevel::weak_validate: to validate if the vertex/edge count or vertex/edge chunk index
 * is non-negative, the adj_list type is valid, the property group exists and the size of
 * input_table is not larger than the chunk size;
 *
 * <p>ValidateLevel::strong_validate: besides weak_validate, also validate the schema of input_table
 * is consistent with that of property group; for writing operations without input_table, such as
 * writing vertices/edges number or copying file, the strong_validate is same as weak_validate.
 */
@FFIGen
@FFITypeAlias(GAR_EDGE_CHUNK_WRITER)
@CXXHead(GAR_ARROW_CHUNK_WRITER_H)
public interface EdgeChunkWriter extends CXXPointer {

    Factory factory = FFITypeFactory.getFactory(EdgeChunkWriter.class);

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
     * Write the number of edges into the file.
     *
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param count The number of edges.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WriteEdgesNum")
    @CXXValue
    Status writeEdgesNum(
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long count,
            @CXXValue ValidateLevel validateLevel);

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
     * Copy a file as a offset chunk.
     *
     * @param fileName The file to copy.
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WriteOffsetChunk")
    @CXXValue
    Status writeOffsetChunk(
            @CXXReference StdString fileName,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Copy a file as an adj list chunk.
     *
     * @param fileName The file to copy.
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param chunkIndex The index of the edge chunk inside the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WriteAdjListChunk")
    @CXXValue
    Status writeAdjListChunk(
            @CXXReference StdString fileName,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long chunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Copy a file as an edge property group chunk.
     *
     * @param fileName The file to copy.
     * @param propertyGroup The property group to write.
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param chunkIndex The index of the edge chunk inside the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WritePropertyChunk")
    @CXXValue
    Status writePropertyChunk(
            @CXXReference StdString fileName,
            @CXXReference PropertyGroup propertyGroup,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long chunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Validate and write the offset chunk for a vertex chunk.
     *
     * @param inputTable The table containing data.
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WriteOffsetChunk")
    @CXXValue
    Status writeOffsetChunk(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Validate and write the adj list chunk for an edge chunk.
     *
     * @param inputTable The table containing data.
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param chunkIndex The index of the edge chunk inside the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WriteAdjListChunk")
    @CXXValue
    Status writeAdjListChunk(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long chunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Validate and write a single edge property group for an edge chunk.
     *
     * @param inputTable The table containing data.
     * @param propertyGroup The property group to write.
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param chunkIndex The index of the edge chunk inside the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WritePropertyChunk")
    @CXXValue
    Status writePropertyChunk(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @CXXReference PropertyGroup propertyGroup,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long chunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Write all edge property groups for an edge chunk.
     *
     * @param inputTable The table containing data.
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param chunkIndex The index of the edge chunk inside the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WritePropertyChunk")
    @CXXValue
    Status writePropertyChunk(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long chunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Write the adj list and all property groups for an edge chunk.
     *
     * @param inputTable The table containing data.
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param chunkIndex The index of the edge chunk inside the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WriteChunk")
    @CXXValue
    Status writeChunk(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long chunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Write the adj list chunks for the edges of a vertex chunk.
     *
     * @param inputTable The table containing data.
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param startChunkIndex The start index of the edge chunks inside the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WriteAdjListTable")
    @CXXValue
    Status writeAdjListTable(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long startChunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Write chunks of a single property group for the edges of a vertex chunk.
     *
     * @param inputTable The table containing data.
     * @param propertyGroup The property group to write.
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param startChunkIndex The start index of the edge chunks inside the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WritePropertyTable")
    @CXXValue
    Status writePropertyTable(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @CXXReference PropertyGroup propertyGroup,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long startChunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Write chunks of all property groups for the edges of a vertex chunk.
     *
     * @param inputTable The table containing data.
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param startChunkIndex The start index of the edge chunks inside the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WritePropertyTable")
    @CXXValue
    Status writePropertyTable(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long startChunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Write chunks of the adj list and all property groups for the edges of a vertex chunk.
     *
     * @param inputTable The table containing data.
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param startChunkIndex The start index of the edge chunks inside the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("WriteTable")
    @CXXValue
    Status writeTable(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long startChunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Sort the edges, and write the adj list chunks for the edges of a vertex chunk.
     *
     * @param inputTable The table containing data.
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param startChunkIndex The start index of the edge chunks inside the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("SortAndWriteAdjListTable")
    @CXXValue
    Status sortAndWriteAdjListTable(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long startChunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Sort the edges, and write the adj list chunks for the edges of a vertex chunk.
     *
     * @param inputTable The table containing data.
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param startChunkIndex The start index of the edge chunks inside the vertex chunk.
     * @return Status: ok or error.
     */
    @FFINameAlias("SortAndWriteAdjListTable")
    @CXXValue
    Status sortAndWriteAdjListTable(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long startChunkIndex);

    /**
     * Sort the edges, and write chunks of a single property group for the edges of a vertex chunk.
     *
     * @param inputTable The table containing data.
     * @param propertyGroup The property group to write.
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param startChunkIndex The start index of the edge chunks inside the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("SortAndWritePropertyTable")
    @CXXValue
    Status sortAndWritePropertyTable(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @CXXReference PropertyGroup propertyGroup,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long startChunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Sort the edges, and write chunks of all property groups for the edges of a vertex chunk.
     *
     * @param inputTable The table containing data.
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param startChunkIndex The start index of the edge chunks inside the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("SortAndWritePropertyTable")
    @CXXValue
    Status sortAndWritePropertyTable(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long startChunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Sort the edges, and write chunks of the adj list and all property groups for the edges of a
     * vertex chunk.
     *
     * @param inputTable The table containing data.
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param startChunkIndex The start index of the edge chunks inside the vertex chunk.
     * @param validateLevel The validate level for this operation, which is the writer's validate
     *     level by default.
     * @return Status: ok or error.
     */
    @FFINameAlias("SortAndWriteTable")
    @CXXValue
    Status sortAndWriteTable(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long startChunkIndex,
            @CXXValue ValidateLevel validateLevel);

    /**
     * Sort the edges, and write chunks of the adj list and all property groups for the edges of a
     * vertex chunk.
     *
     * @param inputTable The table containing data.
     * @param vertexChunkIndex The index of the vertex chunk.
     * @param startChunkIndex The start index of the edge chunks inside the vertex chunk.
     * @return Status: ok or error.
     */
    @FFINameAlias("SortAndWriteTable")
    @CXXValue
    Status sortAndWriteTable(
            @CXXReference StdSharedPtr<ArrowTable> inputTable,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long startChunkIndex);

    @FFIFactory
    interface Factory {
        /**
         * Initialize the EdgeChunkWriter.
         *
         * @param edgeInfo The edge info that describes the edge type.
         * @param prefix The absolute prefix.
         * @param adjListType The adj list type for the edges.
         * @param validateLevel The global validate level for the writer, with no validate by
         *     default. It could be ValidateLevel::no_validate, ValidateLevel::weak_validate or
         *     ValidateLevel::strong_validate, but could not be ValidateLevel::default_validate.
         */
        EdgeChunkWriter create(
                @CXXReference EdgeInfo edgeInfo,
                @CXXReference StdString prefix,
                @CXXValue AdjListType adjListType,
                @CXXValue ValidateLevel validateLevel);

        /**
         * Initialize the EdgeChunkWriter.
         *
         * @param edgeInfo The edge info that describes the edge type.
         * @param prefix The absolute prefix.
         * @param adjListType The adj list type for the edges.
         */
        EdgeChunkWriter create(
                @CXXReference EdgeInfo edgeInfo,
                @CXXReference StdString prefix,
                @CXXValue AdjListType adjListType);

        /**
         * Initialize the EdgeChunkWriter.
         *
         * @param edgeInfo The edge info that describes the edge type.
         * @param prefix The absolute prefix.
         */
        EdgeChunkWriter create(@CXXReference EdgeInfo edgeInfo, @CXXReference StdString prefix);
    }
}
