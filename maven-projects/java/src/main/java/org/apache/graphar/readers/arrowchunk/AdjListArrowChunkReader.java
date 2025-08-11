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

package org.apache.graphar.readers.arrowchunk;

import static org.apache.graphar.util.CppClassName.GAR_ADJ_LIST_ARROW_CHUNK_READER;
import static org.apache.graphar.util.CppClassName.GAR_ID_TYPE;
import static org.apache.graphar.util.CppHeaderName.GAR_ARROW_CHUNK_READER_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import org.apache.graphar.arrow.ArrowTable;
import org.apache.graphar.graphinfo.EdgeInfo;
import org.apache.graphar.stdcxx.StdSharedPtr;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.types.AdjListType;
import org.apache.graphar.util.Result;
import org.apache.graphar.util.Status;

/** The arrow chunk reader for adj list topology chunk. */
@FFIGen
@FFITypeAlias(GAR_ADJ_LIST_ARROW_CHUNK_READER)
@CXXHead(GAR_ARROW_CHUNK_READER_H)
public interface AdjListArrowChunkReader extends CXXPointer {

    Factory factory = FFITypeFactory.getFactory(AdjListArrowChunkReader.class);

    /**
     * Sets chunk position indicator for reader by source vertex id.
     *
     * @param id the source vertex id.
     */
    @FFINameAlias("seek_src")
    @CXXValue
    Status seekSrc(@FFITypeAlias(GAR_ID_TYPE) long id);

    /**
     * Sets chunk position indicator for reader by destination vertex id.
     *
     * @param offset the destination vertex id.
     */
    @FFINameAlias("seek_dst")
    @CXXValue
    Status seekDst(@FFITypeAlias(GAR_ID_TYPE) long offset);

    /**
     * Sets chunk position indicator for reader by edge index.
     *
     * @param offset edge index of the vertex chunk. Note: the offset is the edge index of the
     *     vertex chunk, not the edge index of the whole graph.
     */
    @CXXValue
    Status seek(@FFITypeAlias(GAR_ID_TYPE) long offset);

    /** Get the current offset chunk as arrow::Array. */
    @FFINameAlias("GetChunk")
    @CXXValue
    Result<StdSharedPtr<ArrowTable>> getChunk();

    /** Get the number of rows of the current chunk table. */
    @FFINameAlias("GetRowNumOfChunk")
    @CXXValue
    @FFITypeAlias("graphar::Result<long>")
    Result<Long> getRowNumOfChunk();

    /**
     * Sets chunk position indicator to next chunk.
     *
     * @return Status: ok or EndOfChunk error if the reader is at the end of current vertex chunk,
     *     or IndexError error if the reader is at the end of all vertex chunks.
     */
    @FFINameAlias("next_chunk")
    @CXXValue
    Status nextChunk();

    /**
     * Sets chunk position to the specific vertex chunk and edge chunk.
     *
     * @param vertexChunkIndex the vertex chunk index.
     * @return Status: ok or error
     */
    @FFINameAlias("seek_chunk_index")
    @CXXValue
    Status seekChunkIndex(@FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex);

    /**
     * Sets chunk position to the specific vertex chunk and edge chunk.
     *
     * @param vertexChunkIndex the vertex chunk index.
     * @param chunkIndex the edge chunk index of vertex_chunk_index, C++ default is 0.
     * @return Status: ok or error
     */
    @FFINameAlias("seek_chunk_index")
    @CXXValue
    Status seekChunkIndex(
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long chunkIndex);

    @FFIFactory
    interface Factory {
        /**
         * Initialize the AdjListOffsetArrowChunkReader.
         *
         * @param edgeInfo The edge info that describes the edge type.
         * @param adjListType The adj list type for the edges. Note that the adj list type must be
         *     AdjListType::ordered_by_source or AdjListType::ordered_by_dest.
         * @param prefix The absolute prefix.
         */
        AdjListArrowChunkReader create(
                @CXXReference StdSharedPtr<EdgeInfo> edgeInfo,
                @CXXValue AdjListType adjListType,
                @CXXReference StdString prefix);
    }
}
