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


package com.alibaba.graphar.readers.chunkinfo;

import static com.alibaba.graphar.util.CppClassName.GAR_ADJ_LIST_CHUNK_INFO_READER;
import static com.alibaba.graphar.util.CppClassName.GAR_ID_TYPE;
import static com.alibaba.graphar.util.CppHeaderName.GAR_CHUNK_INFO_READER_H;

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
import com.alibaba.graphar.graphinfo.GraphInfo;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.types.AdjListType;
import com.alibaba.graphar.util.GrapharStaticFunctions;
import com.alibaba.graphar.util.Result;
import com.alibaba.graphar.util.Status;

/** The chunk info reader for adj list topology chunk. */
@FFIGen
@FFITypeAlias(GAR_ADJ_LIST_CHUNK_INFO_READER)
@CXXHead(GAR_CHUNK_INFO_READER_H)
public interface AdjListChunkInfoReader extends CXXPointer {

    Factory factory = FFITypeFactory.getFactory(AdjListChunkInfoReader.class);

    /**
     * Sets chunk position indicator for reader by source internal vertex id.
     *
     * @param id the source internal vertex id.
     */
    @FFINameAlias("seek_src")
    @CXXValue
    Status seekSrc(@FFINameAlias(GAR_ID_TYPE) long id);

    /**
     * Sets chunk position indicator for reader by destination internal vertex id.
     *
     * @param id the destination internal vertex id.
     */
    @FFINameAlias("seek_dst")
    @CXXValue
    Status seekDst(@FFINameAlias(GAR_ID_TYPE) long id);

    /**
     * Sets chunk position indicator for reader by edge index.
     *
     * @param index offset edge index of the vertex chunk. Note: the offset is the edge index of the
     *     vertex chunk, not the edge index of the whole graph.
     */
    @CXXValue
    Status seek(@FFINameAlias(GAR_ID_TYPE) long index);

    /** Return the current chunk file path of chunk position indicator. */
    @FFINameAlias("GetChunk")
    @CXXValue
    Result<StdString> getChunk();

    /**
     * Sets chunk position indicator to next chunk. if current chunk is the last chunk, will return
     * Status::IndexError error.
     */
    @FFINameAlias("next_chunk")
    @CXXValue
    Status nextChunk();

    /**
     * Helper function to Construct AdjListChunkInfoReader.
     *
     * @param graphInfo The graph info to describe the graph.
     * @param srcLabel label of source vertex.
     * @param edgeLabel label of edge.
     * @param dstLabel label of destination vertex.
     * @param adjListType The adj list type for the edges.
     */
    static Result<AdjListChunkInfoReader> constructAdjListChunkInfoReader(
            @CXXReference GraphInfo graphInfo,
            @CXXReference StdString srcLabel,
            @CXXReference StdString edgeLabel,
            @CXXReference StdString dstLabel,
            @CXXValue AdjListType adjListType) {
        return GrapharStaticFunctions.INSTANCE.constructAdjListChunkInfoReader(
                graphInfo, srcLabel, edgeLabel, dstLabel, adjListType);
    }

    @FFIFactory
    interface Factory {
        /**
         * Initialize the AdjListChunkInfoReader.
         *
         * @param edgeInfo The edge info that describes the edge type.
         * @param adjListType The adj list type for the edges.
         * @param prefix The absolute prefix of the graph.
         */
        AdjListChunkInfoReader create(
                @CXXReference EdgeInfo edgeInfo,
                @CXXValue AdjListType adjListType,
                @CXXReference StdString prefix);
    }
}
