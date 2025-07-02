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

package org.apache.graphar.readers.chunkinfo;

import static org.apache.graphar.util.CppClassName.GAR_ADJ_LIST_PROPERTY_CHUNK_INFO_READER;
import static org.apache.graphar.util.CppClassName.GAR_ID_TYPE;
import static org.apache.graphar.util.CppHeaderName.GAR_CHUNK_INFO_READER_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import org.apache.graphar.graphinfo.EdgeInfo;
import org.apache.graphar.graphinfo.PropertyGroup;
import org.apache.graphar.stdcxx.StdSharedPtr;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.types.AdjListType;
import org.apache.graphar.util.Result;
import org.apache.graphar.util.Status;

/** The chunk info reader for edge property group chunk. */
@FFIGen
@FFITypeAlias(GAR_ADJ_LIST_PROPERTY_CHUNK_INFO_READER)
@CXXHead(GAR_CHUNK_INFO_READER_H)
public interface AdjListPropertyChunkInfoReader extends CXXPointer {

    Factory factory = FFITypeFactory.getFactory(AdjListChunkInfoReader.class);

    /**
     * Sets chunk position indicator for reader by source vertex id.
     *
     * @param id the source vertex id.
     */
    @FFINameAlias("seek_src")
    @CXXValue
    Status seekSrc(@FFINameAlias(GAR_ID_TYPE) long id);

    /**
     * Sets chunk position indicator for reader by destination vertex id.
     *
     * @param id the destination vertex id.
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

    @FFIFactory
    interface Factory {
        /**
         * Initialize the AdjListPropertyChunkInfoReader.
         *
         * @param edgeInfo The edge info that describes the edge type.
         * @param propertyGroup The property group of the edge property.
         * @param adjListType The adj list type for the edges.
         * @param prefix The absolute prefix of the graph.
         */
        AdjListPropertyChunkInfoReader create(
                @CXXReference StdSharedPtr<EdgeInfo> edgeInfo,
                @CXXReference StdSharedPtr<PropertyGroup> propertyGroup,
                @CXXValue AdjListType adjListType,
                @CXXReference StdString prefix);
    }
}
