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

import static org.apache.graphar.util.CppClassName.GAR_ADJ_LIST_OFFSET_ARROW_CHUNK_READER;
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
import org.apache.graphar.arrow.ArrowArray;
import org.apache.graphar.graphinfo.EdgeInfo;
import org.apache.graphar.stdcxx.StdSharedPtr;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.types.AdjListType;
import org.apache.graphar.util.Result;
import org.apache.graphar.util.Status;

/** The arrow chunk reader for edge offset. */
@FFIGen
@FFITypeAlias(GAR_ADJ_LIST_OFFSET_ARROW_CHUNK_READER)
@CXXHead(GAR_ARROW_CHUNK_READER_H)
public interface AdjListOffsetArrowChunkReader extends CXXPointer {
    Factory factory = FFITypeFactory.getFactory(AdjListOffsetArrowChunkReader.class);

    /**
     * Sets chunk position indicator for reader by internal vertex id. If internal vertex id is not
     * found, will return Status::IndexError error. After seeking to an invalid vertex id, the next
     * call to GetChunk function may undefined, e.g. return a non exist path.
     *
     * @param id the internal vertex id.
     */
    @CXXValue
    Status seek(@FFINameAlias(GAR_ID_TYPE) long id);

    /** Get the current offset chunk as arrow::Array. */
    @FFINameAlias("GetChunk")
    @CXXValue
    Result<StdSharedPtr<ArrowArray>> getChunk();

    /**
     * Sets chunk position indicator to next chunk. if current chunk is the last chunk, will return
     * Status::IndexError error.
     */
    @FFINameAlias("next_chunk")
    @CXXValue
    Status nextChunk();

    /** Get current vertex chunk index. */
    @FFINameAlias("GetChunkIndex")
    @FFITypeAlias(GAR_ID_TYPE)
    long getChunkIndex();

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
        AdjListOffsetArrowChunkReader create(
                @CXXReference StdSharedPtr<EdgeInfo> edgeInfo,
                @CXXValue AdjListType adjListType,
                @CXXReference StdString prefix);
    }
}
