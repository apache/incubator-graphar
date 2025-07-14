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

import static org.apache.graphar.util.CppClassName.GAR_ID_TYPE;
import static org.apache.graphar.util.CppClassName.GAR_VERTEX_PROPERTY_CHUNK_INFO_READER;
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
import org.apache.graphar.graphinfo.PropertyGroup;
import org.apache.graphar.graphinfo.VertexInfo;
import org.apache.graphar.stdcxx.StdSharedPtr;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.util.Result;
import org.apache.graphar.util.Status;

/** The chunk info reader for vertex property group. */
@FFIGen
@FFITypeAlias(GAR_VERTEX_PROPERTY_CHUNK_INFO_READER)
@CXXHead(GAR_CHUNK_INFO_READER_H)
public interface VertexPropertyChunkInfoReader extends CXXPointer {

    Factory factory = FFITypeFactory.getFactory(VertexPropertyChunkInfoReader.class);

    /**
     * Sets chunk position indicator for reader by internal vertex id. If internal vertex id is not
     * found, will return Status::IndexError error. After seeking to an invalid vertex id, the next
     * call to GetChunk function may undefined, e.g. return a non exist path.
     *
     * @param id the internal vertex id.
     */
    @CXXValue
    Status seek(@FFITypeAlias(GAR_ID_TYPE) long id);

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

    /** Get the chunk number of the current vertex property group. */
    @FFINameAlias("GetChunkNum")
    long getChunkNum();

    @FFIFactory
    interface Factory {
        /**
         * Initialize the VertexPropertyChunkInfoReader.
         *
         * @param vertexInfo The vertex info that describes the vertex type.
         * @param propertyGroup The property group that describes the property group.
         * @param prefix The absolute prefix of the graph.
         */
        VertexPropertyChunkInfoReader create(
                @CXXReference StdSharedPtr<VertexInfo> vertexInfo,
                @CXXReference StdSharedPtr<PropertyGroup> propertyGroup,
                @CXXReference StdString prefix);
    }
}
