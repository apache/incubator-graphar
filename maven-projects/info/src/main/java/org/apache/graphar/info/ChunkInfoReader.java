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

package org.apache.graphar.info;
// based on https://github.com/apache/incubator-graphar/blob/main/cpp/src/graphar/chunk_info_reader.cc
public class ChunkInfoReader {
    private final VertexInfo cachedVertexInfo;
    private final PropertyGroup cachedPropertyGroup;

    public static String getChunk(long, index, VertexInfo vertexInfo) {
        long chunkIndex = chunkExists(index);
        String chunkBasePath = vertexInfo.getPropertyGroupPrefix() + "/chunk";
        return chunkBasePath + String.valueOf(chunkIndex);

    }

    public long chunkExists(long index) {
        int chunkSize = vertexInfo.getChunkSize()
        int totalCount = Integer.valueOf(vertexInfo.getVerticesNumFilePath());
        int chunksCount = totalCount / chunkSize;
        long chunkIndex = index / chunksCount;

        if (chunkIndex < chunkCount) {
            return chunkIndex;
        }
        throw new IndexOutOfBoundsException("Chunk Index out of Range " + Integer.valueOf(index));

    }

    public String getPropertyGroupChunkPath(PropertyGroup propertyGroup, long chunkIndex) {
        // PropertyGroup will be checked in getPropertyGroupPrefix
        return cachedVertexInfo.getPropertyGroupPrefix(propertyGroup) + "/chunk" + chunkIndex;
    }
}
