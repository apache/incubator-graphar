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

package com.alibaba.graphar.util;

import static com.alibaba.graphar.util.CppClassName.GAR_ID_TYPE;
import static com.alibaba.graphar.util.CppClassName.GAR_NAMESPACE;
import static com.alibaba.graphar.util.CppHeaderName.GAR_ARROW_CHUNK_READER_H;
import static com.alibaba.graphar.util.CppHeaderName.GAR_CHUNK_INFO_READER_H;
import static com.alibaba.graphar.util.CppHeaderName.GAR_GRAPH_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFILibrary;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphar.edges.EdgesCollection;
import com.alibaba.graphar.graphinfo.GraphInfo;
import com.alibaba.graphar.graphinfo.PropertyGroup;
import com.alibaba.graphar.readers.arrowchunk.AdjListArrowChunkReader;
import com.alibaba.graphar.readers.arrowchunk.AdjListOffsetArrowChunkReader;
import com.alibaba.graphar.readers.arrowchunk.AdjListPropertyArrowChunkReader;
import com.alibaba.graphar.readers.arrowchunk.VertexPropertyArrowChunkReader;
import com.alibaba.graphar.readers.chunkinfo.AdjListChunkInfoReader;
import com.alibaba.graphar.readers.chunkinfo.AdjListPropertyChunkInfoReader;
import com.alibaba.graphar.readers.chunkinfo.VertexPropertyChunkInfoReader;
import com.alibaba.graphar.stdcxx.StdSharedPtr;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.types.AdjListType;
import com.alibaba.graphar.vertices.VerticesCollection;

@FFIGen
@CXXHead(GAR_CHUNK_INFO_READER_H)
@CXXHead(GAR_ARROW_CHUNK_READER_H)
@CXXHead(GAR_GRAPH_H)
@FFILibrary(value = GAR_NAMESPACE, namespace = GAR_NAMESPACE)
public interface GrapharStaticFunctions {
    GrapharStaticFunctions INSTANCE = FFITypeFactory.getLibrary(GrapharStaticFunctions.class);

    // chunk info reader

    /**
     * Helper function to Construct VertexPropertyChunkInfoReader.
     *
     * @param graphInfo The graph info to describe the graph.
     * @param label label name of the vertex.
     * @param propertyGroup The property group of the vertex.
     */
    @FFINameAlias("ConstructVertexPropertyChunkInfoReader")
    @CXXValue
    Result<VertexPropertyChunkInfoReader> constructVertexPropertyChunkInfoReader(
            @CXXReference GraphInfo graphInfo,
            @CXXReference StdString label,
            @CXXReference PropertyGroup propertyGroup);

    /**
     * Helper function to Construct AdjListPropertyChunkInfoReader.
     *
     * @param graphInfo The graph info to describe the graph.
     * @param srcLabel label of source vertex.
     * @param edgeLabel label of edge.
     * @param dstLabel label of destination vertex.
     * @param propertyGroup The property group of the edge.
     * @param adjListType The adj list type for the edges.
     */
    @FFINameAlias("ConstructAdjListPropertyChunkInfoReader")
    @CXXValue
    Result<AdjListPropertyChunkInfoReader> constructAdjListPropertyChunkInfoReader(
            @CXXReference GraphInfo graphInfo,
            @CXXReference StdString srcLabel,
            @CXXReference StdString edgeLabel,
            @CXXReference StdString dstLabel,
            @CXXReference PropertyGroup propertyGroup,
            @CXXValue AdjListType adjListType);

    /**
     * Helper function to Construct AdjListChunkInfoReader.
     *
     * @param graphInfo The graph info to describe the graph.
     * @param srcLabel label of source vertex.
     * @param edgeLabel label of edge.
     * @param dstLabel label of destination vertex.
     * @param adjListType The adj list type for the edges.
     */
    @FFINameAlias("ConstructAdjListChunkInfoReader")
    @CXXValue
    Result<AdjListChunkInfoReader> constructAdjListChunkInfoReader(
            @CXXReference GraphInfo graphInfo,
            @CXXReference StdString srcLabel,
            @CXXReference StdString edgeLabel,
            @CXXReference StdString dstLabel,
            @CXXValue AdjListType adjListType);

    // arrow chunk reader

    /**
     * Helper function to Construct VertexPropertyArrowChunkReader.
     *
     * @param graphInfo The graph info to describe the graph.
     * @param label label of the vertex.
     * @param propertyGroup The property group of the vertex.
     */
    @FFINameAlias("ConstructVertexPropertyArrowChunkReader")
    @CXXValue
    Result<VertexPropertyArrowChunkReader> constructVertexPropertyArrowChunkReader(
            @CXXReference GraphInfo graphInfo,
            @CXXReference StdString label,
            @CXXReference PropertyGroup propertyGroup);

    /**
     * Helper function to Construct AdjListArrowChunkReader.
     *
     * @param graphInfo The graph info to describe the graph.
     * @param srcLabel label of source vertex.
     * @param edgeLabel label of edge.
     * @param dstLabel label of destination vertex.
     * @param adjListType The adj list type for the edges.
     */
    @FFINameAlias("ConstructAdjListArrowChunkReader")
    @CXXValue
    Result<AdjListArrowChunkReader> constructAdjListArrowChunkReader(
            @CXXReference GraphInfo graphInfo,
            @CXXReference StdString srcLabel,
            @CXXReference StdString edgeLabel,
            @CXXReference StdString dstLabel,
            @CXXValue AdjListType adjListType);

    /**
     * Helper function to Construct AdjListOffsetArrowChunkReader.
     *
     * @param graphInfo The graph info to describe the graph.
     * @param srcLabel label of source vertex.
     * @param edgeLabel label of edge.
     * @param dstLabel label of destination vertex.
     * @param adjListType The adj list type for the edges.
     */
    @FFINameAlias("ConstructAdjListOffsetArrowChunkReader")
    @CXXValue
    Result<AdjListOffsetArrowChunkReader> constructAdjListOffsetArrowChunkReader(
            @CXXReference GraphInfo graphInfo,
            @CXXReference StdString srcLabel,
            @CXXReference StdString edgeLabel,
            @CXXReference StdString dstLabel,
            @CXXValue AdjListType adjListType);

    /**
     * Helper function to Construct AdjListPropertyArrowChunkReader.
     *
     * @param graphInfo The graph info to describe the graph.
     * @param srcLabel label of source vertex.
     * @param edgeLabel label of edge.
     * @param dstLabel label of destination vertex.
     * @param propertyGroup The property group of the edge.
     * @param adjListType The adj list type for the edges.
     */
    @FFINameAlias("ConstructAdjListPropertyArrowChunkReader")
    @CXXValue
    Result<AdjListPropertyArrowChunkReader> constructAdjListPropertyArrowChunkReader(
            @CXXReference GraphInfo graphInfo,
            @CXXReference StdString srcLabel,
            @CXXReference StdString edgeLabel,
            @CXXReference StdString dstLabel,
            @CXXReference PropertyGroup propertyGroup,
            @CXXValue AdjListType adjListType);

    // graph

    /**
     * Construct the collection for vertices with specific label.
     *
     * @param graphInfo The GraphInfo for the graph.
     * @param label The vertex label.
     * @return The constructed collection or error.
     */
    @FFINameAlias("ConstructVerticesCollection")
    @CXXValue
    Result<StdSharedPtr<VerticesCollection>> constructVerticesCollection(
            @CXXReference GraphInfo graphInfo, @CXXReference StdString label);

    /**
     * Construct the collection for a range of edges.
     *
     * @param graphInfo The GraphInfo for the graph.
     * @param srcLabel The source vertex label.
     * @param edgeLabel The edge label.
     * @param dstLabel The destination vertex label.
     * @param adjListType The adjList type.
     * @param vertexChunkBegin The index of the beginning vertex chunk.
     * @param vertexChunkEnd The index of the end vertex chunk (not included).
     * @return The constructed collection or error.
     */
    @FFINameAlias("ConstructEdgesCollection")
    @CXXValue
    Result<StdSharedPtr<EdgesCollection>> constructEdgesCollection(
            @CXXReference GraphInfo graphInfo,
            @CXXReference StdString srcLabel,
            @CXXReference StdString edgeLabel,
            @CXXReference StdString dstLabel,
            @CXXValue AdjListType adjListType,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkBegin,
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkEnd);

    /**
     * Construct the collection for a range of edges.
     *
     * @param graphInfo The GraphInfo for the graph.
     * @param srcLabel The source vertex label.
     * @param edgeLabel The edge label.
     * @param dstLabel The destination vertex label.
     * @param adjListType The adjList type.
     * @return The constructed collection or error.
     */
    @FFINameAlias("ConstructEdgesCollection")
    @CXXValue
    Result<StdSharedPtr<EdgesCollection>> constructEdgesCollection(
            @CXXReference GraphInfo graphInfo,
            @CXXReference StdString srcLabel,
            @CXXReference StdString edgeLabel,
            @CXXReference StdString dstLabel,
            @CXXValue AdjListType adjListType);
}
