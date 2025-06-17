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

package org.apache.graphar.util;

import static org.apache.graphar.util.CppClassName.GAR_ID_TYPE;
import static org.apache.graphar.util.CppClassName.GAR_NAMESPACE;
import static org.apache.graphar.util.CppHeaderName.*;

import com.alibaba.fastffi.*;
import org.apache.graphar.edges.EdgesCollection;
import org.apache.graphar.graphinfo.*;
import org.apache.graphar.readers.arrowchunk.AdjListArrowChunkReader;
import org.apache.graphar.readers.arrowchunk.AdjListOffsetArrowChunkReader;
import org.apache.graphar.readers.arrowchunk.AdjListPropertyArrowChunkReader;
import org.apache.graphar.readers.arrowchunk.VertexPropertyArrowChunkReader;
import org.apache.graphar.readers.chunkinfo.AdjListChunkInfoReader;
import org.apache.graphar.readers.chunkinfo.AdjListPropertyChunkInfoReader;
import org.apache.graphar.readers.chunkinfo.VertexPropertyChunkInfoReader;
import org.apache.graphar.stdcxx.StdSharedPtr;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.stdcxx.StdVector;
import org.apache.graphar.types.AdjListType;
import org.apache.graphar.types.DataType;
import org.apache.graphar.types.FileType;
import org.apache.graphar.vertices.VerticesCollection;

@FFIGen
@CXXHead(GAR_CHUNK_INFO_READER_H)
@CXXHead(GAR_ARROW_CHUNK_READER_H)
@CXXHead(GAR_GRAPH_H)
@CXXHead(GAR_FWD_H)
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
    @FFINameAlias("VertexPropertyChunkInfoReader::Make")
    @CXXValue
    Result<StdSharedPtr<VertexPropertyChunkInfoReader>> constructVertexPropertyChunkInfoReader(
            @CXXReference StdSharedPtr<GraphInfo> graphInfo,
            @CXXReference StdString label,
            @CXXReference StdSharedPtr<PropertyGroup> propertyGroup);

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
    @FFINameAlias("AdjListPropertyChunkInfoReader::Make")
    @CXXValue
    Result<StdSharedPtr<AdjListPropertyChunkInfoReader>> constructAdjListPropertyChunkInfoReader(
            @CXXReference StdSharedPtr<GraphInfo> graphInfo,
            @CXXReference StdString srcLabel,
            @CXXReference StdString edgeLabel,
            @CXXReference StdString dstLabel,
            @CXXReference StdSharedPtr<PropertyGroup> propertyGroup,
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
    @FFINameAlias("AdjListChunkInfoReader::Make")
    @CXXValue
    Result<StdSharedPtr<AdjListChunkInfoReader>> constructAdjListChunkInfoReader(
            @CXXReference StdSharedPtr<GraphInfo> graphInfo,
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
    @FFINameAlias("VertexPropertyArrowChunkReader::Make")
    @CXXValue
    Result<StdSharedPtr<VertexPropertyArrowChunkReader>> constructVertexPropertyArrowChunkReader(
            @CXXReference StdSharedPtr<GraphInfo> graphInfo,
            @CXXReference StdString label,
            @CXXReference StdSharedPtr<PropertyGroup> propertyGroup);

    /**
     * Helper function to Construct AdjListArrowChunkReader.
     *
     * @param graphInfo The graph info to describe the graph.
     * @param srcLabel label of source vertex.
     * @param edgeLabel label of edge.
     * @param dstLabel label of destination vertex.
     * @param adjListType The adj list type for the edges.
     */
    @FFINameAlias("AdjListArrowChunkReader::Make")
    @CXXValue
    Result<StdSharedPtr<AdjListArrowChunkReader>> constructAdjListArrowChunkReader(
            @CXXReference StdSharedPtr<GraphInfo> graphInfo,
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
    @FFINameAlias("AdjListOffsetArrowChunkReader::Make")
    @CXXValue
    Result<StdSharedPtr<AdjListOffsetArrowChunkReader>> constructAdjListOffsetArrowChunkReader(
            @CXXReference StdSharedPtr<GraphInfo> graphInfo,
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
    @FFINameAlias("AdjListPropertyArrowChunkReader::Make")
    @CXXValue
    Result<StdSharedPtr<AdjListPropertyArrowChunkReader>> constructAdjListPropertyArrowChunkReader(
            @CXXReference StdSharedPtr<GraphInfo> graphInfo,
            @CXXReference StdString srcLabel,
            @CXXReference StdString edgeLabel,
            @CXXReference StdString dstLabel,
            @CXXReference StdSharedPtr<PropertyGroup> propertyGroup,
            @CXXValue AdjListType adjListType);

    // graph

    /**
     * Construct the collection for vertices with specific label.
     *
     * @param graphInfo The GraphInfo for the graph.
     * @param label The vertex label.
     * @return The constructed collection or error.
     */
    @FFINameAlias("VerticesCollection::Make")
    @CXXValue
    Result<StdSharedPtr<VerticesCollection>> constructVerticesCollection(
            @CXXReference StdSharedPtr<GraphInfo> graphInfo, @CXXReference StdString label);

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
    @FFINameAlias("EdgesCollection::Make")
    @CXXValue
    Result<StdSharedPtr<EdgesCollection>> constructEdgesCollection(
            @CXXReference StdSharedPtr<GraphInfo> graphInfo,
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
    @FFINameAlias("EdgesCollection::Make")
    @CXXValue
    Result<StdSharedPtr<EdgesCollection>> constructEdgesCollection(
            @CXXReference StdSharedPtr<GraphInfo> graphInfo,
            @CXXReference StdString srcLabel,
            @CXXReference StdString edgeLabel,
            @CXXReference StdString dstLabel,
            @CXXValue AdjListType adjListType);

    @FFINameAlias("CreatePropertyGroup")
    @CXXValue
    StdSharedPtr<PropertyGroup> createPropertyGroup(
            @CXXReference StdVector<Property> properties,
            @CXXValue FileType fileType,
            @CXXReference StdString prefix);

    @FFINameAlias("CreatePropertyGroup")
    @CXXValue
    StdSharedPtr<PropertyGroup> createPropertyGroup(
            @CXXReference StdVector<Property> properties, @CXXValue FileType fileType);

    @FFINameAlias("CreateAdjacentList")
    @CXXValue
    StdSharedPtr<AdjacentList> createAdjacentList(
            @CXXValue AdjListType adjListType,
            @CXXValue FileType fileType,
            @CXXReference StdString prefix);

    @FFINameAlias("CreateAdjacentList")
    @CXXValue
    StdSharedPtr<AdjacentList> createAdjacentList(
            @CXXValue AdjListType adjListType, @CXXValue FileType fileType);

    @FFINameAlias("CreateVertexInfo")
    @CXXValue
    StdSharedPtr<VertexInfo> createVertexInfo(
            @CXXReference StdString label,
            @CXXValue @FFITypeAlias(GAR_ID_TYPE) long chunk_size,
            @CXXReference StdVector<StdSharedPtr<PropertyGroup>> propertyGroups,
            @CXXReference StdString prefix,
            @CXXValue StdSharedPtr<InfoVersion> version);

    @FFINameAlias("CreateVertexInfo")
    @CXXValue
    StdSharedPtr<VertexInfo> createVertexInfo(
            @CXXReference StdString label,
            @CXXValue @FFITypeAlias(GAR_ID_TYPE) long chunk_size,
            @CXXReference StdVector<StdSharedPtr<PropertyGroup>> propertyGroups,
            @CXXReference StdString prefix);

    @FFINameAlias("CreateVertexInfo")
    @CXXValue
    StdSharedPtr<VertexInfo> createVertexInfo(
            @CXXReference StdString label,
            @CXXValue @FFITypeAlias(GAR_ID_TYPE) long chunk_size,
            @CXXReference StdVector<StdSharedPtr<PropertyGroup>> propertyGroups);

    @FFINameAlias("CreateEdgeInfo")
    @CXXValue
    StdSharedPtr<EdgeInfo> createEdgeInfo(
            @CXXReference StdString srcLabel,
            @CXXReference StdString edgeLabel,
            @CXXReference StdString dstLabel,
            @CXXValue @FFITypeAlias(GAR_ID_TYPE) long chunkSize,
            @CXXValue @FFITypeAlias(GAR_ID_TYPE) long srcChunkSize,
            @CXXValue @FFITypeAlias(GAR_ID_TYPE) long dstChunkSize,
            @CXXValue boolean directed,
            @CXXReference StdVector<StdSharedPtr<AdjacentList>> adjacentLists,
            @CXXReference StdVector<StdSharedPtr<PropertyGroup>> propertyGroups,
            @CXXReference StdString prefix,
            @CXXValue StdSharedPtr<InfoVersion> version);

    @FFINameAlias("CreateEdgeInfo")
    @CXXValue
    StdSharedPtr<EdgeInfo> createEdgeInfo(
            @CXXReference StdString srcLabel,
            @CXXReference StdString edgeLabel,
            @CXXReference StdString dstLabel,
            @CXXValue @FFITypeAlias(GAR_ID_TYPE) long chunkSize,
            @CXXValue @FFITypeAlias(GAR_ID_TYPE) long srcChunkSize,
            @CXXValue @FFITypeAlias(GAR_ID_TYPE) long dstChunkSize,
            @CXXValue boolean directed,
            @CXXReference StdVector<StdSharedPtr<AdjacentList>> adjacentLists,
            @CXXReference StdVector<StdSharedPtr<PropertyGroup>> propertyGroups,
            @CXXReference StdString prefix);

    @FFINameAlias("CreateEdgeInfo")
    @CXXValue
    StdSharedPtr<EdgeInfo> createEdgeInfo(
            @CXXReference StdString srcLabel,
            @CXXReference StdString edgeLabel,
            @CXXReference StdString dstLabel,
            @CXXValue @FFITypeAlias(GAR_ID_TYPE) long chunkSize,
            @CXXValue @FFITypeAlias(GAR_ID_TYPE) long srcChunkSize,
            @CXXValue @FFITypeAlias(GAR_ID_TYPE) long dstChunkSize,
            @CXXValue boolean directed,
            @CXXReference StdVector<StdSharedPtr<AdjacentList>> adjacentLists,
            @CXXReference StdVector<StdSharedPtr<PropertyGroup>> propertyGroups);

    @FFINameAlias("CreateGraphInfo")
    @CXXValue
    StdSharedPtr<GraphInfo> createGraphInfo(
            @CXXReference StdString name,
            @CXXReference StdVector<StdSharedPtr<VertexInfo>> vertexInfos,
            @CXXReference StdVector<StdSharedPtr<EdgeInfo>> edgeInfos,
            @CXXReference StdString prefix,
            @CXXValue StdSharedPtr<InfoVersion> version);

    @FFINameAlias("CreateGraphInfo")
    @CXXValue
    StdSharedPtr<GraphInfo> createGraphInfo(
            @CXXReference StdString name,
            @CXXReference StdVector<StdSharedPtr<VertexInfo>> vertexInfos,
            @CXXReference StdVector<StdSharedPtr<EdgeInfo>> edgeInfos,
            @CXXReference StdString prefix);

    @FFINameAlias("boolean")
    @CXXReference
    @FFIConst
    StdSharedPtr<DataType> booleanType();

    @FFINameAlias("int32")
    @CXXReference
    @FFIConst
    StdSharedPtr<DataType> int32Type();

    @FFINameAlias("int64")
    @CXXReference
    @FFIConst
    StdSharedPtr<DataType> int64Type();

    @FFINameAlias("float32")
    @CXXReference
    @FFIConst
    StdSharedPtr<DataType> float32Type();

    @FFINameAlias("float64")
    @CXXReference
    @FFIConst
    StdSharedPtr<DataType> float64Type();

    @FFINameAlias("string")
    @CXXReference
    @FFIConst
    StdSharedPtr<DataType> stringType();

    @FFINameAlias("date")
    @CXXReference
    @FFIConst
    StdSharedPtr<DataType> dateType();

    @FFINameAlias("timestamp")
    @CXXReference
    @FFIConst
    StdSharedPtr<DataType> timestampType();
}
