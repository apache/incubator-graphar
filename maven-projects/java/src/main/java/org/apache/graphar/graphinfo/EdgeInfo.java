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

package org.apache.graphar.graphinfo;

import static org.apache.graphar.util.CppClassName.GAR_EDGE_INFO;
import static org.apache.graphar.util.CppClassName.GAR_ID_TYPE;
import static org.apache.graphar.util.CppHeaderName.GAR_GRAPH_INFO_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIConst;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFILibrary;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import org.apache.graphar.stdcxx.StdSharedPtr;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.stdcxx.StdVector;
import org.apache.graphar.types.AdjListType;
import org.apache.graphar.types.DataType;
import org.apache.graphar.util.InfoVersion;
import org.apache.graphar.util.Result;
import org.apache.graphar.util.Status;
import org.apache.graphar.util.Yaml;

/** EdgeInfo is a class that stores metadata information about an edge. */
@FFIGen
@FFITypeAlias(GAR_EDGE_INFO)
@CXXHead(GAR_GRAPH_INFO_H)
public interface EdgeInfo extends CXXPointer {

    @FFINameAlias("AddAdjacentList")
    @CXXValue
    Result<StdSharedPtr<EdgeInfo>> addAdjacentList(@CXXValue StdSharedPtr<AdjacentList> adjList);

    /** Add a property group to edge info for the given adjacency list type. */
    @FFINameAlias("AddPropertyGroup")
    @CXXValue
    Result<StdSharedPtr<EdgeInfo>> addPropertyGroup(
            @CXXValue StdSharedPtr<PropertyGroup> propertyGroup);

    /**
     * Get the label of the source vertex.
     *
     * @return The label of the source vertex.
     */
    @FFINameAlias("GetSrcLabel")
    @CXXValue
    StdString getSrcLabel();

    /**
     * Get the label of the edge.
     *
     * @return The label of the edge.
     */
    @FFINameAlias("GetEdgeLabel")
    @CXXValue
    StdString getEdgeLabel();

    /**
     * Get the label of the destination vertex.
     *
     * @return The label of the destination vertex.
     */
    @FFINameAlias("GetDstLabel")
    @CXXValue
    StdString getDstLabel();

    /**
     * Get the number of edges in each edge chunk.
     *
     * @return The number of edges in each edge chunk.
     */
    @FFINameAlias("GetChunkSize")
    @CXXValue
    long getChunkSize();

    /**
     * Get the number of source vertices in each vertex chunk.
     *
     * @return The number of source vertices in each vertex chunk.
     */
    @FFINameAlias("GetSrcChunkSize")
    @CXXValue
    long getSrcChunkSize();

    /**
     * Get the number of destination vertices in each vertex chunk.
     *
     * @return The number of destination vertices in each vertex chunk.
     */
    @FFINameAlias("GetDstChunkSize")
    @CXXValue
    long getDstChunkSize();

    /**
     * Get the path prefix of the edge.
     *
     * @return The path prefix of the edge.
     */
    @FFINameAlias("GetPrefix")
    @FFIConst
    @CXXReference
    StdString getPrefix();

    /**
     * Returns whether the edge is directed.
     *
     * @return True if the edge is directed, false otherwise.
     */
    @FFINameAlias("IsDirected")
    boolean isDirected();

    /**
     * Get the version info of the edge.
     *
     * @return The version info of the edge.
     */
    @FFINameAlias("version")
    @CXXReference
    StdSharedPtr<InfoVersion> getVersion();

    /**
     * Return whether the edge info contains the adjacency list information.
     *
     * @param adjListType The adjacency list type.
     * @return True if the edge info contains the adjacency list information, false otherwise.
     */
    @FFINameAlias("HasAdjacentListType")
    boolean hasAdjacentListType(@CXXValue AdjListType adjListType);

    /**
     * Returns whether the edge info contains the given property group for the specified adjacency
     * list type.
     *
     * @param propertyGroup Property group to check.
     * @return True if the edge info contains the property group, false otherwise.
     */
    @FFINameAlias("HasPropertyGroup")
    boolean hasPropertyGroup(@CXXReference StdSharedPtr<PropertyGroup> propertyGroup);

    /**
     * Returns whether the edge info contains the given property for any adjacency list type.
     *
     * @param property Property name to check.
     * @return True if the edge info contains the property, false otherwise.
     */
    @FFINameAlias("HasProperty")
    boolean hasPropertyGroup(@CXXReference StdString property);

    @FFINameAlias("GetAdjacentList")
    @CXXValue
    StdSharedPtr<AdjacentList> getAdjacentList(@CXXValue AdjListType adjListType);

    /** Get the property groups. */
    @FFINameAlias("GetPropertyGroups")
    @FFIConst
    @CXXReference
    StdVector<StdSharedPtr<PropertyGroup>> getPropertyGroups();

    /**
     * Get the property group containing the given property.
     *
     * @param property Property name.
     * @return Property group may be nullptr if the property is not found.
     */
    @FFINameAlias("GetPropertyGroup")
    @CXXValue
    StdSharedPtr<PropertyGroup> getPropertyGroup(@CXXReference StdString property);

    /**
     * Get the file path for the number of vertices.
     *
     * @param adjListType The adjacency list type.
     * @return A Result object containing the file path for the number of edges, or a Status object
     *     indicating an error.
     */
    @FFINameAlias("GetVerticesNumFilePath")
    @CXXValue
    Result<StdString> getVerticesNumFilePath(@CXXValue AdjListType adjListType);

    /**
     * Get the file path for the number of edges.
     *
     * @param vertexChunkIndex the vertex chunk index
     * @param adjListType The adjacency list type.
     * @return A Result object containing the file path for the number of edges, or a Status object
     *     indicating an error.
     */
    @FFINameAlias("GetEdgesNumFilePath")
    @CXXValue
    Result<StdString> getEdgesNumFilePath(
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex, @CXXValue AdjListType adjListType);

    /**
     * Get the file path of adj list topology chunk
     *
     * @param vertexChunkIndex the vertex chunk index
     * @param edgeChunkIndex index of edge adj list chunk of the vertex chunk
     * @param adjListType The adjacency list type.
     */
    @FFINameAlias("GetAdjListFilePath")
    @CXXValue
    Result<StdString> getAdjListFilePath(
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long edgeChunkIndex,
            @CXXValue AdjListType adjListType);

    /**
     * Get the path prefix of the adjacency list topology chunk for the given adjacency list type.
     *
     * @param adjListType The adjacency list type.
     * @return A Result object containing the directory, or a Status object indicating an error.
     */
    @FFINameAlias("GetAdjListPathPrefix")
    @CXXValue
    Result<StdString> getAdjListPathPrefix(@CXXValue AdjListType adjListType);

    /**
     * Get the adjacency list offset chunk file path of vertex chunk the offset chunks is aligned
     * with the vertex chunks
     *
     * @param vertexChunkIndex index of vertex chunk
     * @param adjListType The adjacency list type.
     */
    @FFINameAlias("GetAdjListOffsetFilePath")
    @CXXValue
    Result<StdString> getAdjListOffsetFilePath(
            @FFITypeAlias(GAR_ID_TYPE) long vertexChunkIndex, @CXXValue AdjListType adjListType);

    /**
     * Get the path prefix of the adjacency list offset chunk for the given adjacency list type.
     *
     * @param adjListType The adjacency list type.
     * @return A Result object containing the path prefix, or a Status object indicating an error.
     */
    @FFINameAlias("GetOffsetPathPrefix")
    @CXXValue
    Result<StdString> getOffsetPathPrefix(@CXXValue AdjListType adjListType);

    /**
     * Get the chunk file path of adj list property group the property group chunks is aligned with
     * the adj list topology chunks
     *
     * @param propertyGroup property group
     * @param adjListType adj list type that the property group belongs to
     * @param vertexChunkIndex the vertex chunk index
     * @param edgeChunkIndex index of edge property group chunk of the vertex chunk
     */
    @FFINameAlias("GetPropertyFilePath")
    @CXXValue
    Result<StdString> getPropertyFilePath(
            @CXXReference StdSharedPtr<PropertyGroup> propertyGroup,
            @CXXValue AdjListType adjListType,
            @FFINameAlias(GAR_ID_TYPE) long vertexChunkIndex,
            @FFITypeAlias(GAR_ID_TYPE) long edgeChunkIndex);

    /**
     * Get the path prefix of the property group chunk for the given adjacency list type.
     *
     * @param propertyGroup property group.
     * @param adjListType The adjacency list type.
     * @return A Result object containing the path prefix, or a Status object indicating an error.
     */
    @FFINameAlias("GetPropertyGroupPathPrefix")
    @CXXValue
    Result<StdString> getPropertyGroupPathPrefix(
            @CXXReference StdSharedPtr<PropertyGroup> propertyGroup,
            @CXXValue AdjListType adjListType);

    /**
     * Get the data type of the specified property.
     *
     * @param propertyName The name of the property.
     * @return A Result object containing the data type of the property, or a KeyError Status object
     *     if the property is not found.
     */
    @FFINameAlias("GetPropertyType")
    @CXXValue
    Result<StdSharedPtr<DataType>> getPropertyType(@CXXReference StdString propertyName);

    /**
     * Returns whether the specified property is a primary key.
     *
     * @param propertyName The name of the property.
     * @return A Result object containing a bool indicating whether the property is a primary key,
     *     or a KeyError Status object if the property is not found.
     */
    @FFINameAlias("IsPrimaryKey")
    @CXXValue
    boolean isPrimaryKey(@CXXReference StdString propertyName);

    /**
     * Saves the edge info to a YAML file.
     *
     * @param fileName The name of the file to save to.
     * @return A Status object indicating success or failure.
     */
    @FFINameAlias("Save")
    @CXXValue
    Status save(@CXXReference StdString fileName);

    /**
     * Returns the edge info as a YAML formatted string.
     *
     * @return A Result object containing the YAML string, or a Status object indicating an error.
     */
    @FFINameAlias("Dump")
    @CXXValue
    Result<StdString> dump();

    /**
     * Returns whether the edge info is validated.
     *
     * @return True if the edge info is valid, False otherwise.
     */
    @FFINameAlias("IsValidated")
    boolean isValidated();

    static Result<StdSharedPtr<EdgeInfo>> load(StdSharedPtr<Yaml> yaml) {
        return Static.INSTANCE.Load(yaml);
    }

    @FFIGen
    @CXXHead(GAR_GRAPH_INFO_H)
    @FFILibrary(value = GAR_EDGE_INFO, namespace = GAR_EDGE_INFO)
    interface Static {
        Static INSTANCE = FFITypeFactory.getLibrary(EdgeInfo.Static.class);

        @CXXValue
        Result<StdSharedPtr<EdgeInfo>> Load(@CXXValue StdSharedPtr<Yaml> yaml);
    }
}
