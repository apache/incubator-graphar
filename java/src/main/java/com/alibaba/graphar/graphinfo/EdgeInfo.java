/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.graphinfo;

import static com.alibaba.graphar.util.CppClassName.GAR_EDGE_INFO;
import static com.alibaba.graphar.util.CppClassName.GAR_ID_TYPE;
import static com.alibaba.graphar.util.CppHeaderName.GAR_GRAPH_INFO_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.stdcxx.StdVector;
import com.alibaba.graphar.types.AdjListType;
import com.alibaba.graphar.types.DataType;
import com.alibaba.graphar.types.FileType;
import com.alibaba.graphar.util.InfoVersion;
import com.alibaba.graphar.util.Result;
import com.alibaba.graphar.util.Status;

/** EdgeInfo is a class that stores metadata information about an edge. */
@FFIGen
@FFITypeAlias(GAR_EDGE_INFO)
@CXXHead(GAR_GRAPH_INFO_H)
public interface EdgeInfo extends CXXPointer {

  Factory factory = FFITypeFactory.getFactory(EdgeInfo.class);

  /**
   * Add an adjacency list information to the edge info. The adjacency list information indicating
   * the adjacency list stored with CSR, CSC, or COO format.
   *
   * @param adjListType The type of the adjacency list to add.
   * @param fileType The file type of the adjacency list topology and offset chunk file.
   * @param prefix The prefix of the adjacency list topology chunk (optional, default is empty).
   * @return A Status object indicating success or an error if the adjacency list type has already
   *     been added.
   */
  @FFINameAlias("AddAdjList")
  @CXXValue
  Status addAdjList(
      @CXXValue AdjListType adjListType,
      @CXXValue FileType fileType,
      @CXXReference StdString prefix);

  /**
   * Add an adjacency list information to the edge info. The adjacency list information indicating
   * the adjacency list stored with CSR, CSC, or COO format.
   *
   * @param adjListType The type of the adjacency list to add.
   * @param fileType The file type of the adjacency list topology and offset chunk file.
   * @return A Status object indicating success or an error if the adjacency list type has already
   *     been added.
   */
  @FFINameAlias("AddAdjList")
  @CXXValue
  Status addAdjList(@CXXValue AdjListType adjListType, @CXXValue FileType fileType);

  /**
   * Add a property group to edge info for the given adjacency list type.
   *
   * @param propertyGroup Property group to add.
   * @param adjListType Adjacency list type to add property group to.
   * @return A Status object indicating success or an error if adj_list_type is not supported by
   *     edge info or if the property group is already added to the adjacency list type.
   */
  @FFINameAlias("AddPropertyGroup")
  @CXXValue
  Status addPropertyGroup(
      @CXXReference PropertyGroup propertyGroup, @CXXValue AdjListType adjListType);

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
  @CXXValue
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
  @FFINameAlias("GetVersion")
  @CXXReference
  InfoVersion getVersion();

  /**
   * Return whether the edge info contains the adjacency list information.
   *
   * @param adjListType The adjacency list type.
   * @return True if the edge info contains the adjacency list information, false otherwise.
   */
  @FFINameAlias("ContainAdjList")
  boolean containAdjList(@CXXValue AdjListType adjListType);

  /**
   * Returns whether the edge info contains the given property group for the specified adjacency
   * list type.
   *
   * @param propertyGroup Property group to check.
   * @param adjListType Adjacency list type the property group belongs to.
   * @return True if the edge info contains the property group, false otherwise.
   */
  @FFINameAlias("ContainPropertyGroup")
  boolean containPropertyGroup(
      @CXXReference PropertyGroup propertyGroup, @CXXValue AdjListType adjListType);

  /**
   * Returns whether the edge info contains the given property for any adjacency list type.
   *
   * @param property Property name to check.
   * @return True if the edge info contains the property, false otherwise.
   */
  @FFINameAlias("ContainProperty")
  boolean containProperty(@CXXReference StdString property);

  /**
   * Get the file type of the adjacency list topology and offset chunk file for the given adjacency
   * list type.
   *
   * @param adjListType The adjacency list type.
   * @return A Result object containing the file type, or a Status object indicating an KeyError if
   *     the adjacency list type is not found in the edge info.
   */
  @FFINameAlias("GetFileType")
  @CXXValue
  Result<FileType> getFileType(@CXXValue AdjListType adjListType);

  /**
   * Get the property groups for the given adjacency list type.
   *
   * @param adjListType Adjacency list type.
   * @return A Result object containing reference to the property groups for the given adjacency
   *     list type, or a Status object indicating an KeyError if the adjacency list type is not
   *     found in the edge info.
   */
  @FFINameAlias("GetPropertyGroups")
  @CXXValue
  @FFITypeAlias("GraphArchive::Result<const std::vector<GraphArchive::PropertyGroup>>")
  Result<@CXXReference StdVector<PropertyGroup>> getPropertyGroups(
      @CXXValue AdjListType adjListType);

  /**
   * Get the property group containing the given property and for the specified adjacency list type.
   *
   * @param property Property name.
   * @param adjListType Adjacency list type.
   * @return A Result object containing reference to the property group, or a Status object
   *     indicating an KeyError if the adjacency list type is not found in the edge info.
   */
  @FFINameAlias("GetPropertyGroup")
  @CXXValue
  Result<@CXXReference PropertyGroup> getPropertyGroup(
      @CXXReference StdString property, @CXXValue AdjListType adjListType);

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
   * Get the adjacency list offset chunk file path of vertex chunk the offset chunks is aligned with
   * the vertex chunks
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
      @CXXReference PropertyGroup propertyGroup,
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
      @CXXReference PropertyGroup propertyGroup, @CXXValue AdjListType adjListType);

  /**
   * Get the data type of the specified property.
   *
   * @param propertyName The name of the property.
   * @return A Result object containing the data type of the property, or a KeyError Status object
   *     if the property is not found.
   */
  @FFINameAlias("GetPropertyType")
  @CXXValue
  Result<DataType> getPropertyType(@CXXReference StdString propertyName);

  /**
   * Returns whether the specified property is a primary key.
   *
   * @param propertyName The name of the property.
   * @return A Result object containing a bool indicating whether the property is a primary key, or
   *     a KeyError Status object if the property is not found.
   */
  @FFINameAlias("IsPrimaryKey")
  @CXXValue
  @FFITypeAlias("GraphArchive::Result<bool>")
  Result<Boolean> isPrimaryKey(@CXXReference StdString propertyName);

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
   * Returns a new EdgeInfo object with the specified adjacency list type added to with given
   * metadata.
   *
   * @param adjListType The type of the adjacency list to add.
   * @param fileType The file type of the adjacency list topology and offset chunk file.
   * @param prefix The prefix of the adjacency list topology chunk.
   * @return A Result object containing the new EdgeInfo object, or a Status object indicating an
   *     error.
   */
  @FFINameAlias("ExtendAdjList")
  @CXXValue
  Result<EdgeInfo> extendAdjList(
      @CXXValue AdjListType adjListType,
      @CXXValue FileType fileType,
      @CXXReference StdString prefix);

  /**
   * Returns a new EdgeInfo object with the specified adjacency list type added to with given
   * metadata.
   *
   * @param adjListType The type of the adjacency list to add.
   * @param fileType The file type of the adjacency list topology and offset chunk file.
   * @return A Result object containing the new EdgeInfo object, or a Status object indicating an
   *     error.
   */
  @FFINameAlias("ExtendAdjList")
  @CXXValue
  Result<EdgeInfo> extendAdjList(@CXXValue AdjListType adjListType, @CXXValue FileType fileType);

  /**
   * Returns a new EdgeInfo object with the specified property group added to given adjacency list
   * type.
   *
   * @param propertyGroup The PropertyGroup object to add.
   * @param adjListType The adjacency list type to add the property group to.
   * @return A Result object containing the new EdgeInfo object, or a Status object indicating an
   *     error.
   */
  @FFINameAlias("ExtendPropertyGroup")
  @CXXValue
  Result<EdgeInfo> extendPropertyGroup(
      @CXXReference PropertyGroup propertyGroup, @CXXValue AdjListType adjListType);

  /**
   * Returns whether the edge info is validated.
   *
   * @return True if the edge info is valid, False otherwise.
   */
  @FFINameAlias("IsValidated")
  boolean isValidated();

  @FFIFactory
  interface Factory {
    EdgeInfo create();

    /**
     * Construct an EdgeInfo object with the given metadata information.
     *
     * @param srcLabel The label of the source vertex.
     * @param edgeLabel The label of the edge.
     * @param dstLabel The label of the destination vertex.
     * @param chunkSize The number of edges in each edge chunk.
     * @param srcChunkSize The number of source vertices in each vertex chunk.
     * @param dstChunkSize The number of destination vertices in each vertex chunk.
     * @param directed Whether the edge is directed.
     * @param version The version of the edge info.
     * @param prefix The path prefix of the edge info.
     */
    EdgeInfo create(
        @CXXReference StdString srcLabel,
        @CXXReference StdString edgeLabel,
        @CXXReference StdString dstLabel,
        long chunkSize,
        long srcChunkSize,
        long dstChunkSize,
        boolean directed,
        @CXXReference InfoVersion version,
        @CXXReference StdString prefix);

    /**
     * Construct an EdgeInfo object with the given metadata information.
     *
     * @param srcLabel The label of the source vertex.
     * @param edgeLabel The label of the edge.
     * @param dstLabel The label of the destination vertex.
     * @param chunkSize The number of edges in each edge chunk.
     * @param srcChunkSize The number of source vertices in each vertex chunk.
     * @param dstChunkSize The number of destination vertices in each vertex chunk.
     * @param directed Whether the edge is directed.
     * @param version The version of the edge info.
     */
    EdgeInfo create(
        @CXXReference StdString srcLabel,
        @CXXReference StdString edgeLabel,
        @CXXReference StdString dstLabel,
        long chunkSize,
        long srcChunkSize,
        long dstChunkSize,
        boolean directed,
        @CXXReference InfoVersion version);

    EdgeInfo create(@CXXReference EdgeInfo other);
  }
}
