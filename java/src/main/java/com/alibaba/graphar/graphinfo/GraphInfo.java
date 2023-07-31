/* Copyright 2022 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.alibaba.graphar.graphinfo;

import static com.alibaba.graphar.utils.CppClassName.GAR_GRAPH_INFO;
import static com.alibaba.graphar.utils.CppHeaderName.GAR_GRAPH_INFO_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIConst;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFILibrary;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphar.stdcxx.StdMap;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.types.AdjListType;
import com.alibaba.graphar.utils.InfoVersion;
import com.alibaba.graphar.utils.Result;
import com.alibaba.graphar.utils.Status;

/** GraphInfo is a class to store the graph meta information. */
@FFIGen
@FFITypeAlias(GAR_GRAPH_INFO)
@CXXHead(GAR_GRAPH_INFO_H)
public interface GraphInfo extends CXXPointer {

  Factory factory = FFITypeFactory.getFactory(GraphInfo.class);

  static GraphInfo create(String graphName, InfoVersion version, String prefix) {
    StdString stdGraphName = StdString.create(graphName);
    StdString stdPrefix = StdString.create(prefix);
    GraphInfo res = factory.create(stdGraphName, version, stdPrefix);
    stdGraphName.delete();
    stdPrefix.delete();
    return res;
  }

  /**
   * Loads the input file as a `GraphInfo` instance.
   *
   * @param path The path of the YAML file.
   * @return A Result object containing the GraphInfo instance, or a Status object indicating an
   *     error.
   */
  static Result<GraphInfo> load(String path) {
    StdString stdString = StdString.create(path);
    Result<GraphInfo> result = Static.INSTANCE.Load(stdString);
    stdString.delete();
    return result;
  }

  /**
   * Get the vertex info with the given label.
   *
   * @param label The label of the vertex.
   * @return A Result object containing the vertex info, or a Status object indicating an error.
   */
  default Result<@CXXReference VertexInfo> getVertexInfo(String label) {
    StdString stdString = StdString.create(label);
    Result<@CXXReference VertexInfo> result = getVertexInfo(stdString);
    stdString.delete();
    return result;
  }

  /**
   * Get the edge info with the given source vertex label, edge label, and destination vertex label.
   *
   * @param srcLabel The label of the source vertex.
   * @param edgeLabel The label of the edge.
   * @param dstLabel The label of the destination vertex.
   * @return A Result object containing the edge info, or a Status object indicating an error.
   */
  default Result<@CXXReference EdgeInfo> getEdgeInfo(
      String srcLabel, String edgeLabel, String dstLabel) {
    StdString stdStrSrcLabel = StdString.create(srcLabel);
    StdString stdStrEdgeLabel = StdString.create(edgeLabel);
    StdString stdStrDstLabel = StdString.create(dstLabel);
    Result<@CXXReference EdgeInfo> result =
        getEdgeInfo(stdStrSrcLabel, stdStrEdgeLabel, stdStrDstLabel);
    stdStrSrcLabel.delete();
    stdStrEdgeLabel.delete();
    stdStrDstLabel.delete();
    return result;
  }

  /**
   * Get the property group of vertex by label and property
   *
   * @param label vertex label
   * @param property vertex property that belongs to the group
   */
  @FFINameAlias("GetVertexPropertyGroup")
  @CXXValue
  Result<@CXXReference PropertyGroup> getVertexPropertyGroup(
      @CXXReference StdString label, @CXXReference StdString property);

  /**
   * Get the property group of edge by label, property and adj list type
   *
   * @param srcLabel source vertex label
   * @param edgeLabel edge label
   * @param dstLabel destination vertex label
   * @param property edge property that belongs to the group
   * @param adjListType adj list type of edge
   */
  @FFINameAlias("GetEdgePropertyGroup")
  @CXXValue
  Result<@CXXReference PropertyGroup> getEdgePropertyGroup(
      @CXXReference StdString srcLabel,
      @CXXReference StdString edgeLabel,
      @CXXReference StdString dstLabel,
      @CXXReference StdString property,
      @CXXValue AdjListType adjListType);

  /**
   * Adds a vertex info to the GraphInfo instance.
   *
   * @param vertexInfo The vertex info to add.
   * @return A Status object indicating the success or failure of the operation. Returns
   *     InvalidOperation if the vertex info is already contained.
   */
  @FFINameAlias("AddVertex")
  @CXXValue
  Status addVertex(@CXXReference VertexInfo vertexInfo);

  /**
   * Adds an edge info to the GraphInfo instance.
   *
   * @param edgeInfo The edge info to add.
   * @return A Status object indicating the success or failure of the operation. Returns
   *     `InvalidOperation` if the edge info is already contained.
   */
  @FFINameAlias("AddEdge")
  @CXXValue
  Status addEdge(@CXXReference EdgeInfo edgeInfo);

  /**
   * Add a vertex info path to graph info instance.
   *
   * @param path The vertex info path to add
   */
  @FFINameAlias("AddVertexInfoPath")
  void addVertexInfoPath(@CXXReference StdString path);

  /**
   * Add an edge info path to graph info instance.
   *
   * @param path The edge info path to add
   */
  @FFINameAlias("AddEdgeInfoPath")
  void addEdgeInfoPath(@CXXReference StdString path);

  /**
   * Get the name of the graph.
   *
   * @return The name of the graph.
   */
  @FFINameAlias("GetName")
  @CXXValue
  StdString getName();

  /**
   * Get the absolute path prefix of the chunk files.
   *
   * @return The absolute path prefix of the chunk files.
   */
  @FFINameAlias("GetPrefix")
  @CXXValue
  StdString getPrefix();

  /**
   * Get the vertex infos of graph info
   *
   * @return vertex infos of graph info
   */
  @FFINameAlias("GetVertexInfos")
  @FFIConst
  @CXXReference
  StdMap<StdString, @CXXReference VertexInfo> getVertexInfos();

  /**
   * Get the edge infos of graph info
   *
   * @return edge infos of graph info
   */
  @FFINameAlias("GetEdgeInfos")
  @FFIConst
  @CXXReference
  StdMap<StdString, @CXXReference EdgeInfo> getEdgeInfos();

  /**
   * Saves the graph info to a YAML file.
   *
   * @param path The name of the file to save to.
   * @return A Status object indicating success or failure.
   */
  @FFINameAlias("Save")
  @CXXValue
  Status save(@FFIConst @CXXReference StdString path);

  /**
   * Returns the graph info as a YAML formatted string.
   *
   * @return A Result object containing the YAML string, or a Status object indicating an error.
   */
  @FFINameAlias("Dump")
  @CXXValue
  Result<StdString> dump();

  /**
   * Returns whether the graph info is validated.
   *
   * @return True if the graph info is valid, False otherwise.
   */
  @FFINameAlias("IsValidated")
  boolean isValidated();

  @FFINameAlias("GetVertexInfo")
  @FFIConst
  @CXXValue
  Result<@CXXReference VertexInfo> getVertexInfo(@CXXReference StdString label);

  @FFINameAlias("GetEdgeInfo")
  @FFIConst
  @CXXValue
  Result<@CXXReference EdgeInfo> getEdgeInfo(
      @CXXReference StdString srcLabel,
      @CXXReference StdString edgeLabel,
      @CXXReference StdString dstLabel);

  @FFINameAlias("GetVersion")
  @CXXReference
  InfoVersion getInfoVersion();

  @FFIFactory
  interface Factory {
    /**
     * Constructs a GraphInfo instance.
     *
     * @param graphName The name of the graph.
     * @param version The version of the graph info.
     * @param prefix The absolute path prefix to store chunk files of the graph.
     */
    GraphInfo create(
        @CXXReference StdString graphName,
        @CXXReference InfoVersion version,
        @CXXReference StdString prefix);

    /**
     * Constructs a GraphInfo instance.
     *
     * @param graphName The name of the graph.
     * @param version The version of the graph info.
     */
    GraphInfo create(@CXXReference StdString graphName, @CXXReference InfoVersion version);
  }

  @FFIGen
  @CXXHead(GAR_GRAPH_INFO_H)
  @FFILibrary(value = GAR_GRAPH_INFO, namespace = GAR_GRAPH_INFO)
  interface Static {
    Static INSTANCE = FFITypeFactory.getLibrary(Static.class);

    @CXXValue
    Result<GraphInfo> Load(@CXXReference StdString path);
  }
}
