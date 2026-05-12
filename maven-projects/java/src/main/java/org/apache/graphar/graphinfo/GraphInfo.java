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

import static org.apache.graphar.util.CppClassName.GAR_GRAPH_INFO;
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
import org.apache.graphar.util.InfoVersion;
import org.apache.graphar.util.Result;
import org.apache.graphar.util.Status;

/** GraphInfo is a class to store the graph meta information. */
@FFIGen
@FFITypeAlias(GAR_GRAPH_INFO)
@CXXHead(GAR_GRAPH_INFO_H)
public interface GraphInfo extends CXXPointer {
    /**
     * Loads the input file as a `GraphInfo` instance.
     *
     * @param path The path of the YAML file.
     * @return A Result object containing the GraphInfo instance, or a Status object indicating an
     *     error.
     */
    static Result<StdSharedPtr<GraphInfo>> load(String path) {
        StdString stdString = StdString.create(path);
        Result<StdSharedPtr<GraphInfo>> result = Static.INSTANCE.Load(stdString);
        stdString.delete();
        return result;
    }

    /**
     * Get the vertex info with the given label.
     *
     * @param label The label of the vertex.
     * @return A Result object containing the vertex info, or a Status object indicating an error.
     */
    default StdSharedPtr<VertexInfo> getVertexInfo(String label) {
        StdString stdString = StdString.create(label);
        StdSharedPtr<VertexInfo> result = getVertexInfo(stdString);
        stdString.delete();
        return result;
    }

    /**
     * Get the edge info with the given source vertex label, edge label, and destination vertex
     * label.
     *
     * @param srcLabel The label of the source vertex.
     * @param edgeLabel The label of the edge.
     * @param dstLabel The label of the destination vertex.
     * @return A Result object containing the edge info, or a Status object indicating an error.
     */
    default StdSharedPtr<@CXXReference EdgeInfo> getEdgeInfo(
            String srcLabel, String edgeLabel, String dstLabel) {
        StdString stdStrSrcLabel = StdString.create(srcLabel);
        StdString stdStrEdgeLabel = StdString.create(edgeLabel);
        StdString stdStrDstLabel = StdString.create(dstLabel);
        StdSharedPtr<@CXXReference EdgeInfo> result =
                getEdgeInfo(stdStrSrcLabel, stdStrEdgeLabel, stdStrDstLabel);
        stdStrSrcLabel.delete();
        stdStrEdgeLabel.delete();
        stdStrDstLabel.delete();
        return result;
    }

    /** Get the vertex info index with the given label. */
    default int getVertexInfoIndex(String label) {
        StdString stdLabel = StdString.create(label);
        int index = getVertexInfoIndex(stdLabel);
        stdLabel.delete();
        return index;
    }

    /**
     * Get the edge info index with the given source vertex label, edge label, and destination
     * label.
     */
    default int getEdgeInfoIndex(String srcLabel, String edgeLabel, String dstLabel) {
        StdString stdStrSrcLabel = StdString.create(srcLabel);
        StdString stdStrEdgeLabel = StdString.create(edgeLabel);
        StdString stdStrDstLabel = StdString.create(dstLabel);
        int index = getEdgeInfoIndex(stdStrSrcLabel, stdStrEdgeLabel, stdStrDstLabel);
        stdStrSrcLabel.delete();
        stdStrEdgeLabel.delete();
        stdStrDstLabel.delete();
        return index;
    }

    /**
     * Adds a vertex info to the GraphInfo instance and returns a new GraphInfo.
     *
     * @param vertexInfo The vertex info to add.
     * @return A Status object indicating the success or failure of the operation. Returns
     *     InvalidOperation if the vertex info is already contained.
     */
    @FFINameAlias("AddVertex")
    @CXXValue
    Result<StdSharedPtr<GraphInfo>> addVertex(@CXXReference StdSharedPtr<VertexInfo> vertexInfo);

    /**
     * Adds an edge info to the GraphInfo instance and returns a new GraphInfo.
     *
     * @param edgeInfo The edge info to add.
     * @return A Status object indicating the success or failure of the operation. Returns
     *     `InvalidOperation` if the edge info is already contained.
     */
    @FFINameAlias("AddEdge")
    @CXXValue
    Result<StdSharedPtr<GraphInfo>> addEdge(@CXXReference StdSharedPtr<EdgeInfo> edgeInfo);

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
    @FFIConst
    @CXXReference
    StdString getPrefix();

    /**
     * Get the vertex infos of graph info
     *
     * @return vertex infos of graph info
     */
    @FFINameAlias("GetVertexInfos")
    @FFIConst
    @CXXReference
    StdVector<StdSharedPtr<@CXXReference VertexInfo>> getVertexInfos();

    /**
     * Get the edge infos of graph info
     *
     * @return edge infos of graph info
     */
    @FFINameAlias("GetEdgeInfos")
    @FFIConst
    @CXXReference
    StdVector<StdSharedPtr<@CXXReference EdgeInfo>> getEdgeInfos();

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
    @CXXValue
    StdSharedPtr<VertexInfo> getVertexInfo(@CXXReference StdString label);

    @FFINameAlias("GetEdgeInfo")
    @FFIConst
    @CXXValue
    StdSharedPtr<@CXXReference EdgeInfo> getEdgeInfo(
            @CXXReference StdString srcLabel,
            @CXXReference StdString edgeLabel,
            @CXXReference StdString dstLabel);

    /** Get the vertex info index with the given label. */
    @FFINameAlias("GetVertexInfoIndex")
    @FFIConst
    int getVertexInfoIndex(@CXXReference StdString label);

    /**
     * Get the edge info index with the given source vertex label, edge label, and destination
     * label.
     */
    @FFINameAlias("GetEdgeInfoIndex")
    @FFIConst
    @CXXValue
    int getEdgeInfoIndex(
            @CXXReference StdString srcLabel,
            @CXXReference StdString edgeLabel,
            @CXXReference StdString dstLabel);

    @FFINameAlias("GetVertexInfoByIndex")
    @FFIConst
    @CXXValue
    StdSharedPtr<VertexInfo> getVertexInfoByIndex(int index);

    @FFINameAlias("GetEdgeInfoByIndex")
    @FFIConst
    @CXXValue
    StdSharedPtr<EdgeInfo> getEdgeInfoByIndex(int index);

    @FFINameAlias("version")
    @CXXReference
    StdSharedPtr<InfoVersion> getInfoVersion();

    @FFIGen
    @CXXHead(GAR_GRAPH_INFO_H)
    @FFILibrary(value = GAR_GRAPH_INFO, namespace = GAR_GRAPH_INFO)
    interface Static {
        Static INSTANCE = FFITypeFactory.getLibrary(Static.class);

        @CXXValue
        Result<StdSharedPtr<GraphInfo>> Load(@CXXReference StdString path);
    }
}
