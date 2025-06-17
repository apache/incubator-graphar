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

import static org.apache.graphar.util.CppClassName.GAR_ID_TYPE;
import static org.apache.graphar.util.CppClassName.GAR_VERTEX_INFO;
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
import org.apache.graphar.util.Yaml;

/** VertexInfo is a class that stores metadata information about a vertex. */
@FFIGen
@FFITypeAlias(GAR_VERTEX_INFO)
@CXXHead(GAR_GRAPH_INFO_H)
public interface VertexInfo extends CXXPointer {
    /**
     * Adds a property group to the vertex info.
     *
     * @param propertyGroup The PropertyGroup object to add.
     * @return A Status object indicating success or failure.
     */
    @FFINameAlias("AddPropertyGroup")
    @CXXValue
    Result<StdSharedPtr<VertexInfo>> addPropertyGroup(
            @CXXValue StdSharedPtr<PropertyGroup> propertyGroup);

    /**
     * Get the label of the vertex.
     *
     * @return The label of the vertex.
     */
    @FFINameAlias("GetLabel")
    @CXXValue
    StdString getLabel();

    /**
     * Get the chunk size of the vertex.
     *
     * @return The chunk size of the vertex.
     */
    @FFINameAlias("GetChunkSize")
    long getChunkSize();

    /**
     * Get the path prefix of the vertex.
     *
     * @return The path prefix of the vertex.
     */
    @FFINameAlias("GetPrefix")
    @CXXReference
    @FFIConst
    StdString getPrefix();

    /**
     * Get the version info of the vertex.
     *
     * @return The version info of the vertex.
     */
    @FFINameAlias("version")
    @FFIConst
    @CXXReference
    StdSharedPtr<InfoVersion> getVersion();

    /**
     * Get the property groups of the vertex.
     *
     * @return A vector of PropertyGroup objects for the vertex.
     */
    @FFINameAlias("GetPropertyGroups")
    @FFIConst
    @CXXReference
    StdVector<StdSharedPtr<PropertyGroup>> getPropertyGroups();

    /**
     * Get the property group that contains the specified property.
     *
     * @param propertyName The name of the property.
     * @return A Result object containing the PropertyGroup object, or a KeyError Status object if
     *     the property is not found.
     */
    @FFINameAlias("GetPropertyGroup")
    @CXXValue
    StdSharedPtr<PropertyGroup> getPropertyGroup(@CXXReference StdString propertyName);

    /**
     * Get whether the vertex info contains the specified property.
     *
     * @param propertyName The name of the property.
     * @return True if the property exists in the vertex info, False otherwise.
     */
    @FFINameAlias("HasProperty")
    boolean hasProperty(@CXXReference StdString propertyName);

    /**
     * Saves the vertex info to a YAML file.
     *
     * @param fileName The name of the file to save to.
     * @return A Status object indicating success or failure.
     */
    @FFINameAlias("Save")
    @CXXValue
    Status save(@CXXReference StdString fileName);

    /**
     * Returns the vertex info as a YAML formatted string.
     *
     * @return A Result object containing the YAML string, or a Status object indicating an error.
     */
    @FFINameAlias("Dump")
    @CXXValue
    Result<StdString> dump();

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
     * Returns whether the vertex info contains the specified property group.
     *
     * @param propertyGroup The PropertyGroup object to check for.
     * @return True if the property group exists in the vertex info, False otherwise.
     */
    @FFINameAlias("HasPropertyGroup")
    boolean hasPropertyGroup(@CXXReference StdSharedPtr<PropertyGroup> propertyGroup);

    /**
     * Get the file path for the specified property group and chunk index.
     *
     * @param propertyGroup The PropertyGroup object to get the file path for.
     * @param chunkIndex The chunk index.
     * @return A Result object containing the file path, or a KeyError Status object if the property
     *     group is not found in the vertex info.
     */
    @FFINameAlias("GetFilePath")
    @CXXValue
    Result<StdString> getFilePath(
            @CXXValue StdSharedPtr<PropertyGroup> propertyGroup,
            @CXXValue @FFITypeAlias(GAR_ID_TYPE) long chunkIndex);

    /**
     * Get the path prefix for the specified property group.
     *
     * @param propertyGroup The PropertyGroup object to get the path prefix for.
     * @return A Result object containing the path prefix, or a KeyError Status object if the
     *     property group is not found in the vertex info.
     */
    @FFINameAlias("GetPathPrefix")
    @CXXValue
    Result<StdString> getPathPrefix(@CXXValue StdSharedPtr<PropertyGroup> propertyGroup);

    /**
     * Get the file path for the number of vertices.
     *
     * @return The file path for the number of vertices.
     */
    @FFINameAlias("GetVerticesNumFilePath")
    @CXXValue
    Result<StdString> getVerticesNumFilePath();

    /**
     * Returns whether the vertex info is validated.
     *
     * @return True if the vertex info is valid, False otherwise.
     */
    @FFINameAlias("IsValidated")
    boolean isValidated();

    /**
     * Loads vertex info from a YAML object.
     *
     * @param yaml A shared pointer to a Yaml object containing the YAML string.
     * @return A Result object containing the VertexInfo object, or a Status object indicating an
     *     error.
     */
    static Result<StdSharedPtr<VertexInfo>> load(StdSharedPtr<Yaml> yaml) {
        return Static.INSTANCE.Load(yaml);
    }

    @FFIGen
    @CXXHead(GAR_GRAPH_INFO_H)
    @FFILibrary(value = GAR_VERTEX_INFO, namespace = GAR_VERTEX_INFO)
    interface Static {
        Static INSTANCE = FFITypeFactory.getLibrary(VertexInfo.Static.class);

        @CXXValue
        Result<StdSharedPtr<VertexInfo>> Load(@CXXValue StdSharedPtr<Yaml> yaml);
    }
}
