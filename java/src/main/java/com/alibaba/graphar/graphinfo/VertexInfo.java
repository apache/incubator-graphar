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

import static com.alibaba.graphar.util.CppClassName.GAR_ID_TYPE;
import static com.alibaba.graphar.util.CppClassName.GAR_VERTEX_INFO;
import static com.alibaba.graphar.util.CppHeaderName.GAR_GRAPH_INFO_H;

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
import com.alibaba.graphar.stdcxx.StdSharedPtr;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.stdcxx.StdVector;
import com.alibaba.graphar.types.DataType;
import com.alibaba.graphar.util.InfoVersion;
import com.alibaba.graphar.util.Result;
import com.alibaba.graphar.util.Status;
import com.alibaba.graphar.util.Yaml;

/** VertexInfo is a class that stores metadata information about a vertex. */
@FFIGen
@FFITypeAlias(GAR_VERTEX_INFO)
@CXXHead(GAR_GRAPH_INFO_H)
public interface VertexInfo extends CXXPointer {

    Factory factory = FFITypeFactory.getFactory(VertexInfo.class);

    /**
     * Adds a property group to the vertex info.
     *
     * @param propertyGroup The PropertyGroup object to add.
     * @return A Status object indicating success or failure.
     */
    @FFINameAlias("AddPropertyGroup")
    @CXXValue
    Status addPropertyGroup(@CXXReference PropertyGroup propertyGroup);

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
    @CXXValue
    StdString getPrefix();

    /**
     * Get the version info of the vertex.
     *
     * @return The version info of the vertex.
     */
    @FFINameAlias("GetVersion")
    @FFIConst
    @CXXReference
    InfoVersion getVersion();

    /**
     * Get the property groups of the vertex.
     *
     * @return A vector of PropertyGroup objects for the vertex.
     */
    @FFINameAlias("GetPropertyGroups")
    @FFIConst
    @CXXReference
    StdVector<PropertyGroup> getPropertyGroups();

    /**
     * Get the property group that contains the specified property.
     *
     * @param propertyName The name of the property.
     * @return A Result object containing the PropertyGroup object, or a KeyError Status object if
     *     the property is not found.
     */
    @FFINameAlias("GetPropertyGroup")
    @CXXValue
    Result<@CXXReference PropertyGroup> getPropertyGroup(@CXXReference StdString propertyName);

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
     * Get whether the vertex info contains the specified property.
     *
     * @param propertyName The name of the property.
     * @return True if the property exists in the vertex info, False otherwise.
     */
    @FFINameAlias("ContainProperty")
    boolean containProperty(@CXXReference StdString propertyName);

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
    @FFITypeAlias("GraphArchive::Result<bool>")
    Result<Boolean> isPrimaryKey(@CXXReference StdString propertyName);

    /**
     * Returns whether the vertex info contains the specified property group.
     *
     * @param propertyGroup The PropertyGroup object to check for.
     * @return True if the property group exists in the vertex info, False otherwise.
     */
    @FFINameAlias("ContainPropertyGroup")
    boolean containPropertyGroup(@CXXReference PropertyGroup propertyGroup);

    /**
     * Returns a new VertexInfo object with the specified property group added to it.
     *
     * @param propertyGroup The PropertyGroup object to add.
     * @return A Result object containing the new VertexInfo object, or a Status object indicating
     *     an error.
     */
    @FFINameAlias("Extend")
    @CXXValue
    Result<VertexInfo> extend(@CXXReference PropertyGroup propertyGroup);

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
            @CXXReference PropertyGroup propertyGroup,
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
    Result<StdString> getPathPrefix(@CXXReference PropertyGroup propertyGroup);

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
    static Result<VertexInfo> load(StdSharedPtr<Yaml> yaml) {
        return Static.INSTANCE.Load(yaml);
    }

    @FFIFactory
    interface Factory {
        /**
         * Construct a VertexInfo object with the given metadata information.
         *
         * @param label The label of the vertex.
         * @param chunkSize The number of vertices in each vertex chunk.
         * @param version The version of the vertex info.
         * @param prefix The prefix of the vertex info.
         */
        VertexInfo create(
                @CXXReference StdString label,
                @FFITypeAlias(GAR_ID_TYPE) long chunkSize,
                @CXXReference InfoVersion version,
                @CXXReference StdString prefix);

        /**
         * Construct a VertexInfo object with the given metadata information.
         *
         * @param label The label of the vertex.
         * @param chunkSize The number of vertices in each vertex chunk.
         * @param version The version of the vertex info.
         */
        VertexInfo create(
                @CXXReference StdString label,
                @FFITypeAlias(GAR_ID_TYPE) long chunkSize,
                @CXXReference InfoVersion version);

        /** Copy constructor. */
        VertexInfo create(@CXXReference VertexInfo other);
    }

    @FFIGen
    @CXXHead(GAR_GRAPH_INFO_H)
    @FFILibrary(value = GAR_VERTEX_INFO, namespace = GAR_VERTEX_INFO)
    interface Static {
        Static INSTANCE = FFITypeFactory.getLibrary(VertexInfo.Static.class);

        @CXXValue
        Result<VertexInfo> Load(@CXXValue StdSharedPtr<Yaml> yaml);
    }
}
