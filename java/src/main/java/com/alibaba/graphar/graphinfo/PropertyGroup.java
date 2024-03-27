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

package com.alibaba.graphar.graphinfo;

import static com.alibaba.graphar.util.CppClassName.GAR_PROPERTY_GROUP;
import static com.alibaba.graphar.util.CppHeaderName.GAR_GRAPH_INFO_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIConst;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.stdcxx.StdVector;
import com.alibaba.graphar.types.FileType;

/**
 * PropertyGroup is a class to store the property group information.
 *
 * <p>A property group is a collection of properties with a file type and prefix used for chunk
 * files. The prefix is optional and is the concatenation of property names with '_' as separator by
 * default.
 */
@FFIGen
@FFITypeAlias(GAR_PROPERTY_GROUP)
@CXXHead(GAR_GRAPH_INFO_H)
public interface PropertyGroup extends CXXPointer {

    Factory factory = FFITypeFactory.getFactory(PropertyGroup.class);

    @FFINameAlias("GetProperties")
    @FFIConst
    @CXXReference
    StdVector<Property> getProperties();

    /**
     * Get the file type of property group chunk file.
     *
     * @return The file type of group.
     */
    @FFINameAlias("GetFileType")
    @CXXValue
    FileType getFileType();

    /**
     * Get the prefix of property group chunk file.
     *
     * @return The path prefix of group.
     */
    @FFINameAlias("GetPrefix")
    @CXXReference
    StdString getPrefix();

    @CXXOperator("==")
    boolean eq(@CXXReference PropertyGroup other);

    @FFIFactory
    interface Factory {
        /**
         * Initialize the PropertyGroup with a list of properties, file type, and optional prefix.
         *
         * @param properties Property list of group
         * @param fileType File type of property group chunk file
         * @param prefix prefix of property group chunk file. The default prefix is the
         *     concatenation of property names with '_' as separator
         */
        @CXXValue
        PropertyGroup create(
                @CXXValue StdVector<Property> properties,
                @CXXValue FileType fileType,
                @CXXReference StdString prefix);

        /**
         * Initialize the PropertyGroup with a list of properties, file type, and optional prefix.
         *
         * @param properties Property list of group
         * @param fileType File type of property group chunk file
         */
        @CXXValue
        PropertyGroup create(@CXXValue StdVector<Property> properties, @CXXValue FileType fileType);

        PropertyGroup create();
    }
}
