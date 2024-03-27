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

package com.alibaba.graphar.writers.builder;

import static com.alibaba.graphar.util.CppClassName.GAR_BUILDER_VERTEX;
import static com.alibaba.graphar.util.CppClassName.GAR_ID_TYPE;
import static com.alibaba.graphar.util.CppHeaderName.GAR_VERTICES_BUILDER_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIConst;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISkip;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.stdcxx.StdUnorderedMap;

/** Vertex is designed for constructing vertices builder. */
@FFIGen
@FFITypeAlias(GAR_BUILDER_VERTEX)
@CXXHead(GAR_VERTICES_BUILDER_H)
public interface Vertex extends CXXPointer {

    Factory factory = FFITypeFactory.getFactory(Vertex.class);

    /**
     * Get id of the vertex.
     *
     * @return The id of the vertex.
     */
    @FFINameAlias("GetId")
    @FFITypeAlias(GAR_ID_TYPE)
    long getId();

    /**
     * Set id of the vertex.
     *
     * @param id The id of the vertex.
     */
    @FFINameAlias("SetId")
    void setId(@FFITypeAlias(GAR_ID_TYPE) long id);

    /**
     * Check if the vertex is empty.
     *
     * @return true/false.
     */
    @FFINameAlias("Empty")
    boolean empty();

    /**
     * Add a property to the vertex.
     *
     * @param name The name of the property.
     * @param val The value of the property.
     */
    @FFINameAlias("AddProperty")
    void addProperty(@CXXReference StdString name, @CXXReference StdString val);

    /**
     * Add a property to the vertex.
     *
     * @param name The name of the property.
     * @param val The value of the property.
     */
    @FFINameAlias("AddProperty")
    void addProperty(@CXXReference StdString name, long val);

    /**
     * Get a property of the vertex.
     *
     * @param property The name of the property.
     * @return The value of the property.
     */
    @FFINameAlias("GetProperty")
    @FFIConst
    @CXXReference
    <T> T getProperty(@CXXReference StdString property, @FFISkip T skipT);

    /**
     * Get all properties of the vertex.
     *
     * @return The map containing all properties of the vertex.
     */
    @FFINameAlias("GetProperties")
    @CXXReference
    <T> StdUnorderedMap<StdString, T> getProperties(@FFISkip T skipT);

    /**
     * Check if the vertex contains a property.
     *
     * @param property The name of the property.
     * @return true/false.
     */
    @FFINameAlias("ContainProperty")
    boolean containProperty(@CXXReference StdString property);

    @FFIFactory
    interface Factory {
        Vertex create();

        /**
         * Initialize the vertex with a given id.
         *
         * @param id The id of the vertex.
         */
        Vertex create(@FFITypeAlias(GAR_ID_TYPE) long id);
    }
}
