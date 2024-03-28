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

package org.apache.graphar.writers.builder;

import static org.apache.graphar.util.CppClassName.GAR_BUILDER_EDGE;
import static org.apache.graphar.util.CppClassName.GAR_ID_TYPE;
import static org.apache.graphar.util.CppHeaderName.GAR_EDGES_BUILDER_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.stdcxx.StdUnorderedMap;

/** Edge is designed for constructing edges builder. */
@FFIGen
@FFITypeAlias(GAR_BUILDER_EDGE)
@CXXHead(GAR_EDGES_BUILDER_H)
public interface Edge extends CXXPointer {

    Factory factory = FFITypeFactory.getFactory(Edge.class);

    /**
     * Check if the edge is empty.
     *
     * @return true/false.
     */
    @FFINameAlias("Empty")
    boolean empty();

    /**
     * Get source id of the edge.
     *
     * @return The id of the source vertex.
     */
    @FFINameAlias("GetSource")
    @FFITypeAlias(GAR_ID_TYPE)
    long getSource();

    /**
     * Get destination id of the edge.
     *
     * @return The id of the destination vertex.
     */
    @FFINameAlias("GetDestination")
    @FFITypeAlias(GAR_ID_TYPE)
    long getDestination();

    /**
     * Add a property to the edge.
     *
     * @param name The name of the property.
     * @param val The value of the property.
     */
    @FFINameAlias("AddProperty")
    void addProperty(@CXXReference StdString name, @CXXReference StdString val);

    /**
     * Add a property to the edge.
     *
     * @param name The name of the property.
     * @param val The value of the property.
     */
    @FFINameAlias("AddProperty")
    void addProperty(@CXXReference StdString name, long val);

    /**
     * Get a property of the edge.
     *
     * @param property The name of the property.
     * @return The value of the property.
     */
    @FFINameAlias("GetProperty")
    <T> @CXXReference T getProperty(@CXXReference StdString property);

    /**
     * Get all properties of the edge.
     *
     * @return The map containing all properties of the edge.
     */
    @FFINameAlias("GetProperties")
    <T> @CXXReference StdUnorderedMap<StdString, T> getProperties();

    /**
     * Check if the edge contains a property.
     *
     * @param property The name of the property.
     * @return true/false.
     */
    @FFINameAlias("ContainProperty")
    boolean containProperty(@CXXReference StdString property);

    @FFIFactory
    interface Factory {
        /**
         * Initialize the edge with its source and destination.
         *
         * @param srcId The id of the source vertex.
         * @param dstId The id of the destination vertex.
         */
        Edge create(@FFITypeAlias(GAR_ID_TYPE) long srcId, @FFITypeAlias(GAR_ID_TYPE) long dstId);
    }
}
