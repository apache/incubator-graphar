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

import static org.apache.graphar.util.CppClassName.GAR_PROPERTY;
import static org.apache.graphar.util.CppHeaderName.GAR_GRAPH_INFO_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import org.apache.graphar.stdcxx.StdSharedPtr;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.types.DataType;

/** Property is a class to store the property information for a group. */
@FFIGen
@FFITypeAlias(GAR_PROPERTY)
@CXXHead(GAR_GRAPH_INFO_H)
public interface Property extends CXXPointer {
    Factory factory = FFITypeFactory.getFactory(Property.class);

    @FFIGetter
    @CXXReference
    StdString name();

    @FFISetter
    void name(@CXXReference StdString name);

    @FFIGetter
    @CXXReference
    StdSharedPtr<DataType> type();

    @FFISetter
    void type(@CXXReference StdSharedPtr<DataType> type);

    @FFIGetter
    boolean is_primary();

    @FFISetter
    void is_primary(boolean nullable);

    @FFIGetter
    boolean is_nullable();

    @FFISetter
    void is_nullable(boolean nullable);

    @FFIFactory
    interface Factory {

        Property create(@CXXReference StdString name);
    }
}
