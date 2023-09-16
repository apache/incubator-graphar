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

import static com.alibaba.graphar.util.CppClassName.GAR_PROPERTY;
import static com.alibaba.graphar.util.CppHeaderName.GAR_GRAPH_INFO_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIGetter;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISetter;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.types.DataType;

/** Property is a class to store the property information for a group. */
@FFIGen
@FFITypeAlias(GAR_PROPERTY)
@CXXHead(GAR_GRAPH_INFO_H)
public interface Property extends CXXPointer {
    Factory factory = FFITypeFactory.getFactory(Property.class);

    @FFIGetter
    @FFINameAlias("name")
    @CXXValue
    StdString getName();

    @FFISetter
    @FFINameAlias("name")
    void setName(@CXXValue StdString name);

    @FFIGetter
    @FFINameAlias("type")
    @CXXValue
    DataType getType();

    @FFISetter
    @FFINameAlias("type")
    void setType(@CXXValue DataType type);

    @FFIGetter
    @FFINameAlias("is_primary")
    boolean isPrimary();

    @FFISetter
    @FFINameAlias("is_primary")
    void setPrimary(boolean primary);

    @FFIFactory
    interface Factory {
        @CXXValue
        Property create();
    }
}
